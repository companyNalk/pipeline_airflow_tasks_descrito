import re
import time

from commons.app_inicializer import AppInitializer
from commons.big_query import BigQuery
from commons.memory_monitor import MemoryMonitor
from commons.report_generator import ReportGenerator
from commons.utils import Utils
from generic.argument_manager import ArgumentManager
from generic.http_client import HttpClient
from generic.rate_limiter import RateLimiter

logger = AppInitializer.initialize()

# Shopify basic plan: 2 requests/second
RATE_LIMIT = 2
PAGE_LIMIT = 250  # Shopify max per page


def get_arguments():
    """Configura e retorna os argumentos da linha de comando."""
    return (ArgumentManager("Script para coletar e processar dados da API Shopify")
            .add("SHOP_NAME", "Nome da loja Shopify (ex: minha-loja)", required=True)
            .add("API_ACCESS_TOKEN", "Token de acesso Shopify", required=True)
            .add("PROJECT_ID", "ID do projeto Google Cloud", required=True)
            .add("CRM_TYPE", "Nome da ferramenta", required=True)
            .add("GOOGLE_APPLICATION_CREDENTIALS", "Credencial GCS", required=True)
            .add("API_VERSION", "Versao da API Shopify", required=False)
            .parse())


def get_auth_headers(access_token):
    """Prepara os cabeçalhos de autenticação para a API Shopify."""
    logger.info("Preparando headers de autenticacao Shopify")
    return {
        "X-Shopify-Access-Token": access_token,
        "Content-Type": "application/json",
    }


def fetch_all_pages(http_client, endpoint, headers, resource_key, params=None):
    """
    Busca todas as páginas de um endpoint Shopify via Link header (cursor-based).

    A paginação do Shopify usa o header Link com rel="next" em vez de offset.
    Cada página seguinte já vem com a URL completa no header.
    """
    logger.info(f"Buscando todas as paginas para: {endpoint}")
    start_time = time.time()

    all_items = []
    request_params = dict(params or {})
    request_params["limit"] = PAGE_LIMIT

    # Primeira requisição usa path relativo
    url = endpoint

    while url:
        response = http_client.request("GET", url, headers=headers, params=request_params)

        data = response.json()
        items = data.get(resource_key, [])
        all_items.extend(items)

        page_num = (len(all_items) // PAGE_LIMIT) or 1
        if page_num == 1 or len(items) == PAGE_LIMIT:
            logger.info(f"Pagina {page_num}: {len(items)} itens (total acumulado: {len(all_items)})")

        # Extrair próxima URL do Link header
        link_header = response.headers.get("Link", "")
        match = re.search(r'<([^>]+)>;\s*rel="next"', link_header)
        if match:
            url = match.group(1)  # URL absoluta — HttpClient aceita URLs completas
            request_params = None  # Params já estão embutidos na URL
        else:
            url = None

    duration = time.time() - start_time
    logger.info(f"{endpoint}: {len(all_items)} itens obtidos em {duration:.2f}s")
    return all_items


def extract_order_items(raw_orders):
    """Extrai line_items de dentro de cada order."""
    logger.info(f"Extraindo line_items de {len(raw_orders)} orders")

    order_items = []
    for order in raw_orders:
        order_id = str(order.get("id", ""))
        for line in order.get("line_items", []):
            order_items.append({
                "id": str(line.get("id", "")),
                "order_id": order_id,
                "variant_id": str(line.get("variant_id", "")),
                "product_id": str(line.get("product_id", "")),
                "title": line.get("title"),
                "quantity": line.get("quantity", 1),
                "price": line.get("price", 0),
                "sku": line.get("sku"),
            })

    logger.info(f"order_items: {len(order_items)} itens extraidos")
    return order_items


def map_orders(raw_orders):
    """Mapeia campos dos orders para o formato de saída."""
    orders = []
    for o in raw_orders:
        customer = o.get("customer") or {}
        orders.append({
            "id": str(o.get("id", "")),
            "order_number": o.get("order_number"),
            "email": o.get("email"),
            "total_price": o.get("total_price", 0),
            "subtotal_price": o.get("subtotal_price", 0),
            "total_tax": o.get("total_tax", 0),
            "total_discounts": o.get("total_discounts", 0),
            "currency": o.get("currency"),
            "financial_status": o.get("financial_status"),
            "fulfillment_status": o.get("fulfillment_status"),
            "created_at": o.get("created_at"),
            "updated_at": o.get("updated_at"),
            "customer_id": str(customer.get("id", "")) if customer.get("id") else None,
        })
    return orders


def map_customers(raw_customers):
    """Mapeia campos dos customers para o formato de saída."""
    customers = []
    for c in raw_customers:
        customers.append({
            "id": str(c.get("id", "")),
            "email": c.get("email"),
            "first_name": c.get("first_name"),
            "last_name": c.get("last_name"),
            "orders_count": c.get("orders_count", 0),
            "total_spent": c.get("total_spent", 0),
            "created_at": c.get("created_at"),
            "updated_at": c.get("updated_at"),
        })
    return customers


def process_endpoint(name, data, endpoint_stats):
    """Processa e salva dados de um endpoint, retornando estatísticas."""
    try:
        logger.info(f"\n{'=' * 50}")
        logger.info(f"PROCESSANDO ENDPOINT: {name.upper()}")
        logger.info(f"{'=' * 50}")

        endpoint_start = time.time()
        processed = Utils.process_and_save_data(data, name)
        duration = time.time() - endpoint_start

        endpoint_stats[name] = {
            "registros": len(processed),
            "status": "Sucesso",
            "tempo": duration,
        }
        logger.info(f"{name}: {len(processed)} registros em {duration:.2f}s")

    except Exception as e:
        logger.exception(f"Falha no endpoint {name}")
        endpoint_stats[name] = {
            "registros": 0,
            "status": f"Falha: {type(e).__name__}: {str(e)}",
            "tempo": 0,
        }


def main():
    """Função principal para coleta de dados Shopify."""
    global_start_time = ReportGenerator.init_report(logger, "COLETA DE DADOS SHOPIFY")

    try:
        # 1. Configurações
        args = get_arguments()

        shop_name = args.SHOP_NAME
        access_token = args.API_ACCESS_TOKEN
        api_version = getattr(args, "API_VERSION", None) or "2024-01"

        base_url = f"https://{shop_name}.myshopify.com/admin/api/{api_version}"
        logger.info(f"Base URL: {base_url}")

        # 2. HTTP client com rate limiter
        rate_limiter = RateLimiter(requests_per_window=RATE_LIMIT, window_seconds=1, logger=logger)
        http_client = HttpClient(base_url=base_url, rate_limiter=rate_limiter, logger=logger)

        # 3. Headers
        auth_headers = get_auth_headers(access_token)

        endpoint_stats = {}

        # 4. Orders
        raw_orders = fetch_all_pages(
            http_client, "orders.json", auth_headers, "orders",
            params={"status": "any"},
        )
        orders = map_orders(raw_orders)
        process_endpoint("orders", orders, endpoint_stats)

        # 5. Order items (extraídos dos orders já buscados)
        order_items = extract_order_items(raw_orders)
        process_endpoint("order_items", order_items, endpoint_stats)

        # 6. Customers
        raw_customers = fetch_all_pages(
            http_client, "customers.json", auth_headers, "customers",
        )
        customers = map_customers(raw_customers)
        process_endpoint("customers", customers, endpoint_stats)

        # 7. Resumo
        success = ReportGenerator.final_summary(logger, endpoint_stats, global_start_time)

        # 8. BigQuery
        with MemoryMonitor(logger):
            BigQuery.process_csv_files()

        tables = Utils.get_existing_folders(logger)
        for table in tables:
            BigQuery.start_pipeline(
                args.PROJECT_ID, args.CRM_TYPE,
                table_name=table,
                credentials_path=args.GOOGLE_APPLICATION_CREDENTIALS,
            )

        if not success:
            raise Exception(f"Falhas nos endpoints: {success}")

    except Exception as e:
        logger.exception(f"ERRO CRITICO NA EXECUCAO: {e}")
        raise


if __name__ == "__main__":
    main()
