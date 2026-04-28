import re
import time

import requests

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
            .add("CLIENT_ID", "Client ID do app Shopify (Dev Dashboard)", required=True)
            .add("CLIENT_SECRET", "Client Secret do app Shopify (Dev Dashboard)", required=True)
            .add("PROJECT_ID", "ID do projeto Google Cloud", required=True)
            .add("CRM_TYPE", "Nome da ferramenta", required=True)
            .add("GOOGLE_APPLICATION_CREDENTIALS", "Credencial GCS", required=True)
            .add("API_VERSION", "Versao da API Shopify", required=False)
            .parse())


def fetch_access_token(shop_name, client_id, client_secret):
    """Troca client_id+client_secret por um access_token via OAuth Client Credentials Grant.

    Token retornado expira em 24h, suficiente para uma execução completa.
    """
    logger.info("Solicitando access_token via OAuth Client Credentials")
    url = f"https://{shop_name}.myshopify.com/admin/oauth/access_token"
    response = requests.post(url, data={
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials",
    }, timeout=30)
    response.raise_for_status()
    return response.json()["access_token"]


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
        response = http_client.request("GET", url, headers=headers, params=request_params, raw=True)

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


def map_products(raw_products):
    """Mapeia campos dos products para o formato de saída."""
    products = []
    for p in raw_products:
        products.append({
            "id": str(p.get("id", "")),
            "title": p.get("title"),
            "vendor": p.get("vendor"),
            "product_type": p.get("product_type"),
            "status": p.get("status"),
            "tags": p.get("tags"),
            "created_at": p.get("created_at"),
            "updated_at": p.get("updated_at"),
            "published_at": p.get("published_at"),
        })
    return products


def extract_product_variants(raw_products):
    """Extrai variants de dentro de cada product."""
    logger.info(f"Extraindo variants de {len(raw_products)} products")
    variants = []
    for p in raw_products:
        product_id = str(p.get("id", ""))
        for v in p.get("variants", []):
            variants.append({
                "id": str(v.get("id", "")),
                "product_id": product_id,
                "title": v.get("title"),
                "sku": v.get("sku"),
                "price": v.get("price"),
                "compare_at_price": v.get("compare_at_price"),
                "inventory_quantity": v.get("inventory_quantity"),
                "inventory_item_id": str(v.get("inventory_item_id", "")),
                "created_at": v.get("created_at"),
                "updated_at": v.get("updated_at"),
            })
    logger.info(f"product_variants: {len(variants)} itens extraidos")
    return variants


def map_draft_orders(raw_draft_orders):
    """Mapeia campos dos draft orders para o formato de saída."""
    draft_orders = []
    for d in raw_draft_orders:
        customer = d.get("customer") or {}
        draft_orders.append({
            "id": str(d.get("id", "")),
            "status": d.get("status"),
            "email": d.get("email"),
            "total_price": d.get("total_price", 0),
            "subtotal_price": d.get("subtotal_price", 0),
            "total_tax": d.get("total_tax", 0),
            "currency": d.get("currency"),
            "customer_id": str(customer.get("id", "")) if customer.get("id") else None,
            "created_at": d.get("created_at"),
            "updated_at": d.get("updated_at"),
            "completed_at": d.get("completed_at"),
        })
    return draft_orders


def map_locations(raw_locations):
    """Mapeia campos dos locations para o formato de saída."""
    locations = []
    for l in raw_locations:
        locations.append({
            "id": str(l.get("id", "")),
            "name": l.get("name"),
            "active": l.get("active"),
            "address1": l.get("address1"),
            "city": l.get("city"),
            "province": l.get("province"),
            "country": l.get("country"),
            "zip": l.get("zip"),
        })
    return locations


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
        api_version = getattr(args, "API_VERSION", None) or "2024-01"

        base_url = f"https://{shop_name}.myshopify.com/admin/api/{api_version}"
        logger.info(f"Base URL: {base_url}")

        # 2. Access token via OAuth Client Credentials (24h)
        access_token = fetch_access_token(shop_name, args.CLIENT_ID, args.CLIENT_SECRET)

        # 3. HTTP client com rate limiter
        rate_limiter = RateLimiter(requests_per_window=RATE_LIMIT, window_seconds=1, logger=logger)
        http_client = HttpClient(base_url=base_url, rate_limiter=rate_limiter, logger=logger)

        # 4. Headers
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

        # 7. Products + variants
        raw_products = fetch_all_pages(
            http_client, "products.json", auth_headers, "products",
        )
        products = map_products(raw_products)
        process_endpoint("products", products, endpoint_stats)

        product_variants = extract_product_variants(raw_products)
        process_endpoint("product_variants", product_variants, endpoint_stats)

        # 8. Draft orders
        raw_draft_orders = fetch_all_pages(
            http_client, "draft_orders.json", auth_headers, "draft_orders",
        )
        draft_orders = map_draft_orders(raw_draft_orders)
        process_endpoint("draft_orders", draft_orders, endpoint_stats)

        # 9. Locations
        raw_locations = fetch_all_pages(
            http_client, "locations.json", auth_headers, "locations",
        )
        locations = map_locations(raw_locations)
        process_endpoint("locations", locations, endpoint_stats)

        # 10. Resumo
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
