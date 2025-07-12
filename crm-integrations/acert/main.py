import time
from datetime import timedelta, datetime

from commons.advanced_utils import AdvancedUtils
from commons.app_inicializer import AppInitializer
from commons.big_query import BigQuery
from commons.memory_monitor import MemoryMonitor
from commons.report_generator import ReportGenerator
from commons.utils import Utils
from generic.argument_manager import ArgumentManager
from generic.http_client import HttpClient
from generic.rate_limiter import RateLimiter

logger = AppInitializer.initialize()

RATE_LIMIT = 100
BASE_URL = "https://stores.grupoacert.app/api/v1"

ENDPOINTS = {
    # "customers": "stores/{store_id}/customers",
    # "cash_flow": "stores/{store_id}/cash-flow",
    # "products": "stores/{store_id}/products/",
    # "products_simplified": "stores/{store_id}/products/simplified",
    "sales": "stores/{store_id}/sales",
    # "payments": "stores/{store_id}/payments",
    # "invoice_history": "stores/{store_id}/invoice_history",
    # "types_of_sales": "stores/{store_id}/types-of-sales",
    # "users": "stores/{store_id}/users"
}


def get_arguments():
    """Configura e retorna os argumentos da linha de comando."""
    return (ArgumentManager("Script para coletar e processar dados da API Grupo ACERT")
            .add("API_AUTH_TOKEN", "Token de autenticação", required=True)
            .add("API_STORE_ID", "IDs das lojas separados por vírgula (ex: 183,184)", required=True)
            .add("PROJECT_ID", "Token de autenticação para imóveis", required=True)
            .add("CRM_TYPE", "Token de autenticação para imóveis", required=True)
            .add("GOOGLE_APPLICATION_CREDENTIALS", "Credencial GCS", required=True)
            .parse())


def parse_store_ids(store_ids):
    """Converte string de IDs separados por vírgula ou lista em lista de inteiros."""
    try:
        if isinstance(store_ids, list):
            return store_ids

        store_ids_list = [int(store_id.strip()) for store_id in store_ids.split(',')]
        return store_ids_list
    except ValueError as e:
        logger.error(f"❌ Erro ao processar IDs das lojas: {store_ids}. Erro: {e}")
        raise


def fetch_data(endpoint, token, page=0, params=None, debug_info=None):
    """Busca dados de um endpoint específico."""
    headers = {"Authorization": token}

    params = {**(params or {}), "size": 200, "page": page}
    debug_info = debug_info or f"{endpoint}:p{page}"

    return http_client.get(endpoint, headers=headers, params=params, debug_info=debug_info)


def fetch_page(endpoint, token, page=0, extra_params=None):
    """Busca uma página específica de um endpoint."""
    try:
        data = fetch_data(endpoint, token, page, extra_params)

        # Verificar se a resposta é uma lista direta ou um objeto com content
        if isinstance(data, list):
            # Resposta é uma lista direta (sem paginação)
            items = data
            meta = {
                'numberOfElements': len(data),
                'size': len(data),
                'totalPages': 1,
                'totalElements': len(data),
                'number': 0,
                'empty': len(data) == 0
            }
            total_pages = 1
            logger.info(f"📄 Endpoint {endpoint}: resposta direta com {len(items)} itens (sem paginação)")
        else:
            # Resposta é um objeto com content (com paginação)
            items = data.get('content', [])
            meta = {
                'numberOfElements': int(data.get('numberOfElements', 0)),
                'size': int(data.get('size', 20)),
                'totalPages': int(data.get('totalPages', 1)),
                'totalElements': int(data.get('totalElements', 0)),
                'number': int(data.get('number', page)),
                'empty': data.get('empty', True)
            }
            total_pages = meta['totalPages']

            # Log de progresso
            if page == 0 or page == total_pages - 1 or page % 20 == 0:
                logger.info(f"📄 Endpoint {endpoint}: página {page + 1}/{total_pages} com {len(items)} itens")

        return {
            'items': items,
            'meta': meta,
            'total_pages': total_pages
        }
    except Exception as e:
        logger.error(f"❌ Erro ao buscar página {page} para {endpoint}: {str(e)}")
        raise


def fetch_all_pages(endpoint, token, extra_params=None):
    """Busca todas as páginas de um endpoint."""
    logger.info(f"📚 Buscando todas as páginas para: {endpoint}")
    start_time = time.time()

    # Buscar primeira página (página 0 na API ACERT)
    first_page = fetch_page(endpoint, token, 0, extra_params)
    all_items = first_page['items'].copy()
    total_pages = first_page['total_pages']
    total_elements = first_page['meta'].get('totalElements', len(all_items))

    logger.info(f"📊 Total de elementos esperados: {total_elements}")

    if total_pages <= 1:
        logger.info(f"✓ Endpoint {endpoint}: {total_pages} página(s) com {len(all_items)} itens obtidos")
        return all_items

    # Buscar páginas restantes (começando da página 1)
    for page_num in range(1, total_pages):
        try:
            page_data = fetch_page(endpoint, token, page_num, extra_params)
            all_items.extend(page_data['items'])

            # Log a cada 10 páginas ou na última página
            if page_num % 10 == 0 or page_num == total_pages - 1:
                logger.info(f"📄 Progresso: {len(all_items)}/{total_elements} itens coletados")

            # Pausa pequena entre requisições
            if page_num < total_pages - 1:
                time.sleep(0.5)

        except Exception as e:
            logger.error(f"❌ Erro na página {page_num} para {endpoint}: {str(e)}")

    duration = time.time() - start_time

    # Verificar se o número de itens coletados confere com o total esperado
    if len(all_items) != total_elements:
        logger.warning(f"⚠️  Divergência detectada! Coletado: {len(all_items)}, Esperado: {total_elements}")

    logger.info(f"✅ Endpoint {endpoint}: {total_pages} páginas com {len(all_items)} itens obtidos em {duration:.2f}s")
    return all_items


def get_endpoint_config(endpoint_name):
    """
    Retorna configuração específica para cada endpoint.
    """
    configs = {
        "sales": {
            "extra_params": {"withItems": "true"},
            "relational_config": {
                "items": {
                    "parent_id_field": "sale_id",
                    "fields_to_extract": None,
                    "table_name": "sale_items"
                }
            },
            "use_relational": True
        },
    }

    # Configuração padrão para endpoints sem configuração específica
    default_config = {
        "extra_params": None,
        "relational_config": None,
        "use_relational": False
    }

    return configs.get(endpoint_name, default_config)


def get_last_6_months_dates():
    """Gera lista de datas dos últimos 6 meses no formato YYYY-MM-DD."""
    today = datetime.now()
    six_months_ago = today - timedelta(days=1)  # Aproximadamente 6 meses

    dates = []
    current_date = six_months_ago

    while current_date <= today:
        dates.append(current_date.strftime("%Y-%m-%d"))
        current_date += timedelta(days=1)

    logger.info(f"📅 Período: {dates[0]} até {dates[-1]} ({len(dates)} dias)")
    return dates


def process_cash_flow_endpoint(endpoint_name, endpoint_path, token, store_id):
    """Processa endpoint cash-flow com receiptDate para cada dia dos últimos 6 meses."""
    try:
        formatted_endpoint = endpoint_path.format(store_id=store_id)
        display_name = f"{store_id}_{endpoint_name}"

        logger.info(f"\n{'=' * 50}\n🔍 PROCESSANDO ENDPOINT: {display_name.upper()}\n{'=' * 50}")

        endpoint_start = time.time()
        all_data = []
        dates = get_last_6_months_dates()

        # Buscar dados para cada data
        for i, date in enumerate(dates):
            logger.info(f"📅 Processando data {date} ({i + 1}/{len(dates)})")

            try:
                # Parâmetros específicos para cash-flow
                extra_params = {"receiptDate": date}
                raw_data = fetch_all_pages(formatted_endpoint, token, extra_params)

                # Adicionar a data aos registros para identificação
                for record in raw_data:
                    record['_receiptDate'] = date

                all_data.extend(raw_data)
                logger.info(f"✓ Data {date}: {len(raw_data)} registros coletados")

                # Pausa entre datas para evitar sobrecarga
                if i < len(dates) - 1:
                    time.sleep(1)

            except Exception as e:
                logger.error(f"❌ Erro na data {date}: {str(e)}")
                continue

        # Processar e salvar dados
        logger.info(f"💾 Processando e salvando {len(all_data)} registros para {display_name}")
        processed_data = Utils.process_and_save_data(all_data, display_name)

        endpoint_duration = time.time() - endpoint_start

        return {
            "registros": len(processed_data),
            "status": "Sucesso",
            "tempo": endpoint_duration
        }

    except Exception as e:
        logger.exception(f"❌ Falha no endpoint {display_name}")
        return {
            "registros": 0,
            "status": f"Falha: {type(e).__name__}: {str(e)}",
            "tempo": 0
        }


def process_endpoint(endpoint_name, endpoint_path, token, store_id):
    """Processa um endpoint específico e retorna estatísticas."""
    # Tratamento especial para cash-flow (mantém a lógica original)
    if endpoint_name == "cash_flow":
        return process_cash_flow_endpoint(endpoint_name, endpoint_path, token, store_id)

    try:
        # Formatar endpoint com store_id
        formatted_endpoint = endpoint_path.format(store_id=store_id)
        display_name = f"{store_id}_{endpoint_name}"

        logger.info(f"\n{'=' * 50}\n🔍 PROCESSANDO ENDPOINT: {display_name.upper()}\n{'=' * 50}")

        endpoint_start = time.time()

        # Obter configuração do endpoint
        config = get_endpoint_config(endpoint_name)
        extra_params = config["extra_params"]

        raw_data = fetch_all_pages(formatted_endpoint, token, extra_params)

        # Escolher método de processamento baseado na configuração
        if config["use_relational"] and config["relational_config"]:
            logger.info(f"🔄 Aplicando extração relacional para {endpoint_name}")

            processed_tables = AdvancedUtils.process_with_relational_extraction(
                raw_data=raw_data,
                endpoint_name=display_name,
                table_configs=config["relational_config"],
                parent_id_field="id",
                auto_detect=False
            )

            # Calcular total de registros
            total_records = sum(len(data) if isinstance(data, list) else 0
                                for data in processed_tables.values())

            result = {
                "registros": total_records,
                "tabelas": processed_tables,
                "status": "Sucesso com extração relacional"
            }

        else:
            # Processamento normal para endpoints sem extração relacional
            logger.info(f"📄 Processamento normal para {endpoint_name}")
            processed_data = Utils.process_and_save_data(raw_data, display_name)

            result = {
                "registros": len(processed_data),
                "tabelas": {"main_table": processed_data},
                "status": "Sucesso"
            }

        endpoint_duration = time.time() - endpoint_start

        # Log dos resultados
        if "tabelas" in result:
            for table_name, data in result["tabelas"].items():
                records_count = len(data) if isinstance(data, list) else 0
                logger.info(f"📊 {table_name}: {records_count} registros processados")

        # Retornar estatísticas
        return {
            "registros": result["registros"],
            "status": result["status"],
            "tempo": endpoint_duration,
            "tabelas_geradas": list(result["tabelas"].keys()) if "tabelas" in result else []
        }

    except Exception as e:
        logger.exception(f"❌ Falha no endpoint {display_name}")
        return {
            "registros": 0,
            "status": f"Falha: {type(e).__name__}: {str(e)}",
            "tempo": 0,
            "tabelas_geradas": []
        }


def main():
    """Função principal para coleta de dados."""
    # 1. Obter argumentos de linha de comando
    args = get_arguments()

    # 2. Processar IDs das lojas
    store_ids = parse_store_ids(args.API_STORE_ID)
    logger.info(f"🏪 Lojas a serem processadas: {store_ids}")

    # 3. Configurar cliente HTTP
    global http_client
    api_auth_token = args.API_AUTH_TOKEN

    rate_limiter = RateLimiter(requests_per_window=RATE_LIMIT, logger=logger)
    http_client = HttpClient(base_url=BASE_URL, rate_limiter=rate_limiter, logger=logger)

    # 4. Iniciar relatório
    global_start_time = ReportGenerator.init_report(logger)
    endpoint_stats = {}

    try:
        # 5. Processar endpoints para cada store_id
        for store_id in store_ids:
            logger.info(f"\n🏪 PROCESSANDO LOJA ID: {store_id}")

            for endpoint_name, endpoint_path in ENDPOINTS.items():
                stats_key = f"{store_id}_{endpoint_name}"
                endpoint_stats[stats_key] = process_endpoint(endpoint_name, endpoint_path, api_auth_token, store_id)

                # Log detalhado dos resultados
                stats = endpoint_stats[stats_key]
                logger.info(f"✅ {stats_key}: {stats['registros']} registros em {stats['tempo']:.2f}s")
                logger.info(f"   Status: {stats['status']}")
                if stats.get('tabelas_geradas'):
                    logger.info(f"   Tabelas geradas: {', '.join(stats['tabelas_geradas'])}")

        # 6. Gerar resumo final
        success = ReportGenerator.final_summary(logger, endpoint_stats, global_start_time)

        with MemoryMonitor(logger):
            BigQuery.process_csv_files()

        tables = Utils.get_existing_folders(logger)
        for table in tables:
            BigQuery.start_pipeline(args.PROJECT_ID, args.CRM_TYPE, table_name=table,
                                    credentials_path=args.GOOGLE_APPLICATION_CREDENTIALS)

        # Se houver falhas, lançar exceção
        if not success:
            raise Exception(f"Falhas nos endpoints: {success}")

    except Exception as e:
        logger.exception(f"❌ ERRO CRÍTICO NA EXECUÇÃO: {e}")
        raise


if __name__ == "__main__":
    main()
