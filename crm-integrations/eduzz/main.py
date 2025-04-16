import concurrent.futures
import datetime
import os
import time

from commons.app_inicializer import AppInitializer
from commons.report_generator import ReportGenerator
from commons.utils import Utils
from generic.argument_manager import ArgumentManager
from generic.http_client import HttpClient
from generic.rate_limiter import RateLimiter

logger = AppInitializer.initialize()

MAX_IDS = 100
RATE_LIMIT = 100
MAX_WORKERS = min(10, os.cpu_count() or 5)

# Definição dos endpoints primários da API Eduzz
ENDPOINTS = {
    "customers": "v1/customers",
    "subscriptions_creation": "v1/subscriptions",
    "subscriptions_update": "v1/subscriptions",
    "sales": "v1/sales",
}

# Parâmetros específicos para cada endpoint
ENDPOINT_PARAMS = {
    "customers": {"itemsPerPage": 1000},
    "subscriptions_creation": {
        "filterBy": "creation",
        "startDate": "2020-01-01",
        "endDate": datetime.datetime.now().strftime("%Y-%m-%d"),
    },
    "subscriptions_update": {
        "filterBy": "update",
        "startDate": "2020-01-01",
        "endDate": datetime.datetime.now().strftime("%Y-%m-%d"),
    },
    "sales": {
        "itemsPerPage": 100,
        "startDate": "2020-01-01",
        "endDate": datetime.datetime.now().strftime("%Y-%m-%d"),
    },
}


def get_arguments():
    """Configura e retorna os argumentos da linha de comando."""
    return (ArgumentManager("Script para coletar e processar dados da API Eduzz")
            .add("API_BASE_URL", "URL base", required=True)
            .add("API_AUTH_TOKEN", "Token de acesso para autenticação Bearer", required=True)
            .parse())


def get_auth_headers(access_token):
    """Prepara os cabeçalhos de autenticação para a API Eduzz."""
    logger.info("🔑 Preparando headers de autenticação")
    return {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }


def fetch_data(http_client, endpoint, headers, page=1, params=None, debug_info=None):
    """Busca dados de um endpoint específico."""
    # Parâmetros padrão com possibilidade de override
    params = params or {}
    default_params = {"page": page}
    default_params.update(params)

    # Usar debug_info fornecido ou gerar um padrão
    req_debug = debug_info or f"{endpoint}:p{page}"

    return http_client.get(endpoint, headers=headers, params=default_params, debug_info=req_debug)


def fetch_page(http_client, endpoint, headers, page, base_params=None):
    """Busca uma página específica de um endpoint."""
    try:
        params = base_params.copy() if base_params else {}
        params["page"] = page

        data = fetch_data(http_client, endpoint, headers, page, params)
        items = data.get('items', [])
        total_pages = data.get('pages', 0)
        total_items = data.get('totalItems', 0)
        items_per_page = data.get('itemsPerPage', 100)

        # Log condicional para reduzir verbosidade
        if page == 1 or page == total_pages or page % 20 == 0:
            logger.info(f"📄 Endpoint {endpoint}: página {page}/{total_pages} com {len(items)} itens")

        return {
            'items': items,
            'total_pages': total_pages,
            'total_items': total_items,
            'current_page': page,
            'items_per_page': items_per_page
        }
    except Exception as e:
        logger.error(f"❌ Erro ao buscar página {page} para {endpoint}: {str(e)}")
        raise


def fetch_all_pages(http_client, endpoint, headers, base_params=None):
    """Busca todas as páginas de um endpoint usando ThreadPoolExecutor."""
    logger.info(f"📚 Buscando todas as páginas para: {endpoint}")
    start_time = time.time()

    # Buscar primeira página
    first_page = fetch_page(http_client, endpoint, headers, 1, base_params)
    all_items = first_page['items'].copy()  # Uso de .copy() para evitar referências
    total_pages = first_page['total_pages']

    if total_pages <= 1:
        logger.info(f"✓ Endpoint {endpoint}: 1 página com {len(all_items)} itens obtidos")
        return all_items

    # Definir worker para buscar páginas em paralelo
    def fetch_page_worker(page_num):
        try:
            page_data = fetch_page(http_client, endpoint, headers, page_num, base_params)
            return page_data['items']
        except Exception as e:
            logger.error(f"❌ Worker: erro na página {page_num} para {endpoint}: {str(e)}")
            return []

    # Gerar lista de páginas restantes
    remaining_pages = list(range(2, total_pages + 1))
    logger.info(f"🔄 Coletando {len(remaining_pages)} páginas restantes para {endpoint} com {MAX_WORKERS} workers")

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_page = {executor.submit(fetch_page_worker, page): page for page in remaining_pages}

        completed = 0
        for future in concurrent.futures.as_completed(future_to_page):
            page_items = future.result()
            all_items.extend(page_items)

            # Log de progresso periodicamente
            completed += 1
            if completed % 10 == 0 or completed == len(remaining_pages):
                progress = (completed / len(remaining_pages)) * 100
                logger.info(f"📊 Progresso {endpoint}: {completed}/{len(remaining_pages)} páginas ({progress:.1f}%)")

    duration = time.time() - start_time
    logger.info(f"✅ Endpoint {endpoint}: {total_pages} páginas com {len(all_items)} itens obtidos em {duration:.2f}s")
    return all_items


def process_primary_endpoint(http_client, endpoint_name, endpoint_path, headers, params=None):
    """Processa um endpoint principal e retorna os dados e estatísticas."""
    try:
        logger.info(f"\n{'=' * 50}\n🔍 PROCESSANDO ENDPOINT: {endpoint_name.upper()}\n{'=' * 50}")

        endpoint_start = time.time()
        raw_data = fetch_all_pages(http_client, endpoint_path, headers, params)

        # Processar e salvar dados
        logger.info(f"💾 Processando e salvando {len(raw_data)} registros para {endpoint_name}")
        processed_data = Utils.process_and_save_data(raw_data, endpoint_name)

        endpoint_duration = time.time() - endpoint_start

        # Estatísticas de processamento
        stats = {
            "registros": len(processed_data),
            "status": "Sucesso",
            "tempo": endpoint_duration
        }

        logger.info(f"✅ {endpoint_name}: {len(processed_data)} registros em {endpoint_duration:.2f}s")

        return processed_data, stats

    except Exception as e:
        logger.exception(f"❌ Falha no endpoint {endpoint_name}")

        # Estatísticas em caso de erro
        stats = {
            "registros": 0,
            "status": f"Falha: {type(e).__name__}: {str(e)}",
            "tempo": 0
        }

        return [], stats


def main():
    """Função principal para coleta de dados da API Eduzz."""
    # Iniciar relatório de processamento
    global_start_time = ReportGenerator.init_report(logger)

    try:
        # 1. Obter configurações
        args = get_arguments()

        api_base_url = args.API_BASE_URL.rstrip('/')
        api_access_token = args.API_AUTH_TOKEN

        # 2. Configurar cliente HTTP e rate limiter
        rate_limiter = RateLimiter(requests_per_window=RATE_LIMIT, logger=logger)
        http_client = HttpClient(base_url=api_base_url, rate_limiter=rate_limiter, default_timeout=60, logger=logger)

        # 3. Preparar headers de autenticação
        auth_headers = get_auth_headers(api_access_token)
        logger.info("✅ Headers de autenticação preparados com sucesso")

        # 4. Processamento de endpoints principais
        endpoint_stats = {}

        for endpoint_name, endpoint_path in ENDPOINTS.items():
            # Obter parâmetros específicos para o endpoint
            params = ENDPOINT_PARAMS.get(endpoint_name, {})

            processed_data, stats = process_primary_endpoint(
                http_client,
                endpoint_name,
                endpoint_path,
                auth_headers,
                params
            )

            endpoint_stats[endpoint_name] = stats

        # 5. Gerar resumo final
        success = ReportGenerator.final_summary(logger, endpoint_stats, global_start_time)

        # Se houver falhas, lançar exceção para o Airflow
        if not success:
            raise Exception(f"Falhas nos endpoints: {success}")

    except Exception:
        logger.exception("❌ ERRO CRÍTICO NA EXECUÇÃO")
        raise


if __name__ == "__main__":
    main()
