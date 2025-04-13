import time

from commons.app_inicializer import AppInitializer
from commons.report_generator import ReportGenerator
from commons.utils import Utils
from generic.argument_manager import ArgumentManager
from generic.http_client import HttpClient
from generic.rate_limiter import RateLimiter

logger = AppInitializer.initialize()

RATE_LIMIT = 100
ENDPOINTS = {
    "leads": "leads",
}

Utils.ensure_output_directories(ENDPOINTS)


def get_arguments():
    """Configura e retorna os argumentos da linha de comando."""
    return (ArgumentManager("Script para coletar e processar dados da API ArboCRM")
            .add("API_BASE_URL", "URL base", required=True)
            .add("API_AUTH_TOKEN", "Token de autenticação", required=True)
            .parse())


def fetch_data(endpoint, token, page=1, params=None, debug_info=None):
    """Busca dados de um endpoint específico."""
    headers = {"Authorization": token}

    # Use valor padrão para parâmetros
    params = params or {}
    params.update({"page": page, "perPage": 500})

    debug_info = debug_info or f"{endpoint}:p{page}"

    return http_client.get(endpoint, headers=headers, params=params, debug_info=debug_info)


def fetch_page(endpoint, token, page=1):
    """Busca uma página específica de um endpoint."""
    try:
        data = fetch_data(endpoint, token, page)
        items = data.get('data', [])

        # Simplificando a criação do dicionário meta
        meta = {
            'total': int(data.get('total', 0)),
            'perPage': int(data.get('perPage', 500)),
            'totalPages': int(data.get('lastPage', 1)),
            'currentPage': int(data.get('page', page)),
        }
        total_pages = meta['totalPages']

        # Simplificação da condição de log
        if page == 1 or page == total_pages or page % 20 == 0:
            logger.info(f"📄 Endpoint {endpoint}: página {page}/{total_pages} com {len(items)} itens")

        return {
            'items': items,
            'meta': meta,
            'total_pages': total_pages
        }
    except Exception as e:
        logger.error(f"❌ Erro ao buscar página {page} para {endpoint}: {str(e)}")
        raise


def fetch_all_pages(endpoint, token):
    """Busca todas as páginas de um endpoint."""
    logger.info(f"📚 Buscando todas as páginas para: {endpoint}")
    start_time = time.time()

    # Buscar primeira página
    first_page = fetch_page(endpoint, token)
    all_items = first_page['items'].copy()
    total_pages = first_page['total_pages']

    if total_pages <= 1:
        logger.info(f"✓ Endpoint {endpoint}: 1 página com {len(all_items)} itens obtidos")
        return all_items

    # Buscar páginas restantes
    for page_num in range(2, total_pages + 1):
        try:
            page_data = fetch_page(endpoint, token, page_num)
            all_items.extend(page_data['items'])

            # Pausa pequena entre requisições
            if page_num < total_pages:
                time.sleep(0.5)

        except Exception as e:
            logger.error(f"❌ Erro na página {page_num} para {endpoint}: {str(e)}")

    duration = time.time() - start_time
    logger.info(f"✅ Endpoint {endpoint}: {total_pages} páginas com {len(all_items)} itens obtidos em {duration:.2f}s")
    return all_items


def process_endpoint(endpoint_name, endpoint_path, token):
    """Processa um endpoint específico e retorna estatísticas."""
    try:
        logger.info(f"\n{'=' * 50}\n🔍 PROCESSANDO ENDPOINT: {endpoint_name.upper()}\n{'=' * 50}")

        endpoint_start = time.time()
        raw_data = fetch_all_pages(endpoint_path, token)

        Utils.ensure_output_directories(endpoint_name)

        # Processar e salvar dados
        logger.info(f"💾 Processando e salvando {len(raw_data)} registros para {endpoint_name}")
        processed_data = Utils.process_and_save_data(raw_data, endpoint_name)

        endpoint_duration = time.time() - endpoint_start

        # Retornar estatísticas
        return {
            "registros": len(processed_data),
            "status": "Sucesso",
            "tempo": endpoint_duration
        }

    except Exception as e:
        logger.exception(f"❌ Falha no endpoint {endpoint_name}")
        return {
            "registros": 0,
            "status": f"Falha: {type(e).__name__}: {str(e)}",
            "tempo": 0
        }


def main():
    """Função principal para coleta de dados."""
    # 1. Obter argumentos de linha de comando
    args = get_arguments()

    # 2. Configurar cliente HTTP
    global http_client
    api_base_url = args.API_BASE_URL.rstrip('/')
    api_auth_token = args.API_AUTH_TOKEN

    rate_limiter = RateLimiter(requests_per_window=RATE_LIMIT, logger=logger)
    http_client = HttpClient(base_url=api_base_url, rate_limiter=rate_limiter, logger=logger)

    # 3. Iniciar relatório
    global_start_time = ReportGenerator.init_report(logger)
    endpoint_stats = {}

    try:
        # 4. Processar todos os endpoints
        for endpoint_name, endpoint_path in ENDPOINTS.items():
            endpoint_stats[endpoint_name] = process_endpoint(endpoint_name, endpoint_path, api_auth_token)
            logger.info(
                f"✅ {endpoint_name}: {endpoint_stats[endpoint_name]['registros']} registros em {endpoint_stats[endpoint_name]['tempo']:.2f}s")

        # 5. Gerar resumo final
        success = ReportGenerator.final_summary(logger, endpoint_stats, global_start_time)

        # Se houver falhas, lançar exceção
        if not success:
            error_str = ", ".join([ep for ep, stats in endpoint_stats.items() if "Falha" in stats["status"]])
            raise Exception(f"Falhas nos endpoints: {error_str}")

        return True

    except Exception as e:
        logger.exception(f"❌ ERRO CRÍTICO NA EXECUÇÃO: {e}")
        raise


if __name__ == "__main__":
    main()
