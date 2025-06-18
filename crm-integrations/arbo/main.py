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

CONFIG = {
    "rate_limit": 100,
    "endpoints": {
        "leads": {"path": "leads", "url_key": "API_BASE_URL_LEADS", "token_key": "API_AUTH_TOKEN_LEADS"},
        "imoveis": {"path": "imoveis", "url_key": "API_BASE_URL_IMOVEIS", "token_key": "API_AUTH_TOKEN_IMOVEIS"},
    }
}


def get_arguments():
    """Configura e retorna os argumentos da linha de comando."""
    return (ArgumentManager("Script para coletar e processar dados da API ArboCRM")
            .add("API_BASE_URL_LEADS", "URL base para leads", required=True)
            .add("API_AUTH_TOKEN_LEADS", "Token de autenticação para leads", required=True)
            .add("API_BASE_URL_IMOVEIS", "URL base para imóveis", required=True)
            .add("API_AUTH_TOKEN_IMOVEIS", "Token de autenticação para imóveis", required=True)
            .add("PROJECT_ID", "ID do projeto GCS", required=True)
            .add("CRM_TYPE", "Ferramenta: Nome aba sheets", required=True)
            .add("GOOGLE_APPLICATION_CREDENTIALS", "Credenciais GCS", required=True)
            .parse())


def fetch_all_data(http_client, endpoint, token):
    """Busca todos os dados de um endpoint."""
    logger.info(f"📚 Buscando dados para: {endpoint}")
    start_time = time.time()
    all_items = []

    # Buscar primeira página
    page_num = 1
    headers = {"Authorization": token}

    while True:
        try:
            params = {"page": page_num, "perPage": 500}
            data = http_client.get(endpoint, headers=headers, params=params)

            items = data.get('data', [])
            all_items.extend(items)

            total_pages = int(data.get('lastPage', 1))
            if page_num == 1 or page_num == total_pages or page_num % 20 == 0:
                logger.info(f"📄 Endpoint {endpoint}: página {page_num}/{total_pages} com {len(items)} itens")

            if page_num >= total_pages:
                break

            page_num += 1
            time.sleep(0.5)  # Pausa entre requisições

        except Exception as e:
            logger.error(f"❌ Erro na página {page_num} para {endpoint}: {str(e)}")
            raise

    duration = time.time() - start_time
    logger.info(f"✅ Endpoint {endpoint}: {total_pages} páginas com {len(all_items)} itens obtidos em {duration:.2f}s")
    return all_items


def process_endpoint(endpoint_name, endpoint_config, args):
    """Processa um endpoint específico e retorna estatísticas."""
    logger.info(f"\n{'=' * 50}\n🔍 PROCESSANDO: {endpoint_name.upper()}\n{'=' * 50}")

    try:
        # Configurações
        base_url = getattr(args, endpoint_config['url_key']).rstrip('/')
        token = getattr(args, endpoint_config['token_key'])

        # Cliente HTTP
        rate_limiter = RateLimiter(requests_per_window=CONFIG["rate_limit"], logger=logger)
        http_client = HttpClient(base_url=base_url, rate_limiter=rate_limiter, logger=logger)

        # Buscar e processar dados
        start_time = time.time()
        raw_data = fetch_all_data(http_client, endpoint_config['path'], token)

        # Processar dados
        logger.info(f"💾 Processando e salvando {len(raw_data)} registros para {endpoint_name}")
        processed_data = Utils.process_and_save_data(raw_data, endpoint_name)

        return {
            "registros": len(processed_data),
            "status": "Sucesso",
            "tempo": time.time() - start_time
        }

    except Exception as e:
        logger.exception(f"❌ Falha no endpoint {endpoint_name}")
        return {"registros": 0, "status": f"Falha: {type(e).__name__}: {str(e)}", "tempo": 0}


def main():
    """Função principal para coleta de dados."""
    args = get_arguments()
    global_start_time = ReportGenerator.init_report(logger)
    endpoint_stats = {}

    print(args.GOOGLE_APPLICATION_CREDENTIALS)

    try:
        for endpoint_name, endpoint_config in CONFIG["endpoints"].items():
            endpoint_stats[endpoint_name] = process_endpoint(endpoint_name, endpoint_config, args)
            logger.info(
                f"✅ {endpoint_name}: {endpoint_stats[endpoint_name]['registros']} registros em {endpoint_stats[endpoint_name]['tempo']:.2f}s")

        if not ReportGenerator.final_summary(logger, endpoint_stats, global_start_time):
            raise Exception("Falhas encontradas na execução")

        with MemoryMonitor(logger):
            BigQuery.process_csv_files()

        for endpoint_name in CONFIG["endpoints"].keys():
            BigQuery.start_pipeline(args.PROJECT_ID, args.CRM_TYPE, table_name=endpoint_name, credentials_path=args.GOOGLE_APPLICATION_CREDENTIALS)

    except Exception as e:
        logger.exception(f"❌ ERRO CRÍTICO: {e}")
        raise


if __name__ == "__main__":
    main()
