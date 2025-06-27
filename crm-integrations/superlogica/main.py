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
    "base_url": "http://apps.superlogica.net/imobiliaria/api",
    "endpoints": {
        "contratos": {
            "path": "contratos",
            "data_key": "data"
        },
        "proprietarios": {
            "path": "proprietarios",
            "data_key": "data"
        },
        "locatarios": {
            "path": "locatarios",
            "data_key": "data"
        }
    }
}


def get_arguments():
    """Configura e retorna os argumentos da linha de comando."""
    return (ArgumentManager("Script para coletar e processar dados da API Superlógica")
            .add("API_APP_TOKEN", "Token da aplicação", required=True)
            .add("API_ACCESS_TOKEN", "Token de acesso", required=True)
            .add("PROJECT_ID", "ID do projeto Google Cloud", required=True)
            .add("CRM_TYPE", "Nome da ferramenta", required=True)
            .add("GOOGLE_APPLICATION_CREDENTIALS", "Credencial GCS", required=True)
            .parse())


def fetch_endpoint_data(http_client, endpoint_config, app_token, access_token):
    """Busca dados de um endpoint."""
    headers = {
        "app_token": app_token,
        "access_token": access_token
    }

    try:
        logger.info(f"📊 Coletando dados do endpoint {endpoint_config['path']}")

        response = http_client.get(endpoint_config["path"], headers=headers)

        # Extrair dados da resposta
        if endpoint_config["data_key"]:
            data = response.get(endpoint_config["data_key"], [])
        else:
            data = response if isinstance(response, list) else []

        logger.info(f"✅ Coletados {len(data)} registros")
        return data

    except Exception as e:
        logger.error(f"❌ Erro ao buscar dados do endpoint {endpoint_config['path']}: {str(e)}")
        raise


def process_endpoint(endpoint_name, endpoint_config, app_token, access_token):
    """Processa um endpoint específico e retorna estatísticas."""
    table_name = endpoint_name

    logger.info(f"\n{'=' * 50}\n🔍 PROCESSANDO: {table_name.upper()}\n{'=' * 50}")

    try:
        # Cliente HTTP
        rate_limiter = RateLimiter(requests_per_window=CONFIG["rate_limit"], logger=logger)
        http_client = HttpClient(base_url=CONFIG["base_url"], rate_limiter=rate_limiter, logger=logger)

        start_time = time.time()

        # Buscar dados do endpoint
        all_data = fetch_endpoint_data(http_client, endpoint_config, app_token, access_token)

        # Processar e salvar dados
        logger.info(f"💾 Processando e salvando {len(all_data)} registros para {table_name}")
        processed_data = Utils.process_and_save_data(all_data, table_name)

        return {
            "registros": len(processed_data),
            "status": "Sucesso",
            "tempo": time.time() - start_time,
            "table_name": table_name
        }

    except Exception as e:
        logger.exception(f"❌ Falha no endpoint {table_name}")
        return {
            "registros": 0,
            "status": f"Falha: {type(e).__name__}: {str(e)}",
            "tempo": 0,
            "table_name": table_name
        }


def main():
    """Função principal para coleta de dados."""
    args = get_arguments()

    global_start_time = ReportGenerator.init_report(logger)
    all_endpoint_stats = {}
    all_table_names = []

    logger.info(f"🚀 Iniciando coleta de dados da API Superlógica")

    try:
        # Processar cada endpoint
        for endpoint_name, endpoint_config in CONFIG["endpoints"].items():
            stats = process_endpoint(endpoint_name, endpoint_config, args.API_APP_TOKEN, args.API_ACCESS_TOKEN)

            table_name = stats["table_name"]
            all_endpoint_stats[table_name] = stats
            all_table_names.append(table_name)

            logger.info(
                f"✅ {table_name}: {stats['registros']} registros em {stats['tempo']:.2f}s"
            )

        # Relatório final consolidado
        if not ReportGenerator.final_summary(logger, all_endpoint_stats, global_start_time):
            raise Exception("Falhas encontradas na execução")

        # Processar arquivos CSV
        with MemoryMonitor(logger):
            BigQuery.process_csv_files()

        tables = Utils.get_existing_folders(logger)
        for table in tables:
            BigQuery.start_pipeline(args.PROJECT_ID, args.CRM_TYPE, table_name=table,
                                    credentials_path=args.GOOGLE_APPLICATION_CREDENTIALS)

        logger.info(f"🎉 Integração Superlógica concluída com sucesso! {len(all_table_names)} tabelas processadas.")

    except Exception as e:
        logger.exception(f"❌ ERRO CRÍTICO: {e}")
        raise


if __name__ == "__main__":
    main()
