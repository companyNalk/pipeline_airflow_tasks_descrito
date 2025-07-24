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
    "base_url": "https://api.imobzi.app",
    "page_size": 50,
    "endpoints": {
        "deals": {
            "path": "v1/deals/search",
            "use_pagination": True,
            "data_field": "deals"
        },
        "deals_rotations": {
            "path": "v1/deals-rotations",
            "use_pagination": False,
            "data_field": "rotations"
        },
        "deal_lost_reason": {
            "path": "v1/deal/lost-reason",
            "use_pagination": False,
            "data_field": "deals_lost_reasons"
        },
        "pipelines": {
            "path": "v1/pipelines",
            "use_pagination": False,
            "data_field": None
        },
        "pipeline_groups": {
            "path": "v1/pipeline-groups",
            "use_pagination": False,
            "data_field": None
        },
        "contacts": {
            "path": "v1/contacts",
            "use_pagination": True,
            "data_field": "contacts"
        },
        "contacts_tags": {
            "path": "v1/contacts/tags",
            "use_pagination": False,
            "data_field": "tags"
        },
        "person_fields": {
            "path": "v1/person-fields",
            "use_pagination": False,
            "data_field": "group_contact"
        },
        "organization_fields": {
            "path": "v1/organization-fields",
            "use_pagination": False,
            "data_field": "group_contact"
        }
    }
}


def get_arguments():
    """Configura e retorna os argumentos da linha de comando."""
    return (ArgumentManager("Script para coletar e processar dados da API Imobzi")
            .add("API_SECRET", "Secret para autenticação X-Imobzi-Secret", required=True)
            .add("PROJECT_ID", "ID do projeto Google Cloud", required=True)
            .add("CRM_TYPE", "Nome da ferramenta", required=True)
            .add("GOOGLE_APPLICATION_CREDENTIALS", "Credencial GCS", required=True)
            .parse())


def create_auth_header(api_secret):
    """Cria o header de autenticação Imobzi."""
    return {"X-Imobzi-Secret": api_secret}


def fetch_paginated_data(http_client, endpoint_config, auth_headers):
    """Busca dados paginados usando cursor."""
    all_data = []
    cursor = None
    page_count = 0

    while True:
        params = {}
        if cursor:
            params["cursor"] = cursor

        try:
            page_count += 1
            logger.info(f"📄 Buscando página {page_count} - Cursor: {cursor[:50] if cursor else 'inicial'}...")

            response = http_client.get(endpoint_config["path"], headers=auth_headers, params=params)

            # Extrair dados da resposta
            data_field = endpoint_config.get("data_field")
            page_data = response.get(data_field, []) if data_field else response

            if not page_data:
                logger.info("📄 Não há mais dados para buscar")
                break

            all_data.extend(page_data)
            logger.info(f"✅ Coletados {len(page_data)} registros (Total: {len(all_data)})")

            # Verificar se há próxima página
            cursor = response.get("cursor")
            if not cursor:
                logger.info("📄 Todas as páginas foram coletadas")
                break

            # Pausa entre requisições
            time.sleep(0.1)

        except Exception as e:
            logger.error(f"❌ Erro ao buscar dados na página {page_count}: {str(e)}")
            raise

    return all_data


def fetch_simple_data(http_client, endpoint_config, auth_headers):
    """Busca dados de endpoints sem paginação."""
    try:
        response = http_client.get(endpoint_config["path"], headers=auth_headers)

        # Extrair dados baseado no campo configurado
        data_field = endpoint_config.get("data_field")
        if data_field:
            return response.get(data_field, [])
        else:
            # Resposta direta (lista)
            return response if isinstance(response, list) else []

    except Exception as e:
        logger.error(f"❌ Erro ao buscar dados do endpoint {endpoint_config['path']}: {str(e)}")
        raise


def process_endpoint(endpoint_name, endpoint_config, api_secret):
    """Processa um endpoint específico e retorna estatísticas."""
    table_name = endpoint_name

    logger.info(f"\n{'=' * 50}\n🔍 PROCESSANDO: {table_name.upper()}\n{'=' * 50}")

    try:
        # Cliente HTTP
        rate_limiter = RateLimiter(requests_per_window=CONFIG["rate_limit"], logger=logger)
        http_client = HttpClient(base_url=CONFIG["base_url"], rate_limiter=rate_limiter, logger=logger)

        # Criar headers de autenticação
        auth_headers = create_auth_header(api_secret)

        start_time = time.time()

        if endpoint_config.get("use_pagination"):
            # Endpoints com paginação por cursor
            logger.info(f"📊 Coletando dados paginados do endpoint {endpoint_name}")
            all_data = fetch_paginated_data(http_client, endpoint_config, auth_headers)
        else:
            # Endpoints sem paginação
            logger.info(f"📊 Coletando dados do endpoint {endpoint_name}")
            all_data = fetch_simple_data(http_client, endpoint_config, auth_headers)

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

    logger.info("🚀 Iniciando coleta de dados da API Imobzi")
    logger.info(f"🔗 URL Base: {CONFIG['base_url']}")
    logger.info("🔐 Autenticação: X-Imobzi-Secret Header")

    try:
        # Processar cada endpoint
        for endpoint_name, endpoint_config in CONFIG["endpoints"].items():
            stats = process_endpoint(endpoint_name, endpoint_config, args.API_SECRET)

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

        logger.info(f"🎉 Integração Imobzi concluída com sucesso! {len(all_table_names)} tabelas processadas.")

    except Exception as e:
        logger.exception(f"❌ ERRO CRÍTICO: {e}")
        raise


if __name__ == "__main__":
    main()
