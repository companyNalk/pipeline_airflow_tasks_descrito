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
    "base_url": "https://api2.ploomes.com",
    "page_size": 300,
    "endpoints": {
        "account": {
            "path": "Account",
            "use_pagination": True
        },
        "cities": {
            "path": "Cities",
            "use_pagination": True
        },
        "contacts": {
            "path": "Contacts",
            "use_pagination": True
        },
        "contacts_numbersofemployees": {
            "path": "Contacts@NumbersOfEmployees",
            "use_pagination": True
        },
        "contacts_origins": {
            "path": "Contacts@Origins",
            "use_pagination": True
        },
        "contacts_linesofbusiness": {
            "path": "Contacts@LinesOfBusiness",
            "use_pagination": True
        },
        "contacts_relationships": {
            "path": "Contacts@Relationships",
            "use_pagination": True
        },
        "contacts_status": {
            "path": "Contacts@Status",
            "use_pagination": True
        },
        "contacts_types": {
            "path": "Contacts@Types",
            "use_pagination": True
        },
        "contacts_products": {
            "path": "Contacts@Products",
            "use_pagination": True
        },
        "currencies": {
            "path": "Currencies",
            "use_pagination": True
        },
        "deals": {
            "path": "Deals",
            "use_pagination": True
        },
        "deals_stages": {
            "path": "Deals@Stages",
            "use_pagination": True
        },
        "deals_status": {
            "path": "Deals@Status",
            "use_pagination": True
        },
        "deals_lossreasons": {
            "path": "Deals@LossReasons",
            "use_pagination": True
        },
        "deals_pipelines": {
            "path": "Deals@Pipelines",
            "use_pagination": True
        },
        "orders": {
            "path": "Orders",
            "use_pagination": True
        },
        "orders_stages": {
            "path": "Orders@Stages",
            "use_pagination": True
        },
        "products": {
            "path": "Products",
            "use_pagination": True
        },
        "products_families": {
            "path": "Products@Families",
            "use_pagination": True
        },
        "products_groups": {
            "path": "Products@Groups",
            "use_pagination": True
        },
        "products_parts": {
            "path": "Products@Parts",
            "use_pagination": True
        },
        "tags": {
            "path": "Tags",
            "use_pagination": True
        },
        "teams": {
            "path": "Teams",
            "use_pagination": True
        },
        "users": {
            "path": "Users",
            "use_pagination": True
        }
    }
}


def get_arguments():
    """Configura e retorna os argumentos da linha de comando."""
    return (ArgumentManager("Script para coletar e processar dados da API Ploomes")
            .add("API_USER_KEY", "User-Key de autenticação da API Ploomes", required=True)
            .add("PROJECT_ID", "ID do projeto Google Cloud", required=True)
            .add("CRM_TYPE", "Nome da ferramenta", required=True)
            .add("GOOGLE_APPLICATION_CREDENTIALS", "Credencial GCS", required=True)
            .parse())


def fetch_all_data(http_client, endpoint_config, user_key):
    """Busca todos os dados de um endpoint com paginação OData."""
    headers = {"User-Key": user_key}
    all_data = []
    next_url = endpoint_config["path"]
    page_count = 0

    while next_url:
        page_count += 1

        try:
            logger.info(f"📄 Buscando página {page_count} - URL: {next_url}")

            # Para primeira página, usar path diretamente. Para próximas, usar URL completa
            if page_count == 1:
                response = http_client.get(next_url, headers=headers)
            else:
                # Extrair apenas a parte da query da URL completa
                if "?" in next_url:
                    query_part = next_url.split("?", 1)[1]
                    response = http_client.get(f"{endpoint_config['path']}?{query_part}", headers=headers)
                else:
                    response = http_client.get(next_url, headers=headers)

            # Extrair dados da resposta
            page_data = response.get("value", [])
            all_data.extend(page_data)

            logger.info(f"✅ Coletados {len(page_data)} registros da página {page_count}")

            # Verificar se há próxima página
            next_url = response.get("@odata.nextLink")
            if next_url:
                # Se a URL já contém o domínio completo, extrair apenas a parte do path
                if next_url.startswith("https://"):
                    next_url = next_url.split(".com/", 1)[1] if ".com/" in next_url else next_url
                logger.info(f"📄 Próxima página encontrada")
            else:
                logger.info(f"✅ Não há mais páginas disponíveis")

            # Pausa entre requisições para evitar rate limit
            if next_url:
                time.sleep(0.1)

        except Exception as e:
            logger.error(f"❌ Erro ao buscar página {page_count} do endpoint {endpoint_config['path']}: {str(e)}")
            raise

    logger.info(f"🎯 Total coletado: {len(all_data)} registros em {page_count} páginas")
    return all_data


def process_endpoint(endpoint_name, endpoint_config, user_key):
    """Processa um endpoint específico e retorna estatísticas."""
    table_name = endpoint_name

    logger.info(f"\n{'=' * 50}\n🔍 PROCESSANDO: {table_name.upper()}\n{'=' * 50}")

    try:
        # Cliente HTTP
        rate_limiter = RateLimiter(requests_per_window=CONFIG["rate_limit"], logger=logger)
        http_client = HttpClient(base_url=CONFIG["base_url"], rate_limiter=rate_limiter, logger=logger)

        start_time = time.time()

        # Buscar todos os dados do endpoint
        logger.info(f"📊 Coletando dados do endpoint {endpoint_name}")
        all_data = fetch_all_data(http_client, endpoint_config, user_key)

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

    logger.info("🚀 Iniciando coleta de dados da API Ploomes")

    try:
        # Processar cada endpoint
        for endpoint_name, endpoint_config in CONFIG["endpoints"].items():
            stats = process_endpoint(endpoint_name, endpoint_config, args.API_USER_KEY)

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

        # Enviar dados para BigQuery
        tables = Utils.get_existing_folders(logger)
        for table in tables:
            BigQuery.start_pipeline(args.PROJECT_ID, args.CRM_TYPE, table_name=table,
                                    credentials_path=args.GOOGLE_APPLICATION_CREDENTIALS)

        logger.info(f"🎉 Integração Ploomes concluída com sucesso! {len(all_table_names)} tabelas processadas.")

    except Exception as e:
        logger.exception(f"❌ ERRO CRÍTICO: {e}")
        raise


if __name__ == "__main__":
    main()
