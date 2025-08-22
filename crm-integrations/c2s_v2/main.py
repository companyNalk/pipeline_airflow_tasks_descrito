import time

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

CONFIG = {
    "rate_limit": 10,  # Contact2Sale tem limite de 10 requisições por minuto
    "endpoints": {
        "companies": {
            "path": "me",
            "url_key": "API_BASE_URL",
            "token_key": "API_AUTH_TOKEN",
            "paginated": False
        },
        "leads": {
            "path": "leads",
            "url_key": "API_BASE_URL",
            "token_key": "API_AUTH_TOKEN",
            "paginated": True,
            "use_relational": True
        },
        "sellers": {
            "path": "sellers",
            "url_key": "API_BASE_URL",
            "token_key": "API_AUTH_TOKEN",
            "paginated": False
        },
        "tags": {
            "path": "tags",
            "url_key": "API_BASE_URL",
            "token_key": "API_AUTH_TOKEN",
            "paginated": True
        }
    }
}


def get_arguments():
    """Configura e retorna os argumentos da linha de comando."""
    return (ArgumentManager("Script para coletar e processar dados da API Contact2Sale")
            .add("API_BASE_URL", "URL base da API Contact2Sale (ex: https://api.contact2sale.com/integration)",
                 required=True)
            .add("API_AUTH_TOKEN", "Token de autenticação Bearer para Contact2Sale", required=True)
            # .add("PROJECT_ID", "ID do projeto GCP", required=True)
            .add("TOOL_NAME", "Nome da ferramenta", required=True)
            # .add("GOOGLE_APPLICATION_CREDENTIALS", "Credencial GCS", required=True)
            .parse())


def fetch_all_data(http_client, endpoint, token, is_paginated=False):
    """Busca todos os dados de um endpoint."""
    logger.info(f"📚 Buscando dados para: {endpoint}")
    start_time = time.time()

    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    try:
        if not is_paginated:
            # Endpoints não paginados (companies via /me e sellers)
            data = http_client.get(endpoint, headers=headers)

            # Para companies (/me), processar empresa principal e sub-empresas
            if endpoint == "me":
                companies = []
                # Adicionar empresa principal (todos os campos)
                main_company = dict(data)
                main_company["type"] = "main"
                # Remover sub_companies para não duplicar
                main_company.pop("sub_companies", None)
                companies.append(main_company)

                # Adicionar sub-empresas (todos os campos)
                sub_companies = data.get("sub_companies", [])
                for sub in sub_companies:
                    sub_company = dict(sub)
                    sub_company["type"] = "sub"
                    companies.append(sub_company)

                items = companies
            else:
                # Para sellers e outros, dados vem direto (não dentro de data)
                items = data if isinstance(data, list) else [data]

            duration = time.time() - start_time
            logger.info(f"✅ Endpoint {endpoint}: {len(items)} registros obtidos em {duration:.2f}s")
            return items

        else:
            # Endpoints paginados (leads, tags)
            return fetch_paginated_data_internal(http_client, endpoint, headers)

    except Exception as e:
        logger.error(f"❌ Erro no endpoint {endpoint}: {str(e)}")
        raise


def fetch_paginated_data_internal(http_client, endpoint, headers):
    """Busca dados paginados internamente."""
    all_items = []
    page_num = 1
    per_page = 50  # Máximo permitido pela API

    while True:
        try:
            params = {
                "page": page_num,
                "perpage": per_page,
                "sort": "-created_at"
            }

            # Para leads, adicionar filtro de data (junho do ano atual)
            if endpoint == "leads":
                from datetime import datetime
                start_date = datetime(datetime.now().year, 1, 1).strftime("%Y-%m-%dT%H:%M:%SZ")
                params["created_gte"] = start_date

            data = http_client.get(endpoint, headers=headers, params=params)

            # Dados estão dentro de 'data' para endpoints paginados
            items = data.get('data', [])
            all_items.extend(items)

            # Log de progresso
            total = data.get('meta', {}).get('total', 0)
            if page_num == 1:
                logger.info(f"📊 {endpoint}: Total de registros disponíveis: {total}")

            if page_num % 10 == 0:
                logger.info(
                    f"📄 {endpoint}: página {page_num} com {len(items)} itens (total acumulado: {len(all_items)})")

            # Verificar se há mais páginas
            if len(items) < per_page or len(all_items) >= total:
                break

            page_num += 1

            # Rate limiting - Contact2Sale permite 10 req/min
            time.sleep(6)  # 6 segundos entre requisições

        except Exception as e:
            logger.error(f"❌ Erro na página {page_num} para {endpoint}: {str(e)}")
            raise

    start_time = time.time()
    duration = time.time() - start_time
    logger.info(f"✅ Endpoint {endpoint}: {len(all_items)} registros obtidos em {page_num} páginas em {duration:.2f}s")
    return all_items


def fetch_paginated_data(http_client, endpoint, token):
    """Busca todos os dados de endpoints paginados (leads, tags)."""
    logger.info(f"📚 Buscando dados para: {endpoint}")
    start_time = time.time()
    all_items = []

    page_num = 1
    per_page = 50  # Máximo permitido pela API
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    while True:
        try:
            params = {
                "page": page_num,
                "perpage": per_page,
                "sort": "-created_at"
            }

            # Para leads, adicionar filtro de data (janeiro do ano atual)
            if endpoint == "leads":
                from datetime import datetime
                start_date = datetime(datetime.now().year, 1, 1).strftime("%Y-%m-%dT%H:%M:%SZ")
                params["created_gte"] = start_date

            data = http_client.get(endpoint, headers=headers, params=params)

            # Dados estão dentro de 'data' para endpoints paginados
            items = data.get('data', [])
            all_items.extend(items)

            # Log de progresso
            total = data.get('meta', {}).get('total', 0)
            if page_num == 1:
                logger.info(f"📊 {endpoint}: Total de registros disponíveis: {total}")

            if page_num % 10 == 0 or page_num == 1:
                logger.info(
                    f"📄 {endpoint}: página {page_num} com {len(items)} itens (total acumulado: {len(all_items)})")

            # Verificar se há mais páginas
            if len(items) < per_page or len(all_items) >= total:
                break

            page_num += 1

            # Rate limiting - Contact2Sale permite 10 req/min
            time.sleep(6)  # 6 segundos entre requisições

        except Exception as e:
            logger.error(f"❌ Erro na página {page_num} para {endpoint}: {str(e)}")
            raise

    duration = time.time() - start_time
    logger.info(f"✅ Endpoint {endpoint}: {len(all_items)} registros obtidos em {page_num} páginas em {duration:.2f}s")
    return all_items


def process_endpoint(endpoint_name, endpoint_config, args):
    """Processa um endpoint específico e retorna estatísticas."""
    logger.info(f"\n{'=' * 50}\n🔍 PROCESSANDO: {endpoint_name.upper()}\n{'=' * 50}")

    try:
        # Configurações
        base_url = getattr(args, endpoint_config['url_key']).rstrip('/')
        token = getattr(args, endpoint_config['token_key'])

        # Cliente HTTP com rate limiting específico para Contact2Sale
        rate_limiter = RateLimiter(requests_per_window=CONFIG["rate_limit"], window_seconds=60, logger=logger)
        http_client = HttpClient(base_url=base_url, rate_limiter=rate_limiter, logger=logger)

        # Buscar dados baseado no tipo de endpoint
        start_time = time.time()

        is_paginated = endpoint_config.get("paginated", False)
        raw_data = fetch_all_data(http_client, endpoint_config['path'], token, is_paginated)

        # ===== NOVA LÓGICA - VERIFICAR SE DEVE USAR EXTRAÇÃO RELACIONAL =====
        use_relational = endpoint_config.get("use_relational", False)

        if use_relational and endpoint_name == "leads":
            # ===== PROCESSAMENTO COM EXTRAÇÃO RELACIONAL PARA LEADS =====
            logger.info(f"🔄 Processando {len(raw_data)} registros com extração relacional para {endpoint_name}")

            results_auto = AdvancedUtils.process_with_relational_extraction(
                raw_data=raw_data,
                endpoint_name=f"{endpoint_name}_auto",
                auto_detect=True
            )

            # Log detalhado do que foi criado
            logger.info("📊 TABELAS CRIADAS COM EXTRAÇÃO RELACIONAL:")
            total_records = 0
            relational_records = 0

            for table_name, table_data in results_auto.items():
                record_count = len(table_data) if table_data else 0
                total_records += record_count

                if table_name == "main_table":
                    logger.info(f"   📋 {table_name} (principal): {record_count} registros")
                else:
                    logger.info(f"   📋 {table_name} (relacional): {record_count} registros")
                    relational_records += record_count

            # Retornar estatísticas baseadas na tabela principal
            main_table_count = len(results_auto.get("main_table", []))

            return {
                "registros": main_table_count,
                "registros_relacionais": relational_records,
                "tabelas_criadas": len(results_auto),
                "status": "Sucesso (Relacional)",
                "tempo": time.time() - start_time,
                "tables_detail": {table: len(data) for table, data in results_auto.items()}
            }

        else:
            # ===== LÓGICA ORIGINAL PARA OUTROS ENDPOINTS =====
            logger.info(f"💾 Processando e salvando {len(raw_data)} registros para {endpoint_name}")
            final_data = Utils.process_and_save_data(raw_data, endpoint_name)

            return {
                "registros": len(final_data),
                "status": "Sucesso",
                "tempo": time.time() - start_time
            }

    except Exception as e:
        logger.exception(f"❌ Falha no endpoint {endpoint_name}")
        return {
            "registros": 0,
            "status": f"Falha: {type(e).__name__}: {str(e)}",
            "tempo": 0
        }


def main():
    """Função principal para coleta de dados do Contact2Sale."""
    args = get_arguments()
    global_start_time = ReportGenerator.init_report(logger)
    endpoint_stats = {}

    try:
        logger.info("🚀 Iniciando coleta de dados do Contact2Sale")
        logger.info(f"📍 API Base URL: {args.API_BASE_URL}")
        logger.info(f"⚙️  Rate Limit: {CONFIG['rate_limit']} requisições por minuto")

        # Processar endpoints em ordem específica
        endpoint_order = ["companies", "tags", "sellers", "leads"]  # leads por último pois demora mais

        for endpoint_name in endpoint_order:
            if endpoint_name in CONFIG["endpoints"]:
                endpoint_config = CONFIG["endpoints"][endpoint_name]
                endpoint_stats[endpoint_name] = process_endpoint(endpoint_name, endpoint_config, args)

                # ===== LOG MELHORADO PARA MOSTRAR INFORMAÇÕES RELACIONAIS =====
                stats = endpoint_stats[endpoint_name]

                if "registros_relacionais" in stats:
                    # Log para endpoints com extração relacional
                    logger.info(
                        f"✅ {endpoint_name}: {stats['registros']} registros principais + "
                        f"{stats['registros_relacionais']} relacionais em {stats['tabelas_criadas']} tabelas "
                        f"({stats['tempo']:.2f}s)"
                    )

                    # Log detalhado das tabelas criadas
                    if "tables_detail" in stats:
                        logger.info("   📊 Detalhamento das tabelas:")
                        for table_name, count in stats["tables_detail"].items():
                            logger.info(f"      - {table_name}: {count} registros")
                else:
                    # Log normal para endpoints tradicionais
                    logger.info(
                        f"✅ {endpoint_name}: {stats['registros']} registros em {stats['tempo']:.2f}s"
                    )

        # Relatório final
        if not ReportGenerator.final_summary(logger, endpoint_stats, global_start_time):
            raise Exception("Falhas encontradas na execução")

        with MemoryMonitor(logger):
            BigQuery.process_csv_files()

        for endpoint_name in endpoint_order:
            if endpoint_name in CONFIG["endpoints"]:
                BigQuery.start_pipeline(args.PROJECT_ID, args.TOOL_NAME, table_name=endpoint_name,
                                        credentials_path=args.GOOGLE_APPLICATION_CREDENTIALS)

        logger.info("🎉 Processo Contact2Sale finalizado com sucesso!")

    except Exception as e:
        logger.exception(f"❌ ERRO CRÍTICO: {e}")
        raise


if __name__ == "__main__":
    main()
