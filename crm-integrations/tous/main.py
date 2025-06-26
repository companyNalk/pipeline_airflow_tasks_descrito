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
    "base_url": "https://api.wts.chat",
    "page_size": 100,
    "endpoints": {
        "portfolio": {
            "path": "core/v1/portfolio",
            "use_pagination": True,
            "data_key": "items"
        },
        "contact": {
            "path": "core/v1/contact",
            "use_pagination": True,
            "data_key": "items"
        },
        "panel": {
            "path": "crm/v1/panel",
            "use_pagination": True,
            "data_key": "items"
        },
        "department": {
            "path": "core/v1/department",
            "use_pagination": False,
            "data_key": None
        },
        "tag": {
            "path": "core/v1/tag",
            "use_pagination": False,
            "data_key": None
        },
        "agent": {
            "path": "core/v1/agent",
            "use_pagination": False,
            "data_key": None
        }
    }
}


def get_arguments():
    """Configura e retorna os argumentos da linha de comando."""
    return (ArgumentManager("Script para coletar e processar dados da API WTS")
            .add("API_AUTH_TOKEN", "Token de autenticação da API", required=True)
            .add("PROJECT_ID", "ID do projeto Google Cloud", required=True)
            .add("CRM_TYPE", "Nome da ferramenta", required=True)
            .add("GOOGLE_APPLICATION_CREDENTIALS", "Credencial GCS", required=True)
            .parse())


def fetch_paginated_data(http_client, endpoint_config, token):
    """Busca dados paginados de um endpoint."""
    headers = {"Authorization": f"Bearer {token}"}
    all_data = []
    page_number = 1
    has_more_pages = True

    while has_more_pages:
        params = {
            "pageSize": CONFIG["page_size"],
            "pageNumber": page_number
        }

        try:
            logger.info(f"📄 Buscando página {page_number}")

            response = http_client.get(endpoint_config["path"], headers=headers, params=params)

            # Extrair dados da resposta
            if endpoint_config["data_key"]:
                page_data = response.get(endpoint_config["data_key"], [])
                has_more_pages = response.get("hasMorePages", False)
                total_items = response.get("totalItems", 0)

                if page_number == 1:
                    logger.info(f"📊 Total de registros disponíveis: {total_items}")
            else:
                # Para endpoints simples sem paginação
                page_data = response if isinstance(response, list) else []
                has_more_pages = False

            all_data.extend(page_data)
            logger.info(f"✅ Coletados {len(page_data)} registros da página {page_number}")

            page_number += 1

            # Pausa entre requisições para evitar rate limit
            if has_more_pages:
                time.sleep(0.1)

        except Exception as e:
            logger.error(f"❌ Erro ao buscar página {page_number} do endpoint {endpoint_config['path']}: {str(e)}")
            raise

    return all_data


def fetch_panel_cards_data(http_client, token, panel_id):
    """Busca dados paginados dos cards de um painel específico."""
    headers = {"Authorization": f"Bearer {token}"}
    all_data = []
    page_number = 1
    has_more_pages = True

    endpoint_path = f"crm/v1/panel/card"

    while has_more_pages:
        params = {
            "PanelId": panel_id,
            "pageNumber": page_number
        }

        try:
            logger.info(f"📄 Buscando cards do painel {panel_id} - página {page_number}")

            response = http_client.get(endpoint_path, headers=headers, params=params)

            # Extrair dados da resposta
            page_data = response.get("items", [])
            has_more_pages = response.get("hasMorePages", False)
            total_items = response.get("totalItems", 0)

            if page_number == 1:
                logger.info(f"📊 Total de cards disponíveis para painel {panel_id}: {total_items}")

            all_data.extend(page_data)
            logger.info(f"✅ Coletados {len(page_data)} cards da página {page_number} do painel {panel_id}")

            page_number += 1

            # Pausa entre requisições para evitar rate limit
            if has_more_pages:
                time.sleep(0.1)

        except Exception as e:
            logger.error(f"❌ Erro ao buscar página {page_number} dos cards do painel {panel_id}: {str(e)}")
            raise

    return all_data


def fetch_simple_data(http_client, endpoint_config, token):
    """Busca dados de endpoints simples (sem paginação)."""
    headers = {"Authorization": f"Bearer {token}"}

    try:
        data = http_client.get(endpoint_config["path"], headers=headers)

        if isinstance(data, list):
            return data
        elif isinstance(data, dict):
            return data.get('items', [data])
        else:
            return []

    except Exception as e:
        logger.error(f"❌ Erro ao buscar dados do endpoint {endpoint_config['path']}: {str(e)}")
        raise


def process_panel_cards(token):
    """Processa os painéis e seus respectivos cards."""
    logger.info(f"\n{'=' * 50}\n🔍 PROCESSANDO PANEL CARDS\n{'=' * 50}")

    all_stats = {}

    try:
        # Cliente HTTP
        rate_limiter = RateLimiter(requests_per_window=CONFIG["rate_limit"], logger=logger)
        http_client = HttpClient(base_url=CONFIG["base_url"], rate_limiter=rate_limiter, logger=logger)

        # Primeiro, buscar todos os painéis
        logger.info("📊 Buscando lista de painéis...")
        panel_config = CONFIG["endpoints"]["panel"]
        panels_data = fetch_paginated_data(http_client, panel_config, token)

        logger.info(f"✅ Encontrados {len(panels_data)} painéis")

        # Para cada painel, buscar seus cards
        for panel in panels_data:
            panel_id = panel.get("id")
            panel_name = panel.get("name", f"panel_{panel_id}")

            if not panel_id:
                logger.warning(f"⚠️ Painel sem ID encontrado: {panel}")
                continue

            start_time = time.time()

            try:
                logger.info(f"🔍 Processando cards do painel: {panel_name} (ID: {panel_id})")

                # Buscar cards do painel
                cards_data = fetch_panel_cards_data(http_client, token, panel_id)

                # Processar e salvar dados com nome específico do painel
                panel_id_short = str(panel_id)[:8]
                table_name = f"panel_card_{panel_id_short}"
                logger.info(f"💾 Processando e salvando {len(cards_data)} cards para {table_name}")
                processed_data = Utils.process_and_save_data(cards_data, table_name)

                # Estatísticas
                stats = {
                    "registros": len(processed_data),
                    "status": "Sucesso",
                    "tempo": time.time() - start_time,
                    "table_name": table_name,
                    "panel_id": panel_id,
                    "panel_name": panel_name
                }

                all_stats[table_name] = stats
                logger.info(f"✅ {table_name}: {stats['registros']} registros em {stats['tempo']:.2f}s")

            except Exception as e:
                logger.exception(f"❌ Falha ao processar cards do painel {panel_id}")
                stats = {
                    "registros": 0,
                    "status": f"Falha: {type(e).__name__}: {str(e)}",
                    "tempo": time.time() - start_time,
                    "table_name": f"panel_card_{panel_id}",
                    "panel_id": panel_id,
                    "panel_name": panel_name
                }
                all_stats[f"panel_card_{panel_id}"] = stats

        return all_stats

    except Exception as e:
        logger.exception(f"❌ Falha geral no processamento de panel cards")
        raise


def process_endpoint(endpoint_name, endpoint_config, token):
    """Processa um endpoint específico e retorna estatísticas."""
    table_name = endpoint_name

    logger.info(f"\n{'=' * 50}\n🔍 PROCESSANDO: {table_name.upper()}\n{'=' * 50}")

    try:
        # Cliente HTTP
        rate_limiter = RateLimiter(requests_per_window=CONFIG["rate_limit"], logger=logger)
        http_client = HttpClient(base_url=CONFIG["base_url"], rate_limiter=rate_limiter, logger=logger)

        start_time = time.time()

        if endpoint_config.get("use_pagination"):
            # Endpoints com paginação
            logger.info(f"📊 Coletando dados paginados do endpoint {endpoint_name}")
            all_data = fetch_paginated_data(http_client, endpoint_config, token)
        else:
            # Endpoints simples
            logger.info(f"📊 Coletando dados do endpoint {endpoint_name}")
            all_data = fetch_simple_data(http_client, endpoint_config, token)

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

    logger.info(f"🚀 Iniciando coleta de dados da API WTS")

    try:
        # Processar cada endpoint regular
        for endpoint_name, endpoint_config in CONFIG["endpoints"].items():
            stats = process_endpoint(endpoint_name, endpoint_config, args.API_AUTH_TOKEN)

            table_name = stats["table_name"]
            all_endpoint_stats[table_name] = stats
            all_table_names.append(table_name)

            logger.info(
                f"✅ {table_name}: {stats['registros']} registros em {stats['tempo']:.2f}s"
            )

        # Processar panel cards
        panel_cards_stats = process_panel_cards(args.API_AUTH_TOKEN)
        all_endpoint_stats.update(panel_cards_stats)
        all_table_names.extend(panel_cards_stats.keys())

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

        logger.info(f"🎉 Integração WTS concluída com sucesso! {len(all_table_names)} tabelas processadas.")

    except Exception as e:
        logger.exception(f"❌ ERRO CRÍTICO: {e}")
        raise


if __name__ == "__main__":
    main()
