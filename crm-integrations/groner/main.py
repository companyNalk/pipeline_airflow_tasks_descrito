import time
from datetime import datetime

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

CURRENT_DATE = datetime.now().strftime("%Y-%m-%d")

CONFIG = {
    "rate_limit": 100,
    "base_url": "https://icehot.api.groner.app",
    "page_size": 500,
    "endpoints": {
        "usuario_periodo_funil_status": {
            "path": "api/Health/DadosUsoPorPeriodoPorFunilPorStatus",
            "use_pagination": False,
            "use_date_range": True,
            "extra_params": {"etapaId": 44}
        },
        "usuario_periodo": {
            "path": "api/Health/DadosUsoPorPeriodo",
            "use_pagination": False,
            "use_date_range": True
        },
        "usuario_periodo_vendedor": {
            "path": "api/Health/DadosUsoPorPeriodoPorVendedor",
            "use_pagination": False,
            "use_date_range": True
        },
        "etapa_lead": {
            "path": "api/EtapaLead",
            "use_pagination": True,
            "use_date_range": False
        },
        "status_projeto": {
            "path": "api/StatusProjeto",
            "use_pagination": True,
            "use_date_range": False
        },
        "projeto_cards": {
            "path": "api/projeto/cards",
            "use_pagination": True,
            "use_date_range": False,
            "extra_params": {"ordenarPor": "DataAtualizacao_DESC"},
            "use_advanced_processing": True
        },
        "atividades": {
            "path": "api/Atividades",
            "use_pagination": True,
            "use_date_range": False
        },
        "lead_cards": {
            "path": "api/lead/cards",
            "use_pagination": True,
            "use_date_range": False
        },
        "loja": {
            "path": "api/loja",
            "use_pagination": True,
            "use_date_range": False
        },
        "usuario": {
            "path": "api/usuario",
            "use_pagination": True,
            "use_date_range": False,
            "extra_params": {"somenteAtivos": True},
            "use_advanced_processing": True
        }
    }
}


def get_arguments():
    """Configura e retorna os argumentos da linha de comando."""
    return (ArgumentManager("Script para coletar e processar dados da API IceHot")
            .add("API_ACCESS_TOKEN", "Token de autenticação da API", required=True)
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

        # Adicionar parâmetros de data se necessário
        if endpoint_config.get("use_date_range"):
            params.update({
                "dataInicial": "2024-07-01",
                "dataFinal": CURRENT_DATE
            })

        # Adicionar parâmetros extras se existirem
        if endpoint_config.get("extra_params"):
            params.update(endpoint_config["extra_params"])

        try:
            logger.info(f"📄 Buscando página {page_number}")

            response = http_client.get(endpoint_config["path"], headers=headers, params=params)

            # Extrair dados da resposta baseado na estrutura do IceHot
            content = response.get("Content", {})
            page_data = content.get("list", [])
            total_items = content.get("totalItems", 0)
            has_more_pages = content.get("hasNextPage", False)

            if page_number == 1:
                logger.info(f"📊 Total de registros disponíveis: {total_items}")

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


def fetch_simple_data(http_client, endpoint_config, token):
    """Busca dados de endpoints simples (sem paginação)."""
    headers = {"Authorization": f"Bearer {token}"}

    params = {}

    # Adicionar parâmetros de data se necessário
    if endpoint_config.get("use_date_range"):
        params.update({
            "dataInicial": "2024-07-01",
            "dataFinal": CURRENT_DATE
        })

    # Adicionar parâmetros extras se existirem
    if endpoint_config.get("extra_params"):
        params.update(endpoint_config["extra_params"])

    try:
        response = http_client.get(endpoint_config["path"], headers=headers, params=params)

        # Extrair dados da resposta baseado na estrutura do IceHot
        content = response.get("Content", {})

        # Para endpoints sem paginação, os dados podem estar diretamente em Content
        if isinstance(content, list):
            return content
        elif "list" in content:
            return content["list"]
        else:
            return [content] if content else []

    except Exception as e:
        logger.error(f"❌ Erro ao buscar dados do endpoint {endpoint_config['path']}: {str(e)}")
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

        # Verificar se deve usar processamento avançado
        if endpoint_config.get("use_advanced_processing"):
            # Processar dados com extração automática de listas
            logger.info(f"🔍 Detectando e extraindo listas automaticamente para {table_name}")
            processed_results = AdvancedUtils.process_with_relational_extraction(
                raw_data=all_data,
                endpoint_name=table_name,
                parent_id_field="id",
                auto_detect=True
            )

            # Calcular total de registros processados
            total_records = sum(len(data) for data in processed_results.values())

            # Log detalhado dos resultados
            logger.info(f"📋 RESUMO PROCESSAMENTO {table_name.upper()}:")
            for table_key, data in processed_results.items():
                if data:
                    logger.info(f"   📊 {table_key}: {len(data)} registros")

            return {
                "registros": total_records,
                "status": "Sucesso",
                "tempo": time.time() - start_time,
                "table_name": table_name,
                "tabelas_geradas": list(processed_results.keys()),
                "detalhes": {k: len(v) for k, v in processed_results.items()}
            }
        else:
            # Processar e salvar dados de forma tradicional
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

    logger.info("🚀 Iniciando coleta de dados da API IceHot")
    logger.info(f"📅 Período de dados: 2024-07-01 até {CURRENT_DATE}")

    try:
        # Processar cada endpoint
        for endpoint_name, endpoint_config in CONFIG["endpoints"].items():
            stats = process_endpoint(endpoint_name, endpoint_config, args.API_ACCESS_TOKEN)

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

        logger.info(f"🎉 Integração IceHot concluída com sucesso! {len(all_table_names)} tabelas processadas.")

    except Exception as e:
        logger.exception(f"❌ ERRO CRÍTICO: {e}")
        raise


if __name__ == "__main__":
    main()
