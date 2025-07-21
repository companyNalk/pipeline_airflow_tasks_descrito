import base64
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
    "rate_limit": 100,
    "base_url": "https://evo-integracao-api.w12app.com.br",
    "page_size": 50,
    "endpoints": {
        "activities": {
            "path": "api/v1/activities",
            "use_pagination": False
        },
        "partnership": {
            "path": "api/v1/partnership",
            "use_pagination": False
        },
        "prospects": {
            "path": "api/v1/prospects",
            "use_pagination": False
        },
        "service": {
            "path": "api/v1/service",
            "use_pagination": False
        },
        "employees": {
            "path": "api/v2/employees",
            "use_pagination": False
        },
        "members": {
            "path": "api/v2/members",
            "use_pagination": False
        },
        "sales": {
            "path": "api/v2/sales",
            "use_pagination": False
        },
        "membership": {
            "path": "api/v2/membership",
            "use_pagination": True,
            "use_advanced_processing": True
        }
    }
}


def get_arguments():
    """Configura e retorna os argumentos da linha de comando."""
    return (ArgumentManager("Script para coletar e processar dados da API W12 App")
            .add("API_USERNAME", "Username para autenticação Basic Auth", required=True)
            .add("API_PASSWORD", "Password para autenticação Basic Auth", required=True)
            .add("PROJECT_ID", "ID do projeto Google Cloud", required=True)
            .add("CRM_TYPE", "Nome da ferramenta", required=True)
            .add("GOOGLE_APPLICATION_CREDENTIALS", "Credencial GCS", required=True)
            .parse())


def create_basic_auth_header(username, password):
    """Cria o header de autenticação Basic Auth."""
    credentials = f"{username}:{password}"
    encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
    return f"Basic {encoded_credentials}"


def fetch_paginated_membership_data(http_client, endpoint_config, auth_header):
    """Busca dados paginados do endpoint membership."""
    headers = {"Authorization": auth_header}
    all_data = []
    skip = 0
    take = CONFIG["page_size"]
    total_records = None

    while True:
        params = {
            "take": take,
            "skip": skip
        }

        try:
            logger.info(f"📄 Buscando dados - Skip: {skip}, Take: {take}")

            response = http_client.get(endpoint_config["path"], headers=headers, params=params)

            # Extrair dados da resposta baseado na estrutura da W12 App
            page_data = response.get("list", [])
            current_total = response.get("qtde", 0)

            if total_records is None:
                total_records = current_total
                logger.info(f"📊 Total de registros disponíveis: {total_records}")

            if not page_data:
                logger.info("📄 Não há mais dados para buscar")
                break

            all_data.extend(page_data)
            logger.info(f"✅ Coletados {len(page_data)} registros (Total coletado: {len(all_data)})")

            skip += take

            # Verificar se já coletamos todos os dados
            if len(all_data) >= total_records:
                logger.info(f"📄 Todos os {total_records} registros foram coletados")
                break

            # Pausa entre requisições para evitar rate limit
            time.sleep(0.1)

        except Exception as e:
            logger.error(f"❌ Erro ao buscar dados com skip {skip}: {str(e)}")
            raise

    return all_data


def fetch_simple_list_data(http_client, endpoint_config, auth_header):
    """Busca dados de endpoints que retornam lista simples."""
    headers = {"Authorization": auth_header}

    try:
        response = http_client.get(endpoint_config["path"], headers=headers)

        # Para endpoints v1 e v2 (exceto membership), os dados vêm em uma lista direta
        if isinstance(response, list):
            return response
        else:
            # Caso os dados venham encapsulados em algum campo
            return response.get("data", response.get("list", []))

    except Exception as e:
        logger.error(f"❌ Erro ao buscar dados do endpoint {endpoint_config['path']}: {str(e)}")
        raise


def process_endpoint(endpoint_name, endpoint_config, username, password):
    """Processa um endpoint específico e retorna estatísticas."""
    table_name = endpoint_name

    logger.info(f"\n{'=' * 50}\n🔍 PROCESSANDO: {table_name.upper()}\n{'=' * 50}")

    try:
        # Cliente HTTP
        rate_limiter = RateLimiter(requests_per_window=CONFIG["rate_limit"], logger=logger)
        http_client = HttpClient(base_url=CONFIG["base_url"], rate_limiter=rate_limiter, logger=logger)

        # Criar header de autenticação
        auth_header = create_basic_auth_header(username, password)

        start_time = time.time()

        if endpoint_config.get("use_pagination"):
            # Endpoint membership com paginação especial
            logger.info(f"📊 Coletando dados paginados do endpoint {endpoint_name}")
            all_data = fetch_paginated_membership_data(http_client, endpoint_config, auth_header)
        else:
            # Endpoints que retornam lista simples
            logger.info(f"📊 Coletando dados do endpoint {endpoint_name}")
            all_data = fetch_simple_list_data(http_client, endpoint_config, auth_header)

        # Verificar se deve usar processamento avançado
        if endpoint_config.get("use_advanced_processing"):
            # Processar dados com extração automática de listas
            logger.info(f"🔍 Detectando e extraindo listas automaticamente para {table_name}")
            processed_results = AdvancedUtils.process_with_relational_extraction(
                raw_data=all_data,
                endpoint_name=table_name,
                parent_id_field="idMembership",
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

    logger.info("🚀 Iniciando coleta de dados da API W12 App")
    logger.info(f"🔗 URL Base: {CONFIG['base_url']}")
    logger.info("🔐 Autenticação: Basic Auth")

    try:
        # Processar cada endpoint
        for endpoint_name, endpoint_config in CONFIG["endpoints"].items():
            stats = process_endpoint(endpoint_name, endpoint_config, args.API_USERNAME, args.API_PASSWORD)

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

        logger.info(f"🎉 Integração W12 App concluída com sucesso! {len(all_table_names)} tabelas processadas.")

    except Exception as e:
        logger.exception(f"❌ ERRO CRÍTICO: {e}")
        raise


if __name__ == "__main__":
    main()
