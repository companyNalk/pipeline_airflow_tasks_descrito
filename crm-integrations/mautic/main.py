import time

import requests

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
    "base_url": "https://mautic.condosin.com.br",
    "page_size": 100,
    "endpoints": {
        "contacts": {
            "path": "api/contacts",
            "use_pagination": True,
            "data_key": "contacts",
            "total_key": "total"
        },
        "segments": {
            "path": "api/segments",
            "use_pagination": False,
            "data_key": "lists",
            "total_key": "total"
        },
        "campaigns": {
            "path": "api/campaigns",
            "use_pagination": False,
            "data_key": "campaigns",
            "total_key": "total"
        }
    }
}


def get_arguments():
    """Configura e retorna os argumentos da linha de comando."""
    return (ArgumentManager("Script para coletar e processar dados da API Mautic")
            .add("CLIENT_ID", "Client ID para autenticação OAuth2", required=True)
            .add("CLIENT_SECRET", "Client Secret para autenticação OAuth2", required=True)
            .add("PROJECT_ID", "ID do projeto Google Cloud", required=True)
            .add("CRM_TYPE", "Nome da ferramenta", required=True)
            .add("GOOGLE_APPLICATION_CREDENTIALS", "Credencial GCS", required=True)
            .parse())


def get_oauth_token(client_id, client_secret):
    """Obtém token OAuth2 do Mautic."""
    logger.info("🔐 Obtendo token OAuth2...")

    url = f"{CONFIG['base_url']}/oauth/v2/token"

    data = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials"
    }

    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }

    try:
        response = requests.post(url, data=data, headers=headers)
        response.raise_for_status()

        token_data = response.json()
        access_token = token_data.get("access_token")

        if not access_token:
            raise Exception("Token não encontrado na resposta")

        logger.info("✅ Token OAuth2 obtido com sucesso")
        return access_token

    except Exception as e:
        logger.error(f"❌ Erro ao obter token OAuth2: {str(e)}")
        raise


def fetch_paginated_data(http_client, endpoint_config, token):
    """Busca dados paginados de um endpoint (específico para contacts)."""
    headers = {"Authorization": f"Bearer {token}"}
    all_data = []
    start = 0
    has_more_data = True

    while has_more_data:
        params = {
            "start": start,
            "limit": CONFIG["page_size"],
            "orderBy": "id"
        }

        try:
            logger.info(f"📄 Buscando dados a partir do registro {start}")

            response = http_client.get(endpoint_config["path"], headers=headers, params=params)

            # Extrair dados da resposta
            data_dict = response.get(endpoint_config["data_key"], {})
            total_items = int(response.get(endpoint_config["total_key"], 0))

            if start == 0:
                logger.info(f"📊 Total de registros disponíveis: {total_items}")

            # Converter dict para lista de objetos
            page_data = []
            for item_id, item_data in data_dict.items():
                if isinstance(item_data, dict):
                    # Garantir que o ID está presente no objeto
                    item_data["id"] = item_id
                    page_data.append(item_data)

            all_data.extend(page_data)
            logger.info(f"✅ Coletados {len(page_data)} registros (início: {start})")

            # Verificar se há mais dados
            start += CONFIG["page_size"]
            has_more_data = len(page_data) == CONFIG["page_size"] and start < total_items

            # Pausa entre requisições para evitar rate limit
            if has_more_data:
                time.sleep(0.1)

        except Exception as e:
            logger.error(f"❌ Erro ao buscar dados do endpoint {endpoint_config['path']} (start: {start}): {str(e)}")
            raise

    return all_data


def fetch_simple_data(http_client, endpoint_config, token):
    """Busca dados de endpoints simples (sem paginação)."""
    headers = {"Authorization": f"Bearer {token}"}

    try:
        logger.info(f"📄 Buscando dados do endpoint {endpoint_config['path']}")

        response = http_client.get(endpoint_config["path"], headers=headers)

        # Extrair dados da resposta
        data_dict = response.get(endpoint_config["data_key"], {})
        total_items = int(response.get(endpoint_config["total_key"], 0))

        logger.info(f"📊 Total de registros disponíveis: {total_items}")

        # Converter dict para lista de objetos
        all_data = []
        for item_id, item_data in data_dict.items():
            if isinstance(item_data, dict):
                # Garantir que o ID está presente no objeto
                item_data["id"] = item_id
                all_data.append(item_data)

        logger.info(f"✅ Coletados {len(all_data)} registros")
        return all_data

    except Exception as e:
        logger.error(f"❌ Erro ao buscar dados do endpoint {endpoint_config['path']}: {str(e)}")
        raise


def process_endpoint(endpoint_name, endpoint_config, token):
    """Processa um endpoint específico com extração automática de listas e retorna estatísticas."""
    table_name = endpoint_name

    logger.info(f"\n{'=' * 50}\n🔍 PROCESSANDO: {table_name.upper()}\n{'=' * 50}")

    try:
        # Cliente HTTP
        rate_limiter = RateLimiter(requests_per_window=CONFIG["rate_limit"], logger=logger)
        http_client = HttpClient(base_url=CONFIG["base_url"], rate_limiter=rate_limiter, logger=logger)

        start_time = time.time()

        if endpoint_config.get("use_pagination"):
            # Endpoints com paginação (contacts)
            logger.info(f"📊 Coletando dados paginados do endpoint {endpoint_name}")
            all_data = fetch_paginated_data(http_client, endpoint_config, token)
        else:
            # Endpoints simples (segments, campaigns)
            logger.info(f"📊 Coletando dados do endpoint {endpoint_name}")
            all_data = fetch_simple_data(http_client, endpoint_config, token)

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

    except Exception as e:
        logger.exception(f"❌ Falha no endpoint {table_name}")
        return {
            "registros": 0,
            "status": f"Falha: {type(e).__name__}: {str(e)}",
            "tempo": 0,
            "table_name": table_name,
            "tabelas_geradas": [],
            "detalhes": {}
        }


def main():
    """Função principal para coleta de dados."""
    args = get_arguments()

    global_start_time = ReportGenerator.init_report(logger)
    all_endpoint_stats = {}
    all_table_names = []

    logger.info("🚀 Iniciando coleta de dados da API Mautic")

    try:
        # Obter token OAuth2
        token = get_oauth_token(args.CLIENT_ID, args.CLIENT_SECRET)

        # Processar cada endpoint
        for endpoint_name, endpoint_config in CONFIG["endpoints"].items():
            stats = process_endpoint(endpoint_name, endpoint_config, token)

            table_name = stats["table_name"]
            all_endpoint_stats[table_name] = stats
            all_table_names.append(table_name)

            # Log detalhado das tabelas geradas
            if stats.get("tabelas_geradas"):
                logger.info(f"✅ {table_name}: {stats['registros']} registros totais em {stats['tempo']:.2f}s")
                for tabela_nome, qtd_registros in stats.get("detalhes", {}).items():
                    logger.info(f"   📊 {tabela_nome}: {qtd_registros} registros")
                    # Adicionar cada tabela filha à lista de processamento
                    if tabela_nome != "main_table":
                        all_table_names.append(f"{table_name}_{tabela_nome}")
            else:
                logger.info(f"✅ {table_name}: {stats['registros']} registros em {stats['tempo']:.2f}s")

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

        logger.info(f"🎉 Integração Mautic concluída com sucesso! {len(all_table_names)} tabelas processadas.")

    except Exception as e:
        logger.exception(f"❌ ERRO CRÍTICO: {e}")
        raise


if __name__ == "__main__":
    main()
