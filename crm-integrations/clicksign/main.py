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
    "base_url": "https://app.clicksign.com/api/v3",
    "page_size": 20,
    "endpoints": {
        "envelopes": {
            "path": "envelopes",
            "use_pagination": True,
            "data_key": "data",
            "has_nested_endpoints": True,
            "nested_endpoints": [
                {
                    "name": "documents",
                    "path": "documents",
                    "table_suffix": "documents"
                },
                {
                    "name": "signers",
                    "path": "signers",
                    "table_suffix": "signers"
                }
            ]
        }
    }
}


def get_arguments():
    """Configura e retorna os argumentos da linha de comando."""
    return (ArgumentManager("Script para coletar e processar dados da API ClickSign")
            .add("API_AUTH_TOKEN", "Token de autenticação da API", required=True)
            .add("PROJECT_ID", "ID do projeto Google Cloud", required=True)
            .add("CRM_TYPE", "Nome da ferramenta", required=True)
            .add("GOOGLE_APPLICATION_CREDENTIALS", "Credencial GCS", required=True)
            .parse())


def fetch_paginated_data(http_client, endpoint_path, token, params=None):
    """Busca dados paginados de um endpoint."""
    headers = {"Authorization": token}
    all_data = []
    page_number = 1
    has_more_pages = True

    while has_more_pages:
        current_params = {
            "page[size]": CONFIG["page_size"],
            "page[number]": page_number
        }

        if params:
            current_params.update(params)

        try:
            logger.info(f"📄 Buscando página {page_number} de {endpoint_path}")

            response = http_client.get(endpoint_path, headers=headers, params=current_params)

            # Extrair dados da resposta
            page_data = response.get("data", [])
            meta = response.get("meta", {})
            links = response.get("links", {})

            record_count = meta.get("record_count", 0)

            if page_number == 1:
                logger.info(f"📊 Total de registros disponíveis: {record_count}")

            all_data.extend(page_data)
            logger.info(f"✅ Coletados {len(page_data)} registros da página {page_number}")

            # Verificar se há próxima página
            has_more_pages = "next" in links and links["next"] is not None
            page_number += 1

            # Pausa entre requisições para evitar rate limit
            if has_more_pages:
                time.sleep(0.1)

        except Exception as e:
            logger.error(f"❌ Erro ao buscar página {page_number} do endpoint {endpoint_path}: {str(e)}")
            raise

    return all_data


def fetch_nested_endpoint_data(http_client, token, envelope_id, nested_config):
    """Busca dados de endpoints aninhados (documents e signers)."""
    endpoint_path = f"envelopes/{envelope_id}/{nested_config['path']}"

    try:
        logger.info(f"🔍 Buscando {nested_config['name']} para envelope {envelope_id}")

        nested_data = fetch_paginated_data(http_client, endpoint_path, token)

        # Adicionar envelope_id aos dados para referência
        for item in nested_data:
            if "attributes" not in item:
                item["attributes"] = {}
            item["attributes"]["envelope_id"] = envelope_id

        logger.info(f"✅ Coletados {len(nested_data)} {nested_config['name']} para envelope {envelope_id}")

        return nested_data

    except Exception as e:
        logger.error(f"❌ Erro ao buscar {nested_config['name']} do envelope {envelope_id}: {str(e)}")
        return []


def process_envelopes_with_nested_data(token):
    """Processa os envelopes e seus dados aninhados (documents e signers)."""
    logger.info(f"\n{'=' * 50}\n🔍 PROCESSANDO ENVELOPES E DADOS ANINHADOS\n{'=' * 50}")

    all_stats = {}

    try:
        # Cliente HTTP
        rate_limiter = RateLimiter(requests_per_window=CONFIG["rate_limit"], logger=logger)
        http_client = HttpClient(base_url=CONFIG["base_url"], rate_limiter=rate_limiter, logger=logger)

        # Primeiro, buscar todos os envelopes
        logger.info("📊 Buscando lista de envelopes...")
        envelopes_config = CONFIG["endpoints"]["envelopes"]
        envelopes_data = fetch_paginated_data(http_client, envelopes_config["path"], token)

        logger.info(f"✅ Encontrados {len(envelopes_data)} envelopes")

        # Processar e salvar dados dos envelopes principais
        start_time = time.time()
        table_name = "envelopes"
        logger.info(f"💾 Processando e salvando {len(envelopes_data)} envelopes para {table_name}")
        processed_envelopes = Utils.process_and_save_data(envelopes_data, table_name)

        # Estatísticas dos envelopes
        envelope_stats = {
            "registros": len(processed_envelopes),
            "status": "Sucesso",
            "tempo": time.time() - start_time,
            "table_name": table_name
        }
        all_stats[table_name] = envelope_stats
        logger.info(f"✅ {table_name}: {envelope_stats['registros']} registros em {envelope_stats['tempo']:.2f}s")

        # Para cada endpoint aninhado, coletar dados de todos os envelopes
        for nested_config in envelopes_config.get("nested_endpoints", []):
            nested_table_name = f"envelopes_{nested_config['table_suffix']}"
            logger.info(f"\n🔍 Processando {nested_config['name']} de todos os envelopes...")

            start_time = time.time()
            all_nested_data = []
            processed_count = 0

            for envelope in envelopes_data:
                envelope_id = envelope.get("id")
                if not envelope_id:
                    logger.warning(f"⚠️ Envelope sem ID encontrado: {envelope}")
                    continue

                try:
                    nested_data = fetch_nested_endpoint_data(http_client, token, envelope_id, nested_config)
                    all_nested_data.extend(nested_data)
                    processed_count += 1

                    # Log de progresso a cada 50 envelopes processados
                    if processed_count % 50 == 0:
                        logger.info(f"📊 Progresso: {processed_count}/{len(envelopes_data)} envelopes processados")

                except Exception as e:
                    logger.error(f"❌ Falha ao processar {nested_config['name']} do envelope {envelope_id}: {str(e)}")
                    continue

            # Salvar dados aninhados coletados
            if all_nested_data:
                logger.info(
                    f"💾 Processando e salvando {len(all_nested_data)} {nested_config['name']} para {nested_table_name}")
                processed_nested_data = Utils.process_and_save_data(all_nested_data, nested_table_name)

                nested_stats = {
                    "registros": len(processed_nested_data),
                    "status": "Sucesso",
                    "tempo": time.time() - start_time,
                    "table_name": nested_table_name
                }
                all_stats[nested_table_name] = nested_stats
                logger.info(
                    f"✅ {nested_table_name}: {nested_stats['registros']} registros em {nested_stats['tempo']:.2f}s")
            else:
                nested_stats = {
                    "registros": 0,
                    "status": "Nenhum dado encontrado",
                    "tempo": time.time() - start_time,
                    "table_name": nested_table_name
                }
                all_stats[nested_table_name] = nested_stats
                logger.info(f"⚠️ {nested_table_name}: Nenhum dado encontrado")

        return all_stats

    except Exception as e:
        logger.exception(f"❌ Falha geral no processamento de envelopes: {e}")
        raise


def main():
    """Função principal para coleta de dados."""
    args = get_arguments()

    global_start_time = ReportGenerator.init_report(logger)
    all_endpoint_stats = {}
    all_table_names = []

    logger.info("🚀 Iniciando coleta de dados da API ClickSign")

    try:
        # Processar envelopes e seus dados aninhados
        envelopes_stats = process_envelopes_with_nested_data(args.API_AUTH_TOKEN)
        all_endpoint_stats.update(envelopes_stats)
        all_table_names.extend(envelopes_stats.keys())

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

        logger.info(f"🎉 Integração ClickSign concluída com sucesso! {len(all_table_names)} tabelas processadas.")

    except Exception as e:
        logger.exception(f"❌ ERRO CRÍTICO: {e}")
        raise


if __name__ == "__main__":
    main()
