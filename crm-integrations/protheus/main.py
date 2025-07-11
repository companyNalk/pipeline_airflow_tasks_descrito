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
    "default_tenant_ids": ["01,00", "01,01", "01,09"],
    "endpoints": {
        "vendedores_vnd": {
            "path": "WSGETVND",
            "data_key": "VENDEDORES",
            "tenant_ids": ["01,00"]
        },
        "clientes": {
            "path": "WSGETCLI",
            "data_key": "CLIENTES",
            "tenant_ids": ["01,00"]
        },
        "produtos": {
            "path": "WSGETPRD",
            "data_key": "PRODUTOS",
            "tenant_ids": ["01,00"]
        },
        # "pedidos": {
        #     "path": "WSGETPV",
        #     "data_key": "PEDIDOS",
        #     "tenant_ids": ["01,00"]
        # },
        # "vendedores_sd2": {
        #     "path": "WSGETSD2",
        #     "data_key": "DADOS",
        #     "tenant_ids": ["01,01", "01,09"]
        # },
        # "itens_nf": {
        #     "path": "WSGETSFT",
        #     "data_key": "DADOS",
        #     "tenant_ids": ["01,00"]
        # },
    }
}


def get_arguments():
    """Configura e retorna os argumentos da linha de comando."""
    return (ArgumentManager("Script para coletar e processar dados da API REST ERP")
            # .add("API_BASE_URL", "URL base da API REST", default="http://192.169.0.6:8084/rest/")
            .add("API_BASE_URL", "URL base da API REST", default="http://189.1.106.218:8084/rest")
            .add("API_AUTH_TOKEN", "Token de autenticação Basic", required=True)
            .add("PROJECT_ID", "ID do projeto GCS", required=True)
            .add("CRM_TYPE", "Ferramenta: Nome aba sheets", required=True)
            .add("GOOGLE_APPLICATION_CREDENTIALS", "Credenciais GCS", required=True)
            .parse())


def fetch_all_data(http_client, endpoint_path, data_key, headers):
    """Busca todos os dados de um endpoint usando paginação."""
    logger.info(f"📚 Buscando dados para: {endpoint_path}")
    start_time = time.time()
    all_items = []

    # Buscar primeira página
    page_num = 1
    page_size = 100

    while True:
        try:
            params = {"page": page_num, "pagesize": page_size}
            data = http_client.get(endpoint_path, headers=headers, params=params)

            items = data.get(data_key, [])
            all_items.extend(items)

            has_next = data.get('hasNext', False)

            if page_num == 1 or not has_next or page_num % 20 == 0:
                logger.info(
                    f"📄 Endpoint {endpoint_path}: página {page_num} com {len(items)} itens - hasNext: {has_next}")

            if not has_next:
                break

            page_num += 1
            time.sleep(0.1)  # Pausa entre requisições

        except Exception as e:
            logger.error(f"❌ Erro na página {page_num} para {endpoint_path}: {str(e)}")
            raise

    duration = time.time() - start_time
    logger.info(f"✅ Endpoint {endpoint_path}: {page_num} páginas com {len(all_items)} itens obtidos em {duration:.2f}s")
    return all_items


def process_endpoint_with_tenant(endpoint_name, endpoint_config, tenant_id, args):
    """Processa um endpoint específico com TenantId e retorna estatísticas."""
    logger.info(f"\n{'=' * 60}\n🔍 PROCESSANDO: {endpoint_name.upper()} - TenantId: {tenant_id}\n{'=' * 60}")

    try:
        # Configurações
        base_url = args.API_BASE_URL.rstrip('/')
        auth_token = args.API_AUTH_TOKEN

        # Cliente HTTP
        rate_limiter = RateLimiter(requests_per_window=CONFIG["rate_limit"], logger=logger)
        http_client = HttpClient(base_url=base_url, rate_limiter=rate_limiter, logger=logger)

        headers = {
            "Authorization": f"Basic {auth_token}",
            "TenantId": tenant_id
        }

        # Buscar e processar dados
        start_time = time.time()
        raw_data = fetch_all_data(http_client, endpoint_config['path'], endpoint_config['data_key'], headers)

        # Processar dados com sufixo do tenant
        endpoint_name_with_tenant = f"{endpoint_name}_{tenant_id.replace(',', '_')}"
        logger.info(f"💾 Processando e salvando {len(raw_data)} registros para {endpoint_name_with_tenant}")
        processed_data = Utils.process_and_save_data(raw_data, endpoint_name_with_tenant)

        return {
            "registros": len(processed_data),
            "status": "Sucesso",
            "tempo": time.time() - start_time,
            "tenant_id": tenant_id
        }

    except Exception as e:
        logger.exception(f"❌ Falha no endpoint {endpoint_name} com TenantId {tenant_id}")
        return {
            "registros": 0,
            "status": f"Falha: {type(e).__name__}: {str(e)}",
            "tempo": 0,
            "tenant_id": tenant_id
        }


def get_tenant_ids_for_endpoint(endpoint_config):
    """Retorna os TenantIds para um endpoint específico."""
    # Se o endpoint tem tenant_ids definidos, usa eles
    if "tenant_ids" in endpoint_config:
        return endpoint_config["tenant_ids"]

    # Caso contrário, usa os TenantIds padrão
    return CONFIG["default_tenant_ids"]


def process_endpoint(endpoint_name, endpoint_config, args):
    """Processa um endpoint para seus TenantIds específicos."""
    # Obter TenantIds específicos para este endpoint
    tenant_ids = get_tenant_ids_for_endpoint(endpoint_config)

    logger.info(f"\n{'=' * 70}\n🚀 INICIANDO PROCESSAMENTO DO ENDPOINT: {endpoint_name.upper()}")
    logger.info(f"📋 TenantIds configurados: {tenant_ids}\n{'=' * 70}")

    endpoint_stats = {}
    total_records = 0
    total_time = 0

    for tenant_id in tenant_ids:
        tenant_key = f"{endpoint_name}_{tenant_id.replace(',', '_')}"
        endpoint_stats[tenant_key] = process_endpoint_with_tenant(endpoint_name, endpoint_config, tenant_id, args)

        total_records += endpoint_stats[tenant_key]["registros"]
        total_time += endpoint_stats[tenant_key]["tempo"]

        logger.info(
            f"✅ {endpoint_name} (TenantId: {tenant_id}): {endpoint_stats[tenant_key]['registros']} registros em {endpoint_stats[tenant_key]['tempo']:.2f}s")

    # Log resumo do endpoint
    logger.info(f"\n📊 RESUMO {endpoint_name.upper()}: {total_records} registros totais em {total_time:.2f}s")

    return endpoint_stats


def main():
    """Função principal para coleta de dados."""
    args = get_arguments()
    global_start_time = ReportGenerator.init_report(logger)
    all_endpoint_stats = {}

    try:
        # Processar cada endpoint com todos os TenantIds
        for endpoint_name, endpoint_config in CONFIG["endpoints"].items():
            endpoint_stats = process_endpoint(endpoint_name, endpoint_config, args)
            all_endpoint_stats.update(endpoint_stats)

        # Verificar se houve falhas
        if not ReportGenerator.final_summary(logger, all_endpoint_stats, global_start_time):
            raise Exception("Falhas encontradas na execução")

        # Processar arquivos CSV com monitoramento de memória
        with MemoryMonitor(logger):
            BigQuery.process_csv_files()

        # Executar pipeline do BigQuery para cada tabela
        tables = Utils.get_existing_folders(logger)
        for table in tables:
            BigQuery.start_pipeline(args.PROJECT_ID, args.CRM_TYPE, table_name=table,
                                    credentials_path=args.GOOGLE_APPLICATION_CREDENTIALS)

        logger.info("🎉 Integração ERP concluída com sucesso!")

    except Exception as e:
        logger.exception(f"❌ ERRO CRÍTICO: {e}")
        raise


if __name__ == "__main__":
    main()
