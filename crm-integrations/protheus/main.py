import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

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
    "rate_limit": 200,
    "max_workers": 8,
    "default_tenant_ids": ["01,00", "01,01", "01,09"],
    "endpoints": {
        "vendedores_vnd": {
            "path": "WSGETVND",
            "data_key": "VENDEDORES",
            "tenant_ids": ["01,00"],
            "use_relational": False
        },
        "produtos": {
            "path": "WSGETPRD",
            "data_key": "PRODUTOS",
            "tenant_ids": ["01,00"],
            "use_relational": False
        },
        "clientes": {
            "path": "WSGETCLI",
            "data_key": "CLIENTES",
            "tenant_ids": ["01,00"],
            "use_relational": False
        },
        "pedidos": {
            "path": "WSGETPV",
            "data_key": "PEDIDOS",
            "tenant_ids": ["01,00", "01,01", "01,09"],
            "extra_params": {"emissao": "01/07/2024"},
            "use_relational": True,
            "parent_id_field": "C5_NUM"
        },
        "vendedores_sd2": {
            "path": "WSGETSD2",
            "data_key": "DADOS",
            "tenant_ids": ["01,00", "01,01", "01,09"],
            "extra_params": {"emissao": "01/07/2024"},
            "use_relational": False
        },
        "itens_nf": {
            "path": "WSGETSFT",
            "data_key": "DADOS",
            "tenant_ids": ["01,00", "01,01", "01,09"],
            "extra_params": {"emissao": "01/07/2024"},
            "use_relational": False
        },
    }
}

# Mapeamento dos tenant_ids para nomes descritivos
TENANT_NAMES = {
    "01,00": "matriz",
    "01,01": "filial_sp",
    "01,09": "filial_jf"
}

# Lock para logging thread-safe
log_lock = Lock()


def thread_safe_log(level, message):
    """Função para logging thread-safe."""
    with log_lock:
        if level == "info":
            logger.info(message)
        elif level == "error":
            logger.error(message)
        elif level == "exception":
            logger.exception(message)


def get_tenant_descriptive_name(tenant_id):
    """Retorna o nome descritivo do tenant_id."""
    return TENANT_NAMES.get(tenant_id, tenant_id.replace(',', '_'))


def get_arguments():
    """Configura e retorna os argumentos da linha de comando."""
    return (ArgumentManager("Script para coletar e processar dados da API REST ERP")
            .add("API_BASE_URL", "URL base da API REST", default="http://189.1.106.218:8084/rest")
            .add("API_AUTH_TOKEN", "Token de autenticação Basic", required=True)
            .add("PROJECT_ID", "ID do projeto GCS", required=True)
            .add("CRM_TYPE", "Ferramenta: Nome aba sheets", required=True)
            .add("GOOGLE_APPLICATION_CREDENTIALS", "Credenciais GCS", required=True)
            .parse())


def fetch_all_data_parallel(http_client, endpoint_path, data_key, headers, extra_params=None):
    """Busca todos os dados de um endpoint usando paginação com otimizações."""
    thread_name = threading.current_thread().name
    thread_safe_log("info", f"📚 [{thread_name}] Iniciando coleta para: {endpoint_path}")
    start_time = time.time()
    all_items = []

    # Buscar primeira página
    page_num = 1
    page_size = 100

    while True:
        try:
            params = {"page": page_num, "pagesize": page_size}

            # Adicionar parâmetros extras se existirem
            if extra_params:
                params.update(extra_params)
                thread_safe_log("info", f"📋 [{thread_name}] Usando parâmetros extras: {extra_params}")

            data = http_client.get(endpoint_path, headers=headers, params=params)

            items = data.get(data_key, [])
            all_items.extend(items)

            has_next = data.get('hasNext', False)

            if page_num == 1 or not has_next or page_num % 20 == 0:
                thread_safe_log("info",
                                f"📄 [{thread_name}] {endpoint_path}: página {page_num} com {len(items)} itens - hasNext: {has_next}")

            if not has_next:
                break

            page_num += 1
            time.sleep(0.1)  # Pausa entre requisições

        except Exception as e:
            thread_safe_log("error", f"❌ [{thread_name}] Erro na página {page_num} para {endpoint_path}: {str(e)}")
            raise

    duration = time.time() - start_time
    thread_safe_log("info",
                    f"✅ [{thread_name}] {endpoint_path}: {page_num} páginas com {len(all_items)} itens obtidos em {duration:.2f}s")
    return all_items


def process_endpoint_tenant_worker(endpoint_name, endpoint_config, tenant_id, base_url, auth_token):
    """Worker que processa um endpoint específico com um tenant específico."""
    thread_name = threading.current_thread().name
    tenant_name = get_tenant_descriptive_name(tenant_id)
    endpoint_tenant_key = f"{endpoint_name}_{tenant_name}"

    thread_safe_log("info", f"🚀 [{thread_name}] INICIANDO: {endpoint_name.upper()} - {tenant_name.upper()}")

    try:
        # Criar cliente HTTP específico para esta thread
        rate_limiter = RateLimiter(requests_per_window=CONFIG["rate_limit"], logger=logger)
        http_client = HttpClient(base_url=base_url, rate_limiter=rate_limiter, logger=logger)

        headers = {
            "Authorization": f"Basic {auth_token}",
            "TenantId": tenant_id
        }

        # Buscar dados
        start_time = time.time()
        raw_data = fetch_all_data_parallel(
            http_client,
            endpoint_config['path'],
            endpoint_config['data_key'],
            headers,
            endpoint_config.get('extra_params')
        )

        # Verificar se deve usar extração relacional
        use_relational = endpoint_config.get("use_relational", False)

        thread_safe_log("info", f"💾 [{thread_name}] Processando {len(raw_data)} registros para {endpoint_tenant_key}")

        if use_relational:
            # Obter chave de ligação customizada
            parent_id_field = endpoint_config.get("parent_id_field", "id")

            # Detecção automática com chave customizada
            thread_safe_log("info",
                            f"🔍 [{thread_name}] Detecção automática para {endpoint_name} (chave: {parent_id_field})")

            processed_tables = AdvancedUtils.process_with_relational_extraction(
                raw_data=raw_data,
                endpoint_name=endpoint_tenant_key,
                parent_id_field=parent_id_field
            )

            # Calcular total de registros
            total_records = sum(len(data) if isinstance(data, list) else 0 for data in processed_tables.values())

            # Log das tabelas geradas
            table_names = []
            for table_name, data in processed_tables.items():
                records_count = len(data) if isinstance(data, list) else 0
                if records_count > 0:
                    table_names.append(table_name)
                    thread_safe_log("info", f"📊 [{thread_name}] {table_name}: {records_count} registros processados")

            result = {
                "endpoint_tenant": endpoint_tenant_key,
                "endpoint": endpoint_name,
                "tenant_id": tenant_id,
                "tenant_name": tenant_name,
                "registros": total_records,
                "status": f"Sucesso com detecção automática (chave: {parent_id_field})",
                "tempo": time.time() - start_time,
                "tabelas_geradas": table_names
            }

        else:
            # Processamento normal sem extração relacional
            thread_safe_log("info", f"📄 [{thread_name}] Processamento normal para {endpoint_name}")
            processed_data = Utils.process_and_save_data(raw_data, endpoint_tenant_key)

            result = {
                "endpoint_tenant": endpoint_tenant_key,
                "endpoint": endpoint_name,
                "tenant_id": tenant_id,
                "tenant_name": tenant_name,
                "registros": len(processed_data),
                "status": "Sucesso",
                "tempo": time.time() - start_time,
                "tabelas_geradas": [endpoint_tenant_key]
            }

        thread_safe_log("info",
                        f"✅ [{thread_name}] CONCLUÍDO {endpoint_tenant_key}: {result['registros']} registros em {result['tempo']:.2f}s")
        return result

    except Exception as e:
        thread_safe_log("exception", f"❌ [{thread_name}] FALHA em {endpoint_tenant_key}")
        return {
            "endpoint_tenant": endpoint_tenant_key,
            "endpoint": endpoint_name,
            "tenant_id": tenant_id,
            "tenant_name": tenant_name,
            "registros": 0,
            "status": f"Falha: {type(e).__name__}: {str(e)}",
            "tempo": 0,
            "tabelas_geradas": []
        }


def get_tenant_ids_for_endpoint(endpoint_config):
    """Retorna os TenantIds para um endpoint específico."""
    if "tenant_ids" in endpoint_config:
        return endpoint_config["tenant_ids"]
    return CONFIG["default_tenant_ids"]


def create_all_endpoint_tenant_combinations():
    """Cria todas as combinações de endpoint x tenant para processamento paralelo."""
    combinations = []

    for endpoint_name, endpoint_config in CONFIG["endpoints"].items():
        tenant_ids = get_tenant_ids_for_endpoint(endpoint_config)

        for tenant_id in tenant_ids:
            combinations.append({
                "endpoint_name": endpoint_name,
                "endpoint_config": endpoint_config,
                "tenant_id": tenant_id
            })

    return combinations


def main():
    """Função principal com processamento verdadeiramente paralelo."""
    args = get_arguments()
    global_start_time = ReportGenerator.init_report(logger)
    all_endpoint_stats = {}

    try:
        # Preparar configurações
        base_url = args.API_BASE_URL.rstrip('/')
        auth_token = args.API_AUTH_TOKEN

        # Criar todas as combinações endpoint x tenant
        combinations = create_all_endpoint_tenant_combinations()

        logger.info("🚀" * 30)
        logger.info(f"🚀 INICIANDO PROCESSAMENTO COM CHAVES CUSTOMIZADAS - {len(combinations)} COMBINAÇÕES")
        logger.info(f"📋 Configuração: {CONFIG['max_workers']} workers simultâneos")

        # Mostrar o que será processado
        for combo in combinations:
            tenant_name = get_tenant_descriptive_name(combo["tenant_id"])
            endpoint_name = combo["endpoint_name"]
            endpoint_config = combo["endpoint_config"]

            logger.info(f"📌 {endpoint_name} x {tenant_name} ({combo['tenant_id']})")
            if endpoint_config.get("extra_params"):
                logger.info(f"   ↳ Parâmetros extras: {endpoint_config['extra_params']}")
            if endpoint_config.get("use_relational"):
                parent_key = endpoint_config.get("parent_id_field", "id")
                logger.info(f"   ↳ Detecção automática: ATIVADA (chave: {parent_key})")

        logger.info("🚀" * 30)

        # PROCESSAMENTO SIMULTÂNEO TOTAL
        with ThreadPoolExecutor(max_workers=CONFIG["max_workers"], thread_name_prefix="ERP") as executor:
            # Submeter TODAS as combinações simultaneamente
            futures = {
                executor.submit(
                    process_endpoint_tenant_worker,
                    combo["endpoint_name"],
                    combo["endpoint_config"],
                    combo["tenant_id"],
                    base_url,
                    auth_token
                ): combo
                for combo in combinations
            }

            logger.info(f"🔥 {len(futures)} combinações sendo processadas SIMULTANEAMENTE...")

            # Aguardar conclusão de todas as tarefas
            for future in as_completed(futures):
                combo = futures[future]
                try:
                    result = future.result()
                    all_endpoint_stats[result["endpoint_tenant"]] = result

                    status_msg = f"🎯 RESULTADO: {result['endpoint_tenant']} -> {result['registros']} registros em {result['tempo']:.2f}s ({result['status']})"
                    if result.get('tabelas_geradas'):
                        status_msg += f" - Tabelas: {', '.join(result['tabelas_geradas'])}"

                    logger.info(status_msg)

                except Exception as e:
                    endpoint_tenant_key = f"{combo['endpoint_name']}_{get_tenant_descriptive_name(combo['tenant_id'])}"
                    logger.error(f"❌ Erro crítico em {endpoint_tenant_key}: {str(e)}")
                    all_endpoint_stats[endpoint_tenant_key] = {
                        "endpoint_tenant": endpoint_tenant_key,
                        "registros": 0,
                        "status": f"Falha crítica: {str(e)}",
                        "tempo": 0,
                        "tabelas_geradas": []
                    }

        logger.info("🚀" * 30)
        logger.info("✅ TODAS AS COMBINAÇÕES PROCESSADAS COM CHAVES CUSTOMIZADAS!")

        # Mostrar resumo por endpoint
        endpoint_summary = {}
        for stats in all_endpoint_stats.values():
            endpoint = stats.get("endpoint", "unknown")
            if endpoint not in endpoint_summary:
                endpoint_summary[endpoint] = {"registros": 0, "tempo": 0, "tenants": [], "tabelas": set()}

            endpoint_summary[endpoint]["registros"] += stats["registros"]
            endpoint_summary[endpoint]["tempo"] += stats["tempo"]
            if "tenant_name" in stats:
                endpoint_summary[endpoint]["tenants"].append(stats["tenant_name"])
            if stats.get("tabelas_geradas"):
                endpoint_summary[endpoint]["tabelas"].update(stats["tabelas_geradas"])

        logger.info("\n📊 RESUMO POR ENDPOINT:")
        for endpoint, summary in endpoint_summary.items():
            tabelas_info = f", tabelas: {', '.join(summary['tabelas'])}" if summary['tabelas'] else ""
            logger.info(
                f"📈 {endpoint.upper()}: {summary['registros']} registros totais, {len(summary['tenants'])} tenants, {summary['tempo']:.2f}s{tabelas_info}")

        logger.info("🚀" * 30)

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
