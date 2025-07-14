import json
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

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
    "rate_limit": 300,
    "base_url_template": "https://{subdomain}.hypnobox.com.br",
    "max_workers": 5,
    "max_page_workers_per_endpoint": 10,
    "endpoints": {
        "products": {
            "path": "api/products",
            "data_key": "Produtos",
            "has_pagination": False
        },
        "clients": {
            "path": "api/clients-v2.json",
            "data_key": "Clientes",
            "has_pagination": True,
            "pagination_key": "Paginacao"
        },
        "visitas": {
            "path": "api/visitas",
            "data_key": "Visitas",
            "has_pagination": True,
            "pagination_key": "Paginacao"
        },
        "propostas": {
            "path": "api/consultaproposta",
            "data_key": "Propostas",
            "has_pagination": True,
            "pagination_key": "Paginacao"
        },
        "tarefas": {
            "path": "api/consultatarefa",
            "data_key": "Tarefas",
            "has_pagination": True,
            "pagination_key": "Paginacao"
        }
    }
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


def get_arguments():
    """Configura e retorna os argumentos da linha de comando."""
    return (ArgumentManager("Script para coletar e processar dados da API Planik")
            .add("API_SUBDOMAIN", "Subdomínio da API (ex: planik)", required=True)
            .add("API_LOGIN", "Login para autenticação", required=True)
            .add("API_PASSWORD", "Senha para autenticação", required=True)
            .add("PROJECT_ID", "ID do projeto GCS", required=True)
            .add("CRM_TYPE", "Ferramenta: Nome aba sheets", required=True)
            .add("GOOGLE_APPLICATION_CREDENTIALS", "Credenciais GCS", required=True)
            .parse())


def get_auth_token(http_client, login, password):
    """Obtém o token de autenticação."""
    thread_safe_log("info", "🔐 Obtendo token de autenticação")

    try:
        params = {
            "login": login,
            "password": password,
            "returnType": "json"
        }

        response = http_client.get("api/auth", params=params)

        if isinstance(response, str):
            response = json.loads(response)

        token = response.get('token')
        if not token:
            raise Exception("Token não encontrado na resposta da autenticação")

        thread_safe_log("info", "✅ Token de autenticação obtido com sucesso")
        return token

    except Exception as e:
        thread_safe_log("error", f"❌ Erro ao obter token: {str(e)}")
        raise


def fetch_single_page(base_url, endpoint_config, token, page_num=None):
    """Busca uma única página criando um cliente HTTP isolado."""
    try:
        # Criar cliente HTTP isolado para cada requisição
        rate_limiter = RateLimiter(requests_per_window=CONFIG["rate_limit"], logger=logger)
        http_client = HttpClient(base_url=base_url, rate_limiter=rate_limiter, logger=logger)

        if page_num is None:
            # Endpoint sem paginação
            params = {"token": token, "returnType": "json"}
        else:
            # Endpoint com paginação
            params = {"token": token, "pagina": page_num}

        data = http_client.get(endpoint_config['path'], params=params)

        if isinstance(data, str):
            data = json.loads(data)

        return data
    except Exception as e:
        raise Exception(f"Erro ao buscar página {page_num}: {str(e)}")


def create_page_batches(total_pages, batch_size=50):
    """Cria lotes de páginas para processamento paralelo otimizado."""
    batches = []
    for i in range(1, total_pages + 1, batch_size):
        end_page = min(i + batch_size - 1, total_pages)
        batches.append((i, end_page))
    return batches


def fetch_page_batch(base_url, endpoint_config, token, start_page, end_page, endpoint_name):
    """Busca um lote de páginas sequencialmente (para evitar sobrecarga da API)."""
    thread_name = threading.current_thread().name
    all_items = []

    try:
        # Criar cliente HTTP para este lote
        rate_limiter = RateLimiter(requests_per_window=CONFIG["rate_limit"], logger=logger)
        http_client = HttpClient(base_url=base_url, rate_limiter=rate_limiter, logger=logger)

        for page_num in range(start_page, end_page + 1):
            params = {"token": token, "pagina": page_num}
            data = http_client.get(endpoint_config['path'], params=params)

            if isinstance(data, str):
                data = json.loads(data)

            items = data.get(endpoint_config['data_key'], [])
            all_items.extend(items)

            # Log a cada 10 páginas ou na última página do lote
            if page_num % 10 == 0 or page_num == end_page:
                thread_safe_log("info",
                                f"📄 [{thread_name}] {endpoint_name}: Lote {start_page}-{end_page}, página {page_num} com {len(items)} itens")

    except Exception as e:
        thread_safe_log("error", f"❌ [{thread_name}] Erro no lote {start_page}-{end_page} de {endpoint_name}: {str(e)}")
        raise

    return all_items


def fetch_all_data_with_parallel_pagination(base_url, endpoint_name, endpoint_config, token):
    """Busca todos os dados com paginação paralela otimizada."""
    thread_name = threading.current_thread().name
    thread_safe_log("info", f"🚀 [{thread_name}] Iniciando coleta PARALELA para: {endpoint_name}")
    start_time = time.time()

    if not endpoint_config.get('has_pagination', True):
        # Endpoint sem paginação
        try:
            data = fetch_single_page(base_url, endpoint_config, token)
            items = data.get(endpoint_config['data_key'], [])
            thread_safe_log("info", f"📄 [{thread_name}] {endpoint_name}: {len(items)} itens obtidos")
            return items
        except Exception as e:
            thread_safe_log("error", f"❌ [{thread_name}] Erro em {endpoint_name}: {str(e)}")
            raise
    else:
        # Endpoint com paginação - buscar primeira página para descobrir total
        try:
            first_page_data = fetch_single_page(base_url, endpoint_config, token, 1)

            # Processar primeira página
            all_items = first_page_data.get(endpoint_config['data_key'], [])

            # Verificar paginação
            pagination_info = first_page_data.get(endpoint_config['pagination_key'], {})
            total_pages = int(pagination_info.get('NumerodePaginas', 1))

            thread_safe_log("info", f"📊 [{thread_name}] {endpoint_name}: {total_pages} páginas detectadas")

            if total_pages > 1:
                # Determinar estratégia baseada no número de páginas
                if total_pages <= 100:
                    # Poucas páginas: processamento paralelo total
                    max_workers = min(CONFIG["max_page_workers_per_endpoint"], total_pages - 1)
                    thread_safe_log("info",
                                    f"🔥 [{thread_name}] {endpoint_name}: Processamento paralelo total com {max_workers} workers")

                    with ThreadPoolExecutor(max_workers=max_workers,
                                            thread_name_prefix=f"{endpoint_name}_Page") as page_executor:
                        page_futures = []
                        for page_num in range(2, total_pages + 1):
                            future = page_executor.submit(fetch_single_page, base_url, endpoint_config, token, page_num)
                            page_futures.append((future, page_num))

                        for future, page_num in page_futures:
                            try:
                                page_data = future.result()
                                items = page_data.get(endpoint_config['data_key'], [])
                                all_items.extend(items)

                                if page_num % 20 == 0 or page_num == total_pages:
                                    thread_safe_log("info",
                                                    f"📄 [{thread_name}] {endpoint_name}: Página {page_num}/{total_pages} com {len(items)} itens")

                            except Exception as e:
                                thread_safe_log("error",
                                                f"❌ [{thread_name}] Erro na página {page_num} de {endpoint_name}: {str(e)}")
                                raise

                else:
                    # Muitas páginas: processamento em lotes
                    batch_size = max(20, total_pages // CONFIG["max_page_workers_per_endpoint"])
                    batches = create_page_batches(total_pages - 1, batch_size)  # -1 porque já processamos a página 1

                    # Ajustar os lotes para começar da página 2
                    adjusted_batches = [(start + 1, end + 1) for start, end in batches]

                    thread_safe_log("info",
                                    f"🔥 [{thread_name}] {endpoint_name}: Processamento em {len(adjusted_batches)} lotes paralelos")

                    with ThreadPoolExecutor(max_workers=CONFIG["max_page_workers_per_endpoint"],
                                            thread_name_prefix=f"{endpoint_name}_batch") as batch_executor:
                        batch_futures = []
                        for start_page, end_page in adjusted_batches:
                            future = batch_executor.submit(fetch_page_batch, base_url, endpoint_config, token,
                                                           start_page, end_page, endpoint_name)
                            batch_futures.append((future, start_page, end_page))

                        for future, start_page, end_page in batch_futures:
                            try:
                                batch_items = future.result()
                                all_items.extend(batch_items)
                                thread_safe_log("info",
                                                f"✅ [{thread_name}] {endpoint_name}: Lote {start_page}-{end_page} concluído com {len(batch_items)} itens")

                            except Exception as e:
                                thread_safe_log("error",
                                                f"❌ [{thread_name}] Erro no lote {start_page}-{end_page} de {endpoint_name}: {str(e)}")
                                raise

            duration = time.time() - start_time
            thread_safe_log("info",
                            f"✅ [{thread_name}] {endpoint_name}: {len(all_items)} itens obtidos em {duration:.2f}s")
            return all_items

        except Exception as e:
            thread_safe_log("error", f"❌ [{thread_name}] Erro em {endpoint_name}: {str(e)}")
            raise


def process_endpoint_worker(endpoint_name, endpoint_config, token, base_url):
    """Worker que processa um endpoint específico com paginação paralela."""
    thread_name = threading.current_thread().name
    thread_safe_log("info", f"🚀 [{thread_name}] INICIANDO PROCESSAMENTO: {endpoint_name.upper()}")

    try:
        # Buscar dados com paginação paralela
        start_time = time.time()
        raw_data = fetch_all_data_with_parallel_pagination(base_url, endpoint_name, endpoint_config, token)

        # Processar e salvar dados
        thread_safe_log("info", f"💾 [{thread_name}] Processando {len(raw_data)} registros de {endpoint_name}")
        processed_data = Utils.process_and_save_data(raw_data, endpoint_name, use_pascal_case_conversion=True)

        result = {
            "endpoint": endpoint_name,
            "registros": len(processed_data),
            "status": "Sucesso",
            "tempo": time.time() - start_time
        }

        thread_safe_log("info",
                        f"✅ [{thread_name}] CONCLUÍDO {endpoint_name}: {result['registros']} registros em {result['tempo']:.2f}s")
        return result

    except Exception as e:
        thread_safe_log("exception", f"❌ [{thread_name}] FALHA em {endpoint_name}")
        return {
            "endpoint": endpoint_name,
            "registros": 0,
            "status": f"Falha: {type(e).__name__}: {str(e)}",
            "tempo": 0
        }


def main():
    """Função principal com processamento verdadeiramente paralelo."""
    args = get_arguments()
    global_start_time = ReportGenerator.init_report(logger)
    endpoint_stats = {}

    try:
        # Construir URL base
        base_url = CONFIG["base_url_template"].format(subdomain=args.API_SUBDOMAIN)

        # Obter token de autenticação
        rate_limiter = RateLimiter(requests_per_window=CONFIG["rate_limit"], logger=logger)
        http_client = HttpClient(base_url=base_url, rate_limiter=rate_limiter, logger=logger)
        token = get_auth_token(http_client, args.API_LOGIN, args.API_PASSWORD)

        # PROCESSAMENTO SIMULTÂNEO DOS 5 ENDPOINTS COM PAGINAÇÃO PARALELA
        logger.info(f"🚀 INICIANDO PROCESSAMENTO SIMULTÂNEO DE {len(CONFIG['endpoints'])} ENDPOINTS")
        logger.info(
            f"📋 Configuração: {CONFIG['max_workers']} endpoints simultâneos, {CONFIG['max_page_workers_per_endpoint']} workers por endpoint")
        logger.info("=" * 80)

        # Criar e executar todas as threads simultaneamente
        with ThreadPoolExecutor(max_workers=CONFIG["max_workers"], thread_name_prefix="endpoint") as executor:
            # Submeter TODAS as tarefas de uma vez
            futures = {
                executor.submit(process_endpoint_worker, endpoint_name, endpoint_config, token, base_url): endpoint_name
                for endpoint_name, endpoint_config in CONFIG["endpoints"].items()
            }

            logger.info(f"🔥 {len(futures)} endpoints sendo processados simultaneamente com paginação paralela...")

            # Aguardar conclusão de todas as tarefas
            for future in as_completed(futures):
                endpoint_name = futures[future]
                try:
                    result = future.result()
                    endpoint_stats[result["endpoint"]] = result
                    logger.info(
                        f"🎯 RESULTADO: {result['endpoint']} -> {result['registros']} registros em {result['tempo']:.2f}s ({result['status']})")
                except Exception as e:
                    logger.error(f"❌ Erro crítico no endpoint {endpoint_name}: {str(e)}")
                    endpoint_stats[endpoint_name] = {
                        "endpoint": endpoint_name,
                        "registros": 0,
                        "status": f"Falha crítica: {str(e)}",
                        "tempo": 0
                    }

        logger.info("=" * 80)
        logger.info("✅ TODOS OS ENDPOINTS PROCESSADOS COM PAGINAÇÃO PARALELA!")

        # Validar e continuar com o pipeline
        if not ReportGenerator.final_summary(logger, endpoint_stats, global_start_time):
            raise Exception("Falhas encontradas na execução")

        with MemoryMonitor(logger):
            BigQuery.process_csv_files()

        tables = Utils.get_existing_folders(logger)
        for table in tables:
            BigQuery.start_pipeline(args.PROJECT_ID, args.CRM_TYPE, table_name=table,
                                    credentials_path=args.GOOGLE_APPLICATION_CREDENTIALS)

    except Exception as e:
        logger.exception(f"❌ ERRO CRÍTICO: {e}")
        raise


if __name__ == "__main__":
    main()
