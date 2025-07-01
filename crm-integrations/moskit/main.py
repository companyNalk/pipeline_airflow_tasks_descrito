import concurrent.futures
import os
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

# Configurações padrão
MAX_IDS = 100
RATE_LIMIT = 100
MAX_WORKERS = min(10, os.cpu_count() or 5)
QUANTITY = 50
MAX_RETRIES = 5
TIMEOUT = 10

# Endpoints da API
ENDPOINTS = {
    "deals": "v2/deals",
    "custom_fields": "v2/customFields",
    "stages": "v2/stages",
    "pipelines": "v2/pipelines",
    "lost_reasons": "v2/lostReasons",
    "users": "v2/users",
}

DEPENDENT_ENDPOINTS = {
    "deal_custom_fields": {
        "endpoint": "v2/deals/{id}/customFields",
        "parent": "deals",
        "id_field": "id"
    }
}


def get_arguments():
    """Configura e retorna os argumentos da linha de comando."""
    return (ArgumentManager("Script para coletar e processar dados da API Moskit")
            .add("API_BASE_URL", "URL base", default="https://api.ms.prod.moskit.services")
            .add("API_KEY", "API key para autenticação", required=True)
            .add("PROJECT_ID", "Token de autenticação para imóveis", required=True)
            .add("CRM_TYPE", "Token de autenticação para imóveis", required=True)
            .add("GOOGLE_APPLICATION_CREDENTIALS", "Credencial GCS", required=True)
            .parse())


def get_auth_headers(api_key):
    """Prepara os cabeçalhos de autenticação para a API Moskit."""
    logger.info("🔑 Preparando headers de autenticação")
    return {
        "Accept": "application/json",
        "apikey": api_key
    }


def get_response_headers(http_client, endpoint, headers, params=None):
    """Faz requisição e captura headers usando requests diretamente."""
    import requests

    # Construir URL completa
    url = f"{http_client.base_url}/{endpoint}"

    # Aplicar rate limiting se disponível
    if http_client.rate_limiter:
        http_client.rate_limiter.check()

    # Fazer requisição direta
    response = requests.get(url, headers=headers, params=params, timeout=http_client.default_timeout)
    response.raise_for_status()

    return response.json(), dict(response.headers)


def fetch_page(http_client, endpoint, headers, next_page_token=None, params=None):
    """Busca uma página específica usando token ou offset."""
    try:
        # Configurar parâmetros base
        req_params = {"quantity": QUANTITY}
        if params:
            req_params.update(params)

        # Usar token se disponível, senão usar offset
        if next_page_token:
            req_params["nextPageToken"] = next_page_token
        else:
            req_params["offset"] = 0

        # Buscar dados com headers
        data, headers_dict = get_response_headers(http_client, endpoint, headers, req_params)

        # Extrair itens da resposta
        if isinstance(data, list):
            items = data
        else:
            items = data.get('data', [])

        # Extrair informações de paginação dos headers
        next_token = headers_dict.get("X-Moskit-Listing-Next-Page-Token")
        total_count = headers_dict.get("X-Moskit-Listing-Total")

        # Converter total para int se disponível
        try:
            total_count = int(total_count) if total_count else None
        except Exception:
            total_count = None

        has_more = bool(next_token)

        return {
            'items': items,
            'has_more': has_more,
            'next_page_token': next_token,
            'total_count': total_count
        }

    except Exception as e:
        logger.error(f"❌ Erro ao buscar página de {endpoint}: {str(e)}")
        raise


def fetch_all_pages(http_client, endpoint, headers, params=None):
    """Busca todas as páginas de um endpoint."""
    logger.info(f"📚 Coletando dados de: {endpoint}")
    start_time = time.time()

    all_items = []
    next_token = None
    page_count = 0
    last_log_time = start_time

    while True:
        try:
            page_count += 1

            # Buscar página atual
            page_result = fetch_page(http_client, endpoint, headers, next_token, params)
            items = page_result['items']

            # Se não há itens, parar
            if not items:
                logger.info(f"📄 Página {page_count} vazia, finalizando coleta")
                break

            all_items.extend(items)

            # Log de progresso inicial
            if page_count == 1:
                total = page_result.get('total_count')
                if total:
                    logger.info(f"📊 {endpoint}: {total:,} registros disponíveis")

            # Log de progresso periódico
            current_time = time.time()
            if page_count % 20 == 0 or (current_time - last_log_time) > 30:
                logger.info(f"📄 {endpoint}: {len(all_items):,} itens coletados em {page_count} páginas")
                last_log_time = current_time

            # Verificar se há mais páginas
            if not page_result['has_more']:
                break

            next_token = page_result['next_page_token']
            if not next_token:
                break

            # Pausa entre requisições
            time.sleep(0.1)

        except Exception as e:
            logger.error(f"❌ Erro na página {page_count} de {endpoint}: {str(e)}")
            # Tentar algumas vezes antes de desistir
            if page_count == 1:
                raise  # Se falha na primeira página, é erro crítico
            time.sleep(2)
            continue

    duration = time.time() - start_time
    logger.info(f"✅ {endpoint}: {len(all_items):,} registros em {page_count} páginas ({duration:.1f}s)")

    return all_items


def fetch_data(http_client, endpoint, headers, offset=0, params=None, debug_info=None):
    """Busca dados de um endpoint específico (compatibilidade)."""
    params = params or {}
    default_params = {"quantity": QUANTITY}

    if isinstance(offset, int):
        default_params["offset"] = offset
    elif isinstance(offset, str) and offset.startswith("token:"):
        default_params["nextPageToken"] = offset.replace("token:", "")

    default_params.update(params)
    req_debug = debug_info or f"{endpoint}:o{offset}"

    return http_client.get(endpoint, headers=headers, params=default_params, debug_info=req_debug)


def fetch_with_id(http_client, template, id_value, headers, params=None, no_pagination=False):
    """Busca dados em endpoint com ID específico."""
    endpoint = template.replace("{id}", str(id_value))
    debug_info = f"id:{id_value}"

    try:
        if no_pagination:
            # Endpoint sem paginação
            result = fetch_data(http_client, endpoint, headers, debug_info=debug_info, params=params)

            if isinstance(result, dict) and result:
                if "object" in result and result["object"] != "list":
                    result["parent_id"] = id_value
                    return [result]
                elif "data" in result and isinstance(result["data"], list):
                    for item in result["data"]:
                        item["parent_id"] = id_value
                    return result["data"]
                else:
                    result["parent_id"] = id_value
                    return [result]
            elif isinstance(result, list):
                for item in result:
                    item["parent_id"] = id_value
                return result
            return []
        else:
            # Endpoint com paginação
            items = fetch_all_pages(http_client, endpoint, headers, params)
            for item in items:
                item["parent_id"] = id_value
            return items

    except Exception as e:
        logger.error(f"❌ Erro ao buscar {endpoint} para ID {id_value}: {str(e)}")
        return []


def process_dependent_endpoints(http_client, parent_data, config, headers):
    """Processa endpoints dependentes de IDs."""
    endpoint_name = config.get("name", "")
    endpoint_template = config.get("endpoint", "")
    id_field = config.get("id_field", "id")
    no_pagination = config.get("no_pagination", False)
    params = config.get("params", {})

    logger.info(f"🔗 Processando endpoint dependente: {endpoint_name}")

    if not parent_data:
        logger.warning(f"⚠️ Sem dados parent para {endpoint_name}")
        return []

    # Limitar processamento
    limited_data = parent_data[:MAX_IDS]
    if len(parent_data) > MAX_IDS:
        logger.warning(f"⚠️ Limitando para {MAX_IDS} de {len(parent_data)} itens")

    all_items = []
    batch_size = 5

    for i in range(0, len(limited_data), batch_size):
        batch = limited_data[i:i + batch_size]

        def process_item(item):
            id_value = item.get(id_field)
            if not id_value:
                return []
            return fetch_with_id(http_client, endpoint_template, str(id_value), headers, params, no_pagination)

        # Processar lote
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(batch)) as executor:
            batch_results = list(executor.map(process_item, batch))

        # Coletar resultados
        for result in batch_results:
            all_items.extend(result)

        # Pausa entre lotes
        time.sleep(0.5)

    logger.info(f"✅ {endpoint_name}: {len(all_items)} registros coletados")
    return all_items


def process_primary_endpoint(http_client, endpoint_name, endpoint_path, headers, params=None):
    """Processa um endpoint principal."""
    try:
        logger.info(f"\n{'=' * 20} {endpoint_name.upper()} {'=' * 20}")

        start_time = time.time()
        raw_data = fetch_all_pages(http_client, endpoint_path, headers, params)

        logger.info(f"💾 Salvando {len(raw_data)} registros de {endpoint_name}")
        processed_data = Utils.process_and_save_data(raw_data, endpoint_name)

        duration = time.time() - start_time
        stats = {
            "registros": len(processed_data),
            "status": "Sucesso",
            "tempo": duration
        }

        return processed_data, stats

    except Exception as e:
        logger.exception(f"❌ Falha em {endpoint_name}")
        return [], {
            "registros": 0,
            "status": f"Falha: {str(e)}",
            "tempo": 0
        }


def process_dependent_endpoint(http_client, endpoint_name, config, parent_data, headers):
    """Processa um endpoint dependente."""
    parent_name = config["parent"]

    if not parent_data:
        return {
            "registros": 0,
            "status": f"Sem dados de {parent_name}",
            "tempo": 0
        }

    try:
        full_config = {"name": endpoint_name, **config}
        start_time = time.time()

        dependent_data = process_dependent_endpoints(http_client, parent_data, full_config, headers)

        logger.info(f"💾 Salvando {len(dependent_data)} registros de {endpoint_name}")
        Utils.process_and_save_data(dependent_data, endpoint_name)

        duration = time.time() - start_time
        return {
            "registros": len(dependent_data),
            "status": "Sucesso",
            "tempo": duration
        }

    except Exception as e:
        logger.exception(f"❌ Falha em {endpoint_name}")
        return {
            "registros": 0,
            "status": f"Falha: {str(e)}",
            "tempo": 0
        }


def main():
    """Função principal."""
    global_start_time = ReportGenerator.init_report(logger)

    try:
        # Configurações
        args = get_arguments()
        api_base_url = args.API_BASE_URL.rstrip('/')
        api_key = args.API_KEY

        # Cliente HTTP
        rate_limiter = RateLimiter(requests_per_window=RATE_LIMIT, logger=logger)
        http_client = HttpClient(base_url=api_base_url, rate_limiter=rate_limiter, logger=logger)

        # Headers
        auth_headers = get_auth_headers(api_key)

        # Dados e estatísticas
        parent_data = {}
        endpoint_stats = {}

        # Parâmetros por endpoint
        endpoint_params = {
            "deals": {"sort": "dateCreated", "order": "ASC"},
            "custom_fields": {},
            "stages": {},
            "pipelines": {},
            "lost_reasons": {},
            "users": {}
        }

        # Processar endpoints principais
        endpoint_order = ["custom_fields", "pipelines", "lost_reasons", "users", "stages", "deals"]

        for endpoint_name in endpoint_order:
            processed_data, stats = process_primary_endpoint(
                http_client,
                endpoint_name,
                ENDPOINTS[endpoint_name],
                auth_headers,
                endpoint_params.get(endpoint_name)
            )
            parent_data[endpoint_name] = processed_data
            endpoint_stats[endpoint_name] = stats

        # Processar endpoints dependentes
        for endpoint_name, config in DEPENDENT_ENDPOINTS.items():
            parent_name = config["parent"]
            endpoint_stats[endpoint_name] = process_dependent_endpoint(
                http_client,
                endpoint_name,
                config,
                parent_data.get(parent_name, []),
                auth_headers
            )

        # Resumo final
        success = ReportGenerator.final_summary(logger, endpoint_stats, global_start_time)

        # BigQuery
        with MemoryMonitor(logger):
            BigQuery.process_csv_files()

        tables = Utils.get_existing_folders(logger)
        for table in tables:
            BigQuery.start_pipeline(args.PROJECT_ID, args.CRM_TYPE, table_name=table,
                                    credentials_path=args.GOOGLE_APPLICATION_CREDENTIALS)

        if not success:
            raise Exception(f"Falhas nos endpoints: {success}")

    except Exception as e:
        logger.exception(f"❌ ERRO CRÍTICO: {e}")
        raise


if __name__ == "__main__":
    main()
