import concurrent.futures
import os
import time

from commons.app_inicializer import AppInitializer
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
    "customFields": "v2/customFields",
    "stages": "v2/stages",
    "pipelines": "v2/pipelines",
    "lostReasons": "v2/lostReasons",
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
            .parse())


def get_auth_headers(api_key):
    """Prepara os cabeçalhos de autenticação para a API Moskit."""
    logger.info("🔑 Preparando headers de autenticação")
    return {
        "Accept": "application/json",
        "apikey": api_key
    }


def fetch_data(http_client, endpoint, headers, offset=0, params=None, debug_info=None):
    """Busca dados de um endpoint específico."""
    # Parâmetros padrão com possibilidade de override
    params = params or {}
    default_params = {"quantity": QUANTITY}

    if isinstance(offset, int):
        default_params["offset"] = offset
    elif isinstance(offset, str) and offset.startswith("token:"):
        default_params["nextPageToken"] = offset.replace("token:", "")

    default_params.update(params)

    # Usar debug_info fornecido ou gerar um padrão
    req_debug = debug_info or f"{endpoint}:o{offset}"

    return http_client.get(endpoint, headers=headers, params=default_params, debug_info=req_debug)


def fetch_page(http_client, endpoint, headers, offset, params=None):
    """Busca uma página específica de um endpoint."""
    try:
        data = fetch_data(http_client, endpoint, headers, offset, params)

        # Verificar se a resposta é uma lista ou um objeto com campo 'data'
        if isinstance(data, list):
            items = data
        else:
            items = data.get('data', [])

        # Verificar o token de próxima página nas headers da resposta
        next_page_token = None
        if hasattr(data, 'headers'):
            next_page_token = data.headers.get("X-Moskit-Listing-Next-Page-Token")

        has_more = bool(next_page_token)
        total_count = len(items)  # A API Moskit não fornece contagem total

        # Determinar número da página atual (aproximado)
        current_page = 1
        if isinstance(offset, int):
            limit = params.get('quantity', QUANTITY) if params else QUANTITY
            current_page = (offset // limit) + 1

        # Log condicional para reduzir verbosidade
        if current_page == 1 or not has_more or current_page % 20 == 0:
            logger.info(f"📄 Endpoint {endpoint}: página {current_page} com {len(items)} itens")

        return {
            'items': items,
            'has_more': has_more,
            'next_page_token': next_page_token,
            'total_count': total_count,
            'current_page': current_page
        }
    except Exception as e:
        logger.error(f"❌ Erro ao buscar offset {offset} para {endpoint}: {str(e)}")
        raise


def fetch_all_pages(http_client, endpoint, headers, params=None):
    """Busca todas as páginas de um endpoint."""
    logger.info(f"📚 Buscando todas as páginas para: {endpoint}")
    start_time = time.time()

    # Buscar primeira página
    first_page = fetch_page(http_client, endpoint, headers, 0, params)
    all_items = first_page['items'].copy()  # Uso de .copy() para evitar referências
    has_more = first_page['has_more']
    next_token = first_page.get('next_page_token')

    # Inicializar contadores
    page_count = 1

    # Continuar buscando enquanto houver mais páginas
    while has_more and next_token:
        try:
            page_count += 1

            # Usar o token para a próxima página
            page_data = fetch_page(http_client, endpoint, headers, f"token:{next_token}", params)
            all_items.extend(page_data['items'])

            # Atualizar controles de paginação
            has_more = page_data['has_more']
            next_token = page_data.get('next_page_token')

            # Log de progresso periódico
            if page_count % 10 == 0:
                logger.info(f"📊 Progresso {endpoint}: {len(all_items)} itens coletados em {page_count} páginas")
                time.sleep(0.5)  # Pausa para respeitar rate limit

        except Exception as e:
            logger.error(f"❌ Erro ao buscar página {page_count} de {endpoint}: {str(e)}")
            # Tentar novamente após uma pausa
            time.sleep(2)
            continue

    duration = time.time() - start_time
    logger.info(f"✅ Endpoint {endpoint}: {page_count} páginas com {len(all_items)} itens obtidos em {duration:.2f}s")
    return all_items


def fetch_with_id(http_client, template, id_value, headers, params=None, no_pagination=False):
    """Busca dados em endpoint com ID específico."""
    endpoint = template.replace("{id}", str(id_value))
    debug_info = f"id:{id_value}"

    try:
        if no_pagination:
            # Endpoint sem paginação
            result = fetch_data(http_client, endpoint, headers, debug_info=debug_info, params=params)

            if isinstance(result, dict) and result:
                # Se for um único objeto, adicionar o ID do pai
                if "object" in result and result["object"] != "list":
                    result["parent_id"] = id_value
                    return [result]
                # Se for uma lista mas não no formato padrão
                elif "data" in result and isinstance(result["data"], list):
                    for item in result["data"]:
                        item["parent_id"] = id_value
                    return result["data"]
                # Para o caso de retornar um objeto direto
                else:
                    result["parent_id"] = id_value
                    return [result]
            elif isinstance(result, list):
                # Se já for uma lista, adicionar parent_id a cada item
                for item in result:
                    item["parent_id"] = id_value
                return result

            return []
        else:
            # Endpoint com paginação
            items = fetch_all_pages(http_client, endpoint, headers, params)
            # Adicionar ID do pai a cada item
            for item in items:
                item["parent_id"] = id_value
            return items

    except Exception as e:
        logger.error(f"❌ Erro ao buscar {endpoint} para ID {id_value}: {str(e)}")
        return []


def process_dependent_endpoints(http_client, parent_data, config, headers):
    """Processa endpoints dependentes de IDs usando ThreadPoolExecutor."""
    endpoint_name = config.get("name", "")
    endpoint_template = config.get("endpoint", "")
    id_field = config.get("id_field", "id")
    no_pagination = config.get("no_pagination", False)
    params = config.get("params", {})

    logger.info(f"\n{'=' * 30} PROCESSANDO ENDPOINT DEPENDENTE: {endpoint_name.upper()} {'=' * 30}")

    # Verificar se há dados de parent
    if not parent_data:
        logger.warning(f"⚠️ Sem dados de parent para processar {endpoint_name}")
        return []

    # Limitar número de IDs para processamento
    limited_data = parent_data[:MAX_IDS] if len(parent_data) > MAX_IDS else parent_data
    if len(parent_data) > MAX_IDS:
        logger.warning(f"⚠️ Limitando processamento para {MAX_IDS} de {len(parent_data)} itens para {endpoint_name}")

    all_items = []
    errors = {'rate_limit': 0, 'other': []}

    # Processar em lotes para melhor controle
    batch_size = 5
    total_batches = (len(limited_data) + batch_size - 1) // batch_size
    start_time = time.time()

    logger.info(f"🔄 Processando {len(limited_data)} IDs em {total_batches} lotes para {endpoint_name}")

    for batch_num, i in enumerate(range(0, len(limited_data), batch_size), 1):
        batch = limited_data[i:i + batch_size]
        batch_start_time = time.time()

        # Log condicional para reduzir verbosidade
        if batch_num == 1 or batch_num == total_batches or batch_num % 20 == 0:
            progress = (batch_num / total_batches) * 100
            logger.info(f"📦 Lote {batch_num}/{total_batches} ({progress:.1f}%) para {endpoint_name} ({len(batch)} IDs)")

        # Função para processar cada item do lote
        def process_item(item):
            id_value = item.get(id_field)
            if not id_value:
                return []

            try:
                return fetch_with_id(http_client, endpoint_template, str(id_value), headers, params, no_pagination)
            except Exception as e:
                error_msg = str(e)
                # Categorizar erros
                if "429" in error_msg:
                    errors['rate_limit'] += 1
                else:
                    errors['other'].append(f"ID {id_value}: {error_msg}")
                return []

        # Processar lote em paralelo
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(batch)) as executor:
            batch_results = list(executor.map(process_item, batch))

        # Coletar resultados do lote
        batch_count = 0
        for result in batch_results:
            batch_count += len(result)
            all_items.extend(result)

        # Log de desempenho do lote
        batch_duration = time.time() - batch_start_time
        if batch_num % 10 == 0 or batch_num == total_batches:
            logger.info(f"✓ Lote {batch_num} concluído: {batch_count} itens em {batch_duration:.2f}s")

        # Pausa entre lotes para controle de requisições
        if batch_num < total_batches:
            time.sleep(0.5)

    # Gerar resumo de processamento
    duration = time.time() - start_time
    logger.info(f"{'~' * 50}")
    logger.info(f"📊 RESUMO ENDPOINT {endpoint_name}:")
    logger.info(f"✓ IDs processados: {len(limited_data)}")
    logger.info(f"✓ Registros obtidos: {len(all_items)}")
    logger.info(f"⏱️ Tempo total: {duration:.2f}s ({duration / len(limited_data):.2f}s por ID)")

    # Relatório de erros
    if errors['rate_limit'] > 0:
        logger.warning(f"⚠️ Rate limit atingido {errors['rate_limit']} vezes")

    if errors['other']:
        if len(errors['other']) <= 3:
            for error in errors['other']:
                logger.error(f"❌ Erro: {error}")
        else:
            logger.error(f"❌ {len(errors['other'])} erros. Primeiros 3: {'; '.join(errors['other'][:3])}")

    logger.info(f"{'~' * 50}")
    return all_items


def process_primary_endpoint(http_client, endpoint_name, endpoint_path, headers, params=None):
    """Processa um endpoint principal e retorna os dados e estatísticas."""
    try:
        logger.info(f"\n{'=' * 50}\n🔍 PROCESSANDO ENDPOINT: {endpoint_name.upper()}\n{'=' * 50}")

        endpoint_start = time.time()
        raw_data = fetch_all_pages(http_client, endpoint_path, headers, params)

        # Processar e salvar dados
        logger.info(f"💾 Processando e salvando {len(raw_data)} registros para {endpoint_name}")
        processed_data = Utils.process_and_save_data(raw_data, endpoint_name)

        endpoint_duration = time.time() - endpoint_start

        # Estatísticas de processamento
        stats = {
            "registros": len(processed_data),
            "status": "Sucesso",
            "tempo": endpoint_duration
        }

        logger.info(f"✅ {endpoint_name}: {len(processed_data)} registros em {endpoint_duration:.2f}s")

        return processed_data, stats

    except Exception as e:
        logger.exception(f"❌ Falha no endpoint {endpoint_name}")

        # Estatísticas em caso de erro
        stats = {
            "registros": 0,
            "status": f"Falha: {type(e).__name__}: {str(e)}",
            "tempo": 0
        }

        return [], stats


def process_dependent_endpoint(http_client, endpoint_name, config, parent_data, headers):
    """Processa um endpoint dependente e retorna estatísticas."""
    parent_name = config["parent"]

    # Verificar se temos dados do pai
    if not parent_data:
        logger.warning(f"⚠️ Sem dados do pai '{parent_name}' para {endpoint_name}")
        return {
            "registros": 0,
            "status": f"Ignorado: sem dados do pai '{parent_name}'",
            "tempo": 0
        }

    try:
        # Configurar dados completos
        full_config = {
            "name": endpoint_name,
            **config
        }

        endpoint_start = time.time()

        # Processar endpoint dependente
        dependent_data = process_dependent_endpoints(
            http_client,
            parent_data,
            full_config,
            headers
        )

        # Processar e salvar resultado
        logger.info(f"💾 Processando e salvando {len(dependent_data)} registros para {endpoint_name}")
        Utils.process_and_save_data(dependent_data, endpoint_name)

        endpoint_duration = time.time() - endpoint_start

        # Estatísticas de processamento
        stats = {
            "registros": len(dependent_data),
            "status": "Sucesso",
            "tempo": endpoint_duration
        }

        logger.info(f"✅ {endpoint_name}: {len(dependent_data)} registros em {endpoint_duration:.2f}s")
        return stats

    except Exception as e:
        logger.exception(f"❌ Falha no endpoint dependente {endpoint_name}")
        return {
            "registros": 0,
            "status": f"Falha: {type(e).__name__}: {str(e)}",
            "tempo": 0
        }


def main():
    """Função principal para coleta de dados."""
    # Iniciar relatório de processamento
    global_start_time = ReportGenerator.init_report(logger)

    try:
        # 1. Obter configurações
        args = get_arguments()

        api_base_url = args.API_BASE_URL.rstrip('/')
        api_key = args.API_KEY

        # 2. Configurar cliente HTTP e rate limiter
        rate_limiter = RateLimiter(requests_per_window=RATE_LIMIT, logger=logger)
        http_client = HttpClient(base_url=api_base_url, rate_limiter=rate_limiter, logger=logger)

        # 3. Preparar headers de autenticação
        auth_headers = get_auth_headers(api_key)
        logger.info("✅ Headers de autenticação preparados com sucesso")

        # 4. Processamento de endpoints principais
        parent_data = {}
        endpoint_stats = {}

        # Configuração de parâmetros específicos para cada endpoint
        endpoint_params = {
            "deals": {"sort": "dateCreated", "order": "ASC"},
            "customFields": {},
            "stages": {},
            "pipelines": {},
            "lostReasons": {},
            "users": {}
        }

        # Ordem de processamento para garantir que dados de referência estejam disponíveis primeiro
        endpoint_order = ["customFields", "pipelines", "lostReasons", "users", "stages", "deals"]

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

        # 5. Processamento de endpoints dependentes
        for endpoint_name, config in DEPENDENT_ENDPOINTS.items():
            parent_name = config["parent"]
            endpoint_stats[endpoint_name] = process_dependent_endpoint(
                http_client,
                endpoint_name,
                config,
                parent_data.get(parent_name, []),
                auth_headers
            )

        # 6. Gerar resumo final
        success = ReportGenerator.final_summary(logger, endpoint_stats, global_start_time)

        # Se houver falhas, lançar exceção para o Airflow
        if not success:
            raise Exception(f"Falhas nos endpoints: {success}")

    except Exception as e:
        logger.exception(f"❌ ERRO CRÍTICO NA EXECUÇÃO: {e}")
        raise


if __name__ == "__main__":
    main()
