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

MAX_IDS = 100
RATE_LIMIT = 100
MAX_WORKERS = min(10, os.cpu_count() or 5)

ENDPOINTS = {
    "users": "v2/users",
    "courses": "v2/courses",
    "user_subscriptions": "v2/user-subscriptions",
}

DEPENDENT_ENDPOINTS = {
    # User
    "user_progress": {
        "endpoint": "v2/users/{id}/progress",
        "parent": "users",
        "id_field": "id"
    },
    "user_courses": {
        "endpoint": "v2/users/{id}/courses",
        "parent": "users",
        "id_field": "id"
    },
    # Course
    "course_users": {
        "endpoint": "v2/courses/{id}/users",
        "parent": "courses",
        "id_field": "id"
    },
    "course_analytics": {
        "endpoint": "v2/courses/{id}/analytics",
        "parent": "courses",
        "id_field": "id",
        "no_pagination": True
    }
}


def get_arguments():
    """Configura e retorna os argumentos da linha de comando."""
    return (ArgumentManager("Script para coletar e processar dados da API MevBrasil")
            .add("API_BASE_URL", "URL base", required=True)
            .add("API_CLIENT_ID", "ID do cliente para autenticação", required=True)
            .add("API_CLIENT_SECRET", "Secret do cliente para autenticação", required=True)
            .add("LW_CLIENT", "Identificador do cliente LW", required=True)
            .parse())


def get_auth_token(http_client, client_id, client_secret):
    """Obtém o token de autenticação da API."""
    logger.info("🔑 Obtendo token de autenticação")
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    payload = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret
    }

    data = http_client.post("oauth2/access_token", headers=headers, data=payload, debug_info="auth")

    token = data.get("tokenData", {}).get("access_token")
    if not token:
        raise Exception("Token não encontrado na resposta")

    return token


def fetch_data(http_client, endpoint, token, page=1, params=None, debug_info=None):
    """Busca dados de um endpoint específico."""
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    # Parâmetros padrão com possibilidade de override
    params = params or {}
    default_params = {"items_per_page": 200, "page": page}
    default_params.update(params)

    # Usar debug_info fornecido ou gerar um padrão
    req_debug = debug_info or f"{endpoint}:p{page}"

    return http_client.get(endpoint, headers=headers, params=default_params, debug_info=req_debug)


def fetch_page(http_client, endpoint, token, page):
    """Busca uma página específica de um endpoint."""
    try:
        data = fetch_data(http_client, endpoint, token, page)
        items = data.get('data', [])
        meta = data.get('meta', {})
        total_pages = meta.get('totalPages', 1)

        # Log condicional para reduzir verbosidade
        if page == 1 or page == total_pages or page % 20 == 0:
            logger.info(f"📄 Endpoint {endpoint}: página {page}/{total_pages} com {len(items)} itens")

        return {
            'items': items,
            'meta': meta,
            'total_pages': total_pages
        }
    except Exception as e:
        logger.error(f"❌ Erro ao buscar página {page} para {endpoint}: {str(e)}")
        raise


def fetch_all_pages(http_client, endpoint, token):
    """Busca todas as páginas de um endpoint usando ThreadPoolExecutor."""
    logger.info(f"📚 Buscando todas as páginas para: {endpoint}")
    start_time = time.time()

    # Buscar primeira página
    first_page = fetch_page(http_client, endpoint, token, 1)
    all_items = first_page['items'].copy()  # Uso de .copy() para evitar referências
    total_pages = first_page['total_pages']

    if total_pages <= 1:
        logger.info(f"✓ Endpoint {endpoint}: 1 página com {len(all_items)} itens obtidos")
        return all_items

    # Definir worker para buscar páginas em paralelo
    def fetch_page_worker(page_num):
        try:
            page_data = fetch_page(http_client, endpoint, token, page_num)
            return page_data['items']
        except Exception as e:
            logger.error(f"❌ Worker: erro na página {page_num} para {endpoint}: {str(e)}")
            return []

    # Buscar páginas restantes em paralelo
    remaining_pages = list(range(2, total_pages + 1))
    logger.info(f"🔄 Coletando {len(remaining_pages)} páginas restantes para {endpoint} com {MAX_WORKERS} workers")

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_page = {executor.submit(fetch_page_worker, page): page for page in remaining_pages}

        completed = 0
        for future in concurrent.futures.as_completed(future_to_page):
            page_items = future.result()
            all_items.extend(page_items)

            # Log de progresso periodicamente
            completed += 1
            if completed % 10 == 0 or completed == len(remaining_pages):
                progress = (completed / len(remaining_pages)) * 100
                logger.info(f"📊 Progresso {endpoint}: {completed}/{len(remaining_pages)} páginas ({progress:.1f}%)")

    duration = time.time() - start_time
    logger.info(f"✅ Endpoint {endpoint}: {total_pages} páginas com {len(all_items)} itens obtidos em {duration:.2f}s")
    return all_items


def fetch_with_id(http_client, template, id_value, token, no_pagination=False):
    """Busca dados em endpoint com ID específico."""
    endpoint = template.replace("{id}", str(id_value))
    debug_info = f"id:{id_value}"

    try:
        if no_pagination:
            # Endpoint sem paginação (ex: analytics)
            result = fetch_data(http_client, endpoint, token, debug_info=debug_info)

            if "analytics" in endpoint:
                logger.debug(f"📊 Analytics para ID {id_value}: {result}")

            if isinstance(result, dict) and result:
                result["parent_id"] = id_value
                return [result]
            return []
        else:
            # Endpoint com paginação
            items = fetch_all_pages(http_client, endpoint, token)
            # Adicionar ID do pai a cada item
            for item in items:
                item["parent_id"] = id_value
            return items

    except Exception as e:
        logger.error(f"❌ Erro ao buscar {endpoint} para ID {id_value}: {str(e)}")
        return []


def process_dependent_endpoints(http_client, parent_data, config, token):
    """Processa endpoints dependentes de IDs usando ThreadPoolExecutor."""
    endpoint_name = config.get("name", "")
    endpoint_template = config.get("endpoint", "")
    id_field = config.get("id_field", "id")
    no_pagination = config.get("no_pagination", False)

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
                return fetch_with_id(http_client, endpoint_template, str(id_value), token, no_pagination)
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


def process_primary_endpoint(http_client, endpoint_name, endpoint_path, token):
    """Processa um endpoint principal e retorna os dados e estatísticas."""
    try:
        logger.info(f"\n{'=' * 50}\n🔍 PROCESSANDO ENDPOINT: {endpoint_name.upper()}\n{'=' * 50}")

        endpoint_start = time.time()
        raw_data = fetch_all_pages(http_client, endpoint_path, token)

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


def process_dependent_endpoint(http_client, endpoint_name, config, parent_data, token):
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
            token
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
        api_client_id = args.API_CLIENT_ID
        api_client_secret = args.API_CLIENT_SECRET
        lw_client = args.LW_CLIENT

        # 2. Configurar cliente HTTP e rate limiter
        rate_limiter = RateLimiter(requests_per_window=RATE_LIMIT, logger=logger)
        http_client = HttpClient(base_url=api_base_url, rate_limiter=rate_limiter, logger=logger)

        # Adicionar headers padrão
        http_client.add_default_header("Lw-Client", lw_client)
        http_client.add_default_header("Accept-Encoding", "gzip, deflate")

        # 3. Autenticação
        token = get_auth_token(http_client, api_client_id, api_client_secret)
        logger.info("✅ Autenticação realizada com sucesso")

        # 4. Processamento de endpoints principais
        parent_data = {}
        endpoint_stats = {}

        for endpoint_name, endpoint_path in ENDPOINTS.items():
            processed_data, stats = process_primary_endpoint(http_client, endpoint_name, endpoint_path, token)
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
                token
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
