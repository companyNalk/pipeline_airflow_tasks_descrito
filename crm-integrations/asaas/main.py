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

MAX_IDS = 100
RATE_LIMIT = 100
MAX_WORKERS = min(10, os.cpu_count() or 5)

ENDPOINTS = {
    "payments": "v3/payments",
    "customers": "v3/customers",
    "subscriptions": "v3/subscriptions",
}

DEPENDENT_ENDPOINTS = {
    "subscription_payments": {
        "endpoint": "v3/subscriptions/{id}/payments",
        "parent": "subscriptions",
        "id_field": "id"
    }
}


def get_arguments():
    """Configura e retorna os argumentos da linha de comando."""
    return (ArgumentManager("Script para coletar e processar dados da API Asaas")
            .add("API_BASE_URL", "URL base", required=True)
            .add("API_ACCESS_TOKEN", "Token de acesso para autenticação", required=True)
            .add("PROJECT_ID", "ID do projeto Google Cloud", required=True)
            .add("CRM_TYPE", "Nome da ferramenta", required=True)
            .add("GOOGLE_APPLICATION_CREDENTIALS", "Credencial GCS", required=True)
            .parse())


def get_auth_headers(access_token):
    """Prepara os cabeçalhos de autenticação para a API Asaas."""
    logger.info("🔑 Preparando headers de autenticação")
    return {
        "access_token": access_token,
        "Content-Type": "application/json"
    }


def fetch_data(http_client, endpoint, headers, offset=0, params=None, debug_info=None):
    """Busca dados de um endpoint específico."""
    # Parâmetros padrão com possibilidade de override
    params = params or {}
    default_params = {"limit": 100, "offset": offset}
    default_params.update(params)

    # Usar debug_info fornecido ou gerar um padrão
    req_debug = debug_info or f"{endpoint}:o{offset}"

    return http_client.get(endpoint, headers=headers, params=default_params, debug_info=req_debug)


def fetch_page(http_client, endpoint, headers, offset):
    """Busca uma página específica de um endpoint."""
    try:
        data = fetch_data(http_client, endpoint, headers, offset)
        items = data.get('data', [])
        has_more = data.get('hasMore', False)
        total_count = data.get('totalCount', 0)

        # Calcular número total de páginas
        limit = data.get('limit', 100)
        total_pages = (total_count + limit - 1) // limit
        current_page = (offset // limit) + 1

        # Log condicional para reduzir verbosidade
        if current_page == 1 or not has_more or current_page % 20 == 0:
            logger.info(f"📄 Endpoint {endpoint}: página {current_page}/{total_pages} com {len(items)} itens")

        return {
            'items': items,
            'has_more': has_more,
            'total_count': total_count,
            'total_pages': total_pages,
            'current_page': current_page,
            'limit': limit
        }
    except Exception as e:
        logger.error(f"❌ Erro ao buscar offset {offset} para {endpoint}: {str(e)}")
        raise


def fetch_all_pages(http_client, endpoint, headers):
    """Busca todas as páginas de um endpoint usando ThreadPoolExecutor."""
    logger.info(f"📚 Buscando todas as páginas para: {endpoint}")
    start_time = time.time()

    # Buscar primeira página
    first_page = fetch_page(http_client, endpoint, headers, 0)
    all_items = first_page['items'].copy()  # Uso de .copy() para evitar referências
    has_more = first_page['has_more']
    limit = first_page['limit']

    # Calcular total de páginas
    total_pages = first_page['total_pages']

    if not has_more:
        logger.info(f"✓ Endpoint {endpoint}: 1 página com {len(all_items)} itens obtidos")
        return all_items

    # Definir worker para buscar páginas em paralelo
    def fetch_page_worker(offset_num):
        try:
            page_data = fetch_page(http_client, endpoint, headers, offset_num)
            return page_data['items']
        except Exception as e:
            logger.error(f"❌ Worker: erro no offset {offset_num} para {endpoint}: {str(e)}")
            return []

    # Gerar lista de offsets para as páginas restantes
    remaining_offsets = [i * limit for i in range(1, total_pages)]
    logger.info(f"🔄 Coletando {len(remaining_offsets)} páginas restantes para {endpoint} com {MAX_WORKERS} workers")

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_offset = {executor.submit(fetch_page_worker, offset): offset for offset in remaining_offsets}

        completed = 0
        for future in concurrent.futures.as_completed(future_to_offset):
            page_items = future.result()
            all_items.extend(page_items)

            # Log de progresso periodicamente
            completed += 1
            if completed % 10 == 0 or completed == len(remaining_offsets):
                progress = (completed / len(remaining_offsets)) * 100
                logger.info(f"📊 Progresso {endpoint}: {completed}/{len(remaining_offsets)} páginas ({progress:.1f}%)")

    duration = time.time() - start_time
    logger.info(f"✅ Endpoint {endpoint}: {total_pages} páginas com {len(all_items)} itens obtidos em {duration:.2f}s")
    return all_items


def fetch_with_id(http_client, template, id_value, headers, no_pagination=False):
    """Busca dados em endpoint com ID específico."""
    endpoint = template.replace("{id}", str(id_value))
    debug_info = f"id:{id_value}"

    try:
        if no_pagination:
            # Endpoint sem paginação
            result = fetch_data(http_client, endpoint, headers, debug_info=debug_info)

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

            return []
        else:
            # Endpoint com paginação
            items = fetch_all_pages(http_client, endpoint, headers)
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
                return fetch_with_id(http_client, endpoint_template, str(id_value), headers, no_pagination)
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


def process_primary_endpoint(http_client, endpoint_name, endpoint_path, headers):
    """Processa um endpoint principal e retorna os dados e estatísticas."""
    try:
        logger.info(f"\n{'=' * 50}\n🔍 PROCESSANDO ENDPOINT: {endpoint_name.upper()}\n{'=' * 50}")

        endpoint_start = time.time()
        raw_data = fetch_all_pages(http_client, endpoint_path, headers)

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
        api_access_token = args.API_ACCESS_TOKEN

        # 2. Configurar cliente HTTP e rate limiter
        rate_limiter = RateLimiter(requests_per_window=RATE_LIMIT, logger=logger)
        http_client = HttpClient(base_url=api_base_url, rate_limiter=rate_limiter, logger=logger)

        # 3. Preparar headers de autenticação
        auth_headers = get_auth_headers(api_access_token)
        logger.info("✅ Headers de autenticação preparados com sucesso")

        # 4. Processamento de endpoints principais
        parent_data = {}
        endpoint_stats = {}

        for endpoint_name, endpoint_path in ENDPOINTS.items():
            processed_data, stats = process_primary_endpoint(http_client, endpoint_name, endpoint_path, auth_headers)
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

        with MemoryMonitor(logger):
            BigQuery.process_csv_files()

        tables = Utils.get_existing_folders(logger)
        for table in tables:
            BigQuery.start_pipeline(args.PROJECT_ID, args.CRM_TYPE, table_name=table,
                                    credentials_path=args.GOOGLE_APPLICATION_CREDENTIALS)

        # Se houver falhas, lançar exceção para o Airflow
        if not success:
            raise Exception(f"Falhas nos endpoints: {success}")

    except Exception as e:
        logger.exception(f"❌ ERRO CRÍTICO NA EXECUÇÃO: {e}")
        raise


if __name__ == "__main__":
    main()
