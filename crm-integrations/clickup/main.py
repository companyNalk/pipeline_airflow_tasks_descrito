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
    "teams": "team",
}

DEPENDENT_ENDPOINTS = {
    "team_fields": {
        "endpoint": "team/{id}/field",
        "parent": "teams",
        "id_field": "id",
        "no_pagination": True
    },
    "team_custom_items": {
        "endpoint": "team/{id}/custom_item",
        "parent": "teams",
        "id_field": "id",
        "no_pagination": True
    },
    "team_spaces": {
        "endpoint": "team/{id}/space",
        "parent": "teams",
        "id_field": "id",
        "no_pagination": True
    }
}

# Endpoints que dependem dos IDs dos spaces
SPACE_DEPENDENT_ENDPOINTS = {
    "space_fields": {
        "endpoint": "space/{id}/field",
        "parent": "team_spaces",
        "id_field": "id",
        "no_pagination": True
    },
    "space_tags": {
        "endpoint": "space/{id}/tag",
        "parent": "team_spaces",
        "id_field": "id",
        "no_pagination": True
    },
    "space_folders": {
        "endpoint": "space/{id}/folder",
        "parent": "team_spaces",
        "id_field": "id",
        "no_pagination": True
    }
}

# Endpoints que dependem dos IDs dos folders
FOLDER_DEPENDENT_ENDPOINTS = {
    "folder_fields": {
        "endpoint": "folder/{id}/field",
        "parent": "space_folders",
        "id_field": "id",
        "no_pagination": True
    },
    "folder_lists": {
        "endpoint": "folder/{id}/list",
        "parent": "space_folders",
        "id_field": "id",
        "no_pagination": True
    }
}

# Endpoints que dependem dos IDs das lists
LIST_DEPENDENT_ENDPOINTS = {
    "list_fields": {
        "endpoint": "list/{id}/field",
        "parent": "folder_lists",
        "id_field": "id",
        "no_pagination": True
    },
    "list_tasks": {
        "endpoint": "list/{id}/task",
        "parent": "folder_lists",
        "id_field": "id",
        "no_pagination": True
    }
}


def get_arguments():
    """Configura e retorna os argumentos da linha de comando."""
    return (ArgumentManager("Script para coletar e processar dados da API ClickUp")
            .add("API_BASE_URL", "URL base", required=True, default="https://api.clickup.com/api/v2")
            .add("API_ACCESS_TOKEN", "Token de acesso para autenticação", required=True)
            .parse())


def get_auth_headers(access_token):
    """Prepara os cabeçalhos de autenticação para a API ClickUp."""
    logger.info("🔑 Preparando headers de autenticação")
    return {
        "Authorization": access_token,
        "Content-Type": "application/json"
    }


def fetch_data(http_client, endpoint, headers, offset=0, params=None, debug_info=None):
    """Busca dados de um endpoint específico."""
    # Para ClickUp, não usar offset/limit na maioria dos endpoints
    params = params or {}

    # Usar debug_info fornecido ou gerar um padrão
    req_debug = debug_info or f"{endpoint}"

    return http_client.get(endpoint, headers=headers, params=params, debug_info=req_debug)


def fetch_page(http_client, endpoint, headers, offset):
    """Busca uma página específica de um endpoint."""
    try:
        data = fetch_data(http_client, endpoint, headers, offset)

        # Para a API do ClickUp, extrair dados baseado na estrutura de resposta
        if 'teams' in data:
            items = data.get('teams', [])
        elif 'fields' in data:
            items = data.get('fields', [])
        elif 'custom_items' in data:
            items = data.get('custom_items', [])
        elif 'spaces' in data:
            items = data.get('spaces', [])
        elif 'tags' in data:
            items = data.get('tags', [])
        elif 'folders' in data:
            items = data.get('folders', [])
        elif 'lists' in data:
            items = data.get('lists', [])
        elif 'tasks' in data:
            items = data.get('tasks', [])
        else:
            items = data if isinstance(data, list) else [data] if data else []

        # ClickUp geralmente não tem paginação tradicional para teams
        has_more = False
        total_count = len(items)
        total_pages = 1
        current_page = 1

        logger.info(f"📄 Endpoint {endpoint}: {len(items)} itens obtidos")

        return {
            'items': items,
            'has_more': has_more,
            'total_count': total_count,
            'total_pages': total_pages,
            'current_page': current_page,
            'limit': 100
        }
    except Exception as e:
        logger.error(f"❌ Erro ao buscar {endpoint}: {str(e)}")
        raise


def fetch_all_pages(http_client, endpoint, headers):
    """Busca todas as páginas de um endpoint."""
    logger.info(f"📚 Buscando dados para: {endpoint}")
    start_time = time.time()

    # Para ClickUp, geralmente uma única requisição é suficiente
    page_data = fetch_page(http_client, endpoint, headers, 0)
    all_items = page_data['items']

    duration = time.time() - start_time
    logger.info(f"✅ Endpoint {endpoint}: {len(all_items)} itens obtidos em {duration:.2f}s")
    return all_items


def fetch_with_id(http_client, template, id_value, headers, no_pagination=False):
    """Busca dados em endpoint com ID específico."""
    endpoint = template.replace("{id}", str(id_value))
    debug_info = f"id:{id_value}"

    try:
        result = fetch_data(http_client, endpoint, headers, debug_info=debug_info)

        # Log de debug para investigar estrutura de resposta
        logger.info(f"🔍 DEBUG - Endpoint: {endpoint}")
        logger.info(f"🔍 DEBUG - Tipo de resposta: {type(result)}")
        if isinstance(result, dict):
            logger.info(f"🔍 DEBUG - Chaves da resposta: {list(result.keys())}")
            logger.info(f"🔍 DEBUG - Resposta completa: {str(result)[:500]}...")

        if isinstance(result, dict) and result:
            # Para ClickUp, verificar diferentes estruturas de resposta
            if "fields" in result:
                items = result.get("fields", [])
                logger.info(f"✅ Encontrados {len(items)} fields")
            elif "custom_items" in result:
                items = result.get("custom_items", [])
                logger.info(f"✅ Encontrados {len(items)} custom_items")
            elif "spaces" in result:
                items = result.get("spaces", [])
                logger.info(f"✅ Encontrados {len(items)} spaces")
            elif "tags" in result:
                items = result.get("tags", [])
                logger.info(f"✅ Encontrados {len(items)} tags")
            elif "folders" in result:
                items = result.get("folders", [])
                logger.info(f"✅ Encontrados {len(items)} folders")
            elif "lists" in result:
                items = result.get("lists", [])
                logger.info(f"✅ Encontrados {len(items)} lists")
            elif "tasks" in result:
                items = result.get("tasks", [])
                logger.info(f"✅ Encontrados {len(items)} tasks")
            elif isinstance(result, list):
                items = result
                logger.info(f"✅ Resposta é lista direta com {len(items)} itens")
            else:
                # Se for um único objeto
                items = [result]
                logger.info("✅ Resposta é objeto único, convertendo para lista")

            # Adicionar ID do pai a cada item
            for item in items:
                if isinstance(item, dict):
                    item["parent_id"] = id_value

            logger.info(f"📊 Retornando {len(items)} itens para {endpoint}")
            return items

        logger.warning(f"⚠️ Resposta vazia ou inválida para {endpoint}: {result}")
        return []

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

    if not parent_data:
        logger.warning(f"⚠️ Sem dados de parent para processar {endpoint_name}")
        return []

    # Log de debug dos dados do parent
    logger.info(f"🔍 DEBUG - Parent data recebido: {len(parent_data)} registros")
    if len(parent_data) > 0:
        logger.info(f"🔍 DEBUG - Primeiro registro do parent: {str(parent_data[0])[:300]}...")
        logger.info(
            f"🔍 DEBUG - Chaves do primeiro registro: {list(parent_data[0].keys()) if isinstance(parent_data[0], dict) else 'Não é dict'}")

    # Extrair IDs únicos dos dados do parent (evitar duplicação)
    unique_ids = set()
    for item in parent_data:
        if isinstance(item, dict) and id_field in item:
            unique_ids.add(str(item[id_field]))

    unique_ids_list = list(unique_ids)
    logger.info(f"🎯 Encontrados {len(unique_ids_list)} IDs únicos para processar em {endpoint_name}")

    # Log dos primeiros IDs para debug
    if len(unique_ids_list) > 0:
        sample_ids = unique_ids_list[:3]
        logger.info(f"🔍 DEBUG - Primeiros IDs: {sample_ids}")
        logger.info(f"🔍 DEBUG - Template endpoint: {endpoint_template}")

    if not unique_ids_list:
        logger.warning(f"⚠️ Nenhum ID único encontrado no campo '{id_field}' para {endpoint_name}")
        return []

    # Limitar número de IDs para processamento
    limited_ids = unique_ids_list[:MAX_IDS] if len(unique_ids_list) > MAX_IDS else unique_ids_list
    if len(unique_ids_list) > MAX_IDS:
        logger.warning(
            f"⚠️ Limitando processamento para {MAX_IDS} de {len(unique_ids_list)} IDs únicos para {endpoint_name}")

    all_items = []
    errors = {'rate_limit': 0, 'other': []}

    # Processar em lotes para melhor controle
    batch_size = 5
    total_batches = (len(limited_ids) + batch_size - 1) // batch_size
    start_time = time.time()

    logger.info(f"🔄 Processando {len(limited_ids)} IDs únicos em {total_batches} lotes para {endpoint_name}")

    for batch_num, i in enumerate(range(0, len(limited_ids), batch_size), 1):
        batch_ids = limited_ids[i:i + batch_size]
        batch_start_time = time.time()

        if batch_num == 1 or batch_num == total_batches or batch_num % 20 == 0:
            progress = (batch_num / total_batches) * 100
            logger.info(
                f"📦 Lote {batch_num}/{total_batches} ({progress:.1f}%) para {endpoint_name} ({len(batch_ids)} IDs únicos)")

        # Função para processar cada ID único
        def process_id(id_value):
            try:
                logger.info(
                    f"🔄 Processando ID: {id_value} para endpoint: {endpoint_template.replace('{id}', str(id_value))}")
                result = fetch_with_id(http_client, endpoint_template, id_value, headers, no_pagination)
                logger.info(f"📊 ID {id_value} retornou {len(result)} itens")
                return result
            except Exception as e:
                error_msg = str(e)
                logger.error(f"❌ Erro no ID {id_value}: {error_msg}")
                if "429" in error_msg:
                    errors['rate_limit'] += 1
                else:
                    errors['other'].append(f"ID {id_value}: {error_msg}")
                return []

        # Processar lote em paralelo
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(batch_ids)) as executor:
            batch_results = list(executor.map(process_id, batch_ids))

        # Coletar resultados do lote
        batch_count = 0
        for result in batch_results:
            batch_count += len(result)
            all_items.extend(result)

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
    logger.info(f"✓ IDs únicos processados: {len(limited_ids)}")
    logger.info(f"✓ Registros obtidos: {len(all_items)}")
    logger.info(f"⏱️ Tempo total: {duration:.2f}s ({duration / len(limited_ids):.2f}s por ID)")

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


def clean_text_fields(data):
    """
    Função genérica para limpar campos de texto, removendo TODAS as quebras de linha
    e normalizando espaçamentos de forma mais robusta.
    """
    if not data:
        return data

    def clean_string_value(value):
        """Limpa um valor string individual de forma mais agressiva."""
        if not isinstance(value, str):
            return value

        import re

        # Primeiro, substituir TODAS as possíveis quebras de linha por espaço
        # Incluindo \r\n, \n, \r, e até caracteres Unicode de quebra de linha
        cleaned = re.sub(r'[\r\n\u2028\u2029\u000A\u000B\u000C\u000D\u0085]+', ' ', value)

        # Remover caracteres de controle ASCII (0-31 exceto espaço)
        cleaned = re.sub(r'[\x00-\x08\x0B-\x0C\x0E-\x1F\x7F]', ' ', cleaned)

        # Remover múltiplos espaços consecutivos (incluindo tabs, espaços não-quebráveis, etc)
        cleaned = re.sub(r'\s+', ' ', cleaned)

        # Remover espaços no início e fim
        cleaned = cleaned.strip()

        return cleaned

    def clean_item(item):
        """Limpa um item (dict) recursivamente."""
        if isinstance(item, dict):
            cleaned_item = {}
            for key, value in item.items():
                if isinstance(value, str):
                    cleaned_item[key] = clean_string_value(value)
                elif isinstance(value, dict):
                    cleaned_item[key] = clean_item(value)
                elif isinstance(value, list):
                    cleaned_item[key] = [
                        clean_item(sub_item) if isinstance(sub_item, (dict, list)) else clean_string_value(
                            sub_item) if isinstance(sub_item, str) else sub_item for sub_item in value]
                else:
                    cleaned_item[key] = value
            return cleaned_item
        elif isinstance(item, list):
            return [clean_item(sub_item) for sub_item in item]
        elif isinstance(item, str):
            return clean_string_value(item)
        else:
            return item

    # Processar os dados
    if isinstance(data, list):
        return [clean_item(item) for item in data]
    else:
        return clean_item(data)


def preprocess_teams_data(raw_data):
    """
    Função auxiliar específica para pré-processar dados de teams,
    expandindo o array 'members' em linhas separadas antes de usar Utils.
    """
    if not raw_data or not isinstance(raw_data, list):
        return raw_data

    logger.info("🔧 Pré-processando dados de teams para expandir members")

    # Primeiro, limpar quebras de linha de todos os campos de forma robusta
    cleaned_data = clean_text_fields(raw_data)

    normalized_data = []

    for team in cleaned_data:
        if not isinstance(team, dict):
            continue

        # Verificar se há members para expandir
        if 'members' in team and isinstance(team['members'], list) and len(team['members']) > 0:
            # Extrair dados base do team (sem members)
            team_base = {k: v for k, v in team.items() if k != 'members'}

            # Criar uma linha para cada member
            for member in team['members']:
                member_row = team_base.copy()

                # Adicionar dados do member com prefixo
                if isinstance(member, dict):
                    for member_key, member_value in member.items():
                        member_row[f"member_{member_key}"] = member_value

                normalized_data.append(member_row)

            logger.info(f"✓ Team {team.get('id', 'N/A')}: expandidos {len(team['members'])} members")
        else:
            # Se não há members, manter team como está
            normalized_data.append(team)

    # Aplicar limpeza novamente nos dados expandidos para garantir
    final_cleaned_data = clean_text_fields(normalized_data)

    logger.info(
        f"✅ Pré-processamento concluído: {len(raw_data)} teams → {len(final_cleaned_data)} linhas (texto normalizado)")
    return final_cleaned_data


def process_primary_endpoint(http_client, endpoint_name, endpoint_path, headers):
    """Processa um endpoint principal e retorna os dados e estatísticas."""
    try:
        logger.info(f"\n{'=' * 50}\n🔍 PROCESSANDO ENDPOINT: {endpoint_name.upper()}\n{'=' * 50}")

        endpoint_start = time.time()
        raw_data = fetch_all_pages(http_client, endpoint_path, headers)

        # Aplicar pré-processamento específico para teams
        if endpoint_name == 'teams':
            processed_raw_data = preprocess_teams_data(raw_data)
        else:
            # Para outros endpoints, aplicar limpeza robusta de texto
            processed_raw_data = clean_text_fields(raw_data)

        # Processar e salvar dados usando Utils existente
        logger.info(f"💾 Processando e salvando {len(processed_raw_data)} registros para {endpoint_name}")
        processed_data = Utils.process_and_save_data(processed_raw_data, endpoint_name)

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

        stats = {
            "registros": 0,
            "status": f"Falha: {type(e).__name__}: {str(e)}",
            "tempo": 0
        }

        return [], stats


def process_dependent_endpoint(http_client, endpoint_name, config, parent_data, headers):
    """Processa um endpoint dependente e retorna estatísticas e dados."""
    parent_name = config["parent"]

    if not parent_data:
        logger.warning(f"⚠️ Sem dados do pai '{parent_name}' para {endpoint_name}")
        return {
            "registros": 0,
            "status": f"Ignorado: sem dados do pai '{parent_name}'",
            "tempo": 0,
            "data": []
        }

    try:
        full_config = {
            "name": endpoint_name,
            **config
        }

        endpoint_start = time.time()

        dependent_data = process_dependent_endpoints(
            http_client,
            parent_data,
            full_config,
            headers
        )

        # Aplicar limpeza robusta antes de processar com Utils
        cleaned_dependent_data = clean_text_fields(dependent_data)

        logger.info(f"💾 Processando e salvando {len(cleaned_dependent_data)} registros para {endpoint_name}")
        processed_data = Utils.process_and_save_data(cleaned_dependent_data, endpoint_name)

        endpoint_duration = time.time() - endpoint_start

        stats = {
            "registros": len(dependent_data),
            "status": "Sucesso",
            "tempo": endpoint_duration,
            "data": processed_data  # Incluir dados processados
        }

        logger.info(f"✅ {endpoint_name}: {len(dependent_data)} registros em {endpoint_duration:.2f}s")
        return stats

    except Exception as e:
        logger.exception(f"❌ Falha no endpoint dependente {endpoint_name}")
        return {
            "registros": 0,
            "status": f"Falha: {type(e).__name__}: {str(e)}",
            "tempo": 0,
            "data": []
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

        # 5. Processamento de endpoints dependentes dos teams
        logger.info(f"\n{'🔗' * 20} NÍVEL 1: ENDPOINTS DEPENDENTES DOS TEAMS {'🔗' * 20}")
        for endpoint_name, config in DEPENDENT_ENDPOINTS.items():
            parent_name = config["parent"]
            processed_data = process_dependent_endpoint(
                http_client,
                endpoint_name,
                config,
                parent_data.get(parent_name, []),
                auth_headers
            )
            # Armazenar dados processados para uso posterior
            parent_data[endpoint_name] = processed_data.get("data", []) if isinstance(processed_data, dict) else []
            endpoint_stats[endpoint_name] = processed_data

        # 6. Processamento de endpoints dependentes dos spaces
        logger.info(f"\n{'🔗' * 20} NÍVEL 2: ENDPOINTS DEPENDENTES DOS SPACES {'🔗' * 20}")
        for endpoint_name, config in SPACE_DEPENDENT_ENDPOINTS.items():
            parent_name = config["parent"]
            processed_data = process_dependent_endpoint(
                http_client,
                endpoint_name,
                config,
                parent_data.get(parent_name, []),
                auth_headers
            )
            # Armazenar dados processados para uso posterior
            parent_data[endpoint_name] = processed_data.get("data", []) if isinstance(processed_data, dict) else []
            endpoint_stats[endpoint_name] = processed_data

        # 7. Processamento de endpoints dependentes dos folders
        logger.info(f"\n{'🔗' * 20} NÍVEL 3: ENDPOINTS DEPENDENTES DOS FOLDERS {'🔗' * 20}")
        for endpoint_name, config in FOLDER_DEPENDENT_ENDPOINTS.items():
            parent_name = config["parent"]
            processed_data = process_dependent_endpoint(
                http_client,
                endpoint_name,
                config,
                parent_data.get(parent_name, []),
                auth_headers
            )
            # Armazenar dados processados para uso posterior
            parent_data[endpoint_name] = processed_data.get("data", []) if isinstance(processed_data, dict) else []
            endpoint_stats[endpoint_name] = processed_data

        # 8. Processamento de endpoints dependentes das lists
        logger.info(f"\n{'🔗' * 20} NÍVEL 4: ENDPOINTS DEPENDENTES DAS LISTS {'🔗' * 20}")
        for endpoint_name, config in LIST_DEPENDENT_ENDPOINTS.items():
            parent_name = config["parent"]
            endpoint_stats[endpoint_name] = process_dependent_endpoint(
                http_client,
                endpoint_name,
                config,
                parent_data.get(parent_name, []),
                auth_headers
            )

        # 9. Gerar resumo final
        success = ReportGenerator.final_summary(logger, endpoint_stats, global_start_time)

        # Se houver falhas, lançar exceção para o Airflow
        if not success:
            raise Exception(f"Falhas nos endpoints: {success}")

    except Exception as e:
        logger.exception(f"❌ ERRO CRÍTICO NA EXECUÇÃO: {e}")
        raise


if __name__ == "__main__":
    main()
"""
TEAM (id) → SPACE (parent_id) → FOLDER (parent_id) → LIST (parent_id) → TASK (parent_id)
"""
