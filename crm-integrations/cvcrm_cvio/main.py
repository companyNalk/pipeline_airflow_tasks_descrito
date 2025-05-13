# -*- coding: utf-8 -*-
import functools
import re

from commons.app_inicializer import AppInitializer
from commons.report_generator import ReportGenerator
from commons.utils import Utils
from generic.argument_manager import ArgumentManager
from generic.http_client import HttpClient
from generic.rate_limiter import RateLimiter

logger = AppInitializer.initialize()

RATE_LIMIT = 100

ENDPOINTS = {
    "lead": "{api_plano}/lead",
}


def get_arguments():
    """Configura e retorna os argumentos da linha de comando."""
    return (ArgumentManager("Script para coletar dados do CVCRM")
            .add("API_DOMINIO", "Domínio CVCRM", required=True)
            .add("API_EMAIL", "Email de acesso", required=True)
            .add("API_ACCESS_TOKEN", "Token de acesso", required=True)
            .parse())


def fetch_data(endpoint, headers, offset=0, limit=500, params=None, debug_info=None):
    """Busca dados da API com paginação baseada em offset e limit."""
    if params is None:
        params = {}
    params.update({"offset": offset, "limit": limit})
    debug_info = debug_info or f"{endpoint}:offset{offset}"

    response = http_client.get(endpoint, headers=headers, params=params, debug_info=debug_info)

    if not isinstance(response, dict):
        raise ValueError(f"Resposta inválida da API no offset {offset}: {response[:300]}")

    return response


def fetch_page(endpoint, headers, page=1, limit=500):
    """Busca uma página específica de um endpoint usando offset e limit."""
    try:
        # Calcular o offset baseado na página e no limit
        offset = (page - 1) * limit

        data = fetch_data(endpoint, headers, offset=offset, limit=limit)

        # Extrair dados conforme o novo formato de resposta
        items = data.get('leads', [])
        total_records = int(data.get('total', 0))

        # Calcular o número total de páginas
        total_pages = (total_records + limit - 1) // limit if limit > 0 else 1

        # Construir metadata
        meta = {
            'total': total_records,
            'perPage': limit,
            'totalPages': total_pages,
            'currentPage': page,
            'offset': offset,
            'codigo': data.get('codigo'),
            'totalConteudo': data.get('totalConteudo', len(items))
        }

        if page == 1 or page == total_pages or page % 20 == 0:
            logger.info(
                f"📄 Endpoint {endpoint}: página {page}/{total_pages} com {len(items)} itens (offset: {offset}, total: {total_records})")

        return {
            'items': items,
            'meta': meta,
            'total_pages': total_pages
        }
    except Exception as e:
        logger.error(f"❌ Erro ao buscar página {page} (offset {(page - 1) * limit}) para {endpoint}: {str(e)}")
        raise


##############################################################################
#                                                                            #
#  IMPORTANTE: CAMPOS QUE INICIAM COM "interacao_" e                         #
#             "tarefa_X_descricao" SÃO REMOVIDOS DO CSV FINAL                #
#                                                                            #
##############################################################################
def apply_utils_patches():
    """Aplica patches nas funções da classe Utils para remover campos específicos."""
    logger.info("Aplicando patches nas funções da classe Utils para remover campos específicos")

    # Salvar a referência original da função _flatten_json
    original_flatten_json = Utils._flatten_json

    # Padrões para campos que devem ser removidos
    interacao_pattern = re.compile(r'^interacao_')
    tarefa_descricao_pattern = re.compile(r'^tarefa_[1-9]_descricao$')

    # Criar uma nova versão da função que remove campos indesejados
    @functools.wraps(original_flatten_json)
    def patched_flatten_json(json_obj, parent_key='', sep='_'):
        # Primeiro, usar a função original para achatar o JSON
        flattened = original_flatten_json(json_obj, parent_key, sep)

        # Depois, filtrar as chaves que correspondem aos padrões a serem removidos
        filtered = {}
        for k, v in flattened.items():
            # Verificar se a chave começa com 'interacao_'
            if interacao_pattern.match(k):
                continue

            # Verificar se a chave segue o padrão 'tarefa_X_descricao' onde X é um dígito de 1 a 9
            if tarefa_descricao_pattern.match(k):
                continue

            # Se não corresponder a nenhum padrão a ser filtrado, manter o campo
            filtered[k] = v

        return filtered

    # Substituir a função original pela versão patcheada
    Utils._flatten_json = patched_flatten_json

    logger.info("✅ Patches aplicados com sucesso")


def process_endpoint(endpoint_name, endpoint_path, headers, save_to_disk=True, chunk_size=500, batch_size=1, delay=5):
    """Processa um endpoint específico e retorna estatísticas."""
    try:
        logger.info(f"\n{'=' * 50}\n🔍 PROCESSANDO ENDPOINT: {endpoint_name.upper()}\n{'=' * 50}")

        # Função para processar em lotes com os novos parâmetros de paginação
        return Utils.process_data_in_batches(
            endpoint_name,
            endpoint_path,
            headers,
            lambda ep, hdrs, pg: fetch_page(ep, hdrs, pg, limit=chunk_size),  # Wrapper para passar o limit
            save_to_disk=save_to_disk,
            chunk_size=chunk_size,
            batch_size=batch_size,
            delay_between_pages=delay
        )

    except Exception as e:
        logger.exception(f"❌ Falha no endpoint {endpoint_name}")
        return {
            "registros": 0,
            "status": f"Falha: {type(e).__name__}: {str(e)}",
            "tempo": 0
        }


def main():
    """Função principal para coleta de dados."""
    # 1. Obter argumentos de linha de comando
    args = get_arguments()

    # 2. Configurar cliente HTTP
    global http_client
    api_dominio = args.API_DOMINIO
    api_email = args.API_EMAIL
    api_access_token = args.API_ACCESS_TOKEN
    api_plano = 'cvio'

    # Configurar endpoints formatados com o plano
    formatted_endpoints = {}
    for endpoint_name, endpoint_template in ENDPOINTS.items():
        formatted_endpoints[endpoint_name] = endpoint_template.format(api_plano=api_plano)

    # Configurar cliente HTTP
    api_base_url = f"https://{api_dominio}.cvcrm.com.br/api"
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "email": api_email,
        "token": api_access_token
    }

    rate_limiter = RateLimiter(requests_per_window=RATE_LIMIT, logger=logger)
    http_client = HttpClient(base_url=api_base_url, rate_limiter=rate_limiter, logger=logger)

    # 3. Aplicar patches nas funções Utils para remover campos específicos
    apply_utils_patches()

    # 4. Iniciar relatório
    global_start_time = ReportGenerator.init_report(logger)
    endpoint_stats = {}

    try:
        # 5. Processar todos os endpoints
        for endpoint_name, endpoint_path in formatted_endpoints.items():
            # Usar um valor menor para chunk_size para evitar sobrecarga na API
            endpoint_stats[endpoint_name] = process_endpoint(
                endpoint_name,
                endpoint_path,
                headers,
                save_to_disk=True,
            )
            logger.info(
                f"✅ {endpoint_name}: {endpoint_stats[endpoint_name]['registros']} registros em {endpoint_stats[endpoint_name]['tempo']:.2f}s")

        # 6. Gerar resumo final
        success = ReportGenerator.final_summary(logger, endpoint_stats, global_start_time)

        # Se houver falhas, lançar exceção
        if not success:
            raise RuntimeError(f"Falhas nos endpoints: {success}")

    except Exception as e:
        logger.exception(f"❌ ERRO CRÍTICO NA EXECUÇÃO: {e}")
        raise


if __name__ == "__main__":
    main()
