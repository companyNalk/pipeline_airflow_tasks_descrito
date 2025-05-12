# -*- coding: utf-8 -*-
from commons.app_inicializer import AppInitializer
from commons.report_generator import ReportGenerator
from commons.utils import Utils
from generic.argument_manager import ArgumentManager
from generic.http_client import HttpClient
from generic.rate_limiter import RateLimiter

logger = AppInitializer.initialize()

RATE_LIMIT = 100

ENDPOINTS = {
    "vendas": "{api_plano}/vendas",
    "reservas": "{api_plano}/reservas",
    "leads_visitas": "{api_plano}/leads/visitas",
    "leads": "{api_plano}/leads",
    "leads_historico_situacoes": "{api_plano}/leads/historico/situacoes",

    # PARA CLIENTE WIKIHAUS
    "leads_corretores": "{api_plano}/leads/corretores",
    "precadastros": "{api_plano}/precadastros",

}


def get_arguments():
    """Configura e retorna os argumentos da linha de comando."""
    return (ArgumentManager("Script para coletar dados do CVCRM")
            .add("API_DOMINIO", "Domínio CVCRM", required=True)
            .add("API_EMAIL", "Email de acesso", required=True)
            .add("API_ACCESS_TOKEN", "Token de acesso", required=True)
            .parse())


def fetch_data(endpoint, headers, page=1, params=None, debug_info=None):
    if params is None:
        params = {}
    params.update({"pagina": page, "registros_por_pagina": 500})
    debug_info = debug_info or f"{endpoint}:p{page}"

    response = http_client.get(endpoint, headers=headers, params=params, debug_info=debug_info)

    if not isinstance(response, dict):
        raise ValueError(f"Resposta inválida da API na página {page}: {response[:300]}")

    return response


def fetch_page(endpoint, headers, page=1):
    """Busca uma página específica de um endpoint."""
    try:
        data = fetch_data(endpoint, headers, page)
        items = data.get('dados', [])
        meta = {
            'total': int(data.get('total_de_registros', 0)),
            'perPage': int(data.get('registros_por_pagina', 500)),
            'totalPages': int(data.get('total_de_paginas', 1)),
            'currentPage': int(data.get('pagina_atual', page)),
        }
        total_pages = meta['totalPages']

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


def process_endpoint(endpoint_name, endpoint_path, headers, save_to_disk=True, chunk_size=500, batch_size=1, delay=5):
    """Processa um endpoint específico e retorna estatísticas."""
    try:
        logger.info(f"\n{'=' * 50}\n🔍 PROCESSANDO ENDPOINT: {endpoint_name.upper()}\n{'=' * 50}")

        # Função genérica para processar em lotes
        return Utils.process_data_in_batches(
            endpoint_name,
            endpoint_path,
            headers,
            fetch_page,  # Passando a função fetch_page
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
    api_plano = 'cvdw'

    # Configurar endpoints formatados com o plano
    formatted_endpoints = {}
    for endpoint_name, endpoint_template in ENDPOINTS.items():
        formatted_endpoints[endpoint_name] = endpoint_template.format(api_plano=api_plano)

    # Configurar cliente HTTP
    api_base_url = f"https://{api_dominio}.cvcrm.com.br/api/v1"
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "email": api_email,
        "token": api_access_token
    }

    rate_limiter = RateLimiter(requests_per_window=RATE_LIMIT, logger=logger)
    http_client = HttpClient(base_url=api_base_url, rate_limiter=rate_limiter, logger=logger)

    # 3. Iniciar relatório
    global_start_time = ReportGenerator.init_report(logger)
    endpoint_stats = {}

    try:
        # 4. Processar todos os endpoints
        for endpoint_name, endpoint_path in formatted_endpoints.items():
            endpoint_stats[endpoint_name] = process_endpoint(
                endpoint_name,
                endpoint_path,
                headers,
                save_to_disk=True
            )
            logger.info(
                f"✅ {endpoint_name}: {endpoint_stats[endpoint_name]['registros']} registros em {endpoint_stats[endpoint_name]['tempo']:.2f}s")

        # 5. Gerar resumo final
        success = ReportGenerator.final_summary(logger, endpoint_stats, global_start_time)

        # Se houver falhas, lançar exceção
        if not success:
            raise RuntimeError(f"Falhas nos endpoints: {success}")

    except Exception as e:
        logger.exception(f"❌ ERRO CRÍTICO NA EXECUÇÃO: {e}")
        raise


if __name__ == "__main__":
    main()
