import time

from commons.app_inicializer import AppInitializer
from commons.report_generator import ReportGenerator
from commons.utils import Utils
from generic.argument_manager import ArgumentManager
from generic.http_client import HttpClient
from generic.rate_limiter import RateLimiter

logger = AppInitializer.initialize()

RATE_LIMIT = 5
ENDPOINTS = {
    # "deals": "api/3/deals",
    "deal_groups": "api/3/dealGroups",
    "deal_stages": "api/3/dealStages",
    "deal_custom_field_meta": "api/3/dealCustomFieldMeta",
    # "deal_custom_field_data": "api/3/dealCustomFieldData"
}


def get_arguments():
    """Configura e retorna os argumentos da linha de comando."""
    return (ArgumentManager("Script para coletar e processar dados da API ActiveCampaign")
            .add("ACCOUNT_NAME", "Nome da conta ActiveCampaign", required=True)
            .add("API_TOKEN", "Token de autenticação", required=True)
            .parse())


def fetch_data(endpoint, token, offset=0, params=None, debug_info=None):
    """Busca dados de um endpoint específico."""
    headers = {"Api-Token": token}

    # Use valor padrão para parâmetros - limite máximo da ActiveCampaign
    params = params or {}
    params.update({"offset": offset, "limit": 100})

    debug_info = debug_info or f"{endpoint}:offset{offset}"

    return http_client.get(endpoint, headers=headers, params=params, debug_info=debug_info)


def fetch_page(endpoint, token, offset=0):
    """Busca uma página específica de um endpoint."""
    try:
        data = fetch_data(endpoint, token, offset)

        # Extrair dados baseado na estrutura do endpoint
        endpoint_name = endpoint.split('/')[-1]  # Pega o último segmento da URL
        items = data.get(endpoint_name, [])

        # Informações de paginação da ActiveCampaign
        meta = data.get('meta', {})
        total = int(meta.get('total', 0))
        current_offset = offset
        limit = 100

        # Calcular páginas baseado no offset
        current_page = (current_offset // limit) + 1
        total_pages = (total + limit - 1) // limit if total > 0 else 1

        # Simplificação da condição de log - mais frequente para acompanhar progresso
        if current_page == 1 or current_page == total_pages or current_page % 50 == 0:
            logger.info(f"📄 Endpoint {endpoint}: página {current_page}/{total_pages} com {len(items)} itens")

        return {
            'items': items,
            'meta': {
                'total': total,
                'limit': limit,
                'offset': current_offset,
                'current_page': current_page,
                'total_pages': total_pages
            },
            'total_pages': total_pages,
            'has_more': (current_offset + limit) < total
        }
    except Exception as e:
        logger.error(f"❌ Erro ao buscar offset {offset} para {endpoint}: {str(e)}")
        raise


def fetch_all_pages(endpoint, token):
    """Busca todas as páginas de um endpoint."""
    logger.info(f"📚 Buscando todas as páginas para: {endpoint}")
    start_time = time.time()

    # Buscar primeira página
    first_page = fetch_page(endpoint, token, 0)
    all_items = first_page['items'].copy()
    total_pages = first_page['total_pages']

    if total_pages <= 1 or not first_page['has_more']:
        logger.info(f"✓ Endpoint {endpoint}: 1 página com {len(all_items)} itens obtidos")
        return all_items

    # Buscar páginas restantes usando offset
    current_offset = 100  # Próximo offset
    page_num = 2

    while current_offset < first_page['meta']['total']:
        try:
            page_data = fetch_page(endpoint, token, current_offset)
            all_items.extend(page_data['items'])

            # Pausa otimizada - 0.2s permite ~5 req/s respeitando rate limit
            time.sleep(0.2)

            # Próximo offset
            current_offset += 100
            page_num += 1

            # Se não há mais dados, parar
            if not page_data['has_more']:
                break

        except Exception as e:
            logger.error(f"❌ Erro no offset {current_offset} para {endpoint}: {str(e)}")
            break

    duration = time.time() - start_time
    logger.info(f"✅ Endpoint {endpoint}: {total_pages} páginas com {len(all_items)} itens obtidos em {duration:.2f}s")
    return all_items


def process_endpoint(endpoint_name, endpoint_path, token):
    """Processa um endpoint específico e retorna estatísticas."""
    try:
        logger.info(f"\n{'=' * 50}\n🔍 PROCESSANDO ENDPOINT: {endpoint_name.upper()}\n{'=' * 50}")

        endpoint_start = time.time()
        raw_data = fetch_all_pages(endpoint_path, token)

        # Processar e salvar dados
        logger.info(f"💾 Processando e salvando {len(raw_data)} registros para {endpoint_name}")
        processed_data = Utils.process_and_save_data(raw_data, endpoint_name)

        endpoint_duration = time.time() - endpoint_start

        # Retornar estatísticas
        return {
            "registros": len(processed_data),
            "status": "Sucesso",
            "tempo": endpoint_duration
        }

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
    account_name = args.ACCOUNT_NAME
    api_base_url = f"https://{account_name}.api-us1.com"
    api_token = args.API_TOKEN

    rate_limiter = RateLimiter(requests_per_window=5, window_seconds=1, logger=logger)  # 5 req/s
    http_client = HttpClient(base_url=api_base_url, rate_limiter=rate_limiter, logger=logger)

    # 3. Iniciar relatório
    global_start_time = ReportGenerator.init_report(logger)
    endpoint_stats = {}

    try:
        # 4. Processar todos os endpoints
        for endpoint_name, endpoint_path in ENDPOINTS.items():
            endpoint_stats[endpoint_name] = process_endpoint(endpoint_name, endpoint_path, api_token)
            logger.info(
                f"✅ {endpoint_name}: {endpoint_stats[endpoint_name]['registros']} registros em {endpoint_stats[endpoint_name]['tempo']:.2f}s")

        # 5. Gerar resumo final
        success = ReportGenerator.final_summary(logger, endpoint_stats, global_start_time)

        # Se houver falhas, lançar exceção
        if not success:
            raise Exception(f"Falhas nos endpoints: {success}")

    except Exception as e:
        logger.exception(f"❌ ERRO CRÍTICO NA EXECUÇÃO: {e}")
        raise


if __name__ == "__main__":
    main()
