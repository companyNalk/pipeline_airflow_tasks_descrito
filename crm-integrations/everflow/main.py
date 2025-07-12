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

CONFIG = {
    "rate_limit": 100,
    "endpoints": {
        "clientes": {"path": "v1/clientes", "data_key": "itens"},
        "contratos": {"path": "v1/contratos", "data_key": "itens"},
        "fornecedores": {"path": "v1/fornecedores", "data_key": "itens"},
        "movimento_bancarios": {"path": "v1/movimentosBancarios", "data_key": "itens"},
        "orcamentos": {"path": "v1/orcamentos", "data_key": "itens"},
        "orcamentos_propostas": {"path": "v1/orcamentos/propostas", "data_key": "itens"},
        "orcamentos_propostas_kanban": {"path": "v1/orcamentos/propostas/kanban", "data_key": "itens"},
        "gestao_servicos_ordens_servico": {"path": "v1/gestaoServicos/OrdensServico", "data_key": "itens"},
        "ordens_servico_tarefas": {"path": "v1/ordensServico/tarefas", "data_key": "itens"},
        "pagars": {"path": "v1/pagars", "data_key": "itens"},
        "pedidos_venda": {"path": "v1/pedidosVenda", "data_key": "itens"},
        "recebers": {"path": "v1/recebers", "data_key": "itens"},

        "consultores": {"path": "v1/consultores", "data_key": None},
        "equipamentos": {"path": "v1/equipamentos", "data_key": None},

        "funcionarios": {"path": "v1/funcionarios", "data_key": "data.itens"},
    }
}


def get_arguments():
    """Configura e retorna os argumentos da linha de comando."""
    return (ArgumentManager("Script para coletar e processar dados da API Flow2")
            .add("API_BASE_URL_FLOW2", "URL base para Flow2", required=True, default="https://api.flow2.com.br")
            .add("API_AUTH_TOKEN_FLOW2", "Token de autenticação para Flow2", required=True)
            .add("PROJECT_ID", "ID do projeto GCS", required=True)
            .add("CRM_TYPE", "Ferramenta: Nome aba sheets", required=True)
            .add("GOOGLE_APPLICATION_CREDENTIALS", "Credenciais GCS", required=True)
            .parse())


def extract_data_from_response(data, data_key):
    """Extrai dados da resposta baseado na chave de dados configurada."""
    if isinstance(data, list):
        return data

    if not isinstance(data, dict):
        return []

    # Mapear data_key para o caminho correto
    paths = {
        None: lambda d: d.get('data', []) if isinstance(d.get('data'), list) else [],
        "itens": lambda d: d.get('itens', []),
        "data": lambda d: d.get('data', []),
        "data.itens": lambda d: d.get('data', {}).get('itens', [])
    }

    return paths.get(data_key, lambda d: [])(data)


def get_pagination_info(data, data_key):
    """Obtém informações de paginação baseado na estrutura da resposta."""
    if isinstance(data, list):
        return {'total_pages': 1, 'total_records': len(data)}

    if not isinstance(data, dict):
        return {'total_pages': 1, 'total_records': 0}

    # Para data.itens, buscar paginação em data
    source = data.get('data', {}) if data_key == "data.itens" else data

    return {
        'total_pages': source.get('totalPages', 1),
        'total_records': source.get('totalRecords', 0)
    }


def fetch_all_data(http_client, endpoint_name, endpoint_config, token):
    """Busca todos os dados de um endpoint."""
    logger.info(f"📚 Buscando dados para: {endpoint_name}")
    start_time = time.time()
    all_items = []
    headers = {"Authorization": f"Bearer {token}"}

    # Endpoint sem paginação
    if endpoint_config['data_key'] is None:
        data = http_client.get(endpoint_config['path'], headers=headers)
        items = extract_data_from_response(data, None)
        duration = time.time() - start_time
        logger.info(f"✅ Endpoint {endpoint_name}: requisição única com {len(items)} itens em {duration:.2f}s")
        return items, len(items)

    # Endpoint com paginação
    page_num = 1
    total_records_expected = 0

    while True:
        # CORREÇÃO 1: Sempre incluir o parâmetro page, mesmo na primeira página
        params = {"pageSize": 500, "page": page_num}

        logger.info(f"🔄 Fazendo requisição para página {page_num} com params: {params}")

        data = http_client.get(endpoint_config['path'], headers=headers, params=params)

        # CORREÇÃO 2: Log da resposta para debug
        logger.info(f"📊 Resposta da página {page_num}: tipo={type(data)}")
        if isinstance(data, dict):
            logger.info(f"📊 Chaves da resposta: {list(data.keys())}")

        items = extract_data_from_response(data, endpoint_config['data_key'])

        # CORREÇÃO 3: Log detalhado dos itens extraídos
        logger.info(f"📦 Página {page_num}: extraídos {len(items)} itens")

        # CORREÇÃO 4: Verificar se não há itens para evitar loop infinito
        if not items and page_num > 1:
            logger.info(f"⚠️ Página {page_num} sem itens - finalizando paginação")
            break

        pagination_info = get_pagination_info(data, endpoint_config['data_key'])
        if page_num == 1:
            total_records_expected = pagination_info['total_records']

        # CORREÇÃO PRINCIPAL: Limitar itens ao total_records_expected
        remaining_records = total_records_expected - len(all_items)
        if remaining_records <= 0:
            logger.info(f"🎯 Limite de registros atingido: {len(all_items)}/{total_records_expected}")
            break

        # Se esta página tem mais itens do que o necessário, cortar
        if len(items) > remaining_records:
            items = items[:remaining_records]
            logger.info(
                f"✂️ Cortando página {page_num}: usando apenas {len(items)} de {len(extract_data_from_response(data, endpoint_config['data_key']))} itens")

        all_items.extend(items)

        # Se coletamos todos os registros esperados, parar
        if len(all_items) >= total_records_expected:
            logger.info(f"🎯 Todos os registros coletados: {len(all_items)}/{total_records_expected}")
            break

        total_pages = pagination_info['total_pages']

        # CORREÇÃO 5: Log da informação de paginação
        logger.info(
            f"📄 Paginação - Página atual: {page_num}, Total páginas: {total_pages}, Total registros: {pagination_info['total_records']}")

        if page_num == 1 or page_num == total_pages or page_num % 20 == 0:
            logger.info(
                f"📄 {endpoint_name}: página {page_num}/{total_pages} com {len(items)} itens (total coletado até agora: {len(all_items)})")

        # CORREÇÃO 6: Verificar múltiplas condições de parada
        if page_num >= total_pages or len(items) == 0:
            logger.info(
                f"🏁 Finalizando paginação - Página: {page_num}, Total páginas: {total_pages}, Itens na página: {len(items)}")
            break

        page_num += 1
        time.sleep(0.5)

    # CORREÇÃO 7: Log final detalhado
    duration = time.time() - start_time
    logger.info(
        f"✅ Endpoint {endpoint_name}: {len(all_items)} itens coletados (esperados: {total_records_expected}) em {duration:.2f}s")

    return all_items, total_records_expected


def process_endpoint(endpoint_name, endpoint_config, args):
    """Processa um endpoint específico e retorna estatísticas."""
    logger.info(f"\n{'=' * 50}\n🔍 PROCESSANDO: {endpoint_name.upper()}\n{'=' * 50}")

    try:
        base_url = args.API_BASE_URL_FLOW2.rstrip('/')
        rate_limiter = RateLimiter(requests_per_window=CONFIG["rate_limit"], logger=logger)
        http_client = HttpClient(base_url=base_url, rate_limiter=rate_limiter, logger=logger)

        start_time = time.time()
        raw_data, total_records_expected = fetch_all_data(http_client, endpoint_name, endpoint_config,
                                                          args.API_AUTH_TOKEN_FLOW2)

        Utils.process_and_save_data(raw_data, endpoint_name)

        return {
            "registros": total_records_expected,
            "registros_coletados": len(raw_data),
            "status": "Sucesso",
            "tempo": time.time() - start_time
        }

    except Exception as e:
        logger.exception(f"❌ Falha no endpoint {endpoint_name}")
        return {"registros": 0, "registros_coletados": 0, "status": f"Falha: {type(e).__name__}: {str(e)}", "tempo": 0}


def main():
    """Função principal para coleta de dados."""
    args = get_arguments()
    global_start_time = ReportGenerator.init_report(logger)
    endpoint_stats = {}

    try:
        for endpoint_name, endpoint_config in CONFIG["endpoints"].items():
            endpoint_stats[endpoint_name] = process_endpoint(endpoint_name, endpoint_config, args)
            logger.info(
                f"✅ {endpoint_name}: {endpoint_stats[endpoint_name]['registros']} esperados, {endpoint_stats[endpoint_name]['registros_coletados']} coletados em {endpoint_stats[endpoint_name]['tempo']:.2f}s")  # noqa

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
