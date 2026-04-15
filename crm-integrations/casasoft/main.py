import time

import requests

from commons.app_inicializer import AppInitializer
from commons.big_query import BigQuery
from commons.memory_monitor import MemoryMonitor
from commons.report_generator import ReportGenerator
from commons.utils import Utils
from generic.argument_manager import ArgumentManager
from generic.http_client import HttpClient
from generic.rate_limiter import RateLimiter

logger = AppInitializer.initialize()

AUTH_BASE_URL = "https://auth.api.cloudslim.com.br"

# Limite conservador — ajustar conforme T&C da CasaSoft/CloudSLIM
RATE_LIMIT = 60

# Endpoints paginados via ?pagina=N (começa em 1, para quando array retorna vazio)
ENDPOINTS = {
    "imoveis": "imoveis",
    "inquilinos": "inquilinos",
    "proprietarios": "proprietarios",
    "tickets": "tickets",
    "geds": "geds",
    "rncs": "rncs",
    "taxas_fixas": "TaxaFixas",
    "iptus": "iptus",
}

# Endpoints que aceitam filtro de data (dataInicial / dataFinal)
ENDPOINTS_COM_FILTRO_DATA = {"imoveis", "inquilinos", "proprietarios", "tickets"}

http_client = None  # Inicializado em main()


def get_arguments():
    """Configura e retorna os argumentos da linha de comando."""
    return (
        ArgumentManager("Script para coletar e processar dados da API CasaSoft (CloudSLIM)")
        .add("API_USERNAME", "Usuário de autenticação da API CasaSoft", required=True)
        .add("API_PASSWORD", "Senha de autenticação da API CasaSoft", required=True)
        .add("API_EMPRESA", "Empresa de autenticação da API CasaSoft", required=True)
        .add("API_BASE_URL", "URL base da API (ex: https://api.cloudslim.com.br/api/v1)", required=True)
        .add("PROJECT_ID", "ID do projeto no Google Cloud", required=True)
        .add("CRM_TYPE", "Tipo de CRM para namespacing no BigQuery (ex: casasoft)", required=True)
        .add("GOOGLE_APPLICATION_CREDENTIALS", "Caminho para credenciais do Google Cloud", required=True)
        .add("DATA_INICIAL", "Data inicial para carga incremental (YYYY-MM-DD)", required=False, default=None)
        .add("DATA_FINAL", "Data final para carga incremental (YYYY-MM-DD)", required=False, default=None)
        .parse()
    )


def login(username, password, empresa):
    """Autentica na API CloudSLIM e retorna o access token."""
    url = f"{AUTH_BASE_URL}/login"
    payload = {"username": username, "password": password, "empresa": empresa}
    logger.info(f"Autenticando na API CloudSLIM (empresa: {empresa})")
    response = requests.post(url, json=payload, timeout=30)
    response.raise_for_status()
    token = response.json().get("access")
    if not token:
        raise ValueError("Resposta de login não contém campo 'access'")
    logger.info("Autenticação CloudSLIM bem-sucedida")
    return token


def fetch_page(endpoint, token, pagina=1, extra_params=None):
    """Busca uma página específica de um endpoint."""
    headers = {"Authorization": f"Bearer {token}"}
    params = {**(extra_params or {}), "pagina": str(pagina)}
    debug_info = f"{endpoint}:p{pagina}"
    return http_client.get(endpoint, headers=headers, params=params, debug_info=debug_info)


def extract_items(response):
    """
    Extrai a lista de itens da resposta, compatível com:
      - array direto: [...]
      - objeto wrapper: {"dados": [...]} / {"data": [...]} / {"items": [...]} / etc.
    """
    if isinstance(response, list):
        return response
    if isinstance(response, dict):
        for key in ("dados", "data", "items", "resultado", "registros", "content"):
            if key in response and isinstance(response[key], list):
                return response[key]
    return []


def fetch_all_pages(endpoint, token, extra_params=None):
    """Busca todas as páginas de um endpoint iterando ?pagina=N até array vazio."""
    logger.info(f"Buscando todas as páginas para: {endpoint}")
    start_time = time.time()
    all_items = []
    pagina = 1

    while True:
        try:
            response = fetch_page(endpoint, token, pagina, extra_params)
            items = extract_items(response)

            if not items:
                logger.info(f"Endpoint {endpoint}: página {pagina} vazia — coleta encerrada")
                break

            all_items.extend(items)

            if pagina == 1 or pagina % 10 == 0:
                logger.info(f"Endpoint {endpoint}: página {pagina} — {len(items)} itens (acumulado: {len(all_items)})")

            pagina += 1
            time.sleep(0.2)  # Throttle gentil entre páginas

        except Exception as e:
            logger.error(f"Erro na página {pagina} de {endpoint}: {str(e)}")
            break

    duration = time.time() - start_time
    logger.info(f"Endpoint {endpoint}: {len(all_items)} itens em {pagina - 1} página(s) ({duration:.2f}s)")
    return all_items


def build_date_params(data_inicial, data_final):
    """Monta parâmetros de filtro de data quando informados (carga incremental)."""
    params = {}
    if data_inicial:
        params["dataInicial"] = data_inicial
    if data_final:
        params["dataFinal"] = data_final
    return params if params else None


def process_endpoint(endpoint_name, endpoint_path, token, data_inicial=None, data_final=None):
    """Processa um endpoint: coleta, normaliza e salva os dados."""
    try:
        logger.info(f"\n{'=' * 50}\nPROCESSANDO ENDPOINT: {endpoint_name.upper()}\n{'=' * 50}")
        endpoint_start = time.time()

        extra_params = None
        if endpoint_name in ENDPOINTS_COM_FILTRO_DATA:
            extra_params = build_date_params(data_inicial, data_final)

        raw_data = fetch_all_pages(endpoint_path, token, extra_params)

        if not raw_data:
            logger.warning(f"Endpoint {endpoint_name}: nenhum dado retornado")
            return {
                "registros": 0,
                "status": "Sem dados",
                "tempo": time.time() - endpoint_start,
            }

        processed_data = Utils.process_and_save_data(raw_data, endpoint_name)
        endpoint_duration = time.time() - endpoint_start

        logger.info(f"Endpoint {endpoint_name}: {len(processed_data)} registros processados em {endpoint_duration:.2f}s")
        return {
            "registros": len(processed_data),
            "status": "Sucesso",
            "tempo": endpoint_duration,
        }

    except Exception as e:
        logger.exception(f"Falha no endpoint {endpoint_name}")
        return {
            "registros": 0,
            "status": f"Falha: {type(e).__name__}: {str(e)}",
            "tempo": 0,
        }


def main():
    """Função principal para coleta de dados da API CasaSoft."""
    args = get_arguments()

    # Configurar cliente HTTP com rate limiter
    global http_client
    rate_limiter = RateLimiter(requests_per_window=RATE_LIMIT, window_seconds=60, logger=logger)
    http_client = HttpClient(base_url=args.API_BASE_URL, rate_limiter=rate_limiter, logger=logger)

    token = login(args.API_USERNAME, args.API_PASSWORD, args.API_EMPRESA)

    global_start_time = ReportGenerator.init_report(logger, report_name="COLETA DE DADOS API CASASOFT")
    endpoint_stats = {}

    try:
        for endpoint_name, endpoint_path in ENDPOINTS.items():
            endpoint_stats[endpoint_name] = process_endpoint(
                endpoint_name,
                endpoint_path,
                token,
                data_inicial=args.DATA_INICIAL,
                data_final=args.DATA_FINAL,
            )

        success = ReportGenerator.final_summary(logger, endpoint_stats, global_start_time)

        with MemoryMonitor(logger):
            BigQuery.process_csv_files()

        tables = Utils.get_existing_folders(logger)
        for table in tables:
            BigQuery.start_pipeline(
                args.PROJECT_ID,
                args.CRM_TYPE,
                table_name=table,
                credentials_path=args.GOOGLE_APPLICATION_CREDENTIALS,
            )

        if success is not True:
            raise Exception(f"Falhas detectadas nos endpoints: {success}")

    except Exception as e:
        logger.exception(f"ERRO CRÍTICO NA EXECUÇÃO: {e}")
        raise


if __name__ == "__main__":
    main()
