"""
Extractor ContaAzul API v2 -> BigQuery.

Renova o access_token via refresh_token e extrai os 4 domínios:
  - pessoas (clientes/fornecedores)
  - produtos
  - vendas
  - financeiro (eventos financeiros: contas a pagar/receber)

Segue o padrão do workspace (commons/ + generic/), igual ao asaas.

ATENÇÃO — paths a confirmar no Portal do Desenvolvedor:
  - /v1/pessoas      ✅ confirmado
  - /v1/produtos     ⚠️ inferido
  - /v1/vendas       ⚠️ inferido
  - /v1/financeiro/eventos-financeiros  ⚠️ inferido
  Ajuste o dict ENDPOINTS abaixo após validar no portal.

Paginação da v2: parâmetros `pagina` (1-based) + `tamanho_pagina`.
O formato exato da resposta paginada ainda precisa ser confirmado — o paginador
abaixo é defensivo (detecta a lista e o fim da paginação por heurística).
"""

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

from auth import build_auth_headers, refresh_access_token

logger = AppInitializer.initialize()

# Rate limit da ContaAzul: 600/min e 10/s por conta ERP.
# Mantemos margem de segurança abaixo do teto de 600/min.
RATE_LIMIT = 500
PAGE_SIZE = 100
MAX_WORKERS = min(8, os.cpu_count() or 5)

DEFAULT_BASE_URL = "https://api-v2.contaazul.com/v1"

# Endpoints principais (paths a confirmar no portal — ver docstring)
ENDPOINTS = {
    "pessoas": "pessoas",
    "produtos": "produtos",
    "vendas": "vendas",
    "financeiro_eventos": "financeiro/eventos-financeiros",
}

# Possíveis chaves onde a v2 entrega a lista de itens na resposta paginada.
# O paginador tenta cada uma em ordem.
_LIST_KEYS = ("itens", "data", "content", "registros", "items")


def get_arguments():
    """Configura e retorna os argumentos da linha de comando / env."""
    return (ArgumentManager("Extractor da API ContaAzul v2")
            .add("CLIENT_ID", "Client ID do app ContaAzul", required=True)
            .add("CLIENT_SECRET", "Client Secret do app ContaAzul", required=True)
            .add("REFRESH_TOKEN", "Refresh token OAuth de longa duração", required=True)
            .add("API_BASE_URL", "URL base da API v2", required=False, default=DEFAULT_BASE_URL)
            .add("PROJECT_ID", "ID do projeto Google Cloud", required=True)
            .add("CRM_TYPE", "Nome da ferramenta (dataset destino)", required=True)
            .add("GOOGLE_APPLICATION_CREDENTIALS", "Credencial GCS", required=True)
            .parse())


def _extract_items(payload):
    """Extrai a lista de itens de uma resposta paginada, de forma defensiva."""
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in _LIST_KEYS:
            if isinstance(payload.get(key), list):
                return payload[key]
    return []


def fetch_all_pages(http_client, endpoint, headers):
    """
    Busca todas as páginas de um endpoint usando paginação `pagina`/`tamanho_pagina`.

    Como o total de páginas pode não vir confiável na resposta, paginamos
    sequencialmente até uma página retornar menos itens que PAGE_SIZE (fim).
    """
    logger.info(f"📚 Buscando todas as páginas para: {endpoint}")
    start_time = time.time()

    all_items = []
    pagina = 1
    while True:
        params = {"pagina": pagina, "tamanho_pagina": PAGE_SIZE}
        payload = http_client.get(endpoint, headers=headers, params=params,
                                  debug_info=f"{endpoint}:p{pagina}")
        items = _extract_items(payload)
        all_items.extend(items)

        if pagina == 1 or len(items) < PAGE_SIZE or pagina % 20 == 0:
            logger.info(f"📄 {endpoint}: página {pagina} com {len(items)} itens (acum: {len(all_items)})")

        if len(items) < PAGE_SIZE:
            break
        pagina += 1

    duration = time.time() - start_time
    logger.info(f"✅ {endpoint}: {len(all_items)} itens em {duration:.2f}s ({pagina} páginas)")
    return all_items


def process_primary_endpoint(http_client, endpoint_name, endpoint_path, headers):
    """Processa um endpoint principal e retorna os dados e estatísticas."""
    try:
        logger.info(f"\n{'=' * 50}\n🔍 PROCESSANDO ENDPOINT: {endpoint_name.upper()}\n{'=' * 50}")

        endpoint_start = time.time()
        raw_data = fetch_all_pages(http_client, endpoint_path, headers)

        logger.info(f"💾 Processando e salvando {len(raw_data)} registros para {endpoint_name}")
        processed_data = Utils.process_and_save_data(raw_data, endpoint_name)

        endpoint_duration = time.time() - endpoint_start
        stats = {
            "registros": len(processed_data),
            "status": "Sucesso",
            "tempo": endpoint_duration,
        }
        logger.info(f"✅ {endpoint_name}: {len(processed_data)} registros em {endpoint_duration:.2f}s")
        return processed_data, stats

    except Exception as e:
        logger.exception(f"❌ Falha no endpoint {endpoint_name}")
        stats = {
            "registros": 0,
            "status": f"Falha: {type(e).__name__}: {str(e)}",
            "tempo": 0,
        }
        return [], stats


def main():
    """Função principal para coleta de dados."""
    global_start_time = ReportGenerator.init_report(logger)

    try:
        # 1. Configurações
        args = get_arguments()
        api_base_url = args.API_BASE_URL.rstrip('/')

        # 2. Renovar access_token via refresh_token
        tokens = refresh_access_token(
            args.CLIENT_ID, args.CLIENT_SECRET, args.REFRESH_TOKEN, logger=logger
        )
        auth_headers = build_auth_headers(tokens["access_token"])

        # 3. Cliente HTTP + rate limiter
        rate_limiter = RateLimiter(requests_per_window=RATE_LIMIT, logger=logger)
        http_client = HttpClient(base_url=api_base_url, rate_limiter=rate_limiter, logger=logger)

        # 4. Endpoints principais
        endpoint_stats = {}
        for endpoint_name, endpoint_path in ENDPOINTS.items():
            _, stats = process_primary_endpoint(http_client, endpoint_name, endpoint_path, auth_headers)
            endpoint_stats[endpoint_name] = stats

        # 5. Resumo
        success = ReportGenerator.final_summary(logger, endpoint_stats, global_start_time)

        # 6. Pipeline BigQuery
        with MemoryMonitor(logger):
            BigQuery.process_csv_files()

        tables = Utils.get_existing_folders(logger)
        for table in tables:
            BigQuery.start_pipeline(args.PROJECT_ID, args.CRM_TYPE, table_name=table,
                                    credentials_path=args.GOOGLE_APPLICATION_CREDENTIALS)

        if not success:
            raise Exception(f"Falhas nos endpoints: {endpoint_stats}")

    except Exception as e:
        logger.exception(f"❌ ERRO CRÍTICO NA EXECUÇÃO: {e}")
        raise


if __name__ == "__main__":
    main()
