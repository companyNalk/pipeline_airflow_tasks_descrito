"""
CasaSoft (CloudSLIM) module for data extraction functions.

API: REST com Bearer Token no header Authorization.
Paginação: query string ?pagina=N (começa em 1, encerra quando array vazio).
Endpoints extraídos: imoveis, inquilinos, proprietarios, tickets, geds, rncs, taxas_fixas, iptus.

Campos obrigatórios em customer (spreadsheet.json):
    - api_token     : Bearer token da API CasaSoft
    - api_base_url  : URL base da API (ex: https://api.cloudslim.com.br/api/v1)
    - bucket_name   : Bucket GCS onde os CSVs serão salvos
    - project_id    : ID do projeto GCP

Campos opcionais em customer:
    - start_date    : Filtro de data inicial para carga incremental (YYYY-MM-DD)
    - end_date      : Filtro de data final para carga incremental (YYYY-MM-DD)
"""

import csv
import io
import logging
import time

import requests

from core import gcs

# Endpoints: {nome_tabela: path_da_rota}
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

# Endpoints que aceitam filtro dataInicial / dataFinal
ENDPOINTS_COM_FILTRO_DATA = {"imoveis", "inquilinos", "proprietarios", "tickets"}


def _get_headers(api_token):
    return {
        "Authorization": f"Bearer {api_token}",
        "Content-Type": "application/json",
    }


def _extract_items(response_data):
    """
    Extrai lista de itens da resposta, compatível com:
      - array direto: [...]
      - objeto wrapper: {"dados": [...]} / {"data": [...]} / etc.
    """
    if isinstance(response_data, list):
        return response_data
    if isinstance(response_data, dict):
        for key in ("dados", "data", "items", "resultado", "registros", "content"):
            if key in response_data and isinstance(response_data[key], list):
                return response_data[key]
    return []


def _fetch_all_pages(base_url, path, headers, extra_params=None):
    """
    Percorre todas as páginas de um endpoint via ?pagina=N (começa em 1).
    Encerra quando o array de retorno estiver vazio.
    """
    all_items = []
    pagina = 1
    url = f"{base_url.rstrip('/')}/{path}"

    while True:
        params = {**(extra_params or {}), "pagina": str(pagina)}
        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)

            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 10))
                logging.warning(f"[CasaSoft] Rate limit (429). Aguardando {retry_after}s...")
                time.sleep(retry_after)
                continue

            if response.status_code == 404:
                logging.warning(f"[CasaSoft] Endpoint não encontrado: {path} (404)")
                break

            response.raise_for_status()
            items = _extract_items(response.json())

            if not items:
                logging.info(f"[CasaSoft/{path}] Página {pagina} vazia — coleta encerrada")
                break

            all_items.extend(items)

            if pagina == 1 or pagina % 10 == 0:
                logging.info(f"[CasaSoft/{path}] Página {pagina} — {len(items)} itens (total: {len(all_items)})")

            pagina += 1
            time.sleep(0.2)

        except requests.exceptions.RequestException as e:
            logging.error(f"[CasaSoft/{path}] Erro na página {pagina}: {e}")
            break

    logging.info(f"[CasaSoft/{path}] {len(all_items)} registros coletados em {pagina - 1} página(s)")
    return all_items


def _save_to_gcs(customer, data, endpoint_name):
    """Serializa lista de dicts como CSV e envia ao GCS."""
    if not data:
        logging.warning(f"[CasaSoft] Sem dados para {endpoint_name}, pulando upload.")
        return

    # Coletar todas as chaves únicas para garantir CSV consistente
    all_keys = set()
    for row in data:
        if isinstance(row, dict):
            all_keys.update(row.keys())
    fieldnames = sorted(all_keys)

    # Gerar CSV em memória e salvar em /tmp
    local_path = f"/tmp/{customer['project_id']}.casasoft.{endpoint_name}.csv"
    with open(local_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(data)

    credentials = gcs.load_credentials_from_env()
    gcs.write_file_to_gcs(
        bucket_name=customer["bucket_name"],
        local_file_path=local_path,
        destination_name=f"{endpoint_name}/{endpoint_name}.csv",
        credentials=credentials,
    )
    logging.info(f"[CasaSoft] {endpoint_name}: {len(data)} registros salvos no GCS")


def extract_endpoint(customer, endpoint_name, path):
    """Extrai um endpoint completo e salva no GCS."""
    start_time = time.time()
    api_token = customer["api_token"]
    api_base_url = customer["api_base_url"]
    headers = _get_headers(api_token)

    extra_params = None
    if endpoint_name in ENDPOINTS_COM_FILTRO_DATA:
        start_date = customer.get("start_date")
        end_date = customer.get("end_date")
        if start_date or end_date:
            extra_params = {}
            if start_date:
                extra_params["dataInicial"] = start_date
            if end_date:
                extra_params["dataFinal"] = end_date

    logging.info(f"[CasaSoft] Iniciando extração: {endpoint_name} (projeto: {customer['project_id']})")
    data = _fetch_all_pages(api_base_url, path, headers, extra_params)
    _save_to_gcs(customer, data, endpoint_name)
    logging.info(f"[CasaSoft] {endpoint_name}: concluído em {time.time() - start_time:.2f}s")


def run(customer):
    """Extrai todos os endpoints da API CasaSoft e salva os CSVs no GCS."""
    logging.info(f"[CasaSoft] Iniciando coleta para projeto: {customer['project_id']}")
    start_time = time.time()

    for endpoint_name, path in ENDPOINTS.items():
        try:
            extract_endpoint(customer, endpoint_name, path)
        except Exception:
            logging.exception(f"[CasaSoft] Falha no endpoint {endpoint_name}")

    logging.info(f"[CasaSoft] Coleta finalizada em {time.time() - start_time:.2f}s")


def get_extraction_tasks():
    """
    Retorna as tasks de extração para o dag_unified_crm.

    Returns:
        list: Lista de configurações de tasks.
    """
    return [
        {
            "task_id": "run",
            "python_callable": run,
        }
    ]
