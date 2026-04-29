"""
Sienge module for data extraction functions.
This module contains functions specific to the Sienge Plataforma integration.

Auth: HTTP Basic (api-user:api-password) — credenciais criadas no Painel de Integracoes.
Base URL: https://api.sienge.com.br/{tenant}/public/api/v1
Pagination: limit/offset (max 200) — itera ate retorno vazio.
Rate limit: 200 req/min na conta toda. Respeita 429/Retry-After.
Docs: https://api.sienge.com.br/docs/
"""

import logging
import time
import random

import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
from core import gcs

BASE_URL_TEMPLATE = "https://api.sienge.com.br/{tenant}/public/api/v1"
PAGE_LIMIT = 200  # max suportado por boa parte dos endpoints REST do Sienge
REQUEST_DELAY = 0.35  # ~170 rpm, abaixo do teto de 200 rpm
MAX_RETRIES = 6


def _get_auth(api_user, api_password):
    return HTTPBasicAuth(api_user, api_password)


def _request_with_retry(url, auth, params=None, timeout=60):
    """GET com retry para 429/5xx/timeout. Respeita Retry-After quando presente."""
    for attempt in range(MAX_RETRIES):
        try:
            time.sleep(REQUEST_DELAY)
            response = requests.get(url, auth=auth, params=params, timeout=timeout)

            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 30))
                logging.warning(f"[Sienge] 429 em {url}. Aguardando {retry_after}s...")
                time.sleep(retry_after)
                continue

            if 500 <= response.status_code < 600:
                delay = min(10 * (1.5 ** attempt), 120) * random.uniform(0.75, 1.25)
                logging.warning(
                    f"[Sienge] {response.status_code} em {url}. "
                    f"Tentativa {attempt + 1}/{MAX_RETRIES}. Aguardando {delay:.1f}s..."
                )
                time.sleep(delay)
                continue

            response.raise_for_status()
            return response
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
            delay = min(10 * (1.5 ** attempt), 120) * random.uniform(0.75, 1.25)
            logging.warning(
                f"[Sienge] Timeout/conexao: {e}. "
                f"Tentativa {attempt + 1}/{MAX_RETRIES}. Aguardando {delay:.1f}s..."
            )
            time.sleep(delay)
            continue
        except requests.exceptions.HTTPError as e:
            logging.error(f"[Sienge] HTTPError em {url}: {e}")
            raise

    raise RuntimeError(f"[Sienge] Falha apos {MAX_RETRIES} tentativas em {url}")


def _fetch_all_pages(base_url, endpoint, auth, params=None, results_key="results"):
    """
    Pagina via limit/offset ate retorno vazio.

    Sienge retorna no envelope { "resultSetMetadata": {...}, "results": [...] }
    para a maioria dos endpoints v1. results_key permite override quando necessario.
    """
    all_items = []
    offset = 0
    request_params = dict(params or {})
    request_params["limit"] = PAGE_LIMIT

    while True:
        request_params["offset"] = offset
        url = f"{base_url}/{endpoint}"

        response = _request_with_retry(url, auth, params=request_params)
        payload = response.json()
        items = payload.get(results_key, []) if isinstance(payload, dict) else payload

        if not items:
            break

        all_items.extend(items)
        logging.info(f"[Sienge] {endpoint}: +{len(items)} (total {len(all_items)})")

        if len(items) < PAGE_LIMIT:
            break
        offset += PAGE_LIMIT

    logging.info(f"[Sienge] {endpoint}: {len(all_items)} registros totais")
    return all_items


def _save_to_gcs(customer, data, file_name):
    """Salva lista de dicts como CSV no GCS."""
    if not data:
        logging.warning(f"[Sienge] Sem dados para {file_name}, pulando upload.")
        return

    df = pd.json_normalize(data, sep="_")
    local_path = f"/tmp/{customer['project_id']}.sienge.{file_name}.csv"
    df.to_csv(local_path, index=False)

    credentials = gcs.load_credentials_from_env()
    gcs.write_file_to_gcs(
        bucket_name=customer["bucket_name"],
        local_file_path=local_path,
        destination_name=f"{file_name}/{file_name}.csv",
        credentials=credentials,
    )
    logging.info(f"[Sienge] {file_name}: {len(data)} registros salvos no GCS")


def _extract_simple(customer, endpoint, file_name, params=None, results_key="results"):
    """Wrapper generico: configura auth, pagina, salva."""
    start_time = time.time()
    tenant = customer["tenant"]
    auth = _get_auth(customer["api_user"], customer["api_password"])
    base_url = BASE_URL_TEMPLATE.format(tenant=tenant)

    logging.info(f"[Sienge] Extraindo {endpoint} para {customer['project_id']}...")

    items = _fetch_all_pages(base_url, endpoint, auth, params=params, results_key=results_key)
    _save_to_gcs(customer, items, file_name)

    logging.info(
        f"[Sienge] {endpoint}: {len(items)} registros em {time.time() - start_time:.2f}s"
    )


# === CADASTROS ===

def extract_companies(customer):
    _extract_simple(customer, "companies", "companies")


def extract_customers(customer):
    _extract_simple(customer, "customers", "customers")


def extract_creditors(customer):
    _extract_simple(customer, "creditors", "creditors")


def extract_cost_centers(customer):
    _extract_simple(customer, "cost-centers", "cost_centers")


def extract_departments(customer):
    _extract_simple(customer, "departments", "departments")


# === OBRAS / PROJETOS ===

def extract_enterprises(customer):
    _extract_simple(customer, "enterprises", "enterprises")


def extract_units(customer):
    _extract_simple(customer, "units", "units")


def extract_building_projects(customer):
    _extract_simple(customer, "building-projects", "building_projects")


# === FINANCEIRO ===

def extract_accounts_receivable(customer):
    """Titulos a receber. Aceita filtros por data via customer['receivable_params']."""
    params = customer.get("receivable_params") or {}
    _extract_simple(
        customer,
        "accounts-receivable/receivable-bills",
        "accounts_receivable",
        params=params,
    )


def extract_accounts_payable(customer):
    """Titulos a pagar. Aceita filtros via customer['payable_params']."""
    params = customer.get("payable_params") or {}
    _extract_simple(
        customer,
        "bills",
        "accounts_payable",
        params=params,
    )


def extract_payment_categories(customer):
    _extract_simple(customer, "payment-categories", "payment_categories")


def extract_bank_movements(customer):
    """Movimentos bancarios. Requer startDate/endDate em customer['bank_params']."""
    params = customer.get("bank_params") or {}
    _extract_simple(
        customer,
        "movimentobancario",
        "bank_movements",
        params=params,
    )


# === CONTABIL ===

def extract_accounting_entries(customer):
    """Lancamentos contabeis. Requer startDate/endDate em customer['accounting_params']."""
    params = customer.get("accounting_params") or {}
    _extract_simple(
        customer,
        "accounting-entries",
        "accounting_entries",
        params=params,
    )


def get_extraction_tasks():
    """Tarefas para a DAG. Cadastros primeiro (rapidos), transacionais depois."""
    return [
        # cadastros
        {"task_id": "extract_companies",         "python_callable": extract_companies},
        {"task_id": "extract_customers",         "python_callable": extract_customers},
        {"task_id": "extract_creditors",         "python_callable": extract_creditors},
        {"task_id": "extract_cost_centers",      "python_callable": extract_cost_centers},
        {"task_id": "extract_departments",       "python_callable": extract_departments},
        # obras
        {"task_id": "extract_enterprises",       "python_callable": extract_enterprises},
        {"task_id": "extract_units",             "python_callable": extract_units},
        {"task_id": "extract_building_projects", "python_callable": extract_building_projects},
        # financeiro
        {"task_id": "extract_payment_categories","python_callable": extract_payment_categories},
        {"task_id": "extract_accounts_receivable","python_callable": extract_accounts_receivable},
        {"task_id": "extract_accounts_payable",  "python_callable": extract_accounts_payable},
        {"task_id": "extract_bank_movements",    "python_callable": extract_bank_movements},
        # contabil
        {"task_id": "extract_accounting_entries","python_callable": extract_accounting_entries},
    ]
