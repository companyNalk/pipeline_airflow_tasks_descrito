"""
Feegow module for data extraction functions (legacy single-file style).
Gestão de clínicas/saúde.

Endpoints:
    - Cadastros (sem filtro): profissionais, procedimentos, especialidades,
      convenios, unidades, financeiro_contas, financeiro_fornecedores
    - Por janela de data: agendamentos, financeiro_faturas
    - pacientes (estratégia 'date' ou 'appointments')

Auth: token estático no header `x-access-token`
Paginação: start/offset (default 0/50)
Datas: DD-MM-YYYY (janela calculada a partir de customer['lookback_days'])
"""

import concurrent.futures
import logging
import time
from datetime import datetime, timedelta

import pandas as pd
import requests
from core import gcs

DEFAULT_BASE_URL = "https://api.feegow.com/v1/api"
PAGE_SIZE = 50           # 'offset' default da Feegow = tamanho da página
DEFAULT_LOOKBACK_DAYS = 365
RATE_LIMIT_SLEEP = 0.5   # não documentado — pausa conservadora entre páginas
MAX_PAGES_GUARD = 5000
MAX_WORKERS = 8

# Cadastros sem filtro obrigatório (1 chamada)
SIMPLE_ENDPOINTS = {
    "profissionais": "professional/list",
    "procedimentos": "procedures/list",
    "especialidades": "specialties/list",
    "convenios": "insurance/list",
    "unidades": "company/list-unity",
    "financeiro_contas": "financial/accounts",
    "financeiro_fornecedores": "financial/suppliers",
}

# Endpoints com janela de data (paginados)
DATE_ENDPOINTS = {
    "agendamentos": "appoints/search",
    "financeiro_faturas": "financial/invoice",
}

_LIST_KEYS = ("content", "data", "itens", "items", "registros")


def _get_headers(access_token):
    return {
        "x-access-token": access_token,
        "Content-Type": "application/json",
    }


def _get_date_window(lookback_days):
    """Retorna (data_start, data_end) no formato DD-MM-YYYY."""
    end = datetime.now()
    start = end - timedelta(days=lookback_days)
    fmt = "%d-%m-%Y"
    return start.strftime(fmt), end.strftime(fmt)


def _extract_items(payload):
    """Extrai a lista de itens de uma resposta Feegow, de forma defensiva."""
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in _LIST_KEYS:
            value = payload.get(key)
            if isinstance(value, list):
                return value
            if isinstance(value, dict):
                return [value]
    return []


def _item_signature(item):
    if isinstance(item, dict):
        for key in ("id", "paciente_id", "agendamento_id", "profissional_id"):
            if key in item:
                return f"{key}:{item[key]}"
    return str(item)[:80]


def _get(base_url, endpoint, headers, params):
    """GET único com tratamento simples de 429."""
    url = f"{base_url}/{endpoint}"
    for _ in range(4):
        response = requests.get(url, headers=headers, params=params, timeout=60)
        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 2))
            logging.warning(f"[Feegow] Rate limit (429). Aguardando {retry_after}s...")
            time.sleep(retry_after)
            continue
        response.raise_for_status()
        return response.json()
    raise RuntimeError(f"[Feegow] 429 persistente em {endpoint}")


def _fetch_single(base_url, endpoint, headers, params=None):
    """1 chamada sem paginação (cadastros simples)."""
    return _extract_items(_get(base_url, endpoint, headers, params or {}))


def _fetch_all_pages(base_url, endpoint, headers, extra_params=None):
    """
    Pagina via start/offset até esgotar.
    Para quando uma página retorna menos que PAGE_SIZE. Guarda contra paginação
    que não avança (1º item repetido) e loop infinito (MAX_PAGES_GUARD).
    """
    all_items = []
    start = 0
    page = 0
    seen_first = set()

    while page < MAX_PAGES_GUARD:
        params = {"start": start, "offset": PAGE_SIZE}
        if extra_params:
            params.update(extra_params)

        items = _extract_items(_get(base_url, endpoint, headers, params))
        page += 1

        if not items:
            break

        sig = _item_signature(items[0])
        if sig in seen_first:
            logging.warning(f"[Feegow] {endpoint}: paginação não avança. Usando 1ª página.")
            break
        seen_first.add(sig)

        all_items.extend(items)
        if len(items) < PAGE_SIZE:
            break
        start += PAGE_SIZE
        time.sleep(RATE_LIMIT_SLEEP)

    return all_items


def _save_to_gcs(customer, data, file_name):
    """Salva lista de dicts como CSV no GCS."""
    if not data:
        logging.warning(f"[Feegow] Sem dados para {file_name}, pulando upload.")
        return

    df = pd.DataFrame(data)
    local_path = f"/tmp/{customer['project_id']}.feegow.{file_name}.csv"
    df.to_csv(local_path, index=False)

    credentials = gcs.load_credentials_from_env()
    gcs.write_file_to_gcs(
        bucket_name=customer["bucket_name"],
        local_file_path=local_path,
        destination_name=f"{file_name}/{file_name}.csv",
        credentials=credentials,
    )
    logging.info(f"[Feegow] {file_name}: {len(data)} registros salvos no GCS")


def _base_and_headers(customer):
    base_url = customer.get("api_base_url", DEFAULT_BASE_URL).rstrip('/')
    headers = _get_headers(customer["access_token"])
    return base_url, headers


# ----------------------------------------------------------------------------
# Tarefas de extração (chamadas pela DAG)
# ----------------------------------------------------------------------------

def extract_cadastros(customer):
    """Extrai os cadastros simples (sem filtro de data)."""
    start_time = time.time()
    base_url, headers = _base_and_headers(customer)
    logging.info(f"[Feegow] Extraindo cadastros para {customer['project_id']}...")

    for table, endpoint in SIMPLE_ENDPOINTS.items():
        data = _fetch_single(base_url, endpoint, headers)
        _save_to_gcs(customer, data, table)

    logging.info(f"[Feegow] Cadastros concluídos em {time.time() - start_time:.2f}s")


def extract_agendamentos(customer):
    """Extrai agendamentos na janela de data."""
    start_time = time.time()
    base_url, headers = _base_and_headers(customer)
    data_start, data_end = _get_date_window(customer.get("lookback_days", DEFAULT_LOOKBACK_DAYS))
    logging.info(f"[Feegow] Extraindo agendamentos ({data_start} a {data_end})...")

    data = _fetch_all_pages(base_url, "appoints/search", headers,
                            {"data_start": data_start, "data_end": data_end})
    _save_to_gcs(customer, data, "agendamentos")
    logging.info(f"[Feegow] Agendamentos: {len(data)} registros em {time.time() - start_time:.2f}s")


def extract_financeiro_faturas(customer):
    """Extrai faturas financeiras na janela de data."""
    start_time = time.time()
    base_url, headers = _base_and_headers(customer)
    data_start, data_end = _get_date_window(customer.get("lookback_days", DEFAULT_LOOKBACK_DAYS))
    logging.info(f"[Feegow] Extraindo faturas ({data_start} a {data_end})...")

    data = _fetch_all_pages(base_url, "financial/invoice", headers,
                            {"data_start": data_start, "data_end": data_end})
    _save_to_gcs(customer, data, "financeiro_faturas")
    logging.info(f"[Feegow] Faturas: {len(data)} registros em {time.time() - start_time:.2f}s")


def extract_pacientes(customer):
    """
    Extrai pacientes. Estratégia via customer['patient_strategy']:
      - 'date' (default): /patient/search por janela de data
      - 'appointments': deriva paciente_id dos agendamentos e busca 1 a 1
    """
    start_time = time.time()
    base_url, headers = _base_and_headers(customer)
    strategy = (customer.get("patient_strategy") or "date").lower()
    lookback = customer.get("lookback_days", DEFAULT_LOOKBACK_DAYS)
    data_start, data_end = _get_date_window(lookback)

    if strategy == "appointments":
        logging.info(f"[Feegow] Pacientes via agendamentos para {customer['project_id']}...")
        appoints = _fetch_all_pages(base_url, "appoints/search", headers,
                                    {"data_start": data_start, "data_end": data_end})
        patient_ids = sorted({a.get("paciente_id") for a in appoints if a.get("paciente_id")})
        logging.info(f"[Feegow] {len(patient_ids)} paciente_id únicos derivados")

        def fetch_one(pid):
            try:
                return _extract_items(_get(base_url, "patient/search", headers, {"paciente_id": pid}))
            except Exception as e:
                logging.error(f"[Feegow] Erro paciente {pid}: {e}")
                return []

        data = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            for items in executor.map(fetch_one, patient_ids):
                data.extend(items)
    else:
        logging.info(f"[Feegow] Pacientes por data ({data_start} a {data_end})...")
        data = _fetch_all_pages(base_url, "patient/search", headers,
                                {"data_start": data_start, "data_end": data_end})

    _save_to_gcs(customer, data, "pacientes")
    logging.info(f"[Feegow] Pacientes: {len(data)} registros em {time.time() - start_time:.2f}s")


def get_extraction_tasks():
    """
    Lista de tarefas de extração da Feegow para a DAG.

    Returns:
        list: configurações de tarefa (task_id + python_callable)
    """
    return [
        {"task_id": "extract_cadastros", "python_callable": extract_cadastros},
        {"task_id": "extract_agendamentos", "python_callable": extract_agendamentos},
        {"task_id": "extract_financeiro_faturas", "python_callable": extract_financeiro_faturas},
        {"task_id": "extract_pacientes", "python_callable": extract_pacientes},
    ]
