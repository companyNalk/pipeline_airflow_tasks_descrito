"""
iClin module for data extraction functions (legacy single-file style).
Agendamento de clínicas/saúde.

⚠️ iClin (iclin.com.br) é DIFERENTE de iClinic (iclinic.com.br).

Endpoints (classe/método, POST form-encoded):
    Dimensões (Agendas): listar_unid, listar_age, listar_conv
    Fatos (Atend): listar_atend_data (por dia), mostrar_atend, listar_serv_atend, mostrar_cli

Auth: headers `app-api-user` + `app-api-key` (token estático).
Datas: DD-MM-YYYY, varredura dia-a-dia (a API não aceita range).
Escopo: SOMENTE LEITURA.

Tabelas geradas (dataset `iclin`):
    unidades, agendas, convenios, atendimentos, atendimento_servicos, clientes
"""

import concurrent.futures
import logging
import time
from datetime import datetime, timedelta

import pandas as pd
import requests
from core import gcs

DEFAULT_BASE_URL = "https://iclin.com.br/web/inter"
DEFAULT_LOOKBACK_DAYS = 365
RATE_LIMIT_SLEEP = 0.5   # não documentado — pausa conservadora
MAX_DAYS_GUARD = 1100
MAX_WORKERS = 6

# Chaves candidatas onde o iClin pode entregar a lista (PHP -> JSON).
_LIST_KEYS = ("dados", "data", "result", "resultado", "retorno",
              "lista", "itens", "items", "registros", "rows")

# Chaves candidatas para ids (a doc usa nat/ncli/nage/cod_unid).
_ATEND_ID_KEYS = ("nat", "cod_atend", "id_atend", "id")
_CLI_ID_KEYS = ("ncli", "cod_cli", "id_cli", "id")
_UNID_ID_KEYS = ("cod_unid", "nunid", "id_unid", "id")
_AGE_ID_KEYS = ("nage", "cod_age", "id_age", "id")


def _get_headers(api_user, api_key):
    return {
        "app-api-user": api_user,
        "app-api-key": api_key,
    }


def _get_dates(lookback_days):
    """Lista de datas (dd-mm-yyyy) de hoje retrocedendo lookback_days dias."""
    lookback_days = min(int(lookback_days), MAX_DAYS_GUARD)
    end = datetime.now()
    return [(end - timedelta(days=i)).strftime("%d-%m-%Y") for i in range(lookback_days + 1)]


def _extract_items(payload):
    """Extrai a lista de itens de uma resposta iClin, de forma defensiva."""
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in _LIST_KEYS:
            value = payload.get(key)
            if isinstance(value, list):
                return value
            if isinstance(value, dict):
                return [value]
        if any(k in payload for k in (_ATEND_ID_KEYS + _CLI_ID_KEYS)):
            return [payload]
    return []


def _first_key(item, candidates):
    """Retorna o primeiro valor presente entre `candidates` (case-insensitive)."""
    if not isinstance(item, dict):
        return None
    lowered = {k.lower(): v for k, v in item.items()}
    for cand in candidates:
        if cand in item and item[cand] not in (None, ""):
            return item[cand]
        if cand in lowered and lowered[cand] not in (None, ""):
            return lowered[cand]
    return None


def _post(base_url, classe, metodo, headers, params=None):
    """POST form-encoded para {base_url}/{classe}/{metodo}, com retry simples de 429."""
    url = f"{base_url}/{classe}/{metodo}"
    for _ in range(4):
        response = requests.post(url, headers=headers, data=params or {}, timeout=60)
        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 2))
            logging.warning(f"[iClin] Rate limit (429) em {metodo}. Aguardando {retry_after}s...")
            time.sleep(retry_after)
            continue
        response.raise_for_status()
        try:
            return response.json()
        except ValueError:
            return response.text
    raise RuntimeError(f"[iClin] 429 persistente em {metodo}")


def _save_to_gcs(customer, data, file_name):
    """Salva lista de dicts como CSV no GCS."""
    if not data:
        logging.warning(f"[iClin] Sem dados para {file_name}, pulando upload.")
        return

    df = pd.DataFrame(data)
    local_path = f"/tmp/{customer['project_id']}.iclin.{file_name}.csv"
    df.to_csv(local_path, index=False)

    credentials = gcs.load_credentials_from_env()
    gcs.write_file_to_gcs(
        bucket_name=customer["bucket_name"],
        local_file_path=local_path,
        destination_name=f"{file_name}/{file_name}.csv",
        credentials=credentials,
    )
    logging.info(f"[iClin] {file_name}: {len(data)} registros salvos no GCS")


def _base_and_headers(customer):
    base_url = customer.get("api_base_url", DEFAULT_BASE_URL).rstrip('/')
    headers = _get_headers(customer["api_user"], customer["api_key"])
    return base_url, headers


# ----------------------------------------------------------------------------
# Coletas internas
# ----------------------------------------------------------------------------

def _fetch_unidades(base_url, headers):
    return _extract_items(_post(base_url, "Agendas", "listar_unid", headers))


def _fetch_agendas(base_url, headers, unidades):
    cod_unids = [_first_key(u, _UNID_ID_KEYS) for u in unidades]
    cod_unids = [c for c in cod_unids if c is not None] or [None]

    agendas = []
    for cod_unid in cod_unids:
        params = {"cod_unid": cod_unid} if cod_unid is not None else {}
        items = _extract_items(_post(base_url, "Agendas", "listar_age", headers, params))
        for it in items:
            if cod_unid is not None and isinstance(it, dict):
                it.setdefault("cod_unid", cod_unid)
        agendas.extend(items)
        time.sleep(RATE_LIMIT_SLEEP)
    return agendas


def _fetch_convenios(base_url, headers, agendas):
    nages = sorted({_first_key(a, _AGE_ID_KEYS) for a in agendas} - {None})

    def fetch_one(nage):
        try:
            items = _extract_items(_post(base_url, "Agendas", "listar_conv", headers, {"nage": nage}))
            for it in items:
                if isinstance(it, dict):
                    it.setdefault("nage", nage)
            return items
        except Exception as e:
            logging.error(f"[iClin] Erro convênios agenda {nage}: {e}")
            return []

    convenios = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for items in executor.map(fetch_one, nages):
            convenios.extend(items)
    return convenios


def _fetch_atendimentos_base(base_url, headers, dates):
    def fetch_day(data):
        try:
            items = _extract_items(_post(base_url, "Atend", "listar_atend_data", headers, {"data": data}))
            for it in items:
                if isinstance(it, dict):
                    it.setdefault("data_ref", data)
            return items
        except Exception as e:
            logging.error(f"[iClin] Erro atendimentos {data}: {e}")
            return []

    atendimentos = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for items in executor.map(fetch_day, dates):
            atendimentos.extend(items)
    logging.info(f"[iClin] atendimentos base: {len(atendimentos)} em {len(dates)} dias")
    return atendimentos


def _enrich_atendimentos(base_url, headers, atend_base):
    nats = sorted({_first_key(a, _ATEND_ID_KEYS) for a in atend_base} - {None})

    def fetch_one(nat):
        try:
            items = _extract_items(_post(base_url, "Atend", "mostrar_atend", headers, {"nat": nat}))
            return nat, (items[0] if items else {})
        except Exception as e:
            logging.error(f"[iClin] Erro detalhe atendimento {nat}: {e}")
            return nat, {}

    detail_by_nat = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for nat, detail in executor.map(fetch_one, nats):
            detail_by_nat[nat] = detail

    merged = []
    for a in atend_base:
        nat = _first_key(a, _ATEND_ID_KEYS)
        detail = detail_by_nat.get(nat, {})
        merged.append({**detail, **a} if isinstance(detail, dict) else dict(a))
    return merged


def _fetch_servicos(base_url, headers, atend_base):
    nats = sorted({_first_key(a, _ATEND_ID_KEYS) for a in atend_base} - {None})

    def fetch_one(nat):
        try:
            items = _extract_items(_post(base_url, "Atend", "listar_serv_atend", headers, {"nat": nat}))
            for it in items:
                if isinstance(it, dict):
                    it.setdefault("nat", nat)
            return items
        except Exception as e:
            logging.error(f"[iClin] Erro serviços atendimento {nat}: {e}")
            return []

    servicos = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for items in executor.map(fetch_one, nats):
            servicos.extend(items)
    return servicos


def _fetch_clientes(base_url, headers, atend_base):
    nclis = sorted({_first_key(a, _CLI_ID_KEYS) for a in atend_base} - {None})

    def fetch_one(ncli):
        try:
            return _extract_items(_post(base_url, "Atend", "mostrar_cli", headers, {"ncli": ncli}))
        except Exception as e:
            logging.error(f"[iClin] Erro cliente {ncli}: {e}")
            return []

    clientes = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for items in executor.map(fetch_one, nclis):
            clientes.extend(items)
    return clientes


# ----------------------------------------------------------------------------
# Tarefas de extração (chamadas pela DAG)
# ----------------------------------------------------------------------------

def extract_dimensoes(customer):
    """Extrai unidades, agendas e convênios (classe Agendas)."""
    start_time = time.time()
    base_url, headers = _base_and_headers(customer)
    logging.info(f"[iClin] Extraindo dimensões para {customer['project_id']}...")

    unidades = _fetch_unidades(base_url, headers)
    _save_to_gcs(customer, unidades, "unidades")

    agendas = _fetch_agendas(base_url, headers, unidades)
    _save_to_gcs(customer, agendas, "agendas")

    convenios = _fetch_convenios(base_url, headers, agendas)
    _save_to_gcs(customer, convenios, "convenios")

    logging.info(f"[iClin] Dimensões concluídas em {time.time() - start_time:.2f}s")


def extract_atendimentos(customer):
    """
    Varre a janela dia-a-dia (listar_atend_data) e, salvo fetch_details=False,
    enriquece com detalhe/serviços/cliente. Salva: atendimentos,
    atendimento_servicos, clientes.
    """
    start_time = time.time()
    base_url, headers = _base_and_headers(customer)
    dates = _get_dates(customer.get("lookback_days", DEFAULT_LOOKBACK_DAYS))
    fetch_details = customer.get("fetch_details", True)
    logging.info(f"[iClin] Extraindo atendimentos ({dates[-1]} a {dates[0]})...")

    atend_base = _fetch_atendimentos_base(base_url, headers, dates)

    if fetch_details and atend_base:
        atendimentos = _enrich_atendimentos(base_url, headers, atend_base)
        _save_to_gcs(customer, atendimentos, "atendimentos")
        _save_to_gcs(customer, _fetch_servicos(base_url, headers, atend_base), "atendimento_servicos")
        _save_to_gcs(customer, _fetch_clientes(base_url, headers, atend_base), "clientes")
    else:
        _save_to_gcs(customer, atend_base, "atendimentos")

    logging.info(f"[iClin] Atendimentos: {len(atend_base)} base em {time.time() - start_time:.2f}s")


def get_extraction_tasks():
    """
    Lista de tarefas de extração do iClin para a DAG.

    Returns:
        list: configurações de tarefa (task_id + python_callable)
    """
    return [
        {"task_id": "extract_dimensoes", "python_callable": extract_dimensoes},
        {"task_id": "extract_atendimentos", "python_callable": extract_atendimentos},
    ]
