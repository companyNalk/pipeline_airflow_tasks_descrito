"""
Feegow module for data extraction functions (legacy single-file style).
Gestão de clínicas/saúde.

Endpoints:
    - Cadastros (sem filtro): profissionais, procedimentos, especialidades,
      convenios, unidades
    - Agendamentos: appoints/search fatiado por janela (≤90d/chamada; 409 = vazio)
    - Pacientes: patient/list paginado (limit/offset) — default; ou via agendamentos
    - Financeiro (OPCIONAL): contas, fornecedores, faturas — só se o módulo
      estiver habilitado na licença (senão 422; não falha a task)

Auth: token estático no header `x-access-token`
Datas: DD-MM-YYYY (janela calculada a partir de customer['lookback_days'])

⚠️ appoints/search rejeita janelas grandes (HTTP 409 a partir de ~90-180 dias) →
fatiado em APPOINTS_WINDOW_DAYS. patient/search exige paciente_id/cpf (não aceita
busca por data). Ver crm-integrations/feegow/ENDPOINTS.md para detalhes validados.
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
# appoints/search rejeita janelas grandes com 409 (90d OK, 180d 409) → fatiar.
APPOINTS_WINDOW_DAYS = 60
# patient/list pagina via limit/offset (offset = registros a pular; 500/página).
PATIENT_LIST_PAGE = 500

# Cadastros sem filtro obrigatório (1 chamada)
SIMPLE_ENDPOINTS = {
    "profissionais": "professional/list",
    "procedimentos": "procedures/list",
    "especialidades": "specialties/list",
    "convenios": "insurance/list",
    "unidades": "company/list-unity",
    # Dimensões dos agendamentos (id -> rótulo). Validados na licença 36514:
    # status -> {id, status} (3=Atendido, 6=Não compareceu, 11=Desmarcado pelo paciente);
    # motivos -> {id, motivo}. Essenciais p/ taxa de comparecimento/no-show confiável.
    "status": "appoints/status",
    "motivos": "appoints/motives",
}

# Módulo financeiro — OPCIONAL (paths conforme doc oficial docs.feegow.com).
# Se o token não tiver escopo financeiro: HTTP 422 com message:"" (sem permissão).
# Best-effort: não derruba a task. Populam quando o token tiver permissão financeira.
FINANCIAL_ENDPOINTS = {
    # Transacionais (janela de data)
    "financeiro_vendas": ("financial/sales-list", True),          # Listagem de Vendas (faturamento)
    "financeiro_contas": ("financial/list-accounts", True),       # Listar contas (a pagar/receber)
    "financeiro_repasses": ("financial/list-transfers", True),    # Repasses a profissionais
    "financeiro_vouchers": ("financial/vouchers", True),          # Listagem de vouchers
    # Dimensões (sem filtro de data)
    "financeiro_fornecedores": ("financial/list-providers", False),
    "financeiro_plano_contas": ("financial/chart-accounts", False),
    "financeiro_centros_custo": ("financial/cost-centers", False),
    "financeiro_conta_corrente": ("financial/current-accounts", False),
    "financeiro_tabelas_particulares": ("financial/private-tables", False),
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


def _get_date_chunks(lookback_days, window_days=APPOINTS_WINDOW_DAYS):
    """Fatia a janela em pedaços de até `window_days` dias (DD-MM-YYYY, inclusivos)."""
    end = datetime.now()
    start = end - timedelta(days=lookback_days)
    fmt = "%d-%m-%Y"
    chunks, cur = [], start
    while cur <= end:
        chunk_end = min(cur + timedelta(days=window_days - 1), end)
        chunks.append((cur.strftime(fmt), chunk_end.strftime(fmt)))
        cur = chunk_end + timedelta(days=1)
    return chunks


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


def _fetch_appointments(base_url, headers, lookback_days):
    """
    appoints/search fatiado por janela (≤APPOINTS_WINDOW_DAYS dias).
    HTTP 409 num pedaço = "sem agendamentos no período" → tratado como vazio.
    """
    all_items = []
    chunks = _get_date_chunks(lookback_days)
    for data_start, data_end in chunks:
        try:
            items = _fetch_all_pages(base_url, "appoints/search", headers,
                                     {"data_start": data_start, "data_end": data_end})
            all_items.extend(items)
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 409:
                logging.info(f"[Feegow] appoints {data_start}–{data_end}: sem agendamentos (409)")
                continue
            raise
    logging.info(f"[Feegow] Agendamentos: {len(all_items)} itens em {len(chunks)} janelas")
    return all_items


def extract_agendamentos(customer):
    """Extrai agendamentos na janela (fatiada; 409 = período vazio)."""
    start_time = time.time()
    base_url, headers = _base_and_headers(customer)
    lookback = customer.get("lookback_days", DEFAULT_LOOKBACK_DAYS)
    logging.info(f"[Feegow] Extraindo agendamentos (lookback {lookback}d) para {customer['project_id']}...")

    data = _fetch_appointments(base_url, headers, lookback)
    _save_to_gcs(customer, data, "agendamentos")
    logging.info(f"[Feegow] Agendamentos: {len(data)} registros em {time.time() - start_time:.2f}s")


def extract_financeiro(customer):
    """
    Módulo financeiro (OPCIONAL): contas, fornecedores e faturas.

    Na licença testada (36514) a Feegow responde HTTP 422 (módulo não habilitado).
    Best-effort: cada endpoint é tentado isoladamente; falhas só logam aviso e
    NÃO interrompem a task.
    """
    start_time = time.time()
    base_url, headers = _base_and_headers(customer)
    data_start, data_end = _get_date_window(customer.get("lookback_days", DEFAULT_LOOKBACK_DAYS))

    for table, (endpoint, needs_date) in FINANCIAL_ENDPOINTS.items():
        try:
            if needs_date:
                data = _fetch_all_pages(base_url, endpoint, headers,
                                        {"data_start": data_start, "data_end": data_end})
            else:
                data = _fetch_single(base_url, endpoint, headers)
            _save_to_gcs(customer, data, table)
        except Exception as e:
            logging.warning(f"[Feegow] {table}: módulo financeiro indisponível/sem dados — "
                            f"pulando (opcional). Detalhe: {e}")

    logging.info(f"[Feegow] Financeiro (opcional) concluído em {time.time() - start_time:.2f}s")


def _fetch_patients_list(base_url, headers):
    """patient/list paginado via limit/offset (offset = registros a pular; 500/pág)."""
    all_items, offset, page = [], 0, 0
    while page < MAX_PAGES_GUARD:
        items = _extract_items(_get(base_url, "patient/list", headers,
                                    {"limit": PATIENT_LIST_PAGE, "offset": offset}))
        page += 1
        if not items:
            break
        all_items.extend(items)
        if len(items) < PATIENT_LIST_PAGE:
            break
        offset += PATIENT_LIST_PAGE
        time.sleep(RATE_LIMIT_SLEEP)
    return all_items


def extract_pacientes(customer):
    """
    Extrai pacientes. Estratégia via customer['patient_strategy']:
      - 'list' (default): patient/list paginado (limit/offset). Pega TODOS os
        pacientes em poucas chamadas, sem rate limit. Campos enxutos.
      - 'appointments': deriva paciente_id dos agendamentos e busca 1 a 1
        (patient/search — campos ricos, mas lento + rate limit).
    patient/search exige paciente_id/cpf → busca só por data ('date') não funciona.
    """
    start_time = time.time()
    base_url, headers = _base_and_headers(customer)
    strategy = (customer.get("patient_strategy") or "list").lower()

    if strategy == "appointments":
        logging.info(f"[Feegow] Pacientes via agendamentos para {customer['project_id']}...")
        appoints = _fetch_appointments(base_url, headers,
                                       customer.get("lookback_days", DEFAULT_LOOKBACK_DAYS))
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
        logging.info(f"[Feegow] Pacientes via patient/list para {customer['project_id']}...")
        data = _fetch_patients_list(base_url, headers)

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
        {"task_id": "extract_financeiro", "python_callable": extract_financeiro},
        {"task_id": "extract_pacientes", "python_callable": extract_pacientes},
    ]
