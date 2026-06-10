"""
Extractor Feegow (gestão de clínicas) API v1 -> BigQuery.

Autenticação: token estático no header `x-access-token` (sem OAuth).
Segue o padrão do workspace (commons/ + generic/), igual ao asaas.

Domínios extraídos:
  - Cadastros de apoio (sem filtro): profissionais, procedimentos,
    especialidades, convênios, unidades.
  - Agendamentos: appoints/search fatiado por janela (≤90d/chamada; ver abaixo).
  - Pacientes: derivados dos agendamentos (patient/search exige paciente_id/cpf).
  - Financeiro (OPCIONAL): contas, fornecedores, faturas — só populam se o módulo
    financeiro estiver habilitado na licença (senão a API responde 422; não falha).

Janela de data: calculada a cada run a partir de LOOKBACK_DAYS, no formato
DD-MM-YYYY exigido pela Feegow.

⚠️ appoints/search rejeita janelas grandes com HTTP 409 (validado: 90d OK,
180d 409). Fatiamos em pedaços de APPOINTS_WINDOW_DAYS. 409 num pedaço =
"sem agendamentos no período" (tratado como vazio, não como erro).

⚠️ Estratégia de pacientes (PATIENT_STRATEGY):
  - "list" (default): patient/list paginado por limit/offset (500/página). Pega
    TODOS os pacientes (inclusive sem agenda) em poucas chamadas, sem rate limit.
    Campos enxutos (sem documento/endereço completo).
  - "appointments": deriva paciente_id dos agendamentos e busca 1 a 1 via
    patient/search (campos ricos, mas 1 request/paciente → lento + rate limit).
  - "date": NÃO funciona — patient/search exige paciente_id/cpf (HTTP 422). Compat.
"""

import concurrent.futures
import os
import time
from datetime import datetime, timedelta

from commons.app_inicializer import AppInitializer
from commons.big_query import BigQuery
from commons.memory_monitor import MemoryMonitor
from commons.report_generator import ReportGenerator
from commons.utils import Utils
from generic.argument_manager import ArgumentManager
from generic.http_client import HttpClient
from generic.rate_limiter import RateLimiter

logger = AppInitializer.initialize()

# Rate limit não documentado pela Feegow — começamos conservador.
RATE_LIMIT = 120
PAGE_SIZE = 50  # 'offset' default da Feegow = tamanho da página
MAX_WORKERS = min(8, os.cpu_count() or 5)
MAX_PAGES_GUARD = 5000  # trava de segurança contra paginação infinita

# appoints/search rejeita janelas grandes com HTTP 409 (validado: 90d OK, 180d 409).
# Fatiamos a janela em pedaços com folga sob o limite real (entre 90 e 180 dias).
APPOINTS_WINDOW_DAYS = 60

# patient/list pagina via limit/offset (offset = registros a pular; 500 por página).
# Pega TODOS os pacientes (inclusive sem agenda) em ~N/500 chamadas — muito melhor
# que 1 request por paciente (que estoura o rate limit do Feegow).
PATIENT_LIST_PAGE = 500

DEFAULT_BASE_URL = "https://api.feegow.com/v1/api"

# Cadastros e listagens sem filtro obrigatório (1 chamada, sem paginação por data).
SIMPLE_ENDPOINTS = {
    "profissionais": "professional/list",
    "procedimentos": "procedures/list",
    "especialidades": "specialties/list",
    "convenios": "insurance/list",
    "unidades": "company/list-unity",
}

# Endpoints do módulo financeiro. OPCIONAIS: na licença testada (36514) retornam
# HTTP 422 com mensagem vazia em qualquer variação de parâmetro/caminho — assinatura
# de módulo financeiro não habilitado. Não derrubam o run (process_optional_endpoint).
# Se a clínica contratar o módulo financeiro e o token tiver escopo, voltam a popular.
FINANCIAL_ENDPOINTS = {
    "financeiro_contas": ("financial/accounts", False),       # (path, exige_data)
    "financeiro_fornecedores": ("financial/suppliers", False),
    "financeiro_faturas": ("financial/invoice", True),
}

# Chaves onde a Feegow costuma entregar a lista de itens.
_LIST_KEYS = ("content", "data", "itens", "items", "registros")


def get_arguments():
    """Configura e retorna os argumentos da linha de comando / env."""
    return (ArgumentManager("Extractor da API Feegow")
            .add("API_BASE_URL", "URL base da API", required=False, default=DEFAULT_BASE_URL)
            .add("API_ACCESS_TOKEN", "Token estático (x-access-token)", required=True)
            .add("LOOKBACK_DAYS", "Janela de dias para dados por data", required=False,
                 default=365, arg_type=int)
            .add("PATIENT_STRATEGY", "Estratégia de pacientes: list | appointments | date",
                 required=False, default="list")
            .add("PROJECT_ID", "ID do projeto Google Cloud", required=True)
            .add("CRM_TYPE", "Nome da ferramenta (dataset destino)", required=True)
            .add("GOOGLE_APPLICATION_CREDENTIALS", "Credencial GCS", required=True)
            .parse())


def get_auth_headers(access_token):
    """Headers de autenticação para a API Feegow."""
    logger.info("🔑 Preparando headers de autenticação")
    return {
        "x-access-token": access_token,
        "Content-Type": "application/json",
    }


def get_date_window(lookback_days):
    """Retorna (data_start, data_end) no formato DD-MM-YYYY exigido pela Feegow."""
    end = datetime.now()
    start = end - timedelta(days=lookback_days)
    fmt = "%d-%m-%Y"
    data_start, data_end = start.strftime(fmt), end.strftime(fmt)
    logger.info(f"📅 Janela de data: {data_start} a {data_end} ({lookback_days} dias)")
    return data_start, data_end


def get_date_chunks(lookback_days, window_days=APPOINTS_WINDOW_DAYS):
    """Fatia a janela em pedaços de até `window_days` dias (DD-MM-YYYY, inclusivos)."""
    end = datetime.now()
    start = end - timedelta(days=lookback_days)
    fmt = "%d-%m-%Y"
    chunks = []
    cur = start
    while cur <= end:
        chunk_end = min(cur + timedelta(days=window_days - 1), end)
        chunks.append((cur.strftime(fmt), chunk_end.strftime(fmt)))
        cur = chunk_end + timedelta(days=1)
    logger.info(f"📅 Janela de {lookback_days} dias fatiada em {len(chunks)} pedaços de ≤{window_days}d")
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
            # Alguns endpoints (ex.: unidades) retornam content como dict único
            if isinstance(value, dict):
                return [value]
    return []


def _item_signature(item):
    """Assinatura de um item para detectar paginação que não avança."""
    if isinstance(item, dict):
        for key in ("id", "paciente_id", "agendamento_id", "profissional_id"):
            if key in item:
                return f"{key}:{item[key]}"
    return str(item)[:80]


def fetch_all_pages(http_client, endpoint, headers, extra_params=None):
    """
    Pagina via start/offset até esgotar.

    Feegow: `offset` = tamanho da página, `start` = registro inicial.
    Para na primeira página com menos que PAGE_SIZE itens. Guarda contra
    endpoints que ignoram a paginação (mesmo primeiro item repetido) e contra
    loops infinitos (MAX_PAGES_GUARD).
    """
    logger.info(f"📚 Buscando todas as páginas para: {endpoint}")
    start_time = time.time()

    all_items = []
    start = 0
    page = 0
    seen_first = set()

    while page < MAX_PAGES_GUARD:
        params = {"start": start, "offset": PAGE_SIZE}
        if extra_params:
            params.update(extra_params)

        payload = http_client.get(endpoint, headers=headers, params=params,
                                  debug_info=f"{endpoint}:s{start}")
        items = _extract_items(payload)
        page += 1

        if not items:
            break

        # Detecta paginação que não avança (endpoint ignora start/offset)
        sig = _item_signature(items[0])
        if sig in seen_first:
            logger.warning(f"⚠️ {endpoint}: paginação não avança (1ª página repetida). "
                           f"Assumindo retorno completo na 1ª página.")
            break
        seen_first.add(sig)

        all_items.extend(items)

        if page == 1 or len(items) < PAGE_SIZE or page % 20 == 0:
            logger.info(f"📄 {endpoint}: página {page} com {len(items)} itens (acum: {len(all_items)})")

        if len(items) < PAGE_SIZE:
            break
        start += PAGE_SIZE

    duration = time.time() - start_time
    logger.info(f"✅ {endpoint}: {len(all_items)} itens em {duration:.2f}s ({page} páginas)")
    return all_items


def fetch_single(http_client, endpoint, headers, params=None):
    """1 chamada sem paginação (cadastros simples)."""
    logger.info(f"🔍 Buscando: {endpoint}")
    payload = http_client.get(endpoint, headers=headers, params=params or {}, debug_info=endpoint)
    return _extract_items(payload)


def fetch_appointments(http_client, headers, lookback_days):
    """
    appoints/search fatiado por janela. Concatena todos os pedaços.

    A Feegow retorna HTTP 409 ("Agendamento não existe") quando um período não
    tem agendamentos — tratamos como pedaço vazio, não como erro.
    """
    all_items = []
    chunks = get_date_chunks(lookback_days)
    for data_start, data_end in chunks:
        try:
            items = fetch_all_pages(http_client, "appoints/search", headers,
                                    {"data_start": data_start, "data_end": data_end})
            all_items.extend(items)
        except Exception as e:
            if "409" in str(e):
                logger.info(f"📭 appoints {data_start}–{data_end}: sem agendamentos (409)")
                continue
            raise
    logger.info(f"✅ agendamentos: {len(all_items)} itens em {len(chunks)} janelas")
    return all_items


def process_endpoint(name, fetch_fn):
    """Executa a coleta de um endpoint e devolve (dados, stats)."""
    try:
        logger.info(f"\n{'=' * 50}\n🔍 PROCESSANDO: {name.upper()}\n{'=' * 50}")
        endpoint_start = time.time()

        raw_data = fetch_fn()

        logger.info(f"💾 Processando e salvando {len(raw_data)} registros para {name}")
        processed = Utils.process_and_save_data(raw_data, name)

        duration = time.time() - endpoint_start
        stats = {"registros": len(processed), "status": "Sucesso", "tempo": duration}
        logger.info(f"✅ {name}: {len(processed)} registros em {duration:.2f}s")
        return processed, stats

    except Exception as e:
        logger.exception(f"❌ Falha no endpoint {name}")
        return [], {"registros": 0, "status": f"Falha: {type(e).__name__}: {str(e)}", "tempo": 0}


def process_optional_endpoint(name, fetch_fn):
    """
    Como process_endpoint, mas NÃO derruba o run em falha esperada.

    Usado nos endpoints do módulo financeiro: quando o módulo não está habilitado
    na licença, a Feegow responde HTTP 422 com mensagem vazia. Nesse caso logamos
    um aviso e marcamos status sem a palavra "Falha" (não conta como erro no resumo).
    """
    data, stats = process_endpoint(name, fetch_fn)
    if "Falha" in stats["status"]:
        logger.warning(f"⚠️ {name}: módulo financeiro indisponível/sem dados — pulando (opcional). "
                       f"Detalhe original: {stats['status']}")
        stats["status"] = "Opcional: financeiro indisponível (HTTP 422)"
    return data, stats


def fetch_patients_list(http_client, headers):
    """
    Estratégia LIST (default): patient/list paginado via limit/offset.

    `offset` = registros a pular (NÃO é igual ao start/offset dos outros endpoints);
    página de PATIENT_LIST_PAGE (500). Pega TODOS os pacientes da clínica em ~N/500
    chamadas — inclusive os sem agendamento — sem estourar o rate limit.
    Campos mais enxutos que patient/search (sem documento/endereço completo).
    """
    all_items = []
    offset = 0
    page = 0
    while page < MAX_PAGES_GUARD:
        payload = http_client.get("patient/list", headers=headers,
                                  params={"limit": PATIENT_LIST_PAGE, "offset": offset},
                                  debug_info=f"list:o{offset}")
        items = _extract_items(payload)
        page += 1
        if not items:
            break
        all_items.extend(items)
        logger.info(f"📄 patient/list: offset {offset} +{len(items)} (acum: {len(all_items)})")
        if len(items) < PATIENT_LIST_PAGE:
            break
        offset += PATIENT_LIST_PAGE
    logger.info(f"✅ pacientes (list): {len(all_items)} em {page} páginas")
    return all_items


def fetch_patients_by_date(http_client, headers, data_start, data_end):
    """Estratégia legada: pacientes por janela de data (NÃO funciona — patient/search
    exige paciente_id/cpf; mantida só por compat)."""
    return fetch_all_pages(http_client, "patient/search", headers,
                           extra_params={"data_start": data_start, "data_end": data_end})


def fetch_patients_from_appointments(http_client, headers, appointments):
    """Estratégia B: deriva paciente_id dos agendamentos e busca 1 a 1."""
    patient_ids = sorted({a.get("paciente_id") for a in appointments if a.get("paciente_id")})
    logger.info(f"👥 {len(patient_ids)} paciente_id únicos derivados dos agendamentos")
    if not patient_ids:
        return []

    results = []

    def fetch_one(pid):
        try:
            payload = http_client.get("patient/search", headers=headers,
                                      params={"paciente_id": pid}, debug_info=f"pac:{pid}")
            return _extract_items(payload)
        except Exception as e:
            logger.error(f"❌ Erro ao buscar paciente {pid}: {e}")
            return []

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for items in executor.map(fetch_one, patient_ids):
            results.extend(items)

    return results


def main():
    """Função principal para coleta de dados."""
    global_start_time = ReportGenerator.init_report(logger)

    try:
        # 1. Configurações
        args = get_arguments()
        api_base_url = args.API_BASE_URL.rstrip('/')
        data_start, data_end = get_date_window(args.LOOKBACK_DAYS)
        date_params = {"data_start": data_start, "data_end": data_end}

        # 2. Cliente HTTP + rate limiter + headers
        rate_limiter = RateLimiter(requests_per_window=RATE_LIMIT, logger=logger)
        http_client = HttpClient(base_url=api_base_url, rate_limiter=rate_limiter, logger=logger)
        headers = get_auth_headers(args.API_ACCESS_TOKEN)

        endpoint_stats = {}

        # 3. Cadastros simples (sem filtro)
        for name, path in SIMPLE_ENDPOINTS.items():
            _, stats = process_endpoint(name, lambda p=path: fetch_single(http_client, p, headers))
            endpoint_stats[name] = stats

        # 4. Agendamentos (janela fatiada; 409 = período vazio)
        appointments, endpoint_stats["agendamentos"] = process_endpoint(
            "agendamentos", lambda: fetch_appointments(http_client, headers, args.LOOKBACK_DAYS)
        )

        # 4b. Módulo financeiro (OPCIONAL — não derruba o run se indisponível)
        for name, (path, needs_date) in FINANCIAL_ENDPOINTS.items():
            params = date_params if needs_date else None
            _, stats = process_optional_endpoint(
                name, lambda p=path, pa=params: fetch_all_pages(http_client, p, headers, pa)
                if pa else fetch_single(http_client, p, headers)
            )
            endpoint_stats[name] = stats

        # 5. Pacientes (estratégia configurável; default = list via patient/list,
        #    que pega todos os pacientes em ~N/500 chamadas sem estourar rate limit)
        strategy = (args.PATIENT_STRATEGY or "list").lower()
        if strategy == "list":
            _, stats = process_endpoint(
                "pacientes", lambda: fetch_patients_list(http_client, headers))
        elif strategy == "appointments":
            _, stats = process_endpoint(
                "pacientes",
                lambda: fetch_patients_from_appointments(http_client, headers, appointments))
        else:
            _, stats = process_endpoint(
                "pacientes",
                lambda: fetch_patients_by_date(http_client, headers, data_start, data_end))
        endpoint_stats["pacientes"] = stats

        # 6. Resumo
        success = ReportGenerator.final_summary(logger, endpoint_stats, global_start_time)

        # 7. Pipeline BigQuery
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
