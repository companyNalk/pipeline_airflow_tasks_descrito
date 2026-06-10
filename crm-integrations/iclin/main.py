"""
Extractor iClin (agendamento de clínicas) -> BigQuery.

⚠️ iClin (iclin.com.br) é DIFERENTE de iClinic (iclinic.com.br). Esta integração
é do **iClin** — sistema de agendamento online para clínicas.

Doc da API: https://iclin.com.br/web/inter/apicli_spo.php
  - Autenticação: headers `app-api-user` + `app-api-key` (token estático, sem OAuth).
  - Método: POST exclusivamente, corpo form-encoded (PHP $_POST), UTF-8.
  - Duas classes, cada uma com base URL própria:
      Agendas -> {BASE}/Agendas/{metodo}
      Atend   -> {BASE}/Atend/{metodo}

Escopo: SOMENTE LEITURA (não usamos ocupar_hr/ocupar_paol).

Domínios extraídos (-> dataset `iclin`):
  Dimensões (Agendas):
    - unidades   : listar_unid
    - agendas    : listar_age (por unidade)
    - convenios  : listar_conv (por agenda / nage)
  Fatos (Atend), varrendo dia-a-dia a janela LOOKBACK_DAYS:
    - atendimentos        : listar_atend_data(data) + detalhe mostrar_atend(nat)
    - atendimento_servicos: listar_serv_atend(nat)
    - clientes            : mostrar_cli(ncli) (ncli derivados dos atendimentos)

A classe `Atend.listar_atend_data` aceita filtro por DATA (dd-mm-yyyy) — é o que
viabiliza extração para BI. A classe `Agendas` só lista bookings por CPF, logo
não serve para varredura histórica e não é extraída aqui.

⚠️ Pendências a validar com credenciais reais (ver ENDPOINTS.md):
  - formato exato da resposta (JSON? chave da lista?) — `_extract_items` é defensivo
  - nomes reais dos campos id (nat / ncli / nage / cod_unid) — `_first_key` é defensivo
  - rate limit (não documentado) — começamos conservadores
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

# Rate limit não documentado pelo iClin — começamos conservadores.
RATE_LIMIT = 90
MAX_WORKERS = min(6, os.cpu_count() or 4)
MAX_DAYS_GUARD = 1100  # ~3 anos: trava contra LOOKBACK_DAYS absurdo

DEFAULT_BASE_URL = "https://iclin.com.br/web/inter"

# Chaves candidatas onde o iClin pode entregar a lista de itens (PHP -> JSON).
_LIST_KEYS = ("dados", "data", "result", "resultado", "retorno",
              "lista", "itens", "items", "registros", "rows")

# Chaves candidatas para ids (a doc usa nat/ncli/nage/cod_unid).
_ATEND_ID_KEYS = ("nat", "cod_atend", "id_atend", "id")
_CLI_ID_KEYS = ("ncli", "cod_cli", "id_cli", "id")
_UNID_ID_KEYS = ("cod_unid", "nunid", "id_unid", "id")
_AGE_ID_KEYS = ("nage", "cod_age", "id_age", "id")


def get_arguments():
    """Configura e retorna os argumentos da linha de comando / env."""
    return (ArgumentManager("Extractor da API iClin")
            .add("API_BASE_URL", "URL base da API", required=False, default=DEFAULT_BASE_URL)
            .add("API_USER", "Credencial app-api-user", required=True)
            .add("API_KEY", "Credencial app-api-key", required=True)
            .add("LOOKBACK_DAYS", "Janela de dias para atendimentos", required=False,
                 default=365, arg_type=int)
            .add("FETCH_DETAILS", "Buscar detalhe/serviços/cliente por atendimento (1/0)",
                 required=False, default=1, arg_type=int)
            .add("PROJECT_ID", "ID do projeto Google Cloud", required=True)
            .add("CRM_TYPE", "Nome da ferramenta (dataset destino)", required=True)
            .add("GOOGLE_APPLICATION_CREDENTIALS", "Credencial GCS", required=True)
            .parse())


def get_auth_headers(api_user, api_key):
    """Headers de autenticação para a API iClin."""
    logger.info("🔑 Preparando headers de autenticação (app-api-user / app-api-key)")
    return {
        "app-api-user": api_user,
        "app-api-key": api_key,
    }


def get_date_range(lookback_days):
    """Lista de datas (dd-mm-yyyy) de hoje retrocedendo LOOKBACK_DAYS dias."""
    lookback_days = min(int(lookback_days), MAX_DAYS_GUARD)
    end = datetime.now()
    dates = [(end - timedelta(days=i)).strftime("%d-%m-%Y") for i in range(lookback_days + 1)]
    logger.info(f"📅 Janela: {dates[-1]} a {dates[0]} ({len(dates)} dias)")
    return dates


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
        # Resposta é um dict de campos (1 registro) sem envelope conhecido.
        # Heurística: se parece um registro (tem ids), trata como item único.
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


def post(http_client, classe, metodo, headers, params=None):
    """POST form-encoded para {BASE}/{classe}/{metodo}."""
    path = f"{classe}/{metodo}"
    return http_client.post(path, data=params or {}, headers=headers, debug_info=metodo)


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


# ---------------------------------------------------------------------------
# Dimensões (classe Agendas)
# ---------------------------------------------------------------------------

def fetch_unidades(http_client, headers):
    """listar_unid — todas as unidades da clínica."""
    return _extract_items(post(http_client, "Agendas", "listar_unid", headers))


def fetch_agendas(http_client, headers, unidades):
    """listar_age por unidade (cai para chamada única se não houver unidades)."""
    cod_unids = [_first_key(u, _UNID_ID_KEYS) for u in unidades]
    cod_unids = [c for c in cod_unids if c is not None] or [None]

    agendas = []
    for cod_unid in cod_unids:
        params = {"cod_unid": cod_unid} if cod_unid is not None else {}
        items = _extract_items(post(http_client, "Agendas", "listar_age", headers, params))
        for it in items:
            if cod_unid is not None and isinstance(it, dict):
                it.setdefault("cod_unid", cod_unid)
        agendas.extend(items)
    return agendas


def fetch_convenios(http_client, headers, agendas):
    """listar_conv por agenda (nage* obrigatório)."""
    nages = sorted({_first_key(a, _AGE_ID_KEYS) for a in agendas} - {None})
    logger.info(f"🏷️ {len(nages)} agendas (nage) para buscar convênios")

    convenios = []

    def fetch_one(nage):
        try:
            items = _extract_items(post(http_client, "Agendas", "listar_conv",
                                        headers, {"nage": nage}))
            for it in items:
                if isinstance(it, dict):
                    it.setdefault("nage", nage)
            return items
        except Exception as e:
            logger.error(f"❌ Erro ao buscar convênios da agenda {nage}: {e}")
            return []

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for items in executor.map(fetch_one, nages):
            convenios.extend(items)
    return convenios


# ---------------------------------------------------------------------------
# Fatos (classe Atend)
# ---------------------------------------------------------------------------

def fetch_atendimentos_base(http_client, headers, dates):
    """listar_atend_data para cada dia da janela; concatena os atendimentos."""
    atendimentos = []

    def fetch_day(data):
        try:
            items = _extract_items(post(http_client, "Atend", "listar_atend_data",
                                        headers, {"data": data}))
            for it in items:
                if isinstance(it, dict):
                    it.setdefault("data_ref", data)
            return items
        except Exception as e:
            logger.error(f"❌ Erro ao listar atendimentos de {data}: {e}")
            return []

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for idx, items in enumerate(executor.map(fetch_day, dates), 1):
            atendimentos.extend(items)
            if idx % 60 == 0:
                logger.info(f"📄 {idx}/{len(dates)} dias varridos (acum: {len(atendimentos)})")
    logger.info(f"✅ atendimentos base: {len(atendimentos)} em {len(dates)} dias")
    return atendimentos


def enrich_atendimentos(http_client, headers, atendimentos):
    """mostrar_atend(nat) — detalhe por atendimento, mesclado no registro base."""
    nats = sorted({_first_key(a, _ATEND_ID_KEYS) for a in atendimentos} - {None})
    logger.info(f"🔎 {len(nats)} atendimentos (nat) para detalhar")

    detail_by_nat = {}

    def fetch_one(nat):
        try:
            items = _extract_items(post(http_client, "Atend", "mostrar_atend",
                                        headers, {"nat": nat}))
            return nat, (items[0] if items else {})
        except Exception as e:
            logger.error(f"❌ Erro ao detalhar atendimento {nat}: {e}")
            return nat, {}

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for nat, detail in executor.map(fetch_one, nats):
            detail_by_nat[nat] = detail

    merged = []
    for a in atendimentos:
        nat = _first_key(a, _ATEND_ID_KEYS)
        detail = detail_by_nat.get(nat, {})
        rec = {**detail, **a} if isinstance(detail, dict) else dict(a)
        merged.append(rec)
    return merged


def fetch_servicos(http_client, headers, atendimentos):
    """listar_serv_atend(nat) — serviços por atendimento (1 linha por serviço)."""
    nats = sorted({_first_key(a, _ATEND_ID_KEYS) for a in atendimentos} - {None})

    servicos = []

    def fetch_one(nat):
        try:
            items = _extract_items(post(http_client, "Atend", "listar_serv_atend",
                                        headers, {"nat": nat}))
            for it in items:
                if isinstance(it, dict):
                    it.setdefault("nat", nat)
            return items
        except Exception as e:
            logger.error(f"❌ Erro ao buscar serviços do atendimento {nat}: {e}")
            return []

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for items in executor.map(fetch_one, nats):
            servicos.extend(items)
    return servicos


def fetch_clientes(http_client, headers, atendimentos):
    """mostrar_cli(ncli) — clientes únicos derivados dos atendimentos."""
    nclis = sorted({_first_key(a, _CLI_ID_KEYS) for a in atendimentos} - {None})
    logger.info(f"👥 {len(nclis)} clientes (ncli) únicos derivados dos atendimentos")

    clientes = []

    def fetch_one(ncli):
        try:
            items = _extract_items(post(http_client, "Atend", "mostrar_cli",
                                        headers, {"ncli": ncli}))
            return items
        except Exception as e:
            logger.error(f"❌ Erro ao buscar cliente {ncli}: {e}")
            return []

    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for items in executor.map(fetch_one, nclis):
            clientes.extend(items)
    return clientes


def main():
    """Função principal para coleta de dados."""
    global_start_time = ReportGenerator.init_report(logger)

    try:
        # 1. Configurações
        args = get_arguments()
        api_base_url = args.API_BASE_URL.rstrip('/')
        dates = get_date_range(args.LOOKBACK_DAYS)
        fetch_details = bool(int(args.FETCH_DETAILS))

        # 2. Cliente HTTP + rate limiter + headers
        rate_limiter = RateLimiter(requests_per_window=RATE_LIMIT, logger=logger)
        http_client = HttpClient(base_url=api_base_url, rate_limiter=rate_limiter, logger=logger)
        headers = get_auth_headers(args.API_USER, args.API_KEY)

        endpoint_stats = {}

        # 3. Dimensões (Agendas)
        unidades, endpoint_stats["unidades"] = process_endpoint(
            "unidades", lambda: fetch_unidades(http_client, headers))
        agendas, endpoint_stats["agendas"] = process_endpoint(
            "agendas", lambda: fetch_agendas(http_client, headers, unidades))
        _, endpoint_stats["convenios"] = process_endpoint(
            "convenios", lambda: fetch_convenios(http_client, headers, agendas))

        # 4. Fatos (Atend) — varredura por data
        atend_base = fetch_atendimentos_base(http_client, headers, dates)

        if fetch_details and atend_base:
            atendimentos, endpoint_stats["atendimentos"] = process_endpoint(
                "atendimentos", lambda: enrich_atendimentos(http_client, headers, atend_base))
            _, endpoint_stats["atendimento_servicos"] = process_endpoint(
                "atendimento_servicos", lambda: fetch_servicos(http_client, headers, atend_base))
            _, endpoint_stats["clientes"] = process_endpoint(
                "clientes", lambda: fetch_clientes(http_client, headers, atend_base))
        else:
            # Sem detalhamento: grava só a lista base dos atendimentos.
            _, endpoint_stats["atendimentos"] = process_endpoint(
                "atendimentos", lambda: atend_base)

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
