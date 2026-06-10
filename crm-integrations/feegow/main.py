"""
Extractor Feegow (gestão de clínicas) API v1 -> BigQuery.

Autenticação: token estático no header `x-access-token` (sem OAuth).
Segue o padrão do workspace (commons/ + generic/), igual ao asaas.

Domínios extraídos:
  - Cadastros de apoio (sem filtro): profissionais, procedimentos,
    especialidades, convênios, unidades, contas e fornecedores financeiros.
  - Por janela de data (paginados): agendamentos, faturas financeiras, pacientes.

Janela de data: calculada a cada run a partir de LOOKBACK_DAYS, no formato
DD-MM-YYYY exigido pela Feegow.

⚠️ Estratégia de pacientes (PATIENT_STRATEGY):
  - "date" (default): busca /patient/search por data_start/data_end.
    A doc lista data como OPCIONAL e exige "pelo menos 1 filtro" — se a API
    rejeitar busca só por data, troque para "appointments".
  - "appointments": deriva os paciente_id dos agendamentos extraídos e busca
    paciente a paciente (endpoint dependente). Cobre quem tem agenda.
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

DEFAULT_BASE_URL = "https://api.feegow.com/v1/api"

# Cadastros e listagens sem filtro obrigatório (1 chamada, sem paginação por data).
SIMPLE_ENDPOINTS = {
    "profissionais": "professional/list",
    "procedimentos": "procedures/list",
    "especialidades": "specialties/list",
    "convenios": "insurance/list",
    "unidades": "company/list-unity",
    "financeiro_contas": "financial/accounts",
    "financeiro_fornecedores": "financial/suppliers",
}

# Endpoints que exigem janela de data (data_start/data_end) e são paginados.
DATE_ENDPOINTS = {
    "agendamentos": "appoints/search",
    "financeiro_faturas": "financial/invoice",
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
            .add("PATIENT_STRATEGY", "Estratégia de pacientes: date | appointments",
                 required=False, default="date")
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


def fetch_patients_by_date(http_client, headers, data_start, data_end):
    """Estratégia A: pacientes por janela de data."""
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

        # 4. Endpoints por janela de data (paginados)
        appointments = []
        for name, path in DATE_ENDPOINTS.items():
            data, stats = process_endpoint(
                name, lambda p=path: fetch_all_pages(http_client, p, headers, date_params)
            )
            endpoint_stats[name] = stats
            if name == "agendamentos":
                appointments = data

        # 5. Pacientes (estratégia configurável)
        strategy = (args.PATIENT_STRATEGY or "date").lower()
        if strategy == "appointments":
            _, stats = process_endpoint(
                "pacientes",
                lambda: fetch_patients_from_appointments(http_client, headers, appointments)
            )
        else:
            _, stats = process_endpoint(
                "pacientes",
                lambda: fetch_patients_by_date(http_client, headers, data_start, data_end)
            )
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
