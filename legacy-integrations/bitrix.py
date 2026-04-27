"""
Bitrix module for data extraction functions.

Extracao das entidades CRM via REST -> CSV no GCS.
Tabelas externas em BQ e tabelas gold sao geridas fora deste modulo
(via scheduled queries no BigQuery).

Cada extrator tem dois modos:
  - mode='full'        -> sem filtro, paginacao ASC ate esgotar (uso 1x para backfill)
  - mode='incremental' -> >DATE_MODIFY ultimos N dias (default 7)
"""
import json
import os
import pathlib
import re
import time
import unicodedata
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import requests
from google.cloud import storage


# =====================================================================
# CONFIG
# =====================================================================
HEADER = {
    'Content-Type': 'application/json',
    'Accept': 'application/json',
}

PAGE_SLEEP = 0.2  # ~5 req/s, dentro do limite do Bitrix
BITRIX_TZ_OFFSET = '-03:00'


# =====================================================================
# HELPERS
# =====================================================================

def _setup_credentials():
    path = pathlib.Path('config', 'gcp.json').as_posix()
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = path


class BitrixPaginationError(Exception):
    """Raised quando a paginacao nao consegue concluir (rate limit persistente, rede, etc).
    Marca a task como FAILED no Airflow para evitar upload silencioso de dados parciais."""
    pass


def _make_request(url_base, method, params=None, retries=8, fatal_on_fail=False):
    """POST com retry exponencial e tratamento dedicado para 429.

    - 429: respeita Retry-After header se vier; senao backoff 60/120/240/... segundos
    - Outros erros: backoff 5/10/20/40/80 segundos
    - retries default 8 (era 3) para tolerar bursts de rate limit
    - fatal_on_fail=True: levanta excecao em vez de retornar None (usado em paginacao)
    """
    last_err = None
    for attempt in range(retries):
        try:
            r = requests.post(
                f"{url_base}/{method}.json",
                headers=HEADER,
                data=json.dumps(params or {}),
                timeout=190,
            )
            if r.status_code == 429:
                # Rate limit. Respeita Retry-After, senao backoff longo (60s, 120s, 240s, ...)
                retry_after = r.headers.get('Retry-After')
                if retry_after and retry_after.isdigit():
                    wait = max(int(retry_after), 30)
                else:
                    wait = min(60 * (2 ** attempt), 600)  # cap em 10 min
                print(f"  [{method}] 429 rate limit (attempt {attempt+1}/{retries}). Aguardando {wait}s...")
                time.sleep(wait)
                last_err = '429 rate limit'
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            if attempt < retries - 1:
                wait = min(5 * (2 ** attempt), 300)  # 5,10,20,40,80,160,300...
                print(f"  [{method}] erro (attempt {attempt+1}/{retries}): {str(e)[:100]} - aguardando {wait}s")
                time.sleep(wait)
    msg = f"[ERRO] {method} apos {retries} tentativas: {last_err}"
    print(msg)
    if fatal_on_fail:
        raise BitrixPaginationError(msg)
    return None


def _paginate_list(url_base, method, mode='incremental', days=7,
                   extra_filter=None, select=None, sleep=PAGE_SLEEP):
    """Pagina crm.{entity}.list ate esgotar.

    mode='full'        -> sem filtro de data
    mode='incremental' -> filter >DATE_MODIFY (ou >LAST_UPDATED em activities)

    LEVANTA BitrixPaginationError se nao conseguir concluir a paginacao.
    Isso e proposital: melhor falhar a task no Airflow do que sobrescrever
    o CSV no GCS com dados parciais (que apareceriam como sucesso).
    """
    filter_ = dict(extra_filter or {})
    if mode == 'incremental':
        since = (date.today() - timedelta(days=days)).strftime(f'%Y-%m-%dT00:00:00{BITRIX_TZ_OFFSET}')
        date_field = 'LAST_UPDATED' if method == 'crm.activity.list' else 'DATE_MODIFY'
        filter_[f'>{date_field}'] = since
    select_ = select or ['*', 'UF_*']

    all_items = []
    start = 0
    pages = 0
    total = None
    while True:
        params = {'select': select_, 'order': {'ID': 'ASC'}, 'start': start}
        if filter_:
            params['filter'] = filter_
        # fatal_on_fail=True: se esgotar retries, levanta excecao em vez de "fim silencioso"
        res = _make_request(url_base, method, params, fatal_on_fail=True)
        if 'result' not in res:
            raise BitrixPaginationError(
                f"[{method}] resposta sem 'result' na pagina {pages+1}: {str(res)[:300]}"
            )
        chunk = res['result']
        if not chunk:
            break
        if total is None:
            total = res.get('total')
            print(f"  [{method}] total reportado: {total} (mode={mode})")
        all_items.extend(chunk)
        pages += 1
        if pages % 20 == 0:
            print(f"  [{method}] pagina {pages}, acumulado {len(all_items)}/{total}")
        nxt = res.get('next')
        if nxt is None:
            break
        start = nxt
        time.sleep(sleep)
    print(f"  [{method}] fim: {pages} paginas, {len(all_items)} registros")
    return all_items


def _build_field_maps(url_base, fields_method):
    """code -> label + mapa de enumerations por label."""
    res = _make_request(url_base, fields_method)
    fields_map, enumeration_map = {}, {}
    if not res or 'result' not in res:
        print(f"  [{fields_method}] falha ao obter campos")
        return fields_map, enumeration_map
    for code, meta in res['result'].items():
        name = meta.get('formLabel') or meta.get('listLabel') or meta.get('title') or code
        fields_map[code] = name
        if meta.get('type') == 'enumeration' and 'items' in meta:
            enumeration_map[name] = {str(i['ID']): i['VALUE'] for i in meta['items']}
    return fields_map, enumeration_map


def _build_users_map(url_base):
    """ID (str) -> 'Nome Sobrenome' apenas para usuarios ativos."""
    res = _make_request(url_base, 'user.get', {'FILTER': {'ACTIVE': 'true'}})
    if not res or 'result' not in res:
        return {}
    return {
        str(u['ID']): f"{u.get('NAME', '')} {u.get('LAST_NAME', '')}".strip()
        for u in res['result']
    }


def _normalize_column_name(col):
    """Remove acentos, troca nao-alfanum por _, colapsa underscores. Estabiliza schema."""
    n = unicodedata.normalize('NFKD', str(col)).encode('ASCII', 'ignore').decode('ASCII')
    n = re.sub(r'[^a-zA-Z0-9_]', '_', n)
    n = re.sub(r'_+', '_', n).strip('_')
    return n or 'col'


def _normalize_and_dedup(df):
    """Normaliza nomes e dedupla com sufixo _N em colisoes (intencionais ou de acento)."""
    seen = {}
    new_cols = []
    for col in df.columns:
        n = _normalize_column_name(col)
        if n in seen:
            seen[n] += 1
            new_cols.append(f"{n}_{seen[n]}")
        else:
            seen[n] = 0
            new_cols.append(n)
    df.columns = new_cols
    return df


def _translate_enums(df, enumeration_map):
    """Substitui IDs por VALUE em campos enum. Aplicado ANTES da normalizacao de colunas."""
    for col, enum in enumeration_map.items():
        if col in df.columns:
            df[col] = df[col].astype(str).replace(enum)
    return df


def _translate_users(df, users_map, fields):
    for f in fields:
        if f in df.columns:
            df[f] = df[f].astype(str).replace(users_map)
    return df


def _upload_to_gcs(df, customer, gcs_path, sep=';'):
    if df is None or df.empty:
        print(f"  [upload] DataFrame vazio: {gcs_path} (skip)")
        return False
    csv_data = df.to_csv(sep=sep, index=False, quoting=1)
    storage_client = storage.Client(project=customer['project_id'])
    bucket = storage_client.bucket(customer['bucket_name'])
    blob = bucket.blob(gcs_path)
    blob.upload_from_string(csv_data, content_type='text/csv')
    print(f"  [upload] gs://{customer['bucket_name']}/{gcs_path}  ({len(df)} linhas, {len(df.columns)} cols)")
    return True


def _gcs_path(name, mode):
    suffix = '_full' if mode == 'full' else ''
    return f"{name}{suffix}.csv"


# =====================================================================
# CHECKPOINT + CHUNKED UPLOAD (para full backfill)
# =====================================================================

def _checkpoint_path(base_name):
    return f"_checkpoints/{base_name}_cursor.json"


def _load_checkpoint(customer, base_name):
    """Le cursor JSON do GCS. Retorna dict ou None se nao existir."""
    storage_client = storage.Client(project=customer['project_id'])
    bucket = storage_client.bucket(customer['bucket_name'])
    blob = bucket.blob(_checkpoint_path(base_name))
    if not blob.exists():
        return None
    try:
        return json.loads(blob.download_as_text())
    except Exception as e:
        print(f"  [checkpoint] erro lendo {base_name}: {e}; tratando como inexistente")
        return None


def _save_checkpoint(customer, base_name, data):
    """Sobrescreve cursor JSON no GCS."""
    storage_client = storage.Client(project=customer['project_id'])
    bucket = storage_client.bucket(customer['bucket_name'])
    blob = bucket.blob(_checkpoint_path(base_name))
    blob.upload_from_string(json.dumps(data, indent=2), content_type='application/json')


def _delete_checkpoint(customer, base_name):
    """Remove cursor (apos backfill completo)."""
    storage_client = storage.Client(project=customer['project_id'])
    bucket = storage_client.bucket(customer['bucket_name'])
    blob = bucket.blob(_checkpoint_path(base_name))
    if blob.exists():
        blob.delete()


def _upload_chunk_csv(df, customer, base_name, chunk_idx):
    """Salva CSV chunk numerado: <base_name>_chunk_NNNN.csv"""
    path = f"{base_name}_chunk_{chunk_idx:04d}.csv"
    _upload_to_gcs(df, customer, path)
    return path


def _process_items_to_df(items, fields_map=None, enumeration_map=None,
                         users_map=None, user_translate_fields=None,
                         post_translate=None, url_base=None):
    """Pipeline de transformacao items -> df normalizado.
    Extraido de _extract_crm_list para reuso no chunked."""
    df = pd.DataFrame(items)
    if fields_map:
        df.rename(columns=fields_map, inplace=True)
    if enumeration_map:
        df = _translate_enums(df, enumeration_map)
    if users_map:
        user_fields = user_translate_fields or [
            'Pessoa responsável', 'Criado por', 'Modificado por',
            'Pessoa responsavel', 'Modified by', 'Created by',
        ]
        df = _translate_users(df, users_map, user_fields)
    if post_translate and url_base:
        df = post_translate(df, url_base)
    df = _normalize_and_dedup(df)
    return df


def _extract_crm_list_full(customer, method, fields_method, output_name,
                           post_translate=None, user_translate_fields=None,
                           chunk_pages=100):
    """Pipeline FULL com checkpoint + chunked saves.

    Salva CSVs como <output_name>_full_chunk_NNNN.csv para que a external
    table aponte para glob (gs://.../<output_name>_full_chunk_*.csv).

    Cursor em _checkpoints/<output_name>_full_cursor.json permite RETOMAR
    de onde parou se a task for interrompida (timeout Docker, OOM, rate
    limit esgotado). Cada chunk = chunk_pages requests = ~5000 linhas
    (50/pagina default do Bitrix).

    Comportamento:
      - Sem checkpoint: comeca do zero (start=0, chunk_idx=0).
      - Com checkpoint nao-completo: retoma de start salvo.
      - Com checkpoint completed=True: apaga e recomeca (proxima rodada
        de backfill substitui completamente o conjunto anterior).
    """
    _setup_credentials()
    url_base = customer['url_base']
    base_name = f"{output_name}_full"

    print(f"[{base_name}] PASSO 1 - campos personalizados ({fields_method})")
    fields_map, enumeration_map = _build_field_maps(url_base, fields_method)
    print(f"  -> {len(fields_map)} campos, {len(enumeration_map)} enums")

    print(f"[{base_name}] PASSO 2 - usuarios")
    users_map = _build_users_map(url_base)
    print(f"  -> {len(users_map)} usuarios")

    # Checkpoint
    ckpt = _load_checkpoint(customer, base_name)
    if ckpt and ckpt.get('completed'):
        print(f"[{base_name}] checkpoint anterior marcado como completed em "
              f"{ckpt.get('last_update_utc', '?')}. Apagando e recomecando do zero.")
        _delete_checkpoint(customer, base_name)
        ckpt = None

    start = ckpt.get('start', 0) if ckpt else 0
    chunk_idx = ckpt.get('next_chunk_idx', 0) if ckpt else 0
    pages_done = ckpt.get('pages_done', 0) if ckpt else 0
    total_uploaded = ckpt.get('total_uploaded', 0) if ckpt else 0

    if ckpt:
        print(f"[{base_name}] RETOMANDO: start={start}, chunk_idx={chunk_idx}, "
              f"pages_done={pages_done}, ja_uploaded={total_uploaded}")
    else:
        print(f"[{base_name}] inicio limpo (sem checkpoint)")

    print(f"[{base_name}] PASSO 3 - paginar {method} (mode=full, chunk_pages={chunk_pages})")

    buffer_items = []
    pages_in_chunk = 0
    total_reported = ckpt.get('total_reported') if ckpt else None

    while True:
        params = {'select': ['*', 'UF_*'], 'order': {'ID': 'ASC'}, 'start': start}
        # fatal_on_fail=True garante raise em vez de retornar None silenciosamente
        res = _make_request(url_base, method, params, fatal_on_fail=True)
        if 'result' not in res:
            raise BitrixPaginationError(
                f"[{method}] resposta sem 'result' na pagina {pages_done+1}: {str(res)[:300]}"
            )
        chunk = res['result']
        if not chunk:
            # Sem mais dados (esgotou). Flush do buffer abaixo.
            end_of_data = True
        else:
            end_of_data = False
            if total_reported is None:
                total_reported = res.get('total')
                print(f"  [{method}] total reportado: {total_reported}")

            buffer_items.extend(chunk)
            pages_in_chunk += 1
            pages_done += 1

        nxt = res.get('next') if not end_of_data else None
        if nxt is None:
            end_of_data = True

        # Flush condicional: chunk completo OU fim dos dados
        should_flush = (pages_in_chunk >= chunk_pages or end_of_data) and buffer_items
        if should_flush:
            df = _process_items_to_df(
                buffer_items, fields_map, enumeration_map, users_map,
                user_translate_fields, post_translate, url_base
            )
            _upload_chunk_csv(df, customer, base_name, chunk_idx)
            total_uploaded += len(df)
            print(f"  [{base_name}] chunk {chunk_idx:04d} salvo "
                  f"({len(df)} linhas; acumulado total: {total_uploaded}/{total_reported})")
            chunk_idx += 1
            buffer_items = []
            pages_in_chunk = 0

            # Salva cursor APOS upload bem sucedido
            _save_checkpoint(customer, base_name, {
                'start': nxt,  # None se end_of_data, senao proximo cursor
                'next_chunk_idx': chunk_idx,
                'pages_done': pages_done,
                'total_uploaded': total_uploaded,
                'total_reported': total_reported,
                'completed': end_of_data,
                'last_update_utc': datetime.now(timezone.utc).isoformat(),
            })

        if end_of_data:
            break
        start = nxt
        time.sleep(PAGE_SLEEP)

    print(f"[{base_name}] OK: {pages_done} paginas, {total_uploaded} linhas em {chunk_idx} chunks")
    return {
        'rows': total_uploaded,
        'pages': pages_done,
        'chunks': chunk_idx,
        'total_reported': total_reported,
        'gcs_pattern': f"{base_name}_chunk_*.csv",
    }


def _extract_activities_full(customer, chunk_pages=100):
    """Versao full chunked para crm.activity.list (sem .fields, com user translate)."""
    _setup_credentials()
    url_base = customer['url_base']
    base_name = 'bitrix_crm_activities_full'
    method = 'crm.activity.list'

    users_map = _build_users_map(url_base)

    ckpt = _load_checkpoint(customer, base_name)
    if ckpt and ckpt.get('completed'):
        print(f"[{base_name}] checkpoint completed. Apagando e recomecando.")
        _delete_checkpoint(customer, base_name)
        ckpt = None

    start = ckpt.get('start', 0) if ckpt else 0
    chunk_idx = ckpt.get('next_chunk_idx', 0) if ckpt else 0
    pages_done = ckpt.get('pages_done', 0) if ckpt else 0
    total_uploaded = ckpt.get('total_uploaded', 0) if ckpt else 0

    if ckpt:
        print(f"[{base_name}] RETOMANDO: start={start}, chunk_idx={chunk_idx}, "
              f"pages_done={pages_done}, ja_uploaded={total_uploaded}")
    else:
        print(f"[{base_name}] inicio limpo")

    print(f"[{base_name}] paginar {method} (mode=full, chunk_pages={chunk_pages})")

    buffer_items = []
    pages_in_chunk = 0
    total_reported = ckpt.get('total_reported') if ckpt else None

    while True:
        params = {'select': ['*', 'UF_*'], 'order': {'ID': 'ASC'}, 'start': start}
        res = _make_request(url_base, method, params, fatal_on_fail=True)
        if 'result' not in res:
            raise BitrixPaginationError(
                f"[{method}] resposta sem 'result' na pagina {pages_done+1}: {str(res)[:300]}"
            )
        chunk = res['result']
        end_of_data = not chunk
        if not end_of_data:
            if total_reported is None:
                total_reported = res.get('total')
                print(f"  [{method}] total reportado: {total_reported}")
            buffer_items.extend(chunk)
            pages_in_chunk += 1
            pages_done += 1

        nxt = res.get('next') if not end_of_data else None
        if nxt is None:
            end_of_data = True

        should_flush = (pages_in_chunk >= chunk_pages or end_of_data) and buffer_items
        if should_flush:
            df = pd.DataFrame(buffer_items)
            df = _translate_users(df, users_map,
                                  ['RESPONSIBLE_ID', 'AUTHOR_ID', 'EDITOR_ID'])
            df = _normalize_and_dedup(df)
            _upload_chunk_csv(df, customer, base_name, chunk_idx)
            total_uploaded += len(df)
            print(f"  [{base_name}] chunk {chunk_idx:04d} salvo "
                  f"({len(df)} linhas; acumulado: {total_uploaded}/{total_reported})")
            chunk_idx += 1
            buffer_items = []
            pages_in_chunk = 0

            _save_checkpoint(customer, base_name, {
                'start': nxt,
                'next_chunk_idx': chunk_idx,
                'pages_done': pages_done,
                'total_uploaded': total_uploaded,
                'total_reported': total_reported,
                'completed': end_of_data,
                'last_update_utc': datetime.now(timezone.utc).isoformat(),
            })

        if end_of_data:
            break
        start = nxt
        time.sleep(PAGE_SLEEP)

    print(f"[{base_name}] OK: {pages_done} paginas, {total_uploaded} linhas em {chunk_idx} chunks")
    return {
        'rows': total_uploaded,
        'pages': pages_done,
        'chunks': chunk_idx,
        'total_reported': total_reported,
        'gcs_pattern': f"{base_name}_chunk_*.csv",
    }


# =====================================================================
# CRM ENTITIES (deal/lead/contact/company seguem mesmo padrao)
# =====================================================================

def _extract_crm_list(customer, method, fields_method, output_name,
                     mode='incremental', days=7,
                     post_translate=None, user_translate_fields=None):
    """Pipeline padrao para entidades crm.*.list com fields/enums/usuarios."""
    _setup_credentials()
    url_base = customer['url_base']

    print(f"[{output_name}] PASSO 1 - campos personalizados")
    fields_map, enumeration_map = _build_field_maps(url_base, fields_method)
    print(f"  -> {len(fields_map)} campos, {len(enumeration_map)} enums")

    print(f"[{output_name}] PASSO 2 - usuarios")
    users_map = _build_users_map(url_base)
    print(f"  -> {len(users_map)} usuarios")

    print(f"[{output_name}] PASSO 3 - paginar {method} (mode={mode})")
    items = _paginate_list(url_base, method, mode=mode, days=days)
    if not items:
        print(f"  [{output_name}] sem registros, abort")
        return None

    print(f"[{output_name}] PASSO 4 - processar")
    df = pd.DataFrame(items)
    # rename codigos -> labels (formLabel) ANTES das traducoes (enums e users sao por label)
    df.rename(columns=fields_map, inplace=True)
    df = _translate_enums(df, enumeration_map)
    user_fields = user_translate_fields or [
        'Pessoa responsável', 'Criado por', 'Modificado por',
        'Pessoa responsavel', 'Modified by', 'Created by',
    ]
    df = _translate_users(df, users_map, user_fields)
    if post_translate:
        df = post_translate(df, url_base)
    df = _normalize_and_dedup(df)

    print(f"[{output_name}] PASSO 5 - upload GCS")
    gcs_path = _gcs_path(output_name, mode)
    _upload_to_gcs(df, customer, gcs_path)
    print(f"[{output_name}] OK: {len(df)} linhas, {len(df.columns)} cols")
    return {'rows': len(df), 'cols': len(df.columns), 'gcs_path': gcs_path}


def _translate_deal_stage(df, url_base):
    """Traduz STATUS_ID da fase para nome legivel (multi-funil)."""
    stages_map = {}
    res = _make_request(url_base, 'crm.dealcategory.list', {'order': {'SORT': 'ASC'}})
    if res and 'result' in res:
        for cat in res['result']:
            rs = _make_request(url_base, 'crm.dealcategory.stage.list', {'id': cat['ID']})
            if rs and 'result' in rs:
                for s in rs['result']:
                    stages_map[s['STATUS_ID']] = s['NAME']
    for col in ['Fase do negócio', 'Fase do negocio']:
        if col in df.columns and stages_map:
            df[col] = df[col].astype(str).replace(stages_map)
    return df


def _translate_lead_status(df, url_base):
    """Traduz STATUS_ID do lead para nome legivel."""
    status_map = {}
    res = _make_request(url_base, 'crm.status.list', {
        'order': {'SORT': 'ASC'}, 'filter': {'ENTITY_ID': 'STATUS'},
    })
    if res and 'result' in res:
        for s in res['result']:
            status_map[s['STATUS_ID']] = s['NAME']
    for col in ['Etapa', 'Status']:
        if col in df.columns and status_map:
            df[col] = df[col].astype(str).replace(status_map)
    return df


def run_get_leads(customer, mode='incremental', days=7):
    return _extract_crm_list(
        customer, 'crm.lead.list', 'crm.lead.fields', 'bitrix_crm_leads',
        mode=mode, days=days, post_translate=_translate_lead_status,
    )


def run_get_deals(customer, mode='incremental', days=7):
    return _extract_crm_list(
        customer, 'crm.deal.list', 'crm.deal.fields', 'bitrix_crm_deals',
        mode=mode, days=days, post_translate=_translate_deal_stage,
    )


def run_get_contacts(customer, mode='incremental', days=7):
    return _extract_crm_list(
        customer, 'crm.contact.list', 'crm.contact.fields', 'bitrix_crm_contacts',
        mode=mode, days=days,
    )


def run_get_company(customer, mode='incremental', days=7):
    return _extract_crm_list(
        customer, 'crm.company.list', 'crm.company.fields', 'bitrix_crm_company',
        mode=mode, days=days,
    )


def run_get_activities(customer, mode='incremental', days=7):
    """crm.activity.list nao tem .fields; campos sao standard."""
    _setup_credentials()
    url_base = customer['url_base']
    print(f"[bitrix_crm_activities] paginando crm.activity.list (mode={mode})")
    items = _paginate_list(url_base, 'crm.activity.list', mode=mode, days=days)
    if not items:
        return None
    df = pd.DataFrame(items)
    users_map = _build_users_map(url_base)
    df = _translate_users(df, users_map, ['RESPONSIBLE_ID', 'AUTHOR_ID', 'EDITOR_ID'])
    df = _normalize_and_dedup(df)
    gcs_path = _gcs_path('bitrix_crm_activities', mode)
    _upload_to_gcs(df, customer, gcs_path)
    print(f"[bitrix_crm_activities] OK: {len(df)} linhas")
    return {'rows': len(df), 'cols': len(df.columns), 'gcs_path': gcs_path}


# =====================================================================
# CATALOGOS (sempre full snapshot)
# =====================================================================

def run_get_users(customer, **_):
    _setup_credentials()
    url_base = customer['url_base']
    res = _make_request(url_base, 'user.get', {'FILTER': {}})  # sem ACTIVE filter para pegar todos
    if not res or 'result' not in res:
        return None
    df = pd.DataFrame(res['result'])
    df = _normalize_and_dedup(df)
    _upload_to_gcs(df, customer, 'bitrix_crm_users.csv')
    print(f"[bitrix_crm_users] OK: {len(df)} linhas")
    return {'rows': len(df), 'cols': len(df.columns), 'gcs_path': 'bitrix_crm_users.csv'}


def run_get_funnels(customer, **_):
    _setup_credentials()
    url_base = customer['url_base']
    res = _make_request(url_base, 'crm.dealcategory.list', {'order': {'SORT': 'ASC'}})
    if not res or 'result' not in res:
        return None
    df = pd.DataFrame(res['result'])
    df = _normalize_and_dedup(df)
    _upload_to_gcs(df, customer, 'bitrix_crm_funnels.csv')
    print(f"[bitrix_crm_funnels] OK: {len(df)} linhas")
    return {'rows': len(df), 'cols': len(df.columns), 'gcs_path': 'bitrix_crm_funnels.csv'}


def run_get_stages(customer, **_):
    """Tabela longa: uma linha por (categoria, stage)."""
    _setup_credentials()
    url_base = customer['url_base']
    cats = _make_request(url_base, 'crm.dealcategory.list', {'order': {'SORT': 'ASC'}})
    if not cats or 'result' not in cats:
        return None
    rows = []
    for cat in cats['result']:
        rs = _make_request(url_base, 'crm.dealcategory.stage.list', {'id': cat['ID']})
        if rs and 'result' in rs:
            for s in rs['result']:
                rows.append({
                    'category_id': cat['ID'],
                    'category_name': cat.get('NAME', ''),
                    'status_id': s.get('STATUS_ID'),
                    'name': s.get('NAME'),
                    'sort': s.get('SORT'),
                    'color': s.get('COLOR'),
                    'semantics': s.get('SEMANTICS'),
                })
    df = pd.DataFrame(rows)
    _upload_to_gcs(df, customer, 'bitrix_crm_stages.csv')
    print(f"[bitrix_crm_stages] OK: {len(df)} linhas")
    return {'rows': len(df), 'cols': len(df.columns), 'gcs_path': 'bitrix_crm_stages.csv'}


def run_get_statuses(customer, **_):
    """Catalogo de status (STATUS = leads, DEAL_STAGE, SOURCE, etc)."""
    _setup_credentials()
    url_base = customer['url_base']
    rows = []
    for entity_id in ['STATUS', 'SOURCE', 'DEAL_TYPE', 'CONTACT_TYPE',
                      'COMPANY_TYPE', 'CONTACT_STATUS']:
        res = _make_request(url_base, 'crm.status.list', {
            'order': {'SORT': 'ASC'}, 'filter': {'ENTITY_ID': entity_id},
        })
        if res and 'result' in res:
            for s in res['result']:
                s2 = dict(s)
                s2['entity_id'] = entity_id
                rows.append(s2)
    if not rows:
        return None
    df = pd.DataFrame(rows)
    df = _normalize_and_dedup(df)
    _upload_to_gcs(df, customer, 'bitrix_crm_statuses.csv')
    print(f"[bitrix_crm_statuses] OK: {len(df)} linhas")
    return {'rows': len(df), 'cols': len(df.columns), 'gcs_path': 'bitrix_crm_statuses.csv'}


def run_get_custom_fields(customer, **_):
    """Snapshot do schema crm.{entity}.fields para cada entidade."""
    _setup_credentials()
    url_base = customer['url_base']
    rows = []
    for entity, method in [
        ('lead', 'crm.lead.fields'),
        ('deal', 'crm.deal.fields'),
        ('contact', 'crm.contact.fields'),
        ('company', 'crm.company.fields'),
    ]:
        res = _make_request(url_base, method)
        if not res or 'result' not in res:
            continue
        for code, meta in res['result'].items():
            label = meta.get('formLabel') or meta.get('listLabel') or meta.get('title') or ''
            rows.append({
                'entity': entity,
                'code': code,
                'type': meta.get('type'),
                'is_required': meta.get('isRequired'),
                'is_multiple': meta.get('isMultiple'),
                'label': label,
            })
    df = pd.DataFrame(rows)
    _upload_to_gcs(df, customer, 'bitrix_crm_custom_fields.csv')
    print(f"[bitrix_crm_custom_fields] OK: {len(df)} linhas")
    return {'rows': len(df), 'cols': len(df.columns), 'gcs_path': 'bitrix_crm_custom_fields.csv'}


# =====================================================================
# FULL BACKFILL WRAPPERS (TEMPORARIO - rodar ate completed e remover)
# =====================================================================
# Usam checkpoint+chunked: cada execucao retoma do cursor salvo no GCS,
# salva CSVs em <output>_full_chunk_NNNN.csv, e marca completed quando
# esgotar a paginacao. Ao re-rodar uma task ja completed, recomeca do zero.

def run_get_leads_full(customer, **_):
    return _extract_crm_list_full(
        customer, 'crm.lead.list', 'crm.lead.fields', 'bitrix_crm_leads',
        post_translate=_translate_lead_status,
    )


def run_get_deals_full(customer, **_):
    return _extract_crm_list_full(
        customer, 'crm.deal.list', 'crm.deal.fields', 'bitrix_crm_deals',
        post_translate=_translate_deal_stage,
    )


def run_get_contacts_full(customer, **_):
    return _extract_crm_list_full(
        customer, 'crm.contact.list', 'crm.contact.fields', 'bitrix_crm_contacts',
    )


def run_get_company_full(customer, **_):
    return _extract_crm_list_full(
        customer, 'crm.company.list', 'crm.company.fields', 'bitrix_crm_company',
    )


def run_get_activities_full(customer, **_):
    return _extract_activities_full(customer)


# =====================================================================
# TASK REGISTRY (Airflow)
# =====================================================================

def get_extraction_tasks():
    """Tarefas para a DAG. Cada uma idempotente.
    Catalogos primeiro (rapidos), depois entidades transacionais incrementais.
    Tasks _full sao TEMPORARIAS para o backfill inicial."""
    return [
        {'task_id': 'run_get_users',           'python_callable': run_get_users},
        {'task_id': 'run_get_funnels',         'python_callable': run_get_funnels},
        {'task_id': 'run_get_stages',          'python_callable': run_get_stages},
        {'task_id': 'run_get_statuses',        'python_callable': run_get_statuses},
        {'task_id': 'run_get_custom_fields',   'python_callable': run_get_custom_fields},
        {'task_id': 'run_get_leads',           'python_callable': run_get_leads},
        {'task_id': 'run_get_deals',           'python_callable': run_get_deals},
        {'task_id': 'run_get_contacts',        'python_callable': run_get_contacts},
        {'task_id': 'run_get_company',         'python_callable': run_get_company},
        {'task_id': 'run_get_activities',      'python_callable': run_get_activities},
        # backfill 1x — remover apos rodar
        {'task_id': 'run_get_leads_full',      'python_callable': run_get_leads_full},
        {'task_id': 'run_get_deals_full',      'python_callable': run_get_deals_full},
        {'task_id': 'run_get_contacts_full',   'python_callable': run_get_contacts_full},
        {'task_id': 'run_get_company_full',    'python_callable': run_get_company_full},
        {'task_id': 'run_get_activities_full', 'python_callable': run_get_activities_full},
    ]
