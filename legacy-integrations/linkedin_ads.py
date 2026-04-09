"""
LinkedIn Ads module for data extraction functions.
This module contains functions specific to the LinkedIn Ads integration.
"""

from typing import Any, Dict

from core import gcs


def _make_request_with_retry(func, max_retries=5, base_delay=1, max_delay=30):
    """
    Executa uma função com retry automático para lidar com rate limiting (429).
    Erros permanentes (401, 403, 404) falham imediatamente sem retry.
    """
    import random
    import time
    import requests

    NON_RETRYABLE = {401, 403, 404}

    for attempt in range(max_retries + 1):
        try:
            response = func()
            if response.status_code == 200:
                return response
            elif response.status_code in NON_RETRYABLE:
                response.raise_for_status()
            elif response.status_code == 429:
                if attempt < max_retries:
                    retry_after = response.headers.get('Retry-After')
                    delay = int(retry_after) if retry_after else (base_delay * (2 ** attempt) + random.uniform(0, 1))
                    print(f"LinkedIn Rate limit (429). Tentativa {attempt + 1}/{max_retries + 1}. Aguardando {delay:.2f}s...")
                    time.sleep(min(delay, max_delay))
                    continue
            response.raise_for_status()
            return response
        except requests.exceptions.HTTPError:
            raise
        except Exception as e:
            if attempt < max_retries:
                delay = base_delay * (2 ** attempt) + random.uniform(0, 1)
                print(f"Erro na requisição LinkedIn: {e}. Tentativa {attempt + 1}/{max_retries + 1}. Aguardando {delay:.2f}s...")
                time.sleep(min(delay, max_delay))
                continue
            else:
                raise


def _get_linkedin_access_token(customer: Dict[str, Any]) -> str:
    """
    Gera um novo access_token a partir do refresh_token, com retry.
    """
    import requests

    print("Obtendo access token do LinkedIn...")
    url = "https://www.linkedin.com/oauth/v2/accessToken"
    payload = {
        'grant_type': 'refresh_token',
        'refresh_token': customer['refresh_token'],
        'client_id': customer['client_id'],
        'client_secret': customer['client_secret']
    }
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}

    def fetch():
        return requests.post(url, data=payload, headers=headers)

    response = _make_request_with_retry(fetch)
    print("Access token LinkedIn obtido com sucesso.")
    return response.json()['access_token']


def _get_campaign_names(access_token: str, account_id: str) -> Dict[str, str]:
    """
    Busca os nomes das campanhas para mapear os IDs.
    """
    import requests

    url = "https://api.linkedin.com/v2/adCampaignsV2"
    headers = {
        'Authorization': f'Bearer {access_token}',
        'X-Restli-Protocol-Version': '2.0.0'
    }
    params = {
        'q': 'search',
        'search.account.values[0]': account_id,
        'count': 1000,
        'start': 0
    }

    mapping = {}
    while True:
        snapshot = dict(params)
        def fetch(p=snapshot): return requests.get(url, headers=headers, params=p)
        response = _make_request_with_retry(fetch)
        data = response.json()
        elements = data.get('elements', [])
        if not elements:
            break
        for el in elements:
            mapping[el['id']] = el.get('name', 'Sem Nome')
        if len(elements) < params['count']:
            break
        params['start'] += params['count']
    return mapping


def _is_date_processed(bucket_name: str, destination_name: str, credentials) -> bool:
    """Verifica se um arquivo já existe no GCS (idempotência)."""
    from google.cloud import storage
    client = storage.Client(credentials=credentials)
    bucket = client.get_bucket(bucket_name)
    return bucket.blob(destination_name).exists()


def run_extract_ad_analytics(customer: Dict[str, Any]):
    """
    Extrai métricas diárias de performance das campanhas do LinkedIn Ads.
    Processa dia a dia a partir de customer['start_date'], com idempotência.
    Salva linkedin_ads/analytics_{date}.csv por dia no GCS.
    """
    import datetime
    import time
    import requests
    import pandas as pd

    print(f"Iniciando extração LinkedIn Ads Analytics para {customer.get('alias', 'cliente')}")

    access_token = _get_linkedin_access_token(customer)
    account_id = customer['linkedin_account_id']
    campaign_names = _get_campaign_names(access_token, account_id)

    end_date = datetime.date.today() - datetime.timedelta(days=1)
    default_start = (end_date - datetime.timedelta(days=30)).isoformat()
    start_date = datetime.datetime.strptime(
        customer.get('start_date', default_start), '%Y-%m-%d'
    ).date()

    credentials = gcs.load_credentials_from_env()

    url = "https://api.linkedin.com/v2/adAnalyticsV2"
    headers = {
        'Authorization': f'Bearer {access_token}',
        'X-Restli-Protocol-Version': '2.0.0'
    }

    total_saved = 0
    current = start_date

    while current <= end_date:
        date_str = current.strftime('%Y-%m-%d')
        destination = f"linkedin_ads/analytics_{date_str}.csv"

        if _is_date_processed(customer['bucket_name'], destination, credentials):
            print(f"  └─ {date_str}: já processado, pulando.")
            current += datetime.timedelta(days=1)
            continue

        print(f"  Processando analytics: {date_str}")

        params = {
            'q': 'analytics',
            'pivot': 'CAMPAIGN',
            'timeGranularity': 'DAILY',
            'dateRange.start.day': current.day,
            'dateRange.start.month': current.month,
            'dateRange.start.year': current.year,
            'dateRange.end.day': current.day,
            'dateRange.end.month': current.month,
            'dateRange.end.year': current.year,
            'accounts': f'List({account_id})',
            'fields': 'dateRange,campaign,impressions,clicks,costInLocalCurrency,externalId',
            'count': 1000,
            'start': 0
        }

        all_elements = []
        while True:
            snapshot = dict(params)
            def fetch(p=snapshot): return requests.get(url, headers=headers, params=p)
            response = _make_request_with_retry(fetch)
            data = response.json()
            elements = data.get('elements', [])
            if not elements:
                break
            all_elements.extend(elements)
            if len(elements) < params['count']:
                break
            params['start'] += params['count']
            time.sleep(0.5)

        if not all_elements:
            print(f"  └─ {date_str}: sem dados.")
            current += datetime.timedelta(days=1)
            continue

        rows = []
        for el in all_elements:
            dr = el.get('dateRange', {}).get('start', {})
            row_date = f"{dr.get('year')}-{dr.get('month'):02d}-{dr.get('day'):02d}"
            camp_id_urn = el.get('campaign')
            camp_id = camp_id_urn.split(':')[-1] if camp_id_urn else None
            rows.append({
                'date': row_date,
                'campaign_id': camp_id,
                'campaign_name': campaign_names.get(int(camp_id), 'Desconhecido') if camp_id and camp_id.isdigit() else 'Desconhecido',
                'external_id': el.get('externalId'),
                'impressions': el.get('impressions', 0),
                'clicks': el.get('clicks', 0),
                'cost_local': float(el.get('costInLocalCurrency', 0.0))
            })

        df = pd.DataFrame(rows)
        local_file = f"/tmp/linkedin_ads_analytics_{customer.get('alias', 'ext')}_{date_str}.csv"
        df.to_csv(local_file, index=False)
        gcs.write_file_to_gcs(
            bucket_name=customer['bucket_name'],
            local_file_path=local_file,
            destination_name=destination,
            credentials=credentials
        )
        total_saved += len(df)
        print(f"  └─ {date_str}: {len(df)} registros salvos.")
        current += datetime.timedelta(days=1)

    return f"Sucesso! Analytics LinkedIn: {total_saved} registros salvos."


def run_extract_leads(customer: Dict[str, Any]):
    """
    Extrai leads via formulários do LinkedIn.
    Busca o período completo e salva linkedin_ads/leads_{date}.csv por dia de submissão.
    Leads sem submittedAt são ignorados com aviso.
    """
    import datetime
    import time
    import requests
    import pandas as pd

    print(f"Iniciando extração de Leads LinkedIn para {customer.get('alias', 'cliente')}")

    access_token = _get_linkedin_access_token(customer)
    account_id = customer['linkedin_account_id']

    end_date = datetime.date.today() - datetime.timedelta(days=1)
    default_start = (end_date - datetime.timedelta(days=7)).isoformat()
    start_date = datetime.datetime.strptime(
        customer.get('start_date', default_start), '%Y-%m-%d'
    ).date()

    credentials = gcs.load_credentials_from_env()

    start_ts = int(time.mktime(start_date.timetuple()) * 1000)

    url = "https://api.linkedin.com/v2/adFormResponses"
    headers = {
        'Authorization': f'Bearer {access_token}',
        'X-Restli-Protocol-Version': '2.0.0'
    }
    params = {
        'q': 'account',
        'account': account_id,
        'createdTimestampStart': start_ts,
        'count': 100,
        'start': 0
    }

    all_leads = []
    while True:
        snapshot = dict(params)
        def fetch(p=snapshot): return requests.get(url, headers=headers, params=p)
        response = _make_request_with_retry(fetch)
        data = response.json()
        elements = data.get('elements', [])
        if not elements:
            break
        all_leads.extend(elements)
        if len(elements) < params['count']:
            break
        params['start'] += params['count']
        time.sleep(0.5)

    if not all_leads:
        return f"Nenhum lead encontrado no LinkedIn de {start_date} a {end_date}."

    print(f"Extraídos {len(all_leads)} leads. Processando...")

    processed_leads = []
    skipped = 0
    for lead in all_leads:
        submitted_at_ms = lead.get('submittedAt')
        if submitted_at_ms is None:
            skipped += 1
            continue

        dt = datetime.datetime.fromtimestamp(submitted_at_ms / 1000)
        lead_data = {
            'lead_id': lead.get('id'),
            'form_id': lead.get('form'),
            'campaign_id': lead.get('campaign'),
            'created_at': dt.strftime('%Y-%m-%d %H:%M:%S'),
            '_lead_date': dt.strftime('%Y-%m-%d')
        }
        for field in lead.get('formResponseData', {}).get('responses', []):
            field_name = field.get('fieldIdentifier')
            field_value = field.get('responseValue')
            if field_name:
                lead_data[field_name] = field_value

        processed_leads.append(lead_data)

    if skipped:
        print(f"  Aviso: {skipped} lead(s) ignorados por ausência de submittedAt.")

    if not processed_leads:
        return "Nenhum lead válido para processar."

    df = pd.DataFrame(processed_leads)
    total_saved = 0

    for lead_date, group_df in df.groupby('_lead_date'):
        destination = f"linkedin_ads/leads_{lead_date}.csv"
        if _is_date_processed(customer['bucket_name'], destination, credentials):
            print(f"  └─ {lead_date}: já processado, pulando.")
            continue

        group_df = group_df.drop(columns=['_lead_date'])
        local_file = f"/tmp/linkedin_ads_leads_{customer.get('alias', 'ext')}_{lead_date}.csv"
        group_df.to_csv(local_file, index=False)
        gcs.write_file_to_gcs(
            bucket_name=customer['bucket_name'],
            local_file_path=local_file,
            destination_name=destination,
            credentials=credentials
        )
        total_saved += len(group_df)
        print(f"  └─ {lead_date}: {len(group_df)} leads salvos.")

    return f"Sucesso! Leads LinkedIn: {total_saved} registros salvos."


def get_extraction_tasks():
    """
    Expõe as tarefas de extração para o orquestrador (Airflow).
    """
    return [
        {
            'task_id': 'run_extract_linkedin_analytics',
            'python_callable': run_extract_ad_analytics
        },
        {
            'task_id': 'run_extract_linkedin_leads',
            'python_callable': run_extract_leads
        }
    ]
