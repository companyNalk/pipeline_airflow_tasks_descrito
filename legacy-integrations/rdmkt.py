"""
RD Marketing module for data extraction functions.
This module contains functions specific to the RD Marketing integration.
"""

import random
import time
import requests
import pandas as pd
import datetime
from typing import Any, Dict, List

from core import gcs


def make_request_with_retry(func, max_retries=10, base_delay=1, max_delay=300):
    """
    Executa uma função com retry automático para lidar com rate limiting.
    """
    for attempt in range(max_retries + 1):
        try:
            response = func()
            if response.status_code in [200, 409]:
                return response
            elif response.status_code == 429:
                if attempt < max_retries:
                    retry_after = response.headers.get('Retry-After')
                    delay = min(int(retry_after), max_delay) if retry_after else min(base_delay * (2 ** attempt) + random.uniform(0, 1), max_delay)
                    print(f"Rate limit (429). Tentativa {attempt + 1}/{max_retries + 1}. Aguardando {delay:.2f}s...")
                    time.sleep(delay)
                    continue
                else:
                    print("Máximo de tentativas de rate limit atingido.")
                    response.raise_for_status()
            else:
                if attempt < max_retries:
                    delay = base_delay + random.uniform(0, 1)
                    print(f"Erro HTTP {response.status_code}. Tentativa {attempt + 1}/{max_retries + 1}. Aguardando {delay:.2f}s...")
                    time.sleep(delay)
                    continue
                else:
                    response.raise_for_status()
        except Exception as e:
            if attempt < max_retries:
                delay = base_delay * (2 ** attempt) + random.uniform(0, 1)
                print(f"Erro na requisição: {e}. Tentativa {attempt + 1}/{max_retries + 1}. Aguardando {delay:.2f}s...")
                time.sleep(delay)
                continue
            else:
                print(f"Falha após {max_retries + 1} tentativas: {e}")
                raise
    return response


def run_webhook_register(customer):
    """
    Registra os webhooks de conversão e marcação de oportunidade para um cliente.
    """
    BASE_URL = 'https://webhook-rd.nalk.com.br'
    headers = {'X-API-KEY': customer['x_api_key'], 'accept': 'application/json', 'Content-Type': 'application/json'}

    def check_company_exists(alias):
        def make_check_request():
            url = f"{BASE_URL}/api/v1/company?alias={alias}"
            return requests.get(url, headers={'X-API-KEY': customer['x_api_key'], 'accept': 'application/json'})
        try:
            response = make_request_with_retry(make_check_request)
            return response.json()[0].get('alias') if response.status_code == 200 and response.json() else None
        except Exception as e:
            print(f"Erro ao verificar empresa: {e}"); return None

    def create_company():
        def make_create_request():
            url = f"{BASE_URL}/api/v1/company"
            data = {'name': customer['name'], 'alias': customer['alias'], 'google_company_id': customer['company_id'],
                    'google_project_id': customer['project_id'], 'google_bucket_name': customer['bucket_name']}
            return requests.post(url, headers=headers, json=data)
        response = make_request_with_retry(make_create_request)
        return response.json().get('alias')

    def register_webhook(company_alias, event_type):
        def make_webhook_request():
            url = f"{BASE_URL}/api/v1/rd-station/register-webhook/{company_alias}"
            webhook_headers = {'X-API-KEY': customer['x_api_key'], 'accept': 'application/json', 'rd-client-id': customer['client_id'],
                               'rd-client-secret': customer['client_secret'], 'rd-refresh-token': customer['refresh_token'], 'Content-Type': 'application/json'}
            data = {'event_type': event_type, 'entity_type': 'CONTACT', 'http_method': 'POST', 'use_queue': True}
            return requests.post(url, headers=webhook_headers, json=data)
        try:
            response = make_request_with_retry(make_webhook_request)
            if response.status_code == 200: print(f"Webhook {event_type} registrado com sucesso!")
            elif response.status_code == 409: print(f"Webhook {event_type} já existe para {company_alias}. Continuando...")
            else: print(f"Erro ao registrar webhook {event_type}: Status {response.status_code}")
        except Exception as e: print(f"Erro ao registrar webhook {event_type}: {e}"); raise

    print(f"Verificando empresa: {customer['alias']}")
    company_alias = check_company_exists(customer['alias'])
    if not company_alias:
        print("Empresa não encontrada. Criando nova..."); company_alias = create_company(); print(f"Empresa criada com alias: {company_alias}")
    else: print(f"Empresa encontrada com alias: {company_alias}")
    for event_type in ['WEBHOOK.CONVERTED', 'WEBHOOK.MARKED_OPPORTUNITY']:
        print(f"Registrando webhook para {event_type}"); register_webhook(company_alias, event_type)
    return 'Webhook registration completed'


def run_webhook_leads(customer):
    """
    Extrai todos os leads recebidos via webhook e salva em um arquivo CSV no GCS.
    """
    BASE_URL = 'https://webhook-rd.nalk.com.br'
    headers = {'X-API-KEY': customer['x_api_key'], 'accept': 'application/json'}
    ALIAS = customer['alias']
    BUCKET_NAME = customer['bucket_name']

    def flatten_dict(d: Dict[str, Any], parent_key: str = '', sep: str = '_') -> Dict[str, Any]:
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict): items.extend(flatten_dict(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                if v:
                    for i, item in enumerate(v):
                        if isinstance(item, dict): items.extend(flatten_dict(item, f"{new_key}_{i + 1}", sep=sep).items())
                        else: items.append((f"{new_key}_{i + 1}", item))
                else: items.append((new_key, None))
            else: items.append((new_key, v))
        return dict(items)

    def fetch_webhook_leads(limit: int = 500, offset: int = 0) -> Dict[str, Any]:
        def make_leads_request():
            url = f"{BASE_URL}/api/v1/raw-data/by-company"
            params = {'company_alias': ALIAS, 'limit': limit, 'offset': offset}
            return requests.get(url, headers=headers, params=params)
        response = make_request_with_retry(make_leads_request)
        if response.status_code == 200: return response.json()
        elif response.status_code == 404:
            print(f"Empresa {ALIAS} sem registros no webhook (404). Normal para cadastros novos.")
            return {'data': [], 'total_count': 0, 'returned_count': 0}
        else: print(f"Erro na requisição: Status {response.status_code}"); return {}

    all_leads, offset, limit, total_count = [], 0, 500, None
    while True:
        print(f"Buscando leads - Offset: {offset}, Limit: {limit}"); time.sleep(0.5)
        response = fetch_webhook_leads(limit=limit, offset=offset)
        if not response or 'data' not in response: print("Nenhum dado na resposta."); break
        if total_count is None: total_count = response.get('total_count', 0); print(f"Total de registros: {total_count}")
        leads_batch = response['data']
        if not leads_batch: print("Fim da paginação."); break
        all_leads.extend(leads_batch); print(f"Coletados até agora: {len(all_leads)}")
        if len(all_leads) >= total_count: print("Coleta finalizada."); break
        offset += limit
    
    if not all_leads: return f'Nenhum lead encontrado para {ALIAS}.'

    print(f"Processando {len(all_leads)} leads...")
    df = pd.DataFrame([flatten_dict(lead) for lead in all_leads])
    if not df.empty:
        local_file_path = f"/tmp/{customer['project_id']}.rdmkt.run_webhook_leads.csv"
        df.to_csv(local_file_path, index=False)
        credentials = gcs.load_credentials_from_env()
        gcs.write_file_to_gcs(
            bucket_name=BUCKET_NAME, 
            local_file_path=local_file_path,
            destination_name=f"webhook_users/webhook_users.csv",
            credentials=credentials
        )
        return f'Extração de leads concluída. Total: {len(all_leads)}'
    return "DataFrame vazio. Nenhum dado salvo."


def _get_rd_access_token(customer: Dict[str, Any]) -> str:
    """Função auxiliar para obter um access token da API da RD Station."""
    print("Obtendo access token da RD Station...")
    def make_token_request():
        return requests.post('https://api.rd.services/auth/token', data={
            'client_id': customer['client_id'], 'client_secret': customer['client_secret'],
            'refresh_token': customer['refresh_token'], 'grant_type': 'refresh_token'})
    response = make_request_with_retry(make_token_request); response.raise_for_status()
    print("Access token obtido com sucesso.")
    return response.json()['access_token']


def _run_analytics_extraction(customer: Dict[str, Any], api_url: str, json_key: str, gcs_folder: str, days_to_extract: int):
    """
    Função genérica e auxiliar para extrair dados de analytics da RD Station.
    """
    print(f"Iniciando extração de '{json_key}' para {customer['alias']}")
    access_token = _get_rd_access_token(customer)
    headers = {'Authorization': f'Bearer {access_token}', 'Content-Type': 'application/json'}
    all_data = []
    end_date = datetime.date.today() - datetime.timedelta(days=1)
    start_date = end_date - datetime.timedelta(days=days_to_extract - 1)
    print(f"Período de extração: {start_date} a {end_date}")
    
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime('%Y-%m-%d'); print(f"Buscando dados para: {date_str}")
        def fetch_daily_data(): return requests.get(api_url, headers=headers, params={'start_date': date_str, 'end_date': date_str})
        try:
            response = make_request_with_retry(fetch_daily_data); response.raise_for_status()
            data = response.json()
            if json_key in data and data[json_key]:
                daily_records = data[json_key]
                for record in daily_records: record['extraction_date'] = date_str
                all_data.extend(daily_records)
        except Exception as e: print(f"Erro ao buscar dados para {date_str}: {e}. Pulando dia.")
        current_date += datetime.timedelta(days=1); time.sleep(0.5)

    if not all_data: return f"Nenhum dado de '{json_key}' encontrado."

    print(f"Processando {len(all_data)} registros de '{json_key}'...")
    df = pd.json_normalize(all_data)
    if not df.empty:
        local_file_path = f"/tmp/{customer['project_id']}.rdmkt.{gcs_folder}.csv"
        df.to_csv(local_file_path, index=False)
        credentials = gcs.load_credentials_from_env()
        gcs.write_file_to_gcs(
            bucket_name=customer['bucket_name'],
            local_file_path=local_file_path,
            destination_name=f"{gcs_folder}/{gcs_folder}.csv",
            credentials=credentials
        )
        return f"Extração de '{json_key}' concluída. Total: {len(df)}"
    return "DataFrame vazio. Nenhum dado salvo."


def run_email_analytics(customer: Dict[str, Any]):
    """
    Extrai dados de analytics de e-mails dos últimos 43 dias.
    """
    return _run_analytics_extraction(
        customer=customer,
        api_url='https://api.rd.services/platform/analytics/emails',
        json_key='emails',
        gcs_folder='email_analytics',
        days_to_extract=43
    )


def run_conversion_analytics(customer: Dict[str, Any]):
    """
    Extrai dados de analytics de conversões dos últimos 40 dias.
    """
    return _run_analytics_extraction(
        customer=customer,
        api_url='https://api.rd.services/platform/analytics/conversions',
        json_key='conversions',
        gcs_folder='conversion_analytics',
        days_to_extract=40
    )


def get_extraction_tasks():
    """
    Get the list of data extraction tasks for RD Marketing.

    Returns:
        list: List of task configurations
    """
    return [
        {
            'task_id': 'run_webhook_register',
            'python_callable': run_webhook_register
        },
        {
            'task_id': 'run_webhook_leads',
            'python_callable': run_webhook_leads
        },
        {
            'task_id': 'run_email_analytics',
            'python_callable': run_email_analytics
        },
        {
            'task_id': 'run_conversion_analytics',
            'python_callable': run_conversion_analytics
        }
    ]
