"""
RD Marketing module for data extraction functions.
This module contains functions specific to the RD Marketing integration.
"""

import random
import time

from core import gcs


def make_request_with_retry(func, max_retries=10, base_delay=1, max_delay=300):
    """
    Executa uma função com retry automático para lidar com rate limiting.
    """
    for attempt in range(max_retries + 1):
        try:
            response = func()

            # Se a resposta for bem-sucedida, retorna
            if response.status_code == 200:
                return response

            # Se for rate limit (429), tenta novamente
            elif response.status_code == 429:
                if attempt < max_retries:
                    # Verifica se há header Retry-After
                    retry_after = response.headers.get('Retry-After')
                    if retry_after:
                        delay = min(int(retry_after), max_delay)
                    else:
                        # Backoff exponencial com jitter
                        delay = min(base_delay * (2 ** attempt) + random.uniform(0, 1), max_delay)

                    print(f"Rate limit atingido (429). Tentativa {attempt + 1}/{max_retries + 1}. "
                          f"Aguardando {delay:.2f} segundos...")
                    time.sleep(delay)
                    continue
                else:
                    print(f"Máximo de tentativas atingido para rate limit.")
                    response.raise_for_status()

            # Para outros erros HTTP, tenta novamente com delay menor
            else:
                if attempt < max_retries:
                    delay = base_delay + random.uniform(0, 1)
                    print(f"Erro HTTP {response.status_code}. Tentativa {attempt + 1}/{max_retries + 1}. "
                          f"Aguardando {delay:.2f} segundos...")
                    time.sleep(delay)
                    continue
                else:
                    response.raise_for_status()

        except Exception as e:
            if attempt < max_retries:
                delay = base_delay * (2 ** attempt) + random.uniform(0, 1)
                print(f"Erro na requisição: {str(e)}. Tentativa {attempt + 1}/{max_retries + 1}. "
                      f"Aguardando {delay:.2f} segundos...")
                time.sleep(delay)
                continue
            else:
                print(f"Falha após {max_retries + 1} tentativas: {str(e)}")
                raise

    return response


def run_conversions(customer):
    """
    Extract conversion data from RD Marketing API.
    """
    print(customer)
    import requests
    import pandas as pd
    import datetime

    # Configurações Iniciais
    CLIENT_ID = customer['client_id']
    CLIENT_SECRET = customer['client_secret']
    TOKEN_URL = 'https://api.rd.services/auth/token'
    CONVERSION_STATS_URL = 'https://api.rd.services/platform/analytics/conversions'
    refresh_token = customer['refresh_token']
    BUCKET_NAME = customer['bucket_name']
    SAVE_DIR = customer['save_dir_conversoes']

    def get_access_token():
        """Obtém o token de acesso da API RD Station com retry."""

        def make_token_request():
            data = {
                'client_id': CLIENT_ID,
                'client_secret': CLIENT_SECRET,
                'refresh_token': refresh_token,
                'grant_type': 'refresh_token'
            }
            return requests.post(TOKEN_URL, data=data)

        response = make_request_with_retry(make_token_request)
        return response.json()['access_token']

    def fetch_conversion_stats(start_date, end_date, access_token):
        """Busca estatísticas de conversões da API RD Station com retry."""

        def make_conversion_request():
            headers = {
                'Authorization': f'Bearer {access_token}',
                'Content-Type': 'application/json'
            }
            params = {
                'start_date': start_date,
                'end_date': end_date
            }
            return requests.get(CONVERSION_STATS_URL, headers=headers, params=params)

        response = make_request_with_retry(make_conversion_request)
        return response.json()

    def save_to_csv_and_upload(data, bucket_name, destination_blob_name):
        """Salva os dados em um arquivo CSV e faz o upload para o GCS."""
        if 'conversions' in data:
            df = pd.json_normalize(data['conversions'])
            if not df.empty:
                credentials = gcs.load_credentials_from_env()
                local_file_path = f"/tmp/{customer['project_id']}.rdmkt.run_conversions.csv"
                df.to_csv(local_file_path, index=False)
                gcs.write_file_to_gcs(
                    bucket_name=bucket_name,
                    local_file_path=local_file_path,
                    destination_name=destination_blob_name,
                    credentials=credentials
                )
                print(f"Arquivo salvo em no GCS: {destination_blob_name}.")
            else:
                print("DataFrame está vazio. Nenhum dado para salvar.")
        else:
            print("Nenhum dado para salvar.")

    def main():
        """Função principal para execução local."""
        access_token = get_access_token()
        # Coletar dados dos últimos 40 dias
        end_date = datetime.date.today() - datetime.timedelta(days=1)
        start_date = end_date - datetime.timedelta(days=40)

        current_date = start_date
        while current_date <= end_date:
            next_date = current_date + datetime.timedelta(days=1)
            date_str = current_date.strftime('%Y-%m-%d')
            file_name = f"dados_conversoes_{current_date.strftime('%d_%m_%Y')}.csv"
            destination_blob_name = f"{SAVE_DIR}/{file_name}"

            print(f"Coletando dados de {date_str}")

            # Adiciona um pequeno delay entre requisições para evitar rate limiting
            time.sleep(0.5)

            conversion_stats = fetch_conversion_stats(date_str, date_str, access_token)
            save_to_csv_and_upload(conversion_stats, BUCKET_NAME, destination_blob_name)

            current_date = next_date

        return 'Data collection and upload completed'

    main()


def run_emails(customer):
    """
    Extract email data from RD Marketing API.
    """
    import requests
    import pandas as pd
    import datetime

    # Configurações Iniciais
    CLIENT_ID = customer['client_id']
    CLIENT_SECRET = customer['client_secret']
    TOKEN_URL = 'https://api.rd.services/auth/token'
    EMAIL_STATS_URL = 'https://api.rd.services/platform/analytics/emails'
    refresh_token = customer['refresh_token']
    BUCKET_NAME = customer['bucket_name']
    SAVE_DIR = customer['save_dir_email']

    def get_access_token():
        """Obtém o token de acesso da API RD Station com retry."""

        def make_token_request():
            data = {
                'client_id': CLIENT_ID,
                'client_secret': CLIENT_SECRET,
                'refresh_token': refresh_token,
                'grant_type': 'refresh_token'
            }
            return requests.post(TOKEN_URL, data=data)

        response = make_request_with_retry(make_token_request)
        return response.json()['access_token']

    def fetch_email_stats(start_date, end_date, access_token):
        """Busca estatísticas de e-mails da API RD Station com retry."""

        def make_email_request():
            headers = {
                'Authorization': f'Bearer {access_token}',
                'Content-Type': 'application/json'
            }
            params = {
                'start_date': start_date,
                'end_date': end_date
            }
            return requests.get(EMAIL_STATS_URL, headers=headers, params=params)

        response = make_request_with_retry(make_email_request)
        return response.json()

    def save_to_csv_and_upload(data, bucket_name, destination_blob_name):
        """Salva os dados em um arquivo CSV e faz o upload para o GCS."""
        try:
            if 'emails' in data:
                df = pd.json_normalize(data['emails'])
                if not df.empty:
                    print(f"Arquivo salvo em {destination_blob_name}.")
                    credentials = gcs.load_credentials_from_env()
                    local_file_path = f"/tmp/{customer['project_id']}.rdmkt.run_emails.csv"
                    df.to_csv(local_file_path, index=False)
                    gcs.write_file_to_gcs(
                        bucket_name=bucket_name,
                        local_file_path=local_file_path,
                        destination_name=destination_blob_name,
                        credentials=credentials
                    )
                else:
                    print("DataFrame está vazio. Nenhum dado para salvar.")
            else:
                print("Nenhum dado para salvar.")
        except Exception as e:
            print(f"Erro ao salvar dados no GCS: {str(e)}")
            raise

    def main():
        """Função principal para execução local."""
        access_token = get_access_token()
        # Coletar dados dos últimos 43 dias
        end_date = datetime.date.today() - datetime.timedelta(days=1)
        start_date = end_date - datetime.timedelta(days=43)

        current_date = start_date
        while current_date <= end_date:
            next_date = current_date + datetime.timedelta(days=1)
            date_str = current_date.strftime('%Y-%m-%d')
            file_name = f"dados_emails_{current_date.strftime('%d_%m_%Y')}.csv"
            destination_blob_name = f"{SAVE_DIR}/{file_name}"

            print(f"Coletando dados de {date_str}")

            # Adiciona um pequeno delay entre requisições para evitar rate limiting
            time.sleep(0.5)

            email_stats = fetch_email_stats(date_str, date_str, access_token)
            save_to_csv_and_upload(email_stats, BUCKET_NAME, destination_blob_name)

            current_date = next_date

    main()


def run_webhook_register(customer):
    import requests

    # Configurações Iniciais
    BASE_URL = 'https://webhook-rd.nalk.com.br'
    API_KEY = customer['x_api_key']

    # Dados da empresa
    name = customer['name']
    alias = customer['alias']
    company_id = customer['company_id']
    project_id = customer['project_id']
    bucket_name = customer['bucket_name']

    # Dados RD Station
    rd_client_id = customer['client_id']
    rd_client_secret = customer['client_secret']
    rd_refresh_token = customer['refresh_token']

    headers = {
        'X-API-KEY': API_KEY,
        'accept': 'application/json',
        'Content-Type': 'application/json'
    }

    def check_company_exists(alias):
        """Verifica se a empresa existe e retorna o ID."""

        def make_check_request():
            url = f"{BASE_URL}/api/v1/company?alias={alias}"
            return requests.get(url, headers={'X-API-KEY': API_KEY, 'accept': 'application/json'})

        try:
            response = make_request_with_retry(make_check_request)
            if response.status_code == 200:
                return response.json().get('id')
            return None
        except Exception as e:
            print(f"Erro ao verificar empresa: {str(e)}")
            return None

    def create_company():
        """Cadastra a empresa e retorna o ID."""

        def make_create_request():
            url = f"{BASE_URL}/api/v1/company"
            data = {
                'name': name,
                'alias': alias,
                'google_company_id': company_id,
                'google_project_id': project_id,
                'google_bucket_name': bucket_name
            }
            return requests.post(url, headers=headers, json=data)

        response = make_request_with_retry(make_create_request)
        return response.json().get('id')

    def register_webhook(company_id, event_type):
        """Registra um webhook para um tipo de evento específico."""

        def make_webhook_request():
            url = f"{BASE_URL}/api/v1/rd-station/register-webhook/{company_id}"
            webhook_headers = {
                'X-API-KEY': API_KEY,
                'accept': 'application/json',
                'rd-client-id': rd_client_id,
                'rd-client-secrect': rd_client_secret,
                'rd-refresh-token': rd_refresh_token,
                'Content-Type': 'application/json'
            }
            data = {
                'event_type': event_type,
                'entity_type': 'CONTACT',
                'http_method': 'POST',
                'use_queue': True
            }
            return requests.post(url, headers=webhook_headers, json=data)

        response = make_request_with_retry(make_webhook_request)
        print(f"Webhook registrado para {event_type}: {response.status_code}")
        return response

    def main():
        """Função principal para registro de webhooks."""
        print(f"Verificando empresa: {alias}")

        # Verifica se a empresa existe
        company_db_id = check_company_exists(alias)

        if not company_db_id:
            print("Empresa não encontrada. Criando nova empresa...")
            company_db_id = create_company()
            print(f"Empresa criada com ID: {company_db_id}")
        else:
            print(f"Empresa encontrada com ID: {company_db_id}")

        # Registra os webhooks
        event_types = ['WEBHOOK.CONVERTED', 'WEBHOOK.MARKED_OPPORTUNITY']

        for event_type in event_types:
            print(f"Registrando webhook para {event_type}")
            register_webhook(company_db_id, event_type)

        return 'Webhook registration completed'

    main()


def get_extraction_tasks():
    """
    Get the list of data extraction tasks for RD Marketing.

    Returns:
        list: List of task configurations
    """
    return [
        {
            'task_id': 'run_conversions',
            'python_callable': run_conversions
        },
        {
            'task_id': 'run_emails',
            'python_callable': run_emails
        },
        {
            'task_id': 'run_emails',
            'python_callable': run_webhook_register
        }
    ]
