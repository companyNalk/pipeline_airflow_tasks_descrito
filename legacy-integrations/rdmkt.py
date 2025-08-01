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

            if response.status_code == 409:
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


def run_webhook_register(customer):
    import requests

    # Configurações Iniciais
    BASE_URL = 'https://webhook-rd.nalk.com.br'
    X_API_KEY = customer['x_api_key']

    # Dados da empresa
    NAME = customer['name']
    ALIAS = customer['alias']
    COMPANY_ID = customer['company_id']
    PROJECT_ID = customer['project_id']
    BUCKET_NAME = customer['bucket_name']

    # Dados RD Station
    RD_CLIENT_ID = customer['client_id']
    RD_CLIENT_SECRET = customer['client_secret']
    RD_REFRESH_TOKEN = customer['refresh_token']

    headers = {
        'X-API-KEY': X_API_KEY,
        'accept': 'application/json',
        'Content-Type': 'application/json'
    }

    def check_company_exists(alias):
        """Verifica se a empresa existe e retorna o alias."""

        def make_check_request():
            url = f"{BASE_URL}/api/v1/company?alias={alias}"
            return requests.get(url, headers={'X-API-KEY': X_API_KEY, 'accept': 'application/json'})

        try:
            response = make_request_with_retry(make_check_request)
            if response.status_code == 200:
                return response.json()[0].get('alias')
            return None
        except Exception as e:
            print(f"Erro ao verificar empresa: {str(e)}")
            return None

    def create_company():
        """Cadastra a empresa e retorna o ID."""

        def make_create_request():
            url = f"{BASE_URL}/api/v1/company"
            data = {
                'name': NAME,
                'alias': ALIAS,
                'google_company_id': COMPANY_ID,
                'google_project_id': PROJECT_ID,
                'google_bucket_name': BUCKET_NAME
            }
            return requests.post(url, headers=headers, json=data)

        response = make_request_with_retry(make_create_request)
        return response.json().get('alias')

    def register_webhook(alias, event_type):
        """Registra um webhook para um tipo de evento específico."""

        def make_webhook_request():
            url = f"{BASE_URL}/api/v1/rd-station/register-webhook/{alias}"
            webhook_headers = {
                'X-API-KEY': X_API_KEY,
                'accept': 'application/json',
                'rd-client-id': RD_CLIENT_ID,
                'rd-client-secret': RD_CLIENT_SECRET,
                'rd-refresh-token': RD_REFRESH_TOKEN,
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
        return response

    def main():
        """Função principal para registro de webhooks."""
        ALIAS = customer['alias']
        print(f"Verificando empresa: {ALIAS}")

        # Verifica se a empresa existe
        existed_alias = check_company_exists(ALIAS)

        if not existed_alias:
            print("Empresa não encontrada. Criando nova empresa...")
            alias = create_company()
            print(f"Empresa criada com ID: {alias}")
        else:
            print(f"Empresa encontrada com ID: {ALIAS}")

        # Registra os webhooks
        event_types = ['WEBHOOK.CONVERTED', 'WEBHOOK.MARKED_OPPORTUNITY']

        for event_type in event_types:
            print(f"Registrando webhook para {event_type}")
            register_webhook(alias, event_type)

        return 'Webhook registration completed'

    main()


def run_webhook_leads(customer):
    import requests
    import pandas as pd
    import time
    import random
    from typing import Any, Dict, List

    # Configurações Iniciais
    BASE_URL = 'https://webhook-rd.nalk.com.br'
    X_API_KEY = customer['x_api_key']
    BUCKET_NAME = customer['bucket_name']

    # Dados da empresa
    ALIAS = customer['alias']

    headers = {
        'X-API-KEY': X_API_KEY,
        'accept': 'application/json'
    }

    def flatten_dict(d: Dict[str, Any], parent_key: str = '', sep: str = '_') -> Dict[str, Any]:
        """
        Achata um dicionário recursivamente, incluindo listas.
        """
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k

            if isinstance(v, dict):
                items.extend(flatten_dict(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                if v:  # Se a lista não estiver vazia
                    for i, item in enumerate(v):
                        if isinstance(item, dict):
                            items.extend(flatten_dict(item, f"{new_key}_{i + 1}", sep=sep).items())
                        else:
                            items.append((f"{new_key}_{i + 1}", item))
                else:
                    items.append((new_key, None))
            else:
                items.append((new_key, v))
        return dict(items)

    def make_request_with_retry(request_func, max_retries: int = 11):
        """
        Função de retry modificada para tratar 404 como caso especial
        """
        for attempt in range(1, max_retries + 1):
            try:
                response = request_func()

                # Se for 404, não faz retry - retorna imediatamente
                if response.status_code == 404:
                    return response

                # Se for sucesso, retorna
                if response.status_code == 200:
                    return response

                # Para outros erros, faz retry
                if attempt < max_retries:
                    wait_time = random.uniform(1, 2)
                    print(
                        f"Erro HTTP {response.status_code}. Tentativa {attempt}/{max_retries}. Aguardando {wait_time:.2f} segundos...")
                    time.sleep(wait_time)
                else:
                    # Última tentativa - deixa o erro ser tratado pela função que chama
                    response.raise_for_status()

            except Exception as e:
                if attempt < max_retries:
                    wait_time = random.uniform(1, 2)
                    print(
                        f"Erro na requisição: {str(e)}. Tentativa {attempt}/{max_retries}. Aguardando {wait_time:.2f} segundos...")
                    time.sleep(wait_time)
                else:
                    raise

        return response

    def fetch_webhook_leads(limit: int = 500, offset: int = 0) -> Dict[str, Any]:
        """
        Busca dados de leads do webhook com retry.
        """

        def make_leads_request():
            url = f"{BASE_URL}/api/v1/raw-data/by-company"
            params = {
                'company_alias': ALIAS,
                'limit': limit,
                'offset': offset
            }
            return requests.get(url, headers=headers, params=params)

        response = make_request_with_retry(make_leads_request)

        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            # Empresa nova ou sem registros - retorna estrutura vazia válida
            print(f"Empresa {ALIAS} não possui registros no webhook (404). Isso é normal para cadastros novos.")
            return {'data': [], 'total_count': 0, 'returned_count': 0}
        else:
            print(f"Erro na requisição: Status {response.status_code}")
            return {}

    def get_all_webhook_leads() -> List[Dict[str, Any]]:
        """
        Busca todos os dados de leads paginando até a última página.
        """
        all_leads = []
        offset = 0
        limit = 500
        total_count = None

        while True:
            print(f"Buscando dados - Offset: {offset}, Limit: {limit}")

            # Adiciona um pequeno delay entre requisições para evitar rate limiting
            time.sleep(0.5)

            response = fetch_webhook_leads(limit=limit, offset=offset)

            # Verifica se há dados na resposta
            if not response or 'data' not in response:
                print("Nenhum dado encontrado na resposta.")
                break

            # Obtém informações de paginação
            if total_count is None:
                total_count = response.get('total_count', 0)
                print(f"Total de registros disponíveis: {total_count}")

            returned_count = response.get('returned_count', 0)
            leads_batch = response['data']

            print(f"Registros retornados nesta página: {returned_count}")

            # Se não há dados nesta página, para o loop
            if not leads_batch or returned_count == 0:
                print("Nenhum dado encontrado nesta página ou fim da paginação alcançado.")
                break

            all_leads.extend(leads_batch)
            print(f"Total de registros coletados até agora: {len(all_leads)}")

            # Se coletamos todos os registros ou se o número retornado é menor que o limit
            if len(all_leads) >= total_count or returned_count < limit:
                print(f"Coleta finalizada. Total de registros coletados: {len(all_leads)}")
                break

            offset += limit

        return all_leads

    def save_leads_to_csv_and_upload(leads_data: List[Dict[str, Any]], bucket_name: str, destination_blob_name: str):
        """
        Achata os dados de leads, salva em CSV e faz upload para o GCS.
        """
        try:
            if not leads_data:
                print("Nenhum dado de leads para salvar.")
                return

            # Achata todos os registros
            flattened_leads = []
            for lead in leads_data:
                flattened_lead = flatten_dict(lead)
                flattened_leads.append(flattened_lead)

            # Cria DataFrame
            df = pd.DataFrame(flattened_leads)

            if not df.empty:
                print(f"Processando {len(df)} registros de leads...")

                # Salva localmente
                credentials = gcs.load_credentials_from_env()
                local_file_path = f"/tmp/{customer['project_id']}.rdmkt.run_webhook_leads.csv"
                df.to_csv(local_file_path, index=False)

                # Upload para GCS
                gcs.write_file_to_gcs(
                    bucket_name=bucket_name,
                    local_file_path=local_file_path,
                    destination_name=destination_blob_name,
                    credentials=credentials
                )

                print(f"Arquivo salvo no GCS: {destination_blob_name}")
                print(f"Total de colunas: {len(df.columns)}")
                print(f"Total de registros: {len(df)}")
            else:
                print("DataFrame está vazio. Nenhum dado para salvar.")

        except Exception as e:
            print(f"Erro ao salvar dados de leads no GCS: {str(e)}")
            raise

    def main():
        """
        Função principal para extração de leads do webhook.
        """
        print(f"Iniciando extração de leads do webhook para empresa: {ALIAS}")

        # Busca todos os leads
        all_leads = get_all_webhook_leads()

        if not all_leads:
            print(f"Nenhum lead encontrado para empresa {ALIAS}. Isso é normal para cadastros novos.")
            return f'No leads found for company {ALIAS} - this is normal for new registrations'

        # Nome do arquivo
        file_name = f"webhook_users.csv"
        destination_blob_name = f"webhook_users/{file_name}"

        # Salva e faz upload
        save_leads_to_csv_and_upload(all_leads, BUCKET_NAME, destination_blob_name)

        return f'Webhook leads extraction completed. Total leads: {len(all_leads)}'

    main()


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
        }
    ]
