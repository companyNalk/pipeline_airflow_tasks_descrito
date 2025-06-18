import os
import pathlib
import re
import time
import unicodedata
from datetime import datetime
from io import StringIO

import pandas as pd
import requests
from google.cloud import storage


# FUNÇÕES UTILITÁRIAS COMPARTILHADAS
def setup_gcs_credentials():
    """Configura as credenciais do GCS"""
    SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH


def normalize_column_name(name):
    """Normaliza nomes de colunas removendo acentos e caracteres especiais"""
    nfkd = unicodedata.normalize('NFKD', name)
    ascii_name = nfkd.encode('ASCII', 'ignore').decode('ASCII')
    cleaned = re.sub(r"[^\w\s]", "", ascii_name)
    return re.sub(r"\s+", "_", cleaned).lower()


def flatten_dict(d, parent_key='', sep='_'):
    """Achata dicionários aninhados"""
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def convert_boolean_columns(df):
    """Converte colunas booleanas para inteiros (0/1)"""
    # Converte booleanos detectados automaticamente
    bool_columns = df.select_dtypes(include=['bool']).columns
    for col in bool_columns:
        df[col] = df[col].astype(int)

    # Trata colunas que podem ser booleanas mas não foram detectadas
    boolean_columns = ['active', 'status']
    for col in boolean_columns:
        if col in df.columns:
            df[col] = df[col].astype(str)
            df[col] = df[col].map({
                'True': 1, 'true': 1, '1': 1,
                'False': 0, 'false': 0, '0': 0,
                'None': 0, 'nan': 0, 'null': 0
            })
            df[col] = df[col].fillna(0).astype(int)

    return df


def is_date_format(value):
    """Verifica se o valor está em formato de data brasileiro"""
    if not isinstance(value, str):
        return False

    patterns = [
        r'^\d{2}/\d{2}/\d{4}$',
        r'^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$',
        r'^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$'
    ]
    return any(re.match(pattern, value.strip()) for pattern in patterns)


def convert_date_format(value):
    """Converte datas do formato brasileiro para BigQuery"""
    if not isinstance(value, str):
        return value

    value = value.strip()
    try:
        if re.match(r'^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$', value):
            dt = datetime.strptime(value, '%d/%m/%Y %H:%M')
            return dt.strftime('%Y-%m-%d %H:%M:00')
        elif re.match(r'^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$', value):
            dt = datetime.strptime(value, '%d/%m/%Y %H:%M:%S')
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        elif re.match(r'^\d{2}/\d{2}/\d{4}$', value):
            dt = datetime.strptime(value, '%d/%m/%Y')
            return dt.strftime('%Y-%m-%d')
        return value
    except Exception:
        return value


def detect_and_convert_dates(df):
    """Detecta e converte colunas de data automaticamente"""
    for col in df.columns:
        if df[col].dropna().empty:
            continue

        non_null_values = df[col].dropna()
        if len(non_null_values) > 0:
            date_count = sum(1 for val in non_null_values if is_date_format(str(val)))
            if date_count / len(non_null_values) >= 0.7:
                print(f"Convertendo coluna de data: {col}")
                df[col] = df[col].apply(convert_date_format)

    return df


def clean_dataframe(df, convert_dates=False):
    """Limpa e trata o DataFrame"""
    # Normaliza nomes das colunas
    df.columns = [normalize_column_name(col) for col in df.columns]

    # Converte booleanos
    df = convert_boolean_columns(df)

    # Converte datas se solicitado
    if convert_dates:
        df = detect_and_convert_dates(df)

    return df


def make_api_request(url, headers, params):
    """Faz requisição para a API com tratamento de rate limit"""
    while True:
        response = requests.get(url, headers=headers, params=params)

        if response.status_code == 429:
            print("Limite excedido. Aguardando 30 segundos...")
            time.sleep(30)
            continue

        if response.status_code != 200:
            print(f"Erro {response.status_code}: {response.text}")
            return None

        return response.json()


def extract_data_from_endpoint(api_url, endpoint, headers, start_date=None):
    """Extrai dados de um endpoint específico com paginação"""
    params = {"show": 200, "page": 1}

    if start_date:
        params["created_at_start"] = start_date

    all_data = []
    total_collected = 0

    while True:
        data = make_api_request(f"{api_url}/{endpoint}", headers, params)

        if not data or "data" not in data or not isinstance(data["data"], list):
            print(f"Formato inesperado da resposta da API para {endpoint}.")
            break

        items = data["data"]
        if not items:
            print(f"Nenhum item retornado para {endpoint}.")
            break

        # Achata os dados
        for item in items:
            flat = flatten_dict(item)
            all_data.append(flat)

        total_collected += len(items)
        print(f"Coletados {total_collected} registros de {endpoint}...")

        # Verifica se há próxima página
        if "meta" in data and "links" in data["meta"] and "next" in data["meta"]["links"]:
            params["page"] += 1
        else:
            break

        time.sleep(1)

    return all_data


def upload_to_gcs(df, customer, folder, filename):
    """Upload do DataFrame para o GCS"""
    try:
        storage_client = storage.Client(project=customer['project_id'])
        bucket = storage_client.bucket(customer['bucket_name'])
        blob = bucket.blob(f"{folder}/{filename}")

        buffer = StringIO()
        df.to_csv(buffer, sep=";", index=False)
        buffer.seek(0)
        blob.upload_from_string(buffer.getvalue(), content_type="text/csv")

        print(f"Arquivo enviado: gs://{customer['bucket_name']}/{folder}/{filename}")
    except Exception as e:
        print(f"Erro ao enviar para o GCS: {e}")
        raise


def generic_extraction(customer, endpoint, folder, filename, convert_dates=False):
    """Função genérica para extração de dados"""
    setup_gcs_credentials()

    headers = {
        "accept": "application/json",
        "token": customer['api_token']
    }

    # Extrai dados
    data = extract_data_from_endpoint(
        customer['api_url'],
        endpoint,
        headers,
        customer.get('start_date')
    )

    if not data:
        print("Nenhum dado para exportar.")
        return

    # Processa e limpa dados
    df = pd.DataFrame(data)
    df = clean_dataframe(df, convert_dates=convert_dates)

    # Faz upload
    upload_to_gcs(df, customer, folder, filename)


# FUNÇÕES ESPECÍFICAS DE EXTRAÇÃO
def run_teams(customer):
    """Extrai dados de teams"""
    generic_extraction(customer, "teams", "teams", "teams.csv")


def run_team_groups(customer):
    """Extrai dados de team groups"""
    generic_extraction(customer, "teamGroup", "team_groups", "team_groups.csv")


def run_items(customer):
    """Extrai dados de items"""
    generic_extraction(customer, "items", "items", "items.csv", convert_dates=True)


def run_activities(customer):
    """Extrai dados de activities"""
    generic_extraction(customer, "activities", "activities", "activities.csv")


def run_companies(customer):
    """Extrai dados de companies"""
    generic_extraction(customer, "companies", "companies", "companies.csv")


def run_deals(customer):
    """Extrai dados de deals com campos aninhados"""
    setup_gcs_credentials()

    headers = {
        "accept": "application/json",
        "token": customer['api_token']
    }

    params = {
        "show": 200,
        "page": 1,
        "with": "customFields,tags,owner,pipeline,stage"
    }

    if customer.get('start_date'):
        params["created_at_start"] = customer['start_date']

    all_deals = []
    total_collected = 0

    while True:
        data = make_api_request(f"{customer['api_url']}/deals", headers, params)

        if not data or not isinstance(data.get("data"), list):
            print("Formato inesperado da resposta da API para deals.")
            break

        deals = data["data"]
        if not deals:
            break

        # Processa campos aninhados específicos do deals
        for deal in deals:
            flat = {k: v for k, v in deal.items() if k not in ["customFields", "owner", "pipeline", "stage", "tags"]}

            # CustomFields
            if "customFields" in deal and isinstance(deal["customFields"], list):
                for item in deal["customFields"]:
                    if isinstance(item, dict) and "name" in item:
                        name = normalize_column_name(item["name"])
                        flat[name] = item.get("value")

            # Owner
            if "owner" in deal and isinstance(deal["owner"], dict):
                flat["owner_name"] = deal["owner"].get("name")

            # Tags
            if "tags" in deal and isinstance(deal["tags"], list):
                tag_names = [t.get("name") for t in deal["tags"] if isinstance(t, dict) and "name" in t]
                flat["tags_name"] = "|".join(tag_names)

            # Pipeline
            if "pipeline" in deal and isinstance(deal["pipeline"], dict):
                flat["pipeline_funnel_type_name"] = deal["pipeline"].get("funnel_type_name")
                flat["pipeline_name"] = deal["pipeline"].get("name")

            # Stage
            if "stage" in deal and isinstance(deal["stage"], dict):
                flat["stage_name"] = deal["stage"].get("name")

            all_deals.append(flat)

        total_collected += len(deals)
        print(f"Coletados {total_collected} registros de deals...")

        if "meta" in data and "links" in data["meta"] and "next" in data["meta"]["links"]:
            params["page"] += 1
        else:
            break

        time.sleep(1)

    if not all_deals:
        print("Nenhum dado para processar.")
        return

    df = pd.DataFrame(all_deals)
    df = clean_dataframe(df, convert_dates=True)
    upload_to_gcs(df, customer, "deals", "deals.csv")


def run_lost_reasons(customer):
    """Extrai dados de lost reasons"""
    generic_extraction(customer, "lostReasons", "lost_reasons", "lost_reasons.csv")


def run_origins(customer):
    """Extrai dados de origins"""
    generic_extraction(customer, "origins", "origins", "origins.csv")


def get_extraction_tasks():
    """Lista de tarefas de extração"""
    return [
        {'task_id': 'extract_activities', 'python_callable': run_activities},
        {'task_id': 'extract_companies', 'python_callable': run_companies},
        {'task_id': 'extract_deals', 'python_callable': run_deals},
        {'task_id': 'extract_lost_reasons', 'python_callable': run_lost_reasons},
        {'task_id': 'extract_origins', 'python_callable': run_origins},
        {'task_id': 'extract_teams', 'python_callable': run_teams},
        {'task_id': 'extract_team_groups', 'python_callable': run_team_groups},
        {'task_id': 'extract_items', 'python_callable': run_items}
    ]
