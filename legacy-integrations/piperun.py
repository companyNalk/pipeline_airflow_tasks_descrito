"""
Piperun module for data extraction functions.
This module contains functions specific to the Piperun integration.
"""
import os
import pathlib
import re
import time
import unicodedata
from datetime import datetime
from io import StringIO

import pandas as pd
import requests
from core import gcs
from google.cloud import storage


def run_activities(customer):
    API_URL = customer['api_url']
    API_TOKEN = customer['api_token']
    BUCKET_NAME = customer['bucket_name']
    DESTINATION_FOLDER = "activities"
    DESTINATION_FILENAME = "activities.csv"

    SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH

    HEADERS = {
        "accept": "application/json",
        "token": API_TOKEN
    }
    PARAMS = {
        "show": 200,
        "page": 1
    }

    def flatten_dict(d, parent_key='', sep='_'):
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(flatten_dict(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)

    def get_activities():
        all_data = []
        total_collected = 0

        while True:
            response = requests.get(API_URL, headers=HEADERS, params=PARAMS)

            if response.status_code == 429:
                print("Limite excedido. Aguardando 30 segundos...")
                time.sleep(30)
                continue

            if response.status_code != 200:
                print(f"Erro {response.status_code}: {response.text}")
                break

            try:
                data = response.json()
                if "data" not in data or not isinstance(data["data"], list):
                    print("Formato inesperado da resposta da API.")
                    break

                activities = data["data"]
                if not activities:
                    print("Nenhuma atividade retornada.")
                    break

                for item in activities:
                    flat = flatten_dict(item)
                    all_data.append(flat)

                total_collected += len(activities)
                print(f"Coletados {total_collected} registros até agora...")

                if "meta" in data and "links" in data["meta"] and "next" in data["meta"]["links"]:
                    PARAMS["page"] += 1
                else:
                    break

                time.sleep(1)

            except Exception as e:
                print(f"Erro ao processar resposta: {e}")
                raise

        return all_data

    def upload_to_gcs(df: pd.DataFrame):
        try:
            storage_client = storage.Client(project=customer['project_id'])
            bucket = storage_client.bucket(BUCKET_NAME)
            blob = bucket.blob(f"{DESTINATION_FOLDER}/{DESTINATION_FILENAME}")

            buffer = StringIO()
            df.to_csv(buffer, sep=";", index=False)
            buffer.seek(0)
            blob.upload_from_string(buffer.getvalue(), content_type="text/csv")

            print(f"Arquivo enviado com sucesso para: gs://{BUCKET_NAME}/{DESTINATION_FOLDER}/{DESTINATION_FILENAME}")
        except Exception as e:
            print(f"Erro ao enviar para o GCS: {e}")
            raise

    def main():
        data = get_activities()
        if not data:
            print("Nenhum dado para exportar.")
            return

        df = pd.DataFrame(data)
        upload_to_gcs(df)

    # START
    main()


def run_companies(customer):
    API_URL = customer['api_url']
    API_TOKEN = customer['api_token']
    BUCKET_NAME = customer['bucket_name']
    DESTINATION_FOLDER = "companies"
    DESTINATION_FILENAME = "companies.csv"

    SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH

    HEADERS = {
        "accept": "application/json",
        "token": API_TOKEN
    }
    PARAMS = {
        "show": 200,
        "page": 1
    }

    def normalize_column_name(name):
        nfkd = unicodedata.normalize('NFKD', name)
        ascii_name = nfkd.encode('ASCII', 'ignore').decode('ASCII')
        cleaned = re.sub(r"[^\w\s]", "", ascii_name)
        return re.sub(r"\s+", "_", cleaned).lower()

    def flatten_dict(d, parent_key='', sep='_'):
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(flatten_dict(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)

    def get_companies():
        all_data = []
        total_collected = 0

        while True:
            response = requests.get(API_URL, headers=HEADERS, params=PARAMS)

            if response.status_code == 429:
                print("Limite excedido. Aguardando 30 segundos...")
                time.sleep(30)
                continue

            if response.status_code != 200:
                print(f"Erro {response.status_code}: {response.text}")
                break

            try:
                data = response.json()
                if "data" not in data or not isinstance(data["data"], list):
                    print("Formato inesperado da resposta da API.")
                    break

                companies = data["data"]
                if not companies:
                    print("Nenhuma empresa retornada.")
                    break

                for item in companies:
                    flat = flatten_dict(item)
                    all_data.append(flat)

                total_collected += len(companies)
                print(f"Coletados {total_collected} registros até agora...")

                if "meta" in data and "links" in data["meta"] and "next" in data["meta"]["links"]:
                    PARAMS["page"] += 1
                else:
                    break

                time.sleep(1)

            except Exception as e:
                print(f"Erro ao processar resposta: {e}")
                raise

        return all_data

    def convert_boolean_to_int(df):
        """Converte colunas booleanas para inteiros (0/1)."""
        bool_columns = df.select_dtypes(include=['bool']).columns
        for col in bool_columns:
            df[col] = df[col].astype(int)
        return df

    def clean_dataframe(df):
        """Realiza limpeza e tratamento do DataFrame."""
        # Normaliza os nomes das colunas
        df.columns = [normalize_column_name(col) for col in df.columns]

        # Converte valores booleanos para int (0/1)
        df = convert_boolean_to_int(df)

        # Trata colunas específicas que sabemos que são booleanas mas podem não ser
        # detectadas corretamente pelo pandas
        boolean_columns = [
            'status', 'is_brand', 'is_supplier', 'is_client',
            'is_carrier', 'is_franchise', 'is_channel',
            'is_distributor', 'is_manufacturer', 'is_partner'
        ]

        for col in boolean_columns:
            if col in df.columns:
                # Primeiro converte para string para lidar com possíveis valores nulos
                df[col] = df[col].astype(str)
                # Mapeia valores para 0/1
                df[col] = df[col].map({'True': 1, 'true': 1, '1': 1,
                                       'False': 0, 'false': 0, '0': 0,
                                       'None': 0, 'nan': 0, 'null': 0})
                # Preenche valores nulos com 0
                df[col] = df[col].fillna(0).astype(int)

        return df

    def upload_to_gcs(df: pd.DataFrame):
        try:
            storage_client = storage.Client(project=customer['project_id'])
            bucket = storage_client.bucket(BUCKET_NAME)
            blob = bucket.blob(f"{DESTINATION_FOLDER}/{DESTINATION_FILENAME}")

            buffer = StringIO()
            df.to_csv(buffer, sep=";", index=False)
            buffer.seek(0)
            blob.upload_from_string(buffer.getvalue(), content_type="text/csv")

            print(f"Arquivo enviado com sucesso para: gs://{BUCKET_NAME}/{DESTINATION_FOLDER}/{DESTINATION_FILENAME}")
        except Exception as e:
            print(f"Erro ao enviar para o GCS: {e}")
            raise

    def main():
        data = get_companies()
        if not data:
            print("Nenhum dado para exportar.")
            return

        df = pd.DataFrame(data)

        # Limpa e trata o DataFrame
        df = clean_dataframe(df)

        upload_to_gcs(df)

    # START
    main()


def run_deals(customer):
    API_URL = customer['api_url']
    API_TOKEN = customer['api_token']
    BUCKET_NAME = customer['bucket_name']
    DESTINATION_FOLDER = "deals"
    DESTINATION_FILENAME = "deals.csv"

    SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH

    NESTED_FIELDS = ["customFields", "owner", "pipeline", "stage", "tags"]

    HEADERS = {
        "accept": "application/json",
        "token": API_TOKEN
    }
    PARAMS = {
        "show": 200,
        "page": 1,
        "with": "customFields,tags,owner,pipeline,stage"
    }

    def normalize_column_name(name):
        nfkd = unicodedata.normalize('NFKD', name)
        ascii_name = nfkd.encode('ASCII', 'ignore').decode('ASCII')
        cleaned = re.sub(r"[^\w\s]", "", ascii_name)
        return re.sub(r"\s+", "_", cleaned).lower()

    def flatten_dict(d, parent_key="", sep="_"):
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(flatten_dict(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)

    def flatten_nested_fields(deals):
        flat_deals = []

        for deal in deals:
            flat = {k: v for k, v in deal.items() if k not in NESTED_FIELDS}

            for field in NESTED_FIELDS:
                if field not in deal:
                    continue

                nested_data = deal[field]

                if field == "customFields" and isinstance(nested_data, list):
                    for item in nested_data:
                        if isinstance(item, dict):
                            name = item.get("name")
                            value = item.get("value")
                            if name:
                                normalized = normalize_column_name(name)
                                flat[normalized] = value

                elif field == "owner" and isinstance(nested_data, dict):
                    flat["owner_name"] = nested_data.get("name")

                elif field == "tags" and isinstance(nested_data, list):
                    tag_names = [t.get("name") for t in nested_data if isinstance(t, dict) and "name" in t]
                    flat["tags_name"] = "|".join(tag_names)

                elif field == "pipeline" and isinstance(nested_data, dict):
                    flat["pipeline_funnel_type_name"] = nested_data.get("funnel_type_name")
                    flat["pipeline_name"] = nested_data.get("name")

                elif field == "stage" and isinstance(nested_data, dict):
                    flat["stage_name"] = nested_data.get("name")

            flat_deals.append(flat)

        return flat_deals

    def get_deals():
        all_deals = []
        total_collected = 0

        while True:
            response = requests.get(API_URL, headers=HEADERS, params=PARAMS)

            if response.status_code == 429:
                print("Limite de requisições excedido. Aguardando 30 segundos...")
                time.sleep(30)
                continue

            if response.status_code != 200:
                print(f"Erro ao coletar dados: {response.status_code} - {response.text}")
                break

            try:
                data = response.json()

                if isinstance(data, dict) and "data" in data:
                    deals = data["data"]
                else:
                    print("Formato inesperado da resposta da API.")
                    break

                if not deals:
                    print("Nenhum dado retornado.")
                    break

                all_deals.extend(deals)
                total_collected += len(deals)
                print(f"Coletados {total_collected} registros até agora...")

                if "meta" in data and "links" in data["meta"] and "next" in data["meta"]["links"]:
                    PARAMS["page"] += 1
                else:
                    break

                time.sleep(1)
            except Exception as e:
                print(f"Erro ao processar JSON: {e}")
                raise

        return all_deals

    def is_date_format(value):
        """Verifica se o valor parece estar em formato de data DD/MM/YYYY"""
        if not isinstance(value, str):
            return False

        # Padrões de data comuns no Brasil
        patterns = [
            r'^\d{2}/\d{2}/\d{4}$',  # DD/MM/YYYY
            r'^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$',  # DD/MM/YYYY HH:MM
            r'^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$'  # DD/MM/YYYY HH:MM:SS
        ]

        return any(re.match(pattern, value.strip()) for pattern in patterns)

    def convert_date_format(value):
        """Converte datas do formato brasileiro para o formato BigQuery"""
        if not isinstance(value, str):
            return value

        value = value.strip()

        try:
            # Tenta converter DD/MM/YYYY HH:MM
            if re.match(r'^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$', value):
                dt = datetime.strptime(value, '%d/%m/%Y %H:%M')
                return dt.strftime('%Y-%m-%d %H:%M:00')

            # Tenta converter DD/MM/YYYY HH:MM:SS
            elif re.match(r'^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$', value):
                dt = datetime.strptime(value, '%d/%m/%Y %H:%M:%S')
                return dt.strftime('%Y-%m-%d %H:%M:%S')

            # Tenta converter apenas DD/MM/YYYY (sem hora)
            elif re.match(r'^\d{2}/\d{2}/\d{4}$', value):
                dt = datetime.strptime(value, '%d/%m/%Y')
                return dt.strftime('%Y-%m-%d')

            return value
        except Exception:
            # Se falhar na conversão, retorna o valor original
            return value

    def detect_and_convert_date_columns(df):
        """Detecta colunas de data e converte para o formato BigQuery"""
        # Exemplo das primeiras linhas para detecção
        sample = df.head(10)

        date_columns = []

        # Identifica colunas que parecem conter datas
        for col in df.columns:
            # Pula colunas vazias
            if df[col].dropna().empty:
                continue

            # Verifica se pelo menos 70% dos valores não-nulos parecem ser datas
            non_null_values = df[col].dropna()
            if len(non_null_values) > 0:
                date_count = sum(1 for val in non_null_values if is_date_format(str(val)))
                if date_count / len(non_null_values) >= 0.7:
                    date_columns.append(col)

        # Converte as colunas identificadas
        for col in date_columns:
            print(f"Convertendo coluna de data: {col}")
            df[col] = df[col].apply(convert_date_format)

        return df

    def upload_csv_to_gcs(df):
        try:
            # Detecta e converte colunas de data antes de enviar
            df = detect_and_convert_date_columns(df)

            storage_client = storage.Client(project=customer['project_id'])
            bucket = storage_client.bucket(BUCKET_NAME)
            blob = bucket.blob(f"{DESTINATION_FOLDER}/{DESTINATION_FILENAME}")

            csv_buffer = StringIO()
            df.to_csv(csv_buffer, sep=";", index=False)
            csv_buffer.seek(0)

            blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')
            print(f"Arquivo enviado com sucesso para: gs://{BUCKET_NAME}/{DESTINATION_FOLDER}/{DESTINATION_FILENAME}")
        except Exception as e:
            print(f"Erro ao enviar para o GCS: {e}")
            raise

    def main():
        deals = get_deals()
        if not deals:
            print("Nenhum dado para processar.")
            return

        flat_deals = flatten_nested_fields(deals)
        df = pd.DataFrame(flat_deals)
        upload_csv_to_gcs(df)

    # START
    main()


def run_lost_reasons(customer):
    API_URL = customer['api_url']
    API_TOKEN = customer['api_token']
    BUCKET_NAME = customer['bucket_name']
    DESTINATION_FOLDER = "lost_reasons"
    DESTINATION_FILENAME = "lost_reasons.csv"

    SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH

    HEADERS = {
        "accept": "application/json",
        "token": API_TOKEN
    }
    PARAMS = {
        "show": 200,
        "page": 1
    }

    def normalize_column_name(name):
        nfkd = unicodedata.normalize('NFKD', name)
        ascii_name = nfkd.encode('ASCII', 'ignore').decode('ASCII')
        cleaned = re.sub(r"[^\w\s]", "", ascii_name)
        return re.sub(r"\s+", "_", cleaned).lower()

    def flatten_dict(d, parent_key='', sep='_'):
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(flatten_dict(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)

    def get_lost_reasons():
        all_data = []
        total_collected = 0

        while True:
            response = requests.get(API_URL, headers=HEADERS, params=PARAMS)

            if response.status_code == 429:
                print("Limite excedido. Aguardando 30 segundos...")
                time.sleep(30)
                continue

            if response.status_code != 200:
                print(f"Erro {response.status_code}: {response.text}")
                break

            try:
                data = response.json()
                if "data" not in data or not isinstance(data["data"], list):
                    print("Formato inesperado da resposta da API.")
                    break

                lost_reasons = data["data"]
                if not lost_reasons:
                    print("Nenhuma razão de perda retornada.")
                    break

                for item in lost_reasons:
                    flat = flatten_dict(item)
                    all_data.append(flat)

                total_collected += len(lost_reasons)
                print(f"Coletados {total_collected} registros até agora...")

                if "meta" in data and "links" in data["meta"] and "next" in data["meta"]["links"]:
                    PARAMS["page"] += 1
                else:
                    break

                time.sleep(1)

            except Exception as e:
                print(f"Erro ao processar resposta: {e}")
                raise

        return all_data

    def upload_to_gcs(df: pd.DataFrame):
        try:
            storage_client = storage.Client(project=customer['project_id'])
            bucket = storage_client.bucket(BUCKET_NAME)
            blob = bucket.blob(f"{DESTINATION_FOLDER}/{DESTINATION_FILENAME}")

            buffer = StringIO()
            df.to_csv(buffer, sep=";", index=False)
            buffer.seek(0)
            blob.upload_from_string(buffer.getvalue(), content_type="text/csv")

            print(f"Arquivo enviado com sucesso para: gs://{BUCKET_NAME}/{DESTINATION_FOLDER}/{DESTINATION_FILENAME}")
        except Exception as e:
            print(f"Erro ao enviar para o GCS: {e}")
            raise

    def main():
        data = get_lost_reasons()
        if not data:
            print("Nenhum dado para exportar.")
            return

        df = pd.DataFrame(data)

        # Normaliza os nomes das colunas se necessário
        df.columns = [normalize_column_name(col) for col in df.columns]

        upload_to_gcs(df)

    # START
    main()


def run_origins(customer):
    API_URL = customer['api_url']
    API_TOKEN = customer['api_token']
    BUCKET_NAME = customer['bucket_name']
    DESTINATION_FOLDER = "origins"
    DESTINATION_FILENAME = "origins.csv"

    SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH

    HEADERS = {
        "accept": "application/json",
        "token": API_TOKEN
    }
    PARAMS = {
        "show": 200,
        "page": 1
    }

    def normalize_column_name(name):
        nfkd = unicodedata.normalize('NFKD', name)
        ascii_name = nfkd.encode('ASCII', 'ignore').decode('ASCII')
        cleaned = re.sub(r"[^\w\s]", "", ascii_name)
        return re.sub(r"\s+", "_", cleaned).lower()

    def flatten_dict(d, parent_key='', sep='_'):
        items = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(flatten_dict(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)

    def get_origins():
        all_data = []
        total_collected = 0

        while True:
            response = requests.get(API_URL, headers=HEADERS, params=PARAMS)

            if response.status_code == 429:
                print("Limite excedido. Aguardando 30 segundos...")
                time.sleep(30)
                continue

            if response.status_code != 200:
                print(f"Erro {response.status_code}: {response.text}")
                break

            try:
                data = response.json()
                if "data" not in data or not isinstance(data["data"], list):
                    print("Formato inesperado da resposta da API.")
                    break

                origins = data["data"]
                if not origins:
                    print("Nenhuma origem retornada.")
                    break

                for item in origins:
                    flat = flatten_dict(item)
                    all_data.append(flat)

                total_collected += len(origins)
                print(f"Coletados {total_collected} registros até agora...")

                if "meta" in data and "links" in data["meta"] and "next" in data["meta"]["links"]:
                    PARAMS["page"] += 1
                else:
                    break

                time.sleep(1)

            except Exception as e:
                print(f"Erro ao processar resposta: {e}")
                raise

        return all_data

    def upload_to_gcs(df: pd.DataFrame):
        try:
            storage_client = storage.Client(project=customer['project_id'])
            bucket = storage_client.bucket(BUCKET_NAME)
            blob = bucket.blob(f"{DESTINATION_FOLDER}/{DESTINATION_FILENAME}")

            buffer = StringIO()
            df.to_csv(buffer, sep=";", index=False)
            buffer.seek(0)
            blob.upload_from_string(buffer.getvalue(), content_type="text/csv")

            print(f"Arquivo enviado com sucesso para: gs://{BUCKET_NAME}/{DESTINATION_FOLDER}/{DESTINATION_FILENAME}")
        except Exception as e:
            print(f"Erro ao enviar para o GCS: {e}")
            raise

    def main():
        data = get_origins()
        if not data:
            print("Nenhum dado para exportar.")
            return

        df = pd.DataFrame(data)

        # Normaliza os nomes das colunas se necessário
        df.columns = [normalize_column_name(col) for col in df.columns]

        # Converte valores booleanos para int (0/1) se necessário
        if 'active' in df.columns:
            df['active'] = df['active'].astype(int)

        upload_to_gcs(df)

    # START
    main()


def get_extraction_tasks():
    """
    Get the list of data extraction tasks for Moskit.

    Returns:
        list: List of task configurations
    """
    return [
        {
            'task_id': 'extract_activities',
            'python_callable': run_activities
        },
        {
            'task_id': 'extract_companies',
            'python_callable': run_companies
        },
        {
            'task_id': 'extract_deals',
            'python_callable': run_deals
        },
        {
            'task_id': 'extract_lost_reasons',
            'python_callable': run_lost_reasons
        },
        {
            'task_id': 'extract_origins',
            'python_callable': run_origins
        }
    ]
