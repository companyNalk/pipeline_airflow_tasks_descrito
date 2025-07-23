"""
Moskit module for data extraction functions.
This module contains functions specific to the Moskit integration.
"""

import csv
import io
import os
import re
import time
import unicodedata

import requests
from core import gcs
from google.cloud import storage

# Variáveis globais que serão inicializadas
storage_client = None
bucket = None


def initialize_gcs(customer):
    """
    Inicializa o cliente do Google Cloud Storage.
    Deve ser chamada no início de cada função principal.
    """
    global storage_client, bucket

    BUCKET_NAME = customer['bucket_name']
    import pathlib
    SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()

    # Autenticação no Google Cloud Storage
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH
    credentials = gcs.load_credentials_from_env()
    storage_client = storage.Client(credentials=credentials)
    bucket = storage_client.bucket(BUCKET_NAME)


def normalize_column_name(name):
    """
    Normaliza nomes de campos para uso em CSV.
    """
    nfkd = unicodedata.normalize('NFKD', name)
    ascii_name = nfkd.encode('ASCII', 'ignore').decode('ASCII')
    cleaned = re.sub(r"[^\w\s]", "", ascii_name)
    return re.sub(r"\s+", "_", cleaned).lower()


def upload_to_gcs(data, destination_path):
    global bucket

    if bucket is None:
        raise Exception("GCS não foi inicializado. Chame initialize_gcs() primeiro.")

    if not data:
        print(f"[WARNING] Sem dados para enviar para {destination_path}")
        return

    try:
        # Determinar todas as chaves possíveis (colunas)
        all_keys = set()
        for item in data:
            all_keys.update(item.keys())

        # Normalizar todos os nomes de colunas
        normalized_keys = {key: normalize_column_name(key) for key in all_keys}
        sorted_keys = sorted(list(all_keys))

        # Criar um buffer na memória para o CSV
        output = io.StringIO()
        csv_writer = csv.writer(output, delimiter=';', quotechar='"', quoting=csv.QUOTE_MINIMAL)

        # Escrever cabeçalho normalizado
        csv_writer.writerow([normalized_keys[key] for key in sorted_keys])

        # Escrever linhas
        for item in data:
            row = [item.get(key, "") for key in sorted_keys]
            csv_writer.writerow(row)

        # Upload para o GCS
        blob = bucket.blob(destination_path)
        blob.upload_from_string(output.getvalue(), content_type="text/csv")

        print(f"[SUCCESS] Arquivo salvo em gs://{bucket.name}/{destination_path}")
    except Exception as e:
        print(f"[ERROR] Falha ao fazer upload para {destination_path}: {str(e)}")
        raise


def run(customer):
    # Inicializar GCS no início da função
    initialize_gcs(customer)

    # Configurações da API
    API_KEY = customer['api_key']
    BASE_URL = "https://api.ms.prod.moskit.services/v2/deals"
    CUSTOM_FIELDS_URL = "https://api.ms.prod.moskit.services/v2/customFields"
    STAGES_URL = "https://api.ms.prod.moskit.services/v2/stages"
    PIPELINES_URL = "https://api.ms.prod.moskit.services/v2/pipelines"
    LOST_REASONS_URL = "https://api.ms.prod.moskit.services/v2/lostReasons"
    USERS_URL = "https://api.ms.prod.moskit.services/v2/users"
    HEADERS = {"Accept": "application/json", "apikey": API_KEY}

    # Parâmetros de busca
    QUANTITY = 50
    MAX_RETRIES = 5
    TIMEOUT = 10

    # Cache
    custom_fields_mapping = {}
    stage_to_pipeline = {}
    pipeline_names = {}
    stage_names = {}
    lost_reason_cache = {}
    user_cache = {}

    # Função para buscar nomes das entidades
    def get_name(url, entity_id, cache):
        if not entity_id:
            return ""
        if entity_id in cache:
            return cache[entity_id]

        for attempt in range(MAX_RETRIES):
            try:
                response = requests.get(f"{url}/{entity_id}", headers=HEADERS, timeout=TIMEOUT)
                if response.status_code == 200:
                    data = response.json()
                    name = data.get("name", "")
                    cache[entity_id] = name
                    return name
                elif response.status_code == 429:
                    time.sleep(2)
                else:
                    return ""
            except requests.exceptions.RequestException:
                time.sleep(2)
        return ""

    # Função para buscar nome de um campo personalizado pelo ID
    def get_custom_field_name(field_id):
        if field_id in custom_fields_mapping:
            return custom_fields_mapping[field_id]

        try:
            response = requests.get(f"{CUSTOM_FIELDS_URL}/{field_id}", headers=HEADERS, timeout=TIMEOUT)
            if response.status_code == 200:
                data = response.json()
                field_name = normalize_column_name(data.get("name", field_id))
                custom_fields_mapping[field_id] = field_name
                return field_name
            elif response.status_code == 429:
                time.sleep(2)
        except requests.exceptions.RequestException:
            time.sleep(2)

        return field_id

    # Função para buscar lista de estágios e pipelines
    def fetch_stages_and_pipelines():
        print("[INFO] Buscando lista de estágios e pipelines...")

        stages_data = []
        try:
            response = requests.get(STAGES_URL, headers=HEADERS, timeout=TIMEOUT)
            if response.status_code == 200:
                data = response.json()
                for stage in data:
                    stage_id = stage["id"]
                    stage_names[stage_id] = stage.get("name", "")
                    pipeline_id = stage.get("pipeline", {}).get("id", "")
                    stage_to_pipeline[stage_id] = pipeline_id

                    stages_data.append({
                        "id": stage_id,
                        "name": stage.get("name", ""),
                        "pipeline_id": pipeline_id,
                        "pipeline_name": get_name(PIPELINES_URL, pipeline_id, pipeline_names)
                    })
        except requests.exceptions.RequestException:
            pass

        pipelines_data = []
        try:
            response = requests.get(PIPELINES_URL, headers=HEADERS, timeout=TIMEOUT)
            if response.status_code == 200:
                data = response.json()
                for pipeline in data:
                    pipeline_id = pipeline["id"]
                    pipeline_names[pipeline_id] = pipeline["name"]

                    pipelines_data.append({
                        "id": pipeline_id,
                        "name": pipeline["name"]
                    })
        except requests.exceptions.RequestException:
            pass

        print(f"[SUCCESS] {len(stage_names)} estágios e {len(pipeline_names)} pipelines coletados!")

        # Usar a função global upload_to_gcs
        upload_to_gcs(stages_data, "stages/stages.csv")
        upload_to_gcs(pipelines_data, "pipelines/pipelines.csv")

    # Função para buscar negócios
    def fetch_deals():
        deals = []
        next_page_token = None

        print("[INFO] Iniciando coleta de negócios...")

        while True:
            params = {"quantity": QUANTITY, "sort": "dateCreated", "order": "ASC"}
            if next_page_token:
                params["nextPageToken"] = next_page_token

            try:
                response = requests.get(BASE_URL, headers=HEADERS, params=params, timeout=TIMEOUT)
                if response.status_code == 200:
                    data = response.json()
                    deals.extend(data)
                    print(f"[INFO] Total de negócios coletados até agora: {len(deals)}")

                    next_page_token = response.headers.get("X-Moskit-Listing-Next-Page-Token")
                    if not next_page_token:
                        break
                elif response.status_code == 429:
                    time.sleep(2)
                else:
                    break
            except requests.exceptions.RequestException:
                time.sleep(2)

        print(f"[SUCCESS] Coleta de negócios finalizada. Total coletado: {len(deals)}")
        return deals

    # Função para buscar usuários
    def fetch_users():
        users = []
        next_page_token = None

        print("[INFO] Iniciando coleta de usuários...")

        while True:
            params = {"quantity": QUANTITY}
            if next_page_token:
                params["nextPageToken"] = next_page_token

            try:
                response = requests.get(USERS_URL, headers=HEADERS, params=params, timeout=TIMEOUT)
                if response.status_code == 200:
                    data = response.json()
                    users.extend(data)
                    print(f"[INFO] Total de usuários coletados até agora: {len(users)}")

                    next_page_token = response.headers.get("X-Moskit-Listing-Next-Page-Token")
                    if not next_page_token:
                        break
                elif response.status_code == 429:
                    time.sleep(2)
                else:
                    break
            except requests.exceptions.RequestException:
                time.sleep(2)

        print(f"[SUCCESS] Coleta de usuários finalizada. Total coletado: {len(users)}")

        users_data = []
        for user in users:
            users_data.append({
                "id": user.get("id", ""),
                "name": user.get("name", ""),
                "email": user.get("email", "")
            })

        # Usar a função global upload_to_gcs
        upload_to_gcs(users_data, "users/users.csv")
        return users

    # Função para buscar motivos de perda
    def fetch_lost_reasons():
        print("[INFO] Iniciando coleta de motivos de perda...")

        lost_reasons = []
        try:
            response = requests.get(LOST_REASONS_URL, headers=HEADERS, timeout=TIMEOUT)
            if response.status_code == 200:
                data = response.json()
                lost_reasons = data

                for reason in lost_reasons:
                    lost_reason_cache[reason["id"]] = reason.get("name", "")

                print(f"[INFO] Total de motivos de perda coletados: {len(lost_reasons)}")
            else:
                print(f"[ERROR] Falha ao buscar motivos de perda. Status code: {response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"[ERROR] Exceção ao buscar motivos de perda: {str(e)}")
            raise

        lost_reasons_data = []
        for reason in lost_reasons:
            lost_reasons_data.append({
                "id": reason.get("id", ""),
                "name": reason.get("name", "")
            })

        # Usar a função global upload_to_gcs
        upload_to_gcs(lost_reasons_data, "lost_reasons/lost_reasons.csv")
        return lost_reasons

    # Função para buscar campos personalizados
    def fetch_custom_fields():
        print("[INFO] Iniciando coleta de campos personalizados...")

        custom_fields = []
        try:
            response = requests.get(CUSTOM_FIELDS_URL, headers=HEADERS, timeout=TIMEOUT)
            if response.status_code == 200:
                data = response.json()
                custom_fields = data

                for field in custom_fields:
                    field_id = field["id"]
                    field_name = normalize_column_name(field.get("name", field_id))
                    custom_fields_mapping[field_id] = field_name

                print(f"[INFO] Total de campos personalizados coletados: {len(custom_fields)}")
            else:
                print(f"[ERROR] Falha ao buscar campos personalizados. Status code: {response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"[ERROR] Exceção ao buscar campos personalizados: {str(e)}")
            raise

        custom_fields_data = []
        for field in custom_fields:
            custom_fields_data.append({
                "id": field.get("id", ""),
                "name": field.get("name", ""),
                "type": field.get("type", "")
            })

        # Usar a função global upload_to_gcs
        upload_to_gcs(custom_fields_data, "custom_fields/custom_fields.csv")
        return custom_fields

    # Função para processar negócios e salvar no GCS
    def process_and_save_deals(deals):
        print("[INFO] Identificando campos personalizados...")

        custom_field_ids = set()
        for deal in deals:
            for field in deal.get("entityCustomFields", []):
                custom_field_ids.add(field["id"])

        print(f"[INFO] Total de campos personalizados identificados: {len(custom_field_ids)}")

        for field_id in custom_field_ids:
            get_custom_field_name(field_id)

        print("[INFO] Preparando os dados para inserção no GCS...")

        processed_deals = []
        for i, deal in enumerate(deals, start=1):
            stage_id = deal.get("stage", {}).get("id", "")
            pipeline_id = stage_to_pipeline.get(stage_id, "")
            pipeline_name = pipeline_names.get(pipeline_id, "")
            stage_name = stage_names.get(stage_id, "")

            processed_deal = {
                "id": deal.get("id", ""),
                "name": deal.get("name", ""),
                "dateCreated": deal.get("dateCreated", ""),
                "status": deal.get("status", ""),
                "price": deal.get("price", ""),
                "closeDate": deal.get("closeDate", ""),
                "source": deal.get("source", ""),
                "origin": deal.get("origin", ""),
                "createdBy_id": deal.get("createdBy", {}).get("id", ""),
                "responsible_id": deal.get("responsible", {}).get("id", ""),
                "responsible_name": get_name(USERS_URL, deal.get("responsible", {}).get("id", ""), user_cache),
                "previsionCloseDate": deal.get("previsionCloseDate", ""),
                "stage_id": stage_id,
                "stage_name": stage_name,
                "pipeline_id": pipeline_id,
                "pipeline_name": pipeline_name,
                "lostReason_id": deal.get("lostReason", {}).get("id", ""),
                "lostReason_name": get_name(LOST_REASONS_URL, deal.get("lostReason", {}).get("id", ""),
                                            lost_reason_cache),
            }

            # Adicionar campos personalizados
            for field in deal.get("entityCustomFields", []):
                field_name = custom_fields_mapping.get(field["id"], field["id"])
                processed_deal[f"custom_{field_name}"] = field.get("textValue", "")

            processed_deals.append(processed_deal)

            if i % 100 == 0:
                print(f"[INFO] {i} negócios processados...")

        # Usar a função global upload_to_gcs
        upload_to_gcs(processed_deals, "deals/deals.csv")

    # Executar todas as funções
    fetch_custom_fields()
    fetch_lost_reasons()
    fetch_users()
    fetch_stages_and_pipelines()

    deals = fetch_deals()
    process_and_save_deals(deals)

    print("[COMPLETE] Processo finalizado com sucesso!")


def refactor_deals_custom_fields(customer):
    import pandas as pd
    import time

    # Inicializar GCS no início da função
    initialize_gcs(customer)

    API_KEY = customer['api_key']

    def fetch_moskit_deals_with_custom_fields(quantity: int = 50, limit_pages: int = None) -> pd.DataFrame:
        """
        Busca todos os deals da API Moskit com paginação usando nextPageToken,
        processa os campos personalizados substituindo IDs pelos nomes amigáveis,
        e retorna um único DataFrame com os resultados combinados.
        """
        base_url = "https://api.ms.prod.moskit.services/v2"
        deals_url = f"{base_url}/deals"
        custom_fields_url = f"{base_url}/customFields"

        headers = {
            "accept": "application/json",
            "apikey": API_KEY
        }

        # --- Fetching Deals
        deals_params = {
            "order": "DESC",
            "sort": "dateCreated",
            "quantity": min(quantity, 50)
        }

        all_deals = []
        next_page_token_deals = None
        page_count_deals = 0

        print("🔄 Buscando dados de deals paginados...")

        while True:
            if next_page_token_deals:
                deals_params['nextPageToken'] = next_page_token_deals
            else:
                deals_params.pop('nextPageToken', None)

            max_retries = 13
            for attempt in range(max_retries):
                try:
                    response = requests.get(deals_url, headers=headers, params=deals_params, timeout=30)

                    if response.status_code == 200:
                        break
                    elif response.status_code == 429:
                        wait_time = 5 * (attempt + 1)
                        print(
                            f"⚠️ Rate limit atingido. Aguardando {wait_time} segundos... (tentativa {attempt + 1}/{max_retries})")
                        time.sleep(wait_time)
                    else:
                        raise Exception(f"Erro ao buscar deals: {response.status_code} - {response.text}")

                except requests.exceptions.RequestException as e:
                    if attempt == max_retries - 1:
                        raise Exception(f"Erro de conexão após {max_retries} tentativas: {str(e)}")
                    print(f"⚠️ Erro de conexão. Tentativa {attempt + 1}/{max_retries}. Aguardando 3 segundos...")
                    time.sleep(3)

            if response.status_code != 200:
                raise Exception(
                    f"Erro ao buscar deals após {max_retries} tentativas: {response.status_code} - {response.text}")

            deals_data = response.json()
            all_deals.extend(deals_data)

            next_page_token_deals = response.headers.get("X-Moskit-Listing-Next-Page-Token")

            page_count_deals += 1
            print(f"✅ Página {page_count_deals} de deals carregada. Registros acumulados: {len(all_deals)}")

            if not next_page_token_deals:
                print("🏁 Todas as páginas de deals foram carregadas.")
                break

            if limit_pages and page_count_deals >= limit_pages:
                print("⚠️ Limite de páginas de deals atingido (limit_pages).")
                break

            # Pequena pausa entre requisições para evitar rate limit
            time.sleep(0.5)

        df = pd.json_normalize(all_deals)

        # --- Fetching Custom Fields
        all_custom_fields = []
        next_page_token_custom_fields = None
        page_count_custom_fields = 0

        custom_fields_params = {
            "order": "ASC",
            "quantity": 50
        }

        print("🔄 Buscando dados de custom fields paginados...")

        while True:
            if next_page_token_custom_fields:
                custom_fields_params['nextPageToken'] = next_page_token_custom_fields
            else:
                custom_fields_params.pop('nextPageToken', None)

            # Fazer requisição com retry para rate limit
            max_retries = 13
            for attempt in range(max_retries):
                try:
                    response = requests.get(custom_fields_url, headers=headers, params=custom_fields_params, timeout=30)

                    if response.status_code == 200:
                        break
                    elif response.status_code == 429:
                        wait_time = 5 * (attempt + 1)
                        print(
                            f"⚠️ Rate limit atingido. Aguardando {wait_time} segundos... (tentativa {attempt + 1}/{max_retries})")
                        time.sleep(wait_time)
                    else:
                        raise Exception(f"Erro ao buscar custom fields: {response.status_code} - {response.text}")

                except requests.exceptions.RequestException as e:
                    if attempt == max_retries - 1:
                        raise Exception(f"Erro de conexão após {max_retries} tentativas: {str(e)}")
                    print(f"⚠️ Erro de conexão. Tentativa {attempt + 1}/{max_retries}. Aguardando 3 segundos...")
                    time.sleep(3)

            if response.status_code != 200:
                raise Exception(
                    f"Erro ao buscar custom fields após {max_retries} tentativas: {response.status_code} - {response.text}")

            custom_fields_data = response.json()
            all_custom_fields.extend(custom_fields_data)

            next_page_token_custom_fields = response.headers.get("X-Moskit-Listing-Next-Page-Token")

            page_count_custom_fields += 1
            print(
                f"✅ Página {page_count_custom_fields} de custom fields carregada. Registros acumulados: {len(all_custom_fields)}")

            if not next_page_token_custom_fields:
                print("🏁 Todas as páginas de custom fields foram carregadas.")
                break

            # Pequena pausa entre requisições
            time.sleep(0.5)

        # Cria mapeamento {id: name} apenas para módulo DEAL
        id_to_name = {}
        for field in all_custom_fields:
            if field.get('module') == 'DEAL':
                id_to_name[field['id']] = field['name']

        # Substitui IDs pelos nomes já na extração
        def extract_fields(fields):
            result = {}
            if isinstance(fields, list):
                for item in fields:
                    if isinstance(item, dict):
                        field_id = item.get('id')
                        value = item.get('textValue')
                        field_name = id_to_name.get(field_id, field_id)
                        if field_name:
                            result[field_name] = value
            return result

        # Cria novas colunas a partir de entityCustomFields com nomes amigáveis
        new_cols = df['entityCustomFields'].apply(extract_fields).apply(pd.Series)
        df = pd.concat([df.drop(columns=['entityCustomFields']), new_cols], axis=1)

        # Converter DataFrame para lista de dicionários
        # Substituir valores NaN por strings vazias para evitar problemas no CSV
        df_clean = df.fillna('')
        deals_list = df_clean.to_dict('records')

        # Usar a função upload_to_gcs que trabalha com lista de dicionários
        upload_to_gcs(deals_list, "refactor_deals_custom_fields/refactor_deals_custom_fields.csv")

    fetch_moskit_deals_with_custom_fields()


def get_extraction_tasks():
    """
    Get the list of data extraction tasks for Moskit.

    Returns:
        list: List of task configurations
    """
    return [
        {
            'task_id': 'run',
            'python_callable': run
        },
        {
            'task_id': 'refactor_deals_custom_fields',
            'python_callable': refactor_deals_custom_fields
        },
    ]
