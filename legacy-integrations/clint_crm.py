"""
ClintCRM module for data extraction functions.
This module contains functions specific to the ClintCRM integration.
"""


def run_extract_data(customer):
    from core import gcs
    import requests
    import pandas as pd
    import io
    from google.cloud import storage
    import pathlib
    import os

    SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH
    API_TOKEN = customer['api_token']
    BUCKET_NAME = customer['bucket_name']

    HEADERS = {
        "api-token": f"{API_TOKEN}",
        "accept": "application/json"
    }

    BASE_URL = "https://api.clint.digital/v1"
    ENDPOINTS = {
        "deals": "deals",
        "origins": "origins",
        "lost_status": "lost-status",
        "tags": "tags",
        "contacts": "contacts"
    }

    def upload_to_gcs(bucket_name, destination_blob_name, content):
        """Faz upload de uma string como blob para o Google Cloud Storage (GCS)."""
        storage_client = storage.Client(project=customer['project_id'])
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_string(content)
        print(f"Arquivo {destination_blob_name} enviado com sucesso para o bucket {bucket_name}.")

    def fetch_paginated_data(endpoint):
        all_data = []
        page = 1
        while True:
            url = f"{BASE_URL}/{endpoint}?limit=200&page={page}"
            print(f"Fetching {endpoint} - Page {page}")
            response = requests.get(url, headers=HEADERS)
            if response.status_code != 200:
                raise Exception(
                    f"Erro na requisição {endpoint} página {page}: {response.status_code} - {response.text}")
            json_data = response.json()
            all_data.extend(json_data.get("data", []))
            if not json_data.get("hasNext", False):
                break
            page += 1
        return all_data

    # Baixar dados dos endpoints
    data = {key: fetch_paginated_data(endpoint) for key, endpoint in ENDPOINTS.items()}

    # Criar DataFrames e renomear colunas de ID
    df_deals = pd.json_normalize(data["deals"]).rename(columns={
        "id": "deal_id",
        "contact.id": "contact_id"
    })

    df_origins = pd.json_normalize(data["origins"])[['id', 'name', 'group.id', 'group.name']].rename(columns={
        "id": "origin_id_ref",
        "name": "origin_name",
        'group.id': 'group_id',
        'group.name': 'group_name'
    })
    print("=== Primeiras 5 linhas do df_origins ===")
    print(df_origins.head())

    # 3. Verificar valores únicos na coluna group_name
    print("\n=== Valores únicos em group_name ===")
    print(df_origins['group_name'].unique())

    df_lost_status = pd.json_normalize(data["lost_status"]).rename(columns={
        "id": "lost_status_id_ref",
        "name": "lost_reason"
    })

    df_contacts = pd.json_normalize(data["contacts"]).rename(columns={
        "id": "contact_id"
    })

    # TAGS: explode + pivot
    df_tags_contacts = df_contacts.explode("tags").dropna(subset=["tags"])
    df_tags_contacts["tag_name"] = df_tags_contacts["tags"].apply(lambda x: x["name"])
    df_tags_contacts = df_tags_contacts[["contact_id", "tag_name"]]

    # Agrupa em até 3 tags por contato
    df_tags_contacts["tag_order"] = df_tags_contacts.groupby("contact_id").cumcount() + 1
    df_tags_pivot = df_tags_contacts.pivot(index="contact_id", columns="tag_order", values="tag_name")
    df_tags_pivot.columns = [f"tag_{i}" for i in df_tags_pivot.columns]
    df_tags_pivot = df_tags_pivot.reset_index()

    # MERGES COM OS OUTROS DADOS

    # Deals + Origins
    df = df_deals.merge(df_origins[["origin_id_ref", "origin_name", 'group_name']],
                        left_on="origin_id", right_on="origin_id_ref", how="left")

    # Deals + Lost Status
    df = df.merge(df_lost_status[["lost_status_id_ref", "lost_reason"]],
                  left_on="lost_status_id", right_on="lost_status_id_ref", how="left")

    # Deals + Tags dos contacts
    df = df.merge(df_tags_pivot, on="contact_id", how="left")

    # Gerar CSV em memória
    output = io.StringIO()
    df.to_csv(output, index=False, sep=";")
    csv_content = output.getvalue()

    # Upload para GCS
    destination_blob_name = "clint_crm/clint_crm_deals.csv"
    upload_to_gcs(BUCKET_NAME, destination_blob_name, csv_content)

    print(f"Total de deals processados: {len(df)}")
    print("Dados salvos com sucesso no GCS.")


def get_extraction_tasks():
    return [
        {
            'task_id': 'run_extract_data',
            'python_callable': run_extract_data
        }
    ]
