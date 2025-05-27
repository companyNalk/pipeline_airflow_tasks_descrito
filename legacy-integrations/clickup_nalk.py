"""
Clickup NALK module for data extraction functions.
This module contains functions specific to the Clickup NALK integration.
"""

from core import gcs


def run(customer):
    import requests
    import os
    import pathlib
    from google.cloud import storage

    api_bearer_token = customer['api_bearer_token']
    api_payload = customer['api_payload']
    BUCKET_NAME = customer['bucket_name']
    SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()

    # Autenticação no Google Cloud Storage
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH
    credentials = gcs.load_credentials_from_env()
    storage_client = storage.Client(credentials=credentials)
    bucket = storage_client.bucket(BUCKET_NAME)

    url = "https://cu-prod-prod-us-west-2-2-export-service.clickup.com/v1/exportView"
    headers = {
        "Authorization": f"Bearer {api_bearer_token}",
        "Content-Type": "application/json",
        "x-csrf": "1",
        "x-workspace-id": "9011119715"
    }

    params = {
        "date_type": "normal",
        "time_format": "normal",
        "name_only": "False",
        "all_columns": "True",
        "async": "True",
        "time_in_status_total": "False"
    }


    # Função para fazer upload direto para o GCS
    def upload_csv_to_gcs(csv_content, destination_path):
        try:
            blob = bucket.blob(destination_path)
            blob.upload_from_string(csv_content, content_type="text/csv")
            print(f"✅ CSV salvo em gs://{BUCKET_NAME}/{destination_path}")
            return True
        except Exception as e:
            print(f"❌ Falha ao fazer upload para {destination_path}: {str(e)}")
            return False

    print("[INFO] Iniciando requisição para exportar dados do ClickUp...")

    response = requests.post(url, headers=headers, params=params, json=api_payload)
    if response.ok:
        data = response.json()
        csv_url = data.get("url")  # pega o campo 'url'

        if csv_url:
            print("📎 Link do CSV obtido:", csv_url)

            # Passo 3: faz o download do CSV
            print("[INFO] Fazendo download do CSV...")
            csv_response = requests.get(csv_url)

            if csv_response.ok:
                # Salva diretamente no Google Cloud Storage
                destination_path = "clickup/clientes.csv"

                if upload_csv_to_gcs(csv_response.content.decode('utf-8'), destination_path):
                    print(f"✅ Processo finalizado! CSV salvo no GCS em: gs://{BUCKET_NAME}/{destination_path}")
                else:
                    print("❌ Falha ao salvar no GCS")
            else:
                print(f"❌ Falha ao baixar o CSV: {csv_response.status_code}")
                print(csv_response.text)
        else:
            print("❌ Campo 'url' não encontrado na resposta.")
            print("Resposta completa:", data)
    else:
        print(f"❌ Erro ao fazer POST: {response.status_code}")
        print("Resposta:", response.text)


def get_extraction_tasks():
    """
    Get the list of data extraction tasks for ClickUp NALK.

    Returns:
        list: List of task configurations
    """
    return [
        {
            'task_id': 'run',
            'python_callable': run
        }
    ]