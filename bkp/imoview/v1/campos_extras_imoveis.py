import requests
import pandas as pd
import unicodedata
import re
import io
import time
from google.cloud import storage

# Configurações iniciais
API_URL_CAMPOS_EXTRAS = "https://api.imoview.com.br/Imovel/RetornarCamposExtrasDisponiveis"
CHAVE_API = "hGJta73vJt9IzS3XJzhX83O9PsR2CLE5DMckqymzis8="
SERVICE_ACCOUNT_FILE = r"C:\Users\nicho\Desktop\contas_servico\nalk-app-demo-49a7a06bc8c9.json"
BUCKET_NAME = "imoview-gavea"
FOLDER_NAME = "campos_extras_imoveis"

# Função para normalizar colunas
def normalize_column_name(name):
    nfkd = unicodedata.normalize('NFKD', name)
    ascii_name = nfkd.encode('ASCII', 'ignore').decode('ASCII')
    cleaned = re.sub(r"[^\w\s]", "", ascii_name)
    return re.sub(r"\s+", "_", cleaned).lower()

# Função para enviar arquivo ao Google Cloud Storage
def upload_to_gcs(data, destination_blob_name):
    client = storage.Client.from_service_account_json(SERVICE_ACCOUNT_FILE)
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string(data, content_type='text/csv')
    print(f"Arquivo enviado para {destination_blob_name}")

# Coleta dos campos extras disponíveis
def fetch_campos_extras():
    headers = {"accept": "application/json", "chave": CHAVE_API}
    response = requests.get(API_URL_CAMPOS_EXTRAS, headers=headers)

    if response.status_code != 200:
        print(f"Erro na requisição de campos extras: {response.status_code}")
        return []

    result = response.json()
    print(f"Campos extras coletados com sucesso, total: {len(result)} registros.")

    return result

# Função principal
def main():
    start_time = time.time()

    campos_extras = fetch_campos_extras()

    if campos_extras:
        df = pd.json_normalize(campos_extras, sep='_')
        df.columns = [normalize_column_name(col) for col in df.columns]

        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, sep=';', index=False, encoding='utf-8-sig')

        upload_to_gcs(csv_buffer.getvalue(), f"{FOLDER_NAME}/campos_extras_imoview.csv")

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Tempo de execução: {elapsed_time:.2f} segundos")

if __name__ == "__main__":
    main()
