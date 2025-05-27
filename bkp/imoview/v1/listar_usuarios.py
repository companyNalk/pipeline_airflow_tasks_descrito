import requests
import pandas as pd
import unicodedata
import re
import io
import time
from queue import Queue
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage

# Configurações iniciais
API_URL = "https://api.imoview.com.br/Usuario/RetornarTipo3"
CHAVE_API = "hGJta73vJt9IzS3XJzhX83O9PsR2CLE5DMckqymzis8="
SERVICE_ACCOUNT_FILE = r"C:\Users\nicho\Desktop\contas_servico\nalk-app-demo-49a7a06bc8c9.json"
BUCKET_NAME = "imoview-gavea"
FOLDER_NAME = "listagem_usuarios"

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

# Função para obter dados de uma página específica
def fetch_page_data(pagina):
    headers = {
        "accept": "application/json",
        "chave": CHAVE_API
    }
    params = {
        "numeroPagina": pagina,
        "numeroRegistros": 100
    }

    response = requests.get(API_URL, headers=headers, params=params)

    if response.status_code != 200:
        print(f"Erro na requisição da página {pagina}: {response.status_code}")
        return []

    result = response.json()
    lista = result.get("lista", [])

    print(f"Página {pagina} coletada com {len(lista)} registros.")
    return lista

# Função principal com paralelismo controlado
def main():
    start_time = time.time()

    dados = []
    pagina = 1
    continuar = True

    queue = Queue()

    while continuar:
        queue.put(pagina)
        pagina += 1

        if pagina % 10 == 0:
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = [executor.submit(fetch_page_data, queue.get()) for _ in range(queue.qsize())]
                for future in futures:
                    resultado = future.result()
                    if resultado:
                        dados.extend(resultado)
                    if len(resultado) < 100:
                        continuar = False

    df = pd.json_normalize(dados)
    df.columns = [normalize_column_name(col) for col in df.columns]

    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, sep=';', index=False, encoding='utf-8-sig')

    upload_to_gcs(csv_buffer.getvalue(), f"{FOLDER_NAME}/listar_contatos.csv")

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Tempo de execução: {elapsed_time:.2f} segundos")

if __name__ == "__main__":
    main()
