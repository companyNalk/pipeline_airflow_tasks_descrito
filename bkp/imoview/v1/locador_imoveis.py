import requests
import pandas as pd
import unicodedata
import re
import io
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
from google.cloud import storage

# Configurações iniciais
API_URL_CONTATOS = "https://api.imoview.com.br/Usuario/RetornarTipo3"
API_URL_IMOVEIS_LOCADOR = "https://api.imoview.com.br/Locador/RetornarImoveis"
CHAVE_API = "hGJta73vJt9IzS3XJzhX83O9PsR2CLE5DMckqymzis8="
SERVICE_ACCOUNT_FILE = r"C:\Users\nicho\Desktop\contas_servico\nalk-app-demo-49a7a06bc8c9.json"
BUCKET_NAME = "imoview-gavea"
FOLDER_NAME = "locador_imoveis"

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

# Coleta total de contatos
def get_total_contatos():
    headers = {"accept": "application/json", "chave": CHAVE_API}
    params = {"numeroPagina": 1, "numeroRegistros": 1}
    response = requests.get(API_URL_CONTATOS, headers=headers, params=params)
    if response.status_code == 200:
        return response.json().get("quantidade", 0)
    else:
        print(f"Erro ao obter total de contatos: {response.status_code}")
        return 0

# Coleta dos imóveis por cliente (Locador)
def fetch_imoveis_por_locador(codigo_cliente):
    headers = {"accept": "application/json", "chave": CHAVE_API}
    imoveis = []
    pagina = 1

    while True:
        params = {"numeroPagina": pagina, "numeroRegistros": 20, "codigoCliente": codigo_cliente}
        response = requests.get(API_URL_IMOVEIS_LOCADOR, headers=headers, params=params)

        if response.status_code != 200:
            print(f"Erro na requisição para locador {codigo_cliente}: {response.status_code}")
            break

        result = response.json()
        lista = result.get("lista", [])

        if not lista:
            if pagina == 1:
                print(f"Sem dados do código {codigo_cliente} para coletar.")
            break

        imoveis.extend(lista)
        print(f"Coletando dados do locador {codigo_cliente}, página {pagina}, registros {len(lista)}.")

        if len(lista) < 20:
            break

        pagina += 1

    return imoveis

# Função principal com paralelismo e controle de fila
def main():
    start_time = time.time()
    total_contatos = get_total_contatos()
    print(f"Total de contatos encontrados: {total_contatos}")

    todos_imoveis = []
    codigos_queue = Queue()

    for codigo in range(1, total_contatos + 1):
        codigos_queue.put(codigo)

    def worker():
        while not codigos_queue.empty():
            codigo_cliente = codigos_queue.get()
            imoveis = fetch_imoveis_por_locador(codigo_cliente)
            if imoveis:
                todos_imoveis.extend(imoveis)
            codigos_queue.task_done()

    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(worker) for _ in range(20)]
        for future in as_completed(futures):
            future.result()

    if todos_imoveis:
        df = pd.json_normalize(todos_imoveis, sep='_')
        df.columns = [normalize_column_name(col) for col in df.columns]

        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, sep=';', index=False, encoding='utf-8-sig')

        upload_to_gcs(csv_buffer.getvalue(), f"{FOLDER_NAME}/imoveis_locador_imoview.csv")

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Tempo de execução: {elapsed_time:.2f} segundos")

if __name__ == "__main__":
    main()