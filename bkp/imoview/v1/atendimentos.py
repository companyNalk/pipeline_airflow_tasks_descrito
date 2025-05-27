import io
import pandas as pd
import re
import requests
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.cloud import storage
from queue import Queue

# Configurações iniciais
API_URL_ATENDIMENTOS = "https://api.imoview.com.br/Atendimento/RetornarAtendimentos"
CHAVE_API = "hGJta73vJt9IzS3XJzhX83O9PsR2CLE5DMckqymzis8="
SERVICE_ACCOUNT_FILE = r"C:\Users\nicho\Desktop\contas_servico\nalk-app-demo-49a7a06bc8c9.json"
BUCKET_NAME = "imoview-gavea"
FOLDER_NAME = "atendimentos"


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


# Coleta dos atendimentos por combinação específica
def fetch_atendimentos(finalidade, situacao, fase):
    headers = {"accept": "application/json", "chave": CHAVE_API}
    atendimentos = []
    pagina = 1

    while True:
        params = {
            "numeroPagina": pagina,
            "numeroRegistros": 20,
            "finalidade": finalidade,
            "situacao": situacao,
            "fase": fase
        }
        response = requests.get(API_URL_ATENDIMENTOS, headers=headers, params=params)

        if response.status_code != 200:
            print(
                f"Erro na requisição (finalidade: {finalidade}, situação: {situacao}, fase: {fase}): {response.status_code}")
            break

        result = response.json()
        lista = result.get("lista", [])

        if not lista:
            if pagina == 1:
                print(f"Sem dados para finalidade {finalidade}, situação {situacao}, fase {fase}.")
            break

        atendimentos.extend(lista)
        print(
            f"Coletando dados (finalidade: {finalidade}, situação: {situacao}, fase: {fase}), página {pagina}, registros {len(lista)}.")

        if len(lista) < 20:
            break

        pagina += 1

    return atendimentos


# Função principal com paralelismo e controle eficiente das combinações
def main():
    start_time = time.time()

    combinacoes_queue = Queue()

    finalidades = [1, 2]
    situacoes = [0, 1, 2, 3]
    fases = [1, 2, 3, 4, 5, 6]

    for finalidade in finalidades:
        for situacao in situacoes:
            for fase in fases:
                combinacoes_queue.put((finalidade, situacao, fase))

    todos_atendimentos = []

    def worker():
        while not combinacoes_queue.empty():
            finalidade, situacao, fase = combinacoes_queue.get()
            dados = fetch_atendimentos(finalidade, situacao, fase)
            if dados:
                todos_atendimentos.extend(dados)
            combinacoes_queue.task_done()

    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = [executor.submit(worker) for _ in range(20)]
        for future in as_completed(futures):
            future.result()

    if todos_atendimentos:
        df = pd.json_normalize(todos_atendimentos, sep='_')
        df.columns = [normalize_column_name(col) for col in df.columns]

        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, sep=';', index=False, encoding='utf-8-sig')

        upload_to_gcs(csv_buffer.getvalue(), f"{FOLDER_NAME}/atendimentos_imoview.csv")

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Tempo de execução: {elapsed_time:.2f} segundos")


if __name__ == "__main__":
    main()
