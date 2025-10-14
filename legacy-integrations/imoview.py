"""
Imoview module for data extraction functions.
This module contains functions specific to the Imoview integration.
"""

from core import gcs


def run_services(customer):
    import os
    import io
    import pandas as pd
    import re
    import requests
    import time
    import unicodedata
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from google.cloud import storage
    from queue import Queue
    import pathlib

    # Configurações iniciais
    API_URL_ATENDIMENTOS = f"{customer['url_base']}/Atendimento/RetornarAtendimentos"
    CHAVE_API = customer['api_key']
    BUCKET_NAME = customer['bucket_name']

    # Configuração do GCP
    SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH

    # Função para normalizar colunas
    def normalize_column_name(name):
        nfkd = unicodedata.normalize('NFKD', name)
        ascii_name = nfkd.encode('ASCII', 'ignore').decode('ASCII')
        cleaned = re.sub(r"[^\w\s]", "", ascii_name)
        return re.sub(r"\s+", "_", cleaned).lower()

    # Função para enviar arquivo ao Google Cloud Storage
    def upload_to_gcs(data, destination_blob_name):
        storage_client = storage.Client(project=customer['project_id'])
        bucket = storage_client.bucket(BUCKET_NAME)
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
        try:
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

                upload_to_gcs(csv_buffer.getvalue(), "atendimentos/atendimentos_imoview.csv")

            end_time = time.time()
            elapsed_time = end_time - start_time
            print(f"Tempo de execução: {elapsed_time:.2f} segundos")
        except Exception as e:
            print(e)
            raise

    # START
    main()


def run_extra_fields_available(customer):
    import requests
    import pandas as pd
    import unicodedata
    import re
    import io
    import time
    from google.cloud import storage
    import os
    import pathlib

    # Configurações iniciais
    API_URL_CAMPOS_EXTRAS = f"{customer['url_base']}/Imovel/RetornarCamposExtrasDisponiveis"
    CHAVE_API = customer['api_key']
    BUCKET_NAME = customer['bucket_name']

    # Configuração do GCP
    SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH

    # Função para normalizar colunas
    def normalize_column_name(name):
        nfkd = unicodedata.normalize('NFKD', name)
        ascii_name = nfkd.encode('ASCII', 'ignore').decode('ASCII')
        cleaned = re.sub(r"[^\w\s]", "", ascii_name)
        return re.sub(r"\s+", "_", cleaned).lower()

    # Função para enviar arquivo ao Google Cloud Storage
    def upload_to_gcs(data, destination_blob_name):
        storage_client = storage.Client(project=customer['project_id'])
        bucket = storage_client.bucket(BUCKET_NAME)
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
        try:
            start_time = time.time()

            campos_extras = fetch_campos_extras()

            if campos_extras:
                df = pd.json_normalize(campos_extras, sep='_')
                df.columns = [normalize_column_name(col) for col in df.columns]

                csv_buffer = io.StringIO()
                df.to_csv(csv_buffer, sep=';', index=False, encoding='utf-8-sig')

                upload_to_gcs(csv_buffer.getvalue(), f"campos_extras_imoveis/campos_extras_imoview.csv")

            end_time = time.time()
            elapsed_time = end_time - start_time
            print(f"Tempo de execução: {elapsed_time:.2f} segundos")
        except Exception as e:
            print(e)
            raise

    # START
    main()


def run_business_buyer(customer):
    import io
    import pandas as pd
    import re
    import requests
    import time
    import unicodedata
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from google.cloud import storage
    from queue import Queue
    import os
    import pathlib

    # Configurações iniciais
    API_URL_CONTATOS = f"{customer['url_base']}/Usuario/RetornarTipo3"
    API_URL_NEGOCIOS_COMPRADOR = f"{customer['url_base']}/Comprador/RetornarNegocios"
    CHAVE_API = customer['api_key']
    BUCKET_NAME = customer['bucket_name']

    # Configuração do GCP
    SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH

    # Função para normalizar colunas
    def normalize_column_name(name):
        nfkd = unicodedata.normalize('NFKD', name)
        ascii_name = nfkd.encode('ASCII', 'ignore').decode('ASCII')
        cleaned = re.sub(r"[^\w\s]", "", ascii_name)
        return re.sub(r"\s+", "_", cleaned).lower()

    # Função para enviar arquivo ao Google Cloud Storage
    def upload_to_gcs(data, destination_blob_name):
        storage_client = storage.Client(project=customer['project_id'])
        bucket = storage_client.bucket(BUCKET_NAME)
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

    # Coleta dos negócios por cliente (Comprador)
    def fetch_negocios_por_comprador(codigo_cliente):
        headers = {"accept": "application/json", "chave": CHAVE_API}
        negocios = []
        pagina = 1

        while True:
            params = {"numeroPagina": pagina, "numeroRegistros": 100, "codigoCliente": codigo_cliente}
            response = requests.get(API_URL_NEGOCIOS_COMPRADOR, headers=headers, params=params)

            if response.status_code != 200:
                print(f"Erro na requisição para comprador {codigo_cliente}: {response.status_code}")
                break

            result = response.json()
            lista = result.get("lista", [])

            if not lista:
                if pagina == 1:
                    print(f"Sem dados do código {codigo_cliente} para coletar.")
                break

            negocios.extend(lista)
            print(f"Coletando negócios do comprador {codigo_cliente}, página {pagina}, registros {len(lista)}.")

            if len(lista) < 100:
                break

            pagina += 1

        return negocios

    # Função principal com paralelismo e controle de fila
    def main():
        try:
            start_time = time.time()
            total_contatos = get_total_contatos()
            print(f"Total de contatos encontrados: {total_contatos}")

            todos_negocios = []
            codigos_queue = Queue()

            for codigo in range(1, total_contatos + 1):
                codigos_queue.put(codigo)

            def worker():
                while not codigos_queue.empty():
                    codigo_cliente = codigos_queue.get()
                    negocios = fetch_negocios_por_comprador(codigo_cliente)
                    if negocios:
                        todos_negocios.extend(negocios)
                    codigos_queue.task_done()

            with ThreadPoolExecutor(max_workers=20) as executor:
                futures = [executor.submit(worker) for _ in range(20)]
                for future in as_completed(futures):
                    future.result()

            if todos_negocios:
                df = pd.json_normalize(todos_negocios, sep='_')
                df.columns = [normalize_column_name(col) for col in df.columns]

                csv_buffer = io.StringIO()
                df.to_csv(csv_buffer, sep=';', index=False, encoding='utf-8-sig')

                upload_to_gcs(csv_buffer.getvalue(), f"comprador_negocios/negocios_comprador_imoview.csv")

            end_time = time.time()
            elapsed_time = end_time - start_time
            print(f"Tempo de execução: {elapsed_time:.2f} segundos")
        except Exception as e:
            print(e)
            raise

            # START

    main()


def run_tenant_contracts(customer):
    import io
    import pandas as pd
    import re
    import requests
    import time
    import unicodedata
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from google.cloud import storage
    from queue import Queue
    import os
    import pathlib

    # Configurações iniciais
    API_URL_CONTATOS = f"{customer['url_base']}/Usuario/RetornarTipo3"
    API_URL_CONTRATOS = f"{customer['url_base']}/Locatario/RetornarContratos"
    CHAVE_API = customer['api_key']
    BUCKET_NAME = customer['bucket_name']

    # Configuração do GCP
    SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH

    # Função para normalizar colunas
    def normalize_column_name(name):
        nfkd = unicodedata.normalize('NFKD', name)
        ascii_name = nfkd.encode('ASCII', 'ignore').decode('ASCII')
        cleaned = re.sub(r"[^\w\s]", "", ascii_name)
        return re.sub(r"\s+", "_", cleaned).lower()

    # Função para enviar arquivo ao Google Cloud Storage
    def upload_to_gcs(data, destination_blob_name):
        storage_client = storage.Client(project=customer['project_id'])
        bucket = storage_client.bucket(BUCKET_NAME)
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

    # Coleta dos contratos por cliente
    def fetch_contratos_por_cliente(codigo_cliente):
        headers = {"accept": "application/json", "chave": CHAVE_API}
        contratos = []
        pagina = 1

        while True:
            params = {"numeroPagina": pagina, "numeroRegistros": 20, "codigoCliente": codigo_cliente}
            response = requests.get(API_URL_CONTRATOS, headers=headers, params=params)

            if response.status_code != 200:
                print(f"Erro na requisição para cliente {codigo_cliente}: {response.status_code}")
                break

            result = response.json()
            lista = result.get("lista", [])

            if not lista:
                if pagina == 1:
                    print(f"Sem dados do código {codigo_cliente} para coletar.")
                break

            contratos.extend(lista)
            print(f"Coletando dados do código {codigo_cliente}, página {pagina}, registros {len(lista)}.")

            if len(lista) < 20:
                break

            pagina += 1

        return contratos

    # Função principal com paralelismo e controle de fila
    def main():
        try:
            start_time = time.time()
            total_contatos = get_total_contatos()
            print(f"Total de contatos encontrados: {total_contatos}")

            todos_contratos = []
            codigos_queue = Queue()

            for codigo in range(1, total_contatos + 1):
                codigos_queue.put(codigo)

            def worker():
                while not codigos_queue.empty():
                    codigo_cliente = codigos_queue.get()
                    contratos = fetch_contratos_por_cliente(codigo_cliente)
                    if contratos:
                        todos_contratos.extend(contratos)
                    codigos_queue.task_done()

            with ThreadPoolExecutor(max_workers=20) as executor:
                futures = [executor.submit(worker) for _ in range(100)]
                for future in as_completed(futures):
                    future.result()

            if todos_contratos:
                df = pd.json_normalize(todos_contratos, sep='_')
                df.columns = [normalize_column_name(col) for col in df.columns]

                csv_buffer = io.StringIO()
                df.to_csv(csv_buffer, sep=';', index=False, encoding='utf-8-sig')

                upload_to_gcs(csv_buffer.getvalue(), f"locatario_contratos/contratos_imoview.csv")

            end_time = time.time()
            elapsed_time = end_time - start_time
            print(f"Tempo de execução: {elapsed_time:.2f} segundos")
        except Exception as e:
            print(e)
            raise

    # START
    main()


def run_tenant_properties(customer):
    import io
    import pandas as pd
    import re
    import requests
    import time
    import unicodedata
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from google.cloud import storage
    from queue import Queue
    import os
    import pathlib

    # Configurações iniciais
    API_URL_CONTATOS = f"{customer['url_base']}/Usuario/RetornarTipo3"
    API_URL_IMOVEIS = f"{customer['url_base']}/Locatario/RetornarImoveis"
    CHAVE_API = customer['api_key']
    BUCKET_NAME = customer['bucket_name']

    # Configuração do GCP
    SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH

    # Função para normalizar colunas
    def normalize_column_name(name):
        nfkd = unicodedata.normalize('NFKD', name)
        ascii_name = nfkd.encode('ASCII', 'ignore').decode('ASCII')
        cleaned = re.sub(r"[^\w\s]", "", ascii_name)
        return re.sub(r"\s+", "_", cleaned).lower()

    # Função para enviar arquivo ao Google Cloud Storage
    def upload_to_gcs(data, destination_blob_name):
        storage_client = storage.Client(project=customer['project_id'])
        bucket = storage_client.bucket(BUCKET_NAME)
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

    # Coleta dos imóveis por cliente
    def fetch_imoveis_por_cliente(codigo_cliente):
        headers = {"accept": "application/json", "chave": CHAVE_API}
        imoveis = []
        pagina = 1

        while True:
            params = {"numeroPagina": pagina, "numeroRegistros": 20, "codigoCliente": codigo_cliente}
            response = requests.get(API_URL_IMOVEIS, headers=headers, params=params)

            if response.status_code != 200:
                print(f"Erro na requisição para cliente {codigo_cliente}: {response.status_code}")
                break

            result = response.json()
            lista = result.get("lista", [])

            if not lista:
                if pagina == 1:
                    print(f"Sem dados do código {codigo_cliente} para coletar.")
                break

            imoveis.extend(lista)
            print(f"Coletando dados do código {codigo_cliente}, página {pagina}, registros {len(lista)}.")

            if len(lista) < 20:
                break

            pagina += 1

        return imoveis

    # Função principal com paralelismo e controle de fila
    def main():
        try:
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
                    imoveis = fetch_imoveis_por_cliente(codigo_cliente)
                    if imoveis:
                        todos_imoveis.extend(imoveis)
                    codigos_queue.task_done()

            with ThreadPoolExecutor(max_workers=20) as executor:
                futures = [executor.submit(worker) for _ in range(100)]
                for future in as_completed(futures):
                    future.result()

            if todos_imoveis:
                df = pd.json_normalize(todos_imoveis, sep='_')
                df.columns = [normalize_column_name(col) for col in df.columns]

                csv_buffer = io.StringIO()
                df.to_csv(csv_buffer, sep=';', index=False, encoding='utf-8-sig')

                upload_to_gcs(csv_buffer.getvalue(), f"locatario_imoveis/imoveis_imoview.csv")

            end_time = time.time()
            elapsed_time = end_time - start_time
            print(f"Tempo de execução: {elapsed_time:.2f} segundos")
        except Exception as e:
            print(e)
            raise

    # START
    main()


def run_list_users(customer):
    import requests
    import pandas as pd
    import unicodedata
    import re
    import io
    import time
    from queue import Queue
    from concurrent.futures import ThreadPoolExecutor
    from google.cloud import storage
    import os
    import pathlib

    # Configurações iniciais
    API_URL = f"{customer['url_base']}/Usuario/RetornarTipo3"
    CHAVE_API = customer['api_key']
    BUCKET_NAME = customer['bucket_name']

    # Configuração do GCP
    SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH

    # Função para normalizar colunas
    def normalize_column_name(name):
        nfkd = unicodedata.normalize('NFKD', name)
        ascii_name = nfkd.encode('ASCII', 'ignore').decode('ASCII')
        cleaned = re.sub(r"[^\w\s]", "", ascii_name)
        return re.sub(r"\s+", "_", cleaned).lower()

    # Função para enviar arquivo ao Google Cloud Storage
    def upload_to_gcs(data, destination_blob_name):
        storage_client = storage.Client(project=customer['project_id'])
        bucket = storage_client.bucket(BUCKET_NAME)
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
        try:
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

            upload_to_gcs(csv_buffer.getvalue(), f"listagem_usuarios/listar_contatos.csv")

            end_time = time.time()
            elapsed_time = end_time - start_time
            print(f"Tempo de execução: {elapsed_time:.2f} segundos")
        except Exception as e:
            print(e)
            raise

            # START

    main()


def run_contract_finder(customer):
    import requests
    import pandas as pd
    import unicodedata
    import re
    import io
    import time
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from queue import Queue
    from google.cloud import storage
    import os
    import pathlib

    # Configurações iniciais
    API_URL_CONTATOS = f"{customer['url_base']}/Usuario/RetornarTipo3"
    API_URL_CONTRATOS_LOCADOR = f"{customer['url_base']}/Locador/RetornarContratos"
    CHAVE_API = customer['api_key']
    BUCKET_NAME = customer['bucket_name']

    # Configuração do GCP
    SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH

    # Função para normalizar colunas
    def normalize_column_name(name):
        nfkd = unicodedata.normalize('NFKD', name)
        ascii_name = nfkd.encode('ASCII', 'ignore').decode('ASCII')
        cleaned = re.sub(r"[^\w\s]", "", ascii_name)
        return re.sub(r"\s+", "_", cleaned).lower()

    # Função para enviar arquivo ao Google Cloud Storage
    def upload_to_gcs(data, destination_blob_name):
        storage_client = storage.Client(project=customer['project_id'])
        bucket = storage_client.bucket(BUCKET_NAME)
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

    # Coleta dos contratos por cliente (Locador)
    def fetch_contratos_por_locador(codigo_cliente):
        headers = {"accept": "application/json", "chave": CHAVE_API}
        contratos = []
        pagina = 1

        while True:
            params = {"numeroPagina": pagina, "numeroRegistros": 20, "codigoCliente": codigo_cliente}
            response = requests.get(API_URL_CONTRATOS_LOCADOR, headers=headers, params=params)

            if response.status_code != 200:
                print(f"Erro na requisição para locador {codigo_cliente}: {response.status_code}")
                break

            result = response.json()
            lista = result.get("lista", [])

            if not lista:
                if pagina == 1:
                    print(f"Sem dados do código {codigo_cliente} para coletar.")
                break

            contratos.extend(lista)
            print(f"Coletando contratos do locador {codigo_cliente}, página {pagina}, registros {len(lista)}.")

            if len(lista) < 20:
                break

            pagina += 1

        return contratos

    # Função principal com paralelismo e controle de fila
    def main():
        try:
            start_time = time.time()
            total_contatos = get_total_contatos()
            print(f"Total de contatos encontrados: {total_contatos}")

            todos_contratos = []
            codigos_queue = Queue()

            for codigo in range(1, total_contatos + 1):
                codigos_queue.put(codigo)

            def worker():
                while not codigos_queue.empty():
                    codigo_cliente = codigos_queue.get()
                    contratos = fetch_contratos_por_locador(codigo_cliente)
                    if contratos:
                        todos_contratos.extend(contratos)
                    codigos_queue.task_done()

            with ThreadPoolExecutor(max_workers=20) as executor:
                futures = [executor.submit(worker) for _ in range(20)]
                for future in as_completed(futures):
                    future.result()

            if todos_contratos:
                df = pd.json_normalize(todos_contratos, sep='_')
                df.columns = [normalize_column_name(col) for col in df.columns]

                csv_buffer = io.StringIO()
                df.to_csv(csv_buffer, sep=';', index=False, encoding='utf-8-sig')

                upload_to_gcs(csv_buffer.getvalue(), f"locador_contratos/contratos_locador_imoview.csv")

            end_time = time.time()
            elapsed_time = end_time - start_time
            print(f"Tempo de execução: {elapsed_time:.2f} segundos")
        except Exception as e:
            print(e)
            raise

    # START
    main()


def run_rental_property(customer):
    import requests
    import pandas as pd
    import unicodedata
    import re
    import io
    import time
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from queue import Queue
    from google.cloud import storage
    import os
    import pathlib

    # Configurações iniciais
    API_URL_CONTATOS = f"{customer['url_base']}/Usuario/RetornarTipo3"
    API_URL_IMOVEIS_LOCADOR = f"{customer['url_base']}/Locador/RetornarImoveis"
    CHAVE_API = customer['api_key']
    BUCKET_NAME = customer['bucket_name']

    SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH

    # Função para normalizar colunas
    def normalize_column_name(name):
        nfkd = unicodedata.normalize('NFKD', name)
        ascii_name = nfkd.encode('ASCII', 'ignore').decode('ASCII')
        cleaned = re.sub(r"[^\w\s]", "", ascii_name)
        return re.sub(r"\s+", "_", cleaned).lower()

    # Função para enviar arquivo ao Google Cloud Storage
    def upload_to_gcs(data, destination_blob_name):
        storage_client = storage.Client(project=customer['project_id'])
        bucket = storage_client.bucket(BUCKET_NAME)
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
        try:
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

                upload_to_gcs(csv_buffer.getvalue(), f"locador_imoveis/imoveis_locador_imoview.csv")

            end_time = time.time()
            elapsed_time = end_time - start_time
            print(f"Tempo de execução: {elapsed_time:.2f} segundos")
        except Exception as e:
            print(e)
            raise

    # START
    main()


def run_real_state_agent(customer):
    import io
    import pandas as pd
    import re
    import requests
    import time
    import unicodedata
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from google.cloud import storage
    from queue import Queue
    import os
    import pathlib

    # Configurações iniciais
    API_URL_CONTATOS = f"{customer['url_base']}/Usuario/RetornarTipo3"
    API_URL_IMOVEIS_VENDEDOR = f"{customer['url_base']}/Vendedor/RetornarImoveis"
    CHAVE_API = customer['api_key']
    BUCKET_NAME = customer['bucket_name']

    SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH

    # Função para normalizar colunas
    def normalize_column_name(name):
        nfkd = unicodedata.normalize('NFKD', name)
        ascii_name = nfkd.encode('ASCII', 'ignore').decode('ASCII')
        cleaned = re.sub(r"[^\w\s]", "", ascii_name)
        return re.sub(r"\s+", "_", cleaned).lower()

    # Função para enviar arquivo ao Google Cloud Storage
    def upload_to_gcs(data, destination_blob_name):
        storage_client = storage.Client(project=customer['project_id'])
        bucket = storage_client.bucket(BUCKET_NAME)
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

    # Coleta dos imóveis por cliente (Vendedor)
    def fetch_imoveis_por_vendedor(codigo_cliente):
        headers = {"accept": "application/json", "chave": CHAVE_API}
        imoveis = []
        pagina = 1

        while True:
            params = {"numeroPagina": pagina, "numeroRegistros": 20, "codigoCliente": codigo_cliente}
            response = requests.get(API_URL_IMOVEIS_VENDEDOR, headers=headers, params=params)

            if response.status_code != 200:
                print(f"Erro na requisição para vendedor {codigo_cliente}: {response.status_code}")
                break

            result = response.json()
            lista = result.get("lista", [])

            if not lista:
                if pagina == 1:
                    print(f"Sem dados do código {codigo_cliente} para coletar.")
                break

            imoveis.extend(lista)
            print(f"Coletando imóveis do vendedor {codigo_cliente}, página {pagina}, registros {len(lista)}.")

            if len(lista) < 20:
                break

            pagina += 1

        return imoveis

    # Função principal com paralelismo e controle de fila
    def main():
        try:
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
                    imoveis = fetch_imoveis_por_vendedor(codigo_cliente)
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

                upload_to_gcs(csv_buffer.getvalue(), f"vendedor_imoveis/imoveis_vendedor_imoview.csv")

            end_time = time.time()
            elapsed_time = end_time - start_time
            print(f"Tempo de execução: {elapsed_time:.2f} segundos")
        except Exception as e:
            print(e)
            raise

    # START
    main()

# ------------------------ INICIO - Adição do endpoint de atividades ------------------------- #

def run_get_activities(customer):
    import io
    import pandas as pd
    import re
    import requests
    import time
    import unicodedata
    from concurrent.futures import ThreadPoolExecutor, as_completed
    from google.cloud import storage
    from queue import Queue
    import os
    import pathlib

    # Configurações iniciais
    API_URL_ACTIVITIES = f"{customer['url_base']}/Agenda/RetornarAtividades"
    CHAVE_API = customer['api_key']
    BUCKET_NAME = customer['bucket_name']

    SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH

    # Função para normalizar colunas
    def normalize_column_name(name):
        nfkd = unicodedata.normalize('NFKD', name)
        ascii_name = nfkd.encode('ASCII', 'ignore').decode('ASCII')
        cleaned = re.sub(r"[^\w\s]", "", ascii_name)
        return re.sub(r"\s+", "_", cleaned).lower()

    # Função para enviar arquivo ao Google Cloud Storage
    def upload_to_gcs(data, destination_blob_name):
        storage_client = storage.Client(project=customer['project_id'])
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_string(data, content_type='text/csv')
        print(f"Arquivo enviado para {destination_blob_name}")

    # Coleta total de atividades (ROBUSTO CONTRA RETORNO DE LISTA)
    def get_total_activities():
        headers = {"accept": "application/json", "chave": CHAVE_API}
        params = {"numeroPagina": 1, "numeroRegistros": 1}
        try:
            response = requests.get(API_URL_ACTIVITIES, headers=headers, params=params, timeout=10)
            response.raise_for_status()
            result = response.json()
            
            # Tenta obter "quantidade" se for um dicionário (Formato Paginado Esperado)
            if isinstance(result, dict) and "quantidade" in result:
                return result.get("quantidade", 0)
            
            # Se a API retornar uma lista diretamente, não podemos confiar na contagem
            print("A API de atividades não retornou 'quantidade'. A paginação será feita de forma iterativa.")
            return 0 
        except Exception as e:
            print(f"Erro ao obter total de atividades: {e}")
            return 0

    # Coleta uma página específica de atividades
    def fetch_activities_page(page_number):
        headers = {"accept": "application/json", "chave": CHAVE_API}
        params = {"numeroPagina": page_number, "numeroRegistros": 20}
        
        try:
            response = requests.get(API_URL_ACTIVITIES, headers=headers, params=params, timeout=30)
            response.raise_for_status()  # Lança exceção para códigos de erro HTTP
            
            result = response.json()
            
            # Adaptação: Se for Dicionário, os dados estão em 'lista'; caso contrário, é o próprio resultado (lista).
            activities = result.get("lista", []) if isinstance(result, dict) else result
            
            # Remove campos aninhados antes de estender a lista principal para simplificar o json_normalize
            if isinstance(activities, list):
                # Usamos List Comprehension para limpar e garantir o formato de dicionário
                clean_activities = []
                for activity in activities:
                    if isinstance(activity, dict):
                        # Remove campos que são listas aninhadas, garantindo que o json_normalize funcione
                        activity.pop('notas', None)
                        activity.pop('convidados', None)
                        clean_activities.append(activity)
                
                print(f"Coletado com sucesso: página {page_number}, registros {len(clean_activities)}.")
                return clean_activities
            
            # Se o resultado for um dicionário que não seja a lista ou um resultado inesperado, retorne None
            return None
            
        except requests.exceptions.RequestException as e:
            print(f"Erro na requisição da página {page_number}: {e}")
            return None

    # Função principal com paralelismo e controle de fila
    def main():
        try:
            start_time = time.time()
            total_atividades = get_total_activities()
            page_size = 20
            
            if total_atividades == 0:
                print("Contagem total falhou ou é zero. Assumindo 1000 páginas para coleta iterativa.")
                # Assumimos um limite alto de páginas se a contagem falhar
                total_pages = 1000 
            else:
                total_pages = (total_atividades + page_size - 1) // page_size
                print(f"Total de atividades encontradas: {total_atividades}. Coletando dados de {total_pages} páginas...")
            
            # all_activities é a lista mestre que deve ser populada por threads
            all_activities = [] 
            page_queue = Queue()
            
            for page_num in range(1, total_pages + 1):
                page_queue.put(page_num)

            def worker():
                while not page_queue.empty():
                    page_number = page_queue.get()
                    time.sleep(0.01) # Pequeno delay
                    activities = fetch_activities_page(page_number)
                    
                    if activities is not None and activities: # Garante que não é None e tem dados
                        # CORREÇÃO DEFINITIVA: Estender a lista mestre diretamente com a lista de dicionários.
                        all_activities.extend(activities) 
                        
                    page_queue.task_done()

            # Usar um número menor de workers para evitar sobrecarregar a API
            num_workers = min(10, page_queue.qsize())
            with ThreadPoolExecutor(max_workers=num_workers) as executor:
                futures = [executor.submit(worker) for _ in range(num_workers)]
                
                for future in as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        print(f"Uma thread gerou uma exceção: {e}")
            
            # Removido o passo de "achatamento" complexo que pode ter sido a causa do erro original.

            if all_activities:
                print(f"Processamento finalizado. Total de {len(all_activities)} registros coletados.")
                
                # 1. Converte a lista de dicionários para DataFrame
                df = pd.json_normalize(all_activities, sep='_') 
                
                # 2. Renomeia as colunas
                df.columns = [normalize_column_name(col) for col in df.columns]
                
                # 3. Exibe o cabeçalho da tabela (o que você solicitou)
                print("\n--- Amostra da Tabela (DataFrame) de Atividades ---")
                if hasattr(df.head(), 'to_markdown'):
                    print(df.head().to_markdown(index=False)) 
                else:
                    # Fallback para ambientes sem to_markdown (imprime strings)
                    print(df.head().to_string(index=False)) 
                print("--------------------------------------------------\n")
                
                # 4. Salva a tabela final como CSV
                csv_buffer = io.StringIO()
                df.to_csv(csv_buffer, sep=';', index=False, encoding='utf-8-sig')

                upload_to_gcs(csv_buffer.getvalue(), f"atividades/atividades.csv")
            else:
                print("Nenhuma atividade foi coletada após o processamento.")

            end_time = time.time()
            elapsed_time = end_time - start_time
            print(f"Tempo de execução: {elapsed_time:.2f} segundos")
        except Exception as e:
            print(f"Ocorreu um erro inesperado: {e}")
            raise

    # START
    main()  

# ------------------------- FINAL - Adição do endpoint de atividades ------------------------- #

def get_extraction_tasks():
    """
    Get the list of data extraction tasks for Imoview.

    Returns:
        list: List of task configurations
    """
    return [
        {
            'task_id': 'run_services',
            'python_callable': run_services
        },
        {
            'task_id': 'run_extra_immovable_fields',
            'python_callable': run_extra_fields_available
        },
        {
            'task_id': 'run_business_buyer',
            'python_callable': run_business_buyer
        },
        {
            'task_id': 'run_tenant_contracts',
            'python_callable': run_tenant_contracts
        },
        {
            'task_id': 'run_tenant_properties',
            'python_callable': run_tenant_properties
        },
        {
            'task_id': 'run_list_users',
            'python_callable': run_list_users
        },
        {
            'task_id': 'run_contract_finder',
            'python_callable': run_contract_finder
        },
        {
            'task_id': 'run_rental_property',
            'python_callable': run_rental_property
        },
        {
            'task_id': 'run_real_state_agent',
            'python_callable': run_real_state_agent
        },
        {
            'task_id': 'get_activities',
            'python_callable': run_get_activities
        }
    ]
