"""
CVCRM module for data extraction functions.
This module contains functions specific to the CVCRM integration.
"""
from core import gcs


def run_leads(customer):
    # if not customer['cvdw_active']:
    #     print(f"CVDW não está ativo para este cliente. Pulando execução da função run_leads.")
    #     return
    print(f'VALUE: {customer["cvdw_active"]}')

    import requests
    import pandas as pd
    import os
    import time
    import pathlib
    import random
    from google.cloud import bigquery
    from requests.exceptions import HTTPError, RequestException

    # Configuration
    DOMINIO = customer['api_dominio']
    EMAIL = customer['api_email']
    ACCESS_TOKEN = customer['api_access_token']
    PROJECT_ID = customer['project_id']
    SERVICE_ACCOUNT_FILE = pathlib.Path('config', 'setup_automatico.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_FILE

    URL_BASE = f"https://{DOMINIO}.cvcrm.com.br/api/v1/cvdw/leads"
    dataset_id = 'cvcrm'
    table_id = 'cvcrm_dw_leads'

    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "email": EMAIL,
        "token": ACCESS_TOKEN
    }
    data = {
        "pagina": 1,
        "registros_por_pagina": 500
    }

    # Initialize variables
    todas_dados = []
    pagina_atual = 1
    max_retries = 5
    continuar_paginacao = True

    print(f"Iniciando coleta de dados de leads...")

    while continuar_paginacao:
        data["pagina"] = pagina_atual
        retry_count = 0
        success = False

        # Retry loop with exponential backoff
        while not success and retry_count < max_retries:
            try:
                response = requests.get(URL_BASE, headers=headers, json=data, timeout=30)

                # If we get a 204 No Content, it means we've reached the end of the data
                if response.status_code == 204:
                    print(f"Recebido código 204 (No Content) na página {pagina_atual}. Fim da paginação.")
                    continuar_paginacao = False
                    success = True
                    break

                # Raise exception for other error status codes
                response.raise_for_status()

                # Parse JSON response
                try:
                    response_data = response.json()
                except ValueError:
                    # If response is not JSON but status is 200, we might have empty response
                    if response.status_code == 200 and not response.text.strip():
                        print(f"Resposta vazia na página {pagina_atual}. Fim da paginação.")
                        continuar_paginacao = False
                        success = True
                        break
                    else:
                        raise ValueError(f"Resposta não é um JSON válido na página {pagina_atual}")

                # Process page data
                page_data = response_data.get("dados", [])

                # If no data is returned but response is successful, we've reached the end
                if not page_data:
                    print(f"Página {pagina_atual} retornou array de dados vazio. Fim da paginação.")
                    continuar_paginacao = False
                    success = True
                    break

                # Process valid data
                todas_dados.extend(page_data)
                print(f"Página {pagina_atual} processada com sucesso. Registros: {len(page_data)}")
                success = True

            except HTTPError as e:
                if e.response.status_code == 429:
                    retry_count += 1
                    wait_time = (2 ** retry_count) + random.uniform(0, 1)
                    print(
                        f"Erro 429 (Too Many Requests). Tentativa {retry_count}/{max_retries}. Aguardando {wait_time:.2f} segundos...")
                    time.sleep(wait_time)
                else:
                    print(f"Erro HTTP na página {pagina_atual}: {e}")
                    raise
            except RequestException as e:
                retry_count += 1
                wait_time = (2 ** retry_count) + random.uniform(0, 1)
                print(
                    f"Erro de rede na página {pagina_atual}. Tentativa {retry_count}/{max_retries}. Aguardando {wait_time:.2f} segundos...")
                time.sleep(wait_time)
            except ValueError as e:
                print(f"Erro de validação na página {pagina_atual}: {e}")
                raise
            except Exception as e:
                print(f"Erro inesperado na página {pagina_atual}: {e}")
                raise

        # Handle retry failure
        if not success:
            print(
                f"Falha ao processar a página {pagina_atual} após {max_retries} tentativas. Continuando com os dados coletados.")
            break

        # Increment page if we need to continue
        if continuar_paginacao:
            pagina_atual += 1
            # Adaptive pause to respect API rate limits
            # time.sleep(5 + random.uniform(0, 2))
            time.sleep(5)

    # Check if any data was collected
    if not todas_dados:
        print("Nenhum dado foi coletado. Encerrando sem atualizar o BigQuery.")
        return

    # Convert to DataFrame
    df_leads = pd.DataFrame(todas_dados)
    total_registros = len(df_leads)
    print(f"Total de registros coletados: {total_registros}")

    # Load to BigQuery
    try:
        client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_FILE, project=PROJECT_ID)
        table_ref = client.dataset(dataset_id).table(table_id)
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True
        )
        load_job = client.load_table_from_dataframe(df_leads, table_ref, job_config=job_config)
        load_job.result()
        print(f"Dados carregados com sucesso para {dataset_id}.{table_id}. Total: {total_registros} registros.")
    except Exception as e:
        print(f"Erro ao carregar dados no BigQuery: {e}")
        raise


def run_vendas(customer):
    if not customer['cvdw_active']:
        print(f"CVDW não está ativo para este cliente. Pulando execução da função run_leads.")
        return

    import requests
    import pandas as pd
    import os
    import time
    import pathlib
    import random
    from google.cloud import bigquery
    from requests.exceptions import HTTPError

    DOMINIO = customer['api_dominio']
    EMAIL = customer['api_email']
    ACCESS_TOKEN = customer['api_access_token']
    PROJECT_ID = customer['project_id']
    SERVICE_ACCOUNT_FILE = pathlib.Path('config', 'setup_automatico.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_FILE

    URL_BASE = f"https://{DOMINIO}.cvcrm.com.br/api/v1/cvdw/vendas"

    dataset_id = 'cvcrm'
    table_id = 'cvcrm_vendas'

    url_base = URL_BASE
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "email": EMAIL,
        "token": ACCESS_TOKEN
    }
    data = {
        "pagina": 1,
        "registros_por_pagina": 500
    }

    # Inicialização de variáveis
    todas_dados = []  # Para armazenar os dados coletados
    pagina_atual = 1
    max_retries = 5  # Número máximo de tentativas
    ultima_pagina = None

    while True:
        # Atualizar o número da página na requisição
        data["pagina"] = pagina_atual

        retry_count = 0
        success = False

        # Loop de retry com backoff exponencial
        while not success and retry_count < max_retries:
            try:
                # Fazer a requisição
                response = requests.get(url_base, headers=headers, json=data)
                response.raise_for_status()  # Levanta exceção para status codes de erro
                response_data = response.json()
                success = True  # Se chegar aqui, a requisição foi bem-sucedida

                # Captura o total de páginas
                if ultima_pagina is None:
                    ultima_pagina = response_data["total_de_paginas"]
                    print(f"Total de páginas a processar: {ultima_pagina}")

                # Adicionar os dados da página atual na lista total
                if "dados" in response_data and response_data["dados"]:
                    todas_dados.extend(response_data["dados"])
                    registros_na_pagina = len(response_data["dados"])
                    print(
                        f"Página {pagina_atual}/{ultima_pagina} processada com sucesso. Registros: {registros_na_pagina}")
                else:
                    print(f"Página {pagina_atual}/{ultima_pagina} não contém dados.")

            except HTTPError as e:
                if e.response.status_code == 429:
                    retry_count += 1
                    # Backoff exponencial com jitter (variação aleatória)
                    wait_time = (2 ** retry_count) + random.uniform(0, 1)
                    print(
                        f"Erro 429 (Too Many Requests). Tentativa {retry_count}/{max_retries}. Aguardando {wait_time:.2f} segundos...")
                    time.sleep(wait_time)
                else:
                    # Para outros erros HTTP, registre e levante a exceção
                    print(f"Erro HTTP: {e}")
                    raise
            except Exception as e:
                # Para outros tipos de exceções
                print(f"Erro inesperado: {e}")
                raise

        # Se saiu do loop de retry sem sucesso, interrompe o processamento
        if not success:
            print(
                f"Falha ao processar a página {pagina_atual} após {max_retries} tentativas. Continuando com os dados já coletados.")

        # Avançar para a próxima página
        pagina_atual += 1

        # Verificar se todas as páginas foram processadas
        if pagina_atual > ultima_pagina and ultima_pagina is not None:
            print(f"Todas as {ultima_pagina} páginas foram processadas.")
            break

        # Pausa entre requisições para evitar alcançar limites de taxa
        time.sleep(5)

    # Verificar se foram coletados dados
    if not todas_dados:
        print("Nenhum dado foi coletado. Encerrando sem atualizar o BigQuery.")
        return

    # Converter os dados coletados para um DataFrame
    df_vendas = pd.DataFrame(todas_dados)
    total_registros = len(df_vendas)
    print(f"Total de registros coletados: {total_registros}")

    # Verificar se coletamos o número esperado de registros
    if ultima_pagina is not None:
        registros_esperados = 500 * (ultima_pagina - 1) + registros_na_pagina  # Estimativa
        if total_registros < registros_esperados:
            print(
                f"AVISO: Número de registros coletados ({total_registros}) é menor que o esperado ({registros_esperados}).")

    # Carregar para o BigQuery
    client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_FILE, project=PROJECT_ID)
    table_ref = client.dataset(dataset_id).table(table_id)
    # Define the table schema
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.autodetect = True
    # Load the DataFrame to BigQuery
    load_job = client.load_table_from_dataframe(df_vendas, table_ref, job_config=job_config)
    load_job.result()  # Wait for the job to complete

    print(f"Dados carregados com sucesso para {dataset_id}.{table_id}. Total: {total_registros} registros.")


def run_reservas(customer):
    if not customer['cvdw_active']:
        print(f"CVDW não está ativo para este cliente. Pulando execução da função run_leads.")
        return

    import requests
    import pandas as pd
    import os
    import time
    import pathlib
    import random
    from google.cloud import bigquery
    from requests.exceptions import HTTPError

    DOMINIO = customer['api_dominio']
    EMAIL = customer['api_email']
    ACCESS_TOKEN = customer['api_access_token']
    PROJECT_ID = customer['project_id']
    SERVICE_ACCOUNT_FILE = pathlib.Path('config', 'setup_automatico.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_FILE

    URL_BASE = f"https://{DOMINIO}.cvcrm.com.br/api/v1/cvdw/reservas"

    dataset_id = 'cvcrm'
    table_id = 'cvcrm_reservas'

    url_base = URL_BASE
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "email": EMAIL,
        "token": ACCESS_TOKEN
    }
    data = {
        "pagina": 1,
        "registros_por_pagina": 500
    }

    # Inicialização de variáveis
    todas_dados = []  # Para armazenar os dados coletados
    pagina_atual = 1
    max_retries = 5  # Número máximo de tentativas
    ultima_pagina = None
    registros_na_pagina = 0

    while True:
        # Atualizar o número da página na requisição
        data["pagina"] = pagina_atual

        retry_count = 0
        success = False

        # Loop de retry com backoff exponencial
        while not success and retry_count < max_retries:
            try:
                # Fazer a requisição
                response = requests.get(url_base, headers=headers, json=data)
                response.raise_for_status()  # Levanta exceção para status codes de erro
                response_data = response.json()
                success = True  # Se chegar aqui, a requisição foi bem-sucedida

                # Captura o total de páginas
                if ultima_pagina is None:
                    ultima_pagina = response_data["total_de_paginas"]
                    print(f"Total de páginas a processar: {ultima_pagina}")

                # Adicionar os dados da página atual na lista total
                if "dados" in response_data and response_data["dados"]:
                    todas_dados.extend(response_data["dados"])
                    registros_na_pagina = len(response_data["dados"])
                    print(
                        f"Página {pagina_atual}/{ultima_pagina} processada com sucesso. Registros: {registros_na_pagina}")
                else:
                    print(f"Página {pagina_atual}/{ultima_pagina} não contém dados.")

            except HTTPError as e:
                if e.response.status_code == 429:
                    retry_count += 1
                    # Backoff exponencial com jitter (variação aleatória)
                    wait_time = (2 ** retry_count) + random.uniform(0, 1)
                    print(
                        f"Erro 429 (Too Many Requests). Tentativa {retry_count}/{max_retries}. Aguardando {wait_time:.2f} segundos...")
                    time.sleep(wait_time)
                else:
                    # Para outros erros HTTP, registre e levante a exceção
                    print(f"Erro HTTP: {e}")
                    raise
            except Exception as e:
                # Para outros tipos de exceções
                print(f"Erro inesperado: {e}")
                raise

        # Se saiu do loop de retry sem sucesso, interrompe o processamento
        if not success:
            print(
                f"Falha ao processar a página {pagina_atual} após {max_retries} tentativas. Continuando com os dados já coletados.")

        # Avançar para a próxima página
        pagina_atual += 1

        # Verificar se todas as páginas foram processadas
        if pagina_atual > ultima_pagina and ultima_pagina is not None:
            print(f"Todas as {ultima_pagina} páginas foram processadas.")
            break

        # Pausa entre requisições para evitar alcançar limites de taxa
        time.sleep(5)

    # Verificar se foram coletados dados
    if not todas_dados:
        print("Nenhum dado foi coletado. Encerrando sem atualizar o BigQuery.")
        return

    # Converter os dados coletados para um DataFrame
    df_reservas = pd.DataFrame(todas_dados)
    total_registros = len(df_reservas)
    print(f"Total de registros coletados: {total_registros}")

    # Verificar se coletamos o número esperado de registros
    if ultima_pagina is not None:
        registros_esperados = 500 * (ultima_pagina - 1) + registros_na_pagina  # Estimativa
        if total_registros < registros_esperados:
            print(
                f"AVISO: Número de registros coletados ({total_registros}) é menor que o esperado ({registros_esperados}).")

    # Carregar para o BigQuery
    client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_FILE, project=PROJECT_ID)
    table_ref = client.dataset(dataset_id).table(table_id)
    # Define the table schema
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.autodetect = True
    # Load the DataFrame to BigQuery
    load_job = client.load_table_from_dataframe(df_reservas, table_ref, job_config=job_config)
    load_job.result()  # Wait for the job to complete

    print(f"Dados carregados com sucesso para {dataset_id}.{table_id}. Total: {total_registros} registros.")


def run_precadastros(customer):
    if not customer['cvdw_active']:
        print(f"CVDW não está ativo para este cliente. Pulando execução da função run_leads.")
        return

    import requests
    import pandas as pd
    import os
    import time
    import pathlib
    import random
    from google.cloud import bigquery
    from requests.exceptions import HTTPError

    DOMINIO = customer['api_dominio']
    EMAIL = customer['api_email']
    ACCESS_TOKEN = customer['api_access_token']
    PROJECT_ID = customer['project_id']
    SERVICE_ACCOUNT_FILE = pathlib.Path('config', 'setup_automatico.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_FILE

    URL_BASE = f"https://{DOMINIO}.cvcrm.com.br/api/v1/cvdw/precadastros"

    dataset_id = 'cvcrm'
    table_id = 'cvcrm_precadastros'

    url_base = URL_BASE
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "email": EMAIL,
        "token": ACCESS_TOKEN
    }
    data = {
        "pagina": 1,
        "registros_por_pagina": 500
    }

    # Inicialização de variáveis
    todas_dados = []  # Para armazenar os dados coletados
    pagina_atual = 1
    max_retries = 5  # Número máximo de tentativas
    ultima_pagina = None
    registros_na_pagina = 0

    while True:
        # Atualizar o número da página na requisição
        data["pagina"] = pagina_atual

        retry_count = 0
        success = False

        # Loop de retry com backoff exponencial
        while not success and retry_count < max_retries:
            try:
                # Fazer a requisição
                response = requests.get(url_base, headers=headers, json=data)
                response.raise_for_status()  # Levanta exceção para status codes de erro
                response_data = response.json()
                success = True  # Se chegar aqui, a requisição foi bem-sucedida

                # Captura o total de páginas
                if ultima_pagina is None:
                    ultima_pagina = response_data["total_de_paginas"]
                    print(f"Total de páginas a processar: {ultima_pagina}")

                # Adicionar os dados da página atual na lista total
                if "dados" in response_data and response_data["dados"]:
                    todas_dados.extend(response_data["dados"])
                    registros_na_pagina = len(response_data["dados"])
                    print(
                        f"Página {pagina_atual}/{ultima_pagina} processada com sucesso. Registros: {registros_na_pagina}")
                else:
                    print(f"Página {pagina_atual}/{ultima_pagina} não contém dados.")

            except HTTPError as e:
                if e.response.status_code == 429:
                    retry_count += 1
                    # Backoff exponencial com jitter (variação aleatória)
                    wait_time = (2 ** retry_count) + random.uniform(0, 1)
                    print(
                        f"Erro 429 (Too Many Requests). Tentativa {retry_count}/{max_retries}. Aguardando {wait_time:.2f} segundos...")
                    time.sleep(wait_time)
                else:
                    # Para outros erros HTTP, registre e levante a exceção
                    print(f"Erro HTTP: {e}")
                    raise
            except Exception as e:
                # Para outros tipos de exceções
                print(f"Erro inesperado: {e}")
                raise

        # Se saiu do loop de retry sem sucesso, interrompe o processamento
        if not success:
            print(
                f"Falha ao processar a página {pagina_atual} após {max_retries} tentativas. Continuando com os dados já coletados.")

        # Avançar para a próxima página
        pagina_atual += 1

        # Verificar se todas as páginas foram processadas
        if pagina_atual > ultima_pagina and ultima_pagina is not None:
            print(f"Todas as {ultima_pagina} páginas foram processadas.")
            break

        # Pausa entre requisições para evitar alcançar limites de taxa
        time.sleep(5)

    # Verificar se foram coletados dados
    if not todas_dados:
        print("Nenhum dado foi coletado. Encerrando sem atualizar o BigQuery.")
        return

    # Converter os dados coletados para um DataFrame
    df_precadastros = pd.DataFrame(todas_dados)
    total_registros = len(df_precadastros)
    print(f"Total de registros coletados: {total_registros}")

    # Verificar se coletamos o número esperado de registros
    if ultima_pagina is not None:
        registros_esperados = 500 * (ultima_pagina - 1) + registros_na_pagina  # Estimativa
        if total_registros < registros_esperados:
            print(
                f"AVISO: Número de registros coletados ({total_registros}) é menor que o esperado ({registros_esperados}).")

    # Carregar para o BigQuery
    client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_FILE, project=PROJECT_ID)
    table_ref = client.dataset(dataset_id).table(table_id)
    # Define the table schema
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.autodetect = True
    # Load the DataFrame to BigQuery
    load_job = client.load_table_from_dataframe(df_precadastros, table_ref, job_config=job_config)
    load_job.result()  # Wait for the job to complete

    print(f"Dados carregados com sucesso para {dataset_id}.{table_id}. Total: {total_registros} registros.")


def run_corretores(customer):
    if not customer['cvdw_active']:
        print(f"CVDW não está ativo para este cliente. Pulando execução da função run_leads.")
        return

    import requests
    import pandas as pd
    import os
    import time
    import pathlib
    import random
    from google.cloud import bigquery
    from requests.exceptions import HTTPError

    DOMINIO = customer['api_dominio']
    EMAIL = customer['api_email']
    ACCESS_TOKEN = customer['api_access_token']
    PROJECT_ID = customer['project_id']
    SERVICE_ACCOUNT_FILE = pathlib.Path('config', 'setup_automatico.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_FILE

    URL_BASE = f"https://{DOMINIO}.cvcrm.com.br/api/v1/cvdw/corretores"

    dataset_id = 'cvcrm'
    table_id = 'cvcrm_corretores'

    url_base = URL_BASE
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "email": EMAIL,
        "token": ACCESS_TOKEN
    }
    data = {
        "pagina": 1,
        "registros_por_pagina": 500
    }

    # Inicialização de variáveis
    todas_dados = []  # Para armazenar os dados coletados
    pagina_atual = 1
    max_retries = 5  # Número máximo de tentativas
    ultima_pagina = None
    registros_na_pagina = 0

    while True:
        # Atualizar o número da página na requisição
        data["pagina"] = pagina_atual

        retry_count = 0
        success = False

        # Loop de retry com backoff exponencial
        while not success and retry_count < max_retries:
            try:
                # Fazer a requisição
                response = requests.get(url_base, headers=headers, json=data)
                response.raise_for_status()  # Levanta exceção para status codes de erro
                response_data = response.json()
                success = True  # Se chegar aqui, a requisição foi bem-sucedida

                # Captura o total de páginas
                if ultima_pagina is None:
                    ultima_pagina = response_data["total_de_paginas"]
                    print(f"Total de páginas a processar: {ultima_pagina}")

                # Adicionar os dados da página atual na lista total
                if "dados" in response_data and response_data["dados"]:
                    todas_dados.extend(response_data["dados"])
                    registros_na_pagina = len(response_data["dados"])
                    print(
                        f"Página {pagina_atual}/{ultima_pagina} processada com sucesso. Registros: {registros_na_pagina}")
                else:
                    print(f"Página {pagina_atual}/{ultima_pagina} não contém dados.")

            except HTTPError as e:
                if e.response.status_code == 429:
                    retry_count += 1
                    # Backoff exponencial com jitter (variação aleatória)
                    wait_time = (2 ** retry_count) + random.uniform(0, 1)
                    print(
                        f"Erro 429 (Too Many Requests). Tentativa {retry_count}/{max_retries}. Aguardando {wait_time:.2f} segundos...")
                    time.sleep(wait_time)
                else:
                    # Para outros erros HTTP, registre e levante a exceção
                    print(f"Erro HTTP: {e}")
                    raise
            except Exception as e:
                # Para outros tipos de exceções
                print(f"Erro inesperado: {e}")
                raise

        # Se saiu do loop de retry sem sucesso, interrompe o processamento
        if not success:
            print(
                f"Falha ao processar a página {pagina_atual} após {max_retries} tentativas. Continuando com os dados já coletados.")

        # Avançar para a próxima página
        pagina_atual += 1

        # Verificar se todas as páginas foram processadas
        if pagina_atual > ultima_pagina and ultima_pagina is not None:
            print(f"Todas as {ultima_pagina} páginas foram processadas.")
            break

        # Pausa entre requisições para evitar alcançar limites de taxa
        time.sleep(5)

    # Verificar se foram coletados dados
    if not todas_dados:
        print("Nenhum dado foi coletado. Encerrando sem atualizar o BigQuery.")
        return

    # Converter os dados coletados para um DataFrame
    df_precadastros = pd.DataFrame(todas_dados)
    total_registros = len(df_precadastros)
    print(f"Total de registros coletados: {total_registros}")

    # Verificar se coletamos o número esperado de registros
    if ultima_pagina is not None:
        registros_esperados = 500 * (ultima_pagina - 1) + registros_na_pagina  # Estimativa
        if total_registros < registros_esperados:
            print(
                f"AVISO: Número de registros coletados ({total_registros}) é menor que o esperado ({registros_esperados}).")

    # Carregar para o BigQuery
    client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_FILE, project=PROJECT_ID)
    table_ref = client.dataset(dataset_id).table(table_id)
    # Define the table schema
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.autodetect = True
    # Load the DataFrame to BigQuery
    load_job = client.load_table_from_dataframe(df_precadastros, table_ref, job_config=job_config)
    load_job.result()  # Wait for the job to complete

    print(f"Dados carregados com sucesso para {dataset_id}.{table_id}. Total: {total_registros} registros.")


def run_historico_situacoes(customer):
    if not customer['cvdw_active']:
        print(f"CVDW não está ativo para este cliente. Pulando execução da função run_leads.")
        return

    import requests
    import pandas as pd
    import os
    import time
    import pathlib
    import random
    import datetime
    from google.cloud import bigquery
    from requests.exceptions import HTTPError, JSONDecodeError

    DOMINIO = customer['api_dominio']
    EMAIL = customer['api_email']
    ACCESS_TOKEN = customer['api_access_token']
    PROJECT_ID = customer['project_id']
    SERVICE_ACCOUNT_FILE = pathlib.Path('config', 'setup_automatico.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_FILE

    URL_BASE = f"https://{DOMINIO}.cvcrm.com.br/api/v1/cvdw/leads/historico/situacoes"

    dataset_id = 'cvcrm'
    table_id = 'cvcrm_leads_historico_situacoes'

    url_base = URL_BASE
    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "email": EMAIL,
        "token": ACCESS_TOKEN
    }

    one_year_ago = datetime.datetime.now() - datetime.timedelta(days=365)
    one_year_ago_formatted = one_year_ago.strftime('%Y-%m-%d %H:%M:%S')

    data = {
        "pagina": 1,
        "registros_por_pagina": 500,
        "a_partir_data_referencia": one_year_ago_formatted
    }

    # Inicialização de variáveis
    todas_dados = []  # Para armazenar os dados coletados
    pagina_atual = 1
    max_retries = 5  # Número máximo de tentativas
    ultima_pagina = None

    while True:
        # Atualizar o número da página na requisição
        data["pagina"] = pagina_atual

        retry_count = 0
        success = False

        # Loop de retry com backoff exponencial
        while not success and retry_count < max_retries:
            try:
                # Fazer a requisição
                response = requests.get(url_base, headers=headers, json=data)

                # Verificar se temos um conteúdo vazio antes de processar
                if not response.text.strip():
                    print(f"Resposta vazia na página {pagina_atual}. Provavelmente chegamos ao fim dos dados.")
                    # Se estamos tentando acessar uma página além da última, vamos considerar que terminamos
                    if ultima_pagina is not None and pagina_atual > ultima_pagina:
                        print(f"Página {pagina_atual} está além da última página ({ultima_pagina}). Finalizando.")
                    else:
                        print(f"Resposta vazia na página {pagina_atual}. Finalizando coleta de dados.")
                    success = True
                    break

                # Tenta fazer o parse do JSON com tratamento explícito de erros
                try:
                    response.raise_for_status()  # Levanta exceção para status codes de erro
                    response_data = response.json()
                except JSONDecodeError as e:
                    # Se não conseguimos decodificar o JSON e estamos além da última página conhecida,
                    # significa que provavelmente chegamos ao fim dos dados
                    if ultima_pagina is not None and pagina_atual > ultima_pagina:
                        print(
                            f"Erro ao decodificar JSON na página {pagina_atual}, que está além da última página conhecida ({ultima_pagina}). Finalizando.")
                        success = True
                        break
                    elif pagina_atual > 1:  # Se não é a primeira página, pode ser o fim dos dados
                        print(f"Erro ao decodificar JSON na página {pagina_atual}. Finalizando coleta de dados.")
                        success = True
                        break
                    else:
                        # Se é a primeira página, é um erro real
                        print(f"Erro ao decodificar JSON na página {pagina_atual}: {e}")
                        raise

                success = True  # Se chegar aqui, a requisição foi bem-sucedida

                # Captura o total de páginas
                if ultima_pagina is None and "total_de_paginas" in response_data:
                    ultima_pagina = response_data["total_de_paginas"]
                    print(f"Total de páginas a processar: {ultima_pagina}")

                # Adicionar os dados da página atual na lista total
                if "dados" in response_data and response_data["dados"]:
                    todas_dados.extend(response_data["dados"])
                    registros_na_pagina = len(response_data["dados"])
                    print(
                        f"Página {pagina_atual}/{ultima_pagina if ultima_pagina is not None else '?'} processada com sucesso. Registros: {registros_na_pagina}")
                else:
                    print(
                        f"Página {pagina_atual}/{ultima_pagina if ultima_pagina is not None else '?'} não contém dados.")
                    # Se chegamos aqui e não há dados, é um indicativo que terminamos
                    if ultima_pagina is None or pagina_atual >= ultima_pagina:
                        print("Não há mais dados para coletar. Finalizando.")
                        break

            except HTTPError as e:
                if e.response.status_code == 429:
                    retry_count += 1
                    # Backoff exponencial com jitter (variação aleatória)
                    wait_time = (2 ** retry_count) + random.uniform(0, 1)
                    print(
                        f"Erro 429 (Too Many Requests). Tentativa {retry_count}/{max_retries}. Aguardando {wait_time:.2f} segundos...")
                    time.sleep(wait_time)
                elif e.response.status_code == 204:
                    # Código 204 significa "No Content" - não há mais dados para buscar
                    print(f"Recebido código 204 (No Content) na página {pagina_atual}. Finalizando coleta de dados.")
                    success = True
                    break
                else:
                    # Para outros erros HTTP, registre e levante a exceção
                    print(f"Erro HTTP: {e}")
                    raise
            except JSONDecodeError as e:
                # Se estamos na última página ou além, podemos considerar que terminamos
                if ultima_pagina is not None and pagina_atual >= ultima_pagina:
                    print(
                        f"Erro ao decodificar JSON na página {pagina_atual}, que é a última página ou além. Finalizando.")
                    success = True
                    break
                else:
                    print(f"Erro ao decodificar JSON na página {pagina_atual}: {e}")
                    retry_count += 1
                    wait_time = (2 ** retry_count) + random.uniform(0, 1)
                    print(f"Tentativa {retry_count}/{max_retries}. Aguardando {wait_time:.2f} segundos...")
                    time.sleep(wait_time)
            except Exception as e:
                # Para outros tipos de exceções
                print(f"Erro inesperado: {e}")
                raise

        # Se saiu do loop de retry sem sucesso, interrompe o processamento
        if not success:
            print(
                f"Falha ao processar a página {pagina_atual} após {max_retries} tentativas. Continuando com os dados já coletados.")
            break

        # Verificar se todas as páginas foram processadas ou se não há mais dados
        if ultima_pagina is not None and pagina_atual >= ultima_pagina:
            print(f"Todas as {ultima_pagina} páginas foram processadas.")
            break

        # Avançar para a próxima página
        pagina_atual += 1

        # Pausa entre requisições para evitar alcançar limites de taxa
        time.sleep(5 + random.uniform(0, 2))

    # Verificar se foram coletados dados
    if not todas_dados:
        print("Nenhum dado foi coletado. Encerrando sem atualizar o BigQuery.")
        return

    # Converter os dados coletados para um DataFrame
    df_historico = pd.DataFrame(todas_dados)
    total_registros = len(df_historico)
    print(f"Total de registros coletados: {total_registros}")

    # Carregar para o BigQuery
    try:
        client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_FILE, project=PROJECT_ID)
        table_ref = client.dataset(dataset_id).table(table_id)
        # Define the table schema
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        job_config.autodetect = True
        # Load the DataFrame to BigQuery
        load_job = client.load_table_from_dataframe(df_historico, table_ref, job_config=job_config)
        load_job.result()  # Wait for the job to complete

        print(f"Dados carregados com sucesso para {dataset_id}.{table_id}. Total: {total_registros} registros.")
    except Exception as e:
        print(f"Erro ao carregar dados no BigQuery: {e}")
        raise


def run_lead(customer):
    if not customer['cvdw_active']:
        print(f"CVDW não está ativo para este cliente. Pulando execução da função run_leads.")
        return

    import requests
    import pandas as pd
    import os
    import time
    import pathlib
    import json
    from google.cloud import bigquery
    import psutil
    import numpy as np

    # Configuração
    PROJECT_ID = customer['project_id']
    DOMINIO = customer['api_dominio']
    EMAIL = customer['api_email']
    ACCESS_TOKEN = customer['api_access_token']

    # Configurações da API
    URL_BASE = f"https://{DOMINIO}.cvcrm.com.br/api/cvio/lead"

    # Configurações do BigQuery
    SERVICE_ACCOUNT_FILE = pathlib.Path('config', 'setup_automatico.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_FILE

    dataset_id = 'cvcrm'
    table_id = 'cvcrm_leads'

    def get_leads_data(url_base, headers, limit, offset, max_retries=3):
        params = {
            "limit": str(limit),
            "offset": str(offset)
        }

        for attempt in range(max_retries):
            try:
                response = requests.get(url_base, headers=headers, params=params)
                response.raise_for_status()
                data = response.json()
                # Verifica se há alguma informação sobre total de registros na resposta
                total_records = data.get('total', 0)
                leads = data.get('leads', [])
                return leads, total_records
            except requests.exceptions.HTTPError as e:
                print(f"HTTP error occurred: {e}")
                if response.status_code in [520, 525]:
                    print(f"Erro na requisição: {response.status_code}. Tentando novamente em 5 segundos...")
                    time.sleep(5)
                else:
                    break
            except requests.exceptions.RequestException as e:
                print(f"Error occurred: {e}")
                break
            except json.JSONDecodeError:
                print(f"Erro na decodificação JSON. Resposta: {response.text}")
                break

        return None, 0

    def normalize_nested_columns(df):
        # Extraindo valores de colunas aninhadas
        df['gestor'] = df['gestor'].apply(lambda x: x.get('nome') if isinstance(x, dict) else None)
        df['imobiliaria'] = df['imobiliaria'].apply(lambda x: x.get('nome') if isinstance(x, dict) else None)
        df['corretor'] = df['corretor'].apply(lambda x: x.get('nome') if isinstance(x, dict) else None)
        df['situacao'] = df['situacao'].apply(lambda x: x.get('nome') if isinstance(x, dict) else None)
        df['empreendimento'] = df['empreendimento'].apply(
            lambda x: ', '.join([emp.get('nome') for emp in x]) if isinstance(x, list) else None)
        df['motivo_cancelamento'] = df['motivo_cancelamento'].apply(
            lambda x: x.get('nome') if isinstance(x, dict) else None)
        df['submotivo_cancelamento'] = df['submotivo_cancelamento'].apply(
            lambda x: x.get('nome') if isinstance(x, dict) else None)
        return df

    def clean_value(value):
        if isinstance(value, str):
            value = value.replace("[", "").replace("]", "").replace("'", "").strip()
            if value.endswith('.0'):
                value = value[:-2]
            if 'nan' in value or value == '':
                value = '0'
        elif isinstance(value, list):
            cleaned_list = [str(item).replace('.0', '').replace('nan', '0').strip() for item in value if
                            pd.notnull(item)]
            value = ', '.join(cleaned_list) if cleaned_list else '0'
        elif isinstance(value, float) and np.isnan(value):
            value = '0'
        elif isinstance(value, (int, float)):
            value = str(value)
        else:
            value = '0'
        return value

    def upload_to_bigquery(dataset_id, table_id, dataframe):
        client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_FILE, project=PROJECT_ID)
        table_ref = client.dataset(dataset_id).table(table_id)

        # Define the table schema
        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        job_config.autodetect = True

        # Load the DataFrame to BigQuery
        load_job = client.load_table_from_dataframe(dataframe, table_ref, job_config=job_config)
        load_job.result()  # Wait for the job to complete

        print(f"Table {table_id} loaded to dataset {dataset_id} in BigQuery.")

    def print_memory_usage():
        process = psutil.Process(os.getpid())
        mem_info = process.memory_info()
        print(f"Consumo de memória: {mem_info.rss / (1024 ** 2):.2f} MB")

    def main():
        url_base = URL_BASE
        headers = {
            "token": ACCESS_TOKEN,
            "email": EMAIL
        }

        limit = 1000  # Manter o limite de registros por página em 1000
        offset = 0
        total_records = None  # Variável para armazenar o total de registros

        all_leads = []
        start_time = time.time()

        # Primeira chamada para obter o total de registros
        initial_leads, total_records = get_leads_data(url_base, headers, limit, offset)

        if initial_leads is None:
            print("Falha ao obter leads. Encerrando a coleta de dados.")
            return

        all_leads.extend(initial_leads)
        expected_pages = (total_records // limit) + (1 if total_records % limit > 0 else 0)

        print(f"Total de registros: {total_records}")
        print(f"Páginas esperadas: {expected_pages}")

        # Iniciar da segunda página (offset = limit)
        offset = limit
        page = 1

        # Continuar a paginação até obter todos os registros
        while offset < total_records:
            print(f"Buscando página {page + 1} / {expected_pages} (Offset: {offset})")
            leads, _ = get_leads_data(url_base, headers, limit, offset)

            if leads is None:
                print(f"Falha ao obter leads na página {page + 1}. Tentando novamente...")
                # Esperar um pouco antes de tentar novamente
                time.sleep(10)
                leads, _ = get_leads_data(url_base, headers, limit, offset)

                if leads is None:
                    print(f"Falha novamente. Prosseguindo para a próxima página.")
                    offset += limit
                    page += 1
                    continue

            if not leads:
                print(f"Página {page + 1} retornou vazia. Verificando se é a última página...")

                # Verifica se já obtemos todos os registros esperados
                if len(all_leads) >= total_records:
                    print("Todos os registros foram obtidos. Finalizando.")
                    break

                # Se não obtivemos todos os registros, verificamos se há problemas com o offset
                verify_offset = offset - 1  # Tentativa com offset ajustado
                verify_leads, _ = get_leads_data(url_base, headers, 1, verify_offset)

                if verify_leads:
                    print(f"Encontrados registros com offset ajustado. Tentando continuar a paginação...")
                else:
                    print("Nenhum registro adicional encontrado. Finalizando a coleta.")
                    break

            all_leads.extend(leads)
            offset += limit
            page += 1

            # Imprimir progresso
            print(f"Registros coletados até agora: {len(all_leads)} / {total_records}")

            # Imprimir o uso de memória a cada iteração
            print_memory_usage()

        if all_leads:
            print(f"Total de leads coletados: {len(all_leads)}")
            df = pd.DataFrame(all_leads)

            # Manter apenas as colunas desejadas
            columns_to_keep = [
                'idlead', 'gestor', 'imobiliaria', 'corretor', 'situacao', 'nome', 'score',
                'data_cad', 'midia_principal', 'sexo', 'renda_familiar', 'valor_negocio', 'estado',
                'cidade', 'profissao', 'origem', 'data_reativacao', 'data_vencimento', 'ultima_data_conversao',
                'empreendimento', 'tags', 'autor_ultima_alteracao', 'qtde_simulacoes_associadas',
                'qtde_reservas_associadas', 'link_interacoes', 'link_simulacoes', 'link_reservas',
                'link_interesses', 'idrd_station', 'link_rdstation', 'midias', 'interacao',
                'data_cancelamento', 'motivo_cancelamento', 'submotivo_cancelamento', 'valor_venda',
                'campos_adicionais', 'data_venda'
            ]

            # Verificar se todas as colunas existem no DataFrame
            columns_to_keep = [col for col in columns_to_keep if col in df.columns]

            df = df[columns_to_keep]

            # Normalizar e limpar as colunas aninhadas
            df = normalize_nested_columns(df)

            # Aplicar a limpeza a todos os valores no DataFrame
            for column in df.columns:
                df[column] = df[column].apply(lambda x: clean_value(x))

            # Upload the transformed DataFrame to BigQuery
            upload_to_bigquery(dataset_id, table_id, df)
        else:
            print("Nenhum lead foi coletado.")

        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"Tempo total de execução: {elapsed_time:.2f} segundos")

    # START
    main()


def get_extraction_tasks():
    """
    Get the list of data extraction tasks for CVCRM.

    Returns:
        list: List of task configurations
    """
    return [
        {
            'task_id': 'run_leads',
            'python_callable': run_leads,
        },
        {
            'task_id': 'run_vendas',
            'python_callable': run_vendas
        },
        {
            'task_id': 'run_reservas',
            'python_callable': run_reservas,
        },
        {
            'task_id': 'run_precadastros',
            'python_callable': run_precadastros,
        },
        {
            'task_id': 'run_corretores',
            'python_callable': run_corretores,
        },
        {
            'task_id': 'run_historico_situacoes',
            'python_callable': run_historico_situacoes,
        },
        {
            'task_id': 'run_lead',
            'python_callable': run_lead,
        }
    ]
