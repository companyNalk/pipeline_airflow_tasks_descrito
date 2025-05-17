"""
CVCRM module for data extraction functions.
This module contains functions specific to the CVCRM integration.
"""

from core import gcs
from requests import RequestException


def run_leads(customer):
    import requests
    import pandas as pd
    import os
    import time
    import pathlib
    import random
    from google.cloud import bigquery
    from requests.exceptions import HTTPError

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
    ultima_pagina = None
    registros_na_pagina = 0

    while True:
        data["pagina"] = pagina_atual
        retry_count = 0
        success = False

        # Retry loop with exponential backoff
        while not success and retry_count < max_retries:
            try:
                response = requests.get(URL_BASE, headers=headers, json=data, timeout=30)
                response.raise_for_status()
                response_data = response.json()

                # Validate response structure
                if not isinstance(response_data, dict) or "total_de_paginas" not in response_data:
                    raise ValueError(f"Resposta da API inválida na página {pagina_atual}: {response_data}")

                success = True

                # Capture total pages on first successful request
                if ultima_pagina is None:
                    ultima_pagina = response_data["total_de_paginas"]
                    print(f"Total de páginas a processar: {ultima_pagina}")

                # Process page data
                page_data = response_data.get("dados", [])
                registros_na_pagina = len(page_data)
                if page_data:
                    todas_dados.extend(page_data)
                    print(
                        f"Página {pagina_atual}/{ultima_pagina} processada com sucesso. Registros: {registros_na_pagina}")
                else:
                    print(f"Página {pagina_atual}/{ultima_pagina} não contém dados.")

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
            except RequestException:
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

        # Check if all pages are processed
        if ultima_pagina is not None and pagina_atual >= ultima_pagina:
            print(f"Todas as {ultima_pagina} páginas foram processadas.")
            break

        # Increment page
        pagina_atual += 1

        # Adaptive pause to respect API rate limits
        time.sleep(5 + random.uniform(0, 2))

    # Check if any data was collected
    if not todas_dados:
        print("Nenhum dado foi coletado. Encerrando sem atualizar o BigQuery.")
        return

    # Convert to DataFrame
    df_leads = pd.DataFrame(todas_dados)
    total_registros = len(df_leads)
    print(f"Total de registros coletados: {total_registros}")

    # Validate total records
    if ultima_pagina is not None:
        registros_esperados = 500 * (ultima_pagina - 1) + registros_na_pagina
        if total_registros < registros_esperados:
            print(f"AVISO: Registros coletados ({total_registros}) menor que esperado ({registros_esperados}).")

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


def run_historico_situacoes(customer):
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
    df_historico = pd.DataFrame(todas_dados)
    total_registros = len(df_historico)
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
    load_job = client.load_table_from_dataframe(df_historico, table_ref, job_config=job_config)
    load_job.result()  # Wait for the job to complete

    print(f"Dados carregados com sucesso para {dataset_id}.{table_id}. Total: {total_registros} registros.")


def run_lead(customer):
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

    URL_BASE = f"https://{DOMINIO}.cvcrm.com.br/api/v1/cvdw/leads/corretores"

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
    df_corretores = pd.DataFrame(todas_dados)
    total_registros = len(df_corretores)
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
    load_job = client.load_table_from_dataframe(df_corretores, table_ref, job_config=job_config)
    load_job.result()  # Wait for the job to complete

    print(f"Dados carregados com sucesso para {dataset_id}.{table_id}. Total: {total_registros} registros.")


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
            'task_id': 'run_historico_situacoes',
            'python_callable': run_historico_situacoes,
        },
        {
            'task_id': 'run_lead',
            'python_callable': run_lead,
        }
    ]
