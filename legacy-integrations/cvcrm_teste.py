"""
CVCRM module for data extraction functions.
This module contains functions specific to the CVCRM integration.
"""

from core import gcs


def run_leads(customer):
    pass


def run_vendas(customer):
    import requests
    import pandas as pd
    import os
    import time
    import pathlib
    from google.cloud import bigquery

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

    while True:
        # Atualizar o número da página na requisição
        data["pagina"] = pagina_atual

        # Fazer a requisição
        response = requests.get(url_base, headers=headers, json=data)
        response.raise_for_status()
        response_data = response.json()

        # Adicionar os dados da página atual na lista total
        if "dados" in response_data and response_data["dados"]:
            todas_dados.extend(response_data["dados"])

        # Verificar se todas as páginas foram lidas
        if pagina_atual >= response_data["total_de_paginas"]:
            break  # Finalizar o loop quando atingir a última página

        # Avançar para a próxima página
        # pagina_atual += 1
        time.sleep(5)

        print(f"Página {pagina_atual} processada.")

    # Converter os dados coletados para um DataFrame, se necessário
    df_vendas = pd.DataFrame(todas_dados)
    # print(f"Total de registros coletados: {len(df_historico)}")

    client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_FILE, project=PROJECT_ID)
    table_ref = client.dataset(dataset_id).table(table_id)
    # Define the table schema
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.autodetect = True
    # Load the DataFrame to BigQuery
    load_job = client.load_table_from_dataframe(df_vendas, table_ref, job_config=job_config)
    load_job.result()  # Wait for the job to complete


def run_reservas(customer):
    pass


def run_precadastros(customer):
    pass


def run_historico_situacoes(customer):
    pass


def run_lead(customer):
    pass


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
