"""
CVCRM module for data extraction functions.
This module contains functions specific to the CVCRM integration.
"""

import requests
import pandas as pd
import os
import time
from google.cloud import bigquery
import json
import psutil
import numpy as np
from core import gcs

def run_leads(customer):
    """
    Extract leads data from CVCRM API and load it to BigQuery.
    
    Args:
        customer (dict): Customer information dictionary
    """
    # Configurações da API
    DOMINIO = customer['domain']
    URL_BASE = f"https://{DOMINIO}.cvcrm.com.br/api/cvio/lead"
    EMAIL = customer['email']
    ACCESS_TOKEN = customer['access_token']

    # Configurações do BigQuery
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
                return data.get('leads', [])
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
        
        return None

    def normalize_nested_columns(df):
        # Extraindo valores de colunas aninhadas
        df['gestor'] = df['gestor'].apply(lambda x: x.get('nome') if isinstance(x, dict) else None)
        df['imobiliaria'] = df['imobiliaria'].apply(lambda x: x.get('nome') if isinstance(x, dict) else None)
        df['corretor'] = df['corretor'].apply(lambda x: x.get('nome') if isinstance(x, dict) else None)
        df['situacao'] = df['situacao'].apply(lambda x: x.get('nome') if isinstance(x, dict) else None)
        df['empreendimento'] = df['empreendimento'].apply(lambda x: ', '.join([emp.get('nome') for emp in x]) if isinstance(x, list) else None)
        df['motivo_cancelamento'] = df['motivo_cancelamento'].apply(lambda x: x.get('nome') if isinstance(x, dict) else None)
        df['submotivo_cancelamento'] = df['submotivo_cancelamento'].apply(lambda x: x.get('nome') if isinstance(x, dict) else None)
        return df

    def clean_value(value):
        if isinstance(value, str):
            value = value.replace("[", "").replace("]", "").replace("'", "").strip()
            if value.endswith('.0'):
                value = value[:-2]
            if 'nan' in value or value == '':
                value = '0'
        elif isinstance(value, list):
            cleaned_list = [str(item).replace('.0', '').replace('nan', '0').strip() for item in value if pd.notnull(item)]
            value = ', '.join(cleaned_list) if cleaned_list else '0'
        elif isinstance(value, float) and np.isnan(value):
            value = '0'
        elif isinstance(value, (int, float)):
            value = str(value)
        else:
            value = '0'
        return value

    def upload_to_bigquery(dataset_id, table_id, dataframe):
        client = bigquery.Client(credentials=gcs.load_credentials_from_env(), project=customer['project_id'])
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

        limit = 500
        offset = 0

        all_leads = []

        start_time = time.time()

        while True:
            print(f"Offset: {offset}")
            leads = get_leads_data(url_base, headers, limit, offset)
            if leads is None:
                print("Falha ao obter leads. Encerrando a coleta de dados.")
                break
            if not leads:
                break

            all_leads.extend(leads)
            offset += limit

            # Imprimir o uso de memória a cada iteração
            print_memory_usage()

        if all_leads:
            df = pd.DataFrame(all_leads)

            # Manter apenas as colunas desejadas que existem no DataFrame
            columns_to_keep = [
                'idlead', 'gestor', 'imobiliaria', 'corretor', 'situacao', 'nome', 'score',
                'data_cad', 'midia_principal', 'sexo', 'renda_familiar', 'valor_negocio', 'estado',
                'cidade', 'profissao', 'origem', 'data_reativacao', 'data_vencimento', 'ultima_data_conversao',
                'empreendimento', 'tags', 'autor_ultima_alteracao', 'qtde_simulacoes_associadas', 
                'qtde_reservas_associadas', 'link_interacoes', 'link_simulacoes', 'link_reservas', 
                'link_interesses', 'idrd_station', 'link_rdstation', 'midias', 'interacao', 
                'data_cancelamento', 'motivo_cancelamento', 'submotivo_cancelamento', 'valor_venda', 
                'campos_adicionais', 'data_venda', 'conversao', 'conversao_ultimo', 'conversao_original'
            ]

            # Verificar se as colunas existem antes de tentar acessá-las
            existing_columns = [col for col in columns_to_keep if col in df.columns]
            print(existing_columns)
            df = df[existing_columns]

            # Normalizar e limpar as colunas aninhadas
            df = normalize_nested_columns(df)

            # Aplicar a limpeza a todos os valores no DataFrame
            for column in df.columns:
                df[column] = df[column].apply(lambda x: clean_value(x))

            # Upload the transformed DataFrame to BigQuery
            upload_to_bigquery(dataset_id, table_id, df)

        end_time = time.time()
        elapsed_time = end_time - start_time
        print(f"Tempo total de execução: {elapsed_time:.2f} segundos")
    main()


def run_historico_situacoes(customer):
    """
    Extract historical situation data from CVCRM API and load it to BigQuery.
    
    Args:
        customer (dict): Customer information dictionary
    """
    import requests
    import pandas as pd
    import time
    from google.cloud import bigquery

    DOMINIO = customer['domain']
    EMAIL = customer['email']
    ACCESS_TOKEN = customer['access_token']
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

    while True:
        # Atualizar o número da página na requisição
        data["pagina"] = pagina_atual
        
        # Fazer a requisição
        response = requests.get(url_base, headers=headers, json=data)
        try:
            response.raise_for_status()
        except Exception as e:
            print(response.text)
            raise e
            
        response_data = response.json()
        
        # Adicionar os dados da página atual na lista total
        if "dados" in response_data and response_data["dados"]:
            todas_dados.extend(response_data["dados"])
        
        # Verificar se todas as páginas foram lidas
        if pagina_atual >= response_data["total_de_paginas"]:
            break  # Finalizar o loop quando atingir a última página
        
        # Avançar para a próxima página
        pagina_atual += 1
        time.sleep(15)

        print(f"Página {pagina_atual} processada.")

    # Converter os dados coletados para um DataFrame, se necessário
    df_historico = pd.DataFrame(todas_dados)
    print(f"Total de registros coletados: {len(df_historico)}")

    client = bigquery.Client(credentials=gcs.load_credentials_from_env(), project=customer['project_id'])
    table_ref = client.dataset(dataset_id).table(table_id)
    # Define the table schema
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.autodetect = True
    # Load the DataFrame to BigQuery
    load_job = client.load_table_from_dataframe(df_historico, table_ref, job_config=job_config)
    load_job.result()  # Wait for the job to complete


def run_reservas(customer):
    """
    Extract reservations data from CVCRM API and load it to BigQuery.
    
    Args:
        customer (dict): Customer information dictionary
    """
    import requests
    import pandas as pd
    import time
    from google.cloud import bigquery

    DOMINIO = customer['domain']
    EMAIL = customer['email']
    ACCESS_TOKEN = customer['access_token']
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
        pagina_atual += 1
        time.sleep(5)

        print(f"Página {pagina_atual} processada.")

    # Converter os dados coletados para um DataFrame, se necessário
    df_reservas = pd.DataFrame(todas_dados)
    print(f"Total de registros coletados: {len(df_reservas)}")

    client = bigquery.Client(credentials=gcs.load_credentials_from_env(), project=customer['project_id'])
    table_ref = client.dataset(dataset_id).table(table_id)
    # Define the table schema
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.autodetect = True
    # Load the DataFrame to BigQuery
    load_job = client.load_table_from_dataframe(df_reservas, table_ref, job_config=job_config)
    load_job.result()  # Wait for the job to complete


def run_vendas(customer):
    """
    Extract sales data from CVCRM API and load it to BigQuery.
    
    Args:
        customer (dict): Customer information dictionary
    """
    import requests
    import pandas as pd
    import time
    from google.cloud import bigquery

    DOMINIO = customer['domain']
    EMAIL = customer['email']
    ACCESS_TOKEN = customer['access_token']
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
        pagina_atual += 1
        time.sleep(5)

        print(f"Página {pagina_atual} processada.")

    # Converter os dados coletados para um DataFrame, se necessário
    df_vendas = pd.DataFrame(todas_dados)
    print(f"Total de registros coletados: {len(df_vendas)}")

    client = bigquery.Client(credentials=gcs.load_credentials_from_env(), project=customer['project_id'])
    table_ref = client.dataset(dataset_id).table(table_id)
    # Define the table schema
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.autodetect = True
    # Load the DataFrame to BigQuery
    load_job = client.load_table_from_dataframe(df_vendas, table_ref, job_config=job_config)
    load_job.result()  # Wait for the job to complete


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
            'pool': 'cvcrm_integration'
        },
        {
            'task_id': 'run_historico_situacoes',
            'python_callable': run_historico_situacoes,
            'pool': 'cvcrm_integration'
        },
        {
            'task_id': 'run_reservas',
            'python_callable': run_reservas,
            'pool': 'cvcrm_integration'
        },
        {
            'task_id': 'run_vendas',
            'python_callable': run_vendas,
            'pool': 'cvcrm_integration'
        }
    ]
