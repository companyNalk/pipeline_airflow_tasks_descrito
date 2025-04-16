"""
Exact Sales module for data extraction functions.
This module contains functions specific to the Exact Sales integration.
"""

from core import gcs


def run(customer):
    import requests
    import pandas as pd
    import time
    import csv
    import re
    import unicodedata
    from io import StringIO
    from datetime import datetime
    from google.cloud import storage
    import os
    import pathlib

    # Configurações Globais
    SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'setup_automatico.json').as_posix()

    # Configurações do GCS
    BUCKET_NAME = customer['bucket_name']
    FOLDERS = {
        "leads": "Leads",
        "leads_lost": "LeadsLost",
        "leads_sold": "LeadsSold",
        "stages": "Stages",
        "custom_fields_leads": "CustomFieldsLeads",
        "pipelines": "Pipelines"
    }

    # Configurações da API
    URL_BASE = "https://api.exactspotter.com/v3"
    TOKEN = customer['api_token']
    HEADERS = {
        "Content-Type": "application/json",
        "token_exact": TOKEN
    }

    # Configurar o cliente GCS
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH

    def normalize_column_name(column_name):
        if not isinstance(column_name, str):
            column_name = str(column_name)
        
        column_name = column_name.lower()
        column_name = unicodedata.normalize('NFKD', column_name).encode('ASCII', 'ignore').decode('ASCII')
        column_name = re.sub(r'[^a-z0-9_]', '_', column_name)
        column_name = re.sub(r'_+', '_', column_name).strip('_')
        
        return column_name

    def normalize_dataframe(df):
        if df.empty:
            return df
        
        df.columns = [normalize_column_name(col) for col in df.columns]
        df = df.loc[:, ~df.columns.duplicated()]
        
        return df

    def upload_df_to_gcs(df, folder, filename):
        try:
            df = normalize_dataframe(df)
            
            storage_client = storage.Client()
            bucket = storage_client.bucket(BUCKET_NAME)
            blob_path = f"{folder}/{filename}"
            blob = bucket.blob(blob_path)
            
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False, sep=";", encoding="utf-8", quotechar='"', quoting=csv.QUOTE_ALL)
            csv_buffer.seek(0)

            blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')
            print(f"Arquivo {blob_path} enviado para o bucket {BUCKET_NAME} com sucesso.")
            print(f"Colunas normalizadas: {', '.join(df.columns.tolist())}")
        except Exception as e:
            print(f"Erro ao enviar o arquivo para o GCS: {e}")

    def coletar_dados_com_paginacao(url, headers, page_size=500, max_retries=5, backoff_time=5):
        todos_os_dados = []
        skip = 0
        
        while True:
            params = {
                "$skip": skip,
                "$top": page_size
            }
            
            for attempt in range(max_retries):
                try:
                    # Requisição para a API
                    response = requests.get(url, headers=headers, params=params)
                    
                    if response.status_code == 500:
                        print(f"Erro 500 na requisição. Tentativa {attempt+1}/{max_retries}")
                        time.sleep(backoff_time * (2 ** attempt))  # Backoff exponencial
                        continue
                    elif response.status_code != 200:
                        print(f"Erro na requisição: {response.status_code}, Mensagem: {response.text}")
                        return todos_os_dados
                    
                    dados = response.json().get("value", [])
                    if not dados:
                        return todos_os_dados
                    
                    todos_os_dados.extend(dados)
                    print(f"Coletando dados... até agora {len(todos_os_dados)} registros coletados.")
                    skip += page_size
                    break
                    
                except requests.exceptions.RequestException as e:
                    print(f"Exceção durante a requisição: {e}")
                    time.sleep(backoff_time * (2 ** attempt))
            else:
                print("Número máximo de re-tentativas alcançado, interrompendo a coleta.")
                break

        return todos_os_dados

    def normalizar_dados_leads(dados):
        dados_normalizados = []
        for item in dados:
            if isinstance(item.get('telephones'), list):
                item['telephones'] = ', '.join(str(t) for t in item['telephones'])
            
            for campo in ['source', 'industry', 'subSource']:
                if isinstance(item.get(campo), dict):
                    item[f'{campo}_id'] = item[campo].get('id')
                    item[f'{campo}_value'] = item[campo].get('value')
                    del item[campo]
            
            for campo in ['sdr', 'salesRep']:
                if isinstance(item.get(campo), dict):
                    item[f'{campo}_id'] = item[campo].get('id')
                    item[f'{campo}_name'] = item[campo].get('name')
                    item[f'{campo}_lastName'] = item[campo].get('lastName')
                    item[f'{campo}_email'] = item[campo].get('email')
                    item[f'{campo}_phone'] = item[campo].get('phone')
                    item[f'{campo}_phone2'] = item[campo].get('phone2')
                    item[f'{campo}_active'] = item[campo].get('active')
                    del item[campo]
            
            dados_normalizados.append(item)
        return dados_normalizados

    def normalizar_dados_leads_sold(dados):
        dados_normalizados = []
        for item in dados:
            if 'salesRep' in item and isinstance(item['salesRep'], dict):
                item['salesRep_name'] = item['salesRep'].get('name')
                item['salesRep_lastName'] = item['salesRep'].get('lastName')
                item['salesRep_email'] = item['salesRep'].get('email')
                del item['salesRep']
            
            if 'preSales' in item and isinstance(item['preSales'], dict):
                item['preSales_id'] = item['preSales'].get('id')
                item['preSales_name'] = item['preSales'].get('name')
                item['preSales_lastName'] = item['preSales'].get('lastName')
                item['preSales_email'] = item['preSales'].get('email')
                del item['preSales']
            
            if 'products' in item and isinstance(item['products'], list):
                for i, product in enumerate(item['products']):
                    item[f'product_{i}_id'] = product.get('id')
                    item[f'product_{i}_quantity'] = product.get('quantity')
                    item[f'product_{i}_individualValue'] = product.get('individualValue')
                    item[f'product_{i}_discountAmount'] = product.get('discountAmount')
                    item[f'product_{i}_discountType'] = product.get('discountType')
                    item[f'product_{i}_fullValue'] = product.get('fullValue')
                    item[f'product_{i}_finalValue'] = product.get('finalValue')
                    item[f'product_{i}_name'] = product.get('name')
                del item['products']
            
            dados_normalizados.append(item)
        return dados_normalizados

    def processar_campos_personalizados_leads(dados):
        if not dados:
            return pd.DataFrame()
        
        df = pd.DataFrame(dados)
        df = df[['id', 'key', 'value', 'leadId']]
        df = normalize_dataframe(df)
        
        df_pivot = df.pivot_table(
            index='leadid', 
            columns='key', 
            values='value', 
            aggfunc=lambda x: ' '.join(str(v) for v in x)
        )
        
        df_pivot.reset_index(inplace=True)
        df_pivot = normalize_dataframe(df_pivot)
        
        return df_pivot

    def coletar_leads():
        print("Iniciando coleta de Leads")
        start_time = datetime.now()
        
        endpoint = "/Leads"
        url = f"{URL_BASE}{endpoint}"
        
        try:
            dados = coletar_dados_com_paginacao(url, HEADERS)
            if dados:
                dados_normalizados = normalizar_dados_leads(dados)
                df = pd.DataFrame(dados_normalizados)
                upload_df_to_gcs(df, FOLDERS["leads"], "leads.csv")
                print(f"Processamento de Leads concluído. {len(dados)} registros processados.")
            else:
                print("Nenhum Lead foi coletado.")
        except Exception as e:
            print(f"Erro ao processar Leads: {str(e)}")
        
        end_time = datetime.now()
        print(f"Tempo de execução para Leads: {end_time - start_time}")

    def coletar_leads_perdidos():
        print("Iniciando coleta de Leads Perdidos")
        start_time = datetime.now()
        
        endpoint = "/Losts"
        url = f"{URL_BASE}{endpoint}"
        
        try:
            dados = coletar_dados_com_paginacao(url, HEADERS)
            if dados:
                df = pd.DataFrame(dados)
                upload_df_to_gcs(df, FOLDERS["leads_lost"], "leads_lost.csv")
                print(f"Processamento de Leads Perdidos concluído. {len(dados)} registros processados.")
            else:
                print("Nenhum Lead Perdido foi coletado.")
        except Exception as e:
            print(f"Erro ao processar Leads Perdidos: {str(e)}")
        
        end_time = datetime.now()
        print(f"Tempo de execução para Leads Perdidos: {end_time - start_time}")

    def coletar_leads_vendidos():
        print("Iniciando coleta de Leads Vendidos")
        start_time = datetime.now()
        
        endpoint = "/LeadsSold"
        url = f"{URL_BASE}{endpoint}"
        
        try:
            dados = coletar_dados_com_paginacao(url, HEADERS)
            if dados:
                dados_normalizados = normalizar_dados_leads_sold(dados)
                df = pd.DataFrame(dados_normalizados)
                upload_df_to_gcs(df, FOLDERS["leads_sold"], "leads_sold.csv")
                print(f"Processamento de Leads Vendidos concluído. {len(dados)} registros processados.")
            else:
                print("Nenhum Lead Vendido foi coletado.")
        except Exception as e:
            print(f"Erro ao processar Leads Vendidos: {str(e)}")
        
        end_time = datetime.now()
        print(f"Tempo de execução para Leads Vendidos: {end_time - start_time}")

    def coletar_stages():
        print("Iniciando coleta de Stages")
        start_time = datetime.now()
        
        endpoint = "/stages"
        url = f"{URL_BASE}{endpoint}"
        
        try:
            response = requests.get(url, headers=HEADERS)
            if response.status_code == 200:
                dados = response.json().get("value", [])
                if dados:
                    df = pd.DataFrame(dados)
                    upload_df_to_gcs(df, FOLDERS["stages"], "stages.csv")
                    print(f"Processamento de Stages concluído. {len(dados)} registros processados.")
                else:
                    print("Nenhuma Stage foi coletada.")
            else:
                print(f"Erro na requisição de Stages: {response.status_code}, Mensagem: {response.text}")
        except Exception as e:
            print(f"Erro ao processar Stages: {str(e)}")
        
        end_time = datetime.now()
        print(f"Tempo de execução para Stages: {end_time - start_time}")

    def coletar_campos_personalizados_leads():
        print("Iniciando coleta de Campos Personalizados de Leads")
        start_time = datetime.now()
        
        endpoint = "/CustomFieldsLeads"
        url = f"{URL_BASE}{endpoint}"
        
        try:
            dados = coletar_dados_com_paginacao(url, HEADERS, page_size=250)
            if dados:
                df_processado = processar_campos_personalizados_leads(dados)
                
                if not df_processado.empty:
                    upload_df_to_gcs(df_processado, FOLDERS["custom_fields_leads"], "campos_personalizados_leads.csv")
                    print(f"Processamento de Campos Personalizados concluído. Dados transformados de {len(dados)} registros.")
                else:
                    print("Falha ao processar os Campos Personalizados.")
            else:
                print("Nenhum Campo Personalizado foi coletado.")
        except Exception as e:
            print(f"Erro ao processar Campos Personalizados: {str(e)}")
        
        end_time = datetime.now()
        print(f"Tempo de execução para Campos Personalizados: {end_time - start_time}")

    # Função para coletar Pipelines (Funnels)
    def coletar_pipelines():
        print("Iniciando coleta de Pipelines")
        start_time = datetime.now()
        
        endpoint = "/Funnels"
        url = f"{URL_BASE}{endpoint}"
        
        try:
            dados = coletar_dados_com_paginacao(url, HEADERS)
            if dados:
                df = pd.DataFrame(dados)
                upload_df_to_gcs(df, FOLDERS["pipelines"], "pipelines.csv")
                print(f"Processamento de Pipelines concluído. {len(dados)} registros processados.")
            else:
                print("Nenhum Pipeline foi coletado.")
        except Exception as e:
            print(f"Erro ao processar Pipelines: {str(e)}")
        
        end_time = datetime.now()
        print(f"Tempo de execução para Pipelines: {end_time - start_time}")

    def main():
        print("Iniciando processo de coleta de dados da API Exact Spotter")
        start_time = datetime.now()
        print(f"Iniciando coleta de dados às {start_time}")
        
        # Coletar dados de cada endpoint
        coletar_leads()
        coletar_leads_perdidos()
        coletar_leads_vendidos()
        coletar_stages()
        coletar_campos_personalizados_leads()
        coletar_pipelines()  # Coleta de pipelines adicionada
        
        end_time = datetime.now()
        execution_time = end_time - start_time
        
        print(f"Coleta finalizada às {end_time}")
        print(f"Tempo total de execução: {execution_time}")

    main()

def get_extraction_tasks():
    """
    Get the list of data extraction tasks for Exact Sales.
    
    Returns:
        list: List of task configurations
    """
    return [
        {
            'task_id': 'run',
            'python_callable': run
        }
    ]
