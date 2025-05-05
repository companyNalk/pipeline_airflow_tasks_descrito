import csv
import io
import json
import logging
import os
import pathlib

import pandas as pd
import requests
from google.api_core.exceptions import GoogleAPIError
from google.cloud import storage

logger = logging.getLogger(__name__)


def run_accounts(customer):
    API_BASE_URL = customer['api_base_url']
    API_TOKEN = customer['api_token']
    BUCKET_NAME = customer['bucket_name']
    FOLDER = "accounts"
    SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH

    # Faz upload do arquivo para o GCS
    def upload_to_gcs(bucket_name, destination_blob_name, content):
        """Faz upload de uma string como blob para o Google Cloud Storage (GCS)."""
        storage_client = storage.Client(project=customer['project_id'])
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_string(content)
        print(f"Arquivo {destination_blob_name} enviado com sucesso para o bucket {bucket_name}.")

    # Função para buscar os dados da API
    def get_all_accounts():
        """Busca todas as contas paginadas da API."""
        all_accounts = []
        url = f"{API_BASE_URL}{FOLDER}"
        headers = {
            "Api-Token": API_TOKEN,
        }
        limit = 100
        offset = 0

        print(f"Tentando acessar a URL: {url}")

        while True:
            params = {
                "limit": limit,
                "offset": offset
            }

            response = requests.get(url, headers=headers, params=params)

            print(f"Status code: {response.status_code}")

            if response.status_code == 200:
                data = response.json()

                # Baseado no log, a estrutura correta é data['accounts'], não data[FOLDER]
                accounts = data.get('accounts', [])

                if not accounts:
                    print(f"Nenhum account restante para buscar.")
                    break

                all_accounts.extend(accounts)
                offset += len(accounts)
                print(f"Obtidos {len(all_accounts)} accounts até agora.")
            else:
                print(f"Erro ao buscar {FOLDER}. Código de status: {response.status_code}")
                if response.text:
                    print(f"Resposta do servidor: {response.text}")
                break

        return all_accounts

    # Função para salvar os dados no GCS
    def save_accounts_to_gcs(accounts, filename, bucket_name):
        """Salva os dados das contas em um arquivo CSV no GCS."""
        if len(accounts) == 0:
            print("Nenhum dado para salvar.")
            return

        keys = accounts[0].keys()  # Definir cabeçalhos com base nas chaves do primeiro registro

        # Criar CSV em memória (em vez de no disco)
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=keys)
        writer.writeheader()
        writer.writerows(accounts)
        csv_data = output.getvalue()

        # Fazer upload do arquivo CSV para o GCS
        upload_to_gcs(bucket_name, filename, csv_data)

    # Função principal para buscar contas e salvar no GCS
    def main():
        """Função principal para buscar contas e salvar no GCS."""
        try:
            # Verificar se o arquivo de credenciais existe
            if not os.path.exists(SERVICE_ACCOUNT_PATH):
                print(f"ERRO: Arquivo de credenciais não encontrado: {SERVICE_ACCOUNT_PATH}")
                return

            # Usar diretamente as variáveis globais
            csv_filename = f'{FOLDER}/{FOLDER}.csv'  # Salvar na pasta correspondente ao segmento

            # Buscar as contas da API
            all_accounts = get_all_accounts()

            # Salvar os dados no GCS
            if all_accounts:  # Só tenta salvar se tiver dados
                save_accounts_to_gcs(all_accounts, csv_filename, BUCKET_NAME)
                print(f"Total de {FOLDER} obtidos: {len(all_accounts)}")
                print("Dados salvos com sucesso no GCS.")
            else:
                print("Nenhum dado obtido da API para salvar.")
        except Exception as e:
            print(f"Erro no processo principal: {e}")
            import traceback
            traceback.print_exc()
            raise

    main()


def run_automations(customer):
    API_BASE_URL = customer['api_base_url']
    API_TOKEN = customer['api_token']
    BUCKET_NAME = customer['bucket_name']
    FOLDER = "automations"
    SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH

    # Faz upload do arquivo para o GCS
    def upload_to_gcs(bucket_name, destination_blob_name, content):
        """Faz upload de uma string como blob para o Google Cloud Storage (GCS)."""
        storage_client = storage.Client(project=customer['project_id'])
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_string(content)
        print(f"Arquivo {destination_blob_name} enviado com sucesso para o bucket {bucket_name}.")

    # Função para buscar os dados de automações da API
    def get_all_items():
        """Busca todas as automações paginadas da API."""
        all_items = []
        url = f"{API_BASE_URL}{FOLDER}"
        headers = {
            "Api-Token": API_TOKEN,
        }

        print(f"Tentando acessar a URL: {url}")

        # Determinar método de paginação com base no endpoint
        if FOLDER == "automations":
            # Paginação baseada em página
            page = 1
            while True:
                params = {'page': page}
                response = requests.get(url, headers=headers, params=params)

                print(f"Status code: {response.status_code}")

                if response.status_code == 200:
                    data = response.json()
                    items = data.get(FOLDER, [])

                    if not items:
                        print(f"Nenhum {FOLDER} restante para buscar.")
                        break

                    all_items.extend(items)
                    print(f"Obtidos {len(all_items)} {FOLDER} até agora.")

                    # Verifica se há próxima página
                    if 'meta' in data and data['meta'].get('next_page_url'):
                        page += 1
                    else:
                        break
                else:
                    print(f"Erro ao buscar {FOLDER}. Código de status: {response.status_code}")
                    if response.text:
                        print(f"Resposta do servidor: {response.text}")
                    break
        else:
            # Paginação baseada em offset e limit (usado para accounts e outros endpoints)
            limit = 100
            offset = 0

            while True:
                params = {
                    "limit": limit,
                    "offset": offset
                }

                response = requests.get(url, headers=headers, params=params)

                print(f"Status code: {response.status_code}")

                if response.status_code == 200:
                    data = response.json()

                    # A resposta normalmente contém um objeto com o nome do endpoint como chave
                    items = data.get(FOLDER, [])

                    if not items:
                        print(f"Nenhum {FOLDER} restante para buscar.")
                        break

                    all_items.extend(items)
                    offset += len(items)
                    print(f"Obtidos {len(all_items)} {FOLDER} até agora.")
                else:
                    print(f"Erro ao buscar {FOLDER}. Código de status: {response.status_code}")
                    if response.text:
                        print(f"Resposta do servidor: {response.text}")
                    break

        return all_items

    # Função para salvar os dados no GCS
    def save_items_to_gcs(items, filename, bucket_name):
        """Salva os dados em um arquivo CSV no GCS."""
        if len(items) == 0:
            print("Nenhum dado para salvar.")
            return

        keys = items[0].keys()  # Define os cabeçalhos com base nas chaves do primeiro registro

        # Criar CSV em memória (em vez de no disco)
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=keys)
        writer.writeheader()
        writer.writerows(items)
        csv_data = output.getvalue()

        # Fazer upload do arquivo CSV para o GCS
        upload_to_gcs(bucket_name, filename, csv_data)

    # Função principal
    def main():
        """Função principal para buscar dados e salvar no GCS."""
        try:
            # Verificar se o arquivo de credenciais existe
            if not os.path.exists(SERVICE_ACCOUNT_PATH):
                print(f"ERRO: Arquivo de credenciais não encontrado: {SERVICE_ACCOUNT_PATH}")
                return

            # Usar diretamente as variáveis globais
            csv_filename = f'{FOLDER}/{FOLDER}.csv'  # Salvar na pasta correspondente ao segmento

            # Buscar os dados da API
            all_items = get_all_items()

            # Salvar os dados no GCS
            if all_items:  # Só tenta salvar se tiver dados
                save_items_to_gcs(all_items, csv_filename, BUCKET_NAME)
                print(f"Total de {FOLDER} obtidos: {len(all_items)}")
                print("Dados salvos com sucesso no GCS.")
            else:
                print(f"Nenhum dado de {FOLDER} obtido da API para salvar.")
        except Exception as e:
            print(f"Erro no processo principal: {e}")
            import traceback
            traceback.print_exc()
            raise

    main()


def run_campaigns(customer):
    API_BASE_URL = customer['api_base_url']
    API_TOKEN = customer['api_token']
    BUCKET_NAME = customer['bucket_name']
    FOLDER = "campaings"
    SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH

    # Faz upload do arquivo para o GCS
    def upload_to_gcs(bucket_name, destination_blob_name, content):
        """Faz upload de uma string como blob para o Google Cloud Storage (GCS)."""
        storage_client = storage.Client(project=customer['project_id'])
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_string(content)
        print(f"Arquivo {destination_blob_name} enviado com sucesso para o bucket {bucket_name}.")

    # Função para buscar os dados da API
    def get_all_items():
        """Busca todas as campanhas paginadas da API."""
        all_items = []
        url = f"{API_BASE_URL}{FOLDER}"
        headers = {
            "Api-Token": API_TOKEN,
        }

        print(f"Tentando acessar a URL: {url}")

        # Para campanhas, a paginação é baseada em página
        page = 1
        while True:
            params = {'page': page}
            response = requests.get(url, headers=headers, params=params)

            print(f"Status code: {response.status_code}")

            if response.status_code == 200:
                data = response.json()
                items = data.get(FOLDER, [])

                if not items:
                    print(f"Nenhum {FOLDER} restante para buscar.")
                    break

                all_items.extend(items)
                print(f"Obtidos {len(all_items)} {FOLDER} até agora.")

                # Verifica se há próxima página
                if 'meta' in data and data['meta'].get('next_page_url'):
                    page += 1
                else:
                    break
            else:
                print(f"Erro ao buscar {FOLDER}. Código de status: {response.status_code}")
                if response.text:
                    print(f"Resposta do servidor: {response.text}")
                break

        return all_items

    # Função para salvar os dados no GCS
    def save_items_to_gcs(items, filename, bucket_name):
        """Salva os dados em um arquivo CSV no GCS."""
        if len(items) == 0:
            print("Nenhum dado para salvar.")
            return

        keys = items[0].keys()  # Define os cabeçalhos com base nas chaves do primeiro registro

        # Criar CSV em memória (em vez de no disco)
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=keys)
        writer.writeheader()
        writer.writerows(items)
        csv_data = output.getvalue()

        # Fazer upload do arquivo CSV para o GCS
        upload_to_gcs(bucket_name, filename, csv_data)

    # Função principal
    def main():
        """Função principal para buscar dados e salvar no GCS."""
        try:
            # Verificar se o arquivo de credenciais existe
            if not os.path.exists(SERVICE_ACCOUNT_PATH):
                print(f"ERRO: Arquivo de credenciais não encontrado: {SERVICE_ACCOUNT_PATH}")
                return

            # Usar diretamente as variáveis globais
            csv_filename = f'{FOLDER}/{FOLDER}.csv'  # Salvar na pasta correspondente ao segmento

            # Buscar os dados da API
            all_items = get_all_items()

            # Salvar os dados no GCS
            if all_items:  # Só tenta salvar se tiver dados
                save_items_to_gcs(all_items, csv_filename, BUCKET_NAME)
                print(f"Total de {FOLDER} obtidos: {len(all_items)}")
                print("Dados salvos com sucesso no GCS.")
            else:
                print(f"Nenhum dado de {FOLDER} obtido da API para salvar.")
        except Exception as e:
            print(f"Erro no processo principal: {e}")
            import traceback
            traceback.print_exc()
            raise

    main()


def run_contacts(customer):
    API_BASE_URL = customer['api_base_url']
    API_TOKEN = customer['api_token']
    BUCKET_NAME = customer['bucket_name']
    FOLDER = "contacts"
    SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH

    # Faz upload do arquivo para o GCS
    def upload_to_gcs(bucket_name, destination_blob_name, content):
        """Faz upload de uma string como blob para o Google Cloud Storage (GCS)."""
        storage_client = storage.Client(project=customer['project_id'])
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_string(content)
        print(f"Arquivo {destination_blob_name} enviado com sucesso para o bucket {bucket_name}.")

    # Função para buscar os dados da API
    def get_all_items():
        """Busca todos os contatos paginados da API."""
        all_items = []
        url = f"{API_BASE_URL}{FOLDER}"
        headers = {
            "Api-Token": API_TOKEN,
        }

        print(f"Tentando acessar a URL: {url}")

        # Para contatos, a paginação é baseada em limit/offset
        limit = 100
        offset = 0

        while True:
            params = {
                "limit": limit,
                "offset": offset,
                "sort": "created_timestamp ASC"  # Ordem de criação crescente
            }

            response = requests.get(url, headers=headers, params=params)

            print(f"Status code: {response.status_code}")

            if response.status_code == 200:
                data = response.json()
                items = data.get(FOLDER, [])

                if not items:
                    print(f"Nenhum {FOLDER} restante para buscar.")
                    break

                all_items.extend(items)
                offset += len(items)
                print(f"Obtidos {len(all_items)} {FOLDER} até agora.")
            else:
                print(f"Erro ao buscar {FOLDER}. Código de status: {response.status_code}")
                if response.text:
                    print(f"Resposta do servidor: {response.text}")
                break

        return all_items

    # Função para salvar os dados no GCS
    def save_items_to_gcs(items, filename, bucket_name):
        """Salva os dados em um arquivo CSV no GCS."""
        if len(items) == 0:
            print("Nenhum dado para salvar.")
            return

        keys = items[0].keys()  # Define os cabeçalhos com base nas chaves do primeiro registro

        # Criar CSV em memória (em vez de no disco)
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=keys)
        writer.writeheader()
        writer.writerows(items)
        csv_data = output.getvalue()

        # Fazer upload do arquivo CSV para o GCS
        upload_to_gcs(bucket_name, filename, csv_data)

    # Função principal
    def main():
        """Função principal para buscar dados e salvar no GCS."""
        try:
            # Verificar se o arquivo de credenciais existe
            if not os.path.exists(SERVICE_ACCOUNT_PATH):
                print(f"ERRO: Arquivo de credenciais não encontrado: {SERVICE_ACCOUNT_PATH}")
                return

            # Definir o nome do arquivo CSV e a pasta
            csv_filename = f'{FOLDER}/{FOLDER}.csv'  # Salvar na pasta correspondente ao segmento

            # Buscar os dados da API
            all_items = get_all_items()

            # Salvar os dados no GCS
            if all_items:  # Só tenta salvar se tiver dados
                save_items_to_gcs(all_items, csv_filename, BUCKET_NAME)
                print(f"Total de {FOLDER} obtidos: {len(all_items)}")
                print("Dados salvos com sucesso no GCS.")
            else:
                print(f"Nenhum dado de {FOLDER} obtido da API para salvar.")
        except Exception as e:
            print(f"Erro no processo principal: {e}")
            import traceback
            traceback.print_exc()
            raise

    # Executa o script
    main()


def run_contacts_automations(customer):
    API_BASE_URL = customer['api_base_url']
    API_TOKEN = customer['api_token']
    BUCKET_NAME = customer['bucket_name']
    FOLDER = "contacts_automations"
    SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH

    # Faz upload do arquivo para o GCS
    def upload_to_gcs(bucket_name, destination_blob_name, content):
        """Faz upload de uma string como blob para o Google Cloud Storage (GCS)."""
        storage_client = storage.Client(project=customer['project_id'])
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_string(content)
        print(f"Arquivo {destination_blob_name} enviado com sucesso para o bucket {bucket_name}.")

    # Função para buscar os dados da API
    def get_all_items():
        """Busca todos os contactAutomations paginados da API."""
        all_items = []
        url = f"{API_BASE_URL}{FOLDER}"
        headers = {
            "Api-Token": API_TOKEN,
        }

        print(f"Tentando acessar a URL: {url}")

        # Para contactAutomations, a paginação é baseada em limit/offset
        limit = 100
        offset = 0

        while True:
            params = {
                "limit": limit,
                "offset": offset
            }

            response = requests.get(url, headers=headers, params=params)

            print(f"Status code: {response.status_code}")

            if response.status_code == 200:
                data = response.json()
                items = data.get(FOLDER, [])

                if not items:
                    print(f"Nenhum {FOLDER} restante para buscar.")
                    break

                all_items.extend(items)
                offset += len(items)
                print(f"Obtidos {len(all_items)} {FOLDER} até agora.")
            else:
                print(f"Erro ao buscar {FOLDER}. Código de status: {response.status_code}")
                if response.text:
                    print(f"Resposta do servidor: {response.text}")
                break

        return all_items

    # Função para salvar os dados no GCS
    def save_items_to_gcs(items, filename, bucket_name):
        """Salva os dados em um arquivo CSV no GCS."""
        if len(items) == 0:
            print("Nenhum dado para salvar.")
            return

        keys = items[0].keys()  # Define os cabeçalhos com base nas chaves do primeiro registro

        # Criar CSV em memória (em vez de no disco)
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=keys)
        writer.writeheader()
        writer.writerows(items)
        csv_data = output.getvalue()

        # Fazer upload do arquivo CSV para o GCS
        upload_to_gcs(bucket_name, filename, csv_data)

    # Função principal
    def main():
        """Função principal para buscar dados e salvar no GCS."""
        try:
            # Verificar se o arquivo de credenciais existe
            if not os.path.exists(SERVICE_ACCOUNT_PATH):
                print(f"ERRO: Arquivo de credenciais não encontrado: {SERVICE_ACCOUNT_PATH}")
                return

            # Usar diretamente as variáveis globais
            csv_filename = f'contact-automations/contact-automations.csv'  # Mantendo o formato original do nome do arquivo

            # Buscar os dados da API
            all_items = get_all_items()

            # Salvar os dados no GCS
            if all_items:  # Só tenta salvar se tiver dados
                save_items_to_gcs(all_items, csv_filename, BUCKET_NAME)
                print(f"Total de {FOLDER} obtidos: {len(all_items)}")
                print("Dados salvos com sucesso no GCS.")
            else:
                print(f"Nenhum dado de {FOLDER} obtido da API para salvar.")
        except Exception as e:
            print(f"Erro no processo principal: {e}")
            import traceback
            traceback.print_exc()
            raise

    main()


def run_lists(customer):
    API_BASE_URL = customer['api_base_url']
    API_TOKEN = customer['api_token']
    BUCKET_NAME = customer['bucket_name']
    FOLDER = "lists"
    SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH

    # Faz upload do arquivo para o GCS
    def upload_to_gcs(bucket_name, destination_blob_name, content):
        """Faz upload de uma string como blob para o GCS."""
        try:
            storage_client = storage.Client(project=customer['project_id'])
            bucket = storage_client.bucket(BUCKET_NAME)
            blob = bucket.blob(destination_blob_name)
            blob.upload_from_string(content)
            print(f"Arquivo {destination_blob_name} enviado para {bucket_name}.")
        except GoogleAPIError as e:
            print(f"Erro ao enviar arquivo para o GCS: {e}")
            raise

    # Download de arquivo do GCS
    def download_from_gcs(bucket_name, source_blob_name):
        """Baixa um blob como string do GCS."""
        try:
            storage_client = storage.Client(project=customer['project_id'])
            bucket = storage_client.bucket(BUCKET_NAME)
            blob = bucket.blob(source_blob_name)
            if blob.exists():
                return blob.download_as_text()
            else:
                print(f"Arquivo {source_blob_name} não encontrado no bucket {bucket_name}.")
                return None
        except GoogleAPIError as e:
            print(f"Erro ao baixar arquivo do GCS: {e}")
            raise

    # Função para buscar os dados da API
    def get_all_items():
        """Busca todas as listas paginadas da API."""
        all_items = []
        url = f"{API_BASE_URL}{FOLDER}"
        headers = {
            "Api-Token": API_TOKEN,
        }

        print(f"Tentando acessar a URL: {url}")

        # Para listas, a paginação é baseada em limit/offset
        limit = 100
        offset = 0

        while True:
            params = {
                "limit": limit,
                "offset": offset
            }

            try:
                response = requests.get(url, headers=headers, params=params)
                response.raise_for_status()  # Lança exceção se houver erro HTTP

                print(f"Status code: {response.status_code}")

                data = response.json()
                items = data.get(FOLDER, [])

                if not items:
                    print(f"Nenhum {FOLDER} restante para buscar.")
                    break

                all_items.extend(items)
                offset += len(items)
                print(f"Obtidos {len(all_items)} {FOLDER} até agora.")
            except requests.RequestException as e:
                print(f"Erro na requisição da API: {e}")
                break

        return all_items

    # Função para salvar os dados no GCS
    def save_items_to_gcs(items, csv_file, bucket_name):
        """Salva os dados em um arquivo CSV no GCS, opcionalmente concatenando com dados existentes."""
        if not items:
            print("Nenhum dado para salvar.")
            return False

        df = pd.DataFrame(items)

        # Adicionando o caminho para a pasta específica
        csv_file_path = f"{FOLDER}/{csv_file}"

        # Verificar se já existe um arquivo e fazer download
        csv_content = download_from_gcs(bucket_name, csv_file_path)

        if csv_content:
            try:
                # Verificar se o conteúdo não está vazio
                if csv_content.strip():
                    # Se existir, concatenar com os dados existentes
                    existing_df = pd.read_csv(io.StringIO(csv_content))
                    df = pd.concat([existing_df, df])
                    print("Concatenando com dados existentes do arquivo.")
                else:
                    print("Arquivo existente está vazio. Usando apenas os novos dados.")
            except pd.errors.EmptyDataError:
                print("Arquivo existente está vazio ou mal formatado. Usando apenas os novos dados.")
            except Exception as e:
                print(f"Erro ao processar arquivo existente: {e}. Usando apenas os novos dados.")
        else:
            print("Arquivo não encontrado. Criando um novo.")

        # Salvar o novo CSV no GCS
        csv_data = df.to_csv(index=False)
        upload_to_gcs(bucket_name, csv_file_path, csv_data)
        return True

    # Função principal
    def main():
        """Função principal para buscar dados e salvar no GCS."""
        try:
            # Verificar se o arquivo de credenciais existe
            if not os.path.exists(SERVICE_ACCOUNT_PATH):
                print(f"ERRO: Arquivo de credenciais não encontrado: {SERVICE_ACCOUNT_PATH}")
                return

            # Usar diretamente as variáveis globais
            csv_filename = 'lists.csv'  # Nome do arquivo CSV

            # Buscar os dados da API
            all_items = get_all_items()

            # Salvar os dados no GCS
            if all_items:  # Só tenta salvar se tiver dados
                if save_items_to_gcs(all_items, csv_filename, BUCKET_NAME):
                    print(f"Total de {FOLDER} obtidos: {len(all_items)}")
                    print("Processo concluído e dados salvos no GCS.")
                else:
                    print("Erro ao salvar os dados no GCS.")
            else:
                print(f"Nenhum dado de {FOLDER} obtido da API para salvar.")
        except Exception as e:
            print(f"Erro no processo principal: {e}")
            import traceback
            traceback.print_exc()
            raise

    # Executa o script
    main()


def run_messages(customer):
    API_BASE_URL = customer['api_base_url']
    API_TOKEN = customer['api_token']
    BUCKET_NAME = customer['bucket_name']
    FOLDER = "messagess"
    SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH

    # Faz upload do arquivo para o GCS
    def upload_to_gcs(bucket_name, destination_blob_name, content):
        """Faz upload de uma string JSON como blob para o Google Cloud Storage (GCS)."""
        storage_client = storage.Client(project=customer['project_id'])
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_string(content, content_type='application/json')
        print(f"Arquivo {destination_blob_name} enviado com sucesso para o bucket {bucket_name}.")

    # Função para buscar os dados da API
    def get_all_items():
        """Busca todas as mensagens paginadas da API."""
        all_items = []
        url = f"{API_BASE_URL}{FOLDER}"
        headers = {
            "Api-Token": API_TOKEN,
        }

        print(f"Tentando acessar a URL: {url}")

        # Para mensagens, a paginação é baseada em limit/offset
        limit = 100
        offset = 0

        # Parâmetro de ordenação
        sort_by = "created_timestamp"
        order = "ASC"

        while True:
            params = {
                "limit": limit,
                "offset": offset,
                "sort": f"{sort_by} {order}"
            }

            response = requests.get(url, headers=headers, params=params)

            print(f"Status code: {response.status_code}")

            if response.status_code == 200:
                data = response.json()
                items = data.get(FOLDER, [])

                if not items:
                    print(f"Nenhum {FOLDER} restante para buscar.")
                    break

                all_items.extend(items)
                offset += len(items)
                print(f"Obtidos {len(all_items)} {FOLDER} até agora.")
            else:
                print(f"Erro ao buscar {FOLDER}. Código de status: {response.status_code}")
                if response.text:
                    print(f"Resposta do servidor: {response.text}")
                break

        return all_items

    # Função para salvar os dados no GCS
    def save_items_to_gcs(items, filename, bucket_name):
        """Salva os dados em um arquivo JSON no GCS."""
        if len(items) == 0:
            print("Nenhum dado para salvar.")
            return

        # Converte os dados para JSON com formatação
        json_data = json.dumps(items, indent=4)

        # Fazer upload do arquivo JSON para o GCS
        upload_to_gcs(bucket_name, filename, json_data)

    # Função principal
    def main():
        """Função principal para buscar dados e salvar no GCS."""
        try:
            # Verificar se o arquivo de credenciais existe
            if not os.path.exists(SERVICE_ACCOUNT_PATH):
                print(f"ERRO: Arquivo de credenciais não encontrado: {SERVICE_ACCOUNT_PATH}")
                return

            # Definir o nome do arquivo JSON e a pasta
            json_filename = f'{FOLDER}/{FOLDER}.json'  # Salvar na pasta correspondente ao segmento

            # Buscar os dados da API
            all_items = get_all_items()

            # Salvar os dados no GCS
            if all_items:  # Só tenta salvar se tiver dados
                save_items_to_gcs(all_items, json_filename, BUCKET_NAME)
                print(f"Total de {FOLDER} obtidos: {len(all_items)}")
                print("Dados salvos com sucesso no GCS.")
            else:
                print(f"Nenhum dado de {FOLDER} obtido da API para salvar.")
        except Exception as e:
            print(f"Erro no processo principal: {e}")
            import traceback
            traceback.print_exc()
            raise

    # Executa o script
    main()


def get_extraction_tasks():
    return [
        {
            'task_id': 'extract_accounts',
            'python_callable': run_accounts
        },
        {
            'task_id': 'extract_automations',
            'python_callable': run_automations
        },
        {
            'task_id': 'extract_campaigns',
            'python_callable': run_campaigns
        },
        {
            'task_id': 'extract_contacts',
            'python_callable': run_contacts
        },
        {
            'task_id': 'extract_contacts_automations',
            'python_callable': run_contacts_automations
        },
        {
            'task_id': 'extract_lists',
            'python_callable': run_lists
        },
        {
            'task_id': 'extract_messages',
            'python_callable': run_messages
        }
    ]
