"""
Kommo module for data extraction functions.
This module contains functions specific to the Kommo integration.
"""

import logging
import re
import unicodedata
from typing import Any, Dict

import pandas as pd
from core import gcs

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


class Utils:
    @staticmethod
    def _flatten_json(data: Any, parent_key: str = '', sep: str = '_') -> Dict[str, Any]:
        """
        Achata estruturas JSON aninhadas em um dicionário de nível único.
        """
        try:
            items = []

            if isinstance(data, dict):
                for key, value in data.items():
                    new_key = f"{parent_key}{sep}{key}" if parent_key else key
                    items.extend(Utils._flatten_json(value, new_key, sep).items())
            elif isinstance(data, list):
                for index, item in enumerate(data):
                    new_key = f"{parent_key}{sep}{index + 1}"
                    items.extend(Utils._flatten_json(item, new_key, sep).items())
            else:
                items.append((parent_key, data))

            return dict(items)

        except Exception as e:
            logging.error(f"Erro ao achatar JSON: {str(e)}")
            raise

    @staticmethod
    def _normalize_key(key: str) -> str:
        """
        Normaliza uma chave de string aplicando transformações para padronização.
        """
        key = key.strip('\'"')

        # Detecta se é um verdadeiro camelCase (inicia com minúscula, depois tem maiúscula)
        is_true_camel_case = bool(re.match(r'^[a-z].*[A-Z]', key))

        # Se for camelCase, aplica a conversão para snake_case antes
        if is_true_camel_case:
            key = re.sub(r'([a-z])([A-Z])', r'\1_\2', key)

        # Converte para lowercase
        key = key.lower()

        # Remove acentos e outros caracteres Unicode
        key = unicodedata.normalize('NFKD', key).encode('ASCII', 'ignore').decode('ASCII')

        # Substitui caracteres especiais e espaços por underscores
        key = re.sub(r'[^a-z0-9]+', '_', key)

        # Remove underscores extras no início ou fim
        key = key.strip('_')

        return key

    @staticmethod
    def _normalize_keys(data: Any) -> Any:
        """
        Normaliza todas as chaves em estruturas aninhadas (dict/list).
        """
        try:
            if isinstance(data, dict):
                # Normaliza as chaves do dicionário
                return {Utils._normalize_key(k): Utils._normalize_keys(v) for k, v in data.items()}
            elif isinstance(data, list):
                # Normaliza cada item na lista
                return [Utils._normalize_keys(item) for item in data]
            else:
                # Retorna o valor original se não for um dicionário ou lista
                return data

        except Exception as e:
            logging.error(f"Erro ao normalizar chaves: {str(e)}")
            raise

    @staticmethod
    def _normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
        """
        Normaliza os nomes das colunas de um DataFrame.
        """
        try:
            if df.empty:
                return df

            # Normalização dos nomes das colunas
            df.columns = [Utils._normalize_key(col) for col in df.columns]
            return df

        except Exception as e:
            logging.error(f"Erro ao normalizar nomes de colunas: {str(e)}")
            raise

    @staticmethod
    def _remove_empty_columns(df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove colunas vazias (NaN ou strings vazias) de um DataFrame.
        """
        try:
            if df.empty:
                logging.warning("DataFrame vazio recebido para remoção de colunas")
                return df

            # Número inicial de colunas
            initial_columns = df.shape[1]

            # Remover colunas onde todos os valores são NaN
            df_no_nan = df.dropna(axis=1, how='all')
            nan_columns_removed = initial_columns - df_no_nan.shape[1]

            if nan_columns_removed > 0:
                logging.info(f"Colunas removidas por serem NaN: {nan_columns_removed}")

            # Identificar e remover colunas onde todos os valores são strings vazias
            empty_columns = [
                col for col in df_no_nan.columns
                if df_no_nan[col].astype(str).str.strip().eq('').all()
            ]

            if empty_columns:
                df_cleaned = df_no_nan.drop(columns=empty_columns)
                logging.info(f"Colunas removidas por serem strings vazias: {len(empty_columns)}")
            else:
                df_cleaned = df_no_nan

            # Total de colunas removidas
            total_columns_removed = nan_columns_removed + len(empty_columns)

            if total_columns_removed > 0:
                logging.info(f"Total de colunas removidas: {total_columns_removed}")

            # Verificar se o DataFrame resultante está vazio
            if df_cleaned.empty and not df.empty:
                raise ValueError("O DataFrame resultante está vazio após a remoção de colunas")

            return df_cleaned

        except Exception as e:
            logging.error(f"Erro ao remover colunas vazias: {str(e)}")
            raise

    @staticmethod
    def _convert_columns_to_nullable_int(df: pd.DataFrame) -> pd.DataFrame:
        """
        Converte colunas do DataFrame para Int64 somente se:
        - Todos os valores não nulos forem inteiros "puros" (sem ponto decimal, ex: 7 e não 7.0)
        - E não houver valores float com casas decimais

        Mantém colunas como float se houver valores como 7.0 explicitamente.
        """
        try:
            converted_cols = []
            for col in df.columns:
                if pd.api.types.is_numeric_dtype(df[col]):
                    non_null_series = df[col].dropna()

                    # Se todos os valores forem inteiros puros (ex: 7, não 7.0)
                    if non_null_series.apply(
                            lambda x: isinstance(x, int) or (isinstance(x, float) and x.is_integer())).all():
                        # Se a coluna NÃO tiver valores como 7.0 explicitamente
                        if not non_null_series.apply(lambda x: isinstance(x, float) and not x.is_integer()).any():
                            df[col] = df[col].astype("Int64")
                            converted_cols.append(col)

            if converted_cols:
                preview = ', '.join(converted_cols[:5])
                suffix = "..." if len(converted_cols) > 5 else ""
                logging.info(f"Colunas convertidas para Int64: {preview}{suffix}")
            return df

        except Exception as e:
            logging.error(f"Erro ao converter colunas numericas para Int64: {str(e)}")
            raise


def run_leads(customer):
    try:
        import requests
        import csv
        from datetime import datetime, timezone

        # Token de longa duração
        access_token = customer['token']

        # Subdomínio do Kommo
        subdomain = customer['subdomain']

        # URL base da API
        base_url = f'https://{subdomain}.kommo.com'

        # Nome do bucket e caminho do arquivo no GCS
        bucket_name = customer['bucket_name']
        gcs_file_path = 'leads/leads.csv'

        # Função para desaninhamento dos campos personalizados
        def flatten_custom_fields(custom_fields):
            flattened_fields = {}
            if custom_fields:
                for field in custom_fields:
                    field_name = field['field_name'].rstrip(':')  # Remove os dois pontos do final do nome do campo
                    values = field['values']
                    flattened_fields[field_name] = '; '.join([str(value['value']) for value in values])
            return flattened_fields

        # Função para converter timestamps Unix para datas legíveis com reconhecimento de fuso horário
        def convert_timestamp_to_date(timestamp):
            if isinstance(timestamp, int) or (isinstance(timestamp, str) and timestamp.isdigit()):
                return datetime.fromtimestamp(int(timestamp), timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            return timestamp

        # Função para desaninhamento dos embeddeds
        def flatten_embedded_data(lead):
            embedded_data = {}

            # Desaninhamento de contatos
            if '_embedded' in lead and 'contacts' in lead['_embedded']:
                contacts = lead['_embedded']['contacts']
                embedded_data['contacts'] = '; '.join([str(contact['id']) for contact in contacts])

            # Desaninhamento de empresas
            if '_embedded' in lead and 'companies' in lead['_embedded']:
                companies = lead['_embedded']['companies']
                embedded_data['companies'] = '; '.join([str(company['id']) for company in companies])

            # Desaninhamento de tags
            if '_embedded' in lead and 'tags' in lead['_embedded']:
                tags = lead['_embedded']['tags']
                embedded_data['tags'] = '; '.join([str(tag['name']) for tag in tags])

            # Desaninhamento de loss_reason
            if '_embedded' in lead and 'loss_reason' in lead['_embedded']:
                loss_reasons = lead['_embedded']['loss_reason']
                embedded_data['loss_reason'] = '; '.join([str(loss_reason['name']) for loss_reason in loss_reasons])

            return embedded_data

        # Função para fazer a coleta de leads
        def get_leads():
            headers = {
                'Authorization': f'Bearer {access_token}'
            }
            leads = []
            offset = 0
            while True:
                params = {
                    'limit': 250,
                    'page': offset // 250 + 1,
                    'with': 'contacts,companies,tags,loss_reason,catalog_elements'
                }
                response = requests.get(f'{base_url}/api/v4/leads', headers=headers, params=params)
                print(f"Solicitando leads, página: {offset // 250 + 1}")
                if response.status_code == 204:  # Nenhum conteúdo
                    print("Nenhum lead adicional foi encontrado.")
                    break
                elif response.status_code == 401:  # Erro de autenticação
                    error_msg = "Erro de autenticação. Verifique se o token de longa duração é válido."
                    print(error_msg)
                    raise Exception(error_msg)
                elif response.status_code != 200:
                    error_msg = f"Erro ao coletar leads: {response.status_code}"
                    print(error_msg)
                    print(response.json() if response.text else "Sem mensagem de erro")
                    raise Exception(error_msg)

                try:
                    data = response.json()['_embedded']['leads']
                except (KeyError, ValueError) as e:
                    error_msg = f"Erro na estrutura de resposta da API: {str(e)}"
                    print(error_msg)
                    raise Exception(error_msg)

                for lead in data:
                    # Converter campos de data
                    for date_field in ['closest_task_at', 'created_at', 'closed_at', 'updated_at']:
                        if date_field in lead:
                            lead[date_field] = convert_timestamp_to_date(lead[date_field])

                    # Converter campos de data nos campos personalizados, se existirem
                    if 'custom_fields_values' in lead:
                        custom_fields_flat = flatten_custom_fields(lead['custom_fields_values'])
                        lead.update(custom_fields_flat)
                        del lead['custom_fields_values']  # Remove o campo aninhado original

                    # Desaninhamento dos campos embeddeds
                    embedded_data = flatten_embedded_data(lead)
                    lead.update(embedded_data)

                leads.extend(data)
                print(f"Coletados {len(leads)} leads até agora...")

                if len(data) < 250:  # Se o número de leads retornados for menor que o limite, não há mais dados
                    break

                offset += 250

            if not leads:
                print("Aviso: Nenhum lead foi coletado.")
                # Se quiser tratar como erro:
                # raise Exception("Nenhum lead foi coletado.")

            return leads

        # Função para salvar os leads em um arquivo CSV e fazer upload para o GCS
        def save_leads_to_gcs(leads):
            if not leads:
                error_msg = "Nenhum lead para processar."
                print(error_msg)
                raise Exception(error_msg)

            # Obtenha todos os cabeçalhos únicos
            all_keys = set()
            for lead in leads:
                all_keys.update(lead.keys())

            # Salva o arquivo CSV localmente
            local_file_path = f"/tmp/{customer['project_id']}_leads.csv"
            with open(local_file_path, 'w', newline='', encoding='utf-8') as output_file:
                dict_writer = csv.DictWriter(output_file, fieldnames=list(all_keys), delimiter=';')
                dict_writer.writeheader()
                dict_writer.writerows(leads)

            credentials = gcs.load_credentials_from_env()
            result = gcs.write_file_to_gcs(
                bucket_name=bucket_name,
                local_file_path=local_file_path,
                destination_name=gcs_file_path,
                credentials=credentials
            )

            # Verificar se o upload foi bem-sucedido
            if not result:
                error_msg = f"Falha ao fazer upload do arquivo para GCS: gs://{bucket_name}/{gcs_file_path}"
                print(error_msg)
                raise Exception(error_msg)

            print(f"Arquivo CSV salvo no GCS: gs://{bucket_name}/{gcs_file_path}")

        # Execução principal
        print("Iniciando a coleta de leads...")
        leads = get_leads()
        save_leads_to_gcs(leads)
        print(f"Coleta concluída. Um total de {len(leads)} leads foi salvo em leads.csv no GCS.")

    except Exception as e:
        print(f"FALHA CRÍTICA em run_leads: {str(e)}")
        # Re-lança a exceção para o Airflow
        raise


def run_unsorted_leads(customer):
    try:
        import requests
        import csv
        from datetime import datetime, timezone

        # Token de longa duração
        access_token = customer['token']

        # Subdomínio do Kommo
        subdomain = customer['subdomain']

        # URL base da API
        base_url = f'https://{subdomain}.kommo.com'

        # Nome do bucket e caminho do arquivo no GCS
        bucket_name = customer['bucket_name']
        gcs_file_path = 'leads/unsorted_leads.csv'

        # Caminho do arquivo local
        local_file_path = f"/tmp/{customer['project_id']}_unsorted_leads.csv"

        # Função para converter timestamps Unix para datas legíveis
        def convert_timestamp_to_date(timestamp):
            if isinstance(timestamp, int) or (isinstance(timestamp, str) and timestamp.isdigit()):
                return datetime.fromtimestamp(int(timestamp), timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            return timestamp

        # Função para fazer a coleta de leads não classificados
        def get_unsorted_leads():
            headers = {
                'Authorization': f'Bearer {access_token}'
            }
            unsorted_leads = []
            offset = 0
            while True:
                params = {
                    'limit': 250,
                    'page': offset // 250 + 1,
                    'with': 'contacts,companies,tags,metadata'
                }

                try:
                    response = requests.get(f'{base_url}/api/v4/leads/unsorted', headers=headers, params=params)
                    print(f"Solicitando leads não classificados, página: {offset // 250 + 1}")

                    if response.status_code == 204:  # Nenhum conteúdo
                        print("Nenhum lead não classificado adicional foi encontrado.")
                        break
                    elif response.status_code == 401:  # Erro de autenticação
                        error_msg = "Erro de autenticação. Verifique se o token de longa duração é válido."
                        print(error_msg)
                        raise Exception(error_msg)
                    elif response.status_code != 200:
                        error_msg = f"Erro ao coletar leads não classificados: {response.status_code}"
                        print(error_msg)
                        print(response.json() if response.text else "Sem mensagem de erro")
                        raise Exception(error_msg)

                    data = response.json()
                    if '_embedded' not in data or 'unsorted' not in data['_embedded']:
                        error_msg = "Estrutura de resposta inesperada - não contém leads não classificados."
                        print(error_msg)
                        raise Exception(error_msg)

                    # Obtém a lista de leads não classificados
                    leads_data = data['_embedded']['unsorted']

                    # Normaliza as chaves dos dados JSON recebidos
                    normalized_leads = [Utils._normalize_keys(lead) for lead in leads_data]

                    # Para cada lead normalizado, converte os campos de data
                    for lead in normalized_leads:
                        # Converter campos de data
                        for date_field in ['created_at', 'updated_at']:
                            if date_field in lead:
                                lead[date_field] = convert_timestamp_to_date(lead[date_field])

                    # Achata cada lead normalizado
                    flattened_leads = [Utils._flatten_json(lead) for lead in normalized_leads]

                    unsorted_leads.extend(flattened_leads)
                    print(f"Coletados {len(unsorted_leads)} leads não classificados até agora...")

                    if len(leads_data) < 250:  # Se o número de leads retornados for menor que o limite, não há mais dados
                        break

                    offset += 250

                except Exception as e:
                    error_msg = f"Erro ao processar a requisição: {str(e)}"
                    print(error_msg)
                    raise Exception(error_msg)

            if not unsorted_leads:
                print("Aviso: Nenhum lead não classificado foi coletado.")
                # Se quiser tratar como erro:
                # raise Exception("Nenhum lead não classificado foi coletado.")

            return unsorted_leads

        # Função para processar e salvar os leads não classificados
        def save_unsorted_leads_to_gcs(unsorted_leads):
            if not unsorted_leads:
                error_msg = "Nenhum lead não classificado para processar."
                print(error_msg)
                raise Exception(error_msg)

            try:
                # Converter para DataFrame
                df = pd.DataFrame(unsorted_leads)

                if df.empty:
                    error_msg = "DataFrame vazio após conversão dos leads."
                    print(error_msg)
                    raise Exception(error_msg)

                # Aplicar normalização de nomes de colunas
                df = Utils._normalize_column_names(df)

                # Remover colunas vazias
                df = Utils._remove_empty_columns(df)

                # Converter colunas para Int64 quando apropriado
                df = Utils._convert_columns_to_nullable_int(df)

                # Obtenha todos os cabeçalhos únicos
                all_columns = list(df.columns)

                # Salva o arquivo CSV localmente
                with open(local_file_path, 'w', newline='', encoding='utf-8') as output_file:
                    dict_writer = csv.DictWriter(output_file, fieldnames=all_columns, delimiter=';')
                    dict_writer.writeheader()
                    dict_writer.writerows(df.to_dict('records'))

                # Upload para o GCS
                credentials = gcs.load_credentials_from_env()
                result = gcs.write_file_to_gcs(
                    bucket_name=bucket_name,
                    local_file_path=local_file_path,
                    destination_name=gcs_file_path,
                    credentials=credentials
                )

                # Verificar se o upload foi bem-sucedido
                if not result:
                    error_msg = f"Falha ao fazer upload do arquivo para GCS: gs://{bucket_name}/{gcs_file_path}"
                    print(error_msg)
                    raise Exception(error_msg)

                print(f"Arquivo CSV salvo no GCS: gs://{bucket_name}/{gcs_file_path}")
                print(f"DataFrame processado com {len(df)} linhas e {len(df.columns)} colunas")

            except Exception as e:
                error_msg = f"Erro ao processar e salvar os leads não classificados: {str(e)}"
                print(error_msg)
                raise Exception(error_msg)

        # Execução principal
        print("Iniciando a coleta de leads não classificados com nova normalização...")
        unsorted_leads = get_unsorted_leads()
        save_unsorted_leads_to_gcs(unsorted_leads)
        print(
            f"Coleta concluída. Um total de {len(unsorted_leads)} leads não classificados foi processado e salvo em unsorted_leads.csv no GCS.")

    except Exception as e:
        print(f"FALHA CRÍTICA em run_unsorted_leads: {str(e)}")
        # Re-lança a exceção para o Airflow
        raise


def run_pipelines(customer):
    try:
        import requests
        import csv

        # Token de acesso de longa duração
        access_token = customer['token']

        # Subdomínio do Kommo
        subdomain = customer['subdomain']

        # Bucket
        bucket_name = customer['bucket_name']

        # URL base da API
        base_url = f'https://{subdomain}.kommo.com'

        # Função para fazer a requisição dos pipelines
        def get_pipelines():
            headers = {
                'Authorization': f'Bearer {access_token}'
            }
            response = requests.get(f'{base_url}/api/v4/leads/pipelines', headers=headers)

            if response.status_code == 200:
                try:
                    return response.json()["_embedded"]["pipelines"]
                except (KeyError, ValueError) as e:
                    error_msg = f"Erro na estrutura de resposta da API: {str(e)}"
                    print(error_msg)
                    raise Exception(error_msg)
            elif response.status_code == 401:
                error_msg = "Erro de autenticação. Verifique se o token de longa duração é válido."
                print(error_msg)
                raise Exception(error_msg)
            else:
                error_msg = f"Erro ao obter os pipelines: {response.status_code}"
                print(error_msg)
                print(response.json() if response.text else "Sem mensagem de erro")
                raise Exception(error_msg)

        # Função para salvar os pipelines em um arquivo CSV
        def save_pipelines_to_csv(pipelines_data):
            if not pipelines_data:
                error_msg = "Nenhum pipeline encontrado ou erro na requisição."
                print(error_msg)
                raise Exception(error_msg)

            local_file_path = f"/tmp/{customer['project_id']}_pipelines.csv"
            with open(local_file_path, mode="w", newline='', encoding='utf-8') as file:
                writer = csv.writer(file, delimiter=';')

                # Cabeçalho do CSV
                writer.writerow(["Pipeline ID", "Pipeline Name", "Status ID", "Status Name"])

                # Escrevendo os dados dos pipelines
                try:
                    for pipeline in pipelines_data:
                        pipeline_id = pipeline["id"]
                        pipeline_name = pipeline["name"]
                        if "_embedded" not in pipeline or "statuses" not in pipeline["_embedded"]:
                            error_msg = f"Estrutura de pipeline inesperada para pipeline ID {pipeline_id}"
                            print(error_msg)
                            raise Exception(error_msg)

                        for status in pipeline["_embedded"]["statuses"]:
                            status_id = status["id"]
                            status_name = status["name"]
                            writer.writerow([pipeline_id, pipeline_name, status_id, status_name])
                except Exception as e:
                    error_msg = f"Erro ao processar dados dos pipelines: {str(e)}"
                    print(error_msg)
                    raise Exception(error_msg)

            print("Arquivo pipelines.csv gerado com sucesso.")
            return local_file_path

        # Função para fazer upload do arquivo CSV para o Google Cloud Storage
        def upload_to_gcs(local_file_path):
            try:
                credentials = gcs.load_credentials_from_env()
                result = gcs.write_file_to_gcs(
                    bucket_name=bucket_name,
                    local_file_path=local_file_path,
                    destination_name="pipelines/pipelines.csv",
                    credentials=credentials
                )

                # Verificar se o upload foi bem-sucedido
                if not result:
                    error_msg = f"Falha ao fazer upload do arquivo para GCS: gs://{bucket_name}/pipelines/pipelines.csv"
                    print(error_msg)
                    raise Exception(error_msg)

                print(f"Arquivo pipelines.csv enviado para o bucket {bucket_name} com sucesso.")
            except Exception as e:
                error_msg = f"Erro ao fazer upload para GCS: {str(e)}"
                print(error_msg)
                raise Exception(error_msg)

        # Execução principal
        print("Iniciando a execução do script...")
        pipelines_data = get_pipelines()
        local_file_path = save_pipelines_to_csv(pipelines_data)
        upload_to_gcs(local_file_path)
        print("Processamento de pipelines concluído com sucesso.")

    except Exception as e:
        print(f"FALHA CRÍTICA em run_pipelines: {str(e)}")
        # Re-lança a exceção para o Airflow
        raise


def run_users(customer):
    try:
        import requests
        import csv

        # Token de acesso de longa duração
        access_token = customer['token']

        # Subdomínio do Kommo
        subdomain = customer['subdomain']

        # Bucket
        bucket_name = customer['bucket_name']

        # URL base da API
        base_url = f'https://{subdomain}.kommo.com'

        # Função para coletar dados da lista de usuários
        def get_users_list():
            url = f"{base_url}/api/v4/users"
            headers = {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/hal+json",
            }

            response = requests.get(url, headers=headers)

            if response.status_code == 200:
                try:
                    data = response.json()
                    if "_embedded" not in data or "users" not in data["_embedded"]:
                        error_msg = "Estrutura de resposta inesperada - não contém usuários."
                        print(error_msg)
                        raise Exception(error_msg)
                    return data["_embedded"]["users"]
                except (KeyError, ValueError) as e:
                    error_msg = f"Erro na estrutura de resposta da API: {str(e)}"
                    print(error_msg)
                    raise Exception(error_msg)
            elif response.status_code == 401:
                error_msg = "Erro de autenticação. Verifique se o token de longa duração é válido."
                print(error_msg)
                raise Exception(error_msg)
            else:
                error_msg = f"Erro ao coletar a lista de usuários: {response.status_code}"
                print(error_msg)
                print(response.json() if response.text else "Sem mensagem de erro")
                raise Exception(error_msg)

        # Função para salvar dados no CSV
        def save_to_csv(users):
            if not users:
                error_msg = "Nenhum dado de usuário para salvar."
                print(error_msg)
                raise Exception(error_msg)

            local_file_path = f"/tmp/{customer['project_id']}_users_list.csv"
            try:
                with open(local_file_path, mode="w", newline='', encoding="utf-8") as file:
                    writer = csv.writer(file, delimiter=';')
                    writer.writerow([
                        "ID", "Name", "Email", "Language", "Is Admin",
                        "Is Free", "Is Active", "Group ID", "Role ID"
                    ])

                    for user in users:
                        if "id" not in user or "name" not in user or "email" not in user or "lang" not in user or "rights" not in user:
                            error_msg = f"Estrutura de usuário inesperada para usuário: {user.get('id', 'ID desconhecido')}"
                            print(error_msg)
                            raise Exception(error_msg)

                        writer.writerow([
                            user["id"],
                            user["name"],
                            user["email"],
                            user["lang"],
                            user["rights"]["is_admin"],
                            user["rights"]["is_free"],
                            user["rights"]["is_active"],
                            user["rights"].get("group_id", "N/A"),
                            user["rights"].get("role_id", "N/A")
                        ])

                print("Arquivo users_list.csv gerado com sucesso.")
                return local_file_path
            except Exception as e:
                error_msg = f"Erro ao salvar dados no CSV: {str(e)}"
                print(error_msg)
                raise Exception(error_msg)

        # Função para fazer upload do arquivo CSV para o Google Cloud Storage
        def upload_to_gcs(local_file_path):
            try:
                credentials = gcs.load_credentials_from_env()
                result = gcs.write_file_to_gcs(
                    bucket_name=bucket_name,
                    local_file_path=local_file_path,
                    destination_name="users/users_list.csv",
                    credentials=credentials
                )

                # Verificar se o upload foi bem-sucedido
                if not result:
                    error_msg = f"Falha ao fazer upload do arquivo para GCS: gs://{bucket_name}/users/users_list.csv"
                    print(error_msg)
                    raise Exception(error_msg)

                print(f"Arquivo users_list.csv enviado para o bucket {bucket_name} com sucesso.")
            except Exception as e:
                error_msg = f"Erro ao fazer upload para GCS: {str(e)}"
                print(error_msg)
                raise Exception(error_msg)

        # Execução principal
        print("Coletando lista de usuários...")
        users = get_users_list()
        local_file_path = save_to_csv(users)
        upload_to_gcs(local_file_path)
        print("Dados de usuários salvos com sucesso e enviados para o Google Cloud Storage.")

    except Exception as e:
        print(f"FALHA CRÍTICA em run_users: {str(e)}")
        # Re-lança a exceção para o Airflow
        raise


def get_extraction_tasks():
    """
    Get the list of data extraction tasks for Kommo.

    Returns:
        list: List of task configurations
    """
    return [
        {
            'task_id': 'run_leads',
            'python_callable': run_leads
        },
        {
            'task_id': 'run_unsorted_leads',
            'python_callable': run_unsorted_leads
        },
        {
            'task_id': 'run_pipelines',
            'python_callable': run_pipelines
        },
        {
            'task_id': 'run_users',
            'python_callable': run_users
        }
    ]
