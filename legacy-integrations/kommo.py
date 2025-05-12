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

    @staticmethod
    def upload_to_gcs(bucket_name, local_file_path, destination_path):
        """
        Função unificada para upload de arquivos para o GCS.

        Args:
            bucket_name (str): Nome do bucket no GCS
            local_file_path (str): Caminho local do arquivo
            destination_path (str): Caminho de destino no GCS

        Returns:
            bool: True se o upload foi bem-sucedido

        Raises:
            Exception: Se ocorrer um erro durante o upload
        """
        try:
            logging.info(f"Tentando fazer upload de {local_file_path} para gs://{bucket_name}/{destination_path}")

            credentials = gcs.load_credentials_from_env()
            result = gcs.write_file_to_gcs(
                bucket_name=bucket_name,
                local_file_path=local_file_path,
                destination_name=destination_path,
                credentials=credentials
            )

            # Verificar se o resultado é None ou False
            # if result is None or result is False:
            #     error_msg = f"Falha ao fazer upload do arquivo para GCS: gs://{bucket_name}/{destination_path}"
            #     logging.error(error_msg)
            #     raise Exception(error_msg)

            logging.info(f"Arquivo salvo com sucesso no GCS: gs://{bucket_name}/{destination_path}")
            return True

        except Exception as e:
            error_msg = f"Erro durante upload para GCS: {str(e)}"
            logging.error(error_msg)
            raise Exception(error_msg)


def run_leads(customer):
    """
    Coleta leads do Kommo e salva no GCS.

    Args:
        customer (dict): Informações do cliente

    Raises:
        Exception: Se ocorrer um erro durante o processo
    """
    try:
        import requests
        import csv
        from datetime import datetime, timezone

        # Configurações
        access_token = customer['token']
        subdomain = customer['subdomain']
        base_url = f'https://{subdomain}.kommo.com'
        bucket_name = customer['bucket_name']
        gcs_file_path = 'leads/leads.csv'
        local_file_path = f"/tmp/{customer['project_id']}_leads.csv"

        # Função para desaninhamento dos campos personalizados
        def flatten_custom_fields(custom_fields):
            flattened_fields = {}
            if custom_fields:
                for field in custom_fields:
                    field_name = field['field_name'].rstrip(':')
                    values = field['values']
                    flattened_fields[field_name] = '; '.join([str(value['value']) for value in values])
            return flattened_fields

        # Função para converter timestamps Unix para datas legíveis
        def convert_timestamp_to_date(timestamp):
            if isinstance(timestamp, int) or (isinstance(timestamp, str) and timestamp.isdigit()):
                return datetime.fromtimestamp(int(timestamp), timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            return timestamp

        # Função para desaninhamento dos embeddeds
        def flatten_embedded_data(lead):
            embedded_data = {}

            # Desaninhamento de contatos, empresas, tags e loss_reason
            if '_embedded' in lead:
                for key, field_name in [
                    ('contacts', 'contacts'),
                    ('companies', 'companies'),
                    ('tags', 'tags'),
                    ('loss_reason', 'loss_reason')
                ]:
                    if key in lead['_embedded']:
                        items = lead['_embedded'][key]
                        id_or_name = 'name' if key in ['tags', 'loss_reason'] else 'id'
                        embedded_data[field_name] = '; '.join([str(item[id_or_name]) for item in items])

            return embedded_data

        # Função para fazer a coleta de leads
        def get_leads():
            headers = {
                'Authorization': f'Bearer {access_token}'
            }
            leads = []
            page = 1

            logging.info("Iniciando a coleta de leads...")

            while True:
                params = {
                    'limit': 250,
                    'page': page,
                    'with': 'contacts,companies,tags,loss_reason,catalog_elements'
                }

                logging.info(f"Solicitando leads, página: {page}")
                response = requests.get(f'{base_url}/api/v4/leads', headers=headers, params=params)

                if response.status_code == 204:
                    logging.info("Nenhum lead adicional foi encontrado.")
                    break
                elif response.status_code == 401:
                    error_msg = "Erro de autenticação. Verifique o token de longa duração."
                    logging.error(error_msg)
                    raise Exception(error_msg)
                elif response.status_code != 200:
                    error_msg = f"Erro ao coletar leads: {response.status_code}"
                    logging.error(error_msg)
                    logging.error(response.json() if response.text else "Sem mensagem de erro")
                    raise Exception(error_msg)

                try:
                    data = response.json()['_embedded']['leads']
                except (KeyError, ValueError) as e:
                    error_msg = f"Erro na estrutura de resposta da API: {str(e)}"
                    logging.error(error_msg)
                    raise Exception(error_msg)

                for lead in data:
                    # Converter campos de data
                    for date_field in ['closest_task_at', 'created_at', 'closed_at', 'updated_at']:
                        if date_field in lead:
                            lead[date_field] = convert_timestamp_to_date(lead[date_field])

                    # Processar campos personalizados
                    if 'custom_fields_values' in lead:
                        custom_fields_flat = flatten_custom_fields(lead['custom_fields_values'])
                        lead.update(custom_fields_flat)
                        del lead['custom_fields_values']

                    # Processar campos embedded
                    embedded_data = flatten_embedded_data(lead)
                    lead.update(embedded_data)

                    # Remover o campo _embedded original
                    if '_embedded' in lead:
                        del lead['_embedded']

                leads.extend(data)
                logging.info(f"Coletados {len(leads)} leads até agora...")

                if len(data) < 250:
                    break

                page += 1

            if not leads:
                logging.warning("Aviso: Nenhum lead foi coletado.")

            return leads

        # Função para salvar os leads em CSV e fazer upload para o GCS
        def save_leads_to_gcs(leads):
            if not leads:
                error_msg = "Nenhum lead para processar."
                logging.error(error_msg)
                raise Exception(error_msg)

            try:
                # Obter todos os cabeçalhos únicos
                all_keys = set()
                for lead in leads:
                    all_keys.update(lead.keys())

                # Salvar arquivo CSV localmente
                with open(local_file_path, 'w', newline='', encoding='utf-8') as output_file:
                    dict_writer = csv.DictWriter(output_file, fieldnames=list(all_keys), delimiter=';')
                    dict_writer.writeheader()
                    dict_writer.writerows(leads)

                # Upload para o GCS usando a função unificada
                Utils.upload_to_gcs(bucket_name, local_file_path, gcs_file_path)

                logging.info(f"Total de {len(leads)} leads salvos com sucesso.")

            except Exception as e:
                error_msg = f"Erro ao salvar leads no GCS: {str(e)}"
                logging.error(error_msg)
                raise Exception(error_msg)

        # Execução principal
        leads = get_leads()
        save_leads_to_gcs(leads)
        logging.info(f"Coleta concluída. Total de {len(leads)} leads salvos em {gcs_file_path}.")

    except Exception as e:
        logging.error(f"FALHA CRÍTICA em run_leads: {str(e)}")
        raise


def run_unsorted_leads(customer):
    """
    Coleta leads não classificados do Kommo e salva no GCS.

    Args:
        customer (dict): Informações do cliente

    Raises:
        Exception: Se ocorrer um erro durante o processo
    """
    try:
        import requests
        import csv
        from datetime import datetime, timezone

        # Configurações
        access_token = customer['token']
        subdomain = customer['subdomain']
        base_url = f'https://{subdomain}.kommo.com'
        bucket_name = customer['bucket_name']
        gcs_file_path = 'unsorted_leads/unsorted_leads.csv'
        local_file_path = f"/tmp/{customer['project_id']}_unsorted_leads.csv"

        # Função para converter timestamps
        def convert_timestamp_to_date(timestamp):
            if isinstance(timestamp, int) or (isinstance(timestamp, str) and timestamp.isdigit()):
                return datetime.fromtimestamp(int(timestamp), timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
            return timestamp

        # Função para coletar leads não classificados
        def get_unsorted_leads():
            headers = {
                'Authorization': f'Bearer {access_token}'
            }
            unsorted_leads = []
            page = 1

            logging.info("Iniciando a coleta de leads não classificados...")

            while True:
                params = {
                    'limit': 250,
                    'page': page,
                    'with': 'contacts,companies,tags,metadata'
                }

                try:
                    logging.info(f"Solicitando leads não classificados, página: {page}")
                    response = requests.get(f'{base_url}/api/v4/leads/unsorted', headers=headers, params=params)

                    if response.status_code == 204:
                        logging.info("Nenhum lead não classificado adicional foi encontrado.")
                        break
                    elif response.status_code == 401:
                        error_msg = "Erro de autenticação. Verifique o token de longa duração."
                        logging.error(error_msg)
                        raise Exception(error_msg)
                    elif response.status_code != 200:
                        error_msg = f"Erro ao coletar leads não classificados: {response.status_code}"
                        logging.error(error_msg)
                        logging.error(response.json() if response.text else "Sem mensagem de erro")
                        raise Exception(error_msg)

                    data = response.json()
                    if '_embedded' not in data or 'unsorted' not in data['_embedded']:
                        error_msg = "Estrutura de resposta inesperada - não contém leads não classificados."
                        logging.error(error_msg)
                        raise Exception(error_msg)

                    # Processar leads não classificados
                    leads_data = data['_embedded']['unsorted']
                    normalized_leads = [Utils._normalize_keys(lead) for lead in leads_data]

                    # Converter campos de data
                    for lead in normalized_leads:
                        for date_field in ['created_at', 'updated_at']:
                            if date_field in lead:
                                lead[date_field] = convert_timestamp_to_date(lead[date_field])

                    # Achatar leads
                    flattened_leads = [Utils._flatten_json(lead) for lead in normalized_leads]
                    unsorted_leads.extend(flattened_leads)

                    logging.info(f"Coletados {len(unsorted_leads)} leads não classificados até agora...")

                    if len(leads_data) < 250:
                        break

                    page += 1

                except Exception as e:
                    error_msg = f"Erro ao processar a requisição: {str(e)}"
                    logging.error(error_msg)
                    raise Exception(error_msg)

            if not unsorted_leads:
                logging.warning("Aviso: Nenhum lead não classificado foi coletado.")

            return unsorted_leads

        # Função para processar e salvar leads não classificados
        def save_unsorted_leads_to_gcs(unsorted_leads):
            if not unsorted_leads:
                error_msg = "Nenhum lead não classificado para processar."
                logging.error(error_msg)
                raise Exception(error_msg)

            try:
                # Converter para DataFrame
                df = pd.DataFrame(unsorted_leads)

                if df.empty:
                    error_msg = "DataFrame vazio após conversão dos leads."
                    logging.error(error_msg)
                    raise Exception(error_msg)

                # Processar DataFrame
                df = Utils._normalize_column_names(df)
                df = Utils._remove_empty_columns(df)
                df = Utils._convert_columns_to_nullable_int(df)

                # Salvar CSV localmente
                with open(local_file_path, 'w', newline='', encoding='utf-8') as output_file:
                    dict_writer = csv.DictWriter(output_file, fieldnames=list(df.columns), delimiter=';')
                    dict_writer.writeheader()
                    dict_writer.writerows(df.to_dict('records'))

                # Upload para o GCS usando a função unificada
                Utils.upload_to_gcs(bucket_name, local_file_path, gcs_file_path)

                logging.info(f"DataFrame processado com {len(df)} linhas e {len(df.columns)} colunas")

            except Exception as e:
                error_msg = f"Erro ao processar e salvar leads não classificados: {str(e)}"
                logging.error(error_msg)
                raise Exception(error_msg)

        # Execução principal
        unsorted_leads = get_unsorted_leads()
        save_unsorted_leads_to_gcs(unsorted_leads)
        logging.info(f"Coleta concluída. Total de {len(unsorted_leads)} leads não classificados processados.")

    except Exception as e:
        logging.error(f"FALHA CRÍTICA em run_unsorted_leads: {str(e)}")
        raise


def run_pipelines(customer):
    """
    Coleta pipelines do Kommo e salva no GCS.

    Args:
        customer (dict): Informações do cliente

    Raises:
        Exception: Se ocorrer um erro durante o processo
    """
    try:
        import requests
        import csv

        # Configurações
        access_token = customer['token']
        subdomain = customer['subdomain']
        bucket_name = customer['bucket_name']
        base_url = f'https://{subdomain}.kommo.com'
        local_file_path = f"/tmp/{customer['project_id']}_pipelines.csv"
        gcs_file_path = "pipelines/pipelines.csv"

        # Função para obter pipelines
        def get_pipelines():
            headers = {
                'Authorization': f'Bearer {access_token}'
            }

            logging.info("Obtendo pipelines do Kommo...")
            response = requests.get(f'{base_url}/api/v4/leads/pipelines', headers=headers)

            if response.status_code == 200:
                try:
                    return response.json()["_embedded"]["pipelines"]
                except (KeyError, ValueError) as e:
                    error_msg = f"Erro na estrutura de resposta da API: {str(e)}"
                    logging.error(error_msg)
                    raise Exception(error_msg)
            elif response.status_code == 401:
                error_msg = "Erro de autenticação. Verifique o token de longa duração."
                logging.error(error_msg)
                raise Exception(error_msg)
            else:
                error_msg = f"Erro ao obter os pipelines: {response.status_code}"
                logging.error(error_msg)
                logging.error(response.json() if response.text else "Sem mensagem de erro")
                raise Exception(error_msg)

        # Função para salvar pipelines em CSV
        def save_pipelines_to_csv(pipelines_data):
            if not pipelines_data:
                error_msg = "Nenhum pipeline encontrado ou erro na requisição."
                logging.error(error_msg)
                raise Exception(error_msg)

            try:
                with open(local_file_path, mode="w", newline='', encoding='utf-8') as file:
                    writer = csv.writer(file, delimiter=';')
                    writer.writerow(["Pipeline ID", "Pipeline Name", "Status ID", "Status Name"])

                    for pipeline in pipelines_data:
                        pipeline_id = pipeline["id"]
                        pipeline_name = pipeline["name"]

                        if "_embedded" not in pipeline or "statuses" not in pipeline["_embedded"]:
                            error_msg = f"Estrutura de pipeline inesperada para pipeline ID {pipeline_id}"
                            logging.error(error_msg)
                            raise Exception(error_msg)

                        for status in pipeline["_embedded"]["statuses"]:
                            status_id = status["id"]
                            status_name = status["name"]
                            writer.writerow([pipeline_id, pipeline_name, status_id, status_name])

                logging.info("Arquivo pipelines.csv gerado com sucesso.")
                return local_file_path

            except Exception as e:
                error_msg = f"Erro ao processar dados dos pipelines: {str(e)}"
                logging.error(error_msg)
                raise Exception(error_msg)

        # Execução principal
        logging.info("Iniciando processamento de pipelines...")
        pipelines_data = get_pipelines()
        save_pipelines_to_csv(pipelines_data)

        # Upload para o GCS usando a função unificada
        Utils.upload_to_gcs(bucket_name, local_file_path, gcs_file_path)

        logging.info("Processamento de pipelines concluído com sucesso.")

    except Exception as e:
        logging.error(f"FALHA CRÍTICA em run_pipelines: {str(e)}")
        raise


def run_users(customer):
    """
    Coleta usuários do Kommo e salva no GCS.

    Args:
        customer (dict): Informações do cliente

    Raises:
        Exception: Se ocorrer um erro durante o processo
    """
    try:
        import requests
        import csv

        # Configurações
        access_token = customer['token']
        subdomain = customer['subdomain']
        bucket_name = customer['bucket_name']
        base_url = f'https://{subdomain}.kommo.com'
        local_file_path = f"/tmp/{customer['project_id']}_users_list.csv"
        gcs_file_path = "users/users_list.csv"

        # Função para coletar lista de usuários
        def get_users_list():
            url = f"{base_url}/api/v4/users"
            headers = {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/hal+json",
            }

            logging.info("Obtendo lista de usuários do Kommo...")
            response = requests.get(url, headers=headers)

            if response.status_code == 200:
                try:
                    data = response.json()
                    if "_embedded" not in data or "users" not in data["_embedded"]:
                        error_msg = "Estrutura de resposta inesperada - não contém usuários."
                        logging.error(error_msg)
                        raise Exception(error_msg)
                    return data["_embedded"]["users"]
                except (KeyError, ValueError) as e:
                    error_msg = f"Erro na estrutura de resposta da API: {str(e)}"
                    logging.error(error_msg)
                    raise Exception(error_msg)
            elif response.status_code == 401:
                error_msg = "Erro de autenticação. Verifique o token de longa duração."
                logging.error(error_msg)
                raise Exception(error_msg)
            else:
                error_msg = f"Erro ao coletar a lista de usuários: {response.status_code}"
                logging.error(error_msg)
                logging.error(response.json() if response.text else "Sem mensagem de erro")
                raise Exception(error_msg)

        # Função para salvar usuários em CSV
        def save_to_csv(users):
            if not users:
                error_msg = "Nenhum dado de usuário para salvar."
                logging.error(error_msg)
                raise Exception(error_msg)

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
                            logging.error(error_msg)
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

                logging.info("Arquivo users_list.csv gerado com sucesso.")
                return local_file_path

            except Exception as e:
                error_msg = f"Erro ao salvar dados no CSV: {str(e)}"
                logging.error(error_msg)
                raise Exception(error_msg)

        # Execução principal
        logging.info("Coletando lista de usuários...")
        users = get_users_list()
        save_to_csv(users)

        # Upload para o GCS usando a função unificada
        Utils.upload_to_gcs(bucket_name, local_file_path, gcs_file_path)

        logging.info("Dados de usuários salvos com sucesso e enviados para o Google Cloud Storage.")

    except Exception as e:
        logging.error(f"FALHA CRÍTICA em run_users: {str(e)}")
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
