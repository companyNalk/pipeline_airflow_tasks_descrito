import io
import json
import os
import pathlib
import re
import time
import unicodedata
from datetime import datetime

import pandas as pd
import requests
from core import gcs
from google.cloud import storage


def run_qualification(customer):
    # Configurações Globais
    ENDPOINT = 'qualificacoes'
    API_TOKEN = customer['API_TOKEN']
    BASE_URL = f"{customer['BASE_URL']}/{ENDPOINT}"
    HEADERS = {"Content-Type": "application/json", "Access-Token": API_TOKEN}
    SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH
    BUCKET_NAME = customer['bucket_name']
    OUTPUT_FILE = f"{ENDPOINT}.csv"

    def normalize_text(text):
        if not isinstance(text, str):
            text = str(text)
        text = text.lower()
        return unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('ASCII')

    def normalize_column_name(col_name):
        col_name = normalize_text(col_name)
        col_name = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', col_name)
        col_name = re.sub(r'[^a-z0-9_]', '_', col_name)
        return re.sub(r'_+', '_', col_name).strip('_')

    def fetch_qualificacoes(query_params=None):
        page = 1
        display_length = 200
        all_data = []
        total_collected = 0

        params = {"useAlias": "true", "displayLength": display_length, "page": page}
        if query_params:
            params.update(query_params)

        while True:
            params["page"] = page
            response = requests.get(BASE_URL, headers=HEADERS, params=params)

            if response.status_code != 200:
                print(f"Erro na requisição. Código: {response.status_code}")
                break

            data = response.json()
            if not data:
                break

            all_data.extend(data)
            total_collected += len(data)
            print(f"Coletados {total_collected} registros até agora.")

            page += 1
            time.sleep(1)

        return all_data

    def process_nested_object(obj, flat_dict, prefix=''):
        try:
            if isinstance(obj, dict):
                for k, v in obj.items():
                    if k is None:
                        k = "none"
                    key = normalize_column_name(f"{prefix}{k}")

                    if isinstance(v, dict):
                        process_nested_object(v, flat_dict, f"{key}_")
                    elif isinstance(v, list):
                        if v and isinstance(v[0], dict):
                            for i, item in enumerate(v):
                                process_nested_object(item, flat_dict, f"{key}_{i + 1}_")
                        else:
                            flat_dict[key] = str(v)
                    elif isinstance(v, str) and re.match(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}', v):
                        try:
                            date_obj = datetime.strptime(v.split('T')[0], '%Y-%m-%d')
                            flat_dict[key] = date_obj.strftime('%Y-%m-%d')
                        except:
                            flat_dict[key] = v
                    else:
                        flat_dict[key] = v
            else:
                if prefix:
                    flat_dict[normalize_column_name(prefix.rstrip('_'))] = obj
        except Exception as e:
            print(f"Erro ao processar objeto: {str(e)}")

    def process_nested_data(data):
        if not data:
            return pd.DataFrame()

        df = pd.DataFrame(data)
        df.columns = [normalize_column_name(col) for col in df.columns]

        processed_data = {}

        for idx, row in df.iterrows():
            if idx % 100 == 0:
                print(f"Processando registro {idx} de {len(df)}...")

            flat_row = {}

            for col, value in row.items():
                if isinstance(value, str) and ((value.startswith('{') and value.endswith('}')) or
                                               (value.startswith('[') and value.endswith(']'))):
                    try:
                        nested_data = json.loads(value)
                        process_nested_object(nested_data, flat_row, prefix=f"{col}_")
                    except json.JSONDecodeError:
                        flat_row[col] = value
                elif isinstance(value, dict):
                    process_nested_object(value, flat_row, prefix=f"{col}_")
                elif isinstance(value, list) and value and isinstance(value[0], dict):
                    for i, item in enumerate(value):
                        process_nested_object(item, flat_row, prefix=f"{col}_{i + 1}_")
                elif isinstance(value, str) and re.match(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}', value):
                    flat_row[col] = datetime.strptime(value.split('T')[0], '%Y-%m-%d').strftime('%Y-%m-%d')
                else:
                    flat_row[col] = value

            for k, v in flat_row.items():
                if k not in processed_data:
                    processed_data[k] = [None] * idx

                while len(processed_data[k]) < idx:
                    processed_data[k].append(None)

                processed_data[k].append(v)

        # Garante comprimento igual em todas as listas
        max_length = max([len(arr) for arr in processed_data.values()]) if processed_data else 0
        for k in processed_data:
            while len(processed_data[k]) < max_length:
                processed_data[k].append(None)

        return pd.DataFrame(processed_data)

    def save_to_gcs(df, bucket_name, blob_name):
        if df.empty:
            print("DataFrame vazio, nada para salvar.")
            return

        try:
            client = storage.Client(project=customer['project_id'])

            # Acessa o bucket
            bucket = client.bucket(bucket_name)

            # Converte o DataFrame para CSV em memória
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, sep=';', index=False, encoding='utf-8')
            csv_content = csv_buffer.getvalue()

            # Cria um blob e faz upload do conteúdo
            blob = bucket.blob(blob_name)
            blob.upload_from_string(csv_content, content_type='text/csv')

            print(f"Arquivo {blob_name} salvo com sucesso no bucket {bucket_name}.")
        except Exception as e:
            print(f"Erro ao salvar no GCS: {str(e)}")
            raise

    def main():
        try:
            print("Iniciando a coleta de qualificações...")

            # Parâmetros flexíveis para a consulta - incluindo todos os status possíveis
            query_params = {
                "status": "1,2,3,4,5,6",  # Todos os status possíveis para qualificações
                "useAlias": "true"
            }

            qualificacoes = fetch_qualificacoes(query_params)

            print("Processando dados aninhados...")
            processed_df = process_nested_data(qualificacoes)

            if not processed_df.empty:
                # Garante a presença das colunas essenciais para qualificações
                essential_fields = [
                    'data_limite', 'data_atualizacao', 'data_criacao',
                    'status', 'responsavel_nome', 'cliente_nome', 'codigo',
                    'compromissos', 'tarefas', 'etapa', 'pipeline'
                ]

                for field in essential_fields:
                    if field not in processed_df.columns:
                        field_name = field.replace('_', '')
                        camel_case = ''.join(w.capitalize() if i > 0 else w for i, w in enumerate(field.split('_')))

                        # Mapeamentos específicos para campos importantes
                        field_mappings = {
                            'data_limite': ['dataLimite', 'DataLimite', 'data_limite'],
                            'data_atualizacao': ['dataAtualizacao', 'DataAtualizacao', 'data_atualizacao'],
                            'data_criacao': ['dataCriacao', 'DataCriacao', 'data_criacao'],
                            'responsavel_nome': ['responsavel.nome', 'responsavel_nome'],
                            'cliente_nome': ['cliente.nome', 'cliente_nome'],
                            'pipeline': ['pipeline', 'funilVenda.nome', 'funilVenda_nome']
                        }

                        # Verifica se há um mapeamento específico para este campo
                        field_variations = field_mappings.get(field, [field_name, camel_case])

                        # Tenta encontrar o campo nos dados originais
                        field_found = False
                        for field_var in field_variations:
                            # Para campos aninhados com notação de ponto
                            if '.' in field_var:
                                parts = field_var.split('.')
                                if len(qualificacoes) > 0 and parts[0] in qualificacoes[0] and qualificacoes[0][
                                    parts[0]] is not None:
                                    try:
                                        processed_df[field] = [
                                            op[parts[0]][parts[1]] if op.get(parts[0]) and parts[1] in op[
                                                parts[0]] else None
                                            for op in qualificacoes
                                        ]
                                        field_found = True
                                        break
                                    except:
                                        continue
                            # Para campos de data
                            elif field.startswith('data_') and len(qualificacoes) > 0 and field_var in qualificacoes[0]:
                                try:
                                    processed_df[field] = [
                                        datetime.strptime(str(op[field_var]).split('T')[0], '%Y-%m-%d').strftime(
                                            '%Y-%m-%d')
                                        if op.get(field_var) else None
                                        for op in qualificacoes
                                    ]
                                    field_found = True
                                    break
                                except:
                                    continue
                            # Para campos normais
                            elif len(qualificacoes) > 0 and field_var in qualificacoes[0]:
                                processed_df[field] = [op.get(field_var) for op in qualificacoes]
                                field_found = True
                                break

                        # Se não encontrou o campo, cria vazio
                        if not field_found:
                            processed_df[field] = None
                            print(f"Campo {field} não encontrado nos dados originais, criado como vazio.")

                # Salva no Google Cloud Storage
                save_to_gcs(processed_df, BUCKET_NAME, OUTPUT_FILE)
            else:
                print("Nenhum dado processado para salvar.")
        except Exception as e:
            print(f"Erro durante o processamento: {str(e)}")
            raise e

    # INICAR QUALIFICACOES
    main()


def run_opportunity(customer):
    ENDPOINT = 'oportunidades'
    API_TOKEN = customer['API_TOKEN']
    BASE_URL = f"{customer['BASE_URL']}/{ENDPOINT}"
    HEADERS = {"Content-Type": "application/json", "Access-Token": API_TOKEN}
    SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH
    BUCKET_NAME = customer['bucket_name']
    OUTPUT_FILE = f"{ENDPOINT}.csv"

    def normalize_text(text):
        if not isinstance(text, str):
            text = str(text)
        text = text.lower()
        return unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('ASCII')

    def normalize_column_name(col_name):
        col_name = normalize_text(col_name)
        col_name = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', col_name)
        col_name = re.sub(r'[^a-z0-9_]', '_', col_name)
        return re.sub(r'_+', '_', col_name).strip('_')

    def fetch_oportunidades(query_params=None):
        # Nomes dos status conforme a documentação exata
        status_names = {
            1: "Em andamento",
            2: "Ganha",
            3: "Perdida",
            4: "Cancelada",
            5: "Prorrogada"
        }

        # Consulta inicial silenciosa
        params_count = {
            "useAlias": "true",
            "page": -1,
            "status": "1,2,3,4,5"
        }

        if query_params:
            params_count.update(query_params)

        # Faz a requisição inicial silenciosamente
        requests.get(BASE_URL, headers=HEADERS, params=params_count)

        # Agora inicia a coleta página a página
        page = 1
        display_length = 200
        all_data = []
        total_collected = 0

        # Parâmetros básicos
        params = {
            "useAlias": "true",
            "displayLength": display_length,
            "page": page,
            "status": "1,2,3,4,5"  # Todos os status possíveis
        }

        # Adiciona parâmetros adicionais de consulta se fornecidos
        if query_params:
            params.update(query_params)

        while True:
            params["page"] = page
            response = requests.get(BASE_URL, headers=HEADERS, params=params)

            if response.status_code != 200:
                print(f"Erro na requisição. Código: {response.status_code}")
                break

            data = response.json()
            if not data:
                print(f"Página {page} vazia. Finalizando coleta.")
                break

            # Verifica status das oportunidades em cada página
            status_count = {}
            for opp in data:
                status = opp.get('status')
                if isinstance(status, str):
                    try:
                        status = int(status)
                    except ValueError:
                        pass
                status_count[status] = status_count.get(status, 0) + 1

            # Cria a linha compacta com | como separador
            status_line = " | ".join([f"{status_names.get(status_id, f'Status {status_id}')}: {count}"
                                      for status_id, count in status_count.items()])

            # Adiciona os dados coletados
            all_data.extend(data)
            total_collected += len(data)

            # Linha compacta para cada página
            print(f"Página {page}: {status_line} | Total coletado: {total_collected}")

            page += 1
            time.sleep(1)

        # Relatório final de status
        if all_data:
            status_count = {}
            for opp in all_data:
                status = opp.get('status')
                if isinstance(status, str):
                    try:
                        status = int(status)
                    except ValueError:
                        pass
                status_count[status] = status_count.get(status, 0) + 1

            # Linha compacta para o relatório final
            status_line = " | ".join(
                [
                    f"{status_names.get(status_id, f'Status {status_id}')}: {count} ({(count / total_collected) * 100:.1f}%)"
                    for status_id, count in status_count.items()])

            print(f"\nDistribuição final ({total_collected} registros): {status_line}")

        return all_data

    def process_nested_object(obj, flat_dict, prefix=''):
        """Processa objetos aninhados sem limitação de profundidade ou itens de lista"""
        try:
            if isinstance(obj, dict):
                for k, v in obj.items():
                    if k is None or k == '':
                        continue

                    key = normalize_column_name(f"{prefix}{k}")

                    if isinstance(v, dict):
                        process_nested_object(v, flat_dict, f"{key}_")
                    elif isinstance(v, list):
                        if v and isinstance(v[0], dict):
                            # Processa todos os itens na lista
                            for i, item in enumerate(v):
                                process_nested_object(item, flat_dict, f"{key}_{i + 1}_")
                        else:
                            flat_dict[key] = str(v)
                    elif isinstance(v, str) and re.match(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}', v):
                        try:
                            date_obj = datetime.strptime(v.split('T')[0], '%Y-%m-%d')
                            flat_dict[key] = date_obj.strftime('%Y-%m-%d')
                        except:
                            flat_dict[key] = v
                    else:
                        flat_dict[key] = v
            elif obj is not None and prefix:
                flat_dict[normalize_column_name(prefix.rstrip('_'))] = obj
        except Exception as e:
            print(f"Erro ao processar objeto: {str(e)}")

    def process_nested_data(data):
        """Processa dados aninhados preservando todas as colunas originais"""
        if not data:
            return pd.DataFrame()

        # Inicializa um dicionário para as novas linhas "flat"
        processed_rows = []

        # Primeiro, identifica todas as colunas possíveis para garantir um DataFrame consistente
        all_columns = set()

        # Processa cada registro para identificar todas as colunas
        for idx, row_data in enumerate(data):
            if idx % 100 == 0:
                print(f"Analisando registro {idx} de {len(data)}...")

            flat_row = {}
            process_nested_object(row_data, flat_row)
            all_columns.update(flat_row.keys())

        print(f"Total de {len(all_columns)} colunas únicas identificadas.")

        # Agora processa cada registro com todas as colunas
        for idx, row_data in enumerate(data):
            if idx % 100 == 0:
                print(f"Processando registro {idx} de {len(data)}...")

            flat_row = {}
            process_nested_object(row_data, flat_row)

            # Garante que todas as colunas estão presentes
            for col in all_columns:
                if col not in flat_row:
                    flat_row[col] = None

            processed_rows.append(flat_row)

        # Cria o DataFrame com os dados processados
        processed_df = pd.DataFrame(processed_rows)

        # Adiciona coluna de data_limite (se ausente)
        if 'data_limite' not in processed_df.columns and 'datalimite' in processed_df.columns:
            processed_df['data_limite'] = processed_df['datalimite']

        # Adiciona coluna de data_atualizacao (se ausente)
        if 'data_atualizacao' not in processed_df.columns and 'dataatualizacao' in processed_df.columns:
            processed_df['data_atualizacao'] = processed_df['dataatualizacao']

        # Adiciona coluna de data_criacao (se ausente)
        if 'data_criacao' not in processed_df.columns and 'datacriacao' in processed_df.columns:
            processed_df['data_criacao'] = processed_df['datacriacao']

        # Adiciona coluna de valor_total (se ausente)
        if 'valor_total' not in processed_df.columns and 'valortotal' in processed_df.columns:
            processed_df['valor_total'] = processed_df['valortotal']

        # Adiciona coluna status_texto
        status_names = {
            1: "Em andamento",
            2: "Ganha",
            3: "Perdida",
            4: "Cancelada",
            5: "Prorrogada"
        }

        # Converte status para inteiro (se for string)
        if 'status' in processed_df.columns:
            processed_df['status_texto'] = processed_df['status'].apply(
                lambda x: status_names.get(
                    x if isinstance(x, int)
                    else int(x) if isinstance(x, str) and x.isdigit()
                    else x,
                    f"Status {x}"
                )
            )

        return processed_df

    def save_to_gcs(df, bucket_name, blob_name):
        if df.empty:
            print("DataFrame vazio, nada para salvar.")
            return

        try:
            client = storage.Client(project=customer['project_id'])

            # Acessa o bucket
            bucket = client.bucket(bucket_name)

            # Converte o DataFrame para CSV em memória
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, sep=';', index=False, encoding='utf-8')
            csv_content = csv_buffer.getvalue()

            # Cria um blob e faz upload do conteúdo
            blob = bucket.blob(blob_name)
            blob.upload_from_string(csv_content, content_type='text/csv')

            print(f"Arquivo {blob_name} salvo com sucesso no bucket {bucket_name}.")
        except Exception as e:
            print(f"Erro ao salvar no GCS: {str(e)}")
            raise

    def main():
        try:
            print("Iniciando a coleta de oportunidades...")

            # Parâmetros flexíveis para a consulta
            oportunidades = fetch_oportunidades()

            print("Processando dados aninhados...")
            processed_df = process_nested_data(oportunidades)

            if not processed_df.empty:
                print(
                    f"Processamento concluído. DataFrame resultante com {processed_df.shape[0]} linhas e {processed_df.shape[1]} colunas.")

                # Salva no Google Cloud Storage
                save_to_gcs(processed_df, BUCKET_NAME, OUTPUT_FILE)

                # Status final
                status_counts = processed_df['status'].value_counts()
                status_names = {
                    1: "Em andamento",
                    2: "Ganha",
                    3: "Perdida",
                    4: "Cancelada",
                    5: "Prorrogada"
                }

                # Formato compacto para o status final
                status_line = " | ".join([
                    f"{status_names.get(int(status) if isinstance(status, str) and status.isdigit() else status, f'Status {status}')}: {count}"
                    for status, count in status_counts.items()])
                print(f"DataFrame final: {status_line}")

            else:
                print("Nenhum dado processado para salvar.")
        except Exception as e:
            raise e

    # INICAR OPORTUNIDADES
    main()


def get_extraction_tasks():
    return [
        {
            'task_id': 'extract_qualification',
            'python_callable': run_qualification
        },
        {
            'task_id': 'extract_opportunity',
            'python_callable': run_opportunity
        }
    ]
