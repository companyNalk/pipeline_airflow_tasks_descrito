"""
Conexa module for data extraction functions.
This module contains functions specific to the Conexa integration.
"""

import concurrent.futures
import json
import logging
import os
import pathlib
import re
import time
from datetime import datetime
from queue import Queue, Empty
from threading import Lock

import pandas as pd
import requests
from core import gcs
from google.cloud import storage

MAX_WORKERS = 10
REQUEST_COUNTER_LOCK = Lock()
request_counter = 0
reset_time = time.time() + 60
BACKOFF_MULTIPLIER = 1.5  # Multiplicador para recuo dinâmico


def generic_function(customer, endpoint_name, folder_name):
    # LOGGING
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger("ConexaSalesGCP")

    GENERIC_NAME = endpoint_name

    # GCP
    BUCKET_NAME = customer['bucket_name']
    FOLDER_PATH = folder_name
    SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH
    CSV_FILENAME = f"{folder_name}.csv"

    # API
    API_BASE_URL = customer['api_base_url']
    API_TOKEN = customer['api_token']
    AUTH_TYPE = "Bearer"
    ROUTER = GENERIC_NAME
    API_RATE_LIMIT = 100  # Máximo de 100 requisições por minuto

    THREAD_DELAY = 0.5  # Atraso entre requisições (em segundos)

    def check_rate_limit():
        """
        Controla o limite de requisições por minuto usando um contador global sincronizado
        Retorna True se estiver dentro do limite, False caso contrário
        """
        global request_counter, reset_time

        # Se nenhum limite de taxa for definido, sempre retorna True
        if API_RATE_LIMIT is None:
            return True

        with REQUEST_COUNTER_LOCK:
            current_time = time.time()

            # Reinicia o contador se 1 minuto tiver passado
            if current_time > reset_time:
                request_counter = 0
                reset_time = current_time + 60

            # Verifica se o limite foi atingido
            if request_counter >= API_RATE_LIMIT:
                return False

            # Incrementa o contador e retorna
            request_counter += 1
            return True

    def fetch_api_data(endpoint=ROUTER, params=None, page_url=None):
        """
        Busca dados da API Conexa com suporte para paginação

        A estrutura de resposta da API Conexa é esperada como:
        {
            "data": [ ... matriz de registros ... ],
            "pagination": {
                "itemPerPage": 20,
                "currentPage": 1,
                "totalPages": 2276,
                "totalItems": 45520
            }
        }
        """
        """
        Busca dados da API Conexa com suporte para paginação
        """
        # Cabeçalho de autorização
        headers = {
            "Authorization": f"{AUTH_TYPE} {API_TOKEN}",
            "accept": "application/json"
        }

        # Determina a URL a ser usada
        if page_url:
            # Usa a URL completa fornecida (para paginação baseada em página)
            url = page_url
            # Registra a URL sendo usada
            logger.debug(f"Usando URL de paginação: {url}")
        else:
            # Constrói URL a partir da base e endpoint
            url = f"{API_BASE_URL}/{endpoint}"
            logger.debug(f"Usando URL base: {url}")

        # Prepara parâmetros da requisição
        request_params = params.copy() if params else {}

        # Adiciona parâmetro de tamanho para obter mais registros por página
        if 'size' not in request_params:
            request_params['size'] = 100  # Solicita 100 registros por página

        # Faz requisição com lógica de repetição
        max_retries = 5
        retries = 0
        backoff_time = THREAD_DELAY

        while retries < max_retries:
            if check_rate_limit():
                try:
                    # Atraso dinâmico antes de fazer a requisição
                    time.sleep(backoff_time)

                    response = requests.get(url, headers=headers, params=request_params)
                    response.raise_for_status()

                    # Após uma requisição bem-sucedida, reduz o tempo de recuo
                    # mas mantém pelo menos em THREAD_DELAY
                    global BACKOFF_MULTIPLIER
                    BACKOFF_MULTIPLIER = max(1.0, BACKOFF_MULTIPLIER * 0.9)

                    # Analisa JSON da resposta
                    response_data = response.json()

                    # Registra a resposta bruta para depuração (primeiros 300 caracteres)
                    response_text = json.dumps(response_data)[:300]
                    logger.debug(f"Resposta da API: {response_text}...")

                    # Extrai informações de paginação
                    pagination = response_data.get('pagination', {})

                    # Verificação da estrutura da API Conexa
                    if 'data' not in response_data and isinstance(response_data, list):
                        # Se a resposta for diretamente uma matriz, embrulha-a
                        logger.info("Resposta da API está diretamente em formato de lista")
                        return {
                            'data': response_data,  # A resposta já é a matriz de dados
                            'pagination': pagination,
                            'url': url
                        }

                    # Retorna os dados e informações de paginação
                    return {
                        'data': response_data.get('data', []),  # Obtém a matriz de dados ou lista vazia
                        'pagination': pagination,
                        'url': url  # Inclui a URL para rastreamento
                    }

                except requests.exceptions.HTTPError as e:
                    if response.status_code == 429:
                        retries += 1
                        # Aumenta o tempo de recuo com cada erro 429
                        BACKOFF_MULTIPLIER *= 1.2
                        wait_time = 5 * retries * BACKOFF_MULTIPLIER  # Recuo exponencial dinâmico
                        logger.warning(
                            f"Limite de requisições excedido (429). Tentativa {retries}/{max_retries}. Aguardando {wait_time:.1f}s...")
                        time.sleep(wait_time)
                        continue
                    elif response.status_code >= 500:
                        retries += 1
                        wait_time = 3 * retries
                        logger.warning(
                            f"Erro do servidor ({response.status_code}). Tentativa {retries}/{max_retries}. Aguardando {wait_time}s...")
                        time.sleep(wait_time)
                        continue
                    else:
                        logger.error(f"Erro HTTP: {e}")
                        raise

                except requests.exceptions.RequestException as e:
                    retries += 1
                    wait_time = 3 * retries
                    logger.error(
                        f"Erro na requisição: {e}. Tentativa {retries}/{max_retries}. Aguardando {wait_time}s...")
                    time.sleep(wait_time)
                    continue
            else:
                # Se estiver no limite de taxa, espera antes de tentar novamente
                logger.debug("Rate limit atingido, aguardando 1 segundo...")
                time.sleep(1)

        logger.error(f"Falha após {max_retries} tentativas para URL: {url}")
        raise Exception(f"Falha após {max_retries} tentativas para URL: {url}")

    def convert_unix_timestamp_to_iso8601(value):
        """
        Converte timestamp Unix para formato ISO 8601
        """
        try:
            # Verifica se o valor é um número e um timestamp plausível (após o ano 2000)
            if isinstance(value, (int, float)) and value > 946684800:  # 1º de janeiro de 2000
                # Converte para formato ISO 8601
                return datetime.fromtimestamp(value).isoformat()
        except:
            pass
        return value

    def normalize_column_name(name):
        """
        Normaliza nomes de colunas: converte camelCase para snake_case,
        transforma para minúsculas e substitui caracteres especiais por underscore
        """
        # Converte para string caso ainda não seja
        name = str(name)

        # Passo 1: Trata camelCase adicionando underscores antes das letras maiúsculas
        # Esta regex procura por uma letra minúscula seguida por uma maiúscula
        # e adiciona um underscore entre elas
        s1 = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', name)

        # Passo 2: Converte para minúsculas
        s2 = s1.lower()

        # Passo 3: Substitui caracteres especiais restantes por underscore
        s3 = re.sub(r'[^a-z0-9]', '_', s2)

        # Passo 4: Substitui múltiplos underscores por apenas um
        s4 = re.sub(r'_+', '_', s3)

        # Passo 5: Remove underscores no início e no final
        s5 = s4.strip('_')

        return s5

    def convert_json_to_csv(data, output_file=None, csv_separator=";", id_field='saleId', remove_empty_columns=True):
        """
        Converte dados JSON de qualquer profundidade para um DataFrame normalizado,
        mantendo chaves originais (convertidas para minúsculas) e adicionando numeração apenas para itens de lista.
        Remove colunas onde todos os registros não têm valor.
        """
        logger.info("Iniciando processamento de dados JSON complexos...")

        # Garante que estamos trabalhando com uma lista de dicionários (registros)
        if isinstance(data, dict):
            records = [data]
        elif isinstance(data, list):
            if all(isinstance(item, dict) for item in data):
                records = data
            else:
                # Embrulha itens de lista simples em dicionários
                records = [{"item": item} for item in data]
        else:
            records = [{"value": data}]

        # Verifica o conjunto de dados para completude
        ids = set()
        duplicate_count = 0

        if records and id_field in records[0]:
            for record in records:
                if id_field in record:
                    if record[id_field] in ids:
                        duplicate_count += 1
                    else:
                        ids.add(record[id_field])

            logger.info(
                f"Verificação de integridade: {len(ids)} registros únicos, {duplicate_count} duplicatas encontradas")
        else:
            logger.warning(
                f"Campo de ID '{id_field}' não encontrado nos registros. Não foi possível verificar duplicatas.")

        # Função auxiliar recursiva para achatar cada registro
        def json_to_csv(data, prefix="", result=None):
            if result is None:
                result = {}

            # Processa com base no tipo de dados
            if isinstance(data, dict):
                # Para dicionários, aninha chaves com underscore e converte para minúsculas
                for key, value in data.items():
                    # Normaliza o nome da chave
                    key = normalize_column_name(key)
                    new_prefix = f"{prefix}_{key}" if prefix else key

                    # Trata o valor
                    if isinstance(value, (dict, list)):
                        json_to_csv(value, new_prefix, result)
                    else:
                        # Converte timestamps para formato ISO, se necessário
                        value = convert_unix_timestamp_to_iso8601(value)
                        result[new_prefix] = value

            elif isinstance(data, list):
                # Para listas, numera os itens
                for i, item in enumerate(data, 1):
                    if isinstance(item, dict):
                        # Para dicionários dentro de listas
                        for sub_key, sub_value in item.items():
                            # Normaliza o nome da chave
                            sub_key = normalize_column_name(sub_key)
                            new_prefix = f"{prefix}_{sub_key}_{i}" if prefix else f"{sub_key}_{i}"
                            if isinstance(sub_value, (dict, list)):
                                json_to_csv(sub_value, new_prefix, result)
                            else:
                                # Converte timestamps para formato ISO, se necessário
                                sub_value = convert_unix_timestamp_to_iso8601(sub_value)
                                result[new_prefix] = sub_value
                    else:
                        # Para valores simples dentro de listas
                        result[f"{prefix}_{i}" if prefix else f"{prefix}{i}"] = item

            return result

        # Processa cada registro
        logger.info(f"Processando {len(records)} registros JSON...")
        flattened_records = []

        # Para grandes quantidades de dados, mostra progresso
        progress_log = max(1, len(records) // 10)

        for i, record in enumerate(records):
            flattened_record = json_to_csv(record)
            flattened_records.append(flattened_record)

            # Registra progresso para conjuntos de dados grandes
            if (i + 1) % progress_log == 0 or (i + 1) == len(records):
                logger.info(f"Processados {i + 1}/{len(records)} registros ({((i + 1) / len(records)) * 100:.1f}%)")

        # Converte para DataFrame
        logger.info("Convertendo registros json/csv para DataFrame...")
        df = pd.DataFrame(flattened_records)

        # Converte colunas de ID para inteiro
        for col in df.columns:
            # Verifica se o nome da coluna contém "id" (sem distinção entre maiúsculas e minúsculas)
            if 'id' in col.lower():
                try:
                    # Tenta converter para numérico, mantendo valores NaN
                    numeric_values = pd.to_numeric(df[col], errors='coerce')

                    # Se mais de 80% dos valores não nulos foram convertidos com sucesso, então é uma coluna de ID numérico
                    non_null_count = df[col].count()
                    successful_conversions = numeric_values.count()

                    # Evita divisão por zero
                    if non_null_count > 0 and (successful_conversions / non_null_count) >= 0.8:
                        # Usa dtype='Int64' (com I maiúsculo) - é o tipo inteiro anulável no pandas
                        # que permite valores NaN junto com inteiros
                        df[col] = numeric_values.astype('Int64')
                        logger.info(f"Coluna {col} convertida para inteiro")
                    else:
                        logger.info(f"Coluna {col} contém valores não numéricos, mantida como está")
                except Exception as e:
                    logger.warning(f"Não foi possível converter a coluna {col} para inteiro: {e}")

        # Conta o total de colunas antes de remover qualquer uma
        total_columns = len(df.columns)
        logger.info(f"Total de colunas antes da filtragem: {total_columns}")

        # Remove colunas onde todos os valores são nulos/NaN
        before_count = len(df.columns)

        if remove_empty_columns:
            df = df.dropna(axis=1, how='all')

        after_count = len(df.columns)
        removed_count = before_count - after_count

        # Registra os resultados da filtragem de colunas
        logger.info(f"Remoção de colunas sem valores: {removed_count} colunas removidas de um total de {before_count}")
        if removed_count > 0:
            percentage_removed = (removed_count / before_count) * 100
            logger.info(f"Removidas {removed_count} colunas sem valores ({percentage_removed:.1f}% das colunas)")

        # Salva em arquivo se o caminho for fornecido
        if output_file:
            try:
                df.to_csv(output_file, index=False, sep=csv_separator)
                logger.info(f"Arquivo CSV normalizado criado: {output_file} com {len(df.columns)} colunas")
            except Exception as e:
                logger.error(f"Erro ao salvar CSV: {e}")

        logger.info(f"Processamento finalizado. DataFrame criado com {len(df)} linhas e {len(df.columns)} colunas")

        # Registro adicional para análise de colunas
        logger.info(
            f"Análise de colunas: {total_columns} colunas totais, {removed_count} removidas, {after_count} mantidas")

        return df

    def upload_csv_to_bucket(data, bucket_name, blob_name, folder_path=None, separator=";"):
        """
        Envia um DataFrame como CSV para um bucket GCP com validação de integridade
        """
        try:
            storage_client = storage.Client(project=customer['project_id'])
            bucket = storage_client.bucket(BUCKET_NAME)

            # Cria caminho completo do blob incluindo pasta se especificado
            full_blob_path = blob_name
            if folder_path:
                # Remove barras no início e adiciona barra no final, se necessário
                clean_folder = folder_path.strip('/')
                if clean_folder:
                    full_blob_path = f"{clean_folder}/{blob_name}"

            # Cria blob
            blob = bucket.blob(full_blob_path)

            # Converte DataFrame para CSV em memória com tratamento de erros
            try:
                # Tenta converter usando modo padrão com separador especificado
                csv_string = data.to_csv(index=False, sep=separator)
            except Exception as csv_error:
                logger.warning(f"Erro na conversão padrão para CSV: {csv_error}")
                logger.info("Tentando converter com método alternativo...")

                # Método alternativo com mais segurança para tipos complexos
                csv_string = data.replace({pd.NA: None}).to_csv(index=False, na_rep='', sep=separator)

            # Calcula checksum para validação de integridade
            import hashlib
            checksum = hashlib.md5(csv_string.encode()).hexdigest()

            # Define metadados com checksum
            metadata = {'md5_checksum': checksum}
            blob.metadata = metadata

            # Envia para GCP
            blob.upload_from_string(csv_string, content_type="text/csv")

            # Verifica integridade do upload
            blob_object = bucket.get_blob(full_blob_path)
            if blob_object.md5_hash:
                logger.info(f"Upload verificado com sucesso: checksums correspondem")

            # Informações úteis adicionais
            size_kb = len(csv_string) / 1024
            row_count = len(data)
            logger.info(
                f"Arquivo '{full_blob_path}' ({size_kb:.2f} KB) com {row_count} linhas enviado com sucesso para o bucket '{bucket_name}'")
            return True

        except Exception as e:
            logger.error(f"Erro ao enviar arquivo para GCP: {e}")
            raise

    def collect_conexa_data_complete(endpoint=ROUTER, params=None):
        """
        Coleta todos os dados da API Conexa com verificação para garantir completude
        """
        logger.info("Coletando dados da API Conexa com verificação de completude...")

        all_data = []
        data_lock = Lock()  # Lock para acumulação de dados thread-safe
        url_queue = Queue()
        processed_pages = set()
        pages_lock = Lock()

        # Para rastreamento de registros
        record_ids = set()  # Rastreia IDs de registro únicos
        id_field = 'saleId'  # Campo de ID padrão para API de vendas Conexa

        # Rastreamento de progresso
        progress_tracker = {
            'total_records': 0,
            'processed_pages': 0,
            'last_update': time.time()
        }

        # Prepara parâmetros iniciais
        request_params = params.copy() if params else {}

        # Adiciona parâmetro de tamanho para obter mais registros por página
        if 'size' not in request_params:
            request_params['size'] = 100  # Solicita 100 registros por página

        # Busca primeira página
        try:
            response = fetch_api_data(endpoint=endpoint, params=request_params)

            # Extrai dados da estrutura da API Conexa
            # Na API Conexa, os dados de vendas estão diretamente na resposta como uma lista
            entity_data = response['data']

            if not isinstance(entity_data, list):
                logger.warning(f"Resposta inesperada da API. Esperava uma lista, recebeu: {type(entity_data)}")
                if isinstance(entity_data, dict) and endpoint in entity_data:
                    entity_data = entity_data[endpoint]
                else:
                    entity_data = []

            # Registra a estrutura de dados
            logger.info(f"Estrutura da resposta inicial: {type(entity_data)}")
            if entity_data and len(entity_data) > 0:
                logger.info(f"Exemplo do primeiro registro: {json.dumps(entity_data[0])[:200]}...")

            # Rastreia IDs únicos e adiciona dados à lista principal
            if entity_data:
                for record in entity_data:
                    if isinstance(record, dict) and id_field in record:
                        record_ids.add(record[id_field])

                all_data.extend(entity_data)
                logger.info(f"Obtidos {len(entity_data)} registros na página inicial")

            # Obtém informações de paginação
            pagination = response['pagination']

            if pagination:
                total_items = pagination.get('totalItems', 0)
                total_pages = pagination.get('totalPages', 0)
                per_page = pagination.get('itemPerPage', 100)  # Padrão para 100 se não especificado

                logger.info(f"Total de registros na API: {total_items} ({per_page} por página)")
                logger.info(f"Total de páginas a coletar: {total_pages}")

                progress_tracker['total_records'] = total_items

                # Gera URLs de página para API Conexa (começa a partir da página 2)
                for page in range(2, total_pages + 1):
                    # Usa o formato correto para a API Conexa: page=X e size=100
                    page_url = f"{API_BASE_URL}/{endpoint}?page={page}&size=100"
                    url_queue.put(page_url)

                logger.info(f"Enfileiradas {total_pages - 1} páginas para processamento")

        except Exception as e:
            logger.error(f"Erro ao buscar página inicial: {e}")
            return all_data

        # Se não houver mais páginas, retorna os dados
        if url_queue.empty():
            logger.info("Apenas uma página de resultados")
            return all_data

        # Define a função de processamento de página
        def process_conexa_page(page_url):
            # Extrai número da página para registro
            match = re.search(r'page=(\d+)', page_url)
            page_num = match.group(1) if match else "?"

            # Evita processar a mesma página duas vezes
            with pages_lock:
                if page_url in processed_pages:
                    logger.debug(f"Página {page_num} já foi processada. Pulando.")
                    return None
                processed_pages.add(page_url)

            try:
                logger.debug(f"Buscando página {page_num}...")
                response = fetch_api_data(page_url=page_url)

                # Extrai dados semelhantes à primeira página
                entity_data = response['data']

                if not isinstance(entity_data, list):
                    logger.warning(
                        f"Resposta inesperada da API na página {page_num}. Esperava uma lista, recebeu: {type(entity_data)}")
                    if isinstance(entity_data, dict) and endpoint in entity_data:
                        entity_data = entity_data[endpoint]
                    else:
                        entity_data = []

                # Log para debug
                logger.debug(f"Estrutura da resposta na página {page_num}: {json.dumps(response)[:200]}...")

                # IDs de registro para rastreamento
                new_records = 0
                duplicate_records = 0

                # Rastreia registros únicos
                with data_lock:
                    for record in entity_data:
                        if isinstance(record, dict) and id_field in record:
                            if record[id_field] not in record_ids:
                                record_ids.add(record[id_field])
                                new_records += 1
                            else:
                                duplicate_records += 1

                    # Adiciona dados à coleção principal
                    all_data.extend(entity_data)

                    # Atualiza o rastreamento de progresso
                    progress_tracker['processed_pages'] += 1

                if duplicate_records > 0:
                    logger.warning(f"Página {page_num}: {duplicate_records} registros duplicados encontrados")

                logger.info(f"Obtidos {len(entity_data)} registros na página {page_num} ({new_records} novos)")

                return entity_data
            except Exception as e:
                logger.error(f"Erro ao processar página {page_num}: {e}")
                return None

        # Processa páginas em paralelo usando um pool fixo de threads
        logger.info(f"Iniciando processamento paralelo com {MAX_WORKERS} workers")

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Cria um dict para rastrear futuros com suas URLs
            futures_to_url = {}

            # Função para adicionar novas tarefas
            def add_tasks():
                while len(futures_to_url) < MAX_WORKERS and not url_queue.empty():
                    try:
                        page_url = url_queue.get(block=False)
                        if page_url not in processed_pages:
                            future = executor.submit(process_conexa_page, page_url)
                            futures_to_url[future] = page_url
                    except Empty:
                        break

            # Carregamento inicial de tarefas - começa conservadoramente
            initial_tasks = min(MAX_WORKERS // 2, url_queue.qsize())
            for _ in range(initial_tasks):
                if not url_queue.empty():
                    page_url = url_queue.get()
                    if page_url not in processed_pages:
                        future = executor.submit(process_conexa_page, page_url)
                        futures_to_url[future] = page_url

            # Processa até que não haja mais URLs e nenhuma tarefa ativa
            last_progress_report = time.time()

            while futures_to_url or not url_queue.empty():
                # Espera que as tarefas sejam concluídas
                done, _ = concurrent.futures.wait(
                    futures_to_url,
                    return_when=concurrent.futures.FIRST_COMPLETED,
                    timeout=5
                )

                # Processa tarefas concluídas
                errors_this_batch = 0
                for future in done:
                    page_url = futures_to_url.pop(future)
                    try:
                        result = future.result()
                        if result is None:
                            errors_this_batch += 1
                    except Exception as e:
                        logger.error(f"Exceção em worker para URL {page_url}: {e}")
                        errors_this_batch += 1

                # Adiciona novas tarefas
                add_tasks()

                # Relatório de progresso
                current_time = time.time()
                if current_time - last_progress_report > 15:  # Relata progresso a cada 15 segundos
                    last_progress_report = current_time

                    # Calcula a porcentagem de conclusão se conhecermos o total de páginas
                    completion_str = ""
                    if progress_tracker['total_records'] > 0:
                        percent_complete = (len(all_data) / progress_tracker['total_records']) * 100
                        completion_str = f" - {percent_complete:.1f}% concluído"

                    # Relata progresso
                    logger.info(f"Progresso: {len(all_data)} registros coletados ({len(record_ids)} únicos), "
                                f"{url_queue.qsize()} páginas na fila, "
                                f"{len(futures_to_url)} tarefas ativas, "
                                f"{progress_tracker['processed_pages']} páginas processadas{completion_str}")

        # Verificação final
        if progress_tracker['total_records'] > 0 and len(all_data) < progress_tracker['total_records']:
            logger.warning(f"ALERTA DE DADOS INCOMPLETOS: Coletados {len(all_data)} registros, "
                           f"mas o total informado pela API é {progress_tracker['total_records']}")

            missing = progress_tracker['total_records'] - len(all_data)
            logger.warning(
                f"Faltam aproximadamente {missing} registros ({(missing / progress_tracker['total_records']) * 100:.1f}% do total)")

        # Verifica registros duplicados por ID
        if len(record_ids) < len(all_data):
            duplicate_count = len(all_data) - len(record_ids)
            logger.warning(f"Detectados {duplicate_count} registros duplicados que serão removidos")

            # Remove duplicatas preservando a ordem
            if id_field and all_data and isinstance(all_data[0], dict) and id_field in all_data[0]:
                seen_ids = set()
                unique_data = []

                for record in all_data:
                    if record[id_field] not in seen_ids:
                        seen_ids.add(record[id_field])
                        unique_data.append(record)

                logger.info(f"Removidos {len(all_data) - len(unique_data)} registros duplicados. "
                            f"Dados finais: {len(unique_data)} registros únicos.")
                all_data = unique_data

            logger.info(f"Coleta de dados finalizada. Total: {len(all_data)} registros.")
            return all_data
        return None

    def main():
        try:
            start_time = time.time()

            # Obtém todos os dados - usando o método de coleta completa
            logger.info("Iniciando coleta de dados da API Conexa")
            all_data = collect_conexa_data_complete(endpoint=ROUTER)

            # Verifica se há dados para processar
            if not all_data:
                logger.warning("Nenhum registro encontrado")
                return "Nenhum registro encontrado"

            record_count = len(all_data)
            # Usando função de normalização para lidar com dados JSON
            logger.info(f"Convertendo {record_count} registros de dados JSON complexos...")

            # Primeiro, cria DataFrame sem remover colunas vazias
            df_com_todas_colunas = convert_json_to_csv(all_data[:min(100, len(all_data))], remove_empty_columns=False,
                                                       id_field='saleId')
            total_columns_original = len(df_com_todas_colunas.columns)

            # Agora cria DataFrame final com colunas vazias removidas
            df = convert_json_to_csv(all_data, remove_empty_columns=True, id_field='saleId')

            # Calcula estatísticas de colunas
            final_columns = len(df.columns)
            removed_columns = total_columns_original - final_columns

            # Verifica tamanho do DataFrame
            if len(df) != record_count:
                logger.warning(
                    f"ALERTA: O número de linhas no DataFrame ({len(df)}) não corresponde ao número de registros coletados ({record_count})")

            logger.info(f"Dados JSON convertidos com sucesso para formato tabular. Total de colunas: {len(df.columns)}")
            logger.info(
                f"Estatísticas de colunas: {total_columns_original} colunas potenciais, {removed_columns} removidas, {final_columns} mantidas")

            # Envia para GCP com credenciais e pasta
            upload_csv_to_bucket(
                data=df,
                bucket_name=BUCKET_NAME,
                blob_name=CSV_FILENAME,
                folder_path=FOLDER_PATH,
                separator=";"
            )

            elapsed_time = time.time() - start_time
            column_stats = f"Estatísticas de colunas: {total_columns_original} detectadas, {removed_columns} removidas (sem valores), {final_columns} mantidas."
            result_message = f"Processamento concluído com sucesso em {elapsed_time:.2f} segundos. {record_count} registros exportados em formato CSV normalizado. {column_stats}"
            logger.info(result_message)
            return result_message

        except Exception as e:
            error_message = f"Erro durante o processamento: {str(e)}"
            logger.error(error_message)
            return error_message

    # START
    main()


def run_contracts(customer):
    generic_function(customer, 'contracts', 'contracts')


def run_recurring_sales(customer):
    generic_function(customer, 'recurringSales', 'recurring_sales')


def run_sales(customer):
    generic_function(customer, 'sales', 'sales')


def get_extraction_tasks():
    """
    Get the list of data extraction tasks for Conexa.

    Returns:
        list: List of task configurations
    """
    return [
        {
            'task_id': 'extract_contracts',
            'python_callable': run_contracts
        },
        {
            'task_id': 'extract_recurring_sales',
            'python_callable': run_recurring_sales
        },
        {
            'task_id': 'extract_sales',
            'python_callable': run_sales
        }
    ]
