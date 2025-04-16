"""
Digital Manager Guru module for data extraction functions.
This module contains functions specific to the Digital Manager Guru integration.
"""

from core import gcs

def run_contacts(customer):
    """
    Extract contacts data from Digital Manager Guru API and upload to GCS.
    
    Args:
        customer (dict): Customer configuration with API credentials and settings
    """
    import concurrent.futures
    import logging
    import os
    import re
    import time
    from datetime import datetime, timedelta
    from queue import Queue, Empty
    from threading import Lock

    import pandas as pd
    import requests
    from google.cloud import storage

    # CONFIG DE LOGGING
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger("DigitalManagerGCP")

    # GCP
    GENERIC_NAME = "contacts"
    GCP_BUCKET_NAME = customer['bucket_name']
    GCP_FOLDER_PATH = GENERIC_NAME
    
    # ARQUIVO CSV
    CSV_FILENAME = f"{GENERIC_NAME}.csv"

    # API
    API_BASE_URL = "https://digitalmanager.guru/api/v2"
    API_TOKEN = customer['token']
    ROUTER = GENERIC_NAME
    AUTH = "Bearer"
    API_RATE_LIMIT = 360
    MAX_DAYS_INTERVAL = 180
    INITIAL_DATE = "2024-01-01"

    # CONFIG DE PARALELISMO
    MAX_WORKERS = 10
    REQUEST_COUNTER_LOCK = Lock()
    PROCESSED_CURSORS_LOCK = Lock()
    request_counter = 0
    reset_time = time.time() + 60
    processed_cursors = set()

    def check_rate_limit():
        """
        Controla o limite de requisições por minuto usando um contador global sincronizado
        Retorna True se estiver dentro do limite, False caso contrário
        """
        nonlocal request_counter, reset_time

        with REQUEST_COUNTER_LOCK:
            current_time = time.time()

            # Reseta o contador se 1 minuto tiver passado
            if current_time > reset_time:
                request_counter = 0
                reset_time = current_time + 60

            # Verifica se o limite foi atingido
            if request_counter >= API_RATE_LIMIT:
                return False

            # Incrementa o contador e retorna
            request_counter += 1
            return True

    def fetch_api_data(cursor=None, params=None):
        """
        Busca dados da API com suporte à paginação baseada em cursor
        """
        headers = {
            "Authorization": f"{AUTH} {API_TOKEN}",
            "accept": "application/json"
        }

        url = f"{API_BASE_URL}/{ROUTER}"

        # Prepara os parâmetros
        request_params = params.copy() if params else {}
        if cursor:
            request_params["cursor"] = cursor

        # Faz a requisição com controle de limite de taxa
        max_retries = 5
        retries = 0

        while retries < max_retries:
            if check_rate_limit():
                try:
                    response = requests.get(url, headers=headers, params=request_params)
                    response.raise_for_status()
                    return response.json()

                except requests.exceptions.HTTPError as e:
                    if response.status_code == 429:
                        retries += 1
                        wait_time = 5 * retries  # Backoff exponencial
                        logger.warning(
                            f"Limite de requisições excedido (429). Tentativa {retries}/{max_retries}. Aguardando {wait_time}s...")
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
                    logger.error(f"Erro na requisição: {e}. Tentativa {retries}/{max_retries}. Aguardando {wait_time}s...")
                    time.sleep(wait_time)
                    continue
            else:
                # Se estiver no limite de taxa, espera antes de tentar novamente
                time.sleep(1)

        logger.error(f"Falha após {max_retries} tentativas para cursor {cursor}")
        raise Exception(f"Falha após {max_retries} tentativas para cursor {cursor}")

    def process_page(params, cursor, cursor_queue, results_list, processed_cursors_set):
        """
        Processa uma página específica e encadeia para a próxima página
        """
        # Evita processar o mesmo cursor duas vezes
        with PROCESSED_CURSORS_LOCK:
            if cursor in processed_cursors_set:
                logger.debug(f"Cursor {cursor[:15]}... já foi processado. Pulando.")
                return None
            processed_cursors_set.add(cursor)

        try:
            logger.debug(f"Buscando página com cursor {cursor[:15]}...")
            response = fetch_api_data(cursor, params)

            # Adiciona resultados à lista compartilhada
            if "data" in response and response["data"]:
                results_list.extend(response["data"])
                logger.info(f"Obtidos {len(response['data'])} registros com cursor {cursor[:15]}...")

            # Encadeia próxima página
            if response.get("has_more_pages", 0) and response.get("next_cursor"):
                next_cursor = response.get("next_cursor")
                with PROCESSED_CURSORS_LOCK:
                    if next_cursor not in processed_cursors_set:
                        cursor_queue.put(next_cursor)
                        logger.debug(f"Adicionado próximo cursor {next_cursor[:15]}... à fila")

            return response
        except Exception as e:
            logger.error(f"Erro ao buscar página com cursor {cursor[:15]}: {e}")
            return None

    def collect_data_for_period(start_date, end_date):
        """
        Coleta todos os dados para um período específico usando processamento paralelo com fila
        """
        logger.info(f"Coletando dados do período: {start_date} até {end_date}")

        params = {
            "ordered_at_ini": start_date,
            "ordered_at_end": end_date
        }

        all_data = []
        cursor_queue = Queue()
        processed_cursors = set()

        # Busca a primeira página para iniciar o processo
        try:
            logger.info(f"Buscando página inicial para o período {start_date} a {end_date}")
            response = fetch_api_data(None, params)

            if "data" in response and response["data"]:
                all_data.extend(response["data"])
                logger.info(f"Obtidos {len(response['data'])} registros na página inicial")

            # Se houver mais páginas, adiciona o cursor à fila
            if response.get("has_more_pages", 0) and response.get("next_cursor"):
                cursor_queue.put(response.get("next_cursor"))
                logger.info(f"Adicionado primeiro cursor à fila para processamento paralelo")
        except Exception as e:
            logger.error(f"Erro ao buscar página inicial: {e}")
            return all_data

        # Se não houver mais páginas, retorna os dados
        if cursor_queue.empty():
            logger.info(f"Apenas uma página de resultados para o período {start_date} a {end_date}")
            return all_data

        # Processa páginas em paralelo usando a fila de cursores
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Lista para rastrear tarefas em execução
            futures = set()

            # Loop principal: processa até que não haja mais cursores na fila e nenhuma tarefa em execução
            while True:
                # Adiciona novas tarefas até atingir o máximo de workers ou esvaziar a fila
                while len(futures) < MAX_WORKERS and not cursor_queue.empty():
                    try:
                        cursor = cursor_queue.get(block=False)
                        future = executor.submit(process_page, params, cursor, cursor_queue, all_data, processed_cursors)
                        futures.add(future)
                    except Empty:
                        break  # Fila vazia

                # Se não houver tarefas em execução, terminamos
                if not futures:
                    break

                # Espera até que pelo menos uma tarefa seja concluída
                done, futures = concurrent.futures.wait(
                    futures,
                    return_when=concurrent.futures.FIRST_COMPLETED
                )

                # Verifica os resultados das tarefas concluídas
                for future in done:
                    try:
                        future.result()  # Apenas para capturar exceções
                    except Exception as e:
                        logger.error(f"Exceção em worker: {e}")

                # Verificação periódica de progresso
                if len(all_data) % 1000 == 0 and len(all_data) > 0:
                    logger.info(f"Progresso: {len(all_data)} registros coletados, {cursor_queue.qsize()} páginas na fila")

        logger.info(
            f"Coleta de dados finalizada para o período {start_date} a {end_date}. Total: {len(all_data)} registros.")
        return all_data

    def process_date_range(date_range):
        """
        Processa um intervalo de datas específico - pode ser chamado em paralelo
        """
        start_date, end_date = date_range
        try:
            return collect_data_for_period(start_date, end_date)
        except Exception as e:
            logger.error(f"Erro ao processar intervalo {start_date} a {end_date}: {e}")
            return []

    def generate_date_ranges(start_date_str, end_date_str=None, max_days=180):
        """
        Gera intervalos de datas com no máximo max_days entre início e fim
        """
        # Converte strings para objetos de data
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")

        # Se nenhuma data final for fornecida, usa a data atual
        if end_date_str:
            end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
        else:
            end_date = datetime.now()

        # Lista para armazenar intervalos
        date_ranges = []

        # Data atual para iteração
        current_start = start_date

        # Enquanto a data atual for anterior à data final
        while current_start < end_date:
            # Calcula a data final do intervalo (máximo de max_days a partir da data atual)
            current_end = min(current_start + timedelta(days=max_days - 1), end_date)

            # Adiciona o intervalo à lista
            date_ranges.append((
                current_start.strftime("%Y-%m-%d"),
                current_end.strftime("%Y-%m-%d")
            ))

            # Atualiza a data atual para o próximo intervalo
            current_start = current_end + timedelta(days=1)

        return date_ranges

    def is_unix_timestamp_field(field_name):
        """
        Verifica se o nome do campo termina com _at ou contém _at_ seguido por números
        """
        # Corresponde a nomes de campos terminando com _at ou contendo _at_ seguido por dígitos
        return bool(re.search(r'_at$', field_name) or re.search(r'_at_\d+', field_name))

    def is_already_date_format(value):
        """
        Verifica se o valor já está em formato de data (YYYY-MM-DD)
        """
        if not isinstance(value, str):
            return False

        # Verifica o padrão YYYY-MM-DD
        date_pattern = re.compile(r'^\d{4}-\d{2}-\d{2}$')

        # Versão estendida que também aceita hora
        datetime_pattern = re.compile(r'^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?$')

        return bool(date_pattern.match(value) or datetime_pattern.match(value))

    def convert_unix_to_datetime(timestamp):
        """
        Converte timestamp Unix para string de datetime
        Trata milissegundos (13 dígitos) e segundos (10 dígitos)
        Preserva valores que já estão em formato de data
        """
        if not timestamp or pd.isna(timestamp):
            return None

        # Se o valor já estiver em formato de data, mantém como está
        if is_already_date_format(str(timestamp)):
            return timestamp

        try:
            # Tenta converter para número primeiro
            if isinstance(timestamp, str):
                # Verifica se é uma string numérica
                if not timestamp.replace('.', '', 1).isdigit():
                    return timestamp

            # Converte para inteiro para lidar com possíveis valores de ponto flutuante
            timestamp = int(float(timestamp))

            # Verifica se o timestamp está em milissegundos (13 dígitos) e converte para segundos se necessário
            if len(str(timestamp)) >= 13:
                timestamp = timestamp / 1000

            return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
        except (ValueError, TypeError, OverflowError) as e:
            # Retorna o valor original se a conversão falhar
            logger.debug(f"Mantendo valor original '{timestamp}' - não é um timestamp Unix")
            return timestamp

    def convert_json_to_csv(data, output_file=None, csv_separator=";"):
        """
        Converte dados JSON de qualquer profundidade para um DataFrame normalizado,
        mantendo as chaves originais (convertidas para minúsculas) e adicionando numeração apenas para itens de lista.
        Também converte campos de timestamp Unix para formato datetime.
        """
        logger.info("Iniciando processamento de dados JSON complexos...")

        # Garante que estamos trabalhando com uma lista de dicionários (registros)
        if isinstance(data, dict):
            records = [data]
        elif isinstance(data, list):
            if all(isinstance(item, dict) for item in data):
                records = data
            else:
                # Envolve itens de lista simples em dicionários
                records = [{"item": item} for item in data]
        else:
            records = [{"value": data}]

        # Função auxiliar recursiva para achatar cada registro
        def flatten_record(data, prefix="", result=None):
            if result is None:
                result = {}

            # Processa com base no tipo de dados
            if isinstance(data, dict):
                # Para dicionários, aninha chaves com underscore e converte para minúsculas
                for key, value in data.items():
                    # Converte chave para minúsculas
                    key = key.lower()
                    new_prefix = f"{prefix}_{key}" if prefix else key
                    if isinstance(value, (dict, list)):
                        flatten_record(value, new_prefix, result)
                    else:
                        result[new_prefix] = value

            elif isinstance(data, list):
                # Para listas, numera os itens
                for i, item in enumerate(data, 1):
                    if isinstance(item, dict):
                        # Para dicionários dentro de listas
                        for sub_key, sub_value in item.items():
                            # Converte sub-chave para minúsculas
                            sub_key = sub_key.lower()
                            new_prefix = f"{prefix}_{sub_key}_{i}" if prefix else f"{sub_key}_{i}"
                            if isinstance(sub_value, (dict, list)):
                                flatten_record(sub_value, new_prefix, result)
                            else:
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
            flattened_record = flatten_record(record)
            flattened_records.append(flattened_record)

            # Registra progresso para grandes conjuntos de dados
            if (i + 1) % progress_log == 0 or (i + 1) == len(records):
                logger.info(f"Processados {i + 1}/{len(records)} registros ({((i + 1) / len(records)) * 100:.1f}%)")

        # Converte para DataFrame
        logger.info("Convertendo registros json/csv para DataFrame...")
        df = pd.DataFrame(flattened_records)

        # Converte campos de timestamp Unix para datetime
        logger.info("Verificando e convertendo campos de timestamp Unix para datetime...")
        unix_fields = []
        for column in df.columns:
            if is_unix_timestamp_field(column):
                unix_fields.append(column)

        if unix_fields:
            logger.info(f"Encontrados {len(unix_fields)} campos de timestamp para conversão: {unix_fields[:5]}...")

            # Primeiro verifica para cada coluna se a maioria dos valores já está em formato de data
            for column in unix_fields:
                if column in df.columns:
                    # Amostra de valores não nulos para análise
                    sample_values = df[column].dropna().head(50).astype(str)

                    if len(sample_values) == 0:
                        logger.info(f"Coluna {column} não tem valores não-nulos para analisar. Pulando.")
                        continue

                    # Conta quantos valores da amostra já parecem estar em formato de data
                    date_format_count = sum(1 for val in sample_values if is_already_date_format(val))

                    # Se a maioria dos valores já estiver em formato de data, pula esta coluna
                    if date_format_count > len(sample_values) / 2:
                        logger.info(f"Coluna {column} já parece estar em formato de data. Pulando conversão.")
                        continue

                    # Se precisar converter, faz a conversão com a função melhorada
                    logger.info(f"Convertendo coluna {column} de timestamp Unix para datetime...")
                    df[column] = df[column].apply(convert_unix_to_datetime)
        else:
            logger.info("Nenhum campo de timestamp identificado para conversão.")

        # Salva em arquivo se o caminho for fornecido
        if output_file:
            try:
                df.to_csv(output_file, index=False, sep=csv_separator)
                logger.info(f"Arquivo CSV normalizado criado: {output_file} com {len(df.columns)} colunas")
            except Exception as e:
                logger.error(f"Erro ao salvar CSV: {e}")

        logger.info(f"Processamento finalizado. DataFrame criado com {len(df)} linhas e {len(df.columns)} colunas")
        return df

    def upload_csv_to_bucket(data, bucket_name, blob_name, folder_path=None, separator=";"):
        """
        Envia um DataFrame como CSV para um bucket GCP
        """
        try:
            # Inicializa o cliente de armazenamento
            credentials = gcs.load_credentials_from_env()

            # Cria o caminho completo do blob incluindo a pasta, se especificada
            full_blob_path = blob_name
            if folder_path:
                # Remove barras no início e adiciona barra no final, se necessário
                clean_folder = folder_path.strip('/')
                if clean_folder:
                    full_blob_path = f"{clean_folder}/{blob_name}"

            # Converte DataFrame para CSV em memória com tratamento de erros
            try:
                # Tenta converter usando o modo padrão com o separador especificado
                csv_string = data.to_csv(index=False, sep=separator)
            except Exception as csv_error:
                logger.warning(f"Erro na conversão padrão para CSV: {csv_error}")
                logger.info("Tentando converter com método alternativo...")

                # Método alternativo com mais segurança para tipos complexos
                csv_string = data.replace({pd.NA: None}).to_csv(index=False, na_rep='', sep=separator)

            # Cria arquivo temporário
            local_file_path = f"/tmp/{customer['project_id']}_{blob_name}"
            with open(local_file_path, 'w', encoding='utf-8') as f:
                f.write(csv_string)

            # Envia para o GCP
            gcs.write_file_to_gcs(
                bucket_name=bucket_name,
                local_file_path=local_file_path,
                destination_name=full_blob_path,
                credentials=credentials
            )

            # Informações adicionais úteis
            size_kb = len(csv_string) / 1024
            logger.info(f"Arquivo '{full_blob_path}' ({size_kb:.2f} KB) enviado com sucesso para o bucket '{bucket_name}'")
            return True

        except Exception as e:
            logger.error(f"Erro ao enviar arquivo para GCP: {e}")
            raise

    # Função principal
    try:
        start_time = time.time()
        record_count = 0

        # Define data inicial e máxima (hoje)
        start_date = INITIAL_DATE
        end_date = datetime.now().strftime("%Y-%m-%d")

        # Gera intervalos de até 180 dias
        date_ranges = generate_date_ranges(start_date, end_date, MAX_DAYS_INTERVAL)
        logger.info(f"Coletando dados em {len(date_ranges)} intervalos de data")

        # Processa intervalos de data em paralelo
        all_data = []
        intervals_processed = 0

        # Usa um objeto compartilhado para armazenar resultados
        results_lock = Lock()

        # Melhorando o paralelismo real para processar todos os intervalos simultaneamente
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(date_ranges))) as executor:
            # Prepara todas as tarefas de uma vez
            futures = [executor.submit(process_date_range, date_range) for date_range in date_ranges]

            # Processa os resultados à medida que eles forem completados
            for future in concurrent.futures.as_completed(futures):
                # Determina qual intervalo de data está associado ao future que terminou
                for i, f in enumerate(futures):
                    if f == future:
                        date_range = date_ranges[i]
                        break
                else:
                    # Caso não encontre, continue (não deve acontecer)
                    continue

                start_range, end_range = date_range

                try:
                    interval_data = future.result()

                    # Adicionando thread safety para o objeto compartilhado
                    with results_lock:
                        all_data.extend(interval_data)
                        intervals_processed += 1
                        record_count += len(interval_data)

                    logger.info(f"Processado intervalo {start_range} a {end_range}: {len(interval_data)} registros. " +
                                f"Total: {record_count} registros em {intervals_processed}/{len(date_ranges)} intervalos")

                except Exception as e:
                    logger.error(f"Erro ao processar intervalo {start_range} a {end_range}: {e}")

        # Verifica se há dados para processar
        if not all_data:
            logger.warning("Nenhum registro encontrado em todo o período")
            return "Nenhum registro encontrado"

        # Usando função de normalização para lidar com dados JSON
        logger.info(f"Convertendo {len(all_data)} registros de dados JSON complexos...")

        # Achata dados JSON com a função aprimorada que trata conversão de timestamp
        df = convert_json_to_csv(all_data)
        logger.info(f"Dados JSON convertidos com sucesso para formato tabular. Total de colunas: {len(df.columns)}")

        # Envia para o GCP com credenciais e pasta
        upload_csv_to_bucket(
            data=df,
            bucket_name=GCP_BUCKET_NAME,
            blob_name=CSV_FILENAME,
            folder_path=GCP_FOLDER_PATH,
            separator=";"
        )

        elapsed_time = time.time() - start_time
        result_message = f"Processamento concluído com sucesso em {elapsed_time:.2f} segundos. {len(all_data)} registros exportados em formato CSV normalizado."
        logger.info(result_message)
        return result_message

    except Exception as e:
        error_message = f"Erro durante o processamento: {str(e)}"
        logger.error(error_message)
        return error_message


def run_transactions(customer):
    """
    Extract transactions data from Digital Manager Guru API and upload to GCS.
    
    Args:
        customer (dict): Customer configuration with API credentials and settings
    """
    import concurrent.futures
    import logging
    import os
    import re
    import time
    from datetime import datetime, timedelta
    from queue import Queue, Empty
    from threading import Lock

    import pandas as pd
    import requests
    from google.cloud import storage

    # CONFIG DE LOGGING
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger("DigitalManagerGCP")

    # GCP
    GENERIC_NAME = "transactions"
    GCP_BUCKET_NAME = customer['bucket_name']
    GCP_FOLDER_PATH = GENERIC_NAME
    
    # ARQUIVO CSV
    CSV_FILENAME = f"{GENERIC_NAME}.csv"

    # API
    API_BASE_URL = "https://digitalmanager.guru/api/v2"
    API_TOKEN = customer['token']
    ROUTER = GENERIC_NAME
    AUTH = "Bearer"
    API_RATE_LIMIT = 360
    MAX_DAYS_INTERVAL = 180
    INITIAL_DATE = "2024-01-01"

    # CONFIG DE PARALELISMO
    MAX_WORKERS = 10
    REQUEST_COUNTER_LOCK = Lock()
    PROCESSED_CURSORS_LOCK = Lock()
    request_counter = 0
    reset_time = time.time() + 60
    processed_cursors = set()

    def check_rate_limit():
        """
        Controla o limite de requisições por minuto usando um contador global sincronizado
        Retorna True se estiver dentro do limite, False caso contrário
        """
        nonlocal request_counter, reset_time

        with REQUEST_COUNTER_LOCK:
            current_time = time.time()

            # Reseta o contador se 1 minuto tiver passado
            if current_time > reset_time:
                request_counter = 0
                reset_time = current_time + 60

            # Verifica se o limite foi atingido
            if request_counter >= API_RATE_LIMIT:
                return False

            # Incrementa o contador e retorna
            request_counter += 1
            return True

    def fetch_api_data(cursor=None, params=None):
        """
        Busca dados da API com suporte à paginação baseada em cursor
        """
        headers = {
            "Authorization": f"{AUTH} {API_TOKEN}",
            "accept": "application/json"
        }

        url = f"{API_BASE_URL}/{ROUTER}"

        # Prepara os parâmetros
        request_params = params.copy() if params else {}
        if cursor:
            request_params["cursor"] = cursor

        # Faz a requisição com controle de limite de taxa
        max_retries = 5
        retries = 0

        while retries < max_retries:
            if check_rate_limit():
                try:
                    response = requests.get(url, headers=headers, params=request_params)
                    response.raise_for_status()
                    return response.json()

                except requests.exceptions.HTTPError as e:
                    if response.status_code == 429:
                        retries += 1
                        wait_time = 5 * retries  # Backoff exponencial
                        logger.warning(
                            f"Limite de requisições excedido (429). Tentativa {retries}/{max_retries}. Aguardando {wait_time}s...")
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
                    logger.error(f"Erro na requisição: {e}. Tentativa {retries}/{max_retries}. Aguardando {wait_time}s...")
                    time.sleep(wait_time)
                    continue
            else:
                # Se estiver no limite de taxa, espera antes de tentar novamente
                time.sleep(1)

        logger.error(f"Falha após {max_retries} tentativas para cursor {cursor}")
        raise Exception(f"Falha após {max_retries} tentativas para cursor {cursor}")

    def process_page(params, cursor, cursor_queue, results_list, processed_cursors_set):
        """
        Processa uma página específica e encadeia para a próxima página
        """
        # Evita processar o mesmo cursor duas vezes
        with PROCESSED_CURSORS_LOCK:
            if cursor in processed_cursors_set:
                logger.debug(f"Cursor {cursor[:15]}... já foi processado. Pulando.")
                return None
            processed_cursors_set.add(cursor)

        try:
            logger.debug(f"Buscando página com cursor {cursor[:15]}...")
            response = fetch_api_data(cursor, params)

            # Adiciona resultados à lista compartilhada
            if "data" in response and response["data"]:
                results_list.extend(response["data"])
                logger.info(f"Obtidos {len(response['data'])} registros com cursor {cursor[:15]}...")

            # Encadeia próxima página
            if response.get("has_more_pages", 0) and response.get("next_cursor"):
                next_cursor = response.get("next_cursor")
                with PROCESSED_CURSORS_LOCK:
                    if next_cursor not in processed_cursors_set:
                        cursor_queue.put(next_cursor)
                        logger.debug(f"Adicionado próximo cursor {next_cursor[:15]}... à fila")

            return response
        except Exception as e:
            logger.error(f"Erro ao buscar página com cursor {cursor[:15]}: {e}")
            return None

    def collect_data_for_period(start_date, end_date):
        """
        Coleta todos os dados para um período específico usando processamento paralelo com fila
        """
        logger.info(f"Coletando dados do período: {start_date} até {end_date}")

        params = {
            "ordered_at_ini": start_date,
            "ordered_at_end": end_date
        }

        all_data = []
        cursor_queue = Queue()
        processed_cursors = set()

        # Busca a primeira página para iniciar o processo
        try:
            logger.info(f"Buscando página inicial para o período {start_date} a {end_date}")
            response = fetch_api_data(None, params)

            if "data" in response and response["data"]:
                all_data.extend(response["data"])
                logger.info(f"Obtidos {len(response['data'])} registros na página inicial")

            # Se houver mais páginas, adiciona o cursor à fila
            if response.get("has_more_pages", 0) and response.get("next_cursor"):
                cursor_queue.put(response.get("next_cursor"))
                logger.info(f"Adicionado primeiro cursor à fila para processamento paralelo")
        except Exception as e:
            logger.error(f"Erro ao buscar página inicial: {e}")
            return all_data

        # Se não houver mais páginas, retorna os dados
        if cursor_queue.empty():
            logger.info(f"Apenas uma página de resultados para o período {start_date} a {end_date}")
            return all_data

        # Processa páginas em paralelo usando a fila de cursores
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Lista para rastrear tarefas em execução
            futures = set()

            # Loop principal: processa até que não haja mais cursores na fila e nenhuma tarefa em execução
            while True:
                # Adiciona novas tarefas até atingir o máximo de workers ou esvaziar a fila
                while len(futures) < MAX_WORKERS and not cursor_queue.empty():
                    try:
                        cursor = cursor_queue.get(block=False)
                        future = executor.submit(process_page, params, cursor, cursor_queue, all_data, processed_cursors)
                        futures.add(future)
                    except Empty:
                        break  # Fila vazia

                # Se não houver tarefas em execução, terminamos
                if not futures:
                    break

                # Espera até que pelo menos uma tarefa seja concluída
                done, futures = concurrent.futures.wait(
                    futures,
                    return_when=concurrent.futures.FIRST_COMPLETED
                )

                # Verifica os resultados das tarefas concluídas
                for future in done:
                    try:
                        future.result()  # Apenas para capturar exceções
                    except Exception as e:
                        logger.error(f"Exceção em worker: {e}")

                # Verificação periódica de progresso
                if len(all_data) % 1000 == 0 and len(all_data) > 0:
                    logger.info(f"Progresso: {len(all_data)} registros coletados, {cursor_queue.qsize()} páginas na fila")

        logger.info(
            f"Coleta de dados finalizada para o período {start_date} a {end_date}. Total: {len(all_data)} registros.")
        return all_data

    def process_date_range(date_range):
        """
        Processa um intervalo de datas específico - pode ser chamado em paralelo
        """
        start_date, end_date = date_range
        try:
            return collect_data_for_period(start_date, end_date)
        except Exception as e:
            logger.error(f"Erro ao processar intervalo {start_date} a {end_date}: {e}")
            return []

    def generate_date_ranges(start_date_str, end_date_str=None, max_days=180):
        """
        Gera intervalos de datas com no máximo max_days entre início e fim
        """
        # Converte strings para objetos de data
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")

        # Se nenhuma data final for fornecida, usa a data atual
        if end_date_str:
            end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
        else:
            end_date = datetime.now()

        # Lista para armazenar intervalos
        date_ranges = []

        # Data atual para iteração
        current_start = start_date

        # Enquanto a data atual for anterior à data final
        while current_start < end_date:
            # Calcula a data final do intervalo (máximo de max_days a partir da data atual)
            current_end = min(current_start + timedelta(days=max_days - 1), end_date)

            # Adiciona o intervalo à lista
            date_ranges.append((
                current_start.strftime("%Y-%m-%d"),
                current_end.strftime("%Y-%m-%d")
            ))

            # Atualiza a data atual para o próximo intervalo
            current_start = current_end + timedelta(days=1)

        return date_ranges

    def is_unix_timestamp_field(field_name):
        """
        Verifica se o nome do campo termina com _at ou contém _at_ seguido por números
        """
        # Corresponde a nomes de campos terminando com _at ou contendo _at_ seguido por dígitos
        return bool(re.search(r'_at$', field_name) or re.search(r'_at_\d+', field_name))

    def is_already_date_format(value):
        """
        Verifica se o valor já está em formato de data (YYYY-MM-DD)
        """
        if not isinstance(value, str):
            return False

        # Verifica o padrão YYYY-MM-DD
        date_pattern = re.compile(r'^\d{4}-\d{2}-\d{2}$')

        # Versão estendida que também aceita hora
        datetime_pattern = re.compile(r'^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?$')

        return bool(date_pattern.match(value) or datetime_pattern.match(value))

    def convert_unix_to_datetime(timestamp):
        """
        Converte timestamp Unix para string de datetime
        Trata milissegundos (13 dígitos) e segundos (10 dígitos)
        Preserva valores que já estão em formato de data
        """
        if not timestamp or pd.isna(timestamp):
            return None

        # Se o valor já estiver em formato de data, mantém como está
        if is_already_date_format(str(timestamp)):
            return timestamp

        try:
            # Tenta converter para número primeiro
            if isinstance(timestamp, str):
                # Verifica se é uma string numérica
                if not timestamp.replace('.', '', 1).isdigit():
                    return timestamp

            # Converte para inteiro para lidar com possíveis valores de ponto flutuante
            timestamp = int(float(timestamp))

            # Verifica se o timestamp está em milissegundos (13 dígitos) e converte para segundos se necessário
            if len(str(timestamp)) >= 13:
                timestamp = timestamp / 1000

            return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
        except (ValueError, TypeError, OverflowError) as e:
            # Retorna o valor original se a conversão falhar
            logger.debug(f"Mantendo valor original '{timestamp}' - não é um timestamp Unix")
            return timestamp

    def convert_json_to_csv(data, output_file=None, csv_separator=";"):
        """
        Converte dados JSON de qualquer profundidade para um DataFrame normalizado,
        mantendo as chaves originais (convertidas para minúsculas) e adicionando numeração apenas para itens de lista.
        Também converte campos de timestamp Unix para formato datetime.
        """
        logger.info("Iniciando processamento de dados JSON complexos...")

        # Garante que estamos trabalhando com uma lista de dicionários (registros)
        if isinstance(data, dict):
            records = [data]
        elif isinstance(data, list):
            if all(isinstance(item, dict) for item in data):
                records = data
            else:
                # Envolve itens de lista simples em dicionários
                records = [{"item": item} for item in data]
        else:
            records = [{"value": data}]

        # Função auxiliar recursiva para achatar cada registro
        def flatten_record(data, prefix="", result=None):
            if result is None:
                result = {}

            # Processa com base no tipo de dados
            if isinstance(data, dict):
                # Para dicionários, aninha chaves com underscore e converte para minúsculas
                for key, value in data.items():
                    # Converte chave para minúsculas
                    key = key.lower()
                    new_prefix = f"{prefix}_{key}" if prefix else key
                    if isinstance(value, (dict, list)):
                        flatten_record(value, new_prefix, result)
                    else:
                        result[new_prefix] = value

            elif isinstance(data, list):
                # Para listas, numera os itens
                for i, item in enumerate(data, 1):
                    if isinstance(item, dict):
                        # Para dicionários dentro de listas
                        for sub_key, sub_value in item.items():
                            # Converte sub-chave para minúsculas
                            sub_key = sub_key.lower()
                            new_prefix = f"{prefix}_{sub_key}_{i}" if prefix else f"{sub_key}_{i}"
                            if isinstance(sub_value, (dict, list)):
                                flatten_record(sub_value, new_prefix, result)
                            else:
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
            flattened_record = flatten_record(record)
            flattened_records.append(flattened_record)

            # Registra progresso para grandes conjuntos de dados
            if (i + 1) % progress_log == 0 or (i + 1) == len(records):
                logger.info(f"Processados {i + 1}/{len(records)} registros ({((i + 1) / len(records)) * 100:.1f}%)")

        # Converte para DataFrame
        logger.info("Convertendo registros json/csv para DataFrame...")
        df = pd.DataFrame(flattened_records)

        # Converte campos de timestamp Unix para datetime
        logger.info("Verificando e convertendo campos de timestamp Unix para datetime...")
        unix_fields = []
        for column in df.columns:
            if is_unix_timestamp_field(column):
                unix_fields.append(column)

        if unix_fields:
            logger.info(f"Encontrados {len(unix_fields)} campos de timestamp para conversão: {unix_fields[:5]}...")

            # Primeiro verifica para cada coluna se a maioria dos valores já está em formato de data
            for column in unix_fields:
                if column in df.columns:
                    # Amostra de valores não nulos para análise
                    sample_values = df[column].dropna().head(50).astype(str)

                    if len(sample_values) == 0:
                        logger.info(f"Coluna {column} não tem valores não-nulos para analisar. Pulando.")
                        continue

                    # Conta quantos valores da amostra já parecem estar em formato de data
                    date_format_count = sum(1 for val in sample_values if is_already_date_format(val))

                    # Se a maioria dos valores já estiver em formato de data, pula esta coluna
                    if date_format_count > len(sample_values) / 2:
                        logger.info(f"Coluna {column} já parece estar em formato de data. Pulando conversão.")
                        continue

                    # Se precisar converter, faz a conversão com a função melhorada
                    logger.info(f"Convertendo coluna {column} de timestamp Unix para datetime...")
                    df[column] = df[column].apply(convert_unix_to_datetime)
        else:
            logger.info("Nenhum campo de timestamp identificado para conversão.")

        # Salva em arquivo se o caminho for fornecido
        if output_file:
            try:
                df.to_csv(output_file, index=False, sep=csv_separator)
                logger.info(f"Arquivo CSV normalizado criado: {output_file} com {len(df.columns)} colunas")
            except Exception as e:
                logger.error(f"Erro ao salvar CSV: {e}")

        logger.info(f"Processamento finalizado. DataFrame criado com {len(df)} linhas e {len(df.columns)} colunas")
        return df

    def upload_csv_to_bucket(data, bucket_name, blob_name, folder_path=None, separator=";"):
        """
        Envia um DataFrame como CSV para um bucket GCP
        """
        try:
            # Inicializa o cliente de armazenamento
            credentials = gcs.load_credentials_from_env()

            # Cria o caminho completo do blob incluindo a pasta, se especificada
            full_blob_path = blob_name
            if folder_path:
                # Remove barras no início e adiciona barra no final, se necessário
                clean_folder = folder_path.strip('/')
                if clean_folder:
                    full_blob_path = f"{clean_folder}/{blob_name}"

            # Converte DataFrame para CSV em memória com tratamento de erros
            try:
                # Tenta converter usando o modo padrão com o separador especificado
                csv_string = data.to_csv(index=False, sep=separator)
            except Exception as csv_error:
                logger.warning(f"Erro na conversão padrão para CSV: {csv_error}")
                logger.info("Tentando converter com método alternativo...")

                # Método alternativo com mais segurança para tipos complexos
                csv_string = data.replace({pd.NA: None}).to_csv(index=False, na_rep='', sep=separator)

            # Cria arquivo temporário
            local_file_path = f"/tmp/{customer['project_id']}_{blob_name}"
            with open(local_file_path, 'w', encoding='utf-8') as f:
                f.write(csv_string)

            # Envia para o GCP
            gcs.write_file_to_gcs(
                bucket_name=bucket_name,
                local_file_path=local_file_path,
                destination_name=full_blob_path,
                credentials=credentials
            )

            # Informações adicionais úteis
            size_kb = len(csv_string) / 1024
            logger.info(f"Arquivo '{full_blob_path}' ({size_kb:.2f} KB) enviado com sucesso para o bucket '{bucket_name}'")
            return True

        except Exception as e:
            logger.error(f"Erro ao enviar arquivo para GCP: {e}")
            raise

    # Função principal
    try:
        start_time = time.time()
        record_count = 0

        # Define data inicial e máxima (hoje)
        start_date = INITIAL_DATE
        end_date = datetime.now().strftime("%Y-%m-%d")

        # Gera intervalos de até 180 dias
        date_ranges = generate_date_ranges(start_date, end_date, MAX_DAYS_INTERVAL)
        logger.info(f"Coletando dados em {len(date_ranges)} intervalos de data")

        # Processa intervalos de data em paralelo
        all_data = []
        intervals_processed = 0

        # Usa um objeto compartilhado para armazenar resultados
        results_lock = Lock()

        # Melhorando o paralelismo real para processar todos os intervalos simultaneamente
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(date_ranges))) as executor:
            # Prepara todas as tarefas de uma vez
            futures = [executor.submit(process_date_range, date_range) for date_range in date_ranges]

            # Processa os resultados à medida que eles forem completados
            for future in concurrent.futures.as_completed(futures):
                # Determina qual intervalo de data está associado ao future que terminou
                for i, f in enumerate(futures):
                    if f == future:
                        date_range = date_ranges[i]
                        break
                else:
                    # Caso não encontre, continue (não deve acontecer)
                    continue

                start_range, end_range = date_range

                try:
                    interval_data = future.result()

                    # Adicionando thread safety para o objeto compartilhado
                    with results_lock:
                        all_data.extend(interval_data)
                        intervals_processed += 1
                        record_count += len(interval_data)

                    logger.info(f"Processado intervalo {start_range} a {end_range}: {len(interval_data)} registros. " +
                                f"Total: {record_count} registros em {intervals_processed}/{len(date_ranges)} intervalos")

                except Exception as e:
                    logger.error(f"Erro ao processar intervalo {start_range} a {end_range}: {e}")

        # Verifica se há dados para processar
        if not all_data:
            logger.warning("Nenhum registro encontrado em todo o período")
            return "Nenhum registro encontrado"

        # Usando função de normalização para lidar com dados JSON
        logger.info(f"Convertendo {len(all_data)} registros de dados JSON complexos...")

        # Achata dados JSON com a função aprimorada que trata conversão de timestamp
        df = convert_json_to_csv(all_data)
        logger.info(f"Dados JSON convertidos com sucesso para formato tabular. Total de colunas: {len(df.columns)}")

        # Envia para o GCP com credenciais e pasta
        upload_csv_to_bucket(
            data=df,
            bucket_name=GCP_BUCKET_NAME,
            blob_name=CSV_FILENAME,
            folder_path=GCP_FOLDER_PATH,
            separator=";"
        )

        elapsed_time = time.time() - start_time
        result_message = f"Processamento concluído com sucesso em {elapsed_time:.2f} segundos. {len(all_data)} registros exportados em formato CSV normalizado."
        logger.info(result_message)
        return result_message

    except Exception as e:
        error_message = f"Erro durante o processamento: {str(e)}"
        logger.error(error_message)
        return error_message


def run_subscriptions(customer):
    """
    Extract subscriptions data from Digital Manager Guru API and upload to GCS.
    
    Args:
        customer (dict): Customer configuration with API credentials and settings
    """
    import concurrent.futures
    import logging
    import os
    import re
    import time
    from datetime import datetime, timedelta
    from queue import Queue, Empty
    from threading import Lock

    import pandas as pd
    import requests
    from google.cloud import storage

    # CONFIG DE LOGGING
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger("DigitalManagerGCP")

    # GCP
    GENERIC_NAME = "subscriptions"
    GCP_BUCKET_NAME = customer['bucket_name']
    GCP_FOLDER_PATH = GENERIC_NAME
    
    # ARQUIVO CSV
    CSV_FILENAME = f"{GENERIC_NAME}.csv"

    # API
    API_BASE_URL = "https://digitalmanager.guru/api/v2"
    API_TOKEN = customer['token']
    ROUTER = GENERIC_NAME
    AUTH = "Bearer"
    API_RATE_LIMIT = 360
    MAX_DAYS_INTERVAL = 180
    INITIAL_DATE = "2024-01-01"

    # CONFIG DE PARALELISMO
    MAX_WORKERS = 10
    REQUEST_COUNTER_LOCK = Lock()
    PROCESSED_CURSORS_LOCK = Lock()
    request_counter = 0
    reset_time = time.time() + 60
    processed_cursors = set()

    def check_rate_limit():
        """
        Controla o limite de requisições por minuto usando um contador global sincronizado
        Retorna True se estiver dentro do limite, False caso contrário
        """
        nonlocal request_counter, reset_time

        with REQUEST_COUNTER_LOCK:
            current_time = time.time()

            # Reseta o contador se 1 minuto tiver passado
            if current_time > reset_time:
                request_counter = 0
                reset_time = current_time + 60

            # Verifica se o limite foi atingido
            if request_counter >= API_RATE_LIMIT:
                return False

            # Incrementa o contador e retorna
            request_counter += 1
            return True

    def fetch_api_data(cursor=None, params=None):
        """
        Busca dados da API com suporte à paginação baseada em cursor
        """
        headers = {
            "Authorization": f"{AUTH} {API_TOKEN}",
            "accept": "application/json"
        }

        url = f"{API_BASE_URL}/{ROUTER}"

        # Prepara os parâmetros
        request_params = params.copy() if params else {}
        if cursor:
            request_params["cursor"] = cursor

        # Faz a requisição com controle de limite de taxa
        max_retries = 5
        retries = 0

        while retries < max_retries:
            if check_rate_limit():
                try:
                    response = requests.get(url, headers=headers, params=request_params)
                    response.raise_for_status()
                    return response.json()

                except requests.exceptions.HTTPError as e:
                    if response.status_code == 429:
                        retries += 1
                        wait_time = 5 * retries  # Backoff exponencial
                        logger.warning(
                            f"Limite de requisições excedido (429). Tentativa {retries}/{max_retries}. Aguardando {wait_time}s...")
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
                    logger.error(f"Erro na requisição: {e}. Tentativa {retries}/{max_retries}. Aguardando {wait_time}s...")
                    time.sleep(wait_time)
                    continue
            else:
                # Se estiver no limite de taxa, espera antes de tentar novamente
                time.sleep(1)

        logger.error(f"Falha após {max_retries} tentativas para cursor {cursor}")
        raise Exception(f"Falha após {max_retries} tentativas para cursor {cursor}")

    def process_page(params, cursor, cursor_queue, results_list, processed_cursors_set):
        """
        Processa uma página específica e encadeia para a próxima página
        """
        # Evita processar o mesmo cursor duas vezes
        with PROCESSED_CURSORS_LOCK:
            if cursor in processed_cursors_set:
                logger.debug(f"Cursor {cursor[:15]}... já foi processado. Pulando.")
                return None
            processed_cursors_set.add(cursor)

        try:
            logger.debug(f"Buscando página com cursor {cursor[:15]}...")
            response = fetch_api_data(cursor, params)

            # Adiciona resultados à lista compartilhada
            if "data" in response and response["data"]:
                results_list.extend(response["data"])
                logger.info(f"Obtidos {len(response['data'])} registros com cursor {cursor[:15]}...")

            # Encadeia próxima página
            if response.get("has_more_pages", 0) and response.get("next_cursor"):
                next_cursor = response.get("next_cursor")
                with PROCESSED_CURSORS_LOCK:
                    if next_cursor not in processed_cursors_set:
                        cursor_queue.put(next_cursor)
                        logger.debug(f"Adicionado próximo cursor {next_cursor[:15]}... à fila")

            return response
        except Exception as e:
            logger.error(f"Erro ao buscar página com cursor {cursor[:15]}: {e}")
            return None

    def collect_data_for_period(start_date, end_date):
        """
        Coleta todos os dados para um período específico usando processamento paralelo com fila
        """
        logger.info(f"Coletando dados do período: {start_date} até {end_date}")

        params = {
            "ordered_at_ini": start_date,
            "ordered_at_end": end_date
        }

        all_data = []
        cursor_queue = Queue()
        processed_cursors = set()

        # Busca a primeira página para iniciar o processo
        try:
            logger.info(f"Buscando página inicial para o período {start_date} a {end_date}")
            response = fetch_api_data(None, params)

            if "data" in response and response["data"]:
                all_data.extend(response["data"])
                logger.info(f"Obtidos {len(response['data'])} registros na página inicial")

            # Se houver mais páginas, adiciona o cursor à fila
            if response.get("has_more_pages", 0) and response.get("next_cursor"):
                cursor_queue.put(response.get("next_cursor"))
                logger.info(f"Adicionado primeiro cursor à fila para processamento paralelo")
        except Exception as e:
            logger.error(f"Erro ao buscar página inicial: {e}")
            return all_data

        # Se não houver mais páginas, retorna os dados
        if cursor_queue.empty():
            logger.info(f"Apenas uma página de resultados para o período {start_date} a {end_date}")
            return all_data

        # Processa páginas em paralelo usando a fila de cursores
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Lista para rastrear tarefas em execução
            futures = set()

            # Loop principal: processa até que não haja mais cursores na fila e nenhuma tarefa em execução
            while True:
                # Adiciona novas tarefas até atingir o máximo de workers ou esvaziar a fila
                while len(futures) < MAX_WORKERS and not cursor_queue.empty():
                    try:
                        cursor = cursor_queue.get(block=False)
                        future = executor.submit(process_page, params, cursor, cursor_queue, all_data, processed_cursors)
                        futures.add(future)
                    except Empty:
                        break  # Fila vazia

                # Se não houver tarefas em execução, terminamos
                if not futures:
                    break

                # Espera até que pelo menos uma tarefa seja concluída
                done, futures = concurrent.futures.wait(
                    futures,
                    return_when=concurrent.futures.FIRST_COMPLETED
                )

                # Verifica os resultados das tarefas concluídas
                for future in done:
                    try:
                        future.result()  # Apenas para capturar exceções
                    except Exception as e:
                        logger.error(f"Exceção em worker: {e}")

                # Verificação periódica de progresso
                if len(all_data) % 1000 == 0 and len(all_data) > 0:
                    logger.info(f"Progresso: {len(all_data)} registros coletados, {cursor_queue.qsize()} páginas na fila")

        logger.info(
            f"Coleta de dados finalizada para o período {start_date} a {end_date}. Total: {len(all_data)} registros.")
        return all_data

    def process_date_range(date_range):
        """
        Processa um intervalo de datas específico - pode ser chamado em paralelo
        """
        start_date, end_date = date_range
        try:
            return collect_data_for_period(start_date, end_date)
        except Exception as e:
            logger.error(f"Erro ao processar intervalo {start_date} a {end_date}: {e}")
            return []

    def generate_date_ranges(start_date_str, end_date_str=None, max_days=180):
        """
        Gera intervalos de datas com no máximo max_days entre início e fim
        """
        # Converte strings para objetos de data
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")

        # Se nenhuma data final for fornecida, usa a data atual
        if end_date_str:
            end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
        else:
            end_date = datetime.now()

        # Lista para armazenar intervalos
        date_ranges = []

        # Data atual para iteração
        current_start = start_date

        # Enquanto a data atual for anterior à data final
        while current_start < end_date:
            # Calcula a data final do intervalo (máximo de max_days a partir da data atual)
            current_end = min(current_start + timedelta(days=max_days - 1), end_date)

            # Adiciona o intervalo à lista
            date_ranges.append((
                current_start.strftime("%Y-%m-%d"),
                current_end.strftime("%Y-%m-%d")
            ))

            # Atualiza a data atual para o próximo intervalo
            current_start = current_end + timedelta(days=1)

        return date_ranges

    def is_unix_timestamp_field(field_name):
        """
        Verifica se o nome do campo termina com _at ou contém _at_ seguido por números
        """
        # Corresponde a nomes de campos terminando com _at ou contendo _at_ seguido por dígitos
        return bool(re.search(r'_at$', field_name) or re.search(r'_at_\d+', field_name))

    def is_already_date_format(value):
        """
        Verifica se o valor já está em formato de data (YYYY-MM-DD)
        """
        if not isinstance(value, str):
            return False

        # Verifica o padrão YYYY-MM-DD
        date_pattern = re.compile(r'^\d{4}-\d{2}-\d{2}$')

        # Versão estendida que também aceita hora
        datetime_pattern = re.compile(r'^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?$')

        return bool(date_pattern.match(value) or datetime_pattern.match(value))

    def convert_unix_to_datetime(timestamp):
        """
        Converte timestamp Unix para string de datetime
        Trata milissegundos (13 dígitos) e segundos (10 dígitos)
        Preserva valores que já estão em formato de data
        """
        if not timestamp or pd.isna(timestamp):
            return None

        # Se o valor já estiver em formato de data, mantém como está
        if is_already_date_format(str(timestamp)):
            return timestamp

        try:
            # Tenta converter para número primeiro
            if isinstance(timestamp, str):
                # Verifica se é uma string numérica
                if not timestamp.replace('.', '', 1).isdigit():
                    return timestamp

            # Converte para inteiro para lidar com possíveis valores de ponto flutuante
            timestamp = int(float(timestamp))

            # Verifica se o timestamp está em milissegundos (13 dígitos) e converte para segundos se necessário
            if len(str(timestamp)) >= 13:
                timestamp = timestamp / 1000

            return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
        except (ValueError, TypeError, OverflowError) as e:
            # Retorna o valor original se a conversão falhar
            logger.debug(f"Mantendo valor original '{timestamp}' - não é um timestamp Unix")
            return timestamp

    def convert_json_to_csv(data, output_file=None, csv_separator=";"):
        """
        Converte dados JSON de qualquer profundidade para um DataFrame normalizado,
        mantendo as chaves originais (convertidas para minúsculas) e adicionando numeração apenas para itens de lista.
        Também converte campos de timestamp Unix para formato datetime.
        """
        logger.info("Iniciando processamento de dados JSON complexos...")

        # Garante que estamos trabalhando com uma lista de dicionários (registros)
        if isinstance(data, dict):
            records = [data]
        elif isinstance(data, list):
            if all(isinstance(item, dict) for item in data):
                records = data
            else:
                # Envolve itens de lista simples em dicionários
                records = [{"item": item} for item in data]
        else:
            records = [{"value": data}]

        # Função auxiliar recursiva para achatar cada registro
        def flatten_record(data, prefix="", result=None):
            if result is None:
                result = {}

            # Processa com base no tipo de dados
            if isinstance(data, dict):
                # Para dicionários, aninha chaves com underscore e converte para minúsculas
                for key, value in data.items():
                    # Converte chave para minúsculas
                    key = key.lower()
                    new_prefix = f"{prefix}_{key}" if prefix else key
                    if isinstance(value, (dict, list)):
                        flatten_record(value, new_prefix, result)
                    else:
                        result[new_prefix] = value

            elif isinstance(data, list):
                # Para listas, numera os itens
                for i, item in enumerate(data, 1):
                    if isinstance(item, dict):
                        # Para dicionários dentro de listas
                        for sub_key, sub_value in item.items():
                            # Converte sub-chave para minúsculas
                            sub_key = sub_key.lower()
                            new_prefix = f"{prefix}_{sub_key}_{i}" if prefix else f"{sub_key}_{i}"
                            if isinstance(sub_value, (dict, list)):
                                flatten_record(sub_value, new_prefix, result)
                            else:
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
            flattened_record = flatten_record(record)
            flattened_records.append(flattened_record)

            # Registra progresso para grandes conjuntos de dados
            if (i + 1) % progress_log == 0 or (i + 1) == len(records):
                logger.info(f"Processados {i + 1}/{len(records)} registros ({((i + 1) / len(records)) * 100:.1f}%)")

        # Converte para DataFrame
        logger.info("Convertendo registros json/csv para DataFrame...")
        df = pd.DataFrame(flattened_records)

        # Converte campos de timestamp Unix para datetime
        logger.info("Verificando e convertendo campos de timestamp Unix para datetime...")
        unix_fields = []
        for column in df.columns:
            if is_unix_timestamp_field(column):
                unix_fields.append(column)

        if unix_fields:
            logger.info(f"Encontrados {len(unix_fields)} campos de timestamp para conversão: {unix_fields[:5]}...")

            # Primeiro verifica para cada coluna se a maioria dos valores já está em formato de data
            for column in unix_fields:
                if column in df.columns:
                    # Amostra de valores não nulos para análise
                    sample_values = df[column].dropna().head(50).astype(str)

                    if len(sample_values) == 0:
                        logger.info(f"Coluna {column} não tem valores não-nulos para analisar. Pulando.")
                        continue

                    # Conta quantos valores da amostra já parecem estar em formato de data
                    date_format_count = sum(1 for val in sample_values if is_already_date_format(val))

                    # Se a maioria dos valores já estiver em formato de data, pula esta coluna
                    if date_format_count > len(sample_values) / 2:
                        logger.info(f"Coluna {column} já parece estar em formato de data. Pulando conversão.")
                        continue

                    # Se precisar converter, faz a conversão com a função melhorada
                    logger.info(f"Convertendo coluna {column} de timestamp Unix para datetime...")
                    df[column] = df[column].apply(convert_unix_to_datetime)
        else:
            logger.info("Nenhum campo de timestamp identificado para conversão.")

        # Salva em arquivo se o caminho for fornecido
        if output_file:
            try:
                df.to_csv(output_file, index=False, sep=csv_separator)
                logger.info(f"Arquivo CSV normalizado criado: {output_file} com {len(df.columns)} colunas")
            except Exception as e:
                logger.error(f"Erro ao salvar CSV: {e}")

        logger.info(f"Processamento finalizado. DataFrame criado com {len(df)} linhas e {len(df.columns)} colunas")
        return df

    def upload_csv_to_bucket(data, bucket_name, blob_name, folder_path=None, separator=";"):
        """
        Envia um DataFrame como CSV para um bucket GCP
        """
        try:
            # Inicializa o cliente de armazenamento
            credentials = gcs.load_credentials_from_env()

            # Cria o caminho completo do blob incluindo a pasta, se especificada
            full_blob_path = blob_name
            if folder_path:
                # Remove barras no início e adiciona barra no final, se necessário
                clean_folder = folder_path.strip('/')
                if clean_folder:
                    full_blob_path = f"{clean_folder}/{blob_name}"

            # Converte DataFrame para CSV em memória com tratamento de erros
            try:
                # Tenta converter usando o modo padrão com o separador especificado
                csv_string = data.to_csv(index=False, sep=separator)
            except Exception as csv_error:
                logger.warning(f"Erro na conversão padrão para CSV: {csv_error}")
                logger.info("Tentando converter com método alternativo...")

                # Método alternativo com mais segurança para tipos complexos
                csv_string = data.replace({pd.NA: None}).to_csv(index=False, na_rep='', sep=separator)

            # Cria arquivo temporário
            local_file_path = f"/tmp/{customer['project_id']}_{blob_name}"
            with open(local_file_path, 'w', encoding='utf-8') as f:
                f.write(csv_string)

            # Envia para o GCP
            gcs.write_file_to_gcs(
                bucket_name=bucket_name,
                local_file_path=local_file_path,
                destination_name=full_blob_path,
                credentials=credentials
            )

            # Informações adicionais úteis
            size_kb = len(csv_string) / 1024
            logger.info(f"Arquivo '{full_blob_path}' ({size_kb:.2f} KB) enviado com sucesso para o bucket '{bucket_name}'")
            return True

        except Exception as e:
            logger.error(f"Erro ao enviar arquivo para GCP: {e}")
            raise

    # Função principal
    try:
        start_time = time.time()
        record_count = 0

        # Define data inicial e máxima (hoje)
        start_date = INITIAL_DATE
        end_date = datetime.now().strftime("%Y-%m-%d")

        # Gera intervalos de até 180 dias
        date_ranges = generate_date_ranges(start_date, end_date, MAX_DAYS_INTERVAL)
        logger.info(f"Coletando dados em {len(date_ranges)} intervalos de data")

        # Processa intervalos de data em paralelo
        all_data = []
        intervals_processed = 0

        # Usa um objeto compartilhado para armazenar resultados
        results_lock = Lock()

        # Melhorando o paralelismo real para processar todos os intervalos simultaneamente
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(date_ranges))) as executor:
            # Prepara todas as tarefas de uma vez
            futures = [executor.submit(process_date_range, date_range) for date_range in date_ranges]

            # Processa os resultados à medida que eles forem completados
            for future in concurrent.futures.as_completed(futures):
                # Determina qual intervalo de data está associado ao future que terminou
                for i, f in enumerate(futures):
                    if f == future:
                        date_range = date_ranges[i]
                        break
                else:
                    # Caso não encontre, continue (não deve acontecer)
                    continue

                start_range, end_range = date_range

                try:
                    interval_data = future.result()

                    # Adicionando thread safety para o objeto compartilhado
                    with results_lock:
                        all_data.extend(interval_data)
                        intervals_processed += 1
                        record_count += len(interval_data)

                    logger.info(f"Processado intervalo {start_range} a {end_range}: {len(interval_data)} registros. " +
                                f"Total: {record_count} registros em {intervals_processed}/{len(date_ranges)} intervalos")

                except Exception as e:
                    logger.error(f"Erro ao processar intervalo {start_range} a {end_range}: {e}")

        # Verifica se há dados para processar
        if not all_data:
            logger.warning("Nenhum registro encontrado em todo o período")
            return "Nenhum registro encontrado"

        # Usando função de normalização para lidar com dados JSON
        logger.info(f"Convertendo {len(all_data)} registros de dados JSON complexos...")

        # Achata dados JSON com a função aprimorada que trata conversão de timestamp
        df = convert_json_to_csv(all_data)
        logger.info(f"Dados JSON convertidos com sucesso para formato tabular. Total de colunas: {len(df.columns)}")

        # Envia para o GCP com credenciais e pasta
        upload_csv_to_bucket(
            data=df,
            bucket_name=GCP_BUCKET_NAME,
            blob_name=CSV_FILENAME,
            folder_path=GCP_FOLDER_PATH,
            separator=";"
        )

        elapsed_time = time.time() - start_time
        result_message = f"Processamento concluído com sucesso em {elapsed_time:.2f} segundos. {len(all_data)} registros exportados em formato CSV normalizado."
        logger.info(result_message)
        return result_message

    except Exception as e:
        error_message = f"Erro durante o processamento: {str(e)}"
        logger.error(error_message)
        return error_message


def get_extraction_tasks():
    """
    Get the list of data extraction tasks for Digital Manager Guru.
    
    Returns:
        list: List of task configurations
    """
    return [
        {
            'task_id': 'run_contacts',
            'python_callable': run_contacts
        },
        {
            'task_id': 'run_transactions',
            'python_callable': run_transactions
        },
        {
            'task_id': 'run_subscriptions',
            'python_callable': run_subscriptions
        }
    ]
