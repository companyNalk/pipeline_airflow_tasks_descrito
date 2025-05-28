from core import gcs


def run_bills(customer):
    import base64
    import concurrent.futures
    import logging
    import os
    import re
    import sys
    import time
    from queue import Queue, Empty
    from threading import Lock

    import pandas as pd
    import requests
    from dotenv import load_dotenv
    from google.cloud import storage
    import pathlib
    load_dotenv()

    # LOGGING
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger("VindiERPGCP")

    GENERIC_NAME = os.path.splitext(os.path.basename(sys.argv[0]))[0]

    # GCP
    BUCKET_NAME = customer['bucket_name']
    FOLDER_PATH = GENERIC_NAME
    GOOGLE_APPLICATION_CREDENTIALS = pathlib.Path('config', 'gcp.json')
    GENERIC_NAME = os.path.splitext(os.path.basename(sys.argv[0]))[0]
    CSV_FILENAME = f"{GENERIC_NAME}.csv"

    # APP
    CSV_FILENAME = f"{GENERIC_NAME}.csv"

    # API
    API_BASE_URL = customer['api_base_url']
    API_TOKEN = customer['api_token']
    AUTH_TYPE = "Basic"
    ROUTER = GENERIC_NAME
    API_RATE_LIMIT = 100  # Reduced from 120 to be safer
    MAX_PER_PAGE = 50  # Maximum records per page for Vindi API

    # PARALLELISM
    MAX_WORKERS = 2
    REQUEST_COUNTER_LOCK = Lock()
    request_counter = 0
    reset_time = time.time() + 60
    THREAD_DELAY = 0.5  # Increased delay between requests (in seconds)
    BACKOFF_MULTIPLIER = 1.5  # Multiplier for dynamic backoff

    def check_rate_limit():
        """
        Controls the rate limit of requests per minute using a synchronized global counter
        Returns True if within the limit, False otherwise
        """
        global request_counter, reset_time

        # If no rate limit is set, always return True
        if API_RATE_LIMIT is None:
            return True

        with REQUEST_COUNTER_LOCK:
            current_time = time.time()

            # Reset counter if 1 minute has passed
            if current_time > reset_time:
                request_counter = 0
                reset_time = current_time + 60

            # Check if limit reached
            if request_counter >= API_RATE_LIMIT:
                return False

            # Increment counter and return
            request_counter += 1
            return True

    def fetch_api_data(endpoint=None, params=None, page_url=None):
        """
        Fetches data from the Vindi API with support for header-based pagination
        """
        # Basic Auth: base64 encode the API token with empty password
        auth_string = base64.b64encode(f"{API_TOKEN}:".encode()).decode()

        headers = {
            "Authorization": f"{AUTH_TYPE} {auth_string}",
            "accept": "application/json"
        }

        # Determine the URL to use
        if page_url:
            # Use the provided full URL (for page-based pagination)
            url = page_url
        else:
            # Construct URL from base and endpoint
            api_endpoint = endpoint if endpoint else GENERIC_NAME
            url = f"{API_BASE_URL}/{api_endpoint}"

        # Prepare request parameters
        request_params = params.copy() if params else {}

        # Always request max per page if not specified
        if 'per_page' not in request_params:
            request_params['per_page'] = MAX_PER_PAGE

        # Make request with retry logic
        max_retries = 5
        retries = 0
        backoff_time = THREAD_DELAY

        while retries < max_retries:
            if check_rate_limit():
                try:
                    # Dynamic delay before making request
                    time.sleep(backoff_time)

                    response = requests.get(url, headers=headers, params=request_params)
                    response.raise_for_status()

                    # After successful request, reduce backoff time
                    # but keep it at least at THREAD_DELAY
                    global BACKOFF_MULTIPLIER
                    BACKOFF_MULTIPLIER = max(1.0, BACKOFF_MULTIPLIER * 0.9)

                    # Return both the JSON response and the headers for pagination
                    return {
                        'data': response.json(),
                        'headers': {
                            'total': response.headers.get('Total'),
                            'per_page': response.headers.get('Per-Page'),
                            'link': response.headers.get('Link')
                        },
                        'url': url  # Include the URL for tracking
                    }

                except requests.exceptions.HTTPError as e:
                    if response.status_code == 429:
                        retries += 1
                        # Increase backoff time with each 429 error
                        BACKOFF_MULTIPLIER *= 1.2
                        wait_time = 5 * retries * BACKOFF_MULTIPLIER  # Dynamic exponential backoff
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
                # If at rate limit, wait before trying again
                logger.debug("Rate limit atingido, aguardando 1 segundo...")
                time.sleep(1)

        logger.error(f"Falha após {max_retries} tentativas para URL: {url}")
        raise Exception(f"Falha após {max_retries} tentativas para URL: {url}")

    def extract_all_urls_from_link_header(link_header):
        """
        Extracts all URLs from the Link header, returning a dictionary of URLs by relation
        Example Link header:
        <https://app.vindi.com.br/api/v1/customers?page=3>; rel="last", <https://app.vindi.com.br/api/v1/customers?page=2>; rel="next"
        """
        if not link_header:
            return {}

        # Find all URL and relation pairs
        links = {}
        pattern = r'<([^>]+)>;\s*rel="([^"]+)"'
        matches = re.findall(pattern, link_header)

        for url, rel in matches:
            links[rel] = url

        return links

    def extract_page_number(url):
        """
        Extracts the page number from a URL for logging purposes
        """
        if not url:
            return "1"

        match = re.search(r'page=(\d+)', url)
        if match:
            return match.group(1)
        return "1"

    def generate_sequential_urls(base_pattern, start_page, end_page):
        """
        Generate a sequence of URLs for pages from start_page to end_page using the base pattern
        """
        urls = []
        for page in range(start_page, end_page + 1):
            url = base_pattern.format(page=page)
            urls.append(url)
        return urls

    def generate_page_urls(link_header, total_pages=None):
        """
        Generate URLs for a range of pages based on the link header pattern
        """
        if not link_header:
            return []

        # Extract URLs from link header
        links = extract_all_urls_from_link_header(link_header)

        # If we don't have last or next, we can't generate URLs
        if 'last' not in links and 'next' not in links:
            return []

        # Extract base URL pattern by replacing page number with placeholder
        base_url = None
        if 'next' in links:
            url = links['next']
            base_url = re.sub(r'page=\d+', 'page={page}', url)
        elif 'last' in links:
            url = links['last']
            base_url = re.sub(r'page=\d+', 'page={page}', url)

        if not base_url:
            return []

        # Determine range of pages to generate
        urls = []
        if 'last' in links and total_pages is None:
            last_page = int(extract_page_number(links['last']))
            current_page = int(extract_page_number(links.get('self', ''))) if 'self' in links else 1
            next_page = int(extract_page_number(links.get('next', ''))) if 'next' in links else current_page + 1

            # Generate URLs for the next batch of pages - REDUCED batch size
            max_pages_to_generate = 10  # Limit to avoid generating too many URLs at once
            end_page = min(last_page, next_page + max_pages_to_generate)

            urls = generate_sequential_urls(base_url, next_page, end_page)
        elif total_pages:
            current_page = int(extract_page_number(links.get('self', ''))) if 'self' in links else 1
            next_page = int(extract_page_number(links.get('next', ''))) if 'next' in links else current_page + 1

            # Generate URLs up to the specified total pages
            max_pages = min(total_pages, next_page + 10)  # Limit batch size
            urls = generate_sequential_urls(base_url, next_page, max_pages)

        return urls

    def convert_json_to_csv(data, output_file=None, csv_separator=";", id_field='id', remove_empty_columns=True):
        """
        Converts JSON data of any depth to a normalized DataFrame,
        keeping original keys (converted to lowercase) and adding numbering only for list items.
        Removes columns where all records have no value.
        """
        logger.info("Iniciando processamento de dados JSON complexos...")

        # Ensure we're working with a list of dictionaries (records)
        if isinstance(data, dict):
            records = [data]
        elif isinstance(data, list):
            if all(isinstance(item, dict) for item in data):
                records = data
            else:
                # Wrap simple list items in dictionaries
                records = [{"item": item} for item in data]
        else:
            records = [{"value": data}]

        # Verify dataset for completeness
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

        # Recursive helper function to flatten each record
        def json_to_csv(data, prefix="", result=None):
            if result is None:
                result = {}

            # Process based on data type
            if isinstance(data, dict):
                # For dictionaries, nest keys with underscore and convert to lowercase
                for key, value in data.items():
                    # Convert keys to lowercase
                    key = key.lower()
                    new_prefix = f"{prefix}_{key}" if prefix else key
                    if isinstance(value, (dict, list)):
                        json_to_csv(value, new_prefix, result)
                    else:
                        result[new_prefix] = value

            elif isinstance(data, list):
                # For lists, number the items
                for i, item in enumerate(data, 1):
                    if isinstance(item, dict):
                        # For dictionaries within lists
                        for sub_key, sub_value in item.items():
                            # Convert keys to lowercase
                            sub_key = sub_key.lower()
                            new_prefix = f"{prefix}_{sub_key}_{i}" if prefix else f"{sub_key}_{i}"
                            if isinstance(sub_value, (dict, list)):
                                json_to_csv(sub_value, new_prefix, result)
                            else:
                                result[new_prefix] = sub_value
                    else:
                        # For simple values within lists
                        result[f"{prefix}_{i}" if prefix else f"{prefix}{i}"] = item

            return result

        # Process each record
        logger.info(f"Processando {len(records)} registros JSON...")
        flattened_records = []

        # For large amounts of data, show progress
        progress_log = max(1, len(records) // 10)

        for i, record in enumerate(records):
            flattened_record = json_to_csv(record)
            flattened_records.append(flattened_record)

            # Log progress for large data sets
            if (i + 1) % progress_log == 0 or (i + 1) == len(records):
                logger.info(f"Processados {i + 1}/{len(records)} registros ({((i + 1) / len(records)) * 100:.1f}%)")

        # Convert to DataFrame
        logger.info("Convertendo registros json/csv para DataFrame...")
        df = pd.DataFrame(flattened_records)

        # Converter colunas de ID para inteiros (mantendo NaN onde apropriado)
        for col in df.columns:
            # Verificar se o nome da coluna contém "_id" ou termina com "id"
            if '_id' in col or col.endswith('id'):
                try:
                    # Verificar se a coluna contém valores que podem ser convertidos para números
                    # Primeiro tentar converter para ver se é possível
                    numeric_values = pd.to_numeric(df[col], errors='coerce')

                    # Se mais de 80% dos valores não-nulos foram convertidos com sucesso, então é uma coluna ID numérica
                    non_null_count = df[col].count()
                    successful_conversions = numeric_values.count()

                    # Evitar divisão por zero
                    if non_null_count > 0 and (successful_conversions / non_null_count) >= 0.8:
                        # Usar dtype='Int64' (com I maiúsculo) - é o tipo nullable integer do pandas
                        # que permite valores NaN junto com inteiros
                        df[col] = numeric_values.astype('Int64')
                        logger.info(f"Coluna {col} convertida para inteiro")
                    else:
                        logger.info(f"Coluna {col} contém valores não numéricos, mantida como está")
                except Exception as e:
                    logger.warning(f"Não foi possível converter a coluna {col} para inteiro: {e}")

        # Count total columns before removing any
        total_columns = len(df.columns)
        logger.info(f"Total de colunas antes da filtragem: {total_columns}")

        # Remove columns where all values are null/NaN
        before_count = len(df.columns)

        if remove_empty_columns:
            df = df.dropna(axis=1, how='all')

        after_count = len(df.columns)
        removed_count = before_count - after_count

        # Log the results of column filtering
        logger.info(f"Remoção de colunas sem valores: {removed_count} colunas removidas de um total de {before_count}")
        if removed_count > 0:
            percentage_removed = (removed_count / before_count) * 100
            logger.info(f"Removidas {removed_count} colunas sem valores ({percentage_removed:.1f}% das colunas)")

        # Save to file if path is provided
        if output_file:
            try:
                df.to_csv(output_file, index=False, sep=csv_separator)
                logger.info(f"Arquivo CSV normalizado criado: {output_file} com {len(df.columns)} colunas")
            except Exception as e:
                logger.error(f"Erro ao salvar CSV: {e}")
                raise

        logger.info(f"Processamento finalizado. DataFrame criado com {len(df)} linhas e {len(df.columns)} colunas")

        # Additional logging for columns analysis
        logger.info(
            f"Análise de colunas: {total_columns} colunas totais, {removed_count} removidas, {after_count} mantidas")

        return df

    def upload_csv_to_bucket(data, bucket_name, blob_name, folder_path=None, credentials_path=None, separator=";"):
        """
        Uploads a DataFrame as CSV to a GCP bucket
        """
        try:
            # Configure credentials if path is provided
            if credentials_path and os.path.exists(credentials_path):
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
                logger.info(f"Usando credenciais do arquivo: {credentials_path}")
            else:
                logger.info("Usando credenciais padrão do ambiente")

            # Initialize storage client
            client = storage.Client()

            # Get bucket
            bucket = client.bucket(bucket_name)

            # Create full blob path including folder if specified
            full_blob_path = blob_name
            if folder_path:
                # Remove slashes at beginning and add slash at end if needed
                clean_folder = folder_path.strip('/')
                if clean_folder:
                    full_blob_path = f"{clean_folder}/{blob_name}"

            # Create blob
            blob = bucket.blob(full_blob_path)

            # Convert DataFrame to CSV in memory with error handling
            try:
                # Try to convert using standard mode with specified separator
                csv_string = data.to_csv(index=False, sep=separator)
            except Exception as csv_error:
                logger.warning(f"Erro na conversão padrão para CSV: {csv_error}")
                logger.info("Tentando converter com método alternativo...")

                # Alternative method with more safety for complex types
                csv_string = data.replace({pd.NA: None}).to_csv(index=False, na_rep='', sep=separator)

            # Upload to GCP
            blob.upload_from_string(csv_string, content_type="text/csv")

            # Additional useful information
            size_kb = len(csv_string) / 1024
            row_count = len(data)
            logger.info(
                f"Arquivo '{full_blob_path}' ({size_kb:.2f} KB) com {row_count} linhas enviado com sucesso para o bucket '{bucket_name}'")
            return True

        except Exception as e:
            logger.error(f"Erro ao enviar arquivo para GCP: {e}")
            raise

    def collect_vindi_data_complete(params=None):
        """
        Collects all data from the Vindi API with verification to ensure completeness
        """
        logger.info("Coletando dados da API Vindi com verificação de completude...")

        all_data = []
        data_lock = Lock()  # Lock for thread-safe data accumulation
        url_queue = Queue()
        processed_pages = set()
        pages_lock = Lock()

        # For record tracking
        record_ids = set()  # Track unique record IDs
        id_field = 'id'  # Default ID field

        # Progress tracking
        progress_tracker = {
            'total_records': 0,
            'processed_pages': 0,
            'last_update': time.time()
        }

        # Prepare initial parameters - ensure max per page
        request_params = params.copy() if params else {}
        if 'per_page' not in request_params:
            request_params['per_page'] = MAX_PER_PAGE

        # Fetch first page
        try:
            response = fetch_api_data(endpoint=GENERIC_NAME, params=request_params)

            # Extract data - Vindi usually has a wrapper with the entity name
            data_wrapper = response['data']
            entity_data = []

            # Find the actual data array
            entity_data_key = None

            if isinstance(data_wrapper, dict):
                # Try to get data from the resource key (e.g., 'customers', 'subscriptions')
                if GENERIC_NAME in data_wrapper:
                    entity_data = data_wrapper[GENERIC_NAME]
                    entity_data_key = GENERIC_NAME
                else:
                    # If not found under resource name, take the first list found
                    for key, value in data_wrapper.items():
                        if isinstance(value, list):
                            entity_data = value
                            entity_data_key = key
                            break
            elif isinstance(data_wrapper, list):
                entity_data = data_wrapper

            # Log what we found
            if entity_data_key:
                logger.info(f"Dados encontrados sob a chave: '{entity_data_key}'")

            if entity_data:
                # Detect ID field if possible
                if entity_data and isinstance(entity_data[0], dict):
                    potential_id_fields = ['id', 'uuid', 'code']
                    for field in potential_id_fields:
                        if field in entity_data[0]:
                            id_field = field
                            logger.info(f"Campo de ID detectado: '{id_field}'")
                            break

                # Track unique IDs and add data to main list
                for record in entity_data:
                    if isinstance(record, dict) and id_field in record:
                        record_ids.add(record[id_field])

                all_data.extend(entity_data)
                logger.info(f"Obtidos {len(entity_data)} registros na página inicial")

            # Log total if available
            total_count = 0
            if response['headers']['total']:
                total_count = int(response['headers']['total'])
                per_page = int(response['headers']['per_page']) if response['headers']['per_page'] else MAX_PER_PAGE
                logger.info(f"Total de registros na API: {total_count} (max {per_page} por página)")
                progress_tracker['total_records'] = total_count

            # Get pagination info from Link header
            link_header = response['headers']['link']

            # Extract all link relations
            links = extract_all_urls_from_link_header(link_header)

            # If we have "last" link, try to determine total number of pages
            total_pages = 0
            if 'last' in links:
                last_page_url = links['last']
                last_page_num = int(extract_page_number(last_page_url))
                total_pages = last_page_num
                logger.info(f"Total de páginas a coletar: {total_pages}")

                # Extract the base URL pattern for page generation
                base_url_pattern = re.sub(r'page=\d+', 'page={page}', last_page_url)

                # Generate ALL page URLs to ensure we don't miss any
                # Start from page 2 since we already processed page 1
                all_urls = generate_sequential_urls(base_url_pattern, 2, total_pages)

                # Add ALL URLs to the queue
                for url in all_urls:
                    url_queue.put(url)

                logger.info(f"Enfileiradas {len(all_urls)} páginas para processamento")

        except Exception as e:
            logger.error(f"Erro ao buscar página inicial: {e}")
            return all_data

        # If no more pages, return data
        if url_queue.empty():
            logger.info("Apenas uma página de resultados")
            return all_data

        # Define page processing function
        def process_vindi_page(page_url):
            # Extract page number for logging
            page_num = extract_page_number(page_url)

            # Avoid processing the same page twice
            with pages_lock:
                if page_url in processed_pages:
                    logger.debug(f"Página {page_num} já foi processada. Pulando.")
                    return None
                processed_pages.add(page_url)

            try:
                logger.debug(f"Buscando página {page_num}...")
                response = fetch_api_data(page_url=page_url)

                # Extract data similar to first page
                data_wrapper = response['data']
                entity_data = []

                if isinstance(data_wrapper, dict):
                    if entity_data_key and entity_data_key in data_wrapper:
                        entity_data = data_wrapper[entity_data_key]
                    elif GENERIC_NAME in data_wrapper:
                        entity_data = data_wrapper[GENERIC_NAME]
                    else:
                        for key, value in data_wrapper.items():
                            if isinstance(value, list):
                                entity_data = value
                                break
                elif isinstance(data_wrapper, list):
                    entity_data = data_wrapper

                # Record IDs for tracking
                new_records = 0
                duplicate_records = 0

                # Track unique records
                with data_lock:
                    for record in entity_data:
                        if isinstance(record, dict) and id_field in record:
                            if record[id_field] not in record_ids:
                                record_ids.add(record[id_field])
                                new_records += 1
                            else:
                                duplicate_records += 1

                    # Add data to main collection
                    all_data.extend(entity_data)

                    # Update progress tracking
                    progress_tracker['processed_pages'] += 1

                if duplicate_records > 0:
                    logger.warning(f"Página {page_num}: {duplicate_records} registros duplicados encontrados")

                logger.info(f"Obtidos {len(entity_data)} registros na página {page_num} ({new_records} novos)")

                return entity_data
            except Exception as e:
                logger.error(f"Erro ao processar página {page_num}: {e}")
                return None

        # Process pages in parallel using a fixed thread pool
        logger.info(f"Iniciando processamento paralelo com {MAX_WORKERS} workers")

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Create a dict to track futures with their URLs
            futures_to_url = {}

            # Function to add new tasks
            def add_tasks():
                while len(futures_to_url) < MAX_WORKERS and not url_queue.empty():
                    try:
                        page_url = url_queue.get(block=False)
                        if page_url not in processed_pages:
                            future = executor.submit(process_vindi_page, page_url)
                            futures_to_url[future] = page_url
                    except Empty:
                        break

            # Initial task loading - start conservatively
            initial_tasks = min(MAX_WORKERS // 2, url_queue.qsize())
            for _ in range(initial_tasks):
                if not url_queue.empty():
                    page_url = url_queue.get()
                    if page_url not in processed_pages:
                        future = executor.submit(process_vindi_page, page_url)
                        futures_to_url[future] = page_url

            # Process until no more URLs and no active tasks
            consecutive_errors = 0
            last_progress_report = time.time()

            while futures_to_url or not url_queue.empty():
                # Wait for tasks to complete
                done, _ = concurrent.futures.wait(
                    futures_to_url,
                    return_when=concurrent.futures.FIRST_COMPLETED,
                    timeout=5
                )

                # Process completed tasks
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

                # Add new tasks
                add_tasks()

                # Progress reporting
                current_time = time.time()
                if current_time - last_progress_report > 15:  # Report progress every 15 seconds
                    last_progress_report = current_time

                    # Calculate completion percentage if we know total pages
                    completion_str = ""
                    if progress_tracker['total_records'] > 0:
                        percent_complete = (len(all_data) / progress_tracker['total_records']) * 100
                        completion_str = f" - {percent_complete:.1f}% concluído"

                    # Report progress
                    logger.info(f"Progresso: {len(all_data)} registros coletados ({len(record_ids)} únicos), "
                                f"{url_queue.qsize()} páginas na fila, "
                                f"{len(futures_to_url)} tarefas ativas, "
                                f"{progress_tracker['processed_pages']} páginas processadas{completion_str}")

        # Final verification
        if progress_tracker['total_records'] > 0 and len(all_data) < progress_tracker['total_records']:
            logger.warning(f"ALERTA DE DADOS INCOMPLETOS: Coletados {len(all_data)} registros, "
                           f"mas o total informado pela API é {progress_tracker['total_records']}")

            missing = progress_tracker['total_records'] - len(all_data)
            logger.warning(
                f"Faltam aproximadamente {missing} registros ({(missing / progress_tracker['total_records']) * 100:.1f}% do total)")

        # Check for duplicate records by ID
        if len(record_ids) < len(all_data):
            duplicate_count = len(all_data) - len(record_ids)
            logger.warning(f"Detectados {duplicate_count} registros duplicados que serão removidos")

            # Remove duplicates by preserving order
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

    def main(request=None):
        try:
            start_time = time.time()

            # Check if credentials file exists
            if os.path.exists(GOOGLE_APPLICATION_CREDENTIALS):
                logger.info(f"Arquivo de credenciais encontrado: {GOOGLE_APPLICATION_CREDENTIALS}")
            else:
                logger.warning(f"Arquivo de credenciais não encontrado: {GOOGLE_APPLICATION_CREDENTIALS}")
                logger.warning("Utilizando credenciais padrão do ambiente")

            # Get all data - using the complete collection method
            logger.info("Iniciando coleta de dados da API Vindi")
            all_data = collect_vindi_data_complete()

            # Check if there's data to process
            if not all_data:
                logger.warning("Nenhum registro encontrado")
                return "Nenhum registro encontrado"

            record_count = len(all_data)
            # Using normalization function to handle JSON data
            logger.info(f"Convertendo {record_count} registros de dados JSON complexos...")

            # Primeiro, crie o DataFrame sem remover colunas vazias
            df_com_todas_colunas = convert_json_to_csv(all_data[:min(100, len(all_data))], remove_empty_columns=False)
            total_columns_original = len(df_com_todas_colunas.columns)

            # Agora crie o DataFrame final com a remoção de colunas vazias
            df = convert_json_to_csv(all_data, remove_empty_columns=True)

            # Calculate column stats
            final_columns = len(df.columns)
            removed_columns = total_columns_original - final_columns

            # Verify DataFrame size
            if len(df) != record_count:
                logger.warning(
                    f"ALERTA: O número de linhas no DataFrame ({len(df)}) não corresponde ao número de registros coletados ({record_count})")

            logger.info(f"Dados JSON convertidos com sucesso para formato tabular. Total de colunas: {len(df.columns)}")
            logger.info(
                f"Estatísticas de colunas: {total_columns_original} colunas potenciais, {removed_columns} removidas, {final_columns} mantidas")

            # Upload to GCP with credentials and folder
            upload_csv_to_bucket(
                data=df,
                bucket_name=BUCKET_NAME,
                blob_name=CSV_FILENAME,
                folder_path=FOLDER_PATH,
                credentials_path=GOOGLE_APPLICATION_CREDENTIALS,
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

    main()


def run_subscriptions(customer):
    import base64
    import concurrent.futures
    import logging
    import os
    import re
    import sys
    import time
    from queue import Queue, Empty
    from threading import Lock

    import pandas as pd
    import requests
    from dotenv import load_dotenv
    from google.cloud import storage
    import pathlib
    load_dotenv()

    # LOGGING
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger("VindiERPGCP")

    GENERIC_NAME = os.path.splitext(os.path.basename(sys.argv[0]))[0]

    # GCP
    BUCKET_NAME = customer['bucket_name']
    FOLDER_PATH = GENERIC_NAME
    GOOGLE_APPLICATION_CREDENTIALS = pathlib.Path('config', 'gcp.json')
    GENERIC_NAME = os.path.splitext(os.path.basename(sys.argv[0]))[0]
    CSV_FILENAME = f"{GENERIC_NAME}.csv"

    # APP
    CSV_FILENAME = f"{GENERIC_NAME}.csv"

    # API
    API_BASE_URL = customer['api_base_url']
    API_TOKEN = customer['api_token']
    AUTH_TYPE = "Basic"
    ROUTER = GENERIC_NAME
    API_RATE_LIMIT = 100  # Reduced from 120 to be safer
    MAX_PER_PAGE = 50  # Maximum records per page for Vindi API

    # PARALLELISM
    MAX_WORKERS = 2
    REQUEST_COUNTER_LOCK = Lock()
    request_counter = 0
    reset_time = time.time() + 60
    THREAD_DELAY = 0.5  # Increased delay between requests (in seconds)
    BACKOFF_MULTIPLIER = 1.5  # Multiplier for dynamic backoff

    def check_rate_limit():
        """
        Controls the rate limit of requests per minute using a synchronized global counter
        Returns True if within the limit, False otherwise
        """
        global request_counter, reset_time

        # If no rate limit is set, always return True
        if API_RATE_LIMIT is None:
            return True

        with REQUEST_COUNTER_LOCK:
            current_time = time.time()

            # Reset counter if 1 minute has passed
            if current_time > reset_time:
                request_counter = 0
                reset_time = current_time + 60

            # Check if limit reached
            if request_counter >= API_RATE_LIMIT:
                return False

            # Increment counter and return
            request_counter += 1
            return True

    def fetch_api_data(endpoint=None, params=None, page_url=None):
        """
        Fetches data from the Vindi API with support for header-based pagination
        """
        # Basic Auth: base64 encode the API token with empty password
        auth_string = base64.b64encode(f"{API_TOKEN}:".encode()).decode()

        headers = {
            "Authorization": f"{AUTH_TYPE} {auth_string}",
            "accept": "application/json"
        }

        # Determine the URL to use
        if page_url:
            # Use the provided full URL (for page-based pagination)
            url = page_url
        else:
            # Construct URL from base and endpoint
            api_endpoint = endpoint if endpoint else GENERIC_NAME
            url = f"{API_BASE_URL}/{api_endpoint}"

        # Prepare request parameters
        request_params = params.copy() if params else {}

        # Always request max per page if not specified
        if 'per_page' not in request_params:
            request_params['per_page'] = MAX_PER_PAGE

        # Make request with retry logic
        max_retries = 5
        retries = 0
        backoff_time = THREAD_DELAY

        while retries < max_retries:
            if check_rate_limit():
                try:
                    # Dynamic delay before making request
                    time.sleep(backoff_time)

                    response = requests.get(url, headers=headers, params=request_params)
                    response.raise_for_status()

                    # After successful request, reduce backoff time
                    # but keep it at least at THREAD_DELAY
                    global BACKOFF_MULTIPLIER
                    BACKOFF_MULTIPLIER = max(1.0, BACKOFF_MULTIPLIER * 0.9)

                    # Return both the JSON response and the headers for pagination
                    return {
                        'data': response.json(),
                        'headers': {
                            'total': response.headers.get('Total'),
                            'per_page': response.headers.get('Per-Page'),
                            'link': response.headers.get('Link')
                        },
                        'url': url  # Include the URL for tracking
                    }

                except requests.exceptions.HTTPError as e:
                    if response.status_code == 429:
                        retries += 1
                        # Increase backoff time with each 429 error
                        BACKOFF_MULTIPLIER *= 1.2
                        wait_time = 5 * retries * BACKOFF_MULTIPLIER  # Dynamic exponential backoff
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
                # If at rate limit, wait before trying again
                logger.debug("Rate limit atingido, aguardando 1 segundo...")
                time.sleep(1)

        logger.error(f"Falha após {max_retries} tentativas para URL: {url}")
        raise Exception(f"Falha após {max_retries} tentativas para URL: {url}")

    def extract_all_urls_from_link_header(link_header):
        """
        Extracts all URLs from the Link header, returning a dictionary of URLs by relation
        Example Link header:
        <https://app.vindi.com.br/api/v1/customers?page=3>; rel="last", <https://app.vindi.com.br/api/v1/customers?page=2>; rel="next"
        """
        if not link_header:
            return {}

        # Find all URL and relation pairs
        links = {}
        pattern = r'<([^>]+)>;\s*rel="([^"]+)"'
        matches = re.findall(pattern, link_header)

        for url, rel in matches:
            links[rel] = url

        return links

    def extract_page_number(url):
        """
        Extracts the page number from a URL for logging purposes
        """
        if not url:
            return "1"

        match = re.search(r'page=(\d+)', url)
        if match:
            return match.group(1)
        return "1"

    def generate_sequential_urls(base_pattern, start_page, end_page):
        """
        Generate a sequence of URLs for pages from start_page to end_page using the base pattern
        """
        urls = []
        for page in range(start_page, end_page + 1):
            url = base_pattern.format(page=page)
            urls.append(url)
        return urls

    def generate_page_urls(link_header, total_pages=None):
        """
        Generate URLs for a range of pages based on the link header pattern
        """
        if not link_header:
            return []

        # Extract URLs from link header
        links = extract_all_urls_from_link_header(link_header)

        # If we don't have last or next, we can't generate URLs
        if 'last' not in links and 'next' not in links:
            return []

        # Extract base URL pattern by replacing page number with placeholder
        base_url = None
        if 'next' in links:
            url = links['next']
            base_url = re.sub(r'page=\d+', 'page={page}', url)
        elif 'last' in links:
            url = links['last']
            base_url = re.sub(r'page=\d+', 'page={page}', url)

        if not base_url:
            return []

        # Determine range of pages to generate
        urls = []
        if 'last' in links and total_pages is None:
            last_page = int(extract_page_number(links['last']))
            current_page = int(extract_page_number(links.get('self', ''))) if 'self' in links else 1
            next_page = int(extract_page_number(links.get('next', ''))) if 'next' in links else current_page + 1

            # Generate URLs for the next batch of pages - REDUCED batch size
            max_pages_to_generate = 10  # Limit to avoid generating too many URLs at once
            end_page = min(last_page, next_page + max_pages_to_generate)

            urls = generate_sequential_urls(base_url, next_page, end_page)
        elif total_pages:
            current_page = int(extract_page_number(links.get('self', ''))) if 'self' in links else 1
            next_page = int(extract_page_number(links.get('next', ''))) if 'next' in links else current_page + 1

            # Generate URLs up to the specified total pages
            max_pages = min(total_pages, next_page + 10)  # Limit batch size
            urls = generate_sequential_urls(base_url, next_page, max_pages)

        return urls

    def convert_json_to_csv(data, output_file=None, csv_separator=";", id_field='id', remove_empty_columns=True):
        """
        Converts JSON data of any depth to a normalized DataFrame,
        keeping original keys (converted to lowercase) and adding numbering only for list items.
        Removes columns where all records have no value.
        """
        logger.info("Iniciando processamento de dados JSON complexos...")

        # Ensure we're working with a list of dictionaries (records)
        if isinstance(data, dict):
            records = [data]
        elif isinstance(data, list):
            if all(isinstance(item, dict) for item in data):
                records = data
            else:
                # Wrap simple list items in dictionaries
                records = [{"item": item} for item in data]
        else:
            records = [{"value": data}]

        # Verify dataset for completeness
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

        # Recursive helper function to flatten each record
        def json_to_csv(data, prefix="", result=None):
            if result is None:
                result = {}

            # Process based on data type
            if isinstance(data, dict):
                # For dictionaries, nest keys with underscore and convert to lowercase
                for key, value in data.items():
                    # Convert keys to lowercase
                    key = key.lower()
                    new_prefix = f"{prefix}_{key}" if prefix else key
                    if isinstance(value, (dict, list)):
                        json_to_csv(value, new_prefix, result)
                    else:
                        result[new_prefix] = value

            elif isinstance(data, list):
                # For lists, number the items
                for i, item in enumerate(data, 1):
                    if isinstance(item, dict):
                        # For dictionaries within lists
                        for sub_key, sub_value in item.items():
                            # Convert keys to lowercase
                            sub_key = sub_key.lower()
                            new_prefix = f"{prefix}_{sub_key}_{i}" if prefix else f"{sub_key}_{i}"
                            if isinstance(sub_value, (dict, list)):
                                json_to_csv(sub_value, new_prefix, result)
                            else:
                                result[new_prefix] = sub_value
                    else:
                        # For simple values within lists
                        result[f"{prefix}_{i}" if prefix else f"{prefix}{i}"] = item

            return result

        # Process each record
        logger.info(f"Processando {len(records)} registros JSON...")
        flattened_records = []

        # For large amounts of data, show progress
        progress_log = max(1, len(records) // 10)

        for i, record in enumerate(records):
            flattened_record = json_to_csv(record)
            flattened_records.append(flattened_record)

            # Log progress for large data sets
            if (i + 1) % progress_log == 0 or (i + 1) == len(records):
                logger.info(f"Processados {i + 1}/{len(records)} registros ({((i + 1) / len(records)) * 100:.1f}%)")

        # Convert to DataFrame
        logger.info("Convertendo registros json/csv para DataFrame...")
        df = pd.DataFrame(flattened_records)

        # Converter colunas de ID para inteiros (mantendo NaN onde apropriado)
        for col in df.columns:
            # Verificar se o nome da coluna contém "_id" ou termina com "id"
            if '_id' in col or col.endswith('id'):
                try:
                    # Verificar se a coluna contém valores que podem ser convertidos para números
                    # Primeiro tentar converter para ver se é possível
                    numeric_values = pd.to_numeric(df[col], errors='coerce')

                    # Se mais de 80% dos valores não-nulos foram convertidos com sucesso, então é uma coluna ID numérica
                    non_null_count = df[col].count()
                    successful_conversions = numeric_values.count()

                    # Evitar divisão por zero
                    if non_null_count > 0 and (successful_conversions / non_null_count) >= 0.8:
                        # Usar dtype='Int64' (com I maiúsculo) - é o tipo nullable integer do pandas
                        # que permite valores NaN junto com inteiros
                        df[col] = numeric_values.astype('Int64')
                        logger.info(f"Coluna {col} convertida para inteiro")
                    else:
                        logger.info(f"Coluna {col} contém valores não numéricos, mantida como está")
                except Exception as e:
                    logger.warning(f"Não foi possível converter a coluna {col} para inteiro: {e}")

        # Count total columns before removing any
        total_columns = len(df.columns)
        logger.info(f"Total de colunas antes da filtragem: {total_columns}")

        # Remove columns where all values are null/NaN
        before_count = len(df.columns)

        if remove_empty_columns:
            df = df.dropna(axis=1, how='all')

        after_count = len(df.columns)
        removed_count = before_count - after_count

        # Log the results of column filtering
        logger.info(f"Remoção de colunas sem valores: {removed_count} colunas removidas de um total de {before_count}")
        if removed_count > 0:
            percentage_removed = (removed_count / before_count) * 100
            logger.info(f"Removidas {removed_count} colunas sem valores ({percentage_removed:.1f}% das colunas)")

        # Save to file if path is provided
        if output_file:
            try:
                df.to_csv(output_file, index=False, sep=csv_separator)
                logger.info(f"Arquivo CSV normalizado criado: {output_file} com {len(df.columns)} colunas")
            except Exception as e:
                logger.error(f"Erro ao salvar CSV: {e}")
                raise

        logger.info(f"Processamento finalizado. DataFrame criado com {len(df)} linhas e {len(df.columns)} colunas")

        # Additional logging for columns analysis
        logger.info(
            f"Análise de colunas: {total_columns} colunas totais, {removed_count} removidas, {after_count} mantidas")

        return df

    def upload_csv_to_bucket(data, bucket_name, blob_name, folder_path=None, credentials_path=None, separator=";"):
        """
        Uploads a DataFrame as CSV to a GCP bucket
        """
        try:
            # Configure credentials if path is provided
            if credentials_path and os.path.exists(credentials_path):
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
                logger.info(f"Usando credenciais do arquivo: {credentials_path}")
            else:
                logger.info("Usando credenciais padrão do ambiente")

            # Initialize storage client
            client = storage.Client()

            # Get bucket
            bucket = client.bucket(bucket_name)

            # Create full blob path including folder if specified
            full_blob_path = blob_name
            if folder_path:
                # Remove slashes at beginning and add slash at end if needed
                clean_folder = folder_path.strip('/')
                if clean_folder:
                    full_blob_path = f"{clean_folder}/{blob_name}"

            # Create blob
            blob = bucket.blob(full_blob_path)

            # Convert DataFrame to CSV in memory with error handling
            try:
                # Try to convert using standard mode with specified separator
                csv_string = data.to_csv(index=False, sep=separator)
            except Exception as csv_error:
                logger.warning(f"Erro na conversão padrão para CSV: {csv_error}")
                logger.info("Tentando converter com método alternativo...")

                # Alternative method with more safety for complex types
                csv_string = data.replace({pd.NA: None}).to_csv(index=False, na_rep='', sep=separator)

            # Upload to GCP
            blob.upload_from_string(csv_string, content_type="text/csv")

            # Additional useful information
            size_kb = len(csv_string) / 1024
            row_count = len(data)
            logger.info(
                f"Arquivo '{full_blob_path}' ({size_kb:.2f} KB) com {row_count} linhas enviado com sucesso para o bucket '{bucket_name}'")
            return True

        except Exception as e:
            logger.error(f"Erro ao enviar arquivo para GCP: {e}")
            raise

    def collect_vindi_data_complete(params=None):
        """
        Collects all data from the Vindi API with verification to ensure completeness
        """
        logger.info("Coletando dados da API Vindi com verificação de completude...")

        all_data = []
        data_lock = Lock()  # Lock for thread-safe data accumulation
        url_queue = Queue()
        processed_pages = set()
        pages_lock = Lock()

        # For record tracking
        record_ids = set()  # Track unique record IDs
        id_field = 'id'  # Default ID field

        # Progress tracking
        progress_tracker = {
            'total_records': 0,
            'processed_pages': 0,
            'last_update': time.time()
        }

        # Prepare initial parameters - ensure max per page
        request_params = params.copy() if params else {}
        if 'per_page' not in request_params:
            request_params['per_page'] = MAX_PER_PAGE

        # Fetch first page
        try:
            response = fetch_api_data(endpoint=GENERIC_NAME, params=request_params)

            # Extract data - Vindi usually has a wrapper with the entity name
            data_wrapper = response['data']
            entity_data = []

            # Find the actual data array
            entity_data_key = None

            if isinstance(data_wrapper, dict):
                # Try to get data from the resource key (e.g., 'customers', 'subscriptions')
                if GENERIC_NAME in data_wrapper:
                    entity_data = data_wrapper[GENERIC_NAME]
                    entity_data_key = GENERIC_NAME
                else:
                    # If not found under resource name, take the first list found
                    for key, value in data_wrapper.items():
                        if isinstance(value, list):
                            entity_data = value
                            entity_data_key = key
                            break
            elif isinstance(data_wrapper, list):
                entity_data = data_wrapper

            # Log what we found
            if entity_data_key:
                logger.info(f"Dados encontrados sob a chave: '{entity_data_key}'")

            if entity_data:
                # Detect ID field if possible
                if entity_data and isinstance(entity_data[0], dict):
                    potential_id_fields = ['id', 'uuid', 'code']
                    for field in potential_id_fields:
                        if field in entity_data[0]:
                            id_field = field
                            logger.info(f"Campo de ID detectado: '{id_field}'")
                            break

                # Track unique IDs and add data to main list
                for record in entity_data:
                    if isinstance(record, dict) and id_field in record:
                        record_ids.add(record[id_field])

                all_data.extend(entity_data)
                logger.info(f"Obtidos {len(entity_data)} registros na página inicial")

            # Log total if available
            total_count = 0
            if response['headers']['total']:
                total_count = int(response['headers']['total'])
                per_page = int(response['headers']['per_page']) if response['headers']['per_page'] else MAX_PER_PAGE
                logger.info(f"Total de registros na API: {total_count} (max {per_page} por página)")
                progress_tracker['total_records'] = total_count

            # Get pagination info from Link header
            link_header = response['headers']['link']

            # Extract all link relations
            links = extract_all_urls_from_link_header(link_header)

            # If we have "last" link, try to determine total number of pages
            total_pages = 0
            if 'last' in links:
                last_page_url = links['last']
                last_page_num = int(extract_page_number(last_page_url))
                total_pages = last_page_num
                logger.info(f"Total de páginas a coletar: {total_pages}")

                # Extract the base URL pattern for page generation
                base_url_pattern = re.sub(r'page=\d+', 'page={page}', last_page_url)

                # Generate ALL page URLs to ensure we don't miss any
                # Start from page 2 since we already processed page 1
                all_urls = generate_sequential_urls(base_url_pattern, 2, total_pages)

                # Add ALL URLs to the queue
                for url in all_urls:
                    url_queue.put(url)

                logger.info(f"Enfileiradas {len(all_urls)} páginas para processamento")

        except Exception as e:
            logger.error(f"Erro ao buscar página inicial: {e}")
            return all_data

        # If no more pages, return data
        if url_queue.empty():
            logger.info("Apenas uma página de resultados")
            return all_data

        # Define page processing function
        def process_vindi_page(page_url):
            # Extract page number for logging
            page_num = extract_page_number(page_url)

            # Avoid processing the same page twice
            with pages_lock:
                if page_url in processed_pages:
                    logger.debug(f"Página {page_num} já foi processada. Pulando.")
                    return None
                processed_pages.add(page_url)

            try:
                logger.debug(f"Buscando página {page_num}...")
                response = fetch_api_data(page_url=page_url)

                # Extract data similar to first page
                data_wrapper = response['data']
                entity_data = []

                if isinstance(data_wrapper, dict):
                    if entity_data_key and entity_data_key in data_wrapper:
                        entity_data = data_wrapper[entity_data_key]
                    elif GENERIC_NAME in data_wrapper:
                        entity_data = data_wrapper[GENERIC_NAME]
                    else:
                        for key, value in data_wrapper.items():
                            if isinstance(value, list):
                                entity_data = value
                                break
                elif isinstance(data_wrapper, list):
                    entity_data = data_wrapper

                # Record IDs for tracking
                new_records = 0
                duplicate_records = 0

                # Track unique records
                with data_lock:
                    for record in entity_data:
                        if isinstance(record, dict) and id_field in record:
                            if record[id_field] not in record_ids:
                                record_ids.add(record[id_field])
                                new_records += 1
                            else:
                                duplicate_records += 1

                    # Add data to main collection
                    all_data.extend(entity_data)

                    # Update progress tracking
                    progress_tracker['processed_pages'] += 1

                if duplicate_records > 0:
                    logger.warning(f"Página {page_num}: {duplicate_records} registros duplicados encontrados")

                logger.info(f"Obtidos {len(entity_data)} registros na página {page_num} ({new_records} novos)")

                return entity_data
            except Exception as e:
                logger.error(f"Erro ao processar página {page_num}: {e}")
                return None

        # Process pages in parallel using a fixed thread pool
        logger.info(f"Iniciando processamento paralelo com {MAX_WORKERS} workers")

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Create a dict to track futures with their URLs
            futures_to_url = {}

            # Function to add new tasks
            def add_tasks():
                while len(futures_to_url) < MAX_WORKERS and not url_queue.empty():
                    try:
                        page_url = url_queue.get(block=False)
                        if page_url not in processed_pages:
                            future = executor.submit(process_vindi_page, page_url)
                            futures_to_url[future] = page_url
                    except Empty:
                        break

            # Initial task loading - start conservatively
            initial_tasks = min(MAX_WORKERS // 2, url_queue.qsize())
            for _ in range(initial_tasks):
                if not url_queue.empty():
                    page_url = url_queue.get()
                    if page_url not in processed_pages:
                        future = executor.submit(process_vindi_page, page_url)
                        futures_to_url[future] = page_url

            # Process until no more URLs and no active tasks
            consecutive_errors = 0
            last_progress_report = time.time()

            while futures_to_url or not url_queue.empty():
                # Wait for tasks to complete
                done, _ = concurrent.futures.wait(
                    futures_to_url,
                    return_when=concurrent.futures.FIRST_COMPLETED,
                    timeout=5
                )

                # Process completed tasks
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

                # Add new tasks
                add_tasks()

                # Progress reporting
                current_time = time.time()
                if current_time - last_progress_report > 15:  # Report progress every 15 seconds
                    last_progress_report = current_time

                    # Calculate completion percentage if we know total pages
                    completion_str = ""
                    if progress_tracker['total_records'] > 0:
                        percent_complete = (len(all_data) / progress_tracker['total_records']) * 100
                        completion_str = f" - {percent_complete:.1f}% concluído"

                    # Report progress
                    logger.info(f"Progresso: {len(all_data)} registros coletados ({len(record_ids)} únicos), "
                                f"{url_queue.qsize()} páginas na fila, "
                                f"{len(futures_to_url)} tarefas ativas, "
                                f"{progress_tracker['processed_pages']} páginas processadas{completion_str}")

        # Final verification
        if progress_tracker['total_records'] > 0 and len(all_data) < progress_tracker['total_records']:
            logger.warning(f"ALERTA DE DADOS INCOMPLETOS: Coletados {len(all_data)} registros, "
                           f"mas o total informado pela API é {progress_tracker['total_records']}")

            missing = progress_tracker['total_records'] - len(all_data)
            logger.warning(
                f"Faltam aproximadamente {missing} registros ({(missing / progress_tracker['total_records']) * 100:.1f}% do total)")

        # Check for duplicate records by ID
        if len(record_ids) < len(all_data):
            duplicate_count = len(all_data) - len(record_ids)
            logger.warning(f"Detectados {duplicate_count} registros duplicados que serão removidos")

            # Remove duplicates by preserving order
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

    def main(request=None):
        try:
            start_time = time.time()

            # Check if credentials file exists
            if os.path.exists(GOOGLE_APPLICATION_CREDENTIALS):
                logger.info(f"Arquivo de credenciais encontrado: {GOOGLE_APPLICATION_CREDENTIALS}")
            else:
                logger.warning(f"Arquivo de credenciais não encontrado: {GOOGLE_APPLICATION_CREDENTIALS}")
                logger.warning("Utilizando credenciais padrão do ambiente")

            # Get all data - using the complete collection method
            logger.info("Iniciando coleta de dados da API Vindi")
            all_data = collect_vindi_data_complete()

            # Check if there's data to process
            if not all_data:
                logger.warning("Nenhum registro encontrado")
                return "Nenhum registro encontrado"

            record_count = len(all_data)
            # Using normalization function to handle JSON data
            logger.info(f"Convertendo {record_count} registros de dados JSON complexos...")

            # Primeiro, crie o DataFrame sem remover colunas vazias
            df_com_todas_colunas = convert_json_to_csv(all_data[:min(100, len(all_data))], remove_empty_columns=False)
            total_columns_original = len(df_com_todas_colunas.columns)

            # Agora crie o DataFrame final com a remoção de colunas vazias
            df = convert_json_to_csv(all_data, remove_empty_columns=True)

            # Calculate column stats
            final_columns = len(df.columns)
            removed_columns = total_columns_original - final_columns

            # Verify DataFrame size
            if len(df) != record_count:
                logger.warning(
                    f"ALERTA: O número de linhas no DataFrame ({len(df)}) não corresponde ao número de registros coletados ({record_count})")

            logger.info(f"Dados JSON convertidos com sucesso para formato tabular. Total de colunas: {len(df.columns)}")
            logger.info(
                f"Estatísticas de colunas: {total_columns_original} colunas potenciais, {removed_columns} removidas, {final_columns} mantidas")

            # Upload to GCP with credentials and folder
            upload_csv_to_bucket(
                data=df,
                bucket_name=BUCKET_NAME,
                blob_name=CSV_FILENAME,
                folder_path=FOLDER_PATH,
                credentials_path=GOOGLE_APPLICATION_CREDENTIALS,
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

    main()


def run_customers(customer):
    import base64
    import concurrent.futures
    import logging
    import os
    import re
    import sys
    import time
    from queue import Queue, Empty
    from threading import Lock

    import pandas as pd
    import requests
    from dotenv import load_dotenv
    from google.cloud import storage
    import pathlib
    load_dotenv()

    # LOGGING
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger("VindiERPGCP")

    GENERIC_NAME = os.path.splitext(os.path.basename(sys.argv[0]))[0]

    # GCP
    BUCKET_NAME = customer['bucket_name']
    FOLDER_PATH = GENERIC_NAME
    GOOGLE_APPLICATION_CREDENTIALS = pathlib.Path('config', 'gcp.json')
    GENERIC_NAME = os.path.splitext(os.path.basename(sys.argv[0]))[0]
    CSV_FILENAME = f"{GENERIC_NAME}.csv"

    # APP
    CSV_FILENAME = f"{GENERIC_NAME}.csv"

    # API
    API_BASE_URL = customer['api_base_url']
    API_TOKEN = customer['api_token']
    AUTH_TYPE = "Basic"
    ROUTER = GENERIC_NAME
    API_RATE_LIMIT = 100  # Reduced from 120 to be safer
    MAX_PER_PAGE = 50  # Maximum records per page for Vindi API

    # PARALLELISM
    MAX_WORKERS = 2
    REQUEST_COUNTER_LOCK = Lock()
    request_counter = 0
    reset_time = time.time() + 60
    THREAD_DELAY = 0.5  # Increased delay between requests (in seconds)
    BACKOFF_MULTIPLIER = 1.5  # Multiplier for dynamic backoff

    def check_rate_limit():
        """
        Controls the rate limit of requests per minute using a synchronized global counter
        Returns True if within the limit, False otherwise
        """
        global request_counter, reset_time

        # If no rate limit is set, always return True
        if API_RATE_LIMIT is None:
            return True

        with REQUEST_COUNTER_LOCK:
            current_time = time.time()

            # Reset counter if 1 minute has passed
            if current_time > reset_time:
                request_counter = 0
                reset_time = current_time + 60

            # Check if limit reached
            if request_counter >= API_RATE_LIMIT:
                return False

            # Increment counter and return
            request_counter += 1
            return True

    def fetch_api_data(endpoint=None, params=None, page_url=None):
        """
        Fetches data from the Vindi API with support for header-based pagination
        """
        # Basic Auth: base64 encode the API token with empty password
        auth_string = base64.b64encode(f"{API_TOKEN}:".encode()).decode()

        headers = {
            "Authorization": f"{AUTH_TYPE} {auth_string}",
            "accept": "application/json"
        }

        # Determine the URL to use
        if page_url:
            # Use the provided full URL (for page-based pagination)
            url = page_url
        else:
            # Construct URL from base and endpoint
            api_endpoint = endpoint if endpoint else GENERIC_NAME
            url = f"{API_BASE_URL}/{api_endpoint}"

        # Prepare request parameters
        request_params = params.copy() if params else {}

        # Always request max per page if not specified
        if 'per_page' not in request_params:
            request_params['per_page'] = MAX_PER_PAGE

        # Make request with retry logic
        max_retries = 5
        retries = 0
        backoff_time = THREAD_DELAY

        while retries < max_retries:
            if check_rate_limit():
                try:
                    # Dynamic delay before making request
                    time.sleep(backoff_time)

                    response = requests.get(url, headers=headers, params=request_params)
                    response.raise_for_status()

                    # After successful request, reduce backoff time
                    # but keep it at least at THREAD_DELAY
                    global BACKOFF_MULTIPLIER
                    BACKOFF_MULTIPLIER = max(1.0, BACKOFF_MULTIPLIER * 0.9)

                    # Return both the JSON response and the headers for pagination
                    return {
                        'data': response.json(),
                        'headers': {
                            'total': response.headers.get('Total'),
                            'per_page': response.headers.get('Per-Page'),
                            'link': response.headers.get('Link')
                        },
                        'url': url  # Include the URL for tracking
                    }

                except requests.exceptions.HTTPError as e:
                    if response.status_code == 429:
                        retries += 1
                        # Increase backoff time with each 429 error
                        BACKOFF_MULTIPLIER *= 1.2
                        wait_time = 5 * retries * BACKOFF_MULTIPLIER  # Dynamic exponential backoff
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
                # If at rate limit, wait before trying again
                logger.debug("Rate limit atingido, aguardando 1 segundo...")
                time.sleep(1)

        logger.error(f"Falha após {max_retries} tentativas para URL: {url}")
        raise Exception(f"Falha após {max_retries} tentativas para URL: {url}")

    def extract_all_urls_from_link_header(link_header):
        """
        Extracts all URLs from the Link header, returning a dictionary of URLs by relation
        Example Link header:
        <https://app.vindi.com.br/api/v1/customers?page=3>; rel="last", <https://app.vindi.com.br/api/v1/customers?page=2>; rel="next"
        """
        if not link_header:
            return {}

        # Find all URL and relation pairs
        links = {}
        pattern = r'<([^>]+)>;\s*rel="([^"]+)"'
        matches = re.findall(pattern, link_header)

        for url, rel in matches:
            links[rel] = url

        return links

    def extract_page_number(url):
        """
        Extracts the page number from a URL for logging purposes
        """
        if not url:
            return "1"

        match = re.search(r'page=(\d+)', url)
        if match:
            return match.group(1)
        return "1"

    def generate_sequential_urls(base_pattern, start_page, end_page):
        """
        Generate a sequence of URLs for pages from start_page to end_page using the base pattern
        """
        urls = []
        for page in range(start_page, end_page + 1):
            url = base_pattern.format(page=page)
            urls.append(url)
        return urls

    def generate_page_urls(link_header, total_pages=None):
        """
        Generate URLs for a range of pages based on the link header pattern
        """
        if not link_header:
            return []

        # Extract URLs from link header
        links = extract_all_urls_from_link_header(link_header)

        # If we don't have last or next, we can't generate URLs
        if 'last' not in links and 'next' not in links:
            return []

        # Extract base URL pattern by replacing page number with placeholder
        base_url = None
        if 'next' in links:
            url = links['next']
            base_url = re.sub(r'page=\d+', 'page={page}', url)
        elif 'last' in links:
            url = links['last']
            base_url = re.sub(r'page=\d+', 'page={page}', url)

        if not base_url:
            return []

        # Determine range of pages to generate
        urls = []
        if 'last' in links and total_pages is None:
            last_page = int(extract_page_number(links['last']))
            current_page = int(extract_page_number(links.get('self', ''))) if 'self' in links else 1
            next_page = int(extract_page_number(links.get('next', ''))) if 'next' in links else current_page + 1

            # Generate URLs for the next batch of pages - REDUCED batch size
            max_pages_to_generate = 10  # Limit to avoid generating too many URLs at once
            end_page = min(last_page, next_page + max_pages_to_generate)

            urls = generate_sequential_urls(base_url, next_page, end_page)
        elif total_pages:
            current_page = int(extract_page_number(links.get('self', ''))) if 'self' in links else 1
            next_page = int(extract_page_number(links.get('next', ''))) if 'next' in links else current_page + 1

            # Generate URLs up to the specified total pages
            max_pages = min(total_pages, next_page + 10)  # Limit batch size
            urls = generate_sequential_urls(base_url, next_page, max_pages)

        return urls

    def convert_json_to_csv(data, output_file=None, csv_separator=";", id_field='id', remove_empty_columns=True):
        """
        Converts JSON data of any depth to a normalized DataFrame,
        keeping original keys (converted to lowercase) and adding numbering only for list items.
        Removes columns where all records have no value.
        """
        logger.info("Iniciando processamento de dados JSON complexos...")

        # Ensure we're working with a list of dictionaries (records)
        if isinstance(data, dict):
            records = [data]
        elif isinstance(data, list):
            if all(isinstance(item, dict) for item in data):
                records = data
            else:
                # Wrap simple list items in dictionaries
                records = [{"item": item} for item in data]
        else:
            records = [{"value": data}]

        # Verify dataset for completeness
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

        # Recursive helper function to flatten each record
        def json_to_csv(data, prefix="", result=None):
            if result is None:
                result = {}

            # Process based on data type
            if isinstance(data, dict):
                # For dictionaries, nest keys with underscore and convert to lowercase
                for key, value in data.items():
                    # Convert keys to lowercase
                    key = key.lower()
                    new_prefix = f"{prefix}_{key}" if prefix else key
                    if isinstance(value, (dict, list)):
                        json_to_csv(value, new_prefix, result)
                    else:
                        result[new_prefix] = value

            elif isinstance(data, list):
                # For lists, number the items
                for i, item in enumerate(data, 1):
                    if isinstance(item, dict):
                        # For dictionaries within lists
                        for sub_key, sub_value in item.items():
                            # Convert keys to lowercase
                            sub_key = sub_key.lower()
                            new_prefix = f"{prefix}_{sub_key}_{i}" if prefix else f"{sub_key}_{i}"
                            if isinstance(sub_value, (dict, list)):
                                json_to_csv(sub_value, new_prefix, result)
                            else:
                                result[new_prefix] = sub_value
                    else:
                        # For simple values within lists
                        result[f"{prefix}_{i}" if prefix else f"{prefix}{i}"] = item

            return result

        # Process each record
        logger.info(f"Processando {len(records)} registros JSON...")
        flattened_records = []

        # For large amounts of data, show progress
        progress_log = max(1, len(records) // 10)

        for i, record in enumerate(records):
            flattened_record = json_to_csv(record)
            flattened_records.append(flattened_record)

            # Log progress for large data sets
            if (i + 1) % progress_log == 0 or (i + 1) == len(records):
                logger.info(f"Processados {i + 1}/{len(records)} registros ({((i + 1) / len(records)) * 100:.1f}%)")

        # Convert to DataFrame
        logger.info("Convertendo registros json/csv para DataFrame...")
        df = pd.DataFrame(flattened_records)

        # Converter colunas de ID para inteiros (mantendo NaN onde apropriado)
        for col in df.columns:
            # Verificar se o nome da coluna contém "_id" ou termina com "id"
            if '_id' in col or col.endswith('id'):
                try:
                    # Verificar se a coluna contém valores que podem ser convertidos para números
                    # Primeiro tentar converter para ver se é possível
                    numeric_values = pd.to_numeric(df[col], errors='coerce')

                    # Se mais de 80% dos valores não-nulos foram convertidos com sucesso, então é uma coluna ID numérica
                    non_null_count = df[col].count()
                    successful_conversions = numeric_values.count()

                    # Evitar divisão por zero
                    if non_null_count > 0 and (successful_conversions / non_null_count) >= 0.8:
                        # Usar dtype='Int64' (com I maiúsculo) - é o tipo nullable integer do pandas
                        # que permite valores NaN junto com inteiros
                        df[col] = numeric_values.astype('Int64')
                        logger.info(f"Coluna {col} convertida para inteiro")
                    else:
                        logger.info(f"Coluna {col} contém valores não numéricos, mantida como está")
                except Exception as e:
                    logger.warning(f"Não foi possível converter a coluna {col} para inteiro: {e}")

        # Count total columns before removing any
        total_columns = len(df.columns)
        logger.info(f"Total de colunas antes da filtragem: {total_columns}")

        # Remove columns where all values are null/NaN
        before_count = len(df.columns)

        if remove_empty_columns:
            df = df.dropna(axis=1, how='all')

        after_count = len(df.columns)
        removed_count = before_count - after_count

        # Log the results of column filtering
        logger.info(f"Remoção de colunas sem valores: {removed_count} colunas removidas de um total de {before_count}")
        if removed_count > 0:
            percentage_removed = (removed_count / before_count) * 100
            logger.info(f"Removidas {removed_count} colunas sem valores ({percentage_removed:.1f}% das colunas)")

        # Save to file if path is provided
        if output_file:
            try:
                df.to_csv(output_file, index=False, sep=csv_separator)
                logger.info(f"Arquivo CSV normalizado criado: {output_file} com {len(df.columns)} colunas")
            except Exception as e:
                logger.error(f"Erro ao salvar CSV: {e}")
                raise

        logger.info(f"Processamento finalizado. DataFrame criado com {len(df)} linhas e {len(df.columns)} colunas")

        # Additional logging for columns analysis
        logger.info(
            f"Análise de colunas: {total_columns} colunas totais, {removed_count} removidas, {after_count} mantidas")

        return df

    def upload_csv_to_bucket(data, bucket_name, blob_name, folder_path=None, credentials_path=None, separator=";"):
        """
        Uploads a DataFrame as CSV to a GCP bucket
        """
        try:
            # Configure credentials if path is provided
            if credentials_path and os.path.exists(credentials_path):
                os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
                logger.info(f"Usando credenciais do arquivo: {credentials_path}")
            else:
                logger.info("Usando credenciais padrão do ambiente")

            # Initialize storage client
            client = storage.Client()

            # Get bucket
            bucket = client.bucket(bucket_name)

            # Create full blob path including folder if specified
            full_blob_path = blob_name
            if folder_path:
                # Remove slashes at beginning and add slash at end if needed
                clean_folder = folder_path.strip('/')
                if clean_folder:
                    full_blob_path = f"{clean_folder}/{blob_name}"

            # Create blob
            blob = bucket.blob(full_blob_path)

            # Convert DataFrame to CSV in memory with error handling
            try:
                # Try to convert using standard mode with specified separator
                csv_string = data.to_csv(index=False, sep=separator)
            except Exception as csv_error:
                logger.warning(f"Erro na conversão padrão para CSV: {csv_error}")
                logger.info("Tentando converter com método alternativo...")

                # Alternative method with more safety for complex types
                csv_string = data.replace({pd.NA: None}).to_csv(index=False, na_rep='', sep=separator)

            # Upload to GCP
            blob.upload_from_string(csv_string, content_type="text/csv")

            # Additional useful information
            size_kb = len(csv_string) / 1024
            row_count = len(data)
            logger.info(
                f"Arquivo '{full_blob_path}' ({size_kb:.2f} KB) com {row_count} linhas enviado com sucesso para o bucket '{bucket_name}'")
            return True

        except Exception as e:
            logger.error(f"Erro ao enviar arquivo para GCP: {e}")
            raise

    def collect_vindi_data_complete(params=None):
        """
        Collects all data from the Vindi API with verification to ensure completeness
        """
        logger.info("Coletando dados da API Vindi com verificação de completude...")

        all_data = []
        data_lock = Lock()  # Lock for thread-safe data accumulation
        url_queue = Queue()
        processed_pages = set()
        pages_lock = Lock()

        # For record tracking
        record_ids = set()  # Track unique record IDs
        id_field = 'id'  # Default ID field

        # Progress tracking
        progress_tracker = {
            'total_records': 0,
            'processed_pages': 0,
            'last_update': time.time()
        }

        # Prepare initial parameters - ensure max per page
        request_params = params.copy() if params else {}
        if 'per_page' not in request_params:
            request_params['per_page'] = MAX_PER_PAGE

        # Fetch first page
        try:
            response = fetch_api_data(endpoint=GENERIC_NAME, params=request_params)

            # Extract data - Vindi usually has a wrapper with the entity name
            data_wrapper = response['data']
            entity_data = []

            # Find the actual data array
            entity_data_key = None

            if isinstance(data_wrapper, dict):
                # Try to get data from the resource key (e.g., 'customers', 'subscriptions')
                if GENERIC_NAME in data_wrapper:
                    entity_data = data_wrapper[GENERIC_NAME]
                    entity_data_key = GENERIC_NAME
                else:
                    # If not found under resource name, take the first list found
                    for key, value in data_wrapper.items():
                        if isinstance(value, list):
                            entity_data = value
                            entity_data_key = key
                            break
            elif isinstance(data_wrapper, list):
                entity_data = data_wrapper

            # Log what we found
            if entity_data_key:
                logger.info(f"Dados encontrados sob a chave: '{entity_data_key}'")

            if entity_data:
                # Detect ID field if possible
                if entity_data and isinstance(entity_data[0], dict):
                    potential_id_fields = ['id', 'uuid', 'code']
                    for field in potential_id_fields:
                        if field in entity_data[0]:
                            id_field = field
                            logger.info(f"Campo de ID detectado: '{id_field}'")
                            break

                # Track unique IDs and add data to main list
                for record in entity_data:
                    if isinstance(record, dict) and id_field in record:
                        record_ids.add(record[id_field])

                all_data.extend(entity_data)
                logger.info(f"Obtidos {len(entity_data)} registros na página inicial")

            # Log total if available
            total_count = 0
            if response['headers']['total']:
                total_count = int(response['headers']['total'])
                per_page = int(response['headers']['per_page']) if response['headers']['per_page'] else MAX_PER_PAGE
                logger.info(f"Total de registros na API: {total_count} (max {per_page} por página)")
                progress_tracker['total_records'] = total_count

            # Get pagination info from Link header
            link_header = response['headers']['link']

            # Extract all link relations
            links = extract_all_urls_from_link_header(link_header)

            # If we have "last" link, try to determine total number of pages
            total_pages = 0
            if 'last' in links:
                last_page_url = links['last']
                last_page_num = int(extract_page_number(last_page_url))
                total_pages = last_page_num
                logger.info(f"Total de páginas a coletar: {total_pages}")

                # Extract the base URL pattern for page generation
                base_url_pattern = re.sub(r'page=\d+', 'page={page}', last_page_url)

                # Generate ALL page URLs to ensure we don't miss any
                # Start from page 2 since we already processed page 1
                all_urls = generate_sequential_urls(base_url_pattern, 2, total_pages)

                # Add ALL URLs to the queue
                for url in all_urls:
                    url_queue.put(url)

                logger.info(f"Enfileiradas {len(all_urls)} páginas para processamento")

        except Exception as e:
            logger.error(f"Erro ao buscar página inicial: {e}")
            return all_data

        # If no more pages, return data
        if url_queue.empty():
            logger.info("Apenas uma página de resultados")
            return all_data

        # Define page processing function
        def process_vindi_page(page_url):
            # Extract page number for logging
            page_num = extract_page_number(page_url)

            # Avoid processing the same page twice
            with pages_lock:
                if page_url in processed_pages:
                    logger.debug(f"Página {page_num} já foi processada. Pulando.")
                    return None
                processed_pages.add(page_url)

            try:
                logger.debug(f"Buscando página {page_num}...")
                response = fetch_api_data(page_url=page_url)

                # Extract data similar to first page
                data_wrapper = response['data']
                entity_data = []

                if isinstance(data_wrapper, dict):
                    if entity_data_key and entity_data_key in data_wrapper:
                        entity_data = data_wrapper[entity_data_key]
                    elif GENERIC_NAME in data_wrapper:
                        entity_data = data_wrapper[GENERIC_NAME]
                    else:
                        for key, value in data_wrapper.items():
                            if isinstance(value, list):
                                entity_data = value
                                break
                elif isinstance(data_wrapper, list):
                    entity_data = data_wrapper

                # Record IDs for tracking
                new_records = 0
                duplicate_records = 0

                # Track unique records
                with data_lock:
                    for record in entity_data:
                        if isinstance(record, dict) and id_field in record:
                            if record[id_field] not in record_ids:
                                record_ids.add(record[id_field])
                                new_records += 1
                            else:
                                duplicate_records += 1

                    # Add data to main collection
                    all_data.extend(entity_data)

                    # Update progress tracking
                    progress_tracker['processed_pages'] += 1

                if duplicate_records > 0:
                    logger.warning(f"Página {page_num}: {duplicate_records} registros duplicados encontrados")

                logger.info(f"Obtidos {len(entity_data)} registros na página {page_num} ({new_records} novos)")

                return entity_data
            except Exception as e:
                logger.error(f"Erro ao processar página {page_num}: {e}")
                return None

        # Process pages in parallel using a fixed thread pool
        logger.info(f"Iniciando processamento paralelo com {MAX_WORKERS} workers")

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Create a dict to track futures with their URLs
            futures_to_url = {}

            # Function to add new tasks
            def add_tasks():
                while len(futures_to_url) < MAX_WORKERS and not url_queue.empty():
                    try:
                        page_url = url_queue.get(block=False)
                        if page_url not in processed_pages:
                            future = executor.submit(process_vindi_page, page_url)
                            futures_to_url[future] = page_url
                    except Empty:
                        break

            # Initial task loading - start conservatively
            initial_tasks = min(MAX_WORKERS // 2, url_queue.qsize())
            for _ in range(initial_tasks):
                if not url_queue.empty():
                    page_url = url_queue.get()
                    if page_url not in processed_pages:
                        future = executor.submit(process_vindi_page, page_url)
                        futures_to_url[future] = page_url

            # Process until no more URLs and no active tasks
            consecutive_errors = 0
            last_progress_report = time.time()

            while futures_to_url or not url_queue.empty():
                # Wait for tasks to complete
                done, _ = concurrent.futures.wait(
                    futures_to_url,
                    return_when=concurrent.futures.FIRST_COMPLETED,
                    timeout=5
                )

                # Process completed tasks
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

                # Add new tasks
                add_tasks()

                # Progress reporting
                current_time = time.time()
                if current_time - last_progress_report > 15:  # Report progress every 15 seconds
                    last_progress_report = current_time

                    # Calculate completion percentage if we know total pages
                    completion_str = ""
                    if progress_tracker['total_records'] > 0:
                        percent_complete = (len(all_data) / progress_tracker['total_records']) * 100
                        completion_str = f" - {percent_complete:.1f}% concluído"

                    # Report progress
                    logger.info(f"Progresso: {len(all_data)} registros coletados ({len(record_ids)} únicos), "
                                f"{url_queue.qsize()} páginas na fila, "
                                f"{len(futures_to_url)} tarefas ativas, "
                                f"{progress_tracker['processed_pages']} páginas processadas{completion_str}")

        # Final verification
        if progress_tracker['total_records'] > 0 and len(all_data) < progress_tracker['total_records']:
            logger.warning(f"ALERTA DE DADOS INCOMPLETOS: Coletados {len(all_data)} registros, "
                           f"mas o total informado pela API é {progress_tracker['total_records']}")

            missing = progress_tracker['total_records'] - len(all_data)
            logger.warning(
                f"Faltam aproximadamente {missing} registros ({(missing / progress_tracker['total_records']) * 100:.1f}% do total)")

        # Check for duplicate records by ID
        if len(record_ids) < len(all_data):
            duplicate_count = len(all_data) - len(record_ids)
            logger.warning(f"Detectados {duplicate_count} registros duplicados que serão removidos")

            # Remove duplicates by preserving order
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

    def main(request=None):
        try:
            start_time = time.time()

            # Check if credentials file exists
            if os.path.exists(GOOGLE_APPLICATION_CREDENTIALS):
                logger.info(f"Arquivo de credenciais encontrado: {GOOGLE_APPLICATION_CREDENTIALS}")
            else:
                logger.warning(f"Arquivo de credenciais não encontrado: {GOOGLE_APPLICATION_CREDENTIALS}")
                logger.warning("Utilizando credenciais padrão do ambiente")

            # Get all data - using the complete collection method
            logger.info("Iniciando coleta de dados da API Vindi")
            all_data = collect_vindi_data_complete()

            # Check if there's data to process
            if not all_data:
                logger.warning("Nenhum registro encontrado")
                return "Nenhum registro encontrado"

            record_count = len(all_data)
            # Using normalization function to handle JSON data
            logger.info(f"Convertendo {record_count} registros de dados JSON complexos...")

            # Primeiro, crie o DataFrame sem remover colunas vazias
            df_com_todas_colunas = convert_json_to_csv(all_data[:min(100, len(all_data))], remove_empty_columns=False)
            total_columns_original = len(df_com_todas_colunas.columns)

            # Agora crie o DataFrame final com a remoção de colunas vazias
            df = convert_json_to_csv(all_data, remove_empty_columns=True)

            # Calculate column stats
            final_columns = len(df.columns)
            removed_columns = total_columns_original - final_columns

            # Verify DataFrame size
            if len(df) != record_count:
                logger.warning(
                    f"ALERTA: O número de linhas no DataFrame ({len(df)}) não corresponde ao número de registros coletados ({record_count})")

            logger.info(f"Dados JSON convertidos com sucesso para formato tabular. Total de colunas: {len(df.columns)}")
            logger.info(
                f"Estatísticas de colunas: {total_columns_original} colunas potenciais, {removed_columns} removidas, {final_columns} mantidas")

            # Upload to GCP with credentials and folder
            upload_csv_to_bucket(
                data=df,
                bucket_name=BUCKET_NAME,
                blob_name=CSV_FILENAME,
                folder_path=FOLDER_PATH,
                credentials_path=GOOGLE_APPLICATION_CREDENTIALS,
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

    main()


def get_extraction_tasks():
    """
    Get the list of data extraction tasks for Vindi.
    
    Returns:
        list: List of task configurations
    """
    return [
        {
            'task_id': 'run_bills',
            'python_callable': run_bills
        },
        {
            'task_id': 'run_subscriptions',
            'python_callable': run_subscriptions
        },
        {
            'task_id': 'run_customers',
            'python_callable': run_customers
        }
    ]
