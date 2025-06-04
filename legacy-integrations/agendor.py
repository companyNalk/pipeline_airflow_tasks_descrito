"""
Agendor CRM module for data extraction.
This module handles the extraction of data from Agendor CRM API,
including deals, organizations, and people.
"""

from core import gcs


def get_data(customer, endpoint):
    import concurrent.futures
    import os
    import time
    from queue import Queue, Empty
    from threading import Lock
    from urllib.parse import urlparse, parse_qs

    import pandas as pd
    import requests
    from google.cloud import storage
    import pathlib
    import logging

    logger = logging.getLogger(__name__)

    # LOGGING
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger("Agendor CRM")

    # GCP
    BUCKET_NAME = customer['bucket_name']
    FOLDER_PATH = endpoint
    SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH

    # APP
    CSV_FILENAME = f"{endpoint}.csv"

    # API
    API_BASE_URL = customer['api_base_url']
    API_TOKEN = customer['api_token']
    AUTH_TYPE = "Token"
    ROUTER = endpoint

    # PARALLELISM
    MAX_WORKERS = 10
    PROCESSED_PAGES_LOCK = Lock()
    processed_pages = set()

    def fetch_api_data(page_url=None):
        """
        Fetches data from the API with URL-based pagination support
        """
        headers = {
            "Authorization": f"{AUTH_TYPE} {API_TOKEN}",
            "accept": "application/json"
        }

        # If no specific URL is provided, use the base URL
        if not page_url:
            url = f"{API_BASE_URL}/{ROUTER}"
        else:
            url = page_url

        # Make request with retry logic
        max_retries = 5
        retries = 0

        while retries < max_retries:
            try:
                response = requests.get(url, headers=headers)
                response.raise_for_status()
                return response.json()

            except requests.exceptions.HTTPError as e:
                if response.status_code == 429:
                    retries += 1
                    wait_time = 5 * retries  # Exponential backoff
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

        logger.error(f"Falha após {max_retries} tentativas para URL: {url}")
        raise Exception(f"Falha após {max_retries} tentativas para URL: {url}")

    def extract_page_number(url):
        """
        Extracts the page number from a URL
        """
        if not url:
            return "1"

        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)

        if 'page' in query_params:
            return query_params['page'][0]
        return "1"

    def process_page(page_url, url_queue, results_list, processed_pages_set):
        """
        Processes a specific page and chains to the next page
        """
        # Extract page number for logging
        page_num = extract_page_number(page_url)

        # Avoid processing the same page twice
        with PROCESSED_PAGES_LOCK:
            if page_url in processed_pages_set:
                logger.debug(f"Página {page_num} já foi processada. Pulando.")
                return None
            processed_pages_set.add(page_url)

        try:
            logger.debug(f"Buscando página {page_num}...")
            response = fetch_api_data(page_url)

            # Add results to the shared list
            if "data" in response and response["data"]:
                results_list.extend(response["data"])
                logger.info(f"Obtidos {len(response['data'])} registros na página {page_num}")

            # Check if there's a next page
            if "links" in response and "next" in response["links"] and response["links"]["next"]:
                next_url = response["links"]["next"]
                with PROCESSED_PAGES_LOCK:
                    if next_url not in processed_pages_set:
                        url_queue.put(next_url)
                        next_page = extract_page_number(next_url)
                        logger.debug(f"Adicionada próxima página {next_page} à fila")

            return response
        except Exception as e:
            logger.error(f"Erro ao buscar página {page_num}: {e}")
            return None

    def collect_all_data():
        """
        Collects all data using parallel processing with a queue
        """
        logger.info(f"Coletando dados...")

        all_data = []
        url_queue = Queue()
        processed_pages = set()

        # Fetch the first page to start the process
        try:
            logger.info(f"Buscando página inicial...")
            response = fetch_api_data()

            if "data" in response and response["data"]:
                all_data.extend(response["data"])
                logger.info(f"Obtidos {len(response['data'])} registros na página inicial")

            # If there's a next page, add to queue
            if "links" in response and "next" in response["links"] and response["links"]["next"]:
                next_url = response["links"]["next"]
                url_queue.put(next_url)
                logger.info(f"Adicionada próxima página à fila para processamento paralelo")

            # Log total if available
            if "meta" in response and "totalCount" in response["meta"]:
                total_count = response["meta"]["totalCount"]
                logger.info(f"Total de registros na API: {total_count}")

        except Exception as e:
            logger.error(f"Erro ao buscar página inicial: {e}")
            return all_data

        # If no more pages, return the data
        if url_queue.empty():
            logger.info(f"Apenas uma página de resultados")
            return all_data

        # Process pages in parallel using the URL queue
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # List to track running tasks
            futures = set()

            # Main loop: process until there are no more URLs in the queue and no running tasks
            while True:
                # Add new tasks until maximum workers is reached or queue is empty
                while len(futures) < MAX_WORKERS and not url_queue.empty():
                    try:
                        page_url = url_queue.get(block=False)
                        future = executor.submit(process_page, page_url, url_queue, all_data, processed_pages)
                        futures.add(future)
                    except Empty:
                        break  # Empty queue

                # If no running tasks, we're done
                if not futures:
                    break

                # Wait for at least one task to complete
                done, futures = concurrent.futures.wait(
                    futures,
                    return_when=concurrent.futures.FIRST_COMPLETED
                )

                # Check results of completed tasks
                for future in done:
                    try:
                        future.result()  # Just to catch exceptions
                    except Exception as e:
                        logger.error(f"Exceção em worker: {e}")
                        raise

                # Periodic progress check
                if len(all_data) % 1000 == 0 and len(all_data) > 0:
                    logger.info(f"Progresso: {len(all_data)} registros coletados, {url_queue.qsize()} páginas na fila")

        logger.info(f"Coleta de dados finalizada. Total: {len(all_data)} registros.")
        return all_data

    def convert_json_to_csv(data, output_file=None, csv_separator=";"):
        """
        Converts JSON data of any depth to a normalized DataFrame,
        keeping original keys (converted to lowercase) and adding numbering only for list items.
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

        # Save to file if path is provided
        if output_file:
            try:
                df.to_csv(output_file, index=False, sep=csv_separator)
                logger.info(f"Arquivo CSV normalizado criado: {output_file} com {len(df.columns)} colunas")
            except Exception as e:
                logger.error(f"Erro ao salvar CSV: {e}")
                raise

        logger.info(f"Processamento finalizado. DataFrame criado com {len(df)} linhas e {len(df.columns)} colunas")
        return df

    def upload_csv_to_bucket(data, bucket_name, blob_name, folder_path=None, separator=";"):
        """
        Uploads a DataFrame as CSV to a GCP bucket
        """
        try:
            # Initialize storage client
            storage_client = storage.Client(project=customer['project_id'])
            bucket = storage_client.bucket(BUCKET_NAME)

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
            logger.info(f"Arquivo com tamanho: ({size_kb:.2f} KB), disponivel em: '{bucket_name}/{full_blob_path}'")
            return True

        except Exception as e:
            logger.error(f"Erro ao enviar arquivo para GCP: {e}")
            raise

    def main():
        try:
            start_time = time.time()

            # Get all data
            logger.info("Iniciando coleta de todos os dados")
            all_data = collect_all_data()
            record_count = len(all_data)

            # Check if there's data to process
            if not all_data:
                logger.warning("Nenhum registro encontrado")
                return "Nenhum registro encontrado"

            # Using normalization function to handle JSON data
            logger.info(f"Convertendo {len(all_data)} registros de dados JSON complexos...")

            # Flatten JSON data with the new function
            df = convert_json_to_csv(all_data)
            logger.info(f"Dados JSON convertidos com sucesso para formato tabular. Total de colunas: {len(df.columns)}")

            # Upload to GCP with credentials and folder
            upload_csv_to_bucket(
                data=df,
                bucket_name=BUCKET_NAME,
                blob_name=CSV_FILENAME,
                folder_path=FOLDER_PATH,
                separator=";"
            )

            elapsed_time = time.time() - start_time
            result_message = f"Processamento concluído com sucesso em {elapsed_time:.2f} segundos. {record_count} registros exportados em formato CSV normalizado."
            logger.info(result_message)
            return result_message

        except Exception as e:
            error_message = f"Erro durante o processamento: {str(e)}"
            logger.error(error_message)
            raise

    # START
    main()


def run_deals(customer):
    get_data(customer, 'organizations')


def run_organizations(customer):
    get_data(customer, 'people')


def run_people(customer):
    get_data(customer, 'deals')


def get_extraction_tasks():
    """
    Get the list of data extraction tasks for Agendor CRM.
    
    Returns:
        list: List of task configurations
    """
    return [
        {
            'task_id': 'extract_organizations',
            'python_callable': run_organizations
        },
        {
            'task_id': 'extract_people',
            'python_callable': run_people
        },
        {
            'task_id': 'extract_deals',
            'python_callable': run_deals
        }
    ]
