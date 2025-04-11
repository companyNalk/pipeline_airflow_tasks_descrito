import concurrent.futures
import logging
import time
from threading import Lock
from typing import Dict, List

import pandas as pd
import requests

from commons.env_utils import load_env_file
from commons.google_cloud import GoogleCloud
from commons.utils import flatten_json, remove_empty_columns, normalize_keys, normalize_column_names
from generic.argument_manager import ArgumentManager

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_env_file()


def get_arguments():
    return (ArgumentManager("Script para coletar e processar dados da API MevBrasil")
            .add("API_BASE_URL", "API_BASE_URL", "URL base da API MevBrasil", required=True)
            .add("API_CLIENT_ID", "API_CLIENT_ID", "ID do cliente para autenticação na API", required=True)
            .add("API_CLIENT_SECRET", "API_CLIENT_SECRET", "Secret do cliente para autenticação na API", required=True)
            .add("LW_CLIENT", "LW_CLIENT", "Identificador do cliente LW", required=True)
            .add("BUCKET_NAME", "BUCKET_NAME", "Local para upload dos arquivos", required=True)
            .add("GOOGLE_APPLICATION_CREDENTIALS", "GOOGLE_APPLICATION_CREDENTIALS", "Caminho para credenciais GCP",
                 required=True)
            .parse())


# Obter configurações
args = get_arguments()

API_BASE_URL = args.API_BASE_URL.rstrip('/')
API_CLIENT_ID = args.API_CLIENT_ID
API_CLIENT_SECRET = args.API_CLIENT_SECRET
LW_CLIENT = args.LW_CLIENT
BUCKET_NAME = args.BUCKET_NAME
GOOGLE_APPLICATION_CREDENTIALS = args.GOOGLE_APPLICATION_CREDENTIALS
API_RATE_LIMIT = 100
ITEMS_PER_PAGE = 200

# PARALELISMO
MAX_WORKERS = 10
REQUEST_COUNTER_LOCK = Lock()
request_counter = 0
reset_time = time.time() + 60
BACKOFF_MULTIPLIER = 1.5

# Definição dos endpoints
ENDPOINTS = {
    "users": "v2/users",
    "courses": "v2/courses"
}


def get_auth_token():
    url = f"{API_BASE_URL}/oauth2/access_token"
    payload = {
        "grant_type": "client_credentials",
        "client_id": API_CLIENT_ID,
        "client_secret": API_CLIENT_SECRET
    }
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Lw-Client": LW_CLIENT
    }

    try:
        response = requests.post(url, data=payload, headers=headers)
        response.raise_for_status()
        return response.json().get("tokenData", {}).get("access_token")
    except Exception as e:
        logger.error(f"Falha na autenticação: {str(e)}")
        raise


def check_rate_limit():
    """Controla o limite de requisições por minuto"""
    global request_counter, reset_time

    with REQUEST_COUNTER_LOCK:
        current_time = time.time()

        if current_time >= reset_time:
            request_counter = 0
            reset_time = current_time + 60

        if request_counter >= API_RATE_LIMIT:
            wait_time = reset_time - current_time
            time.sleep(max(0, wait_time))
            request_counter = 0
            reset_time = time.time() + 60

        request_counter += 1


def fetch_data(endpoint: str, token_info: str, page: int = 1) -> Dict:
    """Busca dados de um endpoint específico usando o formato de paginação correto."""
    url = f"{API_BASE_URL}/{endpoint}"
    params = {
        "items_per_page": ITEMS_PER_PAGE,
        "page": page
    }
    headers = {
        "Authorization": f"Bearer {token_info}",
        "Content-Type": "application/json",
        "Lw-Client": LW_CLIENT
    }

    check_rate_limit()

    backoff = 1
    while True:
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            if hasattr(e.response, 'status_code') and e.response.status_code == 429:
                wait_time = backoff
                logger.warning(f"Rate limit atingido. Aguardando {wait_time}s...")
                time.sleep(wait_time)
                backoff *= BACKOFF_MULTIPLIER
                continue
            elif hasattr(e.response, 'status_code') and e.response.status_code == 401:
                logger.error("Token expirado ou inválido")
                raise
            else:
                logger.error(f"Erro na requisição: {e}")
                raise


def fetch_page(endpoint: str, token_info: str, page: int) -> Dict:
    """Busca uma página específica de um endpoint."""
    try:
        data = fetch_data(endpoint, token_info, page)
        items = data.get('data', [])
        meta = data.get('meta', {})
        total_pages = meta.get('totalPages', 1)

        logger.info(f"Endpoint {endpoint}: página {page}/{total_pages} com {len(items)} itens")
        return {
            'items': items,
            'meta': meta,
            'total_pages': total_pages
        }
    except Exception as e:
        logger.error(f"Erro ao buscar página {page} para endpoint {endpoint}: {str(e)}")
        raise


def fetch_all_pages(endpoint: str, token_info: str) -> List[Dict]:
    """Busca todas as páginas de um endpoint específico em paralelo."""
    all_items = []

    # Primeiro obter metadados da primeira página para saber o total de páginas
    first_page_result = fetch_page(endpoint, token_info, 1)
    all_items.extend(first_page_result['items'])

    total_pages = first_page_result['total_pages']

    if total_pages <= 1:
        return all_items

    # Criar uma lista de páginas para buscar em paralelo (páginas 2 até o final)
    remaining_pages = list(range(2, total_pages + 1))

    # Armazenar erros que ocorrem nas threads
    thread_errors = []

    # Buscar as páginas restantes em paralelo
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_page = {
            executor.submit(fetch_page, endpoint, token_info, page): page
            for page in remaining_pages
        }

        for future in concurrent.futures.as_completed(future_to_page):
            page_num = future_to_page[future]
            try:
                result = future.result()
                all_items.extend(result['items'])
            except Exception as e:
                thread_errors.append(f"Erro na página {page_num}: {str(e)}")

    # Se houve erros em alguma thread, lançar exceção
    if thread_errors:
        error_msg = "; ".join(thread_errors)
        logger.error(f"Erros na coleta de dados: {error_msg}")
        raise Exception(f"Falha ao coletar dados: {error_msg}")

    logger.info(f"Total de {len(all_items)} itens obtidos para {endpoint}")
    return all_items


def process_endpoint(endpoint_name: str, endpoint_path: str, token_info: str) -> Dict[str, List[Dict]]:
    """Processa um endpoint, obtém dados, salva CSV e envia para GCS."""
    logger.info(f"Processando endpoint: {endpoint_name}")

    raw_data = fetch_all_pages(endpoint_path, token_info)

    if not raw_data:
        logger.warning(f"Nenhum dado encontrado para o endpoint {endpoint_name}")
        return {endpoint_name: []}

    # Normalizar e processar dados
    normalized_data = normalize_keys(raw_data)
    flattened_data = [flatten_json(item) for item in normalized_data]
    df = pd.DataFrame(flattened_data)

    if df.empty:
        logger.warning(f"DataFrame vazio para {endpoint_name} após processamento")
        return {endpoint_name: []}

    # Normalizar e limpar dados
    df = normalize_column_names(df)
    df = remove_empty_columns(df)

    # Upload para Google Cloud Storage
    blob_name = f'{endpoint_name}/{endpoint_name}.csv'
    success = GoogleCloud.upload_csv(
        data=df,
        bucket_name=BUCKET_NAME,
        blob_name=blob_name,
        credentials_path=GOOGLE_APPLICATION_CREDENTIALS,
        separator=";"
    )

    if not success:
        logger.error(f"Falha ao enviar {blob_name} para GCS")
        raise Exception(f"Falha ao enviar arquivo para o Google Cloud Storage: {blob_name}")

    logger.info(f"Processamento de {endpoint_name} concluído: {len(df)} registros")
    return {endpoint_name: df.to_dict(orient='records')}


def main():
    try:
        logger.info(f"Iniciando coleta de dados da API MevBrasil")

        # Autenticação
        token_info = get_auth_token()
        logger.info("Autenticação realizada com sucesso")

        # Processar endpoints
        results = {}
        endpoint_errors = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_endpoint = {
                executor.submit(process_endpoint, endpoint_name, endpoint_path, token_info): endpoint_name
                for endpoint_name, endpoint_path in ENDPOINTS.items()
            }

            for future in concurrent.futures.as_completed(future_to_endpoint):
                endpoint_name = future_to_endpoint[future]
                try:
                    result = future.result()
                    results.update(result)
                except Exception as e:
                    error_msg = f"Falha no endpoint {endpoint_name}: {str(e)}"
                    logger.error(error_msg)
                    endpoint_errors.append(error_msg)
                    results[endpoint_name] = []

        # Resumo final
        logger.info("=== RESULTADOS ===")
        for endpoint_name, data in results.items():
            logger.info(f"{endpoint_name}: {len(data)} itens processados")

        # Se houve erros em algum endpoint, falhar a execução para o Airflow
        if endpoint_errors:
            error_summary = "; ".join(endpoint_errors)
            raise Exception(f"Falhas na execução: {error_summary}")

        logger.info("Processamento concluído com sucesso")

    except Exception as e:
        logger.error(f"Erro geral na execução do script: {str(e)}")
        raise


if __name__ == "__main__":
    main()
