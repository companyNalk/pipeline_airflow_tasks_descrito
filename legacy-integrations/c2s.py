"""
Contact2Sale module for data extraction functions.
This module contains functions specific to the Contact2Sale integration.
"""

from core import gcs


def run(customer):
    import requests
    import csv
    import time
    from datetime import datetime
    from google.cloud import storage
    import os
    import tracemalloc
    import unicodedata
    import re
    import concurrent.futures
    import pathlib
    # Variáveis compartilhadas
    BUCKET_NAME = customer['bucket_name']
    FOLDER_NAME = customer['folder_name']
    SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()
    API_TOKEN = customer['api_token']
    API_HEADERS = {
        "Authorization": f"Bearer {API_TOKEN}",
        "Content-Type": "application/json"
    }

    # Configuração do ambiente
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH

    # Função de normalização de colunas
    def normalize_column_name(name):
        nfkd = unicodedata.normalize('NFKD', name)
        ascii_name = nfkd.encode('ASCII', 'ignore').decode('ASCII')
        cleaned = re.sub(r"[^\w\s]", "", ascii_name)
        return re.sub(r"\s+", "_", cleaned).lower()

    # Funções para monitoramento de desempenho
    def monitor_performance():
        tracemalloc.start()
        start_time = time.time()
        return start_time

    def report_performance(start_time, operation_name):
        end_time = time.time()
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        print(f"\n[{operation_name}] Tempo total de execução: {end_time - start_time:.2f} segundos")
        print(f"[{operation_name}] Uso de memória atual: {current / 10 ** 6:.2f} MB")
        print(f"[{operation_name}] Pico de uso de memória: {peak / 10 ** 6:.2f} MB")

    # Função genérica para upload de dados para o GCS
    def upload_to_gcs(data, filename, headers=None):
        if not data:
            print(f"[WARNING] Sem dados para enviar para {filename}")
            return False

        try:
            storage_client = storage.Client(project=customer['project_id'])
            bucket = storage_client.bucket(BUCKET_NAME)
            blob = bucket.blob(f"{FOLDER_NAME}/{filename}")

            # Escrevendo diretamente no GCS
            with blob.open("w", newline="", encoding="utf-8") as file:
                writer = csv.writer(file, delimiter=";", quotechar='"', quoting=csv.QUOTE_ALL)

                # Se os cabeçalhos foram fornecidos, use-os
                if headers:
                    # Normalizar os cabeçalhos
                    normalized_headers = [normalize_column_name(header) for header in headers]
                    writer.writerow(normalized_headers)
                elif data:
                    # Caso contrário, extraia os cabeçalhos dos dados
                    if isinstance(data[0], dict):
                        # Extrair todas as chaves únicas de todos os dicionários
                        all_keys = set()
                        for item in data:
                            all_keys.update(item.keys())

                        # Normalizar os cabeçalhos
                        headers = sorted(list(all_keys))
                        normalized_headers = [normalize_column_name(header) for header in headers]
                        writer.writerow(normalized_headers)

                # Escrever os dados
                for item in data:
                    if isinstance(item, dict):
                        if headers:
                            row = [item.get(key, "") for key in headers]
                        else:
                            row = list(item.values())
                    else:
                        row = item
                    writer.writerow(row)

            print(f"[SUCCESS] Arquivo {filename} salvo com sucesso em gs://{BUCKET_NAME}/{FOLDER_NAME}")
            return True
        except Exception as e:
            print(f"[ERROR] Falha ao fazer upload para {filename}: {str(e)}")
            raise

    # Funções para coleta de dados dos diferentes endpoints

    def collect_companies():
        base_url = "https://api.contact2sale.com/integration/me"

        print("[INFO] Iniciando a coleta de empresas...")
        response = requests.get(base_url, headers=API_HEADERS)

        if response.status_code != 200:
            print(f"[ERROR] Erro na requisição: {response.status_code} - {response.text}")
            return []

        data = response.json().get("data", {})

        # Processando a empresa principal e as subempresas
        companies = []
        main_company = {
            "company_name": data.get("company_name", ""),
            "company_id": data.get("company_id", "")
        }
        companies.append(main_company)

        sub_companies = data.get("sub_companies", [])
        for sub in sub_companies:
            companies.append({
                "company_name": sub.get("company_name", ""),
                "company_id": sub.get("company_id", "")
            })

        print(f"[SUCCESS] Coleta de empresas finalizada. Total: {len(companies)}")
        return companies

    def collect_leads():
        base_url = "https://api.contact2sale.com/integration/leads"

        page = 1
        per_page = 50
        all_leads = []

        # Definindo a data de início como janeiro deste ano
        start_date = datetime((datetime.now().year - 1), 1, 1).strftime("%Y-%m-%dT%H:%M:%SZ")

        print("[INFO] Iniciando a coleta de leads...")
        while True:
            params = {
                "page": page,
                "perpage": per_page,
                "sort": "-created_at",
                "created_gte": start_date
            }

            response = requests.get(base_url, headers=API_HEADERS, params=params)

            if response.status_code != 200:
                print(f"[ERROR] Erro na requisição: {response.status_code} - {response.text}")
                break

            data = response.json()
            leads = data.get("data", [])

            if not leads:
                print("[INFO] Todas as páginas foram coletadas.")
                break

            all_leads.extend(leads)
            print(f"[INFO] Página {page} coletada com {len(leads)} leads. Total acumulado: {len(all_leads)}")
            page += 1
            time.sleep(1)  # Intervalo para evitar sobrecarga na API

        print(f"[SUCCESS] Coleta de leads finalizada. Total: {len(all_leads)}")

        # Processar os dados para extração apenas dos atributos
        processed_leads = []

        # Identificar todos os campos possíveis para garantir consistência no CSV
        all_fields = set()
        for lead in all_leads:
            attributes = lead.get("attributes", {})
            all_fields.update(attributes.keys())

        # Ordenar os campos para garantir ordem consistente
        field_list = sorted(list(all_fields))

        # Processar cada lead
        for lead in all_leads:
            attributes = lead.get("attributes", {})
            processed_lead = {field: attributes.get(field, "") for field in field_list}
            processed_leads.append(processed_lead)

        return processed_leads, field_list

    def collect_sellers():
        base_url = "https://api.contact2sale.com/integration/sellers"

        print("[INFO] Iniciando a coleta de sellers...")
        response = requests.get(base_url, headers=API_HEADERS)

        if response.status_code != 200:
            print(f"[ERROR] Erro na requisição: {response.status_code} - {response.text}")
            return []

        # Resposta esperada como lista
        sellers = response.json()
        print(f"[SUCCESS] Coleta de sellers finalizada. Total: {len(sellers)}")
        return sellers

    def collect_tags():
        base_url = "https://api.contact2sale.com/integration/tags"

        print("[INFO] Iniciando a coleta de tags...")
        response = requests.get(base_url, headers=API_HEADERS)

        if response.status_code != 200:
            print(f"[ERROR] Erro na requisição: {response.status_code} - {response.text}")
            return []

        data = response.json()
        tags = data.get("data", [])

        print(f"[SUCCESS] Coleta de tags finalizada. Total: {len(tags)}")
        return tags

    # Funções para processamento de cada endpoint
    def process_companies():
        start_time = monitor_performance()

        companies = collect_companies()
        if companies:
            headers = ["company_name", "company_id"]
            upload_to_gcs(companies, "companies.csv", headers)

        report_performance(start_time, "Companies")

    def process_leads():
        start_time = monitor_performance()

        leads, headers = collect_leads()
        if leads:
            upload_to_gcs(leads, "leads.csv", headers)

        report_performance(start_time, "Leads")

    def process_sellers():
        start_time = monitor_performance()

        sellers = collect_sellers()
        if sellers:
            headers = ["id", "name", "phone", "company"]
            upload_to_gcs(sellers, "sellers.csv", headers)

        report_performance(start_time, "Sellers")

    def process_tags():
        start_time = monitor_performance()

        tags = collect_tags()
        if tags:
            headers = ["tag_id", "name"]
            upload_to_gcs(tags, "tags.csv", headers)

        report_performance(start_time, "Tags")

    # Função principal para execução sequencial
    def run_sequential():
        print("[START] Iniciando coleta sequencial dos dados do Contact2Sale")
        global_start = time.time()

        process_companies()
        process_leads()
        process_sellers()
        process_tags()

        global_end = time.time()
        print(f"\n[COMPLETE] Processo finalizado. Tempo total: {global_end - global_start:.2f} segundos")

    # Função principal para execução paralela
    def run_parallel():
        print("[START] Iniciando coleta paralela dos dados do Contact2Sale")
        global_start = time.time()

        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            futures = [
                executor.submit(process_companies),
                executor.submit(process_leads),
                executor.submit(process_sellers),
                executor.submit(process_tags)
            ]

            concurrent.futures.wait(futures)

        global_end = time.time()
        print(f"\n[COMPLETE] Processo finalizado. Tempo total: {global_end - global_start:.2f} segundos")

    run_sequential()


def get_extraction_tasks():
    """
    Get the list of data extraction tasks for Contact2Sale.
    
    Returns:
        list: List of task configurations
    """
    return [
        {
            'task_id': 'run',
            'python_callable': run
        }
    ]
