"""
RDCRM module for data extraction functions.
This module contains functions specific to the RDCRM integration.
"""

from core import gcs


def run(customer):
    """
    Extract stages data from RDCRM API and load it to GCS.
    """

    import csv
    import logging
    import re
    import time
    import tracemalloc
    import unicodedata
    from datetime import datetime, timedelta
    from io import StringIO
    import pathlib
    import os

    import requests
    from google.cloud import storage

    # Configuração de logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    # Configuração da chave de serviço
    SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH

    # Configurações da API RD Station CRM
    TOKEN = customer['token']
    BASE_URL = 'https://crm.rdstation.com/api/v1/'
    API_URL = BASE_URL + 'deals'
    DEAL_PIPELINES_URL = BASE_URL + 'deal_pipelines'
    DEAL_STAGES_URL = BASE_URL + 'deal_stages'
    LIMIT = 200
    BUCKET_NAME = customer['bucket_name']

    def normalize_text(text: str) -> str:
        """Normaliza texto"""
        if not isinstance(text, str): text = str(text)
        text = text.lower()
        text = unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('ASCII')
        return text

    def normalize_column_name(col_name: str) -> str:
        """Normaliza nomes de colunas"""
        col_name = normalize_text(col_name)
        col_name = re.sub(r'[^a-z0-9_]', '_', col_name)
        return re.sub(r'_+', '_', col_name).strip('_')

    def clean_name(name):
        if name is None:
            return ''
        # Limpa espaços extras e caracteres especiais
        return " ".join(name.replace("\t", " ").split()).replace("•", "")

    def extract_nested_values(deal):
        new_fields = {}

        # Campanha
        campaign = deal.pop('campaign', None)
        if campaign:
            new_fields['campaign_name'] = clean_name(campaign.get('name', ''))

        # Contatos
        contacts = deal.pop('contacts', None)
        if contacts:
            contact = contacts[0]  # Assumindo que pegamos os detalhes do primeiro contato
            new_fields['contact_name'] = clean_name(contact.get('name', ''))
            contact_emails = contact.get('emails', [])
            contact_email = contact_emails[0]['email'] if contact_emails else ''
            new_fields['contact_email'] = contact_email

        # Campos personalizados do negócio - com lógica de renomeação
        custom_field_labels = {}  # Armazena o label e o índice para renomeação
        deal_custom_fields = deal.pop('deal_custom_fields', [])
        for field in deal_custom_fields:
            label = clean_name(field['custom_field'].get('label', ''))
            # Normaliza o nome do campo personalizado
            normalized_label = normalize_column_name(label)
            value = ', '.join(field['value']) if 'value' in field and isinstance(field['value'], list) else str(
                field.get('value', ''))
            if normalized_label:  # Somente adiciona se o label normalizado não estiver vazio
                custom_field_labels[normalized_label] = value

        # Adiciona campos customizados com nomes normalizados
        for normalized_label, value in custom_field_labels.items():
            new_fields[normalized_label] = value

        # Motivo da perda do negócio
        deal_lost_reason = deal.pop('deal_lost_reason', None)
        if deal_lost_reason:
            new_fields['deal_lost_reason_name'] = clean_name(deal_lost_reason.get('name', ''))

        # Produtos do negócio
        deal_products = deal.pop('deal_products', [])
        for i, product in enumerate(deal_products, 1):
            new_fields[f'product_id_{i}'] = product.get('product_id', '')
            new_fields[f'product_name_{i}'] = clean_name(product.get('name', ''))
            new_fields[f'product_description_{i}'] = clean_name(product.get('description', ''))
            new_fields[f'product_base_price_{i}'] = product.get('base_price', 0)
            new_fields[f'product_price_{i}'] = product.get('price', 0)
            new_fields[f'product_discount_{i}'] = product.get('discount', 0)
            new_fields[f'product_discount_type_{i}'] = product.get('discount_type', '')
            new_fields[f'product_total_{i}'] = product.get('total', 0)

        # Fonte do negócio
        deal_source = deal.pop('deal_source', None)
        if deal_source:
            new_fields['deal_source_name'] = clean_name(deal_source.get('name', ''))

        # Etapa do negócio
        deal_stage = deal.pop('deal_stage', None)
        if deal_stage:
            new_fields['deal_stage_id'] = deal_stage.get('id', '')
            new_fields['deal_stage_name'] = clean_name(deal_stage.get('name', ''))

        # Organização
        organization = deal.pop('organization', None)
        if organization:
            new_fields['organization_name'] = clean_name(organization.get('name', ''))

        # Usuário
        user = deal.pop('user', None)
        if user:
            new_fields['user_id'] = user.get('id', '')
            new_fields['user_name'] = clean_name(user.get('name', ''))

        return new_fields

    def clean_deal_data(deals):
        cleaned_deals = []
        # Mapeamento para a renomeação de colunas
        rename_map = {
            'closed_at': 'deal_closed_at',
            'amount_total': 'deal_amount_total',
            'amount_unique': 'deal_amount_unique',
            'created_at': 'deal_created_at',
            'id': 'deal_id',
            'name': 'deal_name',
            'updated_at': 'deal_updated_at',
        }

        for deal in deals:
            deal_cleaned = {}
            # Remove as colunas especificadas e renomeia conforme necessário
            for k, v in deal.items():
                if k not in ["campaign", "contacts", "deal_custom_fields", "deal_lost_reason", "deal_products",
                             "deal_source", "deal_stage", "organization", "user", "next_task", "_id", "markup",
                             "markup_created", "markup_last_activities", "prediction_date", "rating", "stop_time_limit",
                             "user_changed", "amount_monthly"]:
                    new_key = rename_map.get(k, k)  # Renomeia a coluna se necessário

                    # Limpa e ajusta o texto, caso seja uma string
                    v = clean_name(v) if isinstance(v, str) else v
                    # Normaliza o nome da chave
                    normalized_key = normalize_column_name(new_key)
                    deal_cleaned[normalized_key] = v

            # Processa campos aninhados e atualiza o dicionário do negócio
            nested_fields = extract_nested_values(deal)
            deal_cleaned.update(nested_fields)
            cleaned_deals.append(deal_cleaned)

        return cleaned_deals

    def compile_fieldnames(data):
        # Lista de campos base já normalizada
        base_fields = [
            'deal_source_name', 'campaign_name', 'deal_id', 'deal_name', 'deal_created_at',
            'deal_updated_at', 'deal_closed_at', 'last_activity_at', 'last_activity_content',
            'deal_amount_unique', 'deal_amount_total', 'deal_stage_id', 'deal_stage_name',
            'deal_lost_reason_name', 'win', 'organization_name', 'contact_email',
            'contact_name', 'hold', 'interactions', 'user_id', 'user_name'
        ]

        # Certifique-se de que todos os campos base estejam normalizados
        normalized_base_fields = [normalize_column_name(field) for field in base_fields]

        custom_field_names = set()
        max_product_index = 0

        for deal in data:
            # Extrair campos personalizados (que já estão normalizados no processo de limpeza)
            custom_field_names.update(
                key for key in deal.keys()
                if key not in normalized_base_fields and not key.startswith('product_')
            )

            # Extrair índices de produtos
            product_indices = [
                int(key.split('_')[-1]) for key in deal.keys() if key.startswith('product_')
            ]
            if product_indices:
                max_product_index = max(max_product_index, max(product_indices))

        sorted_custom_field_names = sorted(custom_field_names)
        product_fields_ordered = []
        product_fields_template = [
            'product_id_{index}',
            'product_name_{index}',
            'product_base_price_{index}',
            'product_price_{index}',
            'product_discount_{index}',
            'product_discount_type_{index}',
            'product_description_{index}',
            'product_total_{index}'
        ]

        for index in range(1, max_product_index + 1):
            for field in product_fields_template:
                product_fields_ordered.append(field.format(index=index))

        return normalized_base_fields + sorted_custom_field_names + product_fields_ordered

    def upload_to_gcs(bucket_name, destination_blob_name, content, storage_client):
        """Faz o upload do conteúdo para o Google Cloud Storage."""
        try:
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(destination_blob_name)

            blob.upload_from_string(content, 'text/csv')
            print(f"Arquivo {destination_blob_name} enviado para o bucket {bucket_name}.")
        except Exception as e:
            print(e)
            raise

    def get_earliest_date():
        """Define uma data de início fixa de 5 anos atrás"""
        print("Usando data fixa de 5 anos atrás como ponto de partida para a coleta")
        # Retorna uma data definida como 5 anos atrás
        return datetime.now() - timedelta(days=365 * 5)

    def fetch_all_deals_for_period(start_date, end_date):
        """Busca as negociações para um período específico"""
        all_deals = []
        page = 1
        has_more = True

        while has_more:
            url = f"{API_URL}?token={TOKEN}&created_at_period=true&start_date={start_date}&end_date={end_date}&limit={LIMIT}&page={page}"
            print(f"{start_date} até {end_date}: Página {page}...")

            try:
                response = requests.get(url)
                if response.status_code == 200:
                    data = response.json()
                    deals = data.get('deals', [])
                    if deals:
                        all_deals.extend(clean_deal_data(deals))
                        print(f"{start_date} até {end_date}: Página {page} - Coletados {len(deals)} registros.")
                        page += 1
                        has_more = len(deals) == LIMIT
                    else:
                        print(f"Nenhum registro encontrado para o período {start_date} até {end_date}.")
                        has_more = False
                elif response.status_code == 429:
                    print("Atingido o limite de taxa. Aguardando antes de tentar novamente...")
                    time.sleep(60)
                else:
                    print(f"Erro na solicitação: {response.status_code}")
                    if response.text:
                        print(f"Detalhes: {response.text}")
                    has_more = False
            except Exception as e:
                print(f"Erro ao processar requisição: {e}")
                time.sleep(5)
                raise RuntimeError(f"Error envolvido no FETCH ALL DEALS FOR PERIOD: {e}")

        return all_deals

    # --- Funções para pipelines e etapas ---

    def fetch_deal_pipelines():
        """Busca todos os pipelines disponíveis."""
        params = {'token': TOKEN}
        response = requests.get(DEAL_PIPELINES_URL, params=params)
        if response.status_code == 200:
            pipelines = response.json()
            print(f"Total de pipelines coletados: {len(pipelines)}")
            for pipeline in pipelines:
                print(f"Pipeline ID: {pipeline['id']}, Nome: {pipeline['name']}")
            return pipelines
        else:
            logging.error(f"Erro ao buscar pipelines: {response.text}")
            return []

    def fetch_deal_stages(deal_pipeline_id):
        """Busca as etapas do funil de vendas para um dado ID de funil."""
        params = {
            'token': TOKEN,
            'deal_pipeline_id': deal_pipeline_id,
            'limit': LIMIT
        }
        response = requests.get(DEAL_STAGES_URL, params=params)
        if response.status_code == 200:
            print(f"Etapas coletadas para o pipeline ID: {deal_pipeline_id}")
            return response.json()['deal_stages']
        else:
            logging.error(f"Erro ao buscar dados para o pipeline {deal_pipeline_id}: {response.text}")
            return []

    def collect_all_stages():
        """Coleta todas as etapas de todos os pipelines."""
        pipelines = fetch_deal_pipelines()
        all_stages = []

        for pipeline in pipelines:
            pipeline_id = pipeline['id']
            pipeline_name = pipeline['name']

            stages = fetch_deal_stages(pipeline_id)
            for stage in stages:
                stage_info = {
                    'deal_stage_id': stage['id'],
                    'deal_stage_name': normalize_column_name(stage['name']),
                    'stage_order': stage['order'],
                    'pipeline_id': pipeline_id,
                    'pipeline_name': normalize_column_name(pipeline_name)
                }
                all_stages.append(stage_info)

        return all_stages

    def process_pipelines_and_stages(storage_client):
        """Processa pipelines e etapas e salva diretamente no GCS."""
        print("Iniciando coleta de pipelines e etapas...")
        all_stages = collect_all_stages()

        if all_stages:
            # Normalizar nomes de campos
            normalized_stages = []
            for stage in all_stages:
                normalized_stage = {normalize_column_name(k): v for k, v in stage.items()}
                normalized_stages.append(normalized_stage)

            # Preparar conteúdo CSV
            csv_file = StringIO()
            writer = csv.DictWriter(csv_file, fieldnames=normalized_stages[0].keys())
            writer.writeheader()
            for stage in normalized_stages:
                writer.writerow(stage)

            csv_content = csv_file.getvalue()
            destination_blob_name = 'Stages/deal_stages.csv'

            # Fazer upload para GCS
            upload_to_gcs(BUCKET_NAME, destination_blob_name, csv_content, storage_client)
            print(f"Total de {len(normalized_stages)} etapas de funil processadas e salvas.")
        else:
            print("Nenhuma informação de pipelines e etapas foi coletada.")

    def main():
        try:
            tracemalloc.start()
            start_time = time.time()

            # Configurar cliente do GCS com credenciais explícitas
            # storage_client = storage.Client.from_service_account_json(SERVICE_ACCOUNT_PATH)
            storage_client = storage.Client(project=customer['project_id'])

            # Processar pipelines e etapas (independente)
            process_pipelines_and_stages(storage_client)

            # Coleta de negociações
            all_data = []

            # Define data de início como 5 anos atrás
            current_date = get_earliest_date()
            now = datetime.now()

            print(f"Iniciando coleta de negociações a partir de: {current_date}")

            # Coleta dados em janelas de tempo de 30 dias
            while current_date < now:
                start_date_str = current_date.strftime('%Y-%m-%dT%H:%M:%S')
                end_date = current_date + timedelta(days=30)
                if end_date > now:
                    end_date = now
                end_date_str = end_date.strftime('%Y-%m-%dT%H:%M:%S')

                deals = fetch_all_deals_for_period(start_date_str, end_date_str)
                all_data.extend(deals)

                print(f"Período {start_date_str} até {end_date_str}: Coletados {len(deals)} registros.")
                current_date = end_date

            if not all_data:
                print("Nenhum dado de negócios foi coletado. Verifique as credenciais e os parâmetros da API.")
                return

            # Compilar nomes de campo normalizados
            fieldnames = compile_fieldnames(all_data)

            csv_file = StringIO()
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writeheader()
            for deal in all_data:
                # Garantir que todos os nomes de campos estejam normalizados
                normalized_deal = {}
                for k, v in deal.items():
                    normalized_key = normalize_column_name(k)
                    normalized_deal[normalized_key] = v
                writer.writerow(normalized_deal)

            csv_content = csv_file.getvalue()
            destination_blob_name = 'Deals/rd_crm_deals.csv'

            # Upload usando o cliente configurado com credenciais explícitas
            upload_to_gcs(BUCKET_NAME, destination_blob_name, csv_content, storage_client)

            end_time = time.time()
            current, peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()

            print(f"Dados coletados com sucesso. {len(all_data)} negociações processadas.")
            print(f"Execução completada em {end_time - start_time:.2f} segundos.")
            print(f"Pico de uso de memória: {peak / 1024 ** 2:.2f} MB")
        except Exception as e:
            raise RuntimeError(f"Error envolvido no MAIN: {e}")

    # INICIAR
    main()


def get_extraction_tasks():
    """
    Get the list of data extraction tasks for RDCRM.

    Returns:
        list: List of task configurations
    """
    return [
        {
            'task_id': 'run',
            'python_callable': run
        }
    ]
