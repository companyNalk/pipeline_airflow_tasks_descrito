"""
RDCRM module for data extraction functions.
This module contains functions specific to the RDCRM integration.
"""

import requests
import csv
import logging
import time
import pandas as pd
import tracemalloc
from datetime import datetime, timedelta
from google.cloud import storage, bigquery
from io import StringIO
from core import gcs

def run_stages(customer):
    """
    Extract stages data from RDCRM API and load it to GCS.
    
    Args:
        customer (dict): Customer information dictionary
    """
    # Defina o token de autenticação e outras variáveis
    TOKEN = customer['token']
    BASE_URL = 'https://crm.rdstation.com/api/v1/'
    DEAL_PIPELINES_URL = BASE_URL + 'deal_pipelines'
    DEAL_STAGES_URL = BASE_URL + 'deal_stages'
    LIMIT = '200'
    BUCKET_NAME = customer['bucket_name']
    FOLDER_NAME = 'deal_stage'
    LOCAL_CSV_FILE_NAME = customer['project_id'] +'_rd_crm_deal_stage.csv'
    GCS_CSV_FILE_NAME = 'rd_crm_deal_stage.csv'

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
        print('deals stage', response)
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
                    'deal_stage_name': stage['name'],
                    'stage_order': stage['order'],
                    'pipeline_id': pipeline_id,
                    'pipeline_name': pipeline_name
                }
                all_stages.append(stage_info)

        return all_stages

    def save_to_csv(data):
        """Salva os dados coletados em um arquivo CSV."""
        keys = data[0].keys() if data else []
        if keys:
            with open(LOCAL_CSV_FILE_NAME, 'w', newline='', encoding='utf-8') as output_file:
                dict_writer = csv.DictWriter(output_file, fieldnames=keys, delimiter=';', quoting=csv.QUOTE_ALL)
                dict_writer.writeheader()
                dict_writer.writerows(data)
            logging.info(f'Dados salvos com sucesso em "{LOCAL_CSV_FILE_NAME}". Total de registros: {len(data)}')
        else:
            logging.warning("Nenhum dado para salvar.")

    def upload_to_gcs():
        credentials = gcs.load_credentials_from_env()
        gcs.write_file_to_gcs(
            bucket_name=BUCKET_NAME,
            local_file_path=LOCAL_CSV_FILE_NAME,
            destination_name=f"{FOLDER_NAME}/{GCS_CSV_FILE_NAME}",
            credentials=credentials
        )

    all_stages = collect_all_stages()
    save_to_csv(all_stages)
    upload_to_gcs()


def run_deals(customer):
    """
    Extract deals data from RDCRM API and load it to GCS.
    
    Args:
        customer (dict): Customer information dictionary
    """
    TOKEN = customer['token']
    API_URL = 'https://crm.rdstation.com/api/v1/deals'
    START_DATE = datetime.strptime('2024-03-01T15:00:00', '%Y-%m-%dT%H:%M:%S')
    LIMIT = 200
    BUCKET_NAME = customer['bucket_name']

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
            value = ', '.join(field['value']) if 'value' in field and isinstance(field['value'], list) else str(field.get('value', ''))
            if label:  # Somente adiciona se o label não estiver vazio
                custom_field_labels[label] = value

        # Adiciona campos customizados com nomes renomeados
        for i, (label, value) in enumerate(custom_field_labels.items(), 1):
            new_fields[f'{label}'] = value  # Renomeia a coluna para o label do campo personalizado

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
                if k not in ["campaign", "contacts", "deal_custom_fields", "deal_lost_reason", "deal_products", "deal_source", "deal_stage", "organization", "user", "next_task", "_id", "markup", "markup_created", "markup_last_activities", "prediction_date", "rating", "stop_time_limit", "user_changed", "amount_monthly"]:
                    new_key = rename_map.get(k, k)  # Renomeia a coluna se necessário
                    
                    # Limpa e ajusta o texto, caso seja uma string
                    v = clean_name(v) if isinstance(v, str) else v
                    deal_cleaned[new_key] = v
            
            # Processa campos aninhados e atualiza o dicionário do negócio
            nested_fields = extract_nested_values(deal)
            deal_cleaned.update(nested_fields)
            cleaned_deals.append(deal_cleaned)
        
        return cleaned_deals

    def compile_fieldnames(data):
        base_fields = [
            'deal_source_name', 'campaign_name', 'deal_id', 'deal_name', 'deal_created_at', 
            'deal_updated_at', 'deal_closed_at', 'last_activity_at', 'last_activity_content', 
            'deal_amount_unique', 'deal_amount_total', 'deal_stage_id', 'deal_stage_name', 
            'deal_lost_reason_name', 'win', 'organization_name', 'contact_email', 
            'contact_name', 'hold', 'interactions', 'user_id', 'user_name'
        ]
        custom_field_names = set()
        max_product_index = 0
        for deal in data:
            custom_field_names.update(
                key for key in deal.keys() if key not in base_fields and not key.startswith('product_')
            )
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

        return base_fields + sorted_custom_field_names + product_fields_ordered

    def fetch_all_deals_for_period(start_date, end_date):
        all_deals = []
        page = 1
        has_more = True
        while has_more:
            url = f"{API_URL}?token={TOKEN}&created_at_period=true&start_date={start_date}&end_date={end_date}&limit={LIMIT}&page={page}"
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                deals = data.get('deals', [])
                all_deals.extend(clean_deal_data(deals))
                print(f"{start_date} até {end_date}: Página {page} - Coletados {len(deals)} registros.")
                page += 1
                has_more = len(deals) == LIMIT
            elif response.status_code == 429:
                print("Atingido o limite de taxa. Aguardando antes de tentar novamente...")
                time.sleep(60)
            else:
                print(f"Erro na solicitação: {response.status_code}")
                break
        return all_deals

    def main():
        tracemalloc.start()
        start_time = time.time()
        all_data = []

        current_date = START_DATE
        while current_date < datetime.now():
            start_date_str = current_date.strftime('%Y-%m-%dT%H:%M:%S')
            end_date = current_date + timedelta(days=30)
            end_date_str = end_date.strftime('%Y-%m-%dT%H:%M:%S')
            deals = fetch_all_deals_for_period(start_date_str, end_date_str)
            all_data.extend(deals)
            current_date = end_date

        fieldnames = compile_fieldnames(all_data)
        csv_file = open(customer['project_id'] + '_csv.csv', 'w+')
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        for deal in all_data:
            writer.writerow(deal)
        destination_blob_name = 'Deals/rd_crm_deals.csv'
        csv_file.close()
        
        def upload_to_gcs():
            credentials = gcs.load_credentials_from_env()
            gcs.write_file_to_gcs(
                bucket_name=BUCKET_NAME,
                local_file_path=csv_file.name,
                destination_name=destination_blob_name,
                credentials=credentials
            )

        upload_to_gcs()

        end_time = time.time()
        current, peak = tracemalloc.get_traced_memory() 
        tracemalloc.stop()

        print(f"Dados coletados com sucesso. {len(all_data)} registros processados.")
        print(f"Execução completada em {end_time - start_time:.2f} segundos.")
        print(f"Pico de uso de memória: {peak / 1024**2:.2f} MB")

    main()


def get_extraction_tasks():
    """
    Get the list of data extraction tasks for RDCRM.
    
    Returns:
        list: List of task configurations
    """
    return [
        {
            'task_id': 'run_stages',
            'python_callable': run_stages
        },
        {
            'task_id': 'run_deals',
            'python_callable': run_deals
        }
    ]
