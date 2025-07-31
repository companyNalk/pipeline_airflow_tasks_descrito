"""
RDCRM module for data extraction functions - VERSÃO COMPLETA CORRIGIDA.
Este módulo contém funções específicas para integração RDCRM com tratamento robusto de erros.
"""

import logging
import sys

from core import gcs


def run(customer):
    """
    Extract stages data from RDCRM API and load it to GCS.
    """

    try:
        import csv
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

        # Configuração de logging mais robusta
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(sys.stdout),
                logging.FileHandler('rdcrm_extraction.log', mode='a')
            ]
        )
        logger = logging.getLogger(__name__)

        # Validação de parâmetros obrigatórios
        required_fields = ['token', 'bucket_name', 'project_id']
        for field in required_fields:
            if not customer.get(field):
                logger.error(f"Campo obrigatório ausente: {field}")
                raise ValueError(f"Campo obrigatório ausente: {field}")

        # Configuração da chave de serviço com validação
        SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()

        if not os.path.exists(SERVICE_ACCOUNT_PATH):
            logger.warning(f"Arquivo de credenciais não encontrado: {SERVICE_ACCOUNT_PATH}")
            logger.info("Tentando usar credenciais do ambiente...")
        else:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH
            logger.info(f"Credenciais carregadas de: {SERVICE_ACCOUNT_PATH}")

        # Configurações da API RD Station CRM
        TOKEN = customer['token']
        BASE_URL = 'https://crm.rdstation.com/api/v1/'
        API_URL = BASE_URL + 'deals'
        DEAL_PIPELINES_URL = BASE_URL + 'deal_pipelines'
        DEAL_STAGES_URL = BASE_URL + 'deal_stages'
        LIMIT = 200
        BUCKET_NAME = customer['bucket_name']
        PROJECT_ID = customer['project_id']

        # Timeout padrão para requests
        REQUEST_TIMEOUT = 30
        MAX_RETRIES = 3

        def safe_request(url, max_retries=MAX_RETRIES, timeout=REQUEST_TIMEOUT):
            """Faz requisições HTTP com retry e tratamento de erro."""
            for attempt in range(max_retries):
                try:
                    logger.info(f"Tentativa {attempt + 1}/{max_retries}: {url}")
                    response = requests.get(url, timeout=timeout)

                    if response.status_code == 200:
                        return response
                    elif response.status_code == 429:
                        wait_time = 60 * (attempt + 1)
                        logger.warning(f"Rate limit atingido. Aguardando {wait_time}s...")
                        time.sleep(wait_time)
                    elif response.status_code in [401, 403]:
                        logger.error(f"Erro de autenticação: {response.status_code}")
                        logger.error(f"Response: {response.text}")
                        raise requests.exceptions.HTTPError(f"Erro de autenticação: {response.status_code}")
                    else:
                        logger.warning(f"Status code: {response.status_code}")
                        logger.warning(f"Response: {response.text}")

                except requests.exceptions.Timeout:
                    logger.warning(f"Timeout na tentativa {attempt + 1}")
                    if attempt == max_retries - 1:
                        raise
                    time.sleep(10 * (attempt + 1))

                except requests.exceptions.ConnectionError as e:
                    logger.warning(f"Erro de conexão na tentativa {attempt + 1}: {e}")
                    if attempt == max_retries - 1:
                        raise
                    time.sleep(5 * (attempt + 1))

                except Exception as e:
                    logger.error(f"Erro inesperado na requisição: {e}")
                    if attempt == max_retries - 1:
                        raise
                    time.sleep(5)

            raise requests.exceptions.RequestException(f"Falha após {max_retries} tentativas")

        def normalize_text(text: str) -> str:
            """Normaliza texto com tratamento de erro."""
            try:
                if not isinstance(text, str):
                    text = str(text) if text is not None else ''
                text = text.lower()
                text = unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('ASCII')
                return text
            except Exception as e:
                logger.warning(f"Erro ao normalizar texto '{text}': {e}")
                return str(text).lower() if text else ''

        def normalize_column_name(col_name: str) -> str:
            """Normaliza nomes de colunas com tratamento de erro."""
            try:
                col_name = normalize_text(col_name)
                col_name = re.sub(r'[^a-z0-9_]', '_', col_name)
                return re.sub(r'_+', '_', col_name).strip('_')
            except Exception as e:
                logger.warning(f"Erro ao normalizar nome da coluna '{col_name}': {e}")
                return 'unknown_column'

        def clean_name(name):
            """Limpa nomes com tratamento de erro."""
            try:
                if name is None:
                    return ''
                return " ".join(str(name).replace("\t", " ").split()).replace("•", "")
            except Exception as e:
                logger.warning(f"Erro ao limpar nome '{name}': {e}")
                return str(name) if name else ''

        def extract_nested_values(deal):
            """Extrai valores aninhados dos deals com tratamento de erro."""
            new_fields = {}

            try:
                # Campanha
                campaign = deal.pop('campaign', None)
                if campaign:
                    new_fields['campaign_name'] = clean_name(campaign.get('name', ''))

                # Contatos
                contacts = deal.pop('contacts', None)
                if contacts:
                    contact = contacts[0]
                    new_fields['contact_name'] = clean_name(contact.get('name', ''))
                    contact_emails = contact.get('emails', [])
                    contact_email = contact_emails[0]['email'] if contact_emails else ''
                    new_fields['contact_email'] = contact_email

                # Campos personalizados do negócio
                custom_field_labels = {}
                deal_custom_fields = deal.pop('deal_custom_fields', [])
                for field in deal_custom_fields:
                    try:
                        label = clean_name(field['custom_field'].get('label', ''))
                        normalized_label = normalize_column_name(label)
                        value = ', '.join(field['value']) if 'value' in field and isinstance(field['value'],
                                                                                             list) else str(
                            field.get('value', ''))
                        if normalized_label:
                            custom_field_labels[normalized_label] = value
                    except Exception as e:
                        logger.warning(f"Erro ao processar campo personalizado: {e}")

                new_fields.update(custom_field_labels)

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

            except Exception as e:
                logger.error(f"Erro ao extrair valores aninhados: {e}")

            return new_fields

        def clean_deal_data(deals):
            """Limpa dados dos deals com tratamento de erro."""
            cleaned_deals = []
            rename_map = {
                'closed_at': 'deal_closed_at',
                'amount_total': 'deal_amount_total',
                'amount_unique': 'deal_amount_unique',
                'created_at': 'deal_created_at',
                'id': 'deal_id',
                'name': 'deal_name',
                'updated_at': 'deal_updated_at',
            }

            excluded_fields = [
                "campaign", "contacts", "deal_custom_fields", "deal_lost_reason",
                "deal_products", "deal_source", "deal_stage", "organization",
                "user", "next_task", "_id", "markup", "markup_created",
                "markup_last_activities", "prediction_date", "stop_time_limit",
                "user_changed", "amount_monthly"
            ]

            for deal in deals:
                try:
                    deal_cleaned = {}

                    for k, v in deal.items():
                        if k not in excluded_fields:
                            new_key = rename_map.get(k, k)
                            v = clean_name(v) if isinstance(v, str) else v
                            normalized_key = normalize_column_name(new_key)
                            deal_cleaned[normalized_key] = v

                    nested_fields = extract_nested_values(deal)
                    deal_cleaned.update(nested_fields)
                    cleaned_deals.append(deal_cleaned)

                except Exception as e:
                    logger.warning(f"Erro ao limpar deal {deal.get('id', 'unknown')}: {e}")
                    continue

            return cleaned_deals

        def compile_fieldnames(data):
            """Compila nomes de campos com tratamento de erro."""
            try:
                base_fields = [
                    'deal_source_name', 'campaign_name', 'deal_id', 'deal_name', 'deal_created_at',
                    'deal_updated_at', 'deal_closed_at', 'last_activity_at', 'last_activity_content',
                    'deal_amount_unique', 'deal_amount_total', 'deal_stage_id', 'deal_stage_name',
                    'deal_lost_reason_name', 'win', 'organization_name', 'contact_email',
                    'contact_name', 'hold', 'interactions', 'user_id', 'user_name'
                ]

                normalized_base_fields = [normalize_column_name(field) for field in base_fields]
                custom_field_names = set()
                max_product_index = 0

                for deal in data:
                    custom_field_names.update(
                        key for key in deal.keys()
                        if key not in normalized_base_fields and not key.startswith('product_')
                    )

                    product_indices = [
                        int(key.split('_')[-1]) for key in deal.keys()
                        if key.startswith('product_') and key.split('_')[-1].isdigit()
                    ]
                    if product_indices:
                        max_product_index = max(max_product_index, max(product_indices))

                sorted_custom_field_names = sorted(custom_field_names)
                product_fields_ordered = []
                product_fields_template = [
                    'product_id_{index}', 'product_name_{index}', 'product_base_price_{index}',
                    'product_price_{index}', 'product_discount_{index}', 'product_discount_type_{index}',
                    'product_description_{index}', 'product_total_{index}'
                ]

                for index in range(1, max_product_index + 1):
                    for field in product_fields_template:
                        product_fields_ordered.append(field.format(index=index))

                return normalized_base_fields + sorted_custom_field_names + product_fields_ordered

            except Exception as e:
                logger.error(f"Erro ao compilar fieldnames: {e}")
                return ['deal_id', 'deal_name', 'error']

        def create_storage_client():
            """Cria cliente do Storage com tratamento de erro."""
            try:
                if os.path.exists(SERVICE_ACCOUNT_PATH):
                    storage_client = storage.Client.from_service_account_json(
                        SERVICE_ACCOUNT_PATH,
                        project=PROJECT_ID
                    )
                    logger.info("Cliente Storage criado com credenciais explícitas")
                else:
                    storage_client = storage.Client(project=PROJECT_ID)
                    logger.info("Cliente Storage criado com credenciais do ambiente")

                # Testa a conexão
                bucket = storage_client.bucket(BUCKET_NAME)
                bucket.exists()
                logger.info(f"Acesso ao bucket '{BUCKET_NAME}' confirmado")

                return storage_client

            except Exception as e:
                logger.error(f"Erro ao criar cliente Storage: {e}")
                raise

        def upload_to_gcs_safe(bucket_name, destination_blob_name, content, storage_client):
            """Upload seguro para GCS com fallback."""
            try:
                bucket = storage_client.bucket(bucket_name)
                blob = bucket.blob(destination_blob_name)
                blob.upload_from_string(content, 'text/csv')
                logger.info(f"Arquivo {destination_blob_name} enviado para o bucket {bucket_name}")
                return True

            except Exception as e:
                logger.error(f"Erro no upload para GCS: {e}")

                try:
                    local_filename = f"backup_{destination_blob_name.replace('/', '_')}"
                    with open(local_filename, 'w', encoding='utf-8') as f:
                        f.write(content)
                    logger.info(f"Arquivo salvo localmente: {local_filename}")
                    return False
                except Exception as local_error:
                    logger.error(f"Erro ao salvar backup local: {local_error}")
                    raise

        def get_earliest_date():
            """Define uma data de início fixa de 01/07/2024"""
            logger.info("Usando data fixa de 01/07/2024 como ponto de partida")
            return datetime(2022, 1, 1)

        def fetch_all_deals_for_period_safe(start_date, end_date):
            """Busca negociações com tratamento robusto de erro."""
            all_deals = []
            page = 1
            has_more = True
            consecutive_errors = 0
            max_consecutive_errors = 5

            while has_more and consecutive_errors < max_consecutive_errors:
                url = f"{API_URL}?token={TOKEN}&created_at_period=true&start_date={start_date}&end_date={end_date}&limit={LIMIT}&page={page}"

                try:
                    response = safe_request(url)
                    data = response.json()
                    deals = data.get('deals', [])

                    if deals:
                        cleaned_deals = clean_deal_data(deals)
                        all_deals.extend(cleaned_deals)
                        logger.info(f"Período {start_date}-{end_date}: Página {page} - {len(deals)} registros")
                        page += 1
                        has_more = len(deals) == LIMIT
                        consecutive_errors = 0
                    else:
                        logger.info(f"Nenhum registro encontrado para {start_date}-{end_date}")
                        has_more = False

                except Exception as e:
                    consecutive_errors += 1
                    logger.error(f"Erro na página {page} (tentativa {consecutive_errors}): {e}")

                    if consecutive_errors >= max_consecutive_errors:
                        logger.error(f"Máximo de erros consecutivos atingido. Interrompendo período.")
                        break

                    time.sleep(10 * consecutive_errors)

            return all_deals

        def fetch_deal_pipelines():
            """Busca todos os pipelines disponíveis com tratamento de erro."""
            try:
                params = {'token': TOKEN}
                response = safe_request(f"{DEAL_PIPELINES_URL}?token={TOKEN}")
                pipelines = response.json()
                logger.info(f"Total de pipelines coletados: {len(pipelines)}")
                for pipeline in pipelines:
                    logger.info(f"Pipeline ID: {pipeline['id']}, Nome: {pipeline['name']}")
                return pipelines
            except Exception as e:
                logger.error(f"Erro ao buscar pipelines: {e}")
                return []

        def fetch_deal_stages(deal_pipeline_id):
            """Busca as etapas do funil de vendas com tratamento de erro."""
            try:
                url = f"{DEAL_STAGES_URL}?token={TOKEN}&deal_pipeline_id={deal_pipeline_id}&limit={LIMIT}"
                response = safe_request(url)
                data = response.json()
                logger.info(f"Etapas coletadas para o pipeline ID: {deal_pipeline_id}")
                return data.get('deal_stages', [])
            except Exception as e:
                logger.error(f"Erro ao buscar dados para o pipeline {deal_pipeline_id}: {e}")
                return []

        def collect_all_stages():
            """Coleta todas as etapas de todos os pipelines com tratamento de erro."""
            pipelines = fetch_deal_pipelines()
            all_stages = []

            for pipeline in pipelines:
                try:
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
                except Exception as e:
                    logger.error(f"Erro ao processar pipeline {pipeline.get('id', 'unknown')}: {e}")
                    continue

            return all_stages

        def process_pipelines_and_stages(storage_client):
            """Processa pipelines e etapas com tratamento de erro."""
            try:
                logger.info("Iniciando coleta de pipelines e etapas...")
                all_stages = collect_all_stages()

                if all_stages:
                    normalized_stages = []
                    for stage in all_stages:
                        normalized_stage = {normalize_column_name(k): v for k, v in stage.items()}
                        normalized_stages.append(normalized_stage)

                    csv_file = StringIO()
                    writer = csv.DictWriter(csv_file, fieldnames=normalized_stages[0].keys())
                    writer.writeheader()
                    for stage in normalized_stages:
                        writer.writerow(stage)

                    csv_content = csv_file.getvalue()
                    destination_blob_name = 'Stages/deal_stages.csv'

                    upload_to_gcs_safe(BUCKET_NAME, destination_blob_name, csv_content, storage_client)
                    logger.info(f"Total de {len(normalized_stages)} etapas processadas e salvas.")
                else:
                    logger.warning("Nenhuma informação de pipelines e etapas foi coletada.")

            except Exception as e:
                logger.error(f"Erro ao processar pipelines e etapas: {e}")
                raise

        def main():
            """Função principal com tratamento completo de erros."""
            try:
                tracemalloc.start()
                start_time = time.time()

                logger.info("=== INICIANDO EXTRAÇÃO RDCRM ===")
                logger.info(f"Cliente: {customer.get('name', 'Unknown')}")
                logger.info(f"Bucket: {BUCKET_NAME}")
                logger.info(f"Project ID: {PROJECT_ID}")

                # Criar cliente Storage
                storage_client = create_storage_client()

                # Processar pipelines e etapas
                try:
                    process_pipelines_and_stages(storage_client)
                    logger.info("Pipelines e etapas processados com sucesso")
                except Exception as e:
                    logger.error(f"Erro ao processar pipelines/etapas: {e}")

                # Coleta de negociações
                all_data = []
                current_date = get_earliest_date()
                now = datetime.now()

                logger.info(f"Coletando negociações de {current_date} até {now}")

                while current_date < now:
                    start_date_str = current_date.strftime('%Y-%m-%dT%H:%M:%S')
                    end_date = current_date + timedelta(days=7)
                    if end_date > now:
                        end_date = now
                    end_date_str = end_date.strftime('%Y-%m-%dT%H:%M:%S')

                    try:
                        deals = fetch_all_deals_for_period_safe(start_date_str, end_date_str)
                        all_data.extend(deals)
                        logger.info(f"Período {start_date_str}-{end_date_str}: {len(deals)} registros coletados")
                    except Exception as e:
                        logger.error(f"Erro no período {start_date_str}-{end_date_str}: {e}")

                    current_date = end_date

                if not all_data:
                    logger.warning("Nenhum dado coletado")
                    return {"status": "warning", "message": "Nenhum dado coletado"}

                # Processar e fazer upload dos dados
                fieldnames = compile_fieldnames(all_data)

                csv_file = StringIO()
                writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                writer.writeheader()

                for deal in all_data:
                    try:
                        normalized_deal = {normalize_column_name(k): v for k, v in deal.items()}
                        writer.writerow(normalized_deal)
                    except Exception as e:
                        logger.warning(f"Erro ao escrever deal: {e}")

                csv_content = csv_file.getvalue()
                destination_blob_name = 'Deals/rd_crm_deals.csv'

                upload_success = upload_to_gcs_safe(BUCKET_NAME, destination_blob_name, csv_content, storage_client)

                # Estatísticas finais
                end_time = time.time()
                current, peak = tracemalloc.get_traced_memory()
                tracemalloc.stop()

                logger.info(f"=== EXTRAÇÃO CONCLUÍDA ===")
                logger.info(f"Registros processados: {len(all_data)}")
                logger.info(f"Tempo execução: {end_time - start_time:.2f}s")
                logger.info(f"Memória pico: {peak / 1024 ** 2:.2f}MB")
                logger.info(f"Upload GCS: {'Sucesso' if upload_success else 'Falhou (salvo localmente)'}")

                return {
                    "status": "success",
                    "records_processed": len(all_data),
                    "execution_time": end_time - start_time,
                    "upload_success": upload_success
                }

            except Exception as e:
                logger.error(f"ERRO CRÍTICO na função main: {e}")
                logger.error("Traceback completo:", exc_info=True)
                raise RuntimeError(f"Erro crítico na extração RDCRM: {e}")

        # EXECUTAR
        return main()

    except Exception as e:
        print(f"ERRO FATAL na função run: {e}")
        import traceback
        traceback.print_exc()
        raise RuntimeError(f"Erro fatal na extração RDCRM: {e}")


def run_contacts(customer):
    """
    Extrai dados de contatos do RDCRM API e carrega para GCS.
    Versão corrigida com tratamento robusto de erros.
    """

    try:
        if not str(customer.get('contacts_active', '')).strip().lower() == 'true':
            print(f"CONTACTS não está ativo para este cliente. Pulando execução.")
            return {"status": "skipped", "message": "Contacts não ativo"}

        import csv
        import os
        import requests
        import string
        import time
        import tracemalloc
        from datetime import datetime
        from google.cloud import storage
        from io import StringIO
        import pathlib
        import logging

        # Configuração de logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[logging.StreamHandler(sys.stdout)]
        )
        logger = logging.getLogger(__name__)

        # Validação de parâmetros
        required_fields = ['token', 'bucket_name', 'project_id']
        for field in required_fields:
            if not customer.get(field):
                logger.error(f"Campo obrigatório ausente: {field}")
                raise ValueError(f"Campo obrigatório ausente: {field}")

        # Configuração da chave de serviço
        SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()
        if not os.path.exists(SERVICE_ACCOUNT_PATH):
            logger.warning("Arquivo de credenciais não encontrado, usando ambiente")
        else:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH

        TOKEN = customer['token']
        API_URL = "https://crm.rdstation.com/api/v1/contacts"
        LIMIT = 200
        BUCKET_NAME = customer['bucket_name']
        PROJECT_ID = customer['project_id']
        MAX_RESULT_WINDOW = 9500
        REQUEST_TIMEOUT = 30

        def safe_request_contacts(url, timeout=REQUEST_TIMEOUT, max_retries=3):
            """Requisições seguras para API de contatos."""
            for attempt in range(max_retries):
                try:
                    response = requests.get(url, timeout=timeout)

                    if response.status_code == 200:
                        return response
                    elif response.status_code == 429:
                        wait_time = 60 * (attempt + 1)
                        logger.warning(f"Rate limit. Aguardando {wait_time}s...")
                        time.sleep(wait_time)
                    elif response.status_code == 400:
                        logger.warning("Erro 400 - Limitação atingida ou chunk inválido")
                        break
                    else:
                        logger.warning(f"Status code: {response.status_code}")

                except requests.exceptions.Timeout:
                    logger.warning(f"Timeout na tentativa {attempt + 1}")
                    if attempt == max_retries - 1:
                        raise
                    time.sleep(10 * (attempt + 1))

                except requests.exceptions.ConnectionError as e:
                    logger.warning(f"Erro de conexão: {e}")
                    if attempt == max_retries - 1:
                        raise
                    time.sleep(5 * (attempt + 1))

            return None

        def clean_name(name):
            """Limpa espaços extras e caracteres especiais de strings."""
            if name is None:
                return ''
            return " ".join(str(name).replace("\t", " ").split()).replace("•", "")

        def extract_contact_data(contact):
            """Extrai e processa dados de um contato com tratamento de erro."""
            contact_data = {}

            try:
                # Dados básicos do contato
                contact_data['contact_id'] = contact.get('id', '')
                contact_data['contact_name'] = clean_name(contact.get('name', ''))
                contact_data['contact_title'] = clean_name(contact.get('title', ''))
                contact_data['contact_notes'] = clean_name(contact.get('notes', ''))
                contact_data['contact_birthday'] = contact.get('birthday', '')
                contact_data['contact_facebook'] = contact.get('facebook', '')
                contact_data['contact_linkedin'] = contact.get('linkedin', '')
                contact_data['contact_skype'] = contact.get('skype', '')
                contact_data['organization_id'] = contact.get('organization_id', '')
                contact_data['contact_created_at'] = contact.get('created_at', '')
                contact_data['contact_updated_at'] = contact.get('updated_at', '')

                # Processamento de emails
                emails = contact.get('emails', [])
                if emails:
                    contact_data['primary_email'] = emails[0].get('email', '')
                    contact_data['primary_email_id'] = emails[0].get('id', '')
                    all_emails = [email.get('email', '') for email in emails if email.get('email')]
                    contact_data['all_emails'] = '; '.join(all_emails)
                    contact_data['emails_count'] = len(all_emails)
                else:
                    contact_data['primary_email'] = ''
                    contact_data['primary_email_id'] = ''
                    contact_data['all_emails'] = ''
                    contact_data['emails_count'] = 0

                # Processamento de telefones
                phones = contact.get('phones', [])
                if phones:
                    main_phone = phones[0]
                    contact_data['primary_phone'] = main_phone.get('phone', '')
                    contact_data['primary_phone_id'] = main_phone.get('id', '')
                    contact_data['primary_phone_type'] = main_phone.get('type', '')
                    contact_data['primary_phone_whatsapp'] = main_phone.get('whatsapp', False)
                    contact_data['primary_whatsapp_url'] = main_phone.get('whatsapp_url_web', '')
                    contact_data['primary_whatsapp_international'] = main_phone.get('whatsapp_full_internacional', '')

                    all_phones = [phone.get('phone', '') for phone in phones if phone.get('phone')]
                    contact_data['all_phones'] = '; '.join(all_phones)
                    contact_data['phones_count'] = len(all_phones)

                    whatsapp_phones = [phone for phone in phones if phone.get('whatsapp')]
                    contact_data['whatsapp_phones_count'] = len(whatsapp_phones)
                else:
                    contact_data['primary_phone'] = ''
                    contact_data['primary_phone_id'] = ''
                    contact_data['primary_phone_type'] = ''
                    contact_data['primary_phone_whatsapp'] = False
                    contact_data['primary_whatsapp_url'] = ''
                    contact_data['primary_whatsapp_international'] = ''
                    contact_data['all_phones'] = ''
                    contact_data['phones_count'] = 0
                    contact_data['whatsapp_phones_count'] = 0

                # Processamento de bases legais
                legal_bases = contact.get('legal_bases', [])
                if legal_bases:
                    main_legal_base = legal_bases[0]
                    contact_data['legal_base_category'] = main_legal_base.get('category', '')
                    contact_data['legal_base_type'] = main_legal_base.get('type', '')
                    contact_data['legal_base_status'] = main_legal_base.get('status', '')
                    contact_data['legal_bases_count'] = len(legal_bases)

                    all_categories = [lb.get('category', '') for lb in legal_bases if lb.get('category')]
                    contact_data['all_legal_categories'] = '; '.join(all_categories)
                else:
                    contact_data['legal_base_category'] = ''
                    contact_data['legal_base_type'] = ''
                    contact_data['legal_base_status'] = ''
                    contact_data['legal_bases_count'] = 0
                    contact_data['all_legal_categories'] = ''

                # Processamento de campos personalizados
                custom_fields = contact.get('contact_custom_fields', [])
                contact_data['custom_fields_count'] = len(custom_fields)

                for i, field in enumerate(custom_fields, 1):
                    try:
                        if 'custom_field' in field and 'label' in field['custom_field']:
                            label = clean_name(field['custom_field']['label'])
                            if label:
                                safe_label = label.replace(' ', '_').replace('-', '_')[:50]
                                value = field.get('value', '')
                                if isinstance(value, list):
                                    value = '; '.join(str(v) for v in value)
                                contact_data[f'custom_field_{safe_label}'] = str(value)
                    except Exception as e:
                        logger.warning(f"Erro ao processar campo personalizado {i}: {e}")

                # Processamento de deals associados
                deals = contact.get('deals', [])
                contact_data['deals_count'] = len(deals)

                if deals:
                    deal_names = []
                    deal_ids = []
                    deal_wins = []
                    deal_statuses = []

                    for deal in deals:
                        try:
                            deal_names.append(clean_name(deal.get('name', '')))
                            deal_ids.append(str(deal.get('id', '')))

                            win_status = deal.get('win')
                            if win_status is True:
                                deal_wins.append('Won')
                            elif win_status is False:
                                deal_wins.append('Lost')
                            else:
                                deal_wins.append('Open')

                            if deal.get('closed_at'):
                                deal_statuses.append('Closed')
                            else:
                                deal_statuses.append('Open')
                        except Exception as e:
                            logger.warning(f"Erro ao processar deal: {e}")

                    contact_data['deal_names'] = '; '.join(deal_names)
                    contact_data['deal_ids'] = '; '.join(deal_ids)
                    contact_data['deal_win_statuses'] = '; '.join(deal_wins)
                    contact_data['deal_statuses'] = '; '.join(deal_statuses)
                    contact_data['won_deals_count'] = deal_wins.count('Won')
                    contact_data['lost_deals_count'] = deal_wins.count('Lost')
                    contact_data['open_deals_count'] = deal_wins.count('Open')
                    contact_data['closed_deals_count'] = deal_statuses.count('Closed')
                else:
                    contact_data['deal_names'] = ''
                    contact_data['deal_ids'] = ''
                    contact_data['deal_win_statuses'] = ''
                    contact_data['deal_statuses'] = ''
                    contact_data['won_deals_count'] = 0
                    contact_data['lost_deals_count'] = 0
                    contact_data['open_deals_count'] = 0
                    contact_data['closed_deals_count'] = 0

            except Exception as e:
                logger.error(f"Erro ao processar contato {contact.get('id', 'unknown')}: {e}")
                contact_data = {
                    'contact_id': contact.get('id', ''),
                    'contact_name': clean_name(contact.get('name', '')),
                    'error': str(e)
                }

            return contact_data

        def get_ultra_granular_chunks():
            """Gera chunks ultra-granulares para contornar limitação de 10k."""
            chunks = []

            # Chunks por duas letras (AA, AB, AC, ..., ZZ)
            logger.info("Gerando chunks de duas letras...")
            for first in string.ascii_uppercase:
                for second in string.ascii_uppercase:
                    chunks.append({
                        'type': 'double_letter',
                        'query': first + second,
                        'description': f'Nomes iniciados com "{first + second}"'
                    })

            # Chunks por letra + número (A0, A1, ..., Z9)
            logger.info("Gerando chunks de letra + número...")
            for letter in string.ascii_uppercase:
                for digit in string.digits:
                    chunks.append({
                        'type': 'letter_digit',
                        'query': letter + digit,
                        'description': f'Nomes iniciados com "{letter + digit}"'
                    })

            # Chunks por número + letra (0A, 0B, ..., 9Z)
            for digit in string.digits:
                for letter in string.ascii_uppercase:
                    chunks.append({
                        'type': 'digit_letter',
                        'query': digit + letter,
                        'description': f'Nomes iniciados com "{digit + letter}"'
                    })

            # Chunks por dois números (00, 01, ..., 99)
            for first in string.digits:
                for second in string.digits:
                    chunks.append({
                        'type': 'double_digit',
                        'query': first + second,
                        'description': f'Nomes iniciados com "{first + second}"'
                    })

            # Chunks especiais para padrões comuns
            special_patterns = [
                '@gmail', '@hotmail', '@yahoo', '@outlook', '@uol', '@terra',
                '@live', '@msn', '@bol', '@ig', '@globo', '@r7', '@oi',
                '.com', '.br', '.org', '.net', 'admin', 'contact', 'info',
                'suporte', 'vendas', 'comercial', 'financeiro', 'rh'
            ]

            for pattern in special_patterns:
                chunks.append({
                    'type': 'special_pattern',
                    'query': pattern,
                    'description': f'Nomes contendo "{pattern}"'
                })

            # Chunks por símbolos únicos
            symbols = ['.', '-', '_', '@', '+', '(', ')', '[', ']', '{', '}', '&', '#', '*']
            for symbol in symbols:
                chunks.append({
                    'type': 'symbol',
                    'query': symbol,
                    'description': f'Nomes contendo "{symbol}"'
                })

            logger.info(f"Total de chunks gerados: {len(chunks)}")
            return chunks

        def test_chunk_size(chunk_query):
            """Testa quantos resultados um chunk retornaria."""
            url = f"{API_URL}?token={TOKEN}&limit={LIMIT}&page=1&q={chunk_query}"

            try:
                response = safe_request_contacts(url, timeout=10)
                if response and response.status_code == 200:
                    data = response.json()
                    return data.get('total', 0)
                return -1
            except:
                return -1

        def fetch_contacts_for_chunk(chunk_query, chunk_description, max_contacts=MAX_RESULT_WINDOW):
            """Busca contatos para um chunk específico com limitação inteligente."""
            contacts_chunk = []
            page = 1
            has_more = True

            estimated_total = test_chunk_size(chunk_query)
            if estimated_total > max_contacts:
                logger.info(f"  Chunk grande ({estimated_total} estimados). Limitando a {max_contacts}")

            logger.info(f"  Processando: {chunk_description} (estimativa: {estimated_total})")

            while has_more and len(contacts_chunk) < max_contacts:
                url = f"{API_URL}?token={TOKEN}&limit={LIMIT}&page={page}&order=created_at&direction=asc&q={chunk_query}"

                try:
                    response = safe_request_contacts(url)

                    if not response or response.status_code != 200:
                        break

                    data = response.json()
                    contacts = data.get('contacts', [])

                    if not contacts:
                        break

                    for contact in contacts:
                        if len(contacts_chunk) >= max_contacts:
                            break
                        processed_contact = extract_contact_data(contact)
                        contacts_chunk.append(processed_contact)

                    if page % 10 == 0:
                        logger.info(f"    Página {page}: {len(contacts_chunk)} contatos acumulados")

                    has_more = data.get('has_more', False) and len(contacts) == LIMIT
                    page += 1
                    time.sleep(0.05)  # Rate limiting

                except Exception as e:
                    logger.error(f"    Erro na página {page}: {e}")
                    break

            logger.info(f"  Chunk concluído: {len(contacts_chunk)} contatos coletados")
            return contacts_chunk

        def remove_duplicates(all_contacts):
            """Remove contatos duplicados baseado no contact_id."""
            seen_ids = set()
            unique_contacts = []
            duplicates_count = 0

            for contact in all_contacts:
                contact_id = contact.get('contact_id')
                if contact_id and contact_id not in seen_ids:
                    seen_ids.add(contact_id)
                    unique_contacts.append(contact)
                else:
                    duplicates_count += 1

            logger.info(f"Duplicatas removidas: {duplicates_count}")
            return unique_contacts

        def compile_fieldnames_contacts(contacts_data):
            """Compila todos os nomes de campos únicos encontrados nos dados."""
            base_fields = [
                'contact_id', 'contact_name', 'contact_title', 'contact_notes',
                'contact_birthday', 'contact_facebook', 'contact_linkedin', 'contact_skype',
                'organization_id', 'contact_created_at', 'contact_updated_at',
                'primary_email', 'primary_email_id', 'all_emails', 'emails_count',
                'primary_phone', 'primary_phone_id', 'primary_phone_type',
                'primary_phone_whatsapp', 'primary_whatsapp_url', 'primary_whatsapp_international',
                'all_phones', 'phones_count', 'whatsapp_phones_count',
                'legal_base_category', 'legal_base_type', 'legal_base_status',
                'legal_bases_count', 'all_legal_categories',
                'custom_fields_count',
                'deals_count', 'deal_names', 'deal_ids', 'deal_win_statuses', 'deal_statuses',
                'won_deals_count', 'lost_deals_count', 'open_deals_count', 'closed_deals_count'
            ]

            custom_fields = set()
            other_fields = set()

            for contact in contacts_data:
                for field_name in contact.keys():
                    if field_name.startswith('custom_field_'):
                        custom_fields.add(field_name)
                    elif field_name not in base_fields:
                        other_fields.add(field_name)

            return base_fields + sorted(custom_fields) + sorted(other_fields)

        def upload_to_gcs_contacts(bucket_name, destination_blob_name, content):
            """Faz o upload do conteúdo para o Google Cloud Storage."""
            try:
                storage_client = storage.Client(project=PROJECT_ID)
                bucket = storage_client.bucket(bucket_name)
                blob = bucket.blob(destination_blob_name)

                blob.upload_from_string(content, 'text/csv')
                logger.info(f"Arquivo {destination_blob_name} enviado para o bucket {bucket_name}")
                return True
            except Exception as e:
                logger.error(f"Erro ao fazer upload para GCS: {e}")

                try:
                    backup_filename = f"backup_{destination_blob_name.replace('/', '_')}"
                    with open(backup_filename, 'w', encoding='utf-8') as f:
                        f.write(content)
                    logger.info(f"Arquivo salvo localmente como backup: {backup_filename}")
                    return False
                except Exception as local_error:
                    logger.error(f"Erro ao salvar backup local: {local_error}")
                    raise

        def fetch_all_contacts_ultra_chunked():
            """Busca todos os contatos usando estratégia ultra-granular de chunks."""
            all_contacts = []
            chunks = get_ultra_granular_chunks()

            logger.info(f"\nEstratégia Ultra-Granular: {len(chunks)} chunks")
            logger.info("Iniciando coleta com chunks de alta granularidade...")

            successful_chunks = 0
            failed_chunks = 0
            empty_chunks = 0

            for i, chunk in enumerate(chunks, 1):
                logger.info(f"\n--- Chunk {i}/{len(chunks)} ---")

                try:
                    chunk_contacts = fetch_contacts_for_chunk(
                        chunk['query'],
                        chunk['description'],
                        max_contacts=9000
                    )

                    if chunk_contacts:
                        all_contacts.extend(chunk_contacts)
                        successful_chunks += 1
                    else:
                        empty_chunks += 1

                except Exception as e:
                    logger.error(f"Erro no chunk {i}: {e}")
                    failed_chunks += 1
                    continue

                # Status report a cada 50 chunks
                if i % 50 == 0:
                    unique_so_far = len(remove_duplicates(all_contacts))
                    logger.info(f"\n=== PROGRESSO INTERMEDIÁRIO ===")
                    logger.info(f"Chunks processados: {i}/{len(chunks)} ({i / len(chunks) * 100:.1f}%)")
                    logger.info(f"Contatos brutos: {len(all_contacts)}")
                    logger.info(f"Contatos únicos estimados: {unique_so_far}")
                    logger.info(f"Chunks bem-sucedidos: {successful_chunks}")
                    logger.info(f"Chunks vazios: {empty_chunks}")
                    logger.info(f"Chunks falharam: {failed_chunks}")

            logger.info(f"\n=== COLETA ULTRA-GRANULAR FINALIZADA ===")
            logger.info(f"Total de contatos antes da deduplicação: {len(all_contacts)}")

            # Remove duplicatas
            unique_contacts = remove_duplicates(all_contacts)

            logger.info(f"Total de contatos únicos: {len(unique_contacts)}")
            logger.info(f"Chunks bem-sucedidos: {successful_chunks}/{len(chunks)}")
            logger.info(f"Chunks vazios: {empty_chunks}")
            logger.info(f"Taxa de sucesso: {successful_chunks / len(chunks) * 100:.1f}%")

            return unique_contacts

        def main_contacts():
            """Função principal que coordena todo o processo de contatos."""
            tracemalloc.start()
            start_time = time.time()

            logger.info("=== COLETOR DE CONTATOS RD STATION CRM - VERSÃO ULTRA-GRANULAR ===")
            logger.info(f"Início da execução: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info(f"Cliente: {customer.get('name', 'Unknown')}")
            logger.info(f"Bucket: {BUCKET_NAME}")

            # Coleta todos os contatos usando chunks ultra-granulares
            all_contacts_data = fetch_all_contacts_ultra_chunked()

            if not all_contacts_data:
                logger.warning("Nenhum contato foi coletado.")
                return {"status": "warning", "message": "Nenhum contato coletado"}

            logger.info(f"\nTotal de contatos únicos processados: {len(all_contacts_data)}")

            # Compila os nomes dos campos
            fieldnames = compile_fieldnames_contacts(all_contacts_data)
            logger.info(f"Total de campos identificados: {len(fieldnames)}")

            # Cria o CSV em memória
            csv_file = StringIO()
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()

            # Escreve os dados
            for contact in all_contacts_data:
                try:
                    writer.writerow(contact)
                except Exception as e:
                    logger.warning(f"Erro ao escrever contato: {e}")

            # Prepara para upload
            csv_content = csv_file.getvalue()
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            destination_blob_name = f'Contacts/rd_crm_contacts_ultra_complete_{timestamp}.csv'

            # Faz upload para GCS
            upload_success = upload_to_gcs_contacts(BUCKET_NAME, destination_blob_name, csv_content)

            # Estatísticas finais
            end_time = time.time()
            current, peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()

            logger.info(f"\n=== ESTATÍSTICAS FINAIS ===")
            logger.info(f"Contatos únicos coletados: {len(all_contacts_data):,}")
            logger.info(
                f"Tempo de execução: {end_time - start_time:.2f} segundos ({(end_time - start_time) / 60:.1f} minutos)")
            logger.info(f"Pico de uso de memória: {peak / 1024 ** 2:.2f} MB")
            logger.info(f"Arquivo gerado: {destination_blob_name}")
            logger.info(f"Tamanho do CSV: {len(csv_content) / 1024:.2f} KB")
            logger.info(f"Upload GCS: {'Sucesso' if upload_success else 'Falhou (salvo localmente)'}")

            # Cobertura estimada
            estimated_total = 62696  # Estimativa total de contatos
            coverage_percentage = (len(all_contacts_data) / estimated_total) * 100
            logger.info(f"Cobertura estimada: {coverage_percentage:.1f}% dos contatos totais")

            # Estatísticas dos dados
            contacts_with_email = sum(1 for c in all_contacts_data if c.get('primary_email'))
            contacts_with_phone = sum(1 for c in all_contacts_data if c.get('primary_phone'))
            contacts_with_deals = sum(1 for c in all_contacts_data if c.get('deals_count', 0) > 0)

            logger.info(f"\n=== ESTATÍSTICAS DOS DADOS ===")
            logger.info(
                f"Contatos com email: {contacts_with_email:,} ({contacts_with_email / len(all_contacts_data) * 100:.1f}%)")
            logger.info(
                f"Contatos com telefone: {contacts_with_phone:,} ({contacts_with_phone / len(all_contacts_data) * 100:.1f}%)")
            logger.info(
                f"Contatos com deals: {contacts_with_deals:,} ({contacts_with_deals / len(all_contacts_data) * 100:.1f}%)")

            logger.info(f"\nMISSÃO CUMPRIDA! Dados de contatos extraídos com sucesso.")

            return {
                "status": "success",
                "contacts_processed": len(all_contacts_data),
                "execution_time": end_time - start_time,
                "upload_success": upload_success,
                "coverage_percentage": coverage_percentage
            }

        # EXECUTAR
        return main_contacts()

    except Exception as e:
        print(f"ERRO FATAL na função run_contacts: {e}")
        import traceback
        traceback.print_exc()
        raise RuntimeError(f"Erro fatal na extração de contatos RDCRM: {e}")


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
        },
        {
            'task_id': 'run_contacts',
            'python_callable': run_contacts
        }
    ]
