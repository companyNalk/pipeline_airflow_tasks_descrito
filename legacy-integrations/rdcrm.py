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
                             "markup_created", "markup_last_activities", "prediction_date", "stop_time_limit",
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


def run_contacts(customer):
    if not str(customer['contacts_active']).strip().lower() == 'true':
        print(f"CVDW não está ativo para este cliente. Pulando execução da função run_leads.")
        return
    print(f'VALUE: {customer["contacts_active"]}')

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

    # Configuração da chave de serviço
    SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH

    TOKEN = customer['token']
    API_URL = "https://crm.rdstation.com/api/v1/contacts"
    LIMIT = 200  # Máximo permitido pela API
    BUCKET_NAME = customer['bucket_name']
    MAX_RESULT_WINDOW = 9500  # Margem de segurança abaixo de 10k

    def clean_name(name):
        """Limpa espaços extras e caracteres especiais de strings."""
        if name is None:
            return ''
        return " ".join(name.replace("\t", " ").split()).replace("•", "")

    def extract_contact_data(contact):
        """
        Extrai e processa dados de um contato, incluindo campos aninhados.
        Retorna um dicionário com os dados achatados.
        """
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
                if 'custom_field' in field and 'label' in field['custom_field']:
                    label = clean_name(field['custom_field']['label'])
                    if label:
                        safe_label = label.replace(' ', '_').replace('-', '_')[:50]
                        value = field.get('value', '')
                        if isinstance(value, list):
                            value = '; '.join(str(v) for v in value)
                        contact_data[f'custom_field_{safe_label}'] = str(value)

            # Processamento de deals associados
            deals = contact.get('deals', [])
            contact_data['deals_count'] = len(deals)

            if deals:
                deal_names = []
                deal_ids = []
                deal_wins = []
                deal_statuses = []

                for deal in deals:
                    deal_names.append(clean_name(deal.get('name', '')))
                    deal_ids.append(deal.get('id', ''))

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
            print(f"Erro ao processar contato {contact.get('id', 'unknown')}: {str(e)}")
            contact_data = {
                'contact_id': contact.get('id', ''),
                'contact_name': clean_name(contact.get('name', '')),
                'error': str(e)
            }

        return contact_data

    def get_ultra_granular_chunks():
        """
        Gera chunks ultra-granulares para contornar a limitação de 10k.
        Estratégia: combinações de 2 e 3 caracteres para máxima granularidade.
        """
        chunks = []

        # 1. Chunks por duas letras (AA, AB, AC, ..., ZZ)
        print("Gerando chunks de duas letras...")
        for first in string.ascii_uppercase:
            for second in string.ascii_uppercase:
                chunks.append({
                    'type': 'double_letter',
                    'query': first + second,
                    'description': f'Nomes iniciados com "{first + second}"'
                })

        # 2. Chunks por letra + número (A0, A1, ..., Z9)
        print("Gerando chunks de letra + número...")
        for letter in string.ascii_uppercase:
            for digit in string.digits:
                chunks.append({
                    'type': 'letter_digit',
                    'query': letter + digit,
                    'description': f'Nomes iniciados com "{letter + digit}"'
                })

        # 3. Chunks por número + letra (0A, 0B, ..., 9Z)
        print("Gerando chunks de número + letra...")
        for digit in string.digits:
            for letter in string.ascii_uppercase:
                chunks.append({
                    'type': 'digit_letter',
                    'query': digit + letter,
                    'description': f'Nomes iniciados com "{digit + letter}"'
                })

        # 4. Chunks por dois números (00, 01, ..., 99)
        print("Gerando chunks de dois números...")
        for first in string.digits:
            for second in string.digits:
                chunks.append({
                    'type': 'double_digit',
                    'query': first + second,
                    'description': f'Nomes iniciados com "{first + second}"'
                })

        # 5. Chunks especiais para caracteres comuns em emails
        print("Gerando chunks especiais...")
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

        # 6. Chunks por símbolos únicos
        symbols = ['.', '-', '_', '@', '+', '(', ')', '[', ']', '{', '}', '&', '#', '*']
        for symbol in symbols:
            chunks.append({
                'type': 'symbol',
                'query': symbol,
                'description': f'Nomes contendo "{symbol}"'
            })

        # 7. Chunks para espaços em branco e caracteres especiais
        space_patterns = [' ', '%20', '+', '_']
        for pattern in space_patterns:
            chunks.append({
                'type': 'space_pattern',
                'query': pattern,
                'description': f'Nomes contendo espaços/caracteres especiais'
            })

        print(f"Total de chunks gerados: {len(chunks)}")
        return chunks

    def test_chunk_size(chunk_query):
        """
        Testa quantos resultados um chunk retornaria (apenas primeira página).
        Usado para estimar se o chunk pode exceder 10k.
        """
        url = f"{API_URL}?token={TOKEN}&limit={LIMIT}&page=1&q={chunk_query}"

        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                total = data.get('total', 0)
                return total
            else:
                return -1
        except:
            return -1

    def fetch_contacts_for_chunk(chunk_query, chunk_description, max_contacts=MAX_RESULT_WINDOW):
        """
        Busca contatos para um chunk específico com limitação inteligente.
        """
        contacts_chunk = []
        page = 1
        has_more = True

        # Testa o tamanho do chunk primeiro
        estimated_total = test_chunk_size(chunk_query)
        if estimated_total > max_contacts:
            print(f"  Chunk muito grande ({estimated_total} estimados). Limitando a {max_contacts} contatos.")

        print(f"  Processando: {chunk_description} (estimativa: {estimated_total} contatos)")

        while has_more and len(contacts_chunk) < max_contacts:
            url = f"{API_URL}?token={TOKEN}&limit={LIMIT}&page={page}&order=created_at&direction=asc&q={chunk_query}"

            try:
                response = requests.get(url, timeout=30)

                if response.status_code == 200:
                    data = response.json()
                    contacts = data.get('contacts', [])

                    if not contacts:
                        break

                    # Processa os contatos da página atual
                    for contact in contacts:
                        if len(contacts_chunk) >= max_contacts:
                            break
                        processed_contact = extract_contact_data(contact)
                        contacts_chunk.append(processed_contact)

                    if page % 10 == 0:  # Log a cada 10 páginas
                        print(f"    Página {page}: {len(contacts_chunk)} contatos acumulados")

                    # Verifica se há mais páginas
                    has_more = data.get('has_more', False) and len(contacts) == LIMIT
                    page += 1

                    # Pausa para evitar rate limiting
                    time.sleep(0.05)

                elif response.status_code == 429:
                    print("    Rate limit atingido. Aguardando 60 segundos...")
                    time.sleep(60)

                elif response.status_code == 400:
                    print(f"    Erro 400 - Limitação atingida ou chunk inválido.")
                    break

                else:
                    print(f"    Erro na requisição: {response.status_code}")
                    break

            except requests.exceptions.RequestException as e:
                print(f"    Erro de conexão: {str(e)}")
                time.sleep(30)

            except Exception as e:
                print(f"    Erro inesperado: {str(e)}")
                break

        print(f"  Chunk concluído: {len(contacts_chunk)} contatos coletados")
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

        print(f"Duplicatas removidas: {duplicates_count}")
        return unique_contacts

    def compile_fieldnames(contacts_data):
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

    def upload_to_gcs(bucket_name, destination_blob_name, content):
        """Faz o upload do conteúdo para o Google Cloud Storage."""
        try:
            storage_client = storage.Client()
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(destination_blob_name)

            blob.upload_from_string(content, 'text/csv')
            print(f"Arquivo {destination_blob_name} enviado para o bucket {bucket_name}.")
        except Exception as e:
            print(f"Erro ao fazer upload para GCS: {str(e)}")
            backup_filename = f"backup_{destination_blob_name.replace('/', '_')}"
            with open(backup_filename, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"Arquivo salvo localmente como backup: {backup_filename}")

    def fetch_all_contacts_ultra_chunked():
        """
        Busca todos os contatos usando estratégia ultra-granular de chunks.
        """
        all_contacts = []
        chunks = get_ultra_granular_chunks()

        print(f"\nEstratégia Ultra-Granular: {len(chunks)} chunks")
        print("Iniciando coleta com chunks de alta granularidade...")

        successful_chunks = 0
        failed_chunks = 0
        empty_chunks = 0

        for i, chunk in enumerate(chunks, 1):
            print(f"\n--- Chunk {i}/{len(chunks)} ---")

            try:
                chunk_contacts = fetch_contacts_for_chunk(
                    chunk['query'],
                    chunk['description'],
                    max_contacts=9000  # Margem de segurança ainda maior
                )

                if chunk_contacts:
                    all_contacts.extend(chunk_contacts)
                    successful_chunks += 1
                else:
                    empty_chunks += 1

            except Exception as e:
                print(f"Erro no chunk {i}: {str(e)}")
                failed_chunks += 1
                continue

            # Status report a cada 50 chunks (mais frequente devido ao maior número)
            if i % 50 == 0:
                unique_so_far = len(remove_duplicates(all_contacts))
                print(f"\n=== PROGRESSO INTERMEDIÁRIO ===")
                print(f"Chunks processados: {i}/{len(chunks)} ({i / len(chunks) * 100:.1f}%)")
                print(f"Contatos brutos: {len(all_contacts)}")
                print(f"Contatos únicos estimados: {unique_so_far}")
                print(f"Chunks bem-sucedidos: {successful_chunks}")
                print(f"Chunks vazios: {empty_chunks}")
                print(f"Chunks falharam: {failed_chunks}")

        print(f"\n=== COLETA ULTRA-GRANULAR FINALIZADA ===")
        print(f"Total de contatos antes da deduplicação: {len(all_contacts)}")

        # Remove duplicatas
        unique_contacts = remove_duplicates(all_contacts)

        print(f"Total de contatos únicos: {len(unique_contacts)}")
        print(f"Chunks bem-sucedidos: {successful_chunks}/{len(chunks)}")
        print(f"Chunks vazios: {empty_chunks}")
        print(f"Taxa de sucesso: {successful_chunks / len(chunks) * 100:.1f}%")

        return unique_contacts

    def main():
        """Função principal que coordena todo o processo."""
        tracemalloc.start()
        start_time = time.time()

        print("=== COLETOR DE CONTATOS RD STATION CRM - VERSÃO ULTRA-GRANULAR ===")
        print(f"Início da execução: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Meta: Coletar todos os ~62.696 contatos com chunks ultra-granulares")
        print(f"Limitação da API: 10.000 registros por consulta")
        print(f"Estratégia: Chunks de 2-3 caracteres + padrões especiais")

        # Coleta todos os contatos usando chunks ultra-granulares
        all_contacts_data = fetch_all_contacts_ultra_chunked()

        if not all_contacts_data:
            print("Nenhum contato foi coletado. Encerrando execução.")
            return

        print(f"\nTotal de contatos únicos processados: {len(all_contacts_data)}")

        # Compila os nomes dos campos
        fieldnames = compile_fieldnames(all_contacts_data)
        print(f"Total de campos identificados: {len(fieldnames)}")

        # Cria o CSV em memória
        csv_file = StringIO()
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()

        # Escreve os dados
        for contact in all_contacts_data:
            writer.writerow(contact)

        # Prepara para upload
        csv_content = csv_file.getvalue()
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        destination_blob_name = f'Contacts/rd_crm_contacts_ultra_complete_{timestamp}.csv'

        # Faz upload para GCS
        upload_to_gcs(BUCKET_NAME, destination_blob_name, csv_content)

        # Estatísticas finais
        end_time = time.time()
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        print(f"\n=== ESTATÍSTICAS FINAIS ===")
        print(f"Contatos únicos coletados: {len(all_contacts_data):,}")
        print(f"Tempo de execução: {end_time - start_time:.2f} segundos ({(end_time - start_time) / 60:.1f} minutos)")
        print(f"Pico de uso de memória: {peak / 1024 ** 2:.2f} MB")
        print(f"Arquivo gerado: {destination_blob_name}")
        print(f"Tamanho do CSV: {len(csv_content) / 1024:.2f} KB")

        # Cobertura estimada
        coverage_percentage = (len(all_contacts_data) / 62696) * 100
        print(f"Cobertura estimada: {coverage_percentage:.1f}% dos contatos totais")

        if coverage_percentage >= 90:
            print("EXCELENTE! Cobertura superior a 90%")
        elif coverage_percentage >= 80:
            print("BOM! Cobertura superior a 80%")
        elif coverage_percentage >= 70:
            print("REGULAR. Cobertura superior a 70%")
        else:
            print("BAIXA cobertura. Considere ajustar a estratégia.")

        # Estatísticas dos dados
        contacts_with_email = sum(1 for c in all_contacts_data if c.get('primary_email'))
        contacts_with_phone = sum(1 for c in all_contacts_data if c.get('primary_phone'))
        contacts_with_deals = sum(1 for c in all_contacts_data if c.get('deals_count', 0) > 0)

        print(f"\n=== ESTATÍSTICAS DOS DADOS ===")
        print(
            f"Contatos com email: {contacts_with_email:,} ({contacts_with_email / len(all_contacts_data) * 100:.1f}%)")
        print(
            f"Contatos com telefone: {contacts_with_phone:,} ({contacts_with_phone / len(all_contacts_data) * 100:.1f}%)")
        print(
            f"Contatos com deals: {contacts_with_deals:,} ({contacts_with_deals / len(all_contacts_data) * 100:.1f}%)")

        print(f"\nMISSÃO CUMPRIDA! Dados extraídos com sucesso.")

    # START
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
        },
        {
            'task_id': 'run_contacts',
            'python_callable': run_contacts
        }
    ]
