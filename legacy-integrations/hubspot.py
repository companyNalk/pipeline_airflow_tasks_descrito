"""
Hubspot module for data extraction functions.
This module contains functions specific to the Hubspot integration.
"""

import csv
from datetime import datetime
from io import StringIO

from core import gcs
from google.cloud import storage
from hubspot.crm.deals import ApiException as DealsApiException
from hubspot.crm.owners import ApiException as OwnersApiException
from hubspot.crm.properties import ApiException as PropertiesApiException

import hubspot
from hubspot import HubSpot

# Lista de colunas usadas para deals
DEAL_COLUMNS = [
    "closedate",
    "closed_lost_reason",
    "closed_won_reason",
    "closedate",
    "createdate",
    "currency_code",
    "hs_is_closed",
    "hs_is_closed_won",
    "id_do_cliente",
    "tipo_do_produto",
    "dealname",
    "dealstage",
    "dealtype",
    "description",
    "engagements_last_meeting_booked",
    "engagements_last_meeting_booked_campaign",
    "engagements_last_meeting_booked_medium",
    "engagements_last_meeting_booked_source",
    "hs_acv",
    "hs_analytics_source",
    "hs_analytics_source_data_1",
    "hs_analytics_source_data_2",
    "hs_arr",
    "hs_forecast_amount",
    "hs_forecast_probability",
    "hs_manual_forecast_category",
    "hs_mrr",
    "hs_next_step",
    "hs_object_id",
    "hs_priority",
    "hs_tcv",
    "hubspot_owner_assigneddate",
    "hubspot_owner_id",
    "hubspot_team_id",
    "notes_last_contacted",
    "notes_last_updated",
    "notes_next_activity_date",
    "num_associated_contacts",
    "num_contacted_notes",
    "num_notes",
    "pipeline",
    "hs_closed_amount"
]


def parse_date(date_str):
    """Tenta fazer parsing da data em dois formatos possíveis."""
    try:
        return datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S.%fZ')  # Com milissegundos
    except ValueError:
        return datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S%z')  # Sem milissegundos


def run_deals(customer):
    """Coleta dados de deals do HubSpot e salva no GCS."""
    try:
        # Inicializar o cliente do HubSpot
        client = hubspot.Client.create(access_token=customer['access_token'])
        bucket_name = customer['bucket_name']
        filename = f"{customer['save_dir_deals']}/deals.csv"
        limit = 100

        # Autenticação com o GCP usando credenciais do ambiente
        credentials = gcs.load_credentials_from_env()
        storage_client = storage.Client(credentials=credentials)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(filename)

        # Inicializar um StringIO para armazenar os dados CSV
        output = StringIO()
        csv_writer = csv.writer(output, delimiter=';', quoting=csv.QUOTE_MINIMAL)

        csv_writer.writerow(['Deal ID'] + DEAL_COLUMNS)

        has_more = True
        after = 0  # Inicialmente, 'after' é zero, que é o ponto de partida para a primeira página
        total_deal_count = 0

        while has_more:
            print("Iniciando coleta de dados...")

            # Recuperar 'deals' da API do HubSpot
            api_response = client.crm.deals.basic_api.get_page(limit=limit, properties=DEAL_COLUMNS, after=after)
            deals = api_response.results
            has_more = api_response.paging is not None
            after = api_response.paging.next.after if has_more else None

            # Processar cada negócio coletado
            for deal in deals:
                created_date = deal.properties.get("createdate", None)
                if created_date:
                    try:
                        # Tenta converter a data usando os dois formatos possíveis
                        deal_date = parse_date(created_date)

                        deal_id = deal.id
                        row = [deal_id]
                        for column in DEAL_COLUMNS:
                            value = deal.properties.get(column, "N/A")
                            row.append(value)

                        csv_writer.writerow(row)
                        total_deal_count += 1

                        # Log a cada 100 registros
                        if total_deal_count % 100 == 0:
                            print(
                                f"Processados {total_deal_count} registros ({100 * total_deal_count // limit}/{limit})")

                    except ValueError as e:
                        print(f"Erro ao parsear a data {created_date}: {e}")

        print(f"Total de negócios coletados: {total_deal_count}")

        # Carregar o conteúdo do StringIO para o objeto Blob no GCS
        local_file_path = f"/tmp/{customer['project_id']}.hubspot.deals.csv"
        with open(local_file_path, 'w') as f:
            f.write(output.getvalue())

        gcs.write_file_to_gcs(
            bucket_name=bucket_name,
            local_file_path=local_file_path,
            destination_name=filename,
            credentials=credentials
        )
        print(f"Arquivo salvo em: gs://{bucket_name}/{filename}")

    except DealsApiException as e:
        print(f"Erro na API do HubSpot (Deals): {e}")
    except Exception as e:
        print(f"Erro: {e}")


def run_owners(customer):
    """Coleta dados de owners do HubSpot e salva no GCS."""
    try:
        # Inicializar o cliente do HubSpot
        client = HubSpot(access_token=customer['access_token'])
        bucket_name = customer['bucket_name']
        file_path = f"{customer['save_dir_owners']}/owner.csv"

        # Obter os owners
        owners_response = client.crm.owners.owners_api.get_page()

        # Verificar se 'results' está presente na resposta
        if not owners_response or not hasattr(owners_response, 'results'):
            print("Nenhum owner encontrado ou resposta inválida da API.")
            return

        owners = owners_response.results

        # Verificar se há owners antes de tentar salvar
        if not owners:
            print("Não há owners para salvar.")
            return

        output = StringIO()
        csv_writer = csv.writer(output, delimiter=';', quoting=csv.QUOTE_MINIMAL)

        # Verificar se o primeiro owner tem dados e escrever as colunas do CSV
        try:
            owner_dict = owners[0].to_dict()
            csv_writer.writerow(list(owner_dict.keys()))
        except Exception as e:
            print(f"Erro ao escrever colunas no CSV: {e}")
            return

        # Escrever os dados de cada owner
        for owner in owners:
            try:
                owner_dict = owner.to_dict()
                csv_writer.writerow(owner_dict.values())
            except Exception as e:
                print(f"Erro ao escrever dados do owner: {e}")

        # Inicializar o cliente do Google Cloud Storage
        credentials = gcs.load_credentials_from_env()

        # Salvar no arquivo local temporário
        local_file_path = f"/tmp/{customer['project_id']}.hubspot.owners.csv"
        with open(local_file_path, 'w') as f:
            f.write(output.getvalue())

        # Upload para o GCS
        gcs.write_file_to_gcs(
            bucket_name=bucket_name,
            local_file_path=local_file_path,
            destination_name=file_path,
            credentials=credentials
        )
        print(f"Arquivo carregado com sucesso para {bucket_name}/{file_path}")

    except OwnersApiException as e:
        print(f"API Exception ocorreu ao buscar owners: {e}")
    except Exception as e:
        print(f"Ocorreu um erro inesperado: {e}")


def run_pipelines(customer):
    """Coleta dados de pipelines do HubSpot e salva no GCS."""
    try:
        # Inicializar o cliente do HubSpot
        client = HubSpot(access_token=customer['access_token'])
        bucket_name = customer['bucket_name']
        file_path = f"{customer['save_dir_pipelines']}/pipeline.csv"

        # Obter os pipelines
        pipelines_response = client.crm.pipelines.pipelines_api.get_all("deals")

        if not pipelines_response or not hasattr(pipelines_response, 'results'):
            print("No pipelines found or invalid response structure")
            return

        pipelines = pipelines_response.results

        output = StringIO()
        csv_writer = csv.writer(output, delimiter=';', quoting=csv.QUOTE_MINIMAL)
        csv_writer.writerow([
            "pipeline_id", "pipeline_name", "pipeline_stage_created_at",
            "pipeline_stage_display_order", "pipeline_stage_id",
            "pipeline_stage_is_archived", "pipeline_stage_is_closed",
            "pipeline_stage_name", "pipeline_stage_probability",
            "pipeline_stage_updated_at", "date"
        ])

        current_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        for pipeline in pipelines:
            pipeline_id = pipeline.id if hasattr(pipeline, 'id') else 'N/A'
            pipeline_name = pipeline.label if hasattr(pipeline, 'label') else 'N/A'
            if hasattr(pipeline, 'stages'):
                for stage in pipeline.stages:
                    csv_writer.writerow([
                        pipeline_id,
                        pipeline_name,
                        stage.created_at if hasattr(stage, 'created_at') else 'N/A',
                        stage.display_order if hasattr(stage, 'display_order') else 'N/A',
                        stage.id if hasattr(stage, 'id') else 'N/A',
                        stage.archived if hasattr(stage, 'archived') else 'N/A',
                        stage.metadata.get("closed", "N/A"),
                        stage.label if hasattr(stage, 'label') else 'N/A',
                        stage.metadata.get("probability", "N/A"),
                        stage.updated_at if hasattr(stage, 'updated_at') else 'N/A',
                        current_date
                    ])
            else:
                print(f"Pipeline {pipeline_id} has no stages.")

        # Inicializar o cliente do Google Cloud Storage
        credentials = gcs.load_credentials_from_env()

        # Salvar no arquivo local temporário
        local_file_path = f"/tmp/{customer['project_id']}.hubspot.pipelines.csv"
        with open(local_file_path, 'w') as f:
            f.write(output.getvalue())

        # Upload para o GCS
        gcs.write_file_to_gcs(
            bucket_name=bucket_name,
            local_file_path=local_file_path,
            destination_name=file_path,
            credentials=credentials
        )
        print(f"File successfully uploaded to {bucket_name}/{file_path}")

    except PropertiesApiException as e:
        print(f"API Exception occurred while fetching pipelines: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


def run_contacts(customer):
    import csv
    import re
    import time
    import unicodedata
    from datetime import datetime, timedelta
    from io import StringIO

    # Importe suas funções do módulo gcs
    from core import gcs
    import pandas as pd
    import requests
    from hubspot.crm.contacts.exceptions import ApiException as ContactsApiException

    def _normalize_key(key: str) -> str:
        """
        Normaliza uma chave de string aplicando transformações para padronização.
        """
        key = key.strip('\'"')

        # Detecta se é um camelCase com a primeira letra maiúscula (PascalCase)
        is_pascal_case = bool(re.match(r'^[A-Z]', key) and re.search(r'[a-z]', key))

        # Detecta se é um verdadeiro camelCase (inicia com minúscula, depois tem maiúscula)
        is_true_camel_case = bool(re.match(r'^[a-z].*[A-Z]', key))

        # Converte PascalCase para snake_case (ex: ExibeContato -> exibe_contato)
        if is_pascal_case:
            # Insere underscore antes de cada letra maiúscula, exceto a primeira
            key = re.sub(r'(?<!^)([A-Z][a-z])', r'_\1', key)
            # Trata seguências de maiúsculas como siglas (ex: ID em IdStatusGestao)
            key = re.sub(r'([A-Z])([A-Z][a-z])', r'\1_\2', key)
        # Se for camelCase, aplica a conversão para snake_case antes
        elif is_true_camel_case:
            key = re.sub(r'([a-z])([A-Z])', r'\1_\2', key)

        # Converte para lowercase
        key = key.lower()

        # Remove acentos e outros caracteres Unicode
        key = unicodedata.normalize('NFKD', key).encode('ASCII', 'ignore').decode('ASCII')

        # Substitui caracteres especiais e espaços por underscores
        key = re.sub(r'[^a-z0-9]+', '_', key)

        # Remove underscores extras no início ou fim
        key = key.strip('_')

        return key

    def generate_date_ranges(start_date_str, end_date_str=None, interval_days=30):
        """
        Gera intervalos de datas para dividir consultas grandes.
        """
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")

        if end_date_str:
            end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
        else:
            end_date = datetime.now()

        date_ranges = []
        current_start = start_date

        while current_start < end_date:
            current_end = current_start + timedelta(days=interval_days)
            if current_end > end_date:
                current_end = end_date

            date_ranges.append((
                current_start.strftime("%Y-%m-%d"),
                current_end.strftime("%Y-%m-%d")
            ))

            current_start = current_end + timedelta(days=1)

        return date_ranges

    def chunk_list(lst, chunk_size):
        """
        Divide uma lista em chunks (lotes) de tamanho especificado.
        """
        return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]

    def get_all_contacts(headers, start_date="2024-01-01", end_date=None, interval_days=15):
        """
        Recupera todos os contatos do HubSpot, dividindo por intervalos de data
        para lidar com a limitação de 10.000 registros.
        """
        API_BASE_URL = "https://api.hubapi.com/crm/v3/objects"
        all_contacts = []

        # Gera intervalos de datas para dividir a busca
        date_ranges = generate_date_ranges(start_date, end_date, interval_days)

        for range_idx, (range_start, range_end) in enumerate(date_ranges):
            print(
                f"Buscando contatos criados entre {range_start} e {range_end} (intervalo {range_idx + 1}/{len(date_ranges)})...")

            after = None
            page_size = 100  # Máximo permitido pela API do HubSpot

            while True:
                # Preparar parâmetros de busca
                data = {
                    "filterGroups": [
                        {
                            "filters": [
                                {
                                    "propertyName": "createdate",
                                    "operator": "GTE",
                                    "value": range_start
                                },
                                {
                                    "propertyName": "createdate",
                                    "operator": "LTE",
                                    "value": range_end
                                }
                            ]
                        }
                    ],
                    "properties": ['createdate', 'firstname', 'lastname', 'nome', 'cargo', 'email', 'company',
                                   'facebook',
                                   'hs_facebook_ad_clicked', 'hs_analytics_source',
                                   'hs_analytics_source_data_1', 'hs_analytics_source_data_2', 'hs_object_source',
                                   'hs_object_source_detail_1',
                                   'hs_object_source_detail_2', 'hs_object_source_detail_3', 'hs_object_source_label',
                                   'hs_object_source_user_id',
                                   'hs_facebookid', 'hs_journey_stage', 'hs_lead_status', 'hs_pipeline',
                                   'lifecyclestage',
                                   'mk__facebook_lead',
                                   'mk__lead_com_jornada_', 'hs_analytics_first_touch_converting_campaign',
                                   'mk__nome_da_campanha',
                                   'mk__nome_do_anuncio',
                                   'origem_do_lead', 'origem_o_lead', 'ownername', 'utm_campaign', 'utm_content',
                                   'utm_source',
                                   'b2b_i_jornada_do_lead',
                                   'campanhas', 'closedate'],
                    "limit": page_size
                }

                # Adicionar cursor after para paginação se não for a primeira página
                if after:
                    data["after"] = after

                url_contacts = f"{API_BASE_URL}/contacts/search"

                try:
                    response = requests.post(url_contacts, headers=headers, json=data)
                    response.raise_for_status()  # Levanta exceção se a resposta for 4xx ou 5xx

                    results = response.json()
                    contacts_page = results.get('results', [])
                    all_contacts.extend(contacts_page)

                    # Verifica se há mais páginas
                    paging = results.get('paging', {})
                    if paging and 'next' in paging and paging['next'].get('after'):
                        after = paging['next']['after']
                        print(f"  Obtidos {len(contacts_page)} contatos. Buscando próxima página...")
                        time.sleep(0.2)  # Pequena pausa para não sobrecarregar a API
                    else:
                        # Não há mais páginas
                        print(f"  Intervalo concluído: {len(contacts_page)} contatos na última página.")
                        break

                except Exception as e:
                    print(f"  Erro ao buscar contatos para o intervalo {range_start} a {range_end}: {str(e)}")
                    # Continua para o próximo intervalo em caso de erro
                    break

            # Adiciona um intervalo maior entre intervalos de data
            if range_idx < len(date_ranges) - 1:
                print(f"  Pausa entre intervalos de data para respeitar limites de API...")
                time.sleep(1.0)

        print(f"Total de contatos obtidos: {len(all_contacts)}")
        return all_contacts

    def get_deal_associations(contacts_df, headers):
        """
        Recupera associações de deals para os contatos em lotes.
        """
        URL_ASSOCIATIONS = 'https://api.hubapi.com/crm/v3/associations/contacts/deals/batch/read'

        # Inicializa coluna para IDs de deals
        contacts_df['deal_ids'] = [[] for _ in range(len(contacts_df))]

        # Identifica os contact_ids para buscar associações
        contact_ids = contacts_df['id'].tolist()

        # Processa em lotes de 10 para evitar sobrecarga na API
        batch_size = 100
        batches = chunk_list(contact_ids, batch_size)

        print(f"Buscando associações para {len(contact_ids)} contatos em {len(batches)} lotes...")

        # Dicionário para mapear contact_id -> índice no DataFrame
        id_to_index = {contact_id: idx for idx, contact_id in enumerate(contacts_df['id'])}

        # Variável para rastrear o número máximo de deals por contato
        max_deals = 0

        for batch_idx, batch in enumerate(batches):
            try:
                # Prepara os inputs para a requisição de associações
                inputs = [{"id": contact_id} for contact_id in batch]

                # Faz a requisição para obter as associações
                associations_response = requests.post(
                    URL_ASSOCIATIONS,
                    headers=headers,
                    json={'inputs': inputs}
                )

                # Status 207 é normal nesse caso - alguns contatos podem não ter deals associados
                if associations_response.status_code in [200, 207]:
                    response_data = associations_response.json()

                    # Loga informações de erro apenas para debug
                    if associations_response.status_code == 207 and batch_idx % 50 == 0:
                        # Mostra mensagem apenas a cada 50 lotes para não poluir o console
                        print(f"Progresso: lote {batch_idx + 1}/{len(batches)}. Alguns contatos sem deals (normal).")

                    # Processa os resultados bem-sucedidos
                    for result in response_data.get('results', []):
                        contact_id = result.get('from', {}).get('id')
                        deal_associations = result.get('to', [])

                        # Se houver associações, adiciona todos os deal_ids à lista
                        if contact_id and deal_associations:
                            idx = id_to_index.get(contact_id)
                            if idx is not None:
                                # Extrai os IDs dos deals associados
                                deal_ids = [deal.get('id') for deal in deal_associations]
                                contacts_df.at[idx, 'deal_ids'] = deal_ids

                                # Atualiza o número máximo de deals
                                max_deals = max(max_deals, len(deal_ids))
                else:
                    if batch_idx % 50 == 0:  # Reduz a verbosidade dos logs
                        print(f"Aviso no lote {batch_idx + 1}: Status {associations_response.status_code}")

            except Exception as e:
                print(f"Erro ao processar o lote {batch_idx + 1}: {str(e)}")

            # Adiciona um pequeno atraso para não sobrecarregar a API
            if batch_idx < len(batches) - 1 and batch_idx % 20 == 0:
                time.sleep(0.5)

        # Converte a lista de deals para colunas dinâmicas
        print(f"Número máximo de deals por contato: {max_deals}")

        # Cria colunas deal_id_1, deal_id_2, etc. dinamicamente
        for i in range(1, max_deals + 1):
            column_name = f'deal_id_{i}'
            contacts_df[column_name] = contacts_df['deal_ids'].apply(
                lambda ids: ids[i - 1] if len(ids) >= i else None
            )

        return contacts_df

    def normalize_dataframe_headers(df):
        """
        Normaliza os headers de um DataFrame usando a função _normalize_key.
        """
        normalized_columns = {col: _normalize_key(col) for col in df.columns}
        return df.rename(columns=normalized_columns)

    def main():
        """
        Coleta dados de contatos do HubSpot e seus deals associados e salva no GCS.
        """
        try:
            # Configurações de acesso
            access_token = customer['access_token']
            bucket_name = customer['bucket_name']
            file_path = f"{customer.get('save_dir_contacts', 'contacts')}/contacts.csv"

            # Inicializar cliente HubSpot e configurar headers
            headers = {
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json"
            }

            # Obter data de início para consulta (padrão: 01/01/2024 ou configurável)
            start_date = customer.get('contacts_start_date', '2024-01-01')

            print(f"Iniciando coleta de contatos a partir de {start_date}...")

            # Etapa 1: Obter todos os contatos usando intervalos
            contacts = get_all_contacts(
                headers=headers,
                start_date=start_date,
                interval_days=customer.get('interval_days', 15)
            )

            # Verificar se há contatos
            if not contacts:
                print("Nenhum contato encontrado.")
                return

            # Criar dataframe com os contatos
            contact_data = []
            for contact in contacts:
                # Adiciona o id do contato às propriedades
                contact_properties = contact['properties']
                contact_properties['id'] = contact['id']
                contact_data.append(contact_properties)

            # Cria o DataFrame com todos os contatos
            df_contacts = pd.DataFrame(contact_data)
            print(f"DataFrame criado com {len(df_contacts)} contatos.")

            # Removendo possíveis duplicatas (caso o mesmo contato apareça em intervalos diferentes)
            df_contacts = df_contacts.drop_duplicates(subset=['id'])
            print(f"DataFrame após remover duplicatas: {len(df_contacts)} contatos.")

            # Etapa 2: Obter associações de deals
            print("Buscando associações de deals...")
            df_contacts = get_deal_associations(df_contacts, headers)

            # Normaliza os headers do DataFrame
            df_contacts = normalize_dataframe_headers(df_contacts)

            # Estatísticas sobre os deals
            deals_count = df_contacts['deal_id_1'].notna().sum()
            print(f"\nResultados:")
            print(f"Total de contatos: {len(df_contacts)}")
            print(
                f"Contatos com pelo menos um deal associado: {deals_count} ({deals_count / len(df_contacts) * 100:.2f}%)")

            # Prepara o CSV
            output = StringIO()
            df_contacts.to_csv(output, sep=';', index=False, quoting=csv.QUOTE_MINIMAL)

            # Inicializar o cliente do Google Cloud Storage
            credentials = gcs.load_credentials_from_env()

            # Salvar no arquivo local temporário
            local_file_path = f"/tmp/{customer['project_id']}.hubspot.contacts.csv"
            with open(local_file_path, 'w') as f:
                f.write(output.getvalue())

            # Upload para o GCS
            gcs.write_file_to_gcs(
                bucket_name=bucket_name,
                local_file_path=local_file_path,
                destination_name=file_path,
                credentials=credentials
            )
            print(f"Arquivo carregado com sucesso para {bucket_name}/{file_path}")

            return True

        except ContactsApiException as e:
            print(f"API Exception ocorreu ao buscar contatos: {e}")
            return False
        except Exception as e:
            print(f"Ocorreu um erro inesperado: {e}")
            return False

    main()


def get_extraction_tasks():
    """
    Get the list of data extraction tasks for Hubspot.
    
    Returns:
        list: List of task configurations
    """
    return [
        {
            'task_id': 'run_deals',
            'python_callable': run_deals
        },
        {
            'task_id': 'run_owners',
            'python_callable': run_owners
        },
        {
            'task_id': 'run_pipelines',
            'python_callable': run_pipelines
        },
        {
            'task_id': 'run_contacts',
            'python_callable': run_contacts
        }
    ]
