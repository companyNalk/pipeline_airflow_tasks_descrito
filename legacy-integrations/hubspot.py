"""
Hubspot module for data extraction functions.
This module contains functions specific to the Hubspot integration.
"""

from core import gcs
import csv
from io import StringIO
import hubspot
from hubspot.crm.deals import ApiException as DealsApiException
from hubspot.crm.owners import ApiException as OwnersApiException
from hubspot.crm.properties import ApiException as PropertiesApiException
from hubspot import HubSpot
from google.cloud import storage
from datetime import datetime

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
                            print(f"Processados {total_deal_count} registros ({100 * total_deal_count // limit}/{limit})")

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
        }
    ]
