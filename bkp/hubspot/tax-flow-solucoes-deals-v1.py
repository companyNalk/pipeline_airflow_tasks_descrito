import csv
from datetime import datetime
from io import StringIO

import hubspot
from google.cloud import storage
from hubspot.crm.deals import ApiException

# Lista de colunas usadas pelo cliente
NEW_COLUMNS = [
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


def save_deal_properties_to_gcs(client, bucket_name, filename='deals/deals.csv', limit=100):
    try:
        # Inicializar o cliente do Google Cloud Storage
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(filename)

        # Inicializar um StringIO para armazenar os dados CSV
        output = StringIO()
        csv_writer = csv.writer(output, delimiter=';', quoting=csv.QUOTE_MINIMAL)

        csv_writer.writerow(['Deal ID'] + NEW_COLUMNS)

        has_more = True
        after = 0  # Inicialmente, 'after' é zero, que é o ponto de partida para a primeira página
        total_deal_count = 0

        while has_more:
            print("Coletando dados...")

            # Recuperar 'deals' da API do HubSpot
            api_response = client.crm.deals.basic_api.get_page(limit=limit, properties=NEW_COLUMNS, after=after)
            deals = api_response.results
            has_more = api_response.paging is not None
            after = api_response.paging.next.after if has_more else None

            # Processar cada negócio coletado
            for deal in deals:
                created_date = deal.properties.get("createdate", None)
                if created_date:
                    try:
                        # Tenta converter a data usando os dois formatos possíveis
                        parse_date(created_date)

                        deal_id = deal.id
                        row = [deal_id]
                        for column in NEW_COLUMNS:
                            value = deal.properties.get(column, "N/A")

                            # Se o valor for None, não chamar isdigit()
                            if column == "dealstage" and value is not None and not value.isdigit():
                                value = f'"{value}"'

                            row.append(value)

                        csv_writer.writerow(row)
                        total_deal_count += 1

                    except ValueError as e:
                        print(f"Erro ao parsear a data {created_date}: {e}")

        print(f"Total de negócios coletados: {total_deal_count}")

        # Carregar o conteúdo do StringIO para o objeto Blob no GCS
        blob.upload_from_string(output.getvalue(), content_type='text/csv')

    except ApiException as e:
        print(f"Erro na API: {e}")
    except Exception as e:
        print(f"Erro: {e}")


def main():
    # Token e Bucket
    access_token = "XXXXXXXXXXXXXX"
    bucket_name = "hubspot-taxflow"

    client = hubspot.Client.create(access_token=access_token)

    # Coletar os dados e salvar no GCS
    save_deal_properties_to_gcs(client, bucket_name)


main()
