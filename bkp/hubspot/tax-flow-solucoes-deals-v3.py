import csv
from datetime import datetime
from io import StringIO
import json
import requests
import time

import hubspot
from google.cloud import storage
from hubspot.crm.deals import ApiException

# Lista de colunas base usadas pelo cliente
BASE_COLUMNS = [
    "closedate",
    "closed_lost_reason",
    "closed_won_reason",
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
    "hs_closed_amount",

    # Campos personalizados
    "executivo_de_vendas",
    "dealtype",
    "motivo_de_lead_perdido",
    "objecao",
    "state",
    "gerente_de_conta",
    "servico_s__contratado_s_",
    "cross_sell",
    "segmanetacao",
    "classificacao_em_flags"
]


def parse_date(date_str):
    """Tenta fazer parsing da data em dois formatos possíveis."""
    if not date_str:
        return None

    try:
        return datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S.%fZ')  # Com milissegundos
    except ValueError:
        try:
            return datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%S%z')  # Sem milissegundos
        except ValueError:
            try:
                return datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%SZ')  # Formato simples
            except ValueError:
                print(f"Formato de data não reconhecido: {date_str}")
                return None


def get_all_deal_properties(client):
    """Recupera todas as propriedades de deals disponíveis na API do HubSpot."""
    try:
        # Obtém todas as propriedades disponíveis
        properties_response = client.crm.properties.core_api.get_all("deal")
        all_properties = properties_response.results

        # Extrai nomes de propriedades
        property_names = [prop.name for prop in all_properties]

        # Encontra propriedades que contêm 'date_entered'
        date_entered_props = [name for name in property_names if 'date_entered' in name]

        print(f"Encontrados {len(date_entered_props)} campos com 'date_entered'")

        return property_names, date_entered_props

    except Exception as e:
        print(f"Erro ao obter propriedades: {e}")
        return [], []


def obter_ids_unicos_executivos(client, access_token):
    """Obtém todos os IDs únicos do campo executivo_de_vendas."""
    print("Coletando IDs únicos de executivos de vendas...")
    ids_executivos = set()
    limit = 100
    after = None
    total_deals = 0

    try:
        while True:
            params = {
                "properties": ["executivo_de_vendas"],
                "limit": limit
            }
            if after:
                params["after"] = after

            deals = client.crm.deals.basic_api.get_page(**params)

            for deal in deals.results:
                executivo_id = deal.properties.get("executivo_de_vendas")
                if executivo_id and executivo_id not in ["N/A", "null", ""]:
                    ids_executivos.add(executivo_id)

            total_deals += len(deals.results)

            if not deals.paging:
                break

            after = deals.paging.next.after
            print(f"Processados {total_deals} deals, encontrados {len(ids_executivos)} IDs únicos")

    except Exception as e:
        print(f"Erro ao obter IDs únicos: {e}")

    print(
        f"Coleta concluída. Total de {total_deals} deals processados. Encontrados {len(ids_executivos)} IDs únicos de executivos.")
    return list(ids_executivos)


def criar_mapeamento_executivos(ids_executivos, access_token):
    """Cria um mapeamento de IDs para nomes de executivos usando a API direta."""
    print("Criando mapeamento de IDs de executivos para nomes...")
    mapeamento = {}

    # Cabeçalho para as requisições
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    for id_executivo in ids_executivos:
        if not id_executivo:
            continue

        # Tenta obter dados pelo endpoint de owners
        try:
            url = f"https://api.hubapi.com/crm/v3/owners/{id_executivo}"
            response = requests.get(url, headers=headers)

            if response.status_code == 200:
                owner_data = response.json()
                nome = f"{owner_data['firstName']}"
                mapeamento[id_executivo] = nome
                print(f"Mapeado via API de owners: {id_executivo} -> {nome}")
            else:
                # Se não encontrar, tenta via endpoints alternativos
                print(f"Não foi possível mapear o ID {id_executivo} via API de owners. Status: {response.status_code}")

                # Tentar via API de usuários como alternativa
                try:
                    url = f"https://api.hubapi.com/settings/v3/users/{id_executivo}"
                    response = requests.get(url, headers=headers)

                    if response.status_code == 200:
                        user_data = response.json()
                        nome = f"{user_data['firstName']} {user_data['lastName']}"
                        mapeamento[id_executivo] = nome
                        print(f"Mapeado via API de usuários: {id_executivo} -> {nome}")
                    else:
                        # Se não encontrar, mantém o ID original
                        mapeamento[id_executivo] = f"ID {id_executivo}"
                except Exception as e:
                    print(f"Erro ao buscar usuário {id_executivo}: {e}")
                    mapeamento[id_executivo] = f"ID {id_executivo}"

            # Pequena pausa para não sobrecarregar a API
            time.sleep(0.1)

        except Exception as e:
            print(f"Erro ao mapear ID {id_executivo}: {e}")
            mapeamento[id_executivo] = f"ID {id_executivo}"

    print(f"Mapeamento concluído. {len(mapeamento)} IDs mapeados.")

    # Salvar mapeamento em arquivo para referência e uso futuro
    try:
        with open('mapeamento_executivos.json', 'w') as f:
            json.dump(mapeamento, f, indent=4)
        print("Mapeamento salvo em 'mapeamento_executivos.json'")
    except Exception as e:
        print(f"Erro ao salvar mapeamento: {e}")

    return mapeamento


def save_deal_properties_to_gcs(client, bucket_name, access_token, filename='deals/deals.csv', limit=100):
    try:
        # Inicializar o cliente do Google Cloud Storage
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(filename)

        # Obter todas as propriedades disponíveis e campos date_entered
        available_properties, date_entered_props = get_all_deal_properties(client)

        # Adicionar campos date_entered à lista base
        all_columns = BASE_COLUMNS.copy()
        all_columns.extend(date_entered_props)

        # Filtrar apenas propriedades válidas
        valid_columns = [col for col in all_columns if col in available_properties]
        print(f"Total de {len(valid_columns)} propriedades válidas a serem coletadas")

        if len(valid_columns) < len(all_columns):
            missing_props = [prop for prop in all_columns if prop not in available_properties]
            print(f"AVISO: {len(missing_props)} propriedades não encontradas:")
            for prop in missing_props[:10]:
                print(f"  - {prop}")
            if len(missing_props) > 10:
                print(f"  ... e mais {len(missing_props) - 10} propriedades")

        # Obter todos os IDs únicos de executivos
        ids_executivos = obter_ids_unicos_executivos(client, access_token)

        # Criar mapeamento completo de IDs para nomes
        mapeamento_executivos = criar_mapeamento_executivos(ids_executivos, access_token)

        # Inicializar um StringIO para armazenar os dados CSV
        output = StringIO()
        csv_writer = csv.writer(output, delimiter=';', quoting=csv.QUOTE_MINIMAL)

        # Escrever cabeçalho (incluindo a nova coluna)
        csv_writer.writerow(['Deal ID'] + valid_columns + ['executivo_vendas_nome'])

        has_more = True
        after = 0  # Inicialmente, 'after' é zero, que é o ponto de partida para a primeira página
        total_deal_count = 0

        while has_more:
            print(f"Coletando dados (página {total_deal_count // limit + 1})...")

            # Recuperar 'deals' da API do HubSpot
            api_response = client.crm.deals.basic_api.get_page(limit=limit, properties=valid_columns, after=after)
            deals = api_response.results
            has_more = api_response.paging is not None
            after = api_response.paging.next.after if has_more else None

            print(f"Obtidos {len(deals)} negócios nesta página")

            # Processar cada negócio coletado
            for deal in deals:
                created_date = deal.properties.get("createdate", None)
                if created_date:
                    try:
                        # Tenta converter a data usando os formatos possíveis
                        deal_date = parse_date(created_date)

                        if deal_date:  # Se a data foi parseada com sucesso
                            deal_id = deal.id
                            row = [deal_id]

                            # Obter o ID do executivo para usar depois
                            executivo_id = deal.properties.get("executivo_de_vendas", "N/A")

                            # Adicionar todas as colunas originais
                            for column in valid_columns:
                                value = deal.properties.get(column, "N/A")

                                # Se o valor for None, não chamar isdigit()
                                if column == "dealstage" and value is not None and not value.isdigit():
                                    value = f'"{value}"'

                                row.append(value)

                            # Adicionar a coluna com o nome do executivo
                            if executivo_id != "N/A" and executivo_id in mapeamento_executivos:
                                row.append(mapeamento_executivos[executivo_id])
                            else:
                                row.append("N/A")

                            csv_writer.writerow(row)
                            total_deal_count += 1

                            # Exibir progresso a cada 25 negócios
                            if total_deal_count % 25 == 0:
                                print(f"Processados {total_deal_count} negócios até agora")

                    except ValueError as e:
                        print(f"Erro ao parsear a data {created_date}: {e}")

        print(f"Total de negócios coletados: {total_deal_count}")

        # Carregar o conteúdo do StringIO para o objeto Blob no GCS
        blob.upload_from_string(output.getvalue(), content_type='text/csv')
        print(f"Arquivo CSV salvo com sucesso no bucket {bucket_name}, caminho {filename}")

    except ApiException as e:
        print(f"Erro na API: {e}")
    except Exception as e:
        print(f"Erro: {e}")
        import traceback
        traceback.print_exc()


def main():
    # Token e Bucket
    access_token = "XXXXXXXXXXXXXX"
    bucket_name = "hubspot-taxflow"

    client = hubspot.Client.create(access_token=access_token)

    # Coletar os dados e salvar no GCS
    save_deal_properties_to_gcs(client, bucket_name, access_token)


main()
