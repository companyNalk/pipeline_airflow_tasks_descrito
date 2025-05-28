"""
Sharp module for data extraction functions.
This module contains functions specific to the Sharp integration.
"""

from core import gcs


def run(customer):
    import functions_framework
    import json
    import os
    import pandas as pd
    import re
    import requests
    import unicodedata
    from google.cloud import storage
    import pathlib

    BUCKET_NAME = customer['bucket_name']
    API_URL = customer['api_url']
    ACCOUNT_ID = customer['api_account_id']
    API_SECRET = customer['api_secret']
    ACCOUNT = customer['account']

    # Caminho para o arquivo de credenciais da conta de serviço
    SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH

    # Função para normalização de nomes de colunas
    def normalize_column_name(name):
        nfkd = unicodedata.normalize('NFKD', str(name))
        ascii_name = nfkd.encode('ASCII', 'ignore').decode('ASCII')
        cleaned = re.sub(r"[^\w\s]", "", ascii_name)
        return re.sub(r"\s+", "_", cleaned).lower()

    @functions_framework.http
    def main(request):
        storage_client = storage.Client(project=customer['project_id'])
        bucket = storage_client.bucket(BUCKET_NAME)

        # Função para upload e print de sucesso/erro
        def upload_to_gcs(dataframe, path):
            try:
                # Usando ponto e vírgula (;) como separador
                csv_content = dataframe.to_csv(index=False, sep=';')
                bucket.blob(path).upload_from_string(csv_content, 'text/csv')
                print(f"Dados enviados para {path} com sucesso (separador: ponto e vírgula).")
            except Exception as e:
                print(f"Erro ao enviar dados para {path}: {e}")
                raise

        # STAGES
        fr = []
        offset = -500
        limit = 500
        conta = ACCOUNT
        id_conta = ACCOUNT_ID
        chave_secreta = API_SECRET
        while True:
            offset += 500
            api_url = f'{API_URL}?accountID={id_conta}&secretKey={chave_secreta}'
            api_headers = {'Content-Type': 'application/json'}
            api_playload = json.dumps({
                "method": "getDealStages",
                "params": {
                    "where": {},
                    "limit": limit,
                    "offset": offset
                },
                "id": "1"
            })
            response = requests.request("POST", api_url, headers=api_headers, data=api_playload)
            count = len(response.json().get("result").get("dealStage"))

            if count == 0:
                break
            else:
                json_data = response.json()
                df = pd.DataFrame(json_data.get("result").get("dealStage"))
                df['client'] = conta
                df['offset'] = offset
                df['import_date'] = pd.to_datetime('today').strftime('%Y-%m-%d')
                fr.append(df)

        final = pd.concat(fr)
        # Normalizar nomes das colunas
        final.columns = [normalize_column_name(col) for col in final.columns]
        upload_to_gcs(final, f'stages/stages_{ACCOUNT}.csv')

        # USERS
        fr = []
        offset = -500
        limit = 500
        while True:
            offset += 500
            api_url = f'{API_URL}?accountID={id_conta}&secretKey={chave_secreta}'
            api_headers = {'Content-Type': 'application/json'}
            api_playload = json.dumps({
                "method": "getUserProfiles",
                "params": {
                    "where": {},
                    "limit": limit,
                    "offset": offset
                },
                "id": "1"
            })
            response = requests.request("POST", api_url, headers=api_headers, data=api_playload)
            count = len(response.json().get("result").get("userProfile"))

            if count == 0:
                break
            else:
                json_data = response.json()
                df = pd.DataFrame(json_data.get("result").get("userProfile"))
                df['client'] = conta
                df['offset'] = offset
                df['import_date'] = pd.to_datetime('today').strftime('%Y-%m-%d')
                fr.append(df)

        final = pd.concat(fr)
        # Normalizar nomes das colunas
        final.columns = [normalize_column_name(col) for col in final.columns]
        upload_to_gcs(final, 'Users/users_deskbee.csv')

        # CAMPAIGNS
        fr = []
        offset = -500
        limit = 500
        while True:
            offset += 500
            api_url = f'{API_URL}?accountID={id_conta}&secretKey={chave_secreta}'
            api_headers = {'Content-Type': 'application/json'}
            api_playload = json.dumps({
                "method": "getCampaigns",
                "params": {
                    "where": {},
                    "limit": limit,
                    "offset": offset
                },
                "id": "1"
            })
            response = requests.request("POST", api_url, headers=api_headers, data=api_playload)
            count = len(response.json().get("result").get("campaign"))

            if count == 0:
                break
            else:
                json_data = response.json()
                df = pd.DataFrame(json_data.get("result").get("campaign"))
                df['client'] = conta
                df['offset'] = offset
                df['import_date'] = pd.to_datetime('today').strftime('%Y-%m-%d')
                fr.append(df)

        final = pd.concat(fr)
        # Normalizar nomes das colunas
        final.columns = [normalize_column_name(col) for col in final.columns]
        upload_to_gcs(final, f'campaigns/campaigns_{ACCOUNT}.csv')

        # FIELDS
        fr = []
        offset = -500
        limit = 500
        while True:
            offset += 500
            api_url = f'{API_URL}?accountID={id_conta}&secretKey={chave_secreta}'
            api_headers = {'Content-Type': 'application/json'}
            api_playload = json.dumps({
                "method": "getFields",
                "params": {
                    "where": {},
                    "limit": limit,
                    "offset": offset
                },
                "id": "1"
            })
            response = requests.request("POST", api_url, headers=api_headers, data=api_playload)
            count = len(response.json().get("result").get("field"))

            if count == 0:
                break
            else:
                json_data = response.json()
                df = pd.DataFrame(json_data.get("result").get("field"))
                df['client'] = conta
                df['offset'] = offset
                df['import_date'] = pd.to_datetime('today').strftime('%Y-%m-%d')
                fr.append(df)

        final = pd.concat(fr)
        # Normalizar nomes das colunas
        final.columns = [normalize_column_name(col) for col in final.columns]
        upload_to_gcs(final, f'fields/fields_{ACCOUNT}.csv')

        # LEADS
        fr = []
        offset = -500
        limit = 500
        total_leads = 0
        conta = ACCOUNT
        id_conta = ACCOUNT_ID
        chave_secreta = API_SECRET
        while True:
            offset += 500
            api_url = f'{API_URL}?accountID={id_conta}&secretKey={chave_secreta}'
            api_headers = {'Content-Type': 'application/json'}
            api_playload = json.dumps({
                "method": "getLeads",
                "params": {
                    "where": {},
                    "limit": limit,
                    "offset": offset
                },
                "id": "1"
            })
            response = requests.request("POST", api_url, headers=api_headers, data=api_playload)
            try:
                count = len(response.json().get("result").get("lead"))
            except (ValueError, KeyError):
                print(f"Erro ao decodificar resposta JSON ou chave 'lead' ausente na resposta para offset {offset}.")
                break

            if count == 0:
                break
            else:
                total_leads += count
                print(f"Coletados {total_leads} registros até agora do endpoint Leads...")
                json_data = response.json()
                df = pd.DataFrame(json_data.get("result").get("lead"))
                df['client'] = conta
                df['offset'] = offset
                df['import_date'] = pd.to_datetime('today').strftime('%Y-%m-%d')
                fr.append(df)

        if fr:
            final = pd.concat(fr)
            # Normalizar nomes das colunas
            final.columns = [normalize_column_name(col) for col in final.columns]
            upload_to_gcs(final, f'leads/leads_{ACCOUNT}.csv')
            print(f"Total de registros coletados do endpoint Leads: {total_leads}")

        # OPPORTUNITIES
        fr = []
        offset = -500
        limit = 500
        total_opportunities = 0
        conta = ACCOUNT
        id_conta = ACCOUNT_ID
        chave_secreta = API_SECRET

        while True:
            offset += 500
            api_url = f'{API_URL}?accountID={id_conta}&secretKey={chave_secreta}'
            api_headers = {'Content-Type': 'application/json'}
            api_playload = json.dumps({
                "method": "getOpportunities",
                "params": {
                    "where": {},
                    "limit": limit,
                    "offset": offset
                },
                "id": "1"
            })
            response = requests.request("POST", api_url, headers=api_headers, data=api_playload)
            try:
                opportunities = response.json().get("result").get("opportunity")
                count = len(opportunities)
            except (ValueError, KeyError):
                print(
                    f"Erro ao decodificar resposta JSON ou chave 'opportunity' ausente na resposta para offset {offset}.")
                break

            if count == 0:
                break
            else:
                total_opportunities += count
                print(f"Coletados {total_opportunities} registros até agora do endpoint Opportunities...")
                df = pd.DataFrame(opportunities)
                df['client'] = conta
                df['offset'] = offset
                df['import_date'] = pd.to_datetime('today').strftime('%Y-%m-%d')
                fr.append(df)

        if fr:
            final = pd.concat(fr)
            # Normalizar nomes das colunas
            final.columns = [normalize_column_name(col) for col in final.columns]
            upload_to_gcs(final, f'opportunities/opportunities_{ACCOUNT}.csv')
            print(f"Total de registros coletados do endpoint Opportunities: {total_opportunities}")

        # # EVENTS
        # today = str(date.today() - timedelta(days=1)) + " 00:00:00"
        # fr = []
        # offset = -500
        # limit = 500
        # while True:
        #     offset += 500
        #     api_url = f'{API_URL}?accountID={id_conta}&secretKey={chave_secreta}'
        #     api_headers = {'Content-Type': 'application/json'}
        #     api_playload = json.dumps({
        #         "method": "getEvents",
        #         "params": {
        #             "where": {
        #                 "createTimestamp": today
        #             },
        #             "limit": limit,
        #             "offset": offset
        #         },
        #         "id": "1"
        #     })
        #     response = requests.request("POST", api_url, headers=api_headers, data=api_playload)
        #     try:
        #         count = len(response.json().get("result").get("event"))
        #     except (ValueError, KeyError):
        #         print(f"Erro ao decodificar resposta JSON ou chave 'event' ausente na resposta para offset {offset}.")
        #         break
        # 
        #     if count == 0:
        #         break
        #     else:
        #         json_data = response.json()
        #         df = pd.DataFrame(json_data.get("result").get("event"))
        #         df['client'] = conta
        #         df['offset'] = offset
        #         df['import_date'] = pd.to_datetime('today').strftime('%Y-%m-%d')
        #         fr.append(df)
        # 
        # if fr:
        #     final = pd.concat(fr)
        #     # Normalizar nomes das colunas
        #     final.columns = [normalize_column_name(col) for col in final.columns]
        #     upload_to_gcs(final, f'Events/events_deskbee_{datetime.today().strftime("%Y%m%d%H%M%S")}.csv')

    # START
    main(None)


def get_extraction_tasks():
    """
    Get the list of data extraction tasks for Sharp.

    Returns:
        list: List of task configurations
    """
    return [
        {
            'task_id': 'run',
            'python_callable': run
        }
    ]
