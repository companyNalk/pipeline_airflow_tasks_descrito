"""
Bitrix module for data extraction functions.
This module contains functions specific to the Bitrix integration.
"""

from core import gcs


def run_get_deals(customer):
    import requests
    import json
    import time
    import pandas as pd
    from datetime import date
    from dateutil.relativedelta import relativedelta
    import pathlib
    import os
    from google.cloud import storage

    # =======================================================================
    # CONFIGURAÇÃO
    # =======================================================================
    URL_BASE = customer['url_base']
    BUCKET_NAME = customer['bucket_name']
    SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH

    HEADER = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    def make_request(method, params=None):
        """Faz requisição à API do Bitrix."""
        try:
            response = requests.post(
                f"{URL_BASE}/{method}.json",
                headers=HEADER,
                data=json.dumps(params or {}),
                timeout=190
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"[ERRO] {method}: {e}")
            return None

    # =======================================================================
    # UPLOAD
    # =======================================================================
    def upload_to_gcs(df, gcs_path, sep=';'):
        """Upload de DataFrame para o Google Cloud Storage"""
        if df.empty:
            print("DataFrame vazio, upload cancelado.")
            return False
        try:
            csv_data = df.to_csv(sep=sep, index=False, quoting=1)
            storage_client = storage.Client(project=customer['project_id'])
            bucket = storage_client.bucket(BUCKET_NAME)

            # Upload do arquivo principal
            blob = bucket.blob(gcs_path)
            blob.upload_from_string(csv_data, content_type='text/csv')
            print(f"Arquivo enviado: gs://{BUCKET_NAME}/{gcs_path}")

            return True
        except Exception as e:
            print(f"Erro ao fazer upload: {e}")
            raise

    # =======================================================================
    # EXECUÇÃO
    # =======================================================================
    try:
        print("[PASSO 1] Buscando campos personalizados do negócio...")
        fields_map, enumeration_map = {}, {}
        res_fields = make_request('crm.deal.fields')

        if res_fields and 'result' in res_fields:
            for code, meta in res_fields['result'].items():
                name = meta.get('formLabel') or meta.get('listLabel') or meta.get('title')
                fields_map[code] = name
                if meta.get('type') == 'enumeration' and 'items' in meta:
                    enumeration_map[name] = {str(i['ID']): i['VALUE'] for i in meta['items']}
            print(" -> Campos obtidos com sucesso.")
        else:
            raise Exception("Falha ao buscar campos de negócio.")

        print("[PASSO 2] Buscando usuários...")
        res_users = make_request('user.get', {'FILTER': {'ACTIVE': 'true'}})
        users_map = {str(u['ID']): f"{u.get('NAME', '')} {u.get('LAST_NAME', '')}".strip()
                     for u in res_users['result']} if res_users and 'result' in res_users else {}
        print(" -> Usuários mapeados com sucesso.")

        # PASSO 2.5: Mapear todas as Fases de Negócio (Stages) 🗺️
        # -----------------------------------------------------------------------
        print("[PASSO 2.5] Mapeando fases dos negócios (funis)...")
        stages_map = {}
        res_categories = make_request('crm.dealcategory.list', {'order': {'SORT': 'ASC'}})
        if res_categories and 'result' in res_categories:
            for category in res_categories['result']:
                category_id = category['ID']
                res_stages = make_request('crm.dealcategory.stage.list', {'id': category_id})
                if res_stages and 'result' in res_stages:
                    for stage in res_stages['result']:
                        stages_map[stage['STATUS_ID']] = stage['NAME']
            print(f" -> {len(stages_map)} fases mapeadas com sucesso.")
        else:
            print(" -> AVISO: Não foi possível buscar as categorias de negócio. As fases não serão traduzidas.")

        print("[PASSO 3] Buscando negócios (método paginado)...")
        all_deals = []
        start = 0
        periods = 6
        data_filtro = date.today() - relativedelta(months=periods)
        data_iso = data_filtro.strftime('%Y-%m-%dT00:00:00-03:00')

        # Usando '*' é mais simples e garante que todos os campos, inclusive STAGE_ID, sejam trazidos.
        select_fields = ["*", "UF_*"]

        while True:
            print(f"  -> Buscando a partir de {start}...")
            res = make_request('crm.deal.list', {
                'filter': {">DATE_CREATE": data_iso},
                'order': {'ID': 'ASC'},
                'start': start,
                'select': select_fields
            })

            if not res or 'result' not in res:
                print(" -> Erro na resposta ou fim dos dados.")
                break

            chunk = res['result']
            if not chunk:
                break

            all_deals.extend(chunk)
            # O 'next' indica o início da próxima página
            start = res.get('next')
            if not start:
                break

            time.sleep(0.4)  # respeita limite da API

        print(f" -> Total de negócios obtidos: {len(all_deals)}")

        if not all_deals:
            raise Exception("Nenhum negócio retornado.")

        print("[PASSO 4] Processando dados...")
        df = pd.DataFrame(all_deals)
        df.rename(columns=fields_map, inplace=True)

        # Traduz campos de lista
        for col, enum in enumeration_map.items():
            if col in df.columns:
                df[col] = df[col].astype(str).replace(enum)

        # Traduz campos de usuário
        for user_field in ['Pessoa responsável', 'Criado por', 'Modificado por']:
            if user_field in df.columns:
                df[user_field] = df[user_field].astype(str).replace(users_map)

        # Traduz a fase do negócio (STAGE_ID) usando o novo mapa
        df['Fase do negócio'] = df['Fase do negócio'].astype(str).replace(stages_map)

        print(" -> Dados processados com sucesso.")

        print("\n[RESULTADO] Amostra de negócios:")
        col_exibir = ['ID', 'Título do negócio', 'Fase do negócio', 'Pessoa responsável']
        col_existentes = [c for c in col_exibir if c in df.columns]

        print(df[col_existentes].head() if col_existentes else df.head())

        df.to_csv('bitrix_crm_deals.csv')

        # Upload para GCS
        print("[PASSO 5] Enviando dados para Google Cloud Storage...")
        gcs_path = f"bitrix_crm_deals.csv"
        upload_to_gcs(df, gcs_path, sep=';')
        print(" -> Upload concluído com sucesso!")
    except Exception as e:
        print(f"\n!!!!!! ERRO CRÍTICO QUE INTERROMPEU O SCRIPT !!!!!!")
        print(f"O erro foi: {e}")
        raise


def run_get_leads(customer):
    import requests
    import json
    import time
    import pandas as pd
    from datetime import date
    from dateutil.relativedelta import relativedelta
    import pathlib
    import os
    from google.cloud import storage

    # ==============================================================================
    # CONFIGURAÇÃO
    # ==============================================================================
    URL_BASE = customer['url_base']
    BUCKET_NAME = customer['bucket_name']
    SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH

    HEADER = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    def make_request(method, params=None):
        """Faz requisição à API do Bitrix."""
        try:
            response = requests.post(
                f"{URL_BASE}/{method}.json",
                headers=HEADER,
                data=json.dumps(params or {}),
                timeout=190
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"[ERRO] {method}: {e}")
            return None

    # =======================================================================
    # UPLOAD
    # =======================================================================
    def upload_to_gcs(df, gcs_path, sep=';'):
        """Upload de DataFrame para o Google Cloud Storage"""
        if df.empty:
            print("DataFrame vazio, upload cancelado.")
            return False
        try:
            csv_data = df.to_csv(sep=sep, index=False, quoting=1)
            storage_client = storage.Client(project=customer['project_id'])
            bucket = storage_client.bucket(BUCKET_NAME)

            # Upload do arquivo principal
            blob = bucket.blob(gcs_path)
            blob.upload_from_string(csv_data, content_type='text/csv')
            print(f"Arquivo enviado: gs://{BUCKET_NAME}/{gcs_path}")

            return True
        except Exception as e:
            print(f"Erro ao fazer upload: {e}")
            raise

    # =======================================================================
    # EXECUÇÃO (Lógica adaptada para Leads)
    # =======================================================================
    try:
        print("[PASSO 1] Buscando campos personalizados do Lead...")
        fields_map, enumeration_map = {}, {}
        # LÓGICA 1: Alterado de crm.deal.fields para crm.lead.fields
        res_fields = make_request('crm.lead.fields')

        if res_fields and 'result' in res_fields:
            for code, meta in res_fields['result'].items():
                name = meta.get('formLabel') or meta.get('listLabel') or meta.get('title')
                fields_map[code] = name
                if meta.get('type') == 'enumeration' and 'items' in meta:
                    enumeration_map[name] = {str(i['ID']): i['VALUE'] for i in meta['items']}
            print(" -> Campos de Lead obtidos com sucesso.")
        else:
            raise Exception("Falha ao buscar campos de Lead.")

        print("[PASSO 2] Buscando usuários...")
        # LÓGICA 2: Mapeamento de usuários é idêntico e reutilizável
        res_users = make_request('user.get', {'FILTER': {'ACTIVE': 'true'}})
        users_map = {str(u['ID']): f"{u.get('NAME', '')} {u.get('LAST_NAME', '')}".strip()
                     for u in res_users['result']} if res_users and 'result' in res_users else {}
        print(" -> Usuários mapeados com sucesso.")

        # LÓGICA 2.5: Mapear todos os Status de Lead 🗺️
        # -----------------------------------------------------------------------
        # Em vez de funis e fases, Leads têm uma lista única de status.
        print("[PASSO 2.5] Mapeando status dos Leads...")
        status_map = {}
        # A chamada crm.status.list com o filtro correto busca os status dos leads.
        res_statuses = make_request('crm.status.list', {
            'order': {"SORT": "ASC"},
            'filter': {"ENTITY_ID": "STATUS"}  # STATUS é o ID da entidade para status de lead
        })
        if res_statuses and 'result' in res_statuses:
            for status in res_statuses['result']:
                status_map[status['STATUS_ID']] = status['NAME']
            print(f" -> {len(status_map)} status mapeados com sucesso.")
        else:
            print(" -> AVISO: Não foi possível buscar os status. Os status não serão traduzidos.")

        print("[PASSO 3] Buscando Leads (método paginado)...")
        all_leads = []
        start = 0
        batch_size = 50
        # Ajuste o período conforme necessário. Ex: relativedelta(months=6)
        periods = 6
        data_filtro = date.today() - relativedelta(months=periods)
        data_iso = data_filtro.strftime('%Y-%m-%dT00:00:00-03:00')

        select_fields = ["*", "UF_*"]

        while True:
            print(f"   -> Buscando a partir de {start}...")
            # LÓGICA 3: Alterado de crm.deal.list para crm.lead.list
            res = make_request('crm.lead.list', {
                'filter': {">DATE_CREATE": data_iso},
                'order': {'ID': 'ASC'},
                'start': start,
                'select': select_fields
            })

            if not res or 'result' not in res:
                print(" -> Erro na resposta ou fim dos dados.")
                break

            chunk = res['result']
            if not chunk:
                break

            all_leads.extend(chunk)
            start = res.get('next')
            if not start:
                break

            time.sleep(0.4)

        print(f" -> Total de Leads obtidos: {len(all_leads)}")

        if not all_leads:
            raise Exception("Nenhum Lead retornado.")

        print("[PASSO 4] Processando dados...")
        # LÓGICA 4: A lógica de processamento é a mesma, apenas aplicada aos dados dos leads
        df = pd.DataFrame(all_leads)
        df.rename(columns=fields_map, inplace=True)

        # Traduz campos de lista
        for col, enum in enumeration_map.items():
            if col in df.columns:
                df[col] = df[col].astype(str).replace(enum)

        # Traduz campos de usuário
        for user_field in ['Pessoa responsável', 'Criado por', 'Modificado por']:
            if user_field in df.columns:
                df[user_field] = df[user_field].astype(str).replace(users_map)

        # Traduz o status do lead (o campo original é STATUS_ID)
        # O fields_map geralmente renomeia STATUS_ID para "Status"
        df['Etapa'] = df['Etapa'].astype(str).replace(status_map)

        print(" -> Dados processados com sucesso.")

        print("\n[RESULTADO] Amostra de Leads:")
        col_exibir = ['ID', 'Título', 'Status', 'Pessoa responsável']  # Colunas relevantes para Leads
        col_existentes = [c for c in col_exibir if c in df.columns]

        print(df[col_existentes].head() if col_existentes else df.head())
        df.to_csv('bitrix_crm_leads.csv')

        # Upload para GCS
        print("[PASSO 5] Enviando dados para Google Cloud Storage...")
        gcs_path = f"bitrix_crm_leads.csv"
        upload_to_gcs(df, gcs_path, sep=';')
        print(" -> Upload concluído com sucesso!")

    except Exception as e:
        print(f"\n!!!!!! ERRO CRÍTICO QUE INTERROMPEU O SCRIPT !!!!!!")
        print(f"O erro foi: {e}")
        raise


def get_extraction_tasks():
    """
    Get the list of data extraction tasks for Bitrix.

    Returns:
        list: List of task configurations
    """
    return [
        {
            'task_id': 'run_get_deals',
            'python_callable': run_get_deals
        },
        {
            'task_id': 'run_get_leads',
            'python_callable': run_get_leads
        }
    ]
