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
    URL_BASE = customer['URL_BASE']
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
                timeout=180
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"[ERRO] {method}: {e}")
            return None

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
        periods = 6  # Buscando dados dos últimos 6 dias para teste. Mude para relativedelta(months=periods) se necessário.
        data_filtro = date.today() - relativedelta(days=periods)
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
        df_deals = pd.DataFrame(all_deals)
        df_deals.rename(columns=fields_map, inplace=True)

        # Traduz campos de lista
        for col, enum in enumeration_map.items():
            if col in df_deals.columns:
                df_deals[col] = df_deals[col].astype(str).replace(enum)

        # Traduz campos de usuário
        for user_field in ['Pessoa responsável', 'Criado por', 'Modificado por', 'Corretor responsável',
                           'Gerente responsável', 'Diretor responsável']:
            if user_field in df_deals.columns:
                df_deals[user_field] = df_deals[user_field].astype(str).replace(users_map)

        # Traduz a fase do negócio (STAGE_ID) usando o novo mapa
        if 'Fase do negócio' in df_deals.columns:
            df_deals['Fase do negócio'] = df_deals['Fase do negócio'].astype(str).replace(stages_map)

        print(" -> Dados processados com sucesso.")

        print("\n[RESULTADO] Amostra de negócios:")
        col_exibir = ['ID', 'Título do negócio', 'Fase do negócio', 'Pessoa responsável']
        col_existentes = [c for c in col_exibir if c in df_deals.columns]

        print(df_deals[col_existentes].head() if col_existentes else df_deals.head())

        # Salvar CSV local (opcional)
        df_deals.to_csv('bitrix_crm_deals.csv', sep=';', index=False)

        # Upload para GCS
        print("[PASSO 5] Enviando dados para Google Cloud Storage...")
        gcs_path = f"bitrix_crm_deals.csv"
        upload_to_gcs(df_deals, gcs_path, sep=';')
        print(" -> Upload concluído com sucesso!")

    except Exception as e:
        print(f"\n!!!!!! ERRO CRÍTICO !!!!!!\n{e}")
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
    URL_BASE = customer['URL_BASE']
    BUCKET_NAME = customer['bucket_name']
    SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH

    HEADER = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }

    # ==============================================================================
    # FUNÇÃO AUXILIAR
    # ==============================================================================
    def make_request(method, params={}):
        """Função simples para fazer uma requisição à API."""
        try:
            response = requests.post(
                f"{URL_BASE}/{method}.json",
                headers=HEADER,
                data=json.dumps(params),
                timeout=90
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"ERRO na requisição para {method}: {e}")
            return None

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

    # ==============================================================================
    # INÍCIO DA EXECUÇÃO DO SCRIPT
    # ==============================================================================

    try:
        # PASSO 1: Buscar o mapa de campos de Lead
        print("[PASSO 1] Buscando mapa de campos (fields)...")
        fields_data = make_request('crm.lead.fields')
        fields_map = {}
        enumeration_map = {}
        if fields_data and 'result' in fields_data:
            for code, details in fields_data['result'].items():
                friendly_name = details.get('formLabel') or details.get('listLabel') or details.get('title')
                fields_map[code] = friendly_name
                if details.get('type') == 'enumeration' and 'items' in details and friendly_name:
                    enumeration_map[friendly_name] = {str(item['ID']): str(item['VALUE']) for item in details['items']}
            print(" -> Sucesso!")
        else:
            raise Exception("Falha ao buscar o mapa de campos.")

        # PASSO 2: Buscar o mapa de usuários
        print("[PASSO 2] Buscando mapa de usuários...")
        users_data = make_request('user.get', {'FILTER': {'ACTIVE': 'true'}})
        users_map = {}
        if users_data and 'result' in users_data:
            users_map = {str(u['ID']): f"{u.get('NAME', '')} {u.get('LAST_NAME', '')}".strip() for u in
                         users_data['result']}
            print(" -> Sucesso!")
        else:
            raise Exception("Falha ao buscar o mapa de usuários.")

        # PASSO 3: Buscar todos os Leads com paginação
        print("[PASSO 3] Buscando todos os leads...")
        all_leads = []
        start = 0
        periods = 6
        data_filtro = date.today() - relativedelta(months=periods)
        formatted_date = data_filtro.strftime('%Y-%m-%dT00:00:00-03:00')
        search_params = {
            "SELECT": ["*", "UF_*", "DEAL_ID", "ASSIGNED_BY_ID"],
            "ORDER": {"ID": "ASC"},
            "FILTER": {">DATE_CREATE": formatted_date}
        }
        while True:
            params = search_params.copy()
            params['start'] = start
            print(f"  ...buscando a partir do item {start}")
            response = make_request('crm.lead.list', params)
            if not response or 'result' not in response:
                print(" -> Resposta inválida da API. Interrompendo.")
                break
            current_leads = response['result']
            if not current_leads:
                break
            all_leads.extend(current_leads)
            if 'next' in response:
                start = response['next']
                time.sleep(0.5)
            else:
                break
        print(f" -> Sucesso! {len(all_leads)} leads encontrados.")

        # PASSO 4: Processar os dados com Pandas
        print("[PASSO 4] Processando dados com Pandas...")
        if not all_leads:
            print(" -> Nenhum lead para processar.")
            return

        df_leads = pd.DataFrame(all_leads)
        df_leads.rename(columns=fields_map, inplace=True)
        print("  ...colunas renomeadas.")

        print("  ...iniciando mapeamento de campos de lista (enumeração)...")
        for coluna, mapa in enumeration_map.items():
            if coluna in df_leads.columns:
                # ======================================================================
                # SOLUÇÃO FINAL: Trocando a função .map() pela função .replace()
                # que é uma alternativa mais estável para este caso.
                # ======================================================================
                df_leads[coluna] = df_leads[coluna].astype(str).replace(mapa)

        print("  ...campos de lista mapeados.")

        if 'Criado por' in df_leads.columns:
            df_leads['Criado por'] = df_leads['Criado por'].astype(str).replace(users_map)
        if 'Pessoa responsável' in df_leads.columns:
            df_leads['Pessoa responsável'] = df_leads['Pessoa responsável'].astype(str).replace(users_map)
        if 'Modificado por' in df_leads.columns:
            df_leads['Modificado por'] = df_leads['Modificado por'].astype(str).replace(users_map)
        if 'MOVED_BY_ID' in df_leads.columns:
            df_leads['MOVED_BY_ID'] = df_leads['MOVED_BY_ID'].astype(str).replace(users_map)
        print("  ...usuários mapeados.")

        print(" -> Sucesso no processamento!")

        # PASSO 5: Exibir resultado
        print("\n[RESULTADO FINAL] Amostra dos dados processados:")
        colunas_para_exibir = ['ID', 'Nome do Lead', 'Status', 'Responsável']
        colunas_existentes = [col for col in colunas_para_exibir if col in df_leads.columns]
        if colunas_existentes:
            print(df_leads[colunas_existentes].head())
        else:
            print(df_leads.head())

        # Salvar CSV local (opcional)
        df_leads.to_csv('bitrix_crm_leads.csv', sep=';', index=False)

        # Upload para GCS
        print("[PASSO 6] Enviando dados para Google Cloud Storage...")
        gcs_path = f"bitrix_crm_leads.csv"
        upload_to_gcs(df_leads, gcs_path, sep=';')
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
