"""
Pipedrive module for data extraction functions.
This module contains functions specific to the Pipedrive integration.
"""

from core import gcs

def run(customer):
    import requests, pandas as pd, os, re, unicodedata, time, pathlib
    from google.cloud import storage
    from typing import Dict, List, Any, Tuple, Optional

    # Configurações
    SERVICE_ACCOUNT_FILE = pathlib.Path('config', 'setup_automatico.json').as_posix()
    PROJECT_ID, BUCKET_NAME = customer['project_id'], customer['bucket_name']
    API_TOKEN = customer['api_token']
    COMPANY_DOMAIN = customer['company_domain']
    BASE_URL_V1 = customer['base_url_v1'].lower().format(company_domain=COMPANY_DOMAIN)
    BASE_URL_V2 = customer['base_url_v2'].lower().format(company_domain=COMPANY_DOMAIN)
    FOLDERS = {
        'deals': customer['folders_deals'],
        'deals_dados_finais': customer['folders_deals_dados_finais'],
        'pipelines': customer['folders_pipelines'],
        'products': customer['folders_products'],
        'stages': customer['folders_stages'],
        'fields_mapping': customer['folders_fields_mapping'],
    }

    # Iniciar cliente GCS
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_FILE
    storage_client = storage.Client.from_service_account_json(SERVICE_ACCOUNT_FILE, project=PROJECT_ID)

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

    def safe_request(url: str, params: Dict = None, method: str = 'get', retries: int = 3) -> Dict:
        """Realiza requisições HTTP com retry"""
        if params is None: params = {}
        params['api_token'] = API_TOKEN
        
        for attempt in range(retries):
            try:
                if method.lower() == 'get': response = requests.get(url, params=params)
                else: response = requests.post(url, params=params)
                response.raise_for_status()
                return response.json()
            except Exception as e:
                if attempt < retries - 1: time.sleep(2 * (attempt + 1))
                else: return {"data": []}

    def upload_to_storage(dataframe: pd.DataFrame, folder: str, filename: str) -> bool:
        """Upload para o GCS"""
        if dataframe.empty: return False
        
        try:
            dataframe.columns = [normalize_column_name(col) for col in dataframe.columns]
            dataframe = dataframe.loc[:, ~dataframe.columns.duplicated()]
            
            bucket = storage_client.bucket(BUCKET_NAME)
            blob = bucket.blob(f"{folder}/{filename}")
            csv_data = dataframe.to_csv(index=False, sep=';', encoding='utf-8-sig')
            blob.upload_from_string(csv_data, content_type="text/csv")
            print(f"Upload: {len(dataframe)} registros em {folder}/{filename}")
            return True
        except Exception as e:
            print(f"Erro no upload de {filename}: {e}")
            return False

    def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
        """Normaliza DataFrame"""
        if df.empty: return df
        df.columns = [normalize_column_name(col) for col in df.columns]
        return df.loc[:, ~df.columns.duplicated()]

    def debug_stages_data(stages_df, deals_df):
        """Diagnostica problemas com stages"""
        if 'stage_id' not in deals_df.columns:
            print("ERRO: Coluna 'stage_id' não encontrada em deals_df!")
            return
        
        stage_ids_in_deals = deals_df['stage_id'].astype(str).unique()
        stage_ids_in_stages = stages_df['id'].astype(str).unique()
        
        missing_stages = [sid for sid in stage_ids_in_deals if sid not in stage_ids_in_stages]
        if missing_stages:
            print(f"ALERTA: {len(missing_stages)} IDs de stages em deals não estão em stages_df!")

    def diagnose_and_fix_stage_id(deals_df, stages_df):
        """Identifica e corrige o problema da coluna stage_id"""
        stage_columns = ['stage_id', 'etapa', 'stage', 'pipeline_stage_id']
        existing_stage_columns = [col for col in stage_columns if col in deals_df.columns]
        
        if existing_stage_columns:
            stage_column = existing_stage_columns[0]
            if stage_column != 'stage_id':
                deals_df['stage_id'] = deals_df[stage_column]
        else:
            numeric_cols = [col for col in deals_df.columns 
                          if deals_df[col].dtype in ['int64', 'float64'] 
                          or (pd.api.types.is_object_dtype(deals_df[col]) 
                              and deals_df[col].astype(str).str.isdigit().any())]
            
            stage_ids = set(str(x) for x in stages_df['id'])
            
            for col in numeric_cols:
                unique_values = set(str(x) for x in deals_df[col].unique() if pd.notna(x))
                match_count = len(unique_values.intersection(stage_ids))
                
                if match_count > 0 and (match_count >= 3 or match_count / len(unique_values) > 0.2):
                    deals_df['stage_id'] = deals_df[col]
                    break
        
        return deals_df

    def fetch_all_mappings() -> Tuple[Dict, Dict]:
        """Obtém mapeamentos do Pipedrive"""
        field_types = {'deal': 'dealFields', 'product': 'productFields'}
        field_mappings, dropdown_mappings = {}, {}
        all_value_mappings, standard_fields = {}, set()
        
        # Identificar campos padrão
        for entity_type, endpoint in field_types.items():
            for field in safe_request(f"{BASE_URL_V1}/{endpoint}").get('data', []):
                if field.get('edit_flag', False) == False:
                    standard_fields.add(normalize_column_name(field.get('name', '')))
        
        # Processar campos e valores
        for entity_type, endpoint in field_types.items():
            entity_field_map, entity_dropdown_map = {}, {}
            
            for field in safe_request(f"{BASE_URL_V1}/{endpoint}").get('data', []):
                field_id, field_key = str(field.get('id', '')), field.get('key', '')
                if not field_key: continue
                
                name = normalize_column_name(field.get('name', ''))
                
                # Tratar colisão com campo padrão
                if field.get('edit_flag', True) and name in standard_fields:
                    name = f"{name}_custom_{field_id[:8]}"
                    print(f"Colisão: '{field.get('name')}' -> '{name}'")
                
                # Mapear ID e key para nome normalizado
                entity_field_map[field_key] = name
                if field_id and field_id != field_key:
                    entity_field_map[field_id] = name
                
                # Mapear valores para campos de dropdown/enum/set
                if field.get('field_type') in ['enum', 'set', 'status', 'varchar_options'] and field.get('options'):
                    option_map = {}
                    for option in field['options']:
                        option_id = str(option.get('id', ''))
                        if option_id:
                            option_map[option_id] = normalize_column_name(option.get('label', ''))
                            all_value_mappings[option_id] = option_map[option_id]
                    
                    if option_map:
                        entity_dropdown_map[field_key] = option_map
                        if field_id and field_id != field_key:
                            entity_dropdown_map[field_id] = option_map
            
            field_mappings[entity_type] = entity_field_map
            dropdown_mappings[entity_type] = entity_dropdown_map
            print(f"Mapeados {len(entity_field_map)} campos para {entity_type}")
        
        dropdown_mappings['_all_values'] = all_value_mappings
        return field_mappings, dropdown_mappings

    def save_mappings(field_mappings: Dict, dropdown_mappings: Dict) -> None:
        """Salva mapeamentos no GCS"""
        fields_rows, dropdown_rows = [], []
        
        # Preparar dados de field mappings
        for entity_type, mapping in field_mappings.items():
            for field_key, field_name in mapping.items():
                fields_rows.append({'entity_type': entity_type, 'field_key': field_key, 'field_name': field_name})
        
        # Preparar dados de dropdown mappings
        for entity_type, dropdown_maps in dropdown_mappings.items():
            if entity_type == '_all_values': continue
            for field_key, options in dropdown_maps.items():
                for option_id, option_label in options.items():
                    dropdown_rows.append({
                        'entity_type': entity_type, 'field_key': field_key,
                        'option_id': option_id, 'option_label': option_label
                    })
        
        # Upload dos dados
        if fields_rows:
            upload_to_storage(pd.DataFrame(fields_rows), FOLDERS['fields_mapping'], 'fields_mapping.csv')
        if dropdown_rows:
            upload_to_storage(pd.DataFrame(dropdown_rows), FOLDERS['fields_mapping'], 'dropdown_mapping.csv')

    def fetch_paginated_data(endpoint: str, api_version: int = 1, params: Dict = None) -> List[Dict]:
        """Busca dados paginados"""
        all_data, limit = [], 500
        params = params or {}
        
        if api_version == 1:
            start = 0
            while True:
                response = safe_request(f"{BASE_URL_V1}/{endpoint}", {**params, 'start': start, 'limit': limit})
                items = response.get('data', [])
                if not items: break
                
                all_data.extend(items)
                start += len(items)
                print(f"  {endpoint}: {len(all_data)} registros")
                
                if not response.get('additional_data', {}).get('pagination', {}).get('more_items_in_collection', False):
                    break
        else:
            cursor = None
            while True:
                request_params = {**params, 'limit': limit}
                if cursor: request_params['cursor'] = cursor
                
                response = safe_request(f"{BASE_URL_V2}/{endpoint}", request_params)
                items = response.get('data', [])
                if not items: break
                
                all_data.extend(items)
                print(f"  {endpoint}: {len(all_data)} registros")
                
                cursor = response.get('additional_data', {}).get('next_cursor')
                if not cursor: break
        
        return all_data

    def fetch_deal_products(deal_ids: List[int]) -> List[Dict]:
        """Busca produtos vinculados aos negócios"""
        all_products = []
        
        for i in range(0, len(deal_ids), 100):
            chunk = deal_ids[i:i + 100]
            products = safe_request(f"{BASE_URL_V2}/deals/products", {'deal_ids': ','.join(map(str, chunk))})
            if 'data' in products:
                all_products.extend(products['data'])
        
        return all_products

    def process_entity(items: List[Dict], entity_type: str, field_mapping: Dict, dropdown_mapping: Dict) -> List[Dict]:
        """Processa entidades"""
        processed_items = []
        
        # Mapeamentos padrão
        std_value_maps = {
            'status': {'open': 'aberto', 'won': 'ganho', 'lost': 'perdido', 'deleted': 'excluido'},
            'visible_to': {'1': 'proprietario_do_item', '3': 'todos_os_usuarios'},
            'is_archived': {'False': 'nao_arquivado', 'True': 'arquivado'}
        }
        relation_fields = {'creator_user_id': 'criador', 'user_id': 'proprietario'}
        all_values = dropdown_mapping.get('_all_values', {})
        
        for item in items:
            processed = item.copy()
            custom_fields = {}
            
            # Remover campos duplicados
            for field in ['person_id', 'org_id', 'person_name', 'org_name', 'pessoa_de_contato', 'organizacao']:
                if field in processed: del processed[field]
        
            # Processar campos personalizados
            if "custom_fields" in processed and isinstance(processed["custom_fields"], dict):
                for field_id, value in processed["custom_fields"].items():
                    field_name = field_mapping.get(field_id, field_id)
                    
                    # Mapear valores de dropdown
                    if field_id in dropdown_mapping and value is not None:
                        value_map = dropdown_mapping[field_id]
                        if isinstance(value, list):
                            value = ','.join([value_map.get(str(v), str(v)) for v in value]) or ''
                        else:
                            value = value_map.get(str(value), all_values.get(str(value), value))
                    
                    custom_fields[field_name] = value
                del processed["custom_fields"]
            
            # Processar campos relacionais
            for old_field, new_field in relation_fields.items():
                if old_field in processed and processed[old_field] and isinstance(processed[old_field], dict):
                    processed[new_field] = processed[old_field].get('name', '')
                    del processed[old_field]
            
            # Mapear valores padrão
            for field, value_map in std_value_maps.items():
                if field in processed and processed[field] is not None:
                    processed[field] = value_map.get(str(processed[field]), processed[field])
            
            # Mapear outros valores conhecidos
            for field, value in list(processed.items()):
                if isinstance(value, str) and value in all_values:
                    processed[field] = all_values[value]
            
            # Adicionar campos personalizados
            processed.update(custom_fields)
            processed_items.append(processed)
        
        return processed_items

    def apply_mappings(df: pd.DataFrame, field_mapping: Dict, dropdown_mapping: Dict) -> pd.DataFrame:
        """Aplica mapeamentos ao DataFrame"""
        if df.empty: return df
        result_df = df.copy()
        
        # Renomear colunas
        rename_cols = {}
        renamed = set()
        
        for col in result_df.columns:
            if col in field_mapping:
                new_name = field_mapping[col]
                if new_name in renamed:
                    new_name = f"{new_name}_alt_{col[:6]}"
                rename_cols[col] = new_name
                renamed.add(new_name)
        
        if rename_cols:
            result_df.rename(columns=rename_cols, inplace=True)
        
        # Mapear valores de dropdown
        all_values = dropdown_mapping.get('_all_values', {})
        for col in result_df.columns:
            if pd.api.types.is_object_dtype(result_df[col]):
                result_df[col] = result_df[col].astype(str)
                
                # Mapear com dicionário específico da coluna
                if col in dropdown_mapping and dropdown_mapping[col]:
                    result_df[col] = result_df[col].map(
                        lambda x: dropdown_mapping[col].get(x, x) if pd.notna(x) and x != 'nan' else x
                    )
                
                # Mapear com dicionário global de valores
                result_df[col] = result_df[col].map(
                    lambda x: all_values.get(x, x) if pd.notna(x) and x != 'nan' else x
                )
        
        return result_df

    def fix_hash_columns(df: pd.DataFrame, field_mapping: Dict) -> pd.DataFrame:
        """Renomeia colunas com hash"""
        hash_cols = [col for col in df.columns if re.match(r'^[0-9a-f]{40}$', col)]
        
        if hash_cols:
            cols_to_rename = {}
            existing = set(df.columns)
            
            for col in hash_cols:
                if col in field_mapping:
                    new_name = field_mapping[col]
                    if new_name in existing and new_name != col:
                        new_name = f"{new_name}_custom_{col[:6]}"
                    cols_to_rename[col] = new_name
                else:
                    cols_to_rename[col] = f"campo_{col[:6]}"
            
            if cols_to_rename:
                df.rename(columns=cols_to_rename, inplace=True)
        
        return df

    def enrich_deals(deals_df: pd.DataFrame, pipelines_df: pd.DataFrame, 
                    stages_df: pd.DataFrame, products_df: pd.DataFrame) -> pd.DataFrame:
        """Enriquece dados de negócios"""
        enriched = deals_df.copy()
        
        # Verificar stages
        debug_stages_data(stages_df, enriched)
        
        # Join com pipelines
        if 'pipeline_id' in enriched.columns and not pipelines_df.empty:
            try:
                enriched = enriched.merge(
                    pipelines_df[['id', 'name', 'order_nr']], 
                    left_on='pipeline_id', right_on='id',
                    how='left', suffixes=('', '_pipeline')
                )
                enriched.rename(columns={'name': 'pipeline_name', 'order_nr': 'pipeline_order'}, inplace=True)
                if 'id_pipeline' in enriched.columns:
                    enriched.drop(['id_pipeline'], axis=1, inplace=True)
            except Exception as e:
                print(f"Erro ao juntar pipelines: {e}")
        
        # Mapear stage_id para nomes de etapas
        if 'stage_id' in enriched.columns and not stages_df.empty:
            try:
                # Criar dicionário de mapeamento
                stages_dict = dict(zip(stages_df['id'].astype(str), stages_df['name']))
                
                # Mapear diretamente
                enriched['etapa'] = enriched['stage_id'].astype(str).map(stages_dict)
                
                # Mapear order_nr
                order_dict = dict(zip(stages_df['id'].astype(str), stages_df['order_nr']))
                enriched['stage_order'] = enriched['stage_id'].astype(str).map(order_dict)
                
                # Usar ID como fallback
                enriched['etapa'] = enriched['etapa'].fillna(enriched['stage_id'].astype(str))
                
            except Exception as e:
                print(f"Erro ao mapear stages: {e}")
        
        # Adicionar produtos aos deals
        if not products_df.empty and 'deal_id' in products_df.columns:
            try:
                prod_by_deal = products_df.groupby('deal_id')
                for i, deal in enriched.iterrows():
                    deal_id = deal['id']
                    if deal_id in prod_by_deal.groups:
                        prods = prod_by_deal.get_group(deal_id).to_dict('records')
                        for j, prod in enumerate(prods[:5], 1):
                            enriched.at[i, f'product_name_{j}'] = prod.get('name', '')
                            enriched.at[i, f'product_quantity_{j}'] = prod.get('quantity', 0)
                            enriched.at[i, f'product_price_{j}'] = prod.get('item_price', 0)
                            enriched.at[i, f'product_id_{j}'] = prod.get('product_id', '')
            except Exception as e:
                print(f"Erro ao adicionar produtos: {e}")
        
        return enriched

    def main():
        print("\n===== INICIANDO INTEGRAÇÃO PIPEDRIVE =====")
        
        # 1. Obter e salvar mapeamentos
        field_mappings, dropdown_mappings = fetch_all_mappings()
        save_mappings(field_mappings, dropdown_mappings)
        
        # 2. Processar pipelines e stages
        pipelines_data = fetch_paginated_data('pipelines')
        pipelines_df = pd.DataFrame(process_entity(pipelines_data, 'pipeline', {}, {}))
        upload_to_storage(pipelines_df, FOLDERS['pipelines'], 'pipelines.csv')
        
        stages_data = fetch_paginated_data('stages')
        stages_df = pd.DataFrame(process_entity(stages_data, 'stage', {}, {}))
        upload_to_storage(stages_df, FOLDERS['stages'], 'stages.csv')
        
        # 3. Processar deals
        print("\nProcessando deals e produtos...")
        deals_data = fetch_paginated_data('deals')
        deals_df = pd.DataFrame(process_entity(
            deals_data, 'deal', 
            field_mappings.get('deal', {}), 
            dropdown_mappings.get('deal', {})
        ))
        
        # Aplicar mapeamentos e normalizar
        deals_df = apply_mappings(deals_df, field_mappings.get('deal', {}), dropdown_mappings.get('deal', {}))
        deals_df = fix_hash_columns(deals_df, field_mappings.get('deal', {}))
        deals_df = normalize_df(deals_df)
        upload_to_storage(deals_df, FOLDERS['deals'], 'deals.csv')
        
        # 4. Processar produtos
        products_data = []
        if 'products_count' in deals_df.columns:
            deals_with_products = deals_df[deals_df['products_count'] > 0]
            if not deals_with_products.empty:
                products_data = fetch_deal_products(deals_with_products['id'].tolist())
        
        products_df = pd.DataFrame(process_entity(
            products_data, 'product',
            field_mappings.get('product', {}),
            dropdown_mappings.get('product', {})
        ))
        
        # Aplicar mapeamentos e normalizar
        products_df = apply_mappings(products_df, field_mappings.get('product', {}), dropdown_mappings.get('product', {}))
        products_df = fix_hash_columns(products_df, field_mappings.get('product', {}))
        products_df = normalize_df(products_df)
        upload_to_storage(products_df, FOLDERS['products'], 'products.csv')
        
        # 5. Verificar e corrigir stage_id antes de enriquecer deals
        deals_df = diagnose_and_fix_stage_id(deals_df, stages_df)
        
        # 6. Enriquecer deals
        enriched_deals_df = enrich_deals(deals_df, pipelines_df, stages_df, products_df)
        
        # Aplicar mapeamentos finais e normalizar
        enriched_deals_df = apply_mappings(enriched_deals_df, field_mappings.get('deal', {}), dropdown_mappings.get('deal', {}))
        enriched_deals_df = fix_hash_columns(enriched_deals_df, field_mappings.get('deal', {}))
        enriched_deals_df = normalize_df(enriched_deals_df)
        upload_to_storage(enriched_deals_df, FOLDERS['deals_dados_finais'], 'enriched_deals.csv')
        
        print("\n===== INTEGRAÇÃO PIPEDRIVE CONCLUÍDA =====")

    main()

def get_extraction_tasks():
    """
    Get the list of data extraction tasks for Pipedrive.
    
    Returns:
        list: List of task configurations
    """
    return [
        {
            'task_id': 'run_pipedrive_integration',
            'python_callable': run
        }
    ]
