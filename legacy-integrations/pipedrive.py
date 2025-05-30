# # """
# # Pipedrive module for data extraction functions.
# # This module contains functions specific to the Pipedrive integration.
# # """
# #
# # from core import gcs
# #
# #
# # def run(customer):
# #     import concurrent.futures
# #     from datetime import datetime, timedelta
# #     from typing import Dict, List, Tuple
# #
# #     import os
# #     import pandas as pd
# #     import re
# #     import requests
# #     import time
# #     import unicodedata
# #     from google.cloud import storage
# #     import pathlib
# #
# #     # Configurações
# #     SERVICE_ACCOUNT_FILE = pathlib.Path('config', 'setup_automatico.json').as_posix()
# #     PROJECT_ID, BUCKET_NAME = customer['project_id'], customer['bucket_name']
# #     API_TOKEN = customer['api_token']
# #     COMPANY_DOMAIN = customer['company_domain']
# #     BASE_URL_V1 = customer['base_url_v1'].lower().format(company_domain=COMPANY_DOMAIN)
# #     BASE_URL_V2 = customer['base_url_v2'].lower().format(company_domain=COMPANY_DOMAIN)
# #     FOLDERS = {'deals': 'deals', 'deals_dados_finais': 'deals_dados_finais', 'pipelines': 'pipelines',
# #                'products': 'products', 'stages': 'stages', 'fields_mapping': 'fields_mapping',
# #                'activities': 'activities'}
# #
# #     # Iniciar cliente GCS
# #     os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_FILE
# #     storage_client = storage.Client.from_service_account_json(SERVICE_ACCOUNT_FILE, project=PROJECT_ID)
# #
# #     # Configs de paralelismo
# #     MAX_WORKERS = 8
# #     CHUNK_SIZE = 200
# #
# #     # Funções utilitárias
# #     def normalize_text(text: str) -> str:
# #         if not isinstance(text, str): text = str(text)
# #         return re.sub(r'_+', '_', re.sub(r'[^a-z0-9_]', '_',
# #                                          unicodedata.normalize('NFKD', text.lower()).encode('ASCII', 'ignore').decode(
# #                                              'ASCII'))).strip('_')
# #
# #     def normalize_column_name(col_name: str) -> str:
# #         return normalize_text(col_name)
# #
# #     def safe_request(url: str, params: Dict = None, method: str = 'get', retries: int = 3) -> Dict:
# #         if params is None: params = {}
# #         params['api_token'] = API_TOKEN
# #
# #         for attempt in range(retries):
# #             try:
# #                 if method.lower() == 'get':
# #                     response = requests.get(url, params=params)
# #                 else:
# #                     response = requests.post(url, params=params)
# #                 response.raise_for_status()
# #                 return response.json()
# #             except Exception as e:
# #                 if attempt < retries - 1:
# #                     time.sleep(2 * (attempt + 1))
# #                 else:
# #                     return {"data": []}
# #
# #     def upload_to_storage(dataframe: pd.DataFrame, folder: str, filename: str) -> bool:
# #         if dataframe.empty: return False
# #
# #         try:
# #             # Normalização de colunas (mantido como estava)
# #             dataframe.columns = [normalize_column_name(col) for col in dataframe.columns]
# #             dataframe = dataframe.loc[:, ~dataframe.columns.duplicated()]
# #
# #             # Limpar strings "None" para valores None reais do Python em TODAS as colunas
# #             dataframe = dataframe.replace('None', None)
# #
# #             # O restante permanece igual
# #             bucket = storage_client.bucket(BUCKET_NAME)
# #             blob = bucket.blob(f"{folder}/{filename}")
# #
# #             # Adicionar parâmetro na_rep='' para garantir que os None sejam salvos como células vazias no CSV
# #             csv_data = dataframe.to_csv(index=False, sep=';', encoding='utf-8-sig', na_rep='')
# #
# #             blob.upload_from_string(csv_data, content_type="text/csv")
# #             print(f"Upload: {len(dataframe)} registros em {folder}/{filename}")
# #             return True
# #         except Exception as e:
# #             print(f"Erro no upload de {filename}: {e}")
# #             raise
# #
# #     def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
# #         if df.empty: return df
# #         df.columns = [normalize_column_name(col) for col in df.columns]
# #         return df.loc[:, ~df.columns.duplicated()]
# #
# #     # Funções de diagnóstico
# #     def debug_stages_data(stages_df, deals_df):
# #         if 'stage_id' not in deals_df.columns:
# #             print("ERRO: Coluna 'stage_id' não encontrada em deals_df!")
# #             return
# #
# #         stage_ids_in_deals = deals_df['stage_id'].astype(str).unique()
# #         stage_ids_in_stages = stages_df['id'].astype(str).unique()
# #
# #         missing_stages = [sid for sid in stage_ids_in_deals if sid not in stage_ids_in_stages]
# #         if missing_stages:
# #             print(f"ALERTA: {len(missing_stages)} IDs de stages em deals não estão em stages_df!")
# #
# #     def diagnose_and_fix_stage_id(deals_df, stages_df):
# #         stage_columns = ['stage_id', 'etapa', 'stage', 'pipeline_stage_id']
# #         existing_stage_columns = [col for col in stage_columns if col in deals_df.columns]
# #
# #         if existing_stage_columns:
# #             stage_column = existing_stage_columns[0]
# #             if stage_column != 'stage_id':
# #                 deals_df['stage_id'] = deals_df[stage_column]
# #         else:
# #             numeric_cols = [col for col in deals_df.columns
# #                             if deals_df[col].dtype in ['int64', 'float64']
# #                             or (pd.api.types.is_object_dtype(deals_df[col])
# #                                 and deals_df[col].astype(str).str.isdigit().any())]
# #
# #             stage_ids = set(str(x) for x in stages_df['id'])
# #
# #             for col in numeric_cols:
# #                 unique_values = set(str(x) for x in deals_df[col].unique() if pd.notna(x))
# #                 match_count = len(unique_values.intersection(stage_ids))
# #
# #                 if match_count > 0 and (match_count >= 3 or match_count / len(unique_values) > 0.2):
# #                     deals_df['stage_id'] = deals_df[col]
# #                     break
# #
# #         return deals_df
# #
# #     # Funções de mapeamento e busca
# #     def fetch_all_mappings() -> Tuple[Dict, Dict]:
# #         field_types = {'deal': 'dealFields', 'product': 'productFields', 'activity': 'activityFields'}
# #         field_mappings, dropdown_mappings = {}, {}
# #         all_value_mappings, standard_fields = {}, set()
# #
# #         # Identificar campos padrão
# #         for entity_type, endpoint in field_types.items():
# #             for field in safe_request(f"{BASE_URL_V1}/{endpoint}").get('data', []):
# #                 if field.get('edit_flag', False) == False:
# #                     standard_fields.add(normalize_column_name(field.get('name', '')))
# #
# #         # Processar campos e valores
# #         for entity_type, endpoint in field_types.items():
# #             entity_field_map, entity_dropdown_map = {}, {}
# #
# #             for field in safe_request(f"{BASE_URL_V1}/{endpoint}").get('data', []):
# #                 field_id, field_key = str(field.get('id', '')), field.get('key', '')
# #                 if not field_key: continue
# #
# #                 name = normalize_column_name(field.get('name', ''))
# #
# #                 # Tratar colisão com campo padrão
# #                 if field.get('edit_flag', True) and name in standard_fields:
# #                     name = f"{name}_custom_{field_id[:8]}"
# #                     print(f"Colisão: '{field.get('name')}' -> '{name}'")
# #
# #                 # Mapear ID e key para nome normalizado
# #                 entity_field_map[field_key] = name
# #                 if field_id and field_id != field_key:
# #                     entity_field_map[field_id] = name
# #
# #                 # Mapear valores para campos de dropdown/enum/set
# #                 if field.get('field_type') in ['enum', 'set', 'status', 'varchar_options'] and field.get('options'):
# #                     option_map = {}
# #                     for option in field['options']:
# #                         option_id = str(option.get('id', ''))
# #                         if option_id:
# #                             option_map[option_id] = normalize_column_name(option.get('label', ''))
# #                             all_value_mappings[option_id] = option_map[option_id]
# #
# #                     if option_map:
# #                         entity_dropdown_map[field_key] = option_map
# #                         if field_id and field_id != field_key:
# #                             entity_dropdown_map[field_id] = option_map
# #
# #             field_mappings[entity_type] = entity_field_map
# #             dropdown_mappings[entity_type] = entity_dropdown_map
# #             print(f"Mapeados {len(entity_field_map)} campos para {entity_type}")
# #
# #         dropdown_mappings['_all_values'] = all_value_mappings
# #         return field_mappings, dropdown_mappings
# #
# #     def save_mappings(field_mappings: Dict, dropdown_mappings: Dict) -> None:
# #         fields_rows, dropdown_rows = [], []
# #
# #         # Preparar dados de field mappings
# #         for entity_type, mapping in field_mappings.items():
# #             for field_key, field_name in mapping.items():
# #                 fields_rows.append({'entity_type': entity_type, 'field_key': field_key, 'field_name': field_name})
# #
# #         # Preparar dados de dropdown mappings
# #         for entity_type, dropdown_maps in dropdown_mappings.items():
# #             if entity_type == '_all_values': continue
# #             for field_key, options in dropdown_maps.items():
# #                 for option_id, option_label in options.items():
# #                     dropdown_rows.append({
# #                         'entity_type': entity_type, 'field_key': field_key,
# #                         'option_id': option_id, 'option_label': option_label
# #                     })
# #
# #         # Upload dos dados
# #         if fields_rows:
# #             upload_to_storage(pd.DataFrame(fields_rows), FOLDERS['fields_mapping'], 'fields_mapping.csv')
# #         if dropdown_rows:
# #             upload_to_storage(pd.DataFrame(dropdown_rows), FOLDERS['fields_mapping'], 'dropdown_mapping.csv')
# #
# #     def fetch_paginated_data(endpoint: str, api_version: int = 1, params: Dict = None) -> List[Dict]:
# #         all_data, limit = [], 500
# #         params = params or {}
# #
# #         if api_version == 1:
# #             start = 0
# #             while True:
# #                 response = safe_request(f"{BASE_URL_V1}/{endpoint}", {**params, 'start': start, 'limit': limit})
# #                 items = response.get('data', [])
# #                 if not items: break
# #
# #                 all_data.extend(items)
# #                 start += len(items)
# #                 print(f"  {endpoint}: {len(all_data)} registros")
# #
# #                 if not response.get('additional_data', {}).get('pagination', {}).get('more_items_in_collection', False):
# #                     break
# #         else:
# #             cursor = None
# #             while True:
# #                 request_params = {**params, 'limit': limit}
# #                 if cursor: request_params['cursor'] = cursor
# #
# #                 response = safe_request(f"{BASE_URL_V2}/{endpoint}", request_params)
# #                 items = response.get('data', [])
# #                 if not items: break
# #
# #                 all_data.extend(items)
# #                 print(f"  {endpoint}: {len(all_data)} registros")
# #
# #                 cursor = response.get('additional_data', {}).get('next_cursor')
# #                 if not cursor: break
# #
# #         return all_data
# #
# #     def fetch_activities_by_date_range(start_date: str, end_date: str) -> List[Dict]:
# #         params = {
# #             'updated_since': start_date,
# #             'updated_until': end_date,
# #             'sort_by': 'update_time',
# #             'sort_direction': 'asc',
# #             'limit': 500
# #         }
# #
# #         cursor, activities = None, []
# #
# #         while True:
# #             if cursor: params['cursor'] = cursor
# #             response = safe_request(f"{BASE_URL_V2}/activities", params)
# #             items = response.get('data', [])
# #             if not items: break
# #
# #             activities.extend(items)
# #             cursor = response.get('additional_data', {}).get('next_cursor')
# #             if not cursor: break
# #
# #         return activities
# #
# #     def generate_date_ranges(months_back: int = 12) -> List[Tuple[str, str]]:
# #         now = datetime.now()
# #         return [(
# #             (now - timedelta(days=30 * (i + 1))).strftime('%Y-%m-%dT00:00:00Z'),
# #             (now - timedelta(days=30 * i)).strftime('%Y-%m-%dT23:59:59Z')
# #         ) for i in range(months_back)]
# #
# #     def fetch_deal_products(deal_ids: List[int]) -> List[Dict]:
# #         chunks = [deal_ids[i:i + CHUNK_SIZE] for i in range(0, len(deal_ids), CHUNK_SIZE)]
# #
# #         def fetch_chunk(chunk):
# #             products = safe_request(f"{BASE_URL_V2}/deals/products", {'deal_ids': ','.join(map(str, chunk))})
# #             return products.get('data', [])
# #
# #         all_products = []
# #         with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
# #             results = list(executor.map(fetch_chunk, chunks))
# #
# #         for result in results:
# #             all_products.extend(result)
# #
# #         return all_products
# #
# #     # Funções de processamento
# #     def process_entity(items: List[Dict], entity_type: str, field_mapping: Dict, dropdown_mapping: Dict) -> List[Dict]:
# #         processed_items = []
# #
# #         # Mapeamentos padrão
# #         std_value_maps = {
# #             'status': {'open': 'aberto', 'won': 'ganho', 'lost': 'perdido', 'deleted': 'excluido'},
# #             'visible_to': {'1': 'proprietario_do_item', '3': 'todos_os_usuarios'},
# #             'is_archived': {'False': 'nao_arquivado', 'True': 'arquivado'},
# #             'done': {'False': 'nao_concluido', 'True': 'concluido'},
# #             'busy': {'False': 'nao_ocupado', 'True': 'ocupado'}
# #         }
# #         relation_fields = {'creator_user_id': 'criador', 'user_id': 'proprietario', 'owner_id': 'proprietario'}
# #         all_values = dropdown_mapping.get('_all_values', {})
# #
# #         for item in items:
# #             processed = item.copy()
# #             custom_fields = {}
# #
# #             # Remover campos duplicados
# #             for field in ['person_id', 'org_id', 'person_name', 'org_name', 'pessoa_de_contato', 'organizacao']:
# #                 if field in processed: processed.pop(field, None)
# #
# #             # Processar campos personalizados
# #             if "custom_fields" in processed and isinstance(processed["custom_fields"], dict):
# #                 for field_id, value in processed["custom_fields"].items():
# #                     field_name = field_mapping.get(field_id, field_id)
# #
# #                     # Mapear valores de dropdown
# #                     if field_id in dropdown_mapping and value is not None:
# #                         value_map = dropdown_mapping[field_id]
# #                         if isinstance(value, list):
# #                             value = ','.join([value_map.get(str(v), str(v)) for v in value]) or ''
# #                         else:
# #                             value = value_map.get(str(value), all_values.get(str(value), value))
# #
# #                     custom_fields[field_name] = value
# #                 processed.pop("custom_fields", None)
# #
# #             # Processar campos relacionais
# #             for old_field, new_field in relation_fields.items():
# #                 if old_field in processed and processed[old_field] and isinstance(processed[old_field], dict):
# #                     processed[new_field] = processed[old_field].get('name', '')
# #                     processed.pop(old_field, None)
# #
# #             # Mapear valores padrão
# #             for field, value_map in std_value_maps.items():
# #                 if field in processed and processed[field] is not None:
# #                     processed[field] = value_map.get(str(processed[field]), processed[field])
# #
# #             # Mapear outros valores conhecidos
# #             for field, value in list(processed.items()):
# #                 if isinstance(value, str) and value in all_values:
# #                     processed[field] = all_values[value]
# #
# #             # Processar campos aninhados para activities
# #             if entity_type == 'activity':
# #                 # Processar location
# #                 if 'location' in processed and isinstance(processed['location'], dict):
# #                     for loc_key, loc_value in processed['location'].items():
# #                         processed[f'location_{loc_key}'] = loc_value
# #                     processed.pop('location', None)
# #
# #                 # Processar participants e attendees
# #                 for field_name, prefix in [('participants', 'participant_'), ('attendees', 'attendee_')]:
# #                     if field_name in processed and isinstance(processed[field_name], list):
# #                         for i, item in enumerate(processed[field_name][:5], 1):
# #                             for k, v in item.items():
# #                                 processed[f'{prefix}{k}_{i}'] = v
# #                         processed.pop(field_name, None)
# #
# #             # Adicionar campos personalizados
# #             processed.update(custom_fields)
# #             processed_items.append(processed)
# #
# #         return processed_items
# #
# #     def process_entity_chunk(chunk_data: Tuple[List[Dict], str, Dict, Dict]) -> List[Dict]:
# #         items, entity_type, field_mapping, dropdown_mapping = chunk_data
# #         return process_entity(items, entity_type, field_mapping, dropdown_mapping)
# #
# #     def apply_mappings(df: pd.DataFrame, field_mapping: Dict, dropdown_mapping: Dict) -> pd.DataFrame:
# #         if df.empty: return df
# #         result_df = df.copy()
# #
# #         # Renomear colunas
# #         rename_cols = {}
# #         renamed = set()
# #
# #         for col in result_df.columns:
# #             if col in field_mapping:
# #                 new_name = field_mapping[col]
# #                 if new_name in renamed:
# #                     new_name = f"{new_name}_alt_{col[:6]}"
# #                 rename_cols[col] = new_name
# #                 renamed.add(new_name)
# #
# #         if rename_cols:
# #             result_df.rename(columns=rename_cols, inplace=True)
# #
# #         # Mapear valores de dropdown
# #         all_values = dropdown_mapping.get('_all_values', {})
# #         for col in result_df.columns:
# #             if pd.api.types.is_object_dtype(result_df[col]):
# #                 result_df[col] = result_df[col].astype(str)
# #
# #                 # Mapear com dicionário específico da coluna e global de valores
# #                 if col in dropdown_mapping and dropdown_mapping[col]:
# #                     result_df[col] = result_df[col].map(
# #                         lambda x: dropdown_mapping[col].get(x, all_values.get(x, x))
# #                         if pd.notna(x) and x != 'nan' else x
# #                     )
# #
# #         return result_df
# #
# #     def fix_hash_columns(df: pd.DataFrame, field_mapping: Dict) -> pd.DataFrame:
# #         hash_cols = [col for col in df.columns if re.match(r'^[0-9a-f]{40}$', col)]
# #
# #         if hash_cols:
# #             cols_to_rename = {}
# #             existing = set(df.columns)
# #
# #             for col in hash_cols:
# #                 if col in field_mapping:
# #                     new_name = field_mapping[col]
# #                     if new_name in existing and new_name != col:
# #                         new_name = f"{new_name}_custom_{col[:6]}"
# #                     cols_to_rename[col] = new_name
# #                 else:
# #                     cols_to_rename[col] = f"campo_{col[:6]}"
# #
# #             if cols_to_rename:
# #                 df.rename(columns=cols_to_rename, inplace=True)
# #
# #         return df
# #
# #     def fix_channel_fields(df: pd.DataFrame) -> pd.DataFrame:
# #         if df.empty: return df
# #
# #         # Funções auxiliares para verificar tipo de dados
# #         def is_likely_id(value):
# #             if pd.isna(value): return False
# #             if isinstance(value, (int, float)): return True
# #             if isinstance(value, str): return bool(re.match(r'^\d+\.?\d*$', value))
# #             return False
# #
# #         def is_likely_text(value):
# #             if pd.isna(value): return False
# #             if isinstance(value, str) and not re.match(r'^\d+\.?\d*$', value): return True
# #             return False
# #
# #         # Verificar e corrigir pares invertidos
# #         for name_col, id_col in [('canal_de_origem', 'id_do_canal_de_origem'),
# #                                  ('origem', 'id_origem'), ('canal', 'canal_id')]:
# #             if name_col in df.columns and id_col in df.columns:
# #                 try:
# #                     name_col_numeric_ratio = df[name_col].apply(is_likely_id).mean()
# #                     id_col_text_ratio = df[id_col].apply(is_likely_text).mean()
# #
# #                     if name_col_numeric_ratio > 0.5 and id_col_text_ratio > 0.5:
# #                         print(f"Detectada inversão entre {name_col} e {id_col}. Corrigindo...")
# #                         df[name_col], df[id_col] = df[id_col].copy(), df[name_col].copy()
# #                 except Exception as e:
# #                     print(f"Erro ao verificar inversão de {name_col}/{id_col}: {e}")
# #
# #         return df
# #
# #     def enrich_deals(deals_df: pd.DataFrame, pipelines_df: pd.DataFrame,
# #                      stages_df: pd.DataFrame, products_df: pd.DataFrame) -> pd.DataFrame:
# #         enriched = deals_df.copy()
# #
# #         # Verificar stages
# #         debug_stages_data(stages_df, enriched)
# #
# #         # Join com pipelines
# #         if 'pipeline_id' in enriched.columns and not pipelines_df.empty:
# #             try:
# #                 enriched = enriched.merge(
# #                     pipelines_df[['id', 'name', 'order_nr']],
# #                     left_on='pipeline_id', right_on='id',
# #                     how='left', suffixes=('', '_pipeline')
# #                 )
# #                 enriched.rename(columns={'name': 'pipeline_name', 'order_nr': 'pipeline_order'}, inplace=True)
# #                 if 'id_pipeline' in enriched.columns:
# #                     enriched.drop(['id_pipeline'], axis=1, inplace=True)
# #             except Exception as e:
# #                 print(f"Erro ao juntar pipelines: {e}")
# #
# #         # Mapear stage_id para nomes de etapas
# #         if 'stage_id' in enriched.columns and not stages_df.empty:
# #             try:
# #                 # Criar dicionários de mapeamento
# #                 stages_dict = dict(zip(stages_df['id'].astype(str), stages_df['name']))
# #                 order_dict = dict(zip(stages_df['id'].astype(str), stages_df['order_nr']))
# #
# #                 # Aplicar mapeamentos
# #                 enriched['etapa'] = enriched['stage_id'].astype(str).map(stages_dict)
# #                 enriched['stage_order'] = enriched['stage_id'].astype(str).map(order_dict)
# #                 enriched['etapa'] = enriched['etapa'].fillna(enriched['stage_id'].astype(str))
# #             except Exception as e:
# #                 print(f"Erro ao mapear stages: {e}")
# #
# #         # Adicionar produtos aos deals
# #         if not products_df.empty and 'deal_id' in products_df.columns:
# #             try:
# #                 prod_by_deal = products_df.groupby('deal_id')
# #                 for i, deal in enriched.iterrows():
# #                     deal_id = deal['id']
# #                     if deal_id in prod_by_deal.groups:
# #                         prods = prod_by_deal.get_group(deal_id).to_dict('records')
# #                         for j, prod in enumerate(prods[:5], 1):
# #                             enriched.at[i, f'product_name_{j}'] = prod.get('name', '')
# #                             enriched.at[i, f'product_quantity_{j}'] = prod.get('quantity', 0)
# #                             enriched.at[i, f'product_price_{j}'] = prod.get('item_price', 0)
# #                             enriched.at[i, f'product_id_{j}'] = prod.get('product_id', '')
# #             except Exception as e:
# #                 print(f"Erro ao adicionar produtos: {e}")
# #
# #         # Corrigir inversão entre canal_de_origem e id_do_canal_de_origem
# #         return fix_channel_fields(enriched)
# #
# #     def fetch_activities_parallel() -> pd.DataFrame:
# #         print("\nBuscando atividades em paralelo...")
# #         start_time = time.time()
# #
# #         # Gerar intervalos de datas e buscar em paralelo
# #         date_ranges = generate_date_ranges(months_back=24)
# #
# #         with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
# #             results = list(executor.map(
# #                 lambda date_range: fetch_activities_by_date_range(date_range[0], date_range[1]),
# #                 date_ranges
# #             ))
# #
# #         # Combinar resultados
# #         all_activities = []
# #         for result in results:
# #             all_activities.extend(result)
# #
# #         print(f"Recuperadas {len(all_activities)} atividades em {time.time() - start_time:.2f} segundos")
# #
# #         if not all_activities:
# #             print("Nenhuma atividade encontrada!")
# #             return pd.DataFrame()
# #
# #         # Processar atividades em chunks paralelos
# #         chunks = [all_activities[i:i + CHUNK_SIZE] for i in range(0, len(all_activities), CHUNK_SIZE)]
# #         chunk_data = [(chunk, 'activity', field_mappings.get('activity', {}), dropdown_mappings.get('activity', {}))
# #                       for chunk in chunks]
# #
# #         # Processar chunks em paralelo
# #         with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
# #             processed_chunks = list(executor.map(process_entity_chunk, chunk_data))
# #
# #         # Combinar resultados processados
# #         processed_activities = []
# #         for chunk in processed_chunks:
# #             processed_activities.extend(chunk)
# #
# #         print(f"Processadas {len(processed_activities)} atividades em {time.time() - start_time:.2f} segundos")
# #
# #         # Converter para DataFrame e aplicar mapeamentos
# #         activities_df = pd.DataFrame(processed_activities)
# #         activities_df = apply_mappings(activities_df, field_mappings.get('activity', {}),
# #                                        dropdown_mappings.get('activity', {}))
# #         activities_df = fix_hash_columns(activities_df, field_mappings.get('activity', {}))
# #         activities_df = normalize_df(activities_df)
# #         activities_df = fix_channel_fields(activities_df)
# #
# #         return activities_df
# #
# #     def process_entity_parallel(data: List[Dict], entity_type: str, field_mapping: Dict,
# #                                 dropdown_mapping: Dict) -> pd.DataFrame:
# #         """Processa uma lista de entidades em paralelo e retorna um DataFrame"""
# #         chunks = [data[i:i + CHUNK_SIZE] for i in range(0, len(data), CHUNK_SIZE)]
# #         chunk_data = [(chunk, entity_type, field_mapping, dropdown_mapping) for chunk in chunks]
# #
# #         processed_items = []
# #         with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
# #             results = list(executor.map(process_entity_chunk, chunk_data))
# #
# #         for result in results:
# #             processed_items.extend(result)
# #
# #         df = pd.DataFrame(processed_items)
# #         df = apply_mappings(df, field_mapping, dropdown_mapping)
# #         df = fix_hash_columns(df, field_mapping)
# #         df = normalize_df(df)
# #         df = fix_channel_fields(df)
# #
# #         return df
# #
# #     def main():
# #         print("\n===== INICIANDO INTEGRAÇÃO PIPEDRIVE =====")
# #
# #         # 1. Obter e salvar mapeamentos
# #         global field_mappings, dropdown_mappings
# #         field_mappings, dropdown_mappings = fetch_all_mappings()
# #         save_mappings(field_mappings, dropdown_mappings)
# #
# #         # 2. Processar pipelines e stages
# #         pipelines_data = fetch_paginated_data('pipelines')
# #         pipelines_df = pd.DataFrame(process_entity(pipelines_data, 'pipeline', {}, {}))
# #         upload_to_storage(pipelines_df, FOLDERS['pipelines'], 'pipelines.csv')
# #
# #         stages_data = fetch_paginated_data('stages')
# #         stages_df = pd.DataFrame(process_entity(stages_data, 'stage', {}, {}))
# #         upload_to_storage(stages_df, FOLDERS['stages'], 'stages.csv')
# #
# #         # Iniciar busca de atividades em paralelo enquanto processamos outros dados
# #         activities_future = None
# #         with concurrent.futures.ThreadPoolExecutor() as executor:
# #             activities_future = executor.submit(fetch_activities_parallel)
# #
# #         # 3. Processar deals em paralelo
# #         print("\nProcessando deals e produtos...")
# #         deals_data = fetch_paginated_data('deals')
# #         deals_df = process_entity_parallel(deals_data, 'deal',
# #                                            field_mappings.get('deal', {}),
# #                                            dropdown_mappings.get('deal', {}))
# #         upload_to_storage(deals_df, FOLDERS['deals'], 'deals.csv')
# #
# #         # 4. Processar produtos em paralelo
# #         products_data = []
# #         if 'products_count' in deals_df.columns:
# #             deals_with_products = deals_df[deals_df['products_count'] > 0]
# #             if not deals_with_products.empty:
# #                 products_data = fetch_deal_products(deals_with_products['id'].tolist())
# #
# #         products_df = process_entity_parallel(products_data, 'product',
# #                                               field_mappings.get('product', {}),
# #                                               dropdown_mappings.get('product', {}))
# #         upload_to_storage(products_df, FOLDERS['products'], 'products.csv')
# #
# #         # 5. Verificar e corrigir stage_id antes de enriquecer deals
# #         deals_df = diagnose_and_fix_stage_id(deals_df, stages_df)
# #
# #         # 6. Enriquecer deals
# #         enriched_deals_df = enrich_deals(deals_df, pipelines_df, stages_df, products_df)
# #
# #         # Aplicar mapeamentos finais
# #         enriched_deals_df = apply_mappings(enriched_deals_df, field_mappings.get('deal', {}),
# #                                            dropdown_mappings.get('deal', {}))
# #         enriched_deals_df = fix_hash_columns(enriched_deals_df, field_mappings.get('deal', {}))
# #         enriched_deals_df = normalize_df(enriched_deals_df)
# #         enriched_deals_df = fix_channel_fields(enriched_deals_df)
# #
# #         upload_to_storage(enriched_deals_df, FOLDERS['deals_dados_finais'], 'enriched_deals.csv')
# #
# #         # 7. Obter resultados da busca de atividades em paralelo
# #         print("\nFinalizando processamento de atividades...")
# #
# #         # Aguardar a conclusão da busca paralela de atividades
# #         activities_df = activities_future.result()
# #
# #         if not activities_df.empty:
# #             # Salvar em chunks se necessário
# #             chunk_size = 100000
# #             if len(activities_df) > chunk_size:
# #                 for i in range(0, len(activities_df), chunk_size):
# #                     chunk_end = min(i + chunk_size, len(activities_df))
# #                     chunk_df = activities_df.iloc[i:chunk_end]
# #                     filename = f'activities_chunk_{i // chunk_size + 1}.csv'
# #                     upload_to_storage(chunk_df, FOLDERS['activities'], filename)
# #                     print(f"Salvo chunk {i // chunk_size + 1} de atividades")
# #             else:
# #                 upload_to_storage(activities_df, FOLDERS['activities'], 'activities.csv')
# #
# #         print("\n===== INTEGRAÇÃO PIPEDRIVE CONCLUÍDA =====")
# #
# #     # START
# #     main()
# #
# #
# # def run_persons(customer):
# #     import requests, pandas as pd, os, re, unicodedata, time
# #     from google.cloud import storage
# #     from typing import Dict, List, Tuple
# #     import concurrent.futures
# #     import pathlib
# #
# #     SERVICE_ACCOUNT_FILE = pathlib.Path('config', 'setup_automatico.json').as_posix()
# #     PROJECT_ID, BUCKET_NAME = customer['project_id'], customer['bucket_name']
# #     API_TOKEN = customer['api_token']
# #     COMPANY_DOMAIN = customer['company_domain']
# #     BASE_URL_V1 = customer['base_url_v1'].lower().format(company_domain=COMPANY_DOMAIN)
# #
# #     FOLDER = 'persons'
# #
# #     # Configs de paralelismo
# #     MAX_WORKERS = 8
# #     CHUNK_SIZE = 200
# #     BATCH_SIZE = 500  # Tamanho de cada lote para busca
# #
# #     # Variáveis globais para mapeamentos
# #     field_mappings = {}
# #     dropdown_mappings = {}
# #
# #     # Iniciar cliente GCS
# #     os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_FILE
# #     storage_client = storage.Client.from_service_account_json(SERVICE_ACCOUNT_FILE, project=PROJECT_ID)
# #
# #     # Funções utilitárias
# #     def normalize_text(text: str) -> str:
# #         if not isinstance(text, str): text = str(text)
# #         return re.sub(r'_+', '_', re.sub(r'[^a-z0-9_]', '_',
# #                                          unicodedata.normalize('NFKD', text.lower()).encode('ASCII', 'ignore').decode(
# #                                              'ASCII'))).strip('_')
# #
# #     def normalize_column_name(col_name: str) -> str:
# #         return normalize_text(col_name)
# #
# #     def safe_request(url: str, params: Dict = None, method: str = 'get', retries: int = 3) -> Dict:
# #         if params is None: params = {}
# #         params['api_token'] = API_TOKEN
# #
# #         for attempt in range(retries):
# #             try:
# #                 if method.lower() == 'get':
# #                     response = requests.get(url, params=params)
# #                 else:
# #                     response = requests.post(url, params=params)
# #                 response.raise_for_status()
# #                 return response.json()
# #             except Exception as e:
# #                 if attempt < retries - 1:
# #                     time.sleep(2 * (attempt + 1))
# #                 else:
# #                     print(f"Erro na requisição para {url}: {e}")
# #                     return {"data": []}
# #
# #     def upload_to_storage(dataframe: pd.DataFrame, filename: str) -> bool:
# #         if dataframe.empty: return False
# #
# #         try:
# #             dataframe.columns = [normalize_column_name(col) for col in dataframe.columns]
# #             dataframe = dataframe.loc[:, ~dataframe.columns.duplicated()]
# #
# #             bucket = storage_client.bucket(BUCKET_NAME)
# #             blob = bucket.blob(f"{FOLDER}/{filename}")
# #             csv_data = dataframe.to_csv(index=False, sep=';', encoding='utf-8-sig')
# #             blob.upload_from_string(csv_data, content_type="text/csv")
# #             print(f"Upload: {len(dataframe)} registros em {FOLDER}/{filename}")
# #             return True
# #         except Exception as e:
# #             print(f"Erro no upload de {filename}: {e}")
# #             raise
# #
# #     def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
# #         if df.empty: return df
# #         df.columns = [normalize_column_name(col) for col in df.columns]
# #         return df.loc[:, ~df.columns.duplicated()]
# #
# #     # Funções de mapeamento e busca para persons
# #     def fetch_person_fields() -> Tuple[Dict, Dict]:
# #         """Obtém mapeamentos dos campos de person do Pipedrive"""
# #         global field_mappings, dropdown_mappings
# #
# #         person_field_mapping = {}
# #         person_dropdown_mapping = {}
# #         all_value_mappings = {}
# #         standard_fields = set()
# #
# #         # Buscar campos
# #         for field in safe_request(f"{BASE_URL_V1}/personFields").get('data', []):
# #             field_id, field_key = str(field.get('id', '')), field.get('key', '')
# #             if not field_key: continue
# #
# #             name = normalize_column_name(field.get('name', ''))
# #
# #             # Tratar colisão com campo padrão
# #             if field.get('edit_flag', True) and name in standard_fields:
# #                 name = f"{name}_custom_{field_id[:8]}"
# #                 print(f"Colisão: '{field.get('name')}' -> '{name}'")
# #
# #             # Mapear ID e key para nome normalizado
# #             person_field_mapping[field_key] = name
# #             if field_id and field_id != field_key:
# #                 person_field_mapping[field_id] = name
# #
# #             # Mapear valores para campos de dropdown/enum/set
# #             if field.get('field_type') in ['enum', 'set', 'status', 'varchar_options'] and field.get('options'):
# #                 option_map = {}
# #                 for option in field['options']:
# #                     option_id = str(option.get('id', ''))
# #                     if option_id:
# #                         option_map[option_id] = normalize_column_name(option.get('label', ''))
# #                         all_value_mappings[option_id] = option_map[option_id]
# #
# #                 if option_map:
# #                     person_dropdown_mapping[field_key] = option_map
# #                     if field_id and field_id != field_key:
# #                         person_dropdown_mapping[field_id] = option_map
# #
# #         print(f"Mapeados {len(person_field_mapping)} campos para persons")
# #
# #         # Salvar em variáveis globais
# #         field_mappings = person_field_mapping
# #         person_dropdown_mapping['_all_values'] = all_value_mappings
# #         dropdown_mappings = person_dropdown_mapping
# #
# #         return person_field_mapping, person_dropdown_mapping
# #
# #     def fetch_persons_batch(start_offset: int) -> List[Dict]:
# #         """Busca um lote de pessoas a partir de um offset específico"""
# #         try:
# #             url = f"{BASE_URL_V1}/persons"
# #             params = {
# #                 'start': start_offset,
# #                 'limit': BATCH_SIZE
# #             }
# #
# #             response = safe_request(url, params)
# #             return response.get('data', [])
# #         except Exception as e:
# #             print(f"Erro ao buscar lote de pessoas (start={start_offset}): {e}")
# #             return []
# #
# #     def fetch_all_persons() -> List[Dict]:
# #         """Busca todas as pessoas usando paralelismo dinâmico"""
# #         # Primeiro, fazer uma única requisição para obter metadados de paginação
# #         url = f"{BASE_URL_V1}/persons"
# #         params = {'start': 0, 'limit': 1}
# #
# #         try:
# #             response = safe_request(url, params)
# #             pagination = response.get('additional_data', {}).get('pagination', {})
# #             total_count = pagination.get('total_count')
# #
# #             if not total_count:
# #                 # Se não conseguir obter contagem, iniciar com estimativa inicial e ajustar dinamicamente
# #                 print("Não foi possível determinar contagem total. Usando abordagem dinâmica.")
# #                 return fetch_persons_dynamic()
# #
# #             print(f"Total de persons: {total_count}")
# #
# #             # Calcular os offsets para cada thread
# #             offsets = list(range(0, total_count, BATCH_SIZE))
# #
# #             # Executar as buscas em paralelo
# #             all_persons = []
# #             with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
# #                 # Mapear a função fetch_persons_batch para cada offset em paralelo
# #                 results = list(executor.map(fetch_persons_batch, offsets))
# #
# #                 # Processar resultados
# #                 for i, batch in enumerate(results):
# #                     all_persons.extend(batch)
# #                     print(f"Recebido lote {i + 1}/{len(offsets)} - {len(batch)} registros (total: {len(all_persons)})")
# #
# #             return all_persons
# #
# #         except Exception as e:
# #             print(f"Erro na configuração de busca paralela: {e}. Usando abordagem dinâmica.")
# #             return fetch_persons_dynamic()
# #
# #     def fetch_persons_dynamic() -> List[Dict]:
# #         """Busca pessoas usando uma abordagem dinâmica sem conhecer o total antecipadamente"""
# #         all_persons = []
# #         offset = 0
# #         batch_size = BATCH_SIZE
# #         has_more = True
# #         batch_count = 0
# #
# #         # Criar um pool de workers
# #         with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
# #             # Enquanto houver mais registros a buscar
# #             while has_more:
# #                 batch_count += 1
# #                 futures = []
# #
# #                 # Distribuir múltiplos batches para processamento paralelo
# #                 for i in range(MAX_WORKERS):
# #                     current_offset = offset + (i * batch_size)
# #                     futures.append(executor.submit(fetch_persons_batch, current_offset))
# #
# #                 # Coletar resultados deste grupo de batches
# #                 new_batch_total = 0
# #                 for future in concurrent.futures.as_completed(futures):
# #                     batch_result = future.result()
# #                     if batch_result:
# #                         all_persons.extend(batch_result)
# #                         new_batch_total += len(batch_result)
# #
# #                 # Verificar se todos os batches estavam cheios (indicando que pode haver mais)
# #                 if new_batch_total < batch_size * MAX_WORKERS:
# #                     has_more = False
# #
# #                 print(f"Rodada {batch_count}: Recuperados {new_batch_total} registros (total: {len(all_persons)})")
# #
# #                 # Ajustar offset para o próximo grupo de batches
# #                 offset += batch_size * MAX_WORKERS
# #
# #                 # Limitador de segurança para evitar loops infinitos
# #                 if batch_count > 500:  # Um número muito alto que nunca seria atingido em circunstâncias normais
# #                     print("Limite de segurança atingido. Interrompendo busca.")
# #                     break
# #
# #         print(f"Total de {len(all_persons)} registros recuperados com abordagem dinâmica")
# #         return all_persons
# #
# #     def fetch_persons_sequential() -> List[Dict]:
# #         """Busca pessoas de forma sequencial (método alternativo)"""
# #         all_persons = []
# #         start = 0
# #
# #         while True:
# #             try:
# #                 url = f"{BASE_URL_V1}/persons"
# #                 params = {
# #                     'start': start,
# #                     'limit': BATCH_SIZE
# #                 }
# #
# #                 response = safe_request(url, params)
# #                 data = response.get('data', [])
# #
# #                 if not data:
# #                     break
# #
# #                 all_persons.extend(data)
# #                 start += len(data)
# #
# #                 print(f"Recuperados {len(all_persons)} persons (sequencial)...")
# #
# #                 # Verificar se há mais itens para buscar
# #                 if not response.get('additional_data', {}).get('pagination', {}).get('more_items_in_collection', False):
# #                     break
# #
# #             except Exception as e:
# #                 print(f"Erro ao buscar persons: {e}")
# #                 time.sleep(5)  # Esperar um pouco antes de tentar o próximo lote
# #                 # Avançar mesmo em caso de erro para evitar loop infinito
# #                 start += BATCH_SIZE
# #                 if start > 100000:  # Limite de segurança aumentado
# #                     break
# #
# #         return all_persons
# #
# #     def process_person(items: List[Dict]) -> List[Dict]:
# #         """Processa dados de pessoas do Pipedrive"""
# #         processed_items = []
# #
# #         # Mapeamentos padrão
# #         std_value_maps = {
# #             'visible_to': {'1': 'proprietario_do_item', '3': 'todos_os_usuarios', '7': 'toda_a_empresa'},
# #         }
# #         relation_fields = {'owner_id': 'proprietario'}
# #         all_values = dropdown_mappings.get('_all_values', {})
# #
# #         for item in items:
# #             processed = item.copy()
# #             custom_fields = {}
# #
# #             # Processar campos personalizados
# #             if "custom_fields" in processed and isinstance(processed["custom_fields"], dict):
# #                 for field_id, value in processed["custom_fields"].items():
# #                     field_name = field_mappings.get(field_id, field_id)
# #
# #                     # Mapear valores de dropdown
# #                     if field_id in dropdown_mappings and value is not None:
# #                         value_map = dropdown_mappings[field_id]
# #                         if isinstance(value, list):
# #                             value = ','.join([value_map.get(str(v), str(v)) for v in value]) or ''
# #                         else:
# #                             value = value_map.get(str(value), all_values.get(str(value), value))
# #
# #                     custom_fields[field_name] = value
# #                 processed.pop("custom_fields", None)
# #
# #             # Processar campos relacionais
# #             for old_field, new_field in relation_fields.items():
# #                 if old_field in processed and processed[old_field] and isinstance(processed[old_field], dict):
# #                     processed[new_field] = processed[old_field].get('name', '')
# #                     processed.pop(old_field, None)
# #
# #             # Mapear valores padrão
# #             for field, value_map in std_value_maps.items():
# #                 if field in processed and processed[field] is not None:
# #                     processed[field] = value_map.get(str(processed[field]), processed[field])
# #
# #             # Mapear outros valores conhecidos
# #             for field, value in list(processed.items()):
# #                 if isinstance(value, str) and value in all_values:
# #                     processed[field] = all_values[value]
# #
# #             # Processar emails
# #             if 'email' in processed and isinstance(processed['email'], list):
# #                 for i, email in enumerate(processed['email'][:5], 1):
# #                     for k, v in email.items():
# #                         processed[f'email_{k}_{i}'] = v
# #                 processed.pop('email', None)
# #
# #             # Processar phones
# #             if 'phone' in processed and isinstance(processed['phone'], list):
# #                 for i, phone in enumerate(processed['phone'][:5], 1):
# #                     for k, v in phone.items():
# #                         processed[f'phone_{k}_{i}'] = v
# #                 processed.pop('phone', None)
# #
# #             # Processar campos adicionais que podem existir em diferentes formatos
# #             special_fields = ['emails', 'phones', 'im', 'postal_address']
# #             for field in special_fields:
# #                 if field in processed and isinstance(processed[field], list):
# #                     prefix = field[:-1] if field.endswith('s') else field
# #                     for i, item in enumerate(processed[field][:5], 1):
# #                         if isinstance(item, dict):
# #                             for k, v in item.items():
# #                                 processed[f'{prefix}_{k}_{i}'] = v
# #                     processed.pop(field, None)
# #                 elif field in processed and isinstance(processed[field], dict):
# #                     for k, v in processed[field].items():
# #                         processed[f'{field}_{k}'] = v
# #                     processed.pop(field, None)
# #
# #             # Adicionar campos personalizados
# #             processed.update(custom_fields)
# #             processed_items.append(processed)
# #
# #         return processed_items
# #
# #     def process_chunk(items: List[Dict]) -> List[Dict]:
# #         """Processa um chunk de pessoas"""
# #         return process_person(items)
# #
# #     def fix_channel_fields(df: pd.DataFrame) -> pd.DataFrame:
# #         """Corrige inversões em campos de canais"""
# #         if df.empty: return df
# #
# #         # Funções auxiliares para verificar tipo de dados
# #         def is_likely_id(value):
# #             if pd.isna(value): return False
# #             if isinstance(value, (int, float)): return True
# #             if isinstance(value, str): return bool(re.match(r'^\d+\.?\d*$', value))
# #             return False
# #
# #         def is_likely_text(value):
# #             if pd.isna(value): return False
# #             if isinstance(value, str) and not re.match(r'^\d+\.?\d*$', value): return True
# #             return False
# #
# #         # Verificar e corrigir pares invertidos
# #         for name_col, id_col in [('canal_de_origem', 'id_do_canal_de_origem'),
# #                                  ('origem', 'id_origem'), ('canal', 'canal_id')]:
# #             if name_col in df.columns and id_col in df.columns:
# #                 try:
# #                     name_col_numeric_ratio = df[name_col].apply(is_likely_id).mean()
# #                     id_col_text_ratio = df[id_col].apply(is_likely_text).mean()
# #
# #                     if name_col_numeric_ratio > 0.5 and id_col_text_ratio > 0.5:
# #                         print(f"Detectada inversão entre {name_col} e {id_col}. Corrigindo...")
# #                         df[name_col], df[id_col] = df[id_col].copy(), df[name_col].copy()
# #                 except Exception as e:
# #                     print(f"Erro ao verificar inversão de {name_col}/{id_col}: {e}")
# #
# #         return df
# #
# #     def fix_hash_columns(df: pd.DataFrame) -> pd.DataFrame:
# #         """Renomeia colunas com hash"""
# #         hash_cols = [col for col in df.columns if re.match(r'^[0-9a-f]{40}$', col)]
# #
# #         if hash_cols:
# #             cols_to_rename = {}
# #             existing = set(df.columns)
# #
# #             for col in hash_cols:
# #                 if col in field_mappings:
# #                     new_name = field_mappings[col]
# #                     if new_name in existing and new_name != col:
# #                         new_name = f"{new_name}_custom_{col[:6]}"
# #                     cols_to_rename[col] = new_name
# #                 else:
# #                     cols_to_rename[col] = f"campo_{col[:6]}"
# #
# #             if cols_to_rename:
# #                 df.rename(columns=cols_to_rename, inplace=True)
# #
# #         return df
# #
# #     def apply_mappings(df: pd.DataFrame) -> pd.DataFrame:
# #         """Aplica mapeamentos ao DataFrame"""
# #         if df.empty: return df
# #         result_df = df.copy()
# #
# #         # Renomear colunas
# #         rename_cols = {}
# #         renamed = set()
# #
# #         for col in result_df.columns:
# #             if col in field_mappings:
# #                 new_name = field_mappings[col]
# #                 if new_name in renamed:
# #                     new_name = f"{new_name}_alt_{col[:6]}"
# #                 rename_cols[col] = new_name
# #                 renamed.add(new_name)
# #
# #         if rename_cols:
# #             result_df.rename(columns=rename_cols, inplace=True)
# #
# #         # Mapear valores de dropdown
# #         all_values = dropdown_mappings.get('_all_values', {})
# #         for col in result_df.columns:
# #             if pd.api.types.is_object_dtype(result_df[col]):
# #                 result_df[col] = result_df[col].astype(str)
# #
# #                 # Mapear com dicionário específico da coluna e global de valores
# #                 if col in dropdown_mappings and dropdown_mappings[col]:
# #                     result_df[col] = result_df[col].map(
# #                         lambda x: dropdown_mappings[col].get(x, all_values.get(x, x))
# #                         if pd.notna(x) and x != 'nan' else x
# #                     )
# #
# #         return result_df
# #
# #     def process_persons_parallel() -> pd.DataFrame:
# #         """Processa persons em paralelo e retorna um DataFrame"""
# #         global field_mappings, dropdown_mappings
# #
# #         print("\nBuscando pessoas...")
# #         start_time = time.time()
# #
# #         # Obter mapeamentos de campos
# #         field_mappings, dropdown_mappings = fetch_person_fields()
# #
# #         # Buscar pessoas com método dinâmico
# #         all_persons = fetch_all_persons()
# #
# #         print(f"Recuperadas {len(all_persons)} pessoas em {time.time() - start_time:.2f} segundos")
# #
# #         if not all_persons:
# #             print("Nenhuma pessoa encontrada!")
# #             return pd.DataFrame()
# #
# #         print("Processando dados de pessoas em paralelo...")
# #         # Processar em chunks paralelos
# #         chunks = [all_persons[i:i + CHUNK_SIZE] for i in range(0, len(all_persons), CHUNK_SIZE)]
# #
# #         # Processar chunks em paralelo
# #         with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
# #             processed_chunks = list(executor.map(process_chunk, chunks))
# #
# #         # Combinar resultados processados
# #         processed_persons = []
# #         for chunk in processed_chunks:
# #             processed_persons.extend(chunk)
# #
# #         print(f"Processadas {len(processed_persons)} pessoas em {time.time() - start_time:.2f} segundos")
# #
# #         # Converter para DataFrame e aplicar mapeamentos
# #         persons_df = pd.DataFrame(processed_persons)
# #
# #         print("Aplicando mapeamentos e normalizando dados...")
# #         persons_df = apply_mappings(persons_df)
# #         persons_df = fix_hash_columns(persons_df)
# #         persons_df = normalize_df(persons_df)
# #         persons_df = fix_channel_fields(persons_df)
# #
# #         return persons_df
# #
# #     def main():
# #         """Executa o processamento completo de persons"""
# #         try:
# #             start_time = time.time()
# #
# #             # Processar pessoas em paralelo
# #             persons_df = process_persons_parallel()
# #
# #             if not persons_df.empty:
# #                 print(f"Salvando {len(persons_df)} pessoas processadas...")
# #                 # Salvar em chunks se necessário
# #                 chunk_size = 100000
# #                 if len(persons_df) > chunk_size:
# #                     for i in range(0, len(persons_df), chunk_size):
# #                         chunk_end = min(i + chunk_size, len(persons_df))
# #                         chunk_df = persons_df.iloc[i:chunk_end]
# #                         filename = f'persons_chunk_{i // chunk_size + 1}.csv'
# #                         upload_to_storage(chunk_df, filename)
# #                         print(f"Salvo chunk {i // chunk_size + 1} de pessoas")
# #                 else:
# #                     upload_to_storage(persons_df, 'persons.csv')
# #
# #                 print(f"Processadas e salvas {len(persons_df)} pessoas no total")
# #
# #             print(f"\nProcessamento de pessoas concluído em {(time.time() - start_time) / 60:.2f} minutos")
# #             return persons_df
# #
# #         except Exception as e:
# #             print(f"\nErro durante o processamento de pessoas: {e}")
# #             import traceback
# #             traceback.print_exc()
# #             raise
# #             # return pd.DataFrame()
# #
# #     main()
# #
# #
# # def get_extraction_tasks():
# #     """
# #     Get the list of data extraction tasks for Pipedrive.
# #
# #     Returns:
# #         list: List of task configurations
# #     """
# #     return [
# #         {
# #             'task_id': 'run_pipedrive_integration',
# #             'python_callable': run
# #         },
# #         {
# #             'task_id': 'run_persons',
# #             'python_callable': run_persons
# #         }
# #     ]
# """
# Pipedrive module for data extraction functions.
# This module contains functions specific to the Pipedrive integration.
# Version: 2.0 - Fixed ID to Label mapping issues
# """
#
# from core import gcs
#
#
# def run(customer):
#     """
#     Main function to run Pipedrive integration
#     Extracts: Deals, Pipelines, Stages, Products, Activities, and Leads
#     """
#     import concurrent.futures
#     from datetime import datetime, timedelta
#     from typing import Dict, List, Tuple
#     import os
#     import pandas as pd
#     import re
#     import requests
#     import time
#     import unicodedata
#     from google.cloud import storage
#     import pathlib
#
#     # Configurações
#     SERVICE_ACCOUNT_FILE = pathlib.Path('config', 'setup_automatico.json').as_posix()
#     PROJECT_ID, BUCKET_NAME = customer['project_id'], customer['bucket_name']
#     API_TOKEN = customer['api_token']
#     COMPANY_DOMAIN = customer['company_domain']
#     BASE_URL_V1 = customer['base_url_v1'].lower().format(company_domain=COMPANY_DOMAIN)
#     BASE_URL_V2 = customer['base_url_v2'].lower().format(company_domain=COMPANY_DOMAIN)
#
#     FOLDERS = {
#         'deals': 'deals',
#         'deals_dados_finais': 'deals_dados_finais',
#         'pipelines': 'pipelines',
#         'products': 'products',
#         'stages': 'stages',
#         'fields_mapping': 'fields_mapping',
#         'activities': 'activities',
#         'leads': 'leads'
#     }
#
#     # Iniciar cliente GCS
#     os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_FILE
#     storage_client = storage.Client.from_service_account_json(SERVICE_ACCOUNT_FILE, project=PROJECT_ID)
#
#     # Configs de paralelismo
#     MAX_WORKERS = 8
#     CHUNK_SIZE = 200
#
#     # Funções utilitárias
#     def normalize_text(text: str) -> str:
#         if not isinstance(text, str):
#             text = str(text)
#         return re.sub(r'_+', '_', re.sub(r'[^a-z0-9_]', '_',
#                                          unicodedata.normalize('NFKD', text.lower())
#                                          .encode('ASCII', 'ignore')
#                                          .decode('ASCII'))).strip('_')
#
#     def normalize_column_name(col_name: str) -> str:
#         return normalize_text(col_name)
#
#     def safe_request(url: str, params: Dict = None, method: str = 'get', retries: int = 3) -> Dict:
#         if params is None:
#             params = {}
#         params['api_token'] = API_TOKEN
#
#         for attempt in range(retries):
#             try:
#                 if method.lower() == 'get':
#                     response = requests.get(url, params=params)
#                 else:
#                     response = requests.post(url, params=params)
#                 response.raise_for_status()
#                 return response.json()
#             except Exception as e:
#                 if attempt < retries - 1:
#                     time.sleep(2 * (attempt + 1))
#                 else:
#                     print(f"Erro na requisição {url}: {e}")
#                     return {"data": []}
#
#     def upload_to_storage(dataframe: pd.DataFrame, folder: str, filename: str) -> bool:
#         if dataframe.empty:
#             return False
#
#         try:
#             dataframe.columns = [normalize_column_name(col) for col in dataframe.columns]
#             dataframe = dataframe.loc[:, ~dataframe.columns.duplicated()]
#             dataframe = dataframe.replace('None', None)
#
#             bucket = storage_client.bucket(BUCKET_NAME)
#             blob = bucket.blob(f"{folder}/{filename}")
#             csv_data = dataframe.to_csv(index=False, sep=';', encoding='utf-8-sig', na_rep='')
#
#             blob.upload_from_string(csv_data, content_type="text/csv")
#             print(f"Upload: {len(dataframe)} registros em {folder}/{filename}")
#             return True
#         except Exception as e:
#             print(f"Erro no upload de {filename}: {e}")
#             raise
#
#     def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
#         if df.empty:
#             return df
#         df.columns = [normalize_column_name(col) for col in df.columns]
#         return df.loc[:, ~df.columns.duplicated()]
#
#     # Funções de mapeamento
#     def fetch_all_mappings() -> Tuple[Dict, Dict]:
#         field_types = {
#             'deal': 'dealFields',
#             'product': 'productFields',
#             'activity': 'activityFields',
#             'person': 'personFields',
#             'organization': 'organizationFields',
#             'lead': 'leadFields'
#         }
#
#         field_mappings, dropdown_mappings = {}, {}
#         all_value_mappings, standard_fields = {}, set()
#
#         print("\nBuscando mapeamentos de campos...")
#
#         # Identificar campos padrão primeiro
#         for entity_type, endpoint in field_types.items():
#             try:
#                 response = safe_request(f"{BASE_URL_V1}/{endpoint}")
#                 for field in response.get('data', []):
#                     if field.get('edit_flag', False) == False:
#                         standard_fields.add(normalize_column_name(field.get('name', '')))
#             except:
#                 print(f"Aviso: Não foi possível buscar campos padrão de {entity_type}")
#
#         # Processar campos e valores para cada entidade
#         for entity_type, endpoint in field_types.items():
#             print(f"Processando {entity_type}...")
#             entity_field_map, entity_dropdown_map = {}, {}
#
#             try:
#                 response = safe_request(f"{BASE_URL_V1}/{endpoint}")
#                 fields = response.get('data', [])
#
#                 for field in fields:
#                     field_id = str(field.get('id', ''))
#                     field_key = field.get('key', '')
#                     field_name = field.get('name', '')
#
#                     if not field_key:
#                         continue
#
#                     name = normalize_column_name(field_name)
#
#                     if field.get('edit_flag', True) and name in standard_fields:
#                         name = f"{name}_custom_{field_id[:8]}"
#
#                     entity_field_map[field_key] = name
#                     if field_id and field_id != field_key:
#                         entity_field_map[field_id] = name
#
#                     if field.get('field_type') in ['enum', 'set', 'status', 'varchar_options'] and field.get('options'):
#                         option_map = {}
#                         for option in field['options']:
#                             option_id = str(option.get('id', ''))
#                             option_label = option.get('label', '')
#
#                             if option_id and option_label:
#                                 normalized_label = normalize_column_name(option_label)
#                                 option_map[option_id] = normalized_label
#                                 all_value_mappings[option_id] = normalized_label
#
#                         if option_map:
#                             entity_dropdown_map[field_key] = option_map
#                             if field_id and field_id != field_key:
#                                 entity_dropdown_map[field_id] = option_map
#                             entity_dropdown_map[name] = option_map
#
#                 field_mappings[entity_type] = entity_field_map
#                 dropdown_mappings[entity_type] = entity_dropdown_map
#                 print(f"  {len(entity_field_map)} campos mapeados")
#                 print(f"  {len(entity_dropdown_map)} campos com opções dropdown")
#
#             except Exception as e:
#                 print(f"Erro ao buscar campos de {entity_type}: {e}")
#
#         dropdown_mappings['_all_values'] = all_value_mappings
#         print(f"\nTotal de valores únicos mapeados: {len(all_value_mappings)}")
#
#         return field_mappings, dropdown_mappings
#
#     def save_mappings(field_mappings: Dict, dropdown_mappings: Dict) -> None:
#         fields_rows, dropdown_rows = [], []
#
#         for entity_type, mapping in field_mappings.items():
#             for field_key, field_name in mapping.items():
#                 fields_rows.append({
#                     'entity_type': entity_type,
#                     'field_key': field_key,
#                     'field_name': field_name
#                 })
#
#         for entity_type, dropdown_maps in dropdown_mappings.items():
#             if entity_type == '_all_values':
#                 continue
#             for field_key, options in dropdown_maps.items():
#                 for option_id, option_label in options.items():
#                     dropdown_rows.append({
#                         'entity_type': entity_type,
#                         'field_key': field_key,
#                         'option_id': option_id,
#                         'option_label': option_label
#                     })
#
#         if fields_rows:
#             upload_to_storage(pd.DataFrame(fields_rows), FOLDERS['fields_mapping'], 'fields_mapping.csv')
#         if dropdown_rows:
#             upload_to_storage(pd.DataFrame(dropdown_rows), FOLDERS['fields_mapping'], 'dropdown_mapping.csv')
#
#     # Funções de busca de dados
#     def fetch_paginated_data(endpoint: str, api_version: int = 1, params: Dict = None) -> List[Dict]:
#         all_data, limit = [], 500
#         params = params or {}
#
#         if api_version == 1:
#             start = 0
#             while True:
#                 response = safe_request(f"{BASE_URL_V1}/{endpoint}", {**params, 'start': start, 'limit': limit})
#                 items = response.get('data', [])
#                 if not items:
#                     break
#
#                 all_data.extend(items)
#                 start += len(items)
#                 print(f"  {endpoint}: {len(all_data)} registros...")
#
#                 if not response.get('additional_data', {}).get('pagination', {}).get('more_items_in_collection', False):
#                     break
#         else:
#             cursor = None
#             while True:
#                 request_params = {**params, 'limit': limit}
#                 if cursor:
#                     request_params['cursor'] = cursor
#
#                 response = safe_request(f"{BASE_URL_V2}/{endpoint}", request_params)
#                 items = response.get('data', [])
#                 if not items:
#                     break
#
#                 all_data.extend(items)
#                 print(f"  {endpoint}: {len(all_data)} registros...")
#
#                 cursor = response.get('additional_data', {}).get('next_cursor')
#                 if not cursor:
#                     break
#
#         return all_data
#
#     def fetch_activities_by_date_range(start_date: str, end_date: str) -> List[Dict]:
#         params = {
#             'updated_since': start_date,
#             'updated_until': end_date,
#             'sort_by': 'update_time',
#             'sort_direction': 'asc',
#             'limit': 500
#         }
#
#         cursor, activities = None, []
#
#         while True:
#             if cursor:
#                 params['cursor'] = cursor
#             response = safe_request(f"{BASE_URL_V2}/activities", params)
#             items = response.get('data', [])
#             if not items:
#                 break
#
#             activities.extend(items)
#             cursor = response.get('additional_data', {}).get('next_cursor')
#             if not cursor:
#                 break
#
#         return activities
#
#     def generate_date_ranges(months_back: int = 12) -> List[Tuple[str, str]]:
#         now = datetime.now()
#         return [(
#             (now - timedelta(days=30 * (i + 1))).strftime('%Y-%m-%dT00:00:00Z'),
#             (now - timedelta(days=30 * i)).strftime('%Y-%m-%dT23:59:59Z')
#         ) for i in range(months_back)]
#
#     def fetch_deal_products(deal_ids: List[int]) -> List[Dict]:
#         chunks = [deal_ids[i:i + CHUNK_SIZE] for i in range(0, len(deal_ids), CHUNK_SIZE)]
#
#         def fetch_chunk(chunk):
#             products = safe_request(f"{BASE_URL_V2}/deals/products", {'deal_ids': ','.join(map(str, chunk))})
#             return products.get('data', [])
#
#         all_products = []
#         with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
#             results = list(executor.map(fetch_chunk, chunks))
#
#         for result in results:
#             all_products.extend(result)
#
#         return all_products
#
#     # Funções de processamento
#     def process_custom_fields(item: Dict, field_mapping: Dict, dropdown_mapping: Dict) -> Dict:
#         custom_fields = {}
#         all_values = dropdown_mapping.get('_all_values', {})
#
#         if "custom_fields" not in item or not isinstance(item["custom_fields"], dict):
#             return custom_fields
#
#         for field_id, value in item["custom_fields"].items():
#             field_name = field_mapping.get(field_id, field_id)
#
#             if value is not None:
#                 if field_id in dropdown_mapping:
#                     value_map = dropdown_mapping[field_id]
#                     if isinstance(value, list):
#                         mapped_values = []
#                         for v in value:
#                             mapped_val = value_map.get(str(v), all_values.get(str(v), str(v)))
#                             mapped_values.append(mapped_val)
#                         value = ','.join(mapped_values)
#                     else:
#                         value = value_map.get(str(value), all_values.get(str(value), str(value)))
#                 elif str(value).isdigit() and str(value) in all_values:
#                     value = all_values[str(value)]
#
#             custom_fields[field_name] = value
#
#         return custom_fields
#
#     def process_entity(items: List[Dict], entity_type: str, field_mapping: Dict, dropdown_mapping: Dict) -> List[Dict]:
#         processed_items = []
#
#         std_value_maps = {
#             'status': {'open': 'aberto', 'won': 'ganho', 'lost': 'perdido', 'deleted': 'excluido'},
#             'visible_to': {'1': 'proprietario_do_item', '3': 'todos_os_usuarios', '7': 'toda_a_empresa'},
#             'is_archived': {'False': 'nao_arquivado', 'True': 'arquivado'},
#             'done': {'False': 'nao_concluido', 'True': 'concluido'},
#             'busy': {'False': 'nao_ocupado', 'True': 'ocupado'}
#         }
#         relation_fields = {'creator_user_id': 'criador', 'user_id': 'proprietario', 'owner_id': 'proprietario'}
#         all_values = dropdown_mapping.get('_all_values', {})
#
#         for item in items:
#             processed = item.copy()
#
#             for key, value in list(processed.items()):
#                 if value is not None and str(value) in all_values:
#                     processed[key] = all_values[str(value)]
#
#             for field in ['person_id', 'org_id', 'person_name', 'org_name', 'pessoa_de_contato', 'organizacao']:
#                 if field in processed:
#                     processed.pop(field, None)
#
#             custom_fields = process_custom_fields(processed, field_mapping, dropdown_mapping)
#             processed.update(custom_fields)
#             processed.pop("custom_fields", None)
#
#             for old_field, new_field in relation_fields.items():
#                 if old_field in processed and processed[old_field] and isinstance(processed[old_field], dict):
#                     processed[new_field] = processed[old_field].get('name', '')
#                     processed.pop(old_field, None)
#
#             for field, value_map in std_value_maps.items():
#                 if field in processed and processed[field] is not None:
#                     processed[field] = value_map.get(str(processed[field]), processed[field])
#
#             if entity_type == 'activity':
#                 if 'location' in processed and isinstance(processed['location'], dict):
#                     for loc_key, loc_value in processed['location'].items():
#                         processed[f'location_{loc_key}'] = loc_value
#                     processed.pop('location', None)
#
#                 for field_name, prefix in [('participants', 'participant_'), ('attendees', 'attendee_')]:
#                     if field_name in processed and isinstance(processed[field_name], list):
#                         for i, item in enumerate(processed[field_name][:5], 1):
#                             for k, v in item.items():
#                                 processed[f'{prefix}{k}_{i}'] = v
#                         processed.pop(field_name, None)
#
#             processed_items.append(processed)
#
#         return processed_items
#
#     def process_entity_chunk(chunk_data: Tuple[List[Dict], str, Dict, Dict]) -> List[Dict]:
#         items, entity_type, field_mapping, dropdown_mapping = chunk_data
#         return process_entity(items, entity_type, field_mapping, dropdown_mapping)
#
#     def apply_mappings(df: pd.DataFrame, field_mapping: Dict, dropdown_mapping: Dict) -> pd.DataFrame:
#         if df.empty:
#             return df
#         result_df = df.copy()
#
#         rename_cols = {}
#         renamed = set()
#
#         for col in result_df.columns:
#             if col in field_mapping:
#                 new_name = field_mapping[col]
#                 if new_name in renamed:
#                     new_name = f"{new_name}_alt_{col[:6]}"
#                 rename_cols[col] = new_name
#                 renamed.add(new_name)
#
#         if rename_cols:
#             result_df.rename(columns=rename_cols, inplace=True)
#
#         all_values = dropdown_mapping.get('_all_values', {})
#
#         for col in result_df.columns:
#             if pd.api.types.is_object_dtype(result_df[col]):
#                 result_df[col] = result_df[col].astype(str)
#
#                 result_df[col] = result_df[col].map(
#                     lambda x: all_values.get(x, x) if pd.notna(x) and x != 'nan' else x
#                 )
#
#                 if col in dropdown_mapping and dropdown_mapping[col]:
#                     result_df[col] = result_df[col].map(
#                         lambda x: dropdown_mapping[col].get(x, x) if pd.notna(x) and x != 'nan' else x
#                     )
#
#         return result_df
#
#     def fix_hash_columns(df: pd.DataFrame, field_mapping: Dict) -> pd.DataFrame:
#         hash_cols = [col for col in df.columns if re.match(r'^[0-9a-f]{40}$', col)]
#
#         if hash_cols:
#             cols_to_rename = {}
#             existing = set(df.columns)
#
#             for col in hash_cols:
#                 if col in field_mapping:
#                     new_name = field_mapping[col]
#                     if new_name in existing and new_name != col:
#                         new_name = f"{new_name}_custom_{col[:6]}"
#                     cols_to_rename[col] = new_name
#                 else:
#                     cols_to_rename[col] = f"campo_{col[:6]}"
#
#             if cols_to_rename:
#                 df.rename(columns=cols_to_rename, inplace=True)
#
#         return df
#
#     def fix_channel_fields(df: pd.DataFrame) -> pd.DataFrame:
#         if df.empty:
#             return df
#
#         def is_likely_id(value):
#             if pd.isna(value):
#                 return False
#             if isinstance(value, (int, float)):
#                 return True
#             if isinstance(value, str):
#                 return bool(re.match(r'^\d+\.?\d*$', value))
#             return False
#
#         def is_likely_text(value):
#             if pd.isna(value):
#                 return False
#             if isinstance(value, str) and not re.match(r'^\d+\.?\d*$', value):
#                 return True
#             return False
#
#         for name_col, id_col in [('canal_de_origem', 'id_do_canal_de_origem'),
#                                  ('origem', 'id_origem'),
#                                  ('canal', 'canal_id')]:
#             if name_col in df.columns and id_col in df.columns:
#                 try:
#                     name_col_numeric_ratio = df[name_col].apply(is_likely_id).mean()
#                     id_col_text_ratio = df[id_col].apply(is_likely_text).mean()
#
#                     if name_col_numeric_ratio > 0.5 and id_col_text_ratio > 0.5:
#                         print(f"Detectada inversão entre {name_col} e {id_col}. Corrigindo...")
#                         df[name_col], df[id_col] = df[id_col].copy(), df[name_col].copy()
#                 except Exception as e:
#                     print(f"Erro ao verificar inversão de {name_col}/{id_col}: {e}")
#
#         return df
#
#     # Funções de enriquecimento
#     def debug_stages_data(stages_df, deals_df):
#         if 'stage_id' not in deals_df.columns:
#             print("ERRO: Coluna 'stage_id' não encontrada em deals!")
#             return
#
#         stage_ids_in_deals = deals_df['stage_id'].astype(str).unique()
#         stage_ids_in_stages = stages_df['id'].astype(str).unique()
#
#         missing_stages = [sid for sid in stage_ids_in_deals if sid not in stage_ids_in_stages]
#         if missing_stages:
#             print(f"ALERTA: {len(missing_stages)} IDs de stages em deals não estão em stages_df!")
#
#     def diagnose_and_fix_stage_id(deals_df, stages_df):
#         stage_columns = ['stage_id', 'etapa', 'stage', 'pipeline_stage_id']
#         existing_stage_columns = [col for col in stage_columns if col in deals_df.columns]
#
#         if existing_stage_columns:
#             stage_column = existing_stage_columns[0]
#             if stage_column != 'stage_id':
#                 deals_df['stage_id'] = deals_df[stage_column]
#         else:
#             numeric_cols = [col for col in deals_df.columns
#                             if deals_df[col].dtype in ['int64', 'float64']
#                             or (pd.api.types.is_object_dtype(deals_df[col])
#                                 and deals_df[col].astype(str).str.isdigit().any())]
#
#             stage_ids = set(str(x) for x in stages_df['id'])
#
#             for col in numeric_cols:
#                 unique_values = set(str(x) for x in deals_df[col].unique() if pd.notna(x))
#                 match_count = len(unique_values.intersection(stage_ids))
#
#                 if match_count > 0 and (match_count >= 3 or match_count / len(unique_values) > 0.2):
#                     deals_df['stage_id'] = deals_df[col]
#                     print(f"Coluna '{col}' identificada como stage_id")
#                     break
#
#         return deals_df
#
#     def enrich_deals(deals_df: pd.DataFrame, pipelines_df: pd.DataFrame,
#                      stages_df: pd.DataFrame, products_df: pd.DataFrame) -> pd.DataFrame:
#         enriched = deals_df.copy()
#
#         debug_stages_data(stages_df, enriched)
#
#         if 'pipeline_id' in enriched.columns and not pipelines_df.empty:
#             try:
#                 enriched = enriched.merge(
#                     pipelines_df[['id', 'name', 'order_nr']],
#                     left_on='pipeline_id', right_on='id',
#                     how='left', suffixes=('', '_pipeline')
#                 )
#                 enriched.rename(columns={'name': 'pipeline_name', 'order_nr': 'pipeline_order'}, inplace=True)
#                 if 'id_pipeline' in enriched.columns:
#                     enriched.drop(['id_pipeline'], axis=1, inplace=True)
#             except Exception as e:
#                 print(f"Erro ao juntar pipelines: {e}")
#
#         if 'stage_id' in enriched.columns and not stages_df.empty:
#             try:
#                 stages_dict = dict(zip(stages_df['id'].astype(str), stages_df['name']))
#                 order_dict = dict(zip(stages_df['id'].astype(str), stages_df['order_nr']))
#
#                 enriched['etapa'] = enriched['stage_id'].astype(str).map(stages_dict)
#                 enriched['stage_order'] = enriched['stage_id'].astype(str).map(order_dict)
#                 enriched['etapa'] = enriched['etapa'].fillna(enriched['stage_id'].astype(str))
#             except Exception as e:
#                 print(f"Erro ao mapear stages: {e}")
#
#         if not products_df.empty and 'deal_id' in products_df.columns:
#             try:
#                 prod_by_deal = products_df.groupby('deal_id')
#                 for i, deal in enriched.iterrows():
#                     deal_id = deal['id']
#                     if deal_id in prod_by_deal.groups:
#                         prods = prod_by_deal.get_group(deal_id).to_dict('records')
#                         for j, prod in enumerate(prods[:5], 1):
#                             enriched.at[i, f'product_name_{j}'] = prod.get('name', '')
#                             enriched.at[i, f'product_quantity_{j}'] = prod.get('quantity', 0)
#                             enriched.at[i, f'product_price_{j}'] = prod.get('item_price', 0)
#                             enriched.at[i, f'product_id_{j}'] = prod.get('product_id', '')
#             except Exception as e:
#                 print(f"Erro ao adicionar produtos: {e}")
#
#         return fix_channel_fields(enriched)
#
#     # Função para processar leads
#     def fetch_and_process_leads(field_mappings: Dict, dropdown_mappings: Dict) -> pd.DataFrame:
#         print("\nProcessando leads...")
#
#         leads_data = fetch_paginated_data('leads')
#
#         if not leads_data:
#             print("Nenhum lead encontrado!")
#             return pd.DataFrame()
#
#         print(f"{len(leads_data)} leads encontrados")
#
#         processed_leads = []
#         all_values = dropdown_mappings.get('_all_values', {})
#         lead_field_mapping = field_mappings.get('lead', {})
#         lead_dropdown_mapping = dropdown_mappings.get('lead', {})
#
#         for lead in leads_data:
#             processed = lead.copy()
#
#             for field_key, field_value in list(processed.items()):
#                 if field_value is not None and str(field_value) in all_values:
#                     processed[field_key] = all_values[str(field_value)]
#
#                 if field_key in lead_dropdown_mapping:
#                     value_map = lead_dropdown_mapping[field_key]
#                     if str(field_value) in value_map:
#                         processed[field_key] = value_map[str(field_value)]
#
#             if 'custom_fields' in processed:
#                 custom_fields = process_custom_fields(
#                     processed,
#                     lead_field_mapping,
#                     lead_dropdown_mapping
#                 )
#                 processed.update(custom_fields)
#                 processed.pop('custom_fields', None)
#
#             if 'owner_id' in processed and isinstance(processed['owner_id'], dict):
#                 processed['proprietario'] = processed['owner_id'].get('name', '')
#                 processed['proprietario_id'] = processed['owner_id'].get('id', '')
#                 processed.pop('owner_id', None)
#
#             if 'person_id' in processed and isinstance(processed['person_id'], dict):
#                 processed['pessoa_nome'] = processed['person_id'].get('name', '')
#                 processed['pessoa_id_valor'] = processed['person_id'].get('value', '')
#                 processed.pop('person_id', None)
#
#             if 'organization_id' in processed and isinstance(processed['organization_id'], dict):
#                 processed['organizacao_nome'] = processed['organization_id'].get('name', '')
#                 processed['organizacao_id_valor'] = processed['organization_id'].get('value', '')
#                 processed.pop('organization_id', None)
#
#             if 'source_name' in processed and isinstance(processed['source_name'], dict):
#                 processed['origem_nome'] = processed['source_name'].get('name', '')
#                 processed.pop('source_name', None)
#
#             label_fields = ['label_ids', 'label', 'labels']
#             for label_field in label_fields:
#                 if label_field in processed:
#                     if isinstance(processed[label_field], list):
#                         mapped_labels = []
#                         for label_id in processed[label_field]:
#                             if str(label_id) in all_values:
#                                 mapped_labels.append(all_values[str(label_id)])
#                             else:
#                                 mapped_labels.append(str(label_id))
#                         processed[label_field] = ','.join(mapped_labels)
#                     elif str(processed[label_field]) in all_values:
#                         processed[label_field] = all_values[str(processed[label_field])]
#
#             processed_leads.append(processed)
#
#         leads_df = pd.DataFrame(processed_leads)
#
#         leads_df = apply_mappings(leads_df, lead_field_mapping, lead_dropdown_mapping)
#         leads_df = fix_hash_columns(leads_df, lead_field_mapping)
#         leads_df = normalize_df(leads_df)
#         leads_df = fix_channel_fields(leads_df)
#
#         print(f"Leads processados: {len(leads_df)} registros")
#
#         return leads_df
#
#     # Função para processar atividades
#     def fetch_activities_parallel() -> pd.DataFrame:
#         print("\nBuscando atividades em paralelo...")
#         start_time = time.time()
#
#         date_ranges = generate_date_ranges(months_back=24)
#
#         with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
#             results = list(executor.map(
#                 lambda date_range: fetch_activities_by_date_range(date_range[0], date_range[1]),
#                 date_ranges
#             ))
#
#         all_activities = []
#         for result in results:
#             all_activities.extend(result)
#
#         print(f"Recuperadas {len(all_activities)} atividades em {time.time() - start_time:.2f} segundos")
#
#         if not all_activities:
#             print("Nenhuma atividade encontrada!")
#             return pd.DataFrame()
#
#         chunks = [all_activities[i:i + CHUNK_SIZE] for i in range(0, len(all_activities), CHUNK_SIZE)]
#         chunk_data = [(chunk, 'activity', field_mappings.get('activity', {}), dropdown_mappings.get('activity', {}))
#                       for chunk in chunks]
#
#         with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
#             processed_chunks = list(executor.map(process_entity_chunk, chunk_data))
#
#         processed_activities = []
#         for chunk in processed_chunks:
#             processed_activities.extend(chunk)
#
#         print(f"Processadas {len(processed_activities)} atividades")
#
#         activities_df = pd.DataFrame(processed_activities)
#         activities_df = apply_mappings(activities_df, field_mappings.get('activity', {}),
#                                        dropdown_mappings.get('activity', {}))
#         activities_df = fix_hash_columns(activities_df, field_mappings.get('activity', {}))
#         activities_df = normalize_df(activities_df)
#         activities_df = fix_channel_fields(activities_df)
#
#         return activities_df
#
#     def process_entity_parallel(data: List[Dict], entity_type: str, field_mapping: Dict,
#                                 dropdown_mapping: Dict) -> pd.DataFrame:
#         chunks = [data[i:i + CHUNK_SIZE] for i in range(0, len(data), CHUNK_SIZE)]
#         chunk_data = [(chunk, entity_type, field_mapping, dropdown_mapping) for chunk in chunks]
#
#         processed_items = []
#         with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
#             results = list(executor.map(process_entity_chunk, chunk_data))
#
#         for result in results:
#             processed_items.extend(result)
#
#         df = pd.DataFrame(processed_items)
#         df = apply_mappings(df, field_mapping, dropdown_mapping)
#         df = fix_hash_columns(df, field_mapping)
#         df = normalize_df(df)
#         df = fix_channel_fields(df)
#
#         return df
#
#     # Função principal
#     def main():
#         print("\n===== INICIANDO INTEGRAÇÃO PIPEDRIVE =====")
#
#         start_total_time = time.time()
#
#         # 1. Obter e salvar mapeamentos
#         print("\nObtendo mapeamentos de campos...")
#         global field_mappings, dropdown_mappings
#         field_mappings, dropdown_mappings = fetch_all_mappings()
#         save_mappings(field_mappings, dropdown_mappings)
#
#         # 2. Processar pipelines e stages
#         print("\nProcessando pipelines e stages...")
#         pipelines_data = fetch_paginated_data('pipelines')
#         pipelines_df = pd.DataFrame(process_entity(pipelines_data, 'pipeline', {}, {}))
#         upload_to_storage(pipelines_df, FOLDERS['pipelines'], 'pipelines.csv')
#
#         stages_data = fetch_paginated_data('stages')
#         stages_df = pd.DataFrame(process_entity(stages_data, 'stage', {}, {}))
#         upload_to_storage(stages_df, FOLDERS['stages'], 'stages.csv')
#
#         # 3. Processar leads
#         print("\nProcessando leads...")
#         leads_df = fetch_and_process_leads(field_mappings, dropdown_mappings)
#         if not leads_df.empty:
#             upload_to_storage(leads_df, FOLDERS['leads'], 'leads.csv')
#
#         # Iniciar busca de atividades em paralelo
#         activities_future = None
#         with concurrent.futures.ThreadPoolExecutor() as executor:
#             activities_future = executor.submit(fetch_activities_parallel)
#
#         # 4. Processar deals em paralelo
#         print("\nProcessando deals...")
#         deals_data = fetch_paginated_data('deals')
#         deals_df = process_entity_parallel(deals_data, 'deal',
#                                            field_mappings.get('deal', {}),
#                                            dropdown_mappings.get('deal', {}))
#         upload_to_storage(deals_df, FOLDERS['deals'], 'deals.csv')
#
#         # 5. Processar produtos em paralelo
#         print("\nProcessando produtos dos deals...")
#         products_data = []
#         if 'products_count' in deals_df.columns:
#             deals_with_products = deals_df[deals_df['products_count'] > 0]
#             if not deals_with_products.empty:
#                 products_data = fetch_deal_products(deals_with_products['id'].tolist())
#
#         products_df = process_entity_parallel(products_data, 'product',
#                                               field_mappings.get('product', {}),
#                                               dropdown_mappings.get('product', {}))
#         upload_to_storage(products_df, FOLDERS['products'], 'products.csv')
#
#         # 6. Verificar e corrigir stage_id antes de enriquecer deals
#         print("\nEnriquecendo dados de deals...")
#         deals_df = diagnose_and_fix_stage_id(deals_df, stages_df)
#
#         # 7. Enriquecer deals
#         enriched_deals_df = enrich_deals(deals_df, pipelines_df, stages_df, products_df)
#
#         enriched_deals_df = apply_mappings(enriched_deals_df, field_mappings.get('deal', {}),
#                                            dropdown_mappings.get('deal', {}))
#         enriched_deals_df = fix_hash_columns(enriched_deals_df, field_mappings.get('deal', {}))
#         enriched_deals_df = normalize_df(enriched_deals_df)
#         enriched_deals_df = fix_channel_fields(enriched_deals_df)
#
#         upload_to_storage(enriched_deals_df, FOLDERS['deals_dados_finais'], 'enriched_deals.csv')
#
#         # 8. Obter resultados da busca de atividades
#         print("\nFinalizando processamento de atividades...")
#         activities_df = activities_future.result()
#
#         if not activities_df.empty:
#             chunk_size = 100000
#             if len(activities_df) > chunk_size:
#                 for i in range(0, len(activities_df), chunk_size):
#                     chunk_end = min(i + chunk_size, len(activities_df))
#                     chunk_df = activities_df.iloc[i:chunk_end]
#                     filename = f'activities_chunk_{i // chunk_size + 1}.csv'
#                     upload_to_storage(chunk_df, FOLDERS['activities'], filename)
#             else:
#                 upload_to_storage(activities_df, FOLDERS['activities'], 'activities.csv')
#
#         # Resumo final
#         total_time = time.time() - start_total_time
#         print("\n===== INTEGRAÇÃO PIPEDRIVE CONCLUÍDA =====")
#         print(f"\nTempo total: {total_time / 60:.2f} minutos")
#         print("\nResumo dos dados extraídos:")
#         print(f"  Pipelines: {len(pipelines_df)} registros")
#         print(f"  Stages: {len(stages_df)} registros")
#         print(f"  Leads: {len(leads_df)} registros")
#         print(f"  Deals: {len(deals_df)} registros")
#         print(f"  Produtos: {len(products_df)} registros")
#         print(f"  Atividades: {len(activities_df)} registros")
#         print(f"  Deals enriquecidos: {len(enriched_deals_df)} registros")
#
#     # Executar
#     main()
#
#
# def run_persons(customer):
#     """
#     Função para extrair dados de Pessoas (Contacts) do Pipedrive
#     """
#     import requests, pandas as pd, os, re, unicodedata, time
#     from google.cloud import storage
#     from typing import Dict, List, Tuple
#     import concurrent.futures
#     import pathlib
#
#     # Configurações
#     SERVICE_ACCOUNT_FILE = pathlib.Path('config', 'setup_automatico.json').as_posix()
#     PROJECT_ID, BUCKET_NAME = customer['project_id'], customer['bucket_name']
#     API_TOKEN = customer['api_token']
#     COMPANY_DOMAIN = customer['company_domain']
#     BASE_URL_V1 = customer['base_url_v1'].lower().format(company_domain=COMPANY_DOMAIN)
#
#     FOLDER = 'persons'
#
#     # Configs de paralelismo
#     MAX_WORKERS = 8
#     CHUNK_SIZE = 200
#     BATCH_SIZE = 500
#
#     # Variáveis globais para mapeamentos
#     field_mappings = {}
#     dropdown_mappings = {}
#
#     # Iniciar cliente GCS
#     os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_FILE
#     storage_client = storage.Client.from_service_account_json(SERVICE_ACCOUNT_FILE, project=PROJECT_ID)
#
#     # Funções utilitárias
#     def normalize_text(text: str) -> str:
#         if not isinstance(text, str):
#             text = str(text)
#         return re.sub(r'_+', '_', re.sub(r'[^a-z0-9_]', '_',
#                                          unicodedata.normalize('NFKD', text.lower())
#                                          .encode('ASCII', 'ignore')
#                                          .decode('ASCII'))).strip('_')
#
#     def normalize_column_name(col_name: str) -> str:
#         return normalize_text(col_name)
#
#     def safe_request(url: str, params: Dict = None, method: str = 'get', retries: int = 3) -> Dict:
#         if params is None:
#             params = {}
#         params['api_token'] = API_TOKEN
#
#         for attempt in range(retries):
#             try:
#                 if method.lower() == 'get':
#                     response = requests.get(url, params=params)
#                 else:
#                     response = requests.post(url, params=params)
#                 response.raise_for_status()
#                 return response.json()
#             except Exception as e:
#                 if attempt < retries - 1:
#                     time.sleep(2 * (attempt + 1))
#                 else:
#                     print(f"Erro na requisição para {url}: {e}")
#                     return {"data": []}
#
#     def upload_to_storage(dataframe: pd.DataFrame, filename: str) -> bool:
#         if dataframe.empty:
#             return False
#
#         try:
#             dataframe.columns = [normalize_column_name(col) for col in dataframe.columns]
#             dataframe = dataframe.loc[:, ~dataframe.columns.duplicated()]
#             dataframe = dataframe.replace('None', None)
#
#             bucket = storage_client.bucket(BUCKET_NAME)
#             blob = bucket.blob(f"{FOLDER}/{filename}")
#             csv_data = dataframe.to_csv(index=False, sep=';', encoding='utf-8-sig', na_rep='')
#             blob.upload_from_string(csv_data, content_type="text/csv")
#             print(f"Upload: {len(dataframe)} registros em {FOLDER}/{filename}")
#             return True
#         except Exception as e:
#             print(f"Erro no upload de {filename}: {e}")
#             raise
#
#     def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
#         if df.empty:
#             return df
#         df.columns = [normalize_column_name(col) for col in df.columns]
#         return df.loc[:, ~df.columns.duplicated()]
#
#     # Funções de mapeamento
#     def fetch_person_fields() -> Tuple[Dict, Dict]:
#         global field_mappings, dropdown_mappings
#
#         person_field_mapping = {}
#         person_dropdown_mapping = {}
#         all_value_mappings = {}
#         standard_fields = set()
#
#         print("\nBuscando campos de persons...")
#
#         response = safe_request(f"{BASE_URL_V1}/personFields")
#         fields = response.get('data', [])
#
#         for field in fields:
#             field_id = str(field.get('id', ''))
#             field_key = field.get('key', '')
#             field_name = field.get('name', '')
#
#             if not field_key:
#                 continue
#
#             name = normalize_column_name(field_name)
#
#             if field.get('edit_flag', True) and name in standard_fields:
#                 name = f"{name}_custom_{field_id[:8]}"
#
#             person_field_mapping[field_key] = name
#             if field_id and field_id != field_key:
#                 person_field_mapping[field_id] = name
#
#             if field.get('field_type') in ['enum', 'set', 'status', 'varchar_options'] and field.get('options'):
#                 option_map = {}
#                 for option in field['options']:
#                     option_id = str(option.get('id', ''))
#                     option_label = option.get('label', '')
#                     if option_id and option_label:
#                         normalized_label = normalize_column_name(option_label)
#                         option_map[option_id] = normalized_label
#                         all_value_mappings[option_id] = normalized_label
#
#                 if option_map:
#                     person_dropdown_mapping[field_key] = option_map
#                     if field_id and field_id != field_key:
#                         person_dropdown_mapping[field_id] = option_map
#
#         print(f"Mapeados {len(person_field_mapping)} campos para persons")
#
#         field_mappings = person_field_mapping
#         person_dropdown_mapping['_all_values'] = all_value_mappings
#         dropdown_mappings = person_dropdown_mapping
#
#         return person_field_mapping, person_dropdown_mapping
#
#     # Funções de busca
#     def fetch_persons_batch(start_offset: int) -> List[Dict]:
#         try:
#             url = f"{BASE_URL_V1}/persons"
#             params = {
#                 'start': start_offset,
#                 'limit': BATCH_SIZE
#             }
#
#             response = safe_request(url, params)
#             return response.get('data', [])
#         except Exception as e:
#             print(f"Erro ao buscar lote de pessoas (start={start_offset}): {e}")
#             return []
#
#     def fetch_all_persons() -> List[Dict]:
#         url = f"{BASE_URL_V1}/persons"
#         params = {'start': 0, 'limit': 1}
#
#         try:
#             response = safe_request(url, params)
#             pagination = response.get('additional_data', {}).get('pagination', {})
#             total_count = pagination.get('total_count', 0)
#
#             if total_count:
#                 print(f"Total de persons a buscar: {total_count}")
#
#             offsets = list(range(0, total_count if total_count else 50000, BATCH_SIZE))
#
#             all_persons = []
#             with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
#                 results = list(executor.map(fetch_persons_batch, offsets))
#
#                 for i, batch in enumerate(results):
#                     if batch:
#                         all_persons.extend(batch)
#                     if (i + 1) % 10 == 0:
#                         print(f"  Progresso: {len(all_persons)} persons recuperadas...")
#
#             return all_persons
#
#         except Exception as e:
#             print(f"Erro na busca paralela: {e}. Usando método sequencial.")
#             return fetch_persons_sequential()
#
#     def fetch_persons_sequential() -> List[Dict]:
#         all_persons = []
#         start = 0
#
#         while True:
#             try:
#                 response = safe_request(f"{BASE_URL_V1}/persons", {'start': start, 'limit': BATCH_SIZE})
#                 data = response.get('data', [])
#
#                 if not data:
#                     break
#
#                 all_persons.extend(data)
#                 start += len(data)
#
#                 print(f"  {len(all_persons)} persons recuperadas...")
#
#                 if not response.get('additional_data', {}).get('pagination', {}).get('more_items_in_collection', False):
#                     break
#
#             except Exception as e:
#                 print(f"Erro ao buscar persons: {e}")
#                 break
#
#         return all_persons
#
#     # Funções de processamento
#     def process_custom_fields_person(item: Dict) -> Dict:
#         custom_fields = {}
#         all_values = dropdown_mappings.get('_all_values', {})
#
#         if "custom_fields" not in item or not isinstance(item["custom_fields"], dict):
#             return custom_fields
#
#         for field_id, value in item["custom_fields"].items():
#             field_name = field_mappings.get(field_id, field_id)
#
#             if value is not None:
#                 if field_id in dropdown_mappings:
#                     value_map = dropdown_mappings[field_id]
#                     if isinstance(value, list):
#                         mapped_values = []
#                         for v in value:
#                             mapped_val = value_map.get(str(v), all_values.get(str(v), str(v)))
#                             mapped_values.append(mapped_val)
#                         value = ','.join(mapped_values)
#                     else:
#                         value = value_map.get(str(value), all_values.get(str(value), str(value)))
#                 elif str(value).isdigit() and str(value) in all_values:
#                     value = all_values[str(value)]
#
#             custom_fields[field_name] = value
#
#         return custom_fields
#
#     def process_person(items: List[Dict]) -> List[Dict]:
#         processed_items = []
#
#         std_value_maps = {
#             'visible_to': {'1': 'proprietario_do_item', '3': 'todos_os_usuarios', '7': 'toda_a_empresa'},
#         }
#         relation_fields = {'owner_id': 'proprietario'}
#         all_values = dropdown_mappings.get('_all_values', {})
#
#         for item in items:
#             processed = item.copy()
#
#             for key, value in list(processed.items()):
#                 if value is not None and str(value) in all_values:
#                     processed[key] = all_values[str(value)]
#
#             custom_fields = process_custom_fields_person(processed)
#             processed.update(custom_fields)
#             processed.pop("custom_fields", None)
#
#             for old_field, new_field in relation_fields.items():
#                 if old_field in processed and processed[old_field] and isinstance(processed[old_field], dict):
#                     processed[new_field] = processed[old_field].get('name', '')
#                     processed[f'{new_field}_id'] = processed[old_field].get('id', '')
#                     processed.pop(old_field, None)
#
#             for field, value_map in std_value_maps.items():
#                 if field in processed and processed[field] is not None:
#                     processed[field] = value_map.get(str(processed[field]), processed[field])
#
#             if 'email' in processed and isinstance(processed['email'], list):
#                 for i, email in enumerate(processed['email'][:5], 1):
#                     if isinstance(email, dict):
#                         for k, v in email.items():
#                             processed[f'email_{k}_{i}'] = v
#                 processed.pop('email', None)
#
#             if 'phone' in processed and isinstance(processed['phone'], list):
#                 for i, phone in enumerate(processed['phone'][:5], 1):
#                     if isinstance(phone, dict):
#                         for k, v in phone.items():
#                             processed[f'phone_{k}_{i}'] = v
#                 processed.pop('phone', None)
#
#             special_fields = ['emails', 'phones', 'im', 'postal_address']
#             for field in special_fields:
#                 if field in processed and isinstance(processed[field], list):
#                     prefix = field[:-1] if field.endswith('s') else field
#                     for i, item in enumerate(processed[field][:5], 1):
#                         if isinstance(item, dict):
#                             for k, v in item.items():
#                                 processed[f'{prefix}_{k}_{i}'] = v
#                     processed.pop(field, None)
#                 elif field in processed and isinstance(processed[field], dict):
#                     for k, v in processed[field].items():
#                         processed[f'{field}_{k}'] = v
#                     processed.pop(field, None)
#
#             if 'org_id' in processed and isinstance(processed['org_id'], dict):
#                 processed['organizacao'] = processed['org_id'].get('name', '')
#                 processed['organizacao_id'] = processed['org_id'].get('value', processed['org_id'].get('id', ''))
#                 processed.pop('org_id', None)
#
#             processed_items.append(processed)
#
#         return processed_items
#
#     def process_chunk(items: List[Dict]) -> List[Dict]:
#         return process_person(items)
#
#     def apply_mappings_persons(df: pd.DataFrame) -> pd.DataFrame:
#         if df.empty:
#             return df
#         result_df = df.copy()
#
#         rename_cols = {}
#         renamed = set()
#
#         for col in result_df.columns:
#             if col in field_mappings:
#                 new_name = field_mappings[col]
#                 if new_name in renamed:
#                     new_name = f"{new_name}_alt_{col[:6]}"
#                 rename_cols[col] = new_name
#                 renamed.add(new_name)
#
#         if rename_cols:
#             result_df.rename(columns=rename_cols, inplace=True)
#
#         all_values = dropdown_mappings.get('_all_values', {})
#         for col in result_df.columns:
#             if pd.api.types.is_object_dtype(result_df[col]):
#                 result_df[col] = result_df[col].astype(str)
#
#                 result_df[col] = result_df[col].map(
#                     lambda x: all_values.get(x, x) if pd.notna(x) and x != 'nan' else x
#                 )
#
#                 if col in dropdown_mappings and dropdown_mappings[col]:
#                     result_df[col] = result_df[col].map(
#                         lambda x: dropdown_mappings[col].get(x, x)
#                         if pd.notna(x) and x != 'nan' else x
#                     )
#
#         return result_df
#
#     def fix_hash_columns(df: pd.DataFrame) -> pd.DataFrame:
#         hash_cols = [col for col in df.columns if re.match(r'^[0-9a-f]{40}$', col)]
#
#         if hash_cols:
#             cols_to_rename = {}
#             existing = set(df.columns)
#
#             for col in hash_cols:
#                 if col in field_mappings:
#                     new_name = field_mappings[col]
#                     if new_name in existing and new_name != col:
#                         new_name = f"{new_name}_custom_{col[:6]}"
#                     cols_to_rename[col] = new_name
#                 else:
#                     cols_to_rename[col] = f"campo_{col[:6]}"
#
#             if cols_to_rename:
#                 df.rename(columns=cols_to_rename, inplace=True)
#
#         return df
#
#     def fix_channel_fields(df: pd.DataFrame) -> pd.DataFrame:
#         if df.empty:
#             return df
#
#         def is_likely_id(value):
#             if pd.isna(value):
#                 return False
#             if isinstance(value, (int, float)):
#                 return True
#             if isinstance(value, str):
#                 return bool(re.match(r'^\d+\.?\d*', value))
#             return False
#
#         def is_likely_text(value):
#             if pd.isna(value):
#                 return False
#             if isinstance(value, str) and not re.match(r'^\d+\.?\d*', value):
#                 return True
#             return False
#
#         return df
#
#         for name_col, id_col in [('canal_de_origem', 'id_do_canal_de_origem'),
#                                  ('origem', 'id_origem'),
#                                  ('canal', 'canal_id')]:
#             if name_col in df.columns and id_col in df.columns:
#                 try:
#                     name_col_numeric_ratio = df[name_col].apply(is_likely_id).mean()
#                     id_col_text_ratio = df[id_col].apply(is_likely_text).mean()
#
#                     if name_col_numeric_ratio > 0.5 and id_col_text_ratio > 0.5:
#                         print(f"Detectada inversão entre {name_col} e {id_col}. Corrigindo...")
#                         df[name_col], df[id_col] = df[id_col].copy(), df[name_col].copy()
#                 except Exception as e:
#                     print(f"Erro ao verificar inversão de {name_col}/{id_col}: {e}")
#
#         return df
#
#     # Função principal
#     def process_persons_parallel() -> pd.DataFrame:
#         global field_mappings, dropdown_mappings
#
#         print("\nProcessando persons...")
#         start_time = time.time()
#
#         field_mappings, dropdown_mappings = fetch_person_fields()
#
#         print("\nBuscando pessoas...")
#         all_persons = fetch_all_persons()
#
#         print(f"Recuperadas {len(all_persons)} pessoas em {time.time() - start_time:.2f} segundos")
#
#         if not all_persons:
#             print("Nenhuma pessoa encontrada!")
#             return pd.DataFrame()
#
#         print("\nProcessando dados de pessoas...")
#         chunks = [all_persons[i:i + CHUNK_SIZE] for i in range(0, len(all_persons), CHUNK_SIZE)]
#
#         with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
#             processed_chunks = list(executor.map(process_chunk, chunks))
#
#         processed_persons = []
#         for chunk in processed_chunks:
#             processed_persons.extend(chunk)
#
#         print(f"Processadas {len(processed_persons)} pessoas")
#
#         persons_df = pd.DataFrame(processed_persons)
#
#         print("\nAplicando mapeamentos e normalizando dados...")
#         persons_df = apply_mappings_persons(persons_df)
#         persons_df = fix_hash_columns(persons_df)
#         persons_df = normalize_df(persons_df)
#         persons_df = fix_channel_fields(persons_df)
#
#         return persons_df
#
#     def main():
#         try:
#             start_time = time.time()
#
#             persons_df = process_persons_parallel()
#
#             if not persons_df.empty:
#                 print(f"\nSalvando {len(persons_df)} pessoas processadas...")
#
#                 chunk_size = 100000
#                 if len(persons_df) > chunk_size:
#                     for i in range(0, len(persons_df), chunk_size):
#                         chunk_end = min(i + chunk_size, len(persons_df))
#                         chunk_df = persons_df.iloc[i:chunk_end]
#                         filename = f'persons_chunk_{i // chunk_size + 1}.csv'
#                         upload_to_storage(chunk_df, filename)
#                 else:
#                     upload_to_storage(persons_df, 'persons.csv')
#
#                 print(f"\nProcessadas e salvas {len(persons_df)} pessoas no total")
#
#             total_time = time.time() - start_time
#             print(f"\n===== PROCESSAMENTO DE PERSONS CONCLUÍDO =====")
#             print(f"Tempo total: {total_time / 60:.2f} minutos")
#
#             return persons_df
#
#         except Exception as e:
#             print(f"\nErro durante o processamento de pessoas: {e}")
#             import traceback
#             traceback.print_exc()
#             raise
#
#     # Executar
#     main()
#
#
# def get_extraction_tasks():
#     """
#     Get the list of data extraction tasks for Pipedrive.
#
#     Returns:
#         list: List of task configurations
#     """
#     return [
#         {
#             'task_id': 'run_pipedrive_integration',
#             'python_callable': run,
#             'description': 'Extrai deals, pipelines, stages, products, activities e leads do Pipedrive'
#         },
#         {
#             'task_id': 'run_persons',
#             'python_callable': run_persons,
#             'description': 'Extrai contatos (pessoas) do Pipedrive'
#         }
#     ]
"""
Pipedrive Integration Module - Refactored
Modular structure for better maintainability and clarity
Version: 3.0 - Organized by responsibilities
"""

import concurrent.futures
import os
import pathlib
import re
import time
import unicodedata
from datetime import datetime, timedelta
from typing import Dict, List, Tuple

import pandas as pd
import requests
from core import gcs
from google.cloud import storage


class PipedriveConfig:
    """Configuration and constants for Pipedrive integration"""

    def __init__(self, customer: Dict):
        self.SERVICE_ACCOUNT_FILE = pathlib.Path('config', 'setup_automatico.json').as_posix()
        self.PROJECT_ID = customer['project_id']
        self.BUCKET_NAME = customer['bucket_name']
        self.API_TOKEN = customer['api_token']
        self.COMPANY_DOMAIN = customer['company_domain']
        self.BASE_URL_V1 = customer['base_url_v1'].lower().format(company_domain=self.COMPANY_DOMAIN)
        self.BASE_URL_V2 = customer['base_url_v2'].lower().format(company_domain=self.COMPANY_DOMAIN)

        self.FOLDERS = {
            'deals': 'deals',
            'deals_dados_finais': 'deals_dados_finais',
            'pipelines': 'pipelines',
            'products': 'products',
            'stages': 'stages',
            'fields_mapping': 'fields_mapping',
            'activities': 'activities',
            'leads': 'leads',
            'persons': 'persons'
        }

        # Performance settings
        self.MAX_WORKERS = 8
        self.CHUNK_SIZE = 200
        self.BATCH_SIZE = 500


class PipedriveClient:
    """HTTP client for Pipedrive API requests"""

    def __init__(self, config: PipedriveConfig):
        self.config = config

    def safe_request(self, url: str, params: Dict = None, method: str = 'get', retries: int = 3) -> Dict:
        """Make safe API request with retry logic"""
        if params is None:
            params = {}
        params['api_token'] = self.config.API_TOKEN

        for attempt in range(retries):
            try:
                if method.lower() == 'get':
                    response = requests.get(url, params=params)
                else:
                    response = requests.post(url, params=params)
                response.raise_for_status()
                return response.json()
            except Exception as e:
                if attempt < retries - 1:
                    time.sleep(2 * (attempt + 1))
                else:
                    print(f"Erro na requisição {url}: {e}")
                    return {"data": []}

    def fetch_paginated_data(self, endpoint: str, api_version: int = 1, params: Dict = None) -> List[Dict]:
        """Fetch all data from paginated endpoint"""
        all_data, limit = [], 500
        params = params or {}

        if api_version == 1:
            start = 0
            while True:
                base_url = self.config.BASE_URL_V1 if api_version == 1 else self.config.BASE_URL_V2
                response = self.safe_request(f"{base_url}/{endpoint}", {**params, 'start': start, 'limit': limit})
                items = response.get('data', [])
                if not items:
                    break

                all_data.extend(items)
                start += len(items)
                print(f"  {endpoint}: {len(all_data)} registros...")

                if not response.get('additional_data', {}).get('pagination', {}).get('more_items_in_collection', False):
                    break
        else:
            cursor = None
            while True:
                request_params = {**params, 'limit': limit}
                if cursor:
                    request_params['cursor'] = cursor

                response = self.safe_request(f"{self.config.BASE_URL_V2}/{endpoint}", request_params)
                items = response.get('data', [])
                if not items:
                    break

                all_data.extend(items)
                print(f"  {endpoint}: {len(all_data)} registros...")

                cursor = response.get('additional_data', {}).get('next_cursor')
                if not cursor:
                    break

        return all_data


class DataUtils:
    """Utility functions for data processing"""

    @staticmethod
    def normalize_text(text: str) -> str:
        """Normalize text for column names"""
        if not isinstance(text, str):
            text = str(text)
        return re.sub(r'_+', '_', re.sub(r'[^a-z0-9_]', '_',
                                         unicodedata.normalize('NFKD', text.lower())
                                         .encode('ASCII', 'ignore')
                                         .decode('ASCII'))).strip('_')

    @staticmethod
    def normalize_column_name(col_name: str) -> str:
        """Normalize column name"""
        return DataUtils.normalize_text(col_name)

    @staticmethod
    def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
        """Normalize DataFrame columns"""
        if df.empty:
            return df
        df.columns = [DataUtils.normalize_column_name(col) for col in df.columns]
        return df.loc[:, ~df.columns.duplicated()]


class CloudStorageManager:
    """Manages Google Cloud Storage operations"""

    def __init__(self, config: PipedriveConfig):
        self.config = config
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = config.SERVICE_ACCOUNT_FILE
        self.storage_client = storage.Client.from_service_account_json(
            config.SERVICE_ACCOUNT_FILE,
            project=config.PROJECT_ID
        )

    def upload_to_storage(self, dataframe: pd.DataFrame, folder: str, filename: str) -> bool:
        """Upload DataFrame to Google Cloud Storage"""
        if dataframe.empty:
            return False

        try:
            dataframe.columns = [DataUtils.normalize_column_name(col) for col in dataframe.columns]
            dataframe = dataframe.loc[:, ~dataframe.columns.duplicated()]
            dataframe = dataframe.replace('None', None)

            bucket = self.storage_client.bucket(self.config.BUCKET_NAME)
            blob = bucket.blob(f"{folder}/{filename}")
            csv_data = dataframe.to_csv(index=False, sep=';', encoding='utf-8-sig', na_rep='')

            blob.upload_from_string(csv_data, content_type="text/csv")
            print(f"Upload: {len(dataframe)} registros em {folder}/{filename}")
            return True
        except Exception as e:
            print(f"Erro no upload de {filename}: {e}")
            raise


class FieldMappingService:
    """Handles field mappings and dropdown values"""

    def __init__(self, client: PipedriveClient):
        self.client = client
        self.field_mappings = {}
        self.dropdown_mappings = {}

    def fetch_all_mappings(self) -> Tuple[Dict, Dict]:
        """Fetch all field mappings and dropdown values"""
        field_types = {
            'deal': 'dealFields',
            'product': 'productFields',
            'activity': 'activityFields',
            'person': 'personFields',
            'organization': 'organizationFields',
            'lead': 'leadFields'
        }

        field_mappings, dropdown_mappings = {}, {}
        all_value_mappings, standard_fields = {}, set()

        print("\nBuscando mapeamentos de campos...")

        # Identify standard fields first
        for entity_type, endpoint in field_types.items():
            try:
                response = self.client.safe_request(f"{self.client.config.BASE_URL_V1}/{endpoint}")
                for field in response.get('data', []):
                    if field.get('edit_flag', False) == False:
                        standard_fields.add(DataUtils.normalize_column_name(field.get('name', '')))
            except:
                print(f"Aviso: Não foi possível buscar campos padrão de {entity_type}")

        # Process fields and values for each entity
        for entity_type, endpoint in field_types.items():
            print(f"Processando {entity_type}...")
            entity_field_map, entity_dropdown_map = self._process_entity_fields(
                endpoint, standard_fields, all_value_mappings
            )

            field_mappings[entity_type] = entity_field_map
            dropdown_mappings[entity_type] = entity_dropdown_map

        dropdown_mappings['_all_values'] = all_value_mappings
        print(f"\nTotal de valores únicos mapeados: {len(all_value_mappings)}")

        self.field_mappings = field_mappings
        self.dropdown_mappings = dropdown_mappings

        return field_mappings, dropdown_mappings

    def _process_entity_fields(self, endpoint: str, standard_fields: set, all_value_mappings: Dict) -> Tuple[
        Dict, Dict]:
        """Process fields for a specific entity type"""
        entity_field_map, entity_dropdown_map = {}, {}

        try:
            response = self.client.safe_request(f"{self.client.config.BASE_URL_V1}/{endpoint}")
            fields = response.get('data', [])

            for field in fields:
                field_id = str(field.get('id', ''))
                field_key = field.get('key', '')
                field_name = field.get('name', '')

                if not field_key:
                    continue

                name = DataUtils.normalize_column_name(field_name)

                if field.get('edit_flag', True) and name in standard_fields:
                    name = f"{name}_custom_{field_id[:8]}"

                entity_field_map[field_key] = name
                if field_id and field_id != field_key:
                    entity_field_map[field_id] = name

                # Process dropdown options
                if field.get('field_type') in ['enum', 'set', 'status', 'varchar_options'] and field.get('options'):
                    option_map = self._process_field_options(field['options'], all_value_mappings)

                    if option_map:
                        entity_dropdown_map[field_key] = option_map
                        if field_id and field_id != field_key:
                            entity_dropdown_map[field_id] = option_map
                        entity_dropdown_map[name] = option_map

            print(f"  {len(entity_field_map)} campos mapeados")
            print(f"  {len(entity_dropdown_map)} campos com opções dropdown")

        except Exception as e:
            print(f"Erro ao buscar campos de {endpoint}: {e}")

        return entity_field_map, entity_dropdown_map

    def _process_field_options(self, options: List[Dict], all_value_mappings: Dict) -> Dict:
        """Process dropdown options for a field"""
        option_map = {}
        for option in options:
            option_id = str(option.get('id', ''))
            option_label = option.get('label', '')

            if option_id and option_label:
                normalized_label = DataUtils.normalize_column_name(option_label)
                option_map[option_id] = normalized_label
                all_value_mappings[option_id] = normalized_label

        return option_map

    def save_mappings(self, storage_manager: CloudStorageManager) -> None:
        """Save field mappings to storage"""
        fields_rows, dropdown_rows = [], []

        for entity_type, mapping in self.field_mappings.items():
            for field_key, field_name in mapping.items():
                fields_rows.append({
                    'entity_type': entity_type,
                    'field_key': field_key,
                    'field_name': field_name
                })

        for entity_type, dropdown_maps in self.dropdown_mappings.items():
            if entity_type == '_all_values':
                continue
            for field_key, options in dropdown_maps.items():
                for option_id, option_label in options.items():
                    dropdown_rows.append({
                        'entity_type': entity_type,
                        'field_key': field_key,
                        'option_id': option_id,
                        'option_label': option_label
                    })

        if fields_rows:
            storage_manager.upload_to_storage(
                pd.DataFrame(fields_rows),
                storage_manager.config.FOLDERS['fields_mapping'],
                'fields_mapping.csv'
            )
        if dropdown_rows:
            storage_manager.upload_to_storage(
                pd.DataFrame(dropdown_rows),
                storage_manager.config.FOLDERS['fields_mapping'],
                'dropdown_mapping.csv'
            )


class DataProcessor:
    """Processes and transforms Pipedrive data"""

    def __init__(self, field_mappings: Dict, dropdown_mappings: Dict):
        self.field_mappings = field_mappings
        self.dropdown_mappings = dropdown_mappings

    def process_custom_fields(self, item: Dict, entity_type: str) -> Dict:
        """Process custom fields for an entity"""
        custom_fields = {}
        field_mapping = self.field_mappings.get(entity_type, {})
        dropdown_mapping = self.dropdown_mappings.get(entity_type, {})
        all_values = self.dropdown_mappings.get('_all_values', {})

        if "custom_fields" not in item or not isinstance(item["custom_fields"], dict):
            return custom_fields

        for field_id, value in item["custom_fields"].items():
            field_name = field_mapping.get(field_id, field_id)

            if value is not None:
                if field_id in dropdown_mapping:
                    value_map = dropdown_mapping[field_id]
                    if isinstance(value, list):
                        mapped_values = []
                        for v in value:
                            mapped_val = value_map.get(str(v), all_values.get(str(v), str(v)))
                            mapped_values.append(mapped_val)
                        value = ','.join(mapped_values)
                    else:
                        value = value_map.get(str(value), all_values.get(str(value), str(value)))
                elif str(value).isdigit() and str(value) in all_values:
                    value = all_values[str(value)]

            custom_fields[field_name] = value

        return custom_fields

    def process_relation_fields(self, processed: Dict) -> Dict:
        """Process relation fields (user references, etc.)"""
        relation_fields = {
            'creator_user_id': 'criador',
            'user_id': 'proprietario',
            'owner_id': 'proprietario'
        }

        # Handle standard relation fields
        for old_field, new_field in relation_fields.items():
            if old_field in processed and processed[old_field] and isinstance(processed[old_field], dict):
                processed[new_field] = processed[old_field].get('name', '')
                processed.pop(old_field, None)

        # Handle SDR and Closer responsável fields
        self._process_responsavel_fields(processed)

        return processed

    def _process_responsavel_fields(self, processed: Dict) -> None:
        """Process SDR and Closer responsável fields"""
        responsavel_fields = ['sdr_responsavel', 'closer_responsavel']

        for field in responsavel_fields:
            if field in processed and isinstance(processed[field], dict):
                field_data = processed[field]

                # Extract key information
                processed[f'{field}_id'] = field_data.get('id', '')
                processed[f'{field}_name'] = field_data.get('name', '')
                processed[f'{field}_email'] = field_data.get('email', '')
                processed[f'{field}_active'] = field_data.get('active_flag', '')

                # Remove original field
                processed.pop(field, None)

    def process_standard_value_mappings(self, processed: Dict) -> Dict:
        """Apply standard value mappings"""
        std_value_maps = {
            'status': {'open': 'aberto', 'won': 'ganho', 'lost': 'perdido', 'deleted': 'excluido'},
            'visible_to': {'1': 'proprietario_do_item', '3': 'todos_os_usuarios', '7': 'toda_a_empresa'},
            'is_archived': {'False': 'nao_arquivado', 'True': 'arquivado'},
            'done': {'False': 'nao_concluido', 'True': 'concluido'},
            'busy': {'False': 'nao_ocupado', 'True': 'ocupado'}
        }

        for field, value_map in std_value_maps.items():
            if field in processed and processed[field] is not None:
                processed[field] = value_map.get(str(processed[field]), processed[field])

        return processed

    def process_entity(self, items: List[Dict], entity_type: str) -> List[Dict]:
        """Process a list of entities"""
        processed_items = []
        all_values = self.dropdown_mappings.get('_all_values', {})

        for item in items:
            processed = item.copy()

            # Apply global value mappings
            for key, value in list(processed.items()):
                if value is not None and str(value) in all_values:
                    processed[key] = all_values[str(value)]

            # Remove unwanted fields
            for field in ['person_id', 'org_id', 'person_name', 'org_name',
                          'pessoa_de_contato', 'organizacao']:
                processed.pop(field, None)

            # Process custom fields
            custom_fields = self.process_custom_fields(processed, entity_type)
            processed.update(custom_fields)
            processed.pop("custom_fields", None)

            # Process relation fields
            processed = self.process_relation_fields(processed)

            # Apply standard value mappings
            processed = self.process_standard_value_mappings(processed)

            # Handle specific entity processing
            if entity_type == 'activity':
                processed = self._process_activity_specific(processed)
            elif entity_type == 'person':
                processed = self._process_person_specific(processed)

            processed_items.append(processed)

        return processed_items

    def _process_activity_specific(self, processed: Dict) -> Dict:
        """Process activity-specific fields"""
        # Handle location
        if 'location' in processed and isinstance(processed['location'], dict):
            for loc_key, loc_value in processed['location'].items():
                processed[f'location_{loc_key}'] = loc_value
            processed.pop('location', None)

        # Handle participants and attendees
        for field_name, prefix in [('participants', 'participant_'), ('attendees', 'attendee_')]:
            if field_name in processed and isinstance(processed[field_name], list):
                for i, item in enumerate(processed[field_name][:5], 1):
                    for k, v in item.items():
                        processed[f'{prefix}{k}_{i}'] = v
                processed.pop(field_name, None)

        return processed

    def _process_person_specific(self, processed: Dict) -> Dict:
        """Process person-specific fields"""
        # Handle email arrays
        if 'email' in processed and isinstance(processed['email'], list):
            for i, email in enumerate(processed['email'][:5], 1):
                if isinstance(email, dict):
                    for k, v in email.items():
                        processed[f'email_{k}_{i}'] = v
            processed.pop('email', None)

        # Handle phone arrays
        if 'phone' in processed and isinstance(processed['phone'], list):
            for i, phone in enumerate(processed['phone'][:5], 1):
                if isinstance(phone, dict):
                    for k, v in phone.items():
                        processed[f'phone_{k}_{i}'] = v
            processed.pop('phone', None)

        # Handle other contact fields
        special_fields = ['emails', 'phones', 'im', 'postal_address']
        for field in special_fields:
            if field in processed and isinstance(processed[field], list):
                prefix = field[:-1] if field.endswith('s') else field
                for i, item in enumerate(processed[field][:5], 1):
                    if isinstance(item, dict):
                        for k, v in item.items():
                            processed[f'{prefix}_{k}_{i}'] = v
                processed.pop(field, None)
            elif field in processed and isinstance(processed[field], dict):
                for k, v in processed[field].items():
                    processed[f'{field}_{k}'] = v
                processed.pop(field, None)

        # Handle organization
        if 'org_id' in processed and isinstance(processed['org_id'], dict):
            processed['organizacao'] = processed['org_id'].get('name', '')
            processed['organizacao_id'] = processed['org_id'].get('value', processed['org_id'].get('id', ''))
            processed.pop('org_id', None)

        return processed


class DealsService:
    """Service for handling deals and related data"""

    def __init__(self, client: PipedriveClient, processor: DataProcessor, storage: CloudStorageManager):
        self.client = client
        self.processor = processor
        self.storage = storage

    def fetch_deal_products(self, deal_ids: List[int]) -> List[Dict]:
        """Fetch products for given deal IDs"""
        chunks = [deal_ids[i:i + self.client.config.CHUNK_SIZE]
                  for i in range(0, len(deal_ids), self.client.config.CHUNK_SIZE)]

        def fetch_chunk(chunk):
            products = self.client.safe_request(
                f"{self.client.config.BASE_URL_V2}/deals/products",
                {'deal_ids': ','.join(map(str, chunk))}
            )
            return products.get('data', [])

        all_products = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.client.config.MAX_WORKERS) as executor:
            results = list(executor.map(fetch_chunk, chunks))

        for result in results:
            all_products.extend(result)

        return all_products

    def enrich_deals(self, deals_df: pd.DataFrame, pipelines_df: pd.DataFrame,
                     stages_df: pd.DataFrame, products_df: pd.DataFrame) -> pd.DataFrame:
        """Enrich deals with pipeline, stage, and product information"""
        enriched = deals_df.copy()

        # Enrich with pipeline data
        if 'pipeline_id' in enriched.columns and not pipelines_df.empty:
            enriched = self._enrich_with_pipelines(enriched, pipelines_df)

        # Enrich with stage data
        if 'stage_id' in enriched.columns and not stages_df.empty:
            enriched = self._enrich_with_stages(enriched, stages_df)

        # Enrich with products data
        if not products_df.empty and 'deal_id' in products_df.columns:
            enriched = self._enrich_with_products(enriched, products_df)

        return enriched

    def _enrich_with_pipelines(self, enriched: pd.DataFrame, pipelines_df: pd.DataFrame) -> pd.DataFrame:
        """Enrich deals with pipeline information"""
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

        return enriched

    def _enrich_with_stages(self, enriched: pd.DataFrame, stages_df: pd.DataFrame) -> pd.DataFrame:
        """Enrich deals with stage information"""
        try:
            stages_dict = dict(zip(stages_df['id'].astype(str), stages_df['name']))
            order_dict = dict(zip(stages_df['id'].astype(str), stages_df['order_nr']))

            enriched['etapa'] = enriched['stage_id'].astype(str).map(stages_dict)
            enriched['stage_order'] = enriched['stage_id'].astype(str).map(order_dict)
            enriched['etapa'] = enriched['etapa'].fillna(enriched['stage_id'].astype(str))
        except Exception as e:
            print(f"Erro ao mapear stages: {e}")

        return enriched

    def _enrich_with_products(self, enriched: pd.DataFrame, products_df: pd.DataFrame) -> pd.DataFrame:
        """Enrich deals with product information"""
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


class ActivitiesService:
    """Service for handling activities data"""

    def __init__(self, client: PipedriveClient):
        self.client = client

    def generate_date_ranges(self, months_back: int = 12) -> List[Tuple[str, str]]:
        """Generate date ranges for activity fetching"""
        now = datetime.now()
        return [(
            (now - timedelta(days=30 * (i + 1))).strftime('%Y-%m-%dT00:00:00Z'),
            (now - timedelta(days=30 * i)).strftime('%Y-%m-%dT23:59:59Z')
        ) for i in range(months_back)]

    def fetch_activities_by_date_range(self, start_date: str, end_date: str) -> List[Dict]:
        """Fetch activities for a specific date range"""
        params = {
            'updated_since': start_date,
            'updated_until': end_date,
            'sort_by': 'update_time',
            'sort_direction': 'asc',
            'limit': 500
        }

        cursor, activities = None, []

        while True:
            if cursor:
                params['cursor'] = cursor
            response = self.client.safe_request(f"{self.client.config.BASE_URL_V2}/activities", params)
            items = response.get('data', [])
            if not items:
                break

            activities.extend(items)
            cursor = response.get('additional_data', {}).get('next_cursor')
            if not cursor:
                break

        return activities

    def fetch_activities_parallel(self) -> List[Dict]:
        """Fetch activities in parallel using multiple date ranges"""
        print("\nBuscando atividades em paralelo...")
        start_time = time.time()

        date_ranges = self.generate_date_ranges(months_back=24)

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.client.config.MAX_WORKERS) as executor:
            results = list(executor.map(
                lambda date_range: self.fetch_activities_by_date_range(date_range[0], date_range[1]),
                date_ranges
            ))

        all_activities = []
        for result in results:
            all_activities.extend(result)

        print(f"Recuperadas {len(all_activities)} atividades em {time.time() - start_time:.2f} segundos")
        return all_activities


class PipedriveIntegration:
    """Main integration class that orchestrates all operations"""

    def __init__(self, customer: Dict):
        self.config = PipedriveConfig(customer)
        self.client = PipedriveClient(self.config)
        self.storage = CloudStorageManager(self.config)
        self.field_mapping_service = FieldMappingService(self.client)

    def run_main_integration(self):
        """Run the main Pipedrive integration"""
        print("\n===== INICIANDO INTEGRAÇÃO PIPEDRIVE =====")
        start_total_time = time.time()

        # 1. Get and save field mappings
        print("\nObtendo mapeamentos de campos...")
        field_mappings, dropdown_mappings = self.field_mapping_service.fetch_all_mappings()
        self.field_mapping_service.save_mappings(self.storage)

        # Initialize processor with mappings
        processor = DataProcessor(field_mappings, dropdown_mappings)
        deals_service = DealsService(self.client, processor, self.storage)
        activities_service = ActivitiesService(self.client)

        # 2. Process pipelines and stages
        print("\nProcessando pipelines e stages...")
        pipelines_df = self._process_and_save('pipelines', 'pipeline', processor)
        stages_df = self._process_and_save('stages', 'stage', processor)

        # 3. Process leads
        print("\nProcessando leads...")
        leads_df = self._process_leads(processor)

        # 4. Start activities fetch in parallel
        activities_future = None
        with concurrent.futures.ThreadPoolExecutor() as executor:
            activities_future = executor.submit(activities_service.fetch_activities_parallel)

        # 5. Process deals
        print("\nProcessando deals...")
        deals_df = self._process_and_save('deals', 'deal', processor)

        # 6. Process deal products
        print("\nProcessando produtos dos deals...")
        products_df = self._process_deal_products(deals_df, deals_service, processor)

        # 7. Enrich deals
        print("\nEnriquecendo dados de deals...")
        enriched_deals_df = deals_service.enrich_deals(deals_df, pipelines_df, stages_df, products_df)
        self.storage.upload_to_storage(
            enriched_deals_df,
            self.config.FOLDERS['deals_dados_finais'],
            'enriched_deals.csv'
        )

        # 8. Process activities
        print("\nFinalizando processamento de atividades...")
        activities_data = activities_future.result()
        activities_df = self._process_activities(activities_data, processor)

        # Final summary
        self._print_summary(start_total_time, pipelines_df, stages_df, leads_df,
                            deals_df, products_df, activities_df, enriched_deals_df)

    def run_persons_integration(self):
        """Run the persons integration"""
        print("\n===== INICIANDO INTEGRAÇÃO PERSONS =====")
        start_time = time.time()

        # Get person field mappings
        field_mappings, dropdown_mappings = self.field_mapping_service.fetch_person_fields()
        processor = DataProcessor({'person': field_mappings}, {'person': dropdown_mappings})

        # Fetch and process persons
        persons_data = self._fetch_all_persons()
        persons_df = self._process_persons_data(persons_data, processor)

        # Save persons data
        if not persons_df.empty:
            self._save_large_dataset(persons_df, 'persons', 'persons')

        total_time = time.time() - start_time
        print(f"\n===== PROCESSAMENTO DE PERSONS CONCLUÍDO =====")
        print(f"Tempo total: {total_time / 60:.2f} minutos")
        print(f"Persons processadas: {len(persons_df)} registros")

    def _process_and_save(self, endpoint: str, entity_type: str, processor: DataProcessor) -> pd.DataFrame:
        """Generic method to process and save entity data"""
        data = self.client.fetch_paginated_data(endpoint)
        processed_data = processor.process_entity(data, entity_type)
        df = pd.DataFrame(processed_data)
        df = DataUtils.normalize_df(df)

        self.storage.upload_to_storage(df, self.config.FOLDERS[endpoint], f'{endpoint}.csv')
        return df

    def _process_leads(self, processor: DataProcessor) -> pd.DataFrame:
        """Process leads with specific handling"""
        leads_data = self.client.fetch_paginated_data('leads')

        if not leads_data:
            print("Nenhum lead encontrado!")
            return pd.DataFrame()

        print(f"{len(leads_data)} leads encontrados")

        # Process leads with specific logic
        processed_leads = []
        all_values = processor.dropdown_mappings.get('_all_values', {})
        lead_field_mapping = processor.field_mappings.get('lead', {})
        lead_dropdown_mapping = processor.dropdown_mappings.get('lead', {})

        for lead in leads_data:
            processed = self._process_single_lead(lead, processor, all_values,
                                                  lead_field_mapping, lead_dropdown_mapping)
            processed_leads.append(processed)

        leads_df = pd.DataFrame(processed_leads)
        leads_df = DataUtils.normalize_df(leads_df)

        if not leads_df.empty:
            self.storage.upload_to_storage(leads_df, self.config.FOLDERS['leads'], 'leads.csv')

        return leads_df

    def _process_single_lead(self, lead: Dict, processor: DataProcessor, all_values: Dict,
                             field_mapping: Dict, dropdown_mapping: Dict) -> Dict:
        """Process a single lead"""
        processed = lead.copy()

        # Apply global value mappings
        for field_key, field_value in list(processed.items()):
            if field_value is not None and str(field_value) in all_values:
                processed[field_key] = all_values[str(field_value)]

            if field_key in dropdown_mapping:
                value_map = dropdown_mapping[field_key]
                if str(field_value) in value_map:
                    processed[field_key] = value_map[str(field_value)]

        # Process custom fields
        if 'custom_fields' in processed:
            custom_fields = processor.process_custom_fields(processed, 'lead')
            processed.update(custom_fields)
            processed.pop('custom_fields', None)

        # Process specific lead fields
        self._process_lead_specific_fields(processed)

        return processed

    def _process_lead_specific_fields(self, processed: Dict) -> None:
        """Process lead-specific fields"""
        # Handle owner
        if 'owner_id' in processed and isinstance(processed['owner_id'], dict):
            processed['proprietario'] = processed['owner_id'].get('name', '')
            processed['proprietario_id'] = processed['owner_id'].get('id', '')
            processed.pop('owner_id', None)

        # Handle person
        if 'person_id' in processed and isinstance(processed['person_id'], dict):
            processed['pessoa_nome'] = processed['person_id'].get('name', '')
            processed['pessoa_id_valor'] = processed['person_id'].get('value', '')
            processed.pop('person_id', None)

        # Handle organization
        if 'organization_id' in processed and isinstance(processed['organization_id'], dict):
            processed['organizacao_nome'] = processed['organization_id'].get('name', '')
            processed['organizacao_id_valor'] = processed['organization_id'].get('value', '')
            processed.pop('organization_id', None)

        # Handle source
        if 'source_name' in processed and isinstance(processed['source_name'], dict):
            processed['origem_nome'] = processed['source_name'].get('name', '')
            processed.pop('source_name', None)

        # Handle labels
        self._process_lead_labels(processed)

    def _process_lead_labels(self, processed: Dict) -> None:
        """Process lead labels"""
        all_values = self.field_mapping_service.dropdown_mappings.get('_all_values', {})
        label_fields = ['label_ids', 'label', 'labels']

        for label_field in label_fields:
            if label_field in processed:
                if isinstance(processed[label_field], list):
                    mapped_labels = []
                    for label_id in processed[label_field]:
                        if str(label_id) in all_values:
                            mapped_labels.append(all_values[str(label_id)])
                        else:
                            mapped_labels.append(str(label_id))
                    processed[label_field] = ','.join(mapped_labels)
                elif str(processed[label_field]) in all_values:
                    processed[label_field] = all_values[str(processed[label_field])]

    def _process_deal_products(self, deals_df: pd.DataFrame, deals_service: DealsService,
                               processor: DataProcessor) -> pd.DataFrame:
        """Process products for deals"""
        products_data = []
        if 'products_count' in deals_df.columns:
            deals_with_products = deals_df[deals_df['products_count'] > 0]
            if not deals_with_products.empty:
                products_data = deals_service.fetch_deal_products(deals_with_products['id'].tolist())

        processed_products = processor.process_entity(products_data, 'product')
        products_df = pd.DataFrame(processed_products)
        products_df = DataUtils.normalize_df(products_df)

        self.storage.upload_to_storage(products_df, self.config.FOLDERS['products'], 'products.csv')
        return products_df

    def _process_activities(self, activities_data: List[Dict], processor: DataProcessor) -> pd.DataFrame:
        """Process activities data"""
        if not activities_data:
            print("Nenhuma atividade encontrada!")
            return pd.DataFrame()

        # Process in chunks for performance
        chunks = [activities_data[i:i + self.config.CHUNK_SIZE]
                  for i in range(0, len(activities_data), self.config.CHUNK_SIZE)]

        processed_activities = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.config.MAX_WORKERS) as executor:
            chunk_results = list(executor.map(
                lambda chunk: processor.process_entity(chunk, 'activity'),
                chunks
            ))

        for chunk_result in chunk_results:
            processed_activities.extend(chunk_result)

        activities_df = pd.DataFrame(processed_activities)
        activities_df = DataUtils.normalize_df(activities_df)

        # Save activities (potentially in chunks if too large)
        self._save_large_dataset(activities_df, 'activities', 'activities')

        return activities_df

    def _fetch_all_persons(self) -> List[Dict]:
        """Fetch all persons from Pipedrive"""
        print("\nBuscando pessoas...")

        try:
            # Get total count first
            response = self.client.safe_request(f"{self.config.BASE_URL_V1}/persons", {'start': 0, 'limit': 1})
            pagination = response.get('additional_data', {}).get('pagination', {})
            total_count = pagination.get('total_count', 0)

            if total_count:
                print(f"Total de persons a buscar: {total_count}")

            # Fetch in parallel batches
            offsets = list(range(0, total_count if total_count else 50000, self.config.BATCH_SIZE))

            with concurrent.futures.ThreadPoolExecutor(max_workers=self.config.MAX_WORKERS) as executor:
                results = list(executor.map(self._fetch_persons_batch, offsets))

            all_persons = []
            for batch in results:
                if batch:
                    all_persons.extend(batch)

            print(f"Recuperadas {len(all_persons)} pessoas")
            return all_persons

        except Exception as e:
            print(f"Erro na busca paralela: {e}. Usando método sequencial.")
            return self._fetch_persons_sequential()

    def _fetch_persons_batch(self, start_offset: int) -> List[Dict]:
        """Fetch a batch of persons"""
        try:
            params = {'start': start_offset, 'limit': self.config.BATCH_SIZE}
            response = self.client.safe_request(f"{self.config.BASE_URL_V1}/persons", params)
            return response.get('data', [])
        except Exception as e:
            print(f"Erro ao buscar lote de pessoas (start={start_offset}): {e}")
            return []

    def _fetch_persons_sequential(self) -> List[Dict]:
        """Fetch persons sequentially as fallback"""
        all_persons = []
        start = 0

        while True:
            try:
                response = self.client.safe_request(
                    f"{self.config.BASE_URL_V1}/persons",
                    {'start': start, 'limit': self.config.BATCH_SIZE}
                )
                data = response.get('data', [])

                if not data:
                    break

                all_persons.extend(data)
                start += len(data)
                print(f"  {len(all_persons)} persons recuperadas...")

                if not response.get('additional_data', {}).get('pagination', {}).get('more_items_in_collection', False):
                    break

            except Exception as e:
                print(f"Erro ao buscar persons: {e}")
                break

        return all_persons

    def _process_persons_data(self, persons_data: List[Dict], processor: DataProcessor) -> pd.DataFrame:
        """Process persons data in parallel"""
        if not persons_data:
            return pd.DataFrame()

        print(f"\nProcessando {len(persons_data)} pessoas...")

        # Process in chunks
        chunks = [persons_data[i:i + self.config.CHUNK_SIZE]
                  for i in range(0, len(persons_data), self.config.CHUNK_SIZE)]

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.config.MAX_WORKERS) as executor:
            processed_chunks = list(executor.map(
                lambda chunk: processor.process_entity(chunk, 'person'),
                chunks
            ))

        processed_persons = []
        for chunk in processed_chunks:
            processed_persons.extend(chunk)

        persons_df = pd.DataFrame(processed_persons)
        persons_df = DataUtils.normalize_df(persons_df)

        return persons_df

    def _save_large_dataset(self, df: pd.DataFrame, folder_key: str, base_filename: str) -> None:
        """Save large datasets in chunks if necessary"""
        if df.empty:
            return

        chunk_size = 100000
        if len(df) > chunk_size:
            for i in range(0, len(df), chunk_size):
                chunk_end = min(i + chunk_size, len(df))
                chunk_df = df.iloc[i:chunk_end]
                filename = f'{base_filename}_chunk_{i // chunk_size + 1}.csv'
                self.storage.upload_to_storage(chunk_df, self.config.FOLDERS[folder_key], filename)
        else:
            filename = f'{base_filename}.csv'
            self.storage.upload_to_storage(df, self.config.FOLDERS[folder_key], filename)

    def _print_summary(self, start_time: float, pipelines_df: pd.DataFrame, stages_df: pd.DataFrame,
                       leads_df: pd.DataFrame, deals_df: pd.DataFrame, products_df: pd.DataFrame,
                       activities_df: pd.DataFrame, enriched_deals_df: pd.DataFrame) -> None:
        """Print integration summary"""
        total_time = time.time() - start_time
        print("\n===== INTEGRAÇÃO PIPEDRIVE CONCLUÍDA =====")
        print(f"\nTempo total: {total_time / 60:.2f} minutos")
        print("\nResumo dos dados extraídos:")
        print(f"  Pipelines: {len(pipelines_df)} registros")
        print(f"  Stages: {len(stages_df)} registros")
        print(f"  Leads: {len(leads_df)} registros")
        print(f"  Deals: {len(deals_df)} registros")
        print(f"  Produtos: {len(products_df)} registros")
        print(f"  Atividades: {len(activities_df)} registros")
        print(f"  Deals enriquecidos: {len(enriched_deals_df)} registros")


# Extension to FieldMappingService for person-specific mappings
class FieldMappingService(FieldMappingService):
    """Extended field mapping service with person-specific methods"""

    def fetch_person_fields(self) -> Tuple[Dict, Dict]:
        """Fetch field mappings specific to persons"""
        person_field_mapping = {}
        person_dropdown_mapping = {}
        all_value_mappings = {}

        print("\nBuscando campos de persons...")

        try:
            response = self.client.safe_request(f"{self.client.config.BASE_URL_V1}/personFields")
            fields = response.get('data', [])

            for field in fields:
                field_id = str(field.get('id', ''))
                field_key = field.get('key', '')
                field_name = field.get('name', '')

                if not field_key:
                    continue

                name = DataUtils.normalize_column_name(field_name)
                person_field_mapping[field_key] = name
                if field_id and field_id != field_key:
                    person_field_mapping[field_id] = name

                # Process dropdown options
                if field.get('field_type') in ['enum', 'set', 'status', 'varchar_options'] and field.get('options'):
                    option_map = self._process_field_options(field['options'], all_value_mappings)

                    if option_map:
                        person_dropdown_mapping[field_key] = option_map
                        if field_id and field_id != field_key:
                            person_dropdown_mapping[field_id] = option_map

            print(f"Mapeados {len(person_field_mapping)} campos para persons")

        except Exception as e:
            print(f"Erro ao buscar campos de persons: {e}")

        person_dropdown_mapping['_all_values'] = all_value_mappings
        return person_field_mapping, person_dropdown_mapping


# Main execution functions
def run(customer):
    """
    Main function to run Pipedrive integration
    Extracts: Deals, Pipelines, Stages, Products, Activities, and Leads
    """
    integration = PipedriveIntegration(customer)
    integration.run_main_integration()


def run_persons(customer):
    """
    Function to extract Persons (Contacts) data from Pipedrive
    """
    integration = PipedriveIntegration(customer)
    integration.run_persons_integration()


def get_extraction_tasks():
    """
    Get the list of data extraction tasks for Pipedrive.

    Returns:
        list: List of task configurations
    """
    return [
        {
            'task_id': 'run_pipedrive_integration',
            'python_callable': run,
            'description': 'Extrai deals, pipelines, stages, products, activities e leads do Pipedrive'
        },
        {
            'task_id': 'run_persons',
            'python_callable': run_persons,
            'description': 'Extrai contatos (pessoas) do Pipedrive'
        }
    ]
