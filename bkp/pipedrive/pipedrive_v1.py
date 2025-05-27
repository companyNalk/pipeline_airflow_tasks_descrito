"""
Pipedrive module for data extraction functions.
This module contains functions specific to the Pipedrive integration.
"""

from core import gcs


def run(customer):
    import requests, pandas as pd, os, re, unicodedata, time
    from google.cloud import storage
    import pathlib

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

    CAMPOS_IGNORAR = ['id', 'pipeline_id', 'products_count', 'files_count', 'notes_count', 'followers_count',
                      'participants_count', 'stage_order_nr', 'cnpj', 'no_de_apartamento_de_endereco',
                      'numero_da_casa_de_endereco', 'cep_codigo_postal_de_endereco', 'telefone_do_responsavel',
                      'sum', 'imposto', 'deal_id', 'product_id', 'discount', 'quantity', 'item_price',
                      'product_quantity_1', 'product_price_1', 'product_id_1', 'product_quantity_2',
                      'product_price_2', 'product_id_2', 'product_price_3', 'product_id_3']

    # Campos que devem ser tratados como datas (sem hora)
    CAMPOS_DATA = [
        'add_time', 'update_time', 'close_time', 'lost_time', 'first_won_time',
        'won_time', 'lost_time', 'expected_close_date', 'date_created',
        'last_activity_date', 'next_activity_date', 'last_outgoing_mail_time',
        'last_incoming_mail_time', 'data_adicionado', 'data_modificado',
        'data_fechamento', 'data_fechamento_esperado'
    ]

    # Iniciar cliente GCS
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_FILE
    storage_client = storage.Client.from_service_account_json(SERVICE_ACCOUNT_FILE, project=PROJECT_ID)

    def normalize_text(text):
        if not isinstance(text, str): text = str(text)
        text = text.lower()
        return unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('ASCII')

    def normalize_column_name(col_name):
        col_name = normalize_text(col_name)
        col_name = re.sub(r'[^a-z0-9_]', '_', col_name)
        return re.sub(r'_+', '_', col_name).strip('_')

    def safe_request(url, params=None, method='get', retries=3):
        if params is None: params = {}
        params['api_token'] = API_TOKEN

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
                    return {"data": []}

    def detect_date_columns(df):
        """
        Detecta colunas que contêm datas baseado em nomes de colunas comuns
        e padrões de dados.
        """
        date_columns = []

        # Verificar por nomes de colunas conhecidos relacionados a datas
        for col in df.columns:
            col_lower = col.lower()
            if (any(data_field in col_lower for data_field in CAMPOS_DATA) or
                    any(termo in col_lower for termo in ['data', 'date', 'time', 'dt_'])):
                date_columns.append(col)

        # Opcionalmente, também pode verificar padrões de dados nas colunas
        for col in df.columns:
            if col not in date_columns and pd.api.types.is_object_dtype(df[col]):
                # Amostra de valores não nulos para verificar padrões de data
                sample = df[col].dropna().astype(str).head(100).tolist()

                # Verifica se os valores parecem datas (formato ISO, etc.)
                date_patterns = [
                    r'\d{4}-\d{2}-\d{2}',  # YYYY-MM-DD
                    r'\d{2}/\d{2}/\d{4}',  # DD/MM/YYYY ou MM/DD/YYYY
                    r'\d{4}/\d{2}/\d{2}',  # YYYY/MM/DD
                    r'\d{2}\.\d{2}\.\d{4}',  # DD.MM.YYYY
                ]

                # Conta quantos valores correspondem a um padrão de data
                matching = 0
                for value in sample:
                    if any(re.match(pattern, value.strip()) for pattern in date_patterns):
                        matching += 1

                # Se mais de 70% dos valores parecem datas, considera como coluna de data
                if sample and matching / len(sample) > 0.7:
                    date_columns.append(col)

        return date_columns

    def convert_date_columns(df):
        """
        Converte colunas de data para o formato YYYY-MM-DD (sem hora)
        """
        date_columns = detect_date_columns(df)

        for col in date_columns:
            if col in df.columns:
                # Tenta converter para datetime
                try:
                    # Primeiro converte para datetime
                    df[col] = pd.to_datetime(df[col], errors='coerce')

                    # Depois extrai apenas a parte da data (sem hora)
                    df[col] = df[col].dt.strftime('%Y-%m-%d')

                    print(f"Coluna convertida para formato de data: {col}")
                except Exception as e:
                    print(f"Erro ao converter coluna {col} para data: {e}")

        return df

    def upload_to_storage(dataframe, folder, filename):
        if dataframe.empty: return False

        try:
            dataframe.columns = [normalize_column_name(col) for col in dataframe.columns]
            dataframe = dataframe.loc[:, ~dataframe.columns.duplicated()]

            # Converter colunas de data antes de salvar
            dataframe = convert_date_columns(dataframe)

            bucket = storage_client.bucket(BUCKET_NAME)
            blob = bucket.blob(f"{folder}/{filename}")
            csv_data = dataframe.to_csv(index=False, sep=';', encoding='utf-8-sig')
            blob.upload_from_string(csv_data, content_type="text/csv")
            print(f"Upload: {len(dataframe)} registros em {folder}/{filename}")
            return True
        except Exception as e:
            print(f"Erro no upload de {filename}: {e}")
            raise
            # return False

    def normalize_df(df):
        if df.empty: return df
        df.columns = [normalize_column_name(col) for col in df.columns]
        return df.loc[:, ~df.columns.duplicated()]

    def fetch_all_mappings():
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

                # Tratar colisão
                if field.get('edit_flag', True) and name in standard_fields:
                    name = f"{name}_custom_{field_id[:8]}"
                    print(f"Colisão: '{field.get('name')}' -> '{name}'")

                entity_field_map[field_key] = name
                if field_id and field_id != field_key:
                    entity_field_map[field_id] = name

                # Mapear valores para campos dropdown
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

    def save_mappings(field_mappings, dropdown_mappings):
        fields_rows, dropdown_rows = [], []

        for entity_type, mapping in field_mappings.items():
            for field_key, field_name in mapping.items():
                fields_rows.append({'entity_type': entity_type, 'field_key': field_key, 'field_name': field_name})

        for entity_type, dropdown_maps in dropdown_mappings.items():
            if entity_type == '_all_values': continue
            for field_key, options in dropdown_maps.items():
                for option_id, option_label in options.items():
                    dropdown_rows.append({
                        'entity_type': entity_type, 'field_key': field_key,
                        'option_id': option_id, 'option_label': option_label
                    })

        if fields_rows:
            upload_to_storage(pd.DataFrame(fields_rows), FOLDERS['fields_mapping'], 'fields_mapping.csv')
        if dropdown_rows:
            upload_to_storage(pd.DataFrame(dropdown_rows), FOLDERS['fields_mapping'], 'dropdown_mapping.csv')

    def fetch_paginated_data(endpoint, api_version=1, params=None):
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

    def process_entity(items, entity_type, field_mapping, dropdown_mapping):
        processed_items = []
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

            # Adicionar campos personalizados
            processed.update(custom_fields)
            processed_items.append(processed)

        return processed_items

    def fetch_deal_products(deal_ids):
        all_products = []

        for i in range(0, len(deal_ids), 100):
            chunk = deal_ids[i:i + 100]
            products = safe_request(f"{BASE_URL_V2}/deals/products", {'deal_ids': ','.join(map(str, chunk))})
            if 'data' in products:
                all_products.extend(products['data'])

        return all_products

    def verificar_campos_nao_mapeados(df, dropdown_mappings):
        all_values = dropdown_mappings.get('_all_values', {})
        campos_problema = {}

        for col in df.columns:
            if col in CAMPOS_IGNORAR:
                continue

            if pd.api.types.is_object_dtype(df[col]) or pd.api.types.is_numeric_dtype(df[col]):
                valores_unicos = df[col].dropna().unique()
                valores_str = [str(v) for v in valores_unicos if pd.notna(v) and str(v) != 'nan' and str(v) != 'None']

                # Extrai todos os valores individuais de valores compostos (com vírgula)
                valores_simples = []
                valores_compostos = []

                for valor in valores_str:
                    if ',' in valor and all(part.strip().isdigit() for part in valor.split(',')):
                        # É um valor composto numérico
                        valores_compostos.append(valor)
                        # Adiciona cada parte como valor individual
                        valores_simples.extend(part.strip() for part in valor.split(','))
                    elif valor.isdigit():
                        # É um valor simples numérico
                        valores_simples.append(valor)

                # Filtra apenas valores que não têm mapeamento
                valores_sem_mapeamento = [v for v in valores_simples if v not in all_values and v.isdigit()]

                if valores_sem_mapeamento:
                    print(f"Coluna {col}: {len(valores_sem_mapeamento)} valores sem mapeamento")
                    campos_problema[col] = valores_sem_mapeamento

        return campos_problema

    def buscar_mapeamentos_dinamicos(campo_problema, valores_problema):
        valores_problema = [str(v) for v in valores_problema if v is not None and str(v) != 'nan' and str(v) != 'None']
        print(f"Buscando mapeamentos para campo '{campo_problema}' ({len(valores_problema)} valores)")
        mapeamentos = {}

        # Buscar campo correspondente
        deal_fields = safe_request(f"{BASE_URL_V1}/dealFields").get('data', [])
        campo_normalizado = normalize_text(campo_problema)
        campo_id = None

        for field in deal_fields:
            field_name = field.get('name', '')
            if normalize_text(field_name) == campo_normalizado or campo_normalizado in normalize_text(field_name):
                campo_id = field.get('id')
                print(f"Encontrado campo correspondente: {field_name} (ID: {campo_id})")
                break

        # Buscar opções do campo
        if campo_id and field.get('field_type') in ['enum', 'set', 'status', 'varchar_options']:
            try:
                field_details = safe_request(f"{BASE_URL_V1}/dealFields/{campo_id}")

                if 'data' in field_details and 'options' in field_details['data']:
                    options = field_details['data']['options']
                    options_dict = {str(option.get('id', '')): normalize_column_name(option.get('label', ''))
                                    for option in options if option.get('id')}

                    novos_mapeamentos = {v: options_dict[v] for v in valores_problema
                                         if v in options_dict}

                    mapeamentos.update(novos_mapeamentos)
            except Exception as e:
                print(f"Erro ao buscar opções para o campo {campo_id}: {e}")

        # Buscar em outros tipos de entidades
        if len(mapeamentos) < len(valores_problema):
            valores_sem_mapeamento = [v for v in valores_problema if v not in mapeamentos]
            print(f"Ainda faltam {len(valores_sem_mapeamento)} mapeamentos")

            field_endpoints = {
                'deal': 'dealFields',
                'product': 'productFields',
                'person': 'personFields',
                'organization': 'organizationFields'
            }

            for entity_type, endpoint in field_endpoints.items():
                entity_fields = safe_request(f"{BASE_URL_V1}/{endpoint}").get('data', [])

                entity_mapeados = 0
                for field in entity_fields:
                    if field.get('field_type') in ['enum', 'set', 'status', 'varchar_options'] and field.get('options'):
                        options = field.get('options', [])
                        options_dict = {str(option.get('id', '')): normalize_column_name(option.get('label', ''))
                                        for option in options if option.get('id')}

                        for v in list(valores_sem_mapeamento):
                            if v in options_dict:
                                mapeamentos[v] = options_dict[v]
                                valores_sem_mapeamento.remove(v)
                                entity_mapeados += 1

                if entity_mapeados > 0:
                    print(f"  Mapeados {entity_mapeados} valores de {entity_type}")

        print(f"Mapeamento concluído: {len(mapeamentos)}/{len(valores_problema)} valores mapeados")
        return mapeamentos

    def atualizar_mappings_global(dropdown_mappings, novos_mapeamentos, campo=None):
        if '_all_values' not in dropdown_mappings:
            dropdown_mappings['_all_values'] = {}

        dropdown_mappings['_all_values'].update(novos_mapeamentos)

        if campo and 'deal' in dropdown_mappings and novos_mapeamentos:
            print(f"Adicionados {len(novos_mapeamentos)} novos mapeamentos encontrados para {campo}")

        return dropdown_mappings

    def tratar_valores_compostos(valor, all_values):
        """
        Trata valores compostos (ex: "84,89") mapeando cada parte individualmente
        e combinando o resultado
        """
        if not isinstance(valor, str) or ',' not in valor:
            return valor

        # Verifica se é um valor composto de IDs numéricos
        partes = [part.strip() for part in valor.split(',')]
        if not all(part.isdigit() for part in partes):
            return valor

        # Mapeia cada parte usando all_values
        partes_mapeadas = []
        for parte in partes:
            mapeado = all_values.get(parte, parte)
            if mapeado != parte:  # só adiciona se houver mapeamento
                partes_mapeadas.append(mapeado)

        # Se alguma parte foi mapeada, retorna a combinação
        if partes_mapeadas:
            return '_'.join(partes_mapeadas)

        # Se nenhuma parte foi mapeada, retorna o valor original
        return valor

    def apply_mappings_enhanced(df, field_mapping, dropdown_mapping):
        if df.empty:
            return df

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

        # Verificar valores não mapeados
        campos_problema = verificar_campos_nao_mapeados(result_df, dropdown_mapping)

        # Buscar mapeamentos dinamicamente
        for campo, valores in campos_problema.items():
            if valores:
                novos_mapeamentos = buscar_mapeamentos_dinamicos(campo, valores)
                if novos_mapeamentos:
                    dropdown_mapping = atualizar_mappings_global(dropdown_mapping, novos_mapeamentos, campo)

        # Mapear valores
        all_values = dropdown_mapping.get('_all_values', {})
        for col in result_df.columns:
            if pd.api.types.is_object_dtype(result_df[col]) or pd.api.types.is_numeric_dtype(result_df[col]):
                result_df[col] = result_df[col].astype(str)

                # Mapear específico da coluna
                if col in dropdown_mapping and dropdown_mapping[col]:
                    result_df[col] = result_df[col].map(
                        lambda x: dropdown_mapping[col].get(x, x) if pd.notna(x) and x != 'nan' and x != 'None' else x
                    )

                # Mapear valores simples com dicionário global
                result_df[col] = result_df[col].map(
                    lambda x: all_values.get(x, x) if pd.notna(x) and x != 'nan' and x != 'None' else x
                )

                # Tratar valores compostos (com vírgulas) mapeando cada parte
                result_df[col] = result_df[col].map(
                    lambda x: tratar_valores_compostos(x, all_values) if pd.notna(
                        x) and x != 'nan' and x != 'None' else x
                )

        return result_df

    def fix_hash_columns(df, field_mapping):
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

    def diagnose_and_fix_stage_id(deals_df, stages_df):
        stage_columns = ['stage_id', 'etapa', 'stage', 'pipeline_stage_id']
        existing_stage_columns = [col for col in stage_columns if col in deals_df.columns]

        if existing_stage_columns:
            stage_column = existing_stage_columns[0]
            if stage_column != 'stage_id':
                deals_df['stage_id'] = deals_df[stage_column]

        return deals_df

    def enrich_deals(deals_df, pipelines_df, stages_df, products_df):
        enriched = deals_df.copy()

        # Join com pipelines - garantir que os tipos sejam compatíveis
        if 'pipeline_id' in enriched.columns and not pipelines_df.empty:
            try:
                # Converter para mesmo tipo antes do merge
                enriched['pipeline_id'] = enriched['pipeline_id'].astype(str)
                pipelines_df['id'] = pipelines_df['id'].astype(str)

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
                stages_dict = dict(zip(stages_df['id'].astype(str), stages_df['name']))
                enriched['etapa'] = enriched['stage_id'].astype(str).map(stages_dict)

                order_dict = dict(zip(stages_df['id'].astype(str), stages_df['order_nr']))
                enriched['stage_order'] = enriched['stage_id'].astype(str).map(order_dict)

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
        deals_df = apply_mappings_enhanced(deals_df, field_mappings.get('deal', {}), dropdown_mappings)
        deals_df = fix_hash_columns(deals_df, field_mappings.get('deal', {}))
        deals_df = normalize_df(deals_df)
        upload_to_storage(deals_df, FOLDERS['deals'], 'deals.csv')

        # 4. Processar produtos - SOLUÇÃO SEGURA PARA O ERRO COM products_count
        products_data = []
        if 'products_count' in deals_df.columns:
            # Filtrar de forma segura, tratando valores não numéricos
            try:
                # Filtrar apenas IDs com products_count numérico e > 0
                deal_ids_with_products = []
                for idx, row in deals_df.iterrows():
                    try:
                        count = row['products_count']
                        if pd.notna(count):
                            count_val = int(count) if str(count).isdigit() else 0
                            if count_val > 0:
                                deal_ids_with_products.append(row['id'])
                    except (ValueError, TypeError):
                        continue

                if deal_ids_with_products:
                    products_data = fetch_deal_products(deal_ids_with_products)
            except Exception as e:
                print(f"Erro ao processar products_count: {e}")

        products_df = pd.DataFrame(process_entity(
            products_data, 'product',
            field_mappings.get('product', {}),
            dropdown_mappings.get('product', {})
        ))

        # Aplicar mapeamentos e normalizar
        products_df = apply_mappings_enhanced(products_df, field_mappings.get('product', {}), dropdown_mappings)
        products_df = fix_hash_columns(products_df, field_mappings.get('product', {}))
        products_df = normalize_df(products_df)
        upload_to_storage(products_df, FOLDERS['products'], 'products.csv')

        # 5. Verificar e corrigir stage_id antes de enriquecer deals
        deals_df = diagnose_and_fix_stage_id(deals_df, stages_df)

        # 6. Enriquecer deals
        enriched_deals_df = enrich_deals(deals_df, pipelines_df, stages_df, products_df)

        # Aplicar mapeamentos finais e normalizar
        enriched_deals_df = apply_mappings_enhanced(enriched_deals_df, field_mappings.get('deal', {}),
                                                    dropdown_mappings)
        enriched_deals_df = fix_hash_columns(enriched_deals_df, field_mappings.get('deal', {}))
        enriched_deals_df = normalize_df(enriched_deals_df)
        upload_to_storage(enriched_deals_df, FOLDERS['deals_dados_finais'], 'enriched_deals.csv')

        print("\n===== INTEGRAÇÃO PIPEDRIVE CONCLUÍDA =====")

    main()


def run_persons(customer):
    import requests, pandas as pd, os, re, unicodedata, time
    from google.cloud import storage
    from typing import Dict, List, Tuple
    import concurrent.futures
    import pathlib

    SERVICE_ACCOUNT_FILE = pathlib.Path('config', 'setup_automatico.json').as_posix()
    PROJECT_ID, BUCKET_NAME = customer['project_id'], customer['bucket_name']
    API_TOKEN = customer['api_token']
    COMPANY_DOMAIN = customer['company_domain']
    BASE_URL_V1 = customer['base_url_v1'].lower().format(company_domain=COMPANY_DOMAIN)

    FOLDER = 'persons'

    # Configs de paralelismo
    MAX_WORKERS = 8
    CHUNK_SIZE = 200
    BATCH_SIZE = 500  # Tamanho de cada lote para busca

    # Variáveis globais para mapeamentos
    field_mappings = {}
    dropdown_mappings = {}

    # Iniciar cliente GCS
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_FILE
    storage_client = storage.Client.from_service_account_json(SERVICE_ACCOUNT_FILE, project=PROJECT_ID)

    # Funções utilitárias
    def normalize_text(text: str) -> str:
        if not isinstance(text, str): text = str(text)
        return re.sub(r'_+', '_', re.sub(r'[^a-z0-9_]', '_',
                                         unicodedata.normalize('NFKD', text.lower()).encode('ASCII', 'ignore').decode(
                                             'ASCII'))).strip('_')

    def normalize_column_name(col_name: str) -> str:
        return normalize_text(col_name)

    def safe_request(url: str, params: Dict = None, method: str = 'get', retries: int = 3) -> Dict:
        if params is None: params = {}
        params['api_token'] = API_TOKEN

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
                    print(f"Erro na requisição para {url}: {e}")
                    return {"data": []}

    def upload_to_storage(dataframe: pd.DataFrame, filename: str) -> bool:
        if dataframe.empty: return False

        try:
            dataframe.columns = [normalize_column_name(col) for col in dataframe.columns]
            dataframe = dataframe.loc[:, ~dataframe.columns.duplicated()]

            bucket = storage_client.bucket(BUCKET_NAME)
            blob = bucket.blob(f"{FOLDER}/{filename}")
            csv_data = dataframe.to_csv(index=False, sep=';', encoding='utf-8-sig')
            blob.upload_from_string(csv_data, content_type="text/csv")
            print(f"Upload: {len(dataframe)} registros em {FOLDER}/{filename}")
            return True
        except Exception as e:
            print(f"Erro no upload de {filename}: {e}")
            # return False
            raise

    def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty: return df
        df.columns = [normalize_column_name(col) for col in df.columns]
        return df.loc[:, ~df.columns.duplicated()]

    # Funções de mapeamento e busca para persons
    def fetch_person_fields() -> Tuple[Dict, Dict]:
        """Obtém mapeamentos dos campos de person do Pipedrive"""
        global field_mappings, dropdown_mappings

        person_field_mapping = {}
        person_dropdown_mapping = {}
        all_value_mappings = {}
        standard_fields = set()

        # Buscar campos
        for field in safe_request(f"{BASE_URL_V1}/personFields").get('data', []):
            field_id, field_key = str(field.get('id', '')), field.get('key', '')
            if not field_key: continue

            name = normalize_column_name(field.get('name', ''))

            # Tratar colisão com campo padrão
            if field.get('edit_flag', True) and name in standard_fields:
                name = f"{name}_custom_{field_id[:8]}"
                print(f"Colisão: '{field.get('name')}' -> '{name}'")

            # Mapear ID e key para nome normalizado
            person_field_mapping[field_key] = name
            if field_id and field_id != field_key:
                person_field_mapping[field_id] = name

            # Mapear valores para campos de dropdown/enum/set
            if field.get('field_type') in ['enum', 'set', 'status', 'varchar_options'] and field.get('options'):
                option_map = {}
                for option in field['options']:
                    option_id = str(option.get('id', ''))
                    if option_id:
                        option_map[option_id] = normalize_column_name(option.get('label', ''))
                        all_value_mappings[option_id] = option_map[option_id]

                if option_map:
                    person_dropdown_mapping[field_key] = option_map
                    if field_id and field_id != field_key:
                        person_dropdown_mapping[field_id] = option_map

        print(f"Mapeados {len(person_field_mapping)} campos para persons")

        # Salvar em variáveis globais
        field_mappings = person_field_mapping
        person_dropdown_mapping['_all_values'] = all_value_mappings
        dropdown_mappings = person_dropdown_mapping

        return person_field_mapping, person_dropdown_mapping

    def fetch_persons_batch(start_offset: int) -> List[Dict]:
        """Busca um lote de pessoas a partir de um offset específico"""
        try:
            url = f"{BASE_URL_V1}/persons"
            params = {
                'start': start_offset,
                'limit': BATCH_SIZE
            }

            response = safe_request(url, params)
            return response.get('data', [])
        except Exception as e:
            print(f"Erro ao buscar lote de pessoas (start={start_offset}): {e}")
            return []

    def fetch_all_persons() -> List[Dict]:
        """Busca todas as pessoas usando paralelismo dinâmico"""
        # Primeiro, fazer uma única requisição para obter metadados de paginação
        url = f"{BASE_URL_V1}/persons"
        params = {'start': 0, 'limit': 1}

        try:
            response = safe_request(url, params)
            pagination = response.get('additional_data', {}).get('pagination', {})
            total_count = pagination.get('total_count')

            if not total_count:
                # Se não conseguir obter contagem, iniciar com estimativa inicial e ajustar dinamicamente
                print("Não foi possível determinar contagem total. Usando abordagem dinâmica.")
                return fetch_persons_dynamic()

            print(f"Total de persons: {total_count}")

            # Calcular os offsets para cada thread
            offsets = list(range(0, total_count, BATCH_SIZE))

            # Executar as buscas em paralelo
            all_persons = []
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                # Mapear a função fetch_persons_batch para cada offset em paralelo
                results = list(executor.map(fetch_persons_batch, offsets))

                # Processar resultados
                for i, batch in enumerate(results):
                    all_persons.extend(batch)
                    print(f"Recebido lote {i + 1}/{len(offsets)} - {len(batch)} registros (total: {len(all_persons)})")

            return all_persons

        except Exception as e:
            print(f"Erro na configuração de busca paralela: {e}. Usando abordagem dinâmica.")
            return fetch_persons_dynamic()

    def fetch_persons_dynamic() -> List[Dict]:
        """Busca pessoas usando uma abordagem dinâmica sem conhecer o total antecipadamente"""
        all_persons = []
        offset = 0
        batch_size = BATCH_SIZE
        has_more = True
        batch_count = 0

        # Criar um pool de workers
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Enquanto houver mais registros a buscar
            while has_more:
                batch_count += 1
                futures = []

                # Distribuir múltiplos batches para processamento paralelo
                for i in range(MAX_WORKERS):
                    current_offset = offset + (i * batch_size)
                    futures.append(executor.submit(fetch_persons_batch, current_offset))

                # Coletar resultados deste grupo de batches
                new_batch_total = 0
                for future in concurrent.futures.as_completed(futures):
                    batch_result = future.result()
                    if batch_result:
                        all_persons.extend(batch_result)
                        new_batch_total += len(batch_result)

                # Verificar se todos os batches estavam cheios (indicando que pode haver mais)
                if new_batch_total < batch_size * MAX_WORKERS:
                    has_more = False

                print(f"Rodada {batch_count}: Recuperados {new_batch_total} registros (total: {len(all_persons)})")

                # Ajustar offset para o próximo grupo de batches
                offset += batch_size * MAX_WORKERS

                # Limitador de segurança para evitar loops infinitos
                if batch_count > 500:  # Um número muito alto que nunca seria atingido em circunstâncias normais
                    print("Limite de segurança atingido. Interrompendo busca.")
                    break

        print(f"Total de {len(all_persons)} registros recuperados com abordagem dinâmica")
        return all_persons

    def fetch_persons_sequential() -> List[Dict]:
        """Busca pessoas de forma sequencial (método alternativo)"""
        all_persons = []
        start = 0

        while True:
            try:
                url = f"{BASE_URL_V1}/persons"
                params = {
                    'start': start,
                    'limit': BATCH_SIZE
                }

                response = safe_request(url, params)
                data = response.get('data', [])

                if not data:
                    break

                all_persons.extend(data)
                start += len(data)

                print(f"Recuperados {len(all_persons)} persons (sequencial)...")

                # Verificar se há mais itens para buscar
                if not response.get('additional_data', {}).get('pagination', {}).get('more_items_in_collection', False):
                    break

            except Exception as e:
                print(f"Erro ao buscar persons: {e}")
                time.sleep(5)  # Esperar um pouco antes de tentar o próximo lote
                # Avançar mesmo em caso de erro para evitar loop infinito
                start += BATCH_SIZE
                if start > 100000:  # Limite de segurança aumentado
                    break

        return all_persons

    def process_person(items: List[Dict]) -> List[Dict]:
        """Processa dados de pessoas do Pipedrive"""
        processed_items = []

        # Mapeamentos padrão
        std_value_maps = {
            'visible_to': {'1': 'proprietario_do_item', '3': 'todos_os_usuarios', '7': 'toda_a_empresa'},
        }
        relation_fields = {'owner_id': 'proprietario'}
        all_values = dropdown_mappings.get('_all_values', {})

        for item in items:
            processed = item.copy()
            custom_fields = {}

            # Processar campos personalizados
            if "custom_fields" in processed and isinstance(processed["custom_fields"], dict):
                for field_id, value in processed["custom_fields"].items():
                    field_name = field_mappings.get(field_id, field_id)

                    # Mapear valores de dropdown
                    if field_id in dropdown_mappings and value is not None:
                        value_map = dropdown_mappings[field_id]
                        if isinstance(value, list):
                            value = ','.join([value_map.get(str(v), str(v)) for v in value]) or ''
                        else:
                            value = value_map.get(str(value), all_values.get(str(value), value))

                    custom_fields[field_name] = value
                processed.pop("custom_fields", None)

            # Processar campos relacionais
            for old_field, new_field in relation_fields.items():
                if old_field in processed and processed[old_field] and isinstance(processed[old_field], dict):
                    processed[new_field] = processed[old_field].get('name', '')
                    processed.pop(old_field, None)

            # Mapear valores padrão
            for field, value_map in std_value_maps.items():
                if field in processed and processed[field] is not None:
                    processed[field] = value_map.get(str(processed[field]), processed[field])

            # Mapear outros valores conhecidos
            for field, value in list(processed.items()):
                if isinstance(value, str) and value in all_values:
                    processed[field] = all_values[value]

            # Processar emails
            if 'email' in processed and isinstance(processed['email'], list):
                for i, email in enumerate(processed['email'][:5], 1):
                    for k, v in email.items():
                        processed[f'email_{k}_{i}'] = v
                processed.pop('email', None)

            # Processar phones
            if 'phone' in processed and isinstance(processed['phone'], list):
                for i, phone in enumerate(processed['phone'][:5], 1):
                    for k, v in phone.items():
                        processed[f'phone_{k}_{i}'] = v
                processed.pop('phone', None)

            # Processar campos adicionais que podem existir em diferentes formatos
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

            # Adicionar campos personalizados
            processed.update(custom_fields)
            processed_items.append(processed)

        return processed_items

    def process_chunk(items: List[Dict]) -> List[Dict]:
        """Processa um chunk de pessoas"""
        return process_person(items)

    def fix_channel_fields(df: pd.DataFrame) -> pd.DataFrame:
        """Corrige inversões em campos de canais"""
        if df.empty: return df

        # Funções auxiliares para verificar tipo de dados
        def is_likely_id(value):
            if pd.isna(value): return False
            if isinstance(value, (int, float)): return True
            if isinstance(value, str): return bool(re.match(r'^\d+\.?\d*$', value))
            return False

        def is_likely_text(value):
            if pd.isna(value): return False
            if isinstance(value, str) and not re.match(r'^\d+\.?\d*$', value): return True
            return False

        # Verificar e corrigir pares invertidos
        for name_col, id_col in [('canal_de_origem', 'id_do_canal_de_origem'),
                                 ('origem', 'id_origem'), ('canal', 'canal_id')]:
            if name_col in df.columns and id_col in df.columns:
                try:
                    name_col_numeric_ratio = df[name_col].apply(is_likely_id).mean()
                    id_col_text_ratio = df[id_col].apply(is_likely_text).mean()

                    if name_col_numeric_ratio > 0.5 and id_col_text_ratio > 0.5:
                        print(f"Detectada inversão entre {name_col} e {id_col}. Corrigindo...")
                        df[name_col], df[id_col] = df[id_col].copy(), df[name_col].copy()
                except Exception as e:
                    print(f"Erro ao verificar inversão de {name_col}/{id_col}: {e}")

        return df

    def fix_hash_columns(df: pd.DataFrame) -> pd.DataFrame:
        """Renomeia colunas com hash"""
        hash_cols = [col for col in df.columns if re.match(r'^[0-9a-f]{40}$', col)]

        if hash_cols:
            cols_to_rename = {}
            existing = set(df.columns)

            for col in hash_cols:
                if col in field_mappings:
                    new_name = field_mappings[col]
                    if new_name in existing and new_name != col:
                        new_name = f"{new_name}_custom_{col[:6]}"
                    cols_to_rename[col] = new_name
                else:
                    cols_to_rename[col] = f"campo_{col[:6]}"

            if cols_to_rename:
                df.rename(columns=cols_to_rename, inplace=True)

        return df

    def apply_mappings(df: pd.DataFrame) -> pd.DataFrame:
        """Aplica mapeamentos ao DataFrame"""
        if df.empty: return df
        result_df = df.copy()

        # Renomear colunas
        rename_cols = {}
        renamed = set()

        for col in result_df.columns:
            if col in field_mappings:
                new_name = field_mappings[col]
                if new_name in renamed:
                    new_name = f"{new_name}_alt_{col[:6]}"
                rename_cols[col] = new_name
                renamed.add(new_name)

        if rename_cols:
            result_df.rename(columns=rename_cols, inplace=True)

        # Mapear valores de dropdown
        all_values = dropdown_mappings.get('_all_values', {})
        for col in result_df.columns:
            if pd.api.types.is_object_dtype(result_df[col]):
                result_df[col] = result_df[col].astype(str)

                # Mapear com dicionário específico da coluna e global de valores
                if col in dropdown_mappings and dropdown_mappings[col]:
                    result_df[col] = result_df[col].map(
                        lambda x: dropdown_mappings[col].get(x, all_values.get(x, x))
                        if pd.notna(x) and x != 'nan' else x
                    )

        return result_df

    def process_persons_parallel() -> pd.DataFrame:
        """Processa persons em paralelo e retorna um DataFrame"""
        global field_mappings, dropdown_mappings

        print("\nBuscando pessoas...")
        start_time = time.time()

        # Obter mapeamentos de campos
        field_mappings, dropdown_mappings = fetch_person_fields()

        # Buscar pessoas com método dinâmico
        all_persons = fetch_all_persons()

        print(f"Recuperadas {len(all_persons)} pessoas em {time.time() - start_time:.2f} segundos")

        if not all_persons:
            print("Nenhuma pessoa encontrada!")
            return pd.DataFrame()

        print("Processando dados de pessoas em paralelo...")
        # Processar em chunks paralelos
        chunks = [all_persons[i:i + CHUNK_SIZE] for i in range(0, len(all_persons), CHUNK_SIZE)]

        # Processar chunks em paralelo
        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            processed_chunks = list(executor.map(process_chunk, chunks))

        # Combinar resultados processados
        processed_persons = []
        for chunk in processed_chunks:
            processed_persons.extend(chunk)

        print(f"Processadas {len(processed_persons)} pessoas em {time.time() - start_time:.2f} segundos")

        # Converter para DataFrame e aplicar mapeamentos
        persons_df = pd.DataFrame(processed_persons)

        print("Aplicando mapeamentos e normalizando dados...")
        persons_df = apply_mappings(persons_df)
        persons_df = fix_hash_columns(persons_df)
        persons_df = normalize_df(persons_df)
        persons_df = fix_channel_fields(persons_df)

        return persons_df

    def main():
        """Executa o processamento completo de persons"""
        try:
            start_time = time.time()

            # Processar pessoas em paralelo
            persons_df = process_persons_parallel()

            if not persons_df.empty:
                print(f"Salvando {len(persons_df)} pessoas processadas...")
                # Salvar em chunks se necessário
                chunk_size = 100000
                if len(persons_df) > chunk_size:
                    for i in range(0, len(persons_df), chunk_size):
                        chunk_end = min(i + chunk_size, len(persons_df))
                        chunk_df = persons_df.iloc[i:chunk_end]
                        filename = f'persons_chunk_{i // chunk_size + 1}.csv'
                        upload_to_storage(chunk_df, filename)
                        print(f"Salvo chunk {i // chunk_size + 1} de pessoas")
                else:
                    upload_to_storage(persons_df, 'persons.csv')

                print(f"Processadas e salvas {len(persons_df)} pessoas no total")

            print(f"\nProcessamento de pessoas concluído em {(time.time() - start_time) / 60:.2f} minutos")
            return persons_df

        except Exception as e:
            print(f"\nErro durante o processamento de pessoas: {e}")
            import traceback
            traceback.print_exc()
            raise
            # return pd.DataFrame()

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
        },
        {
            'task_id': 'run_persons',
            'python_callable': run_persons
        }
    ]
