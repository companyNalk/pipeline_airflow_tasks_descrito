def run(customer):
    import asyncio
    import json
    import time
    import traceback
    from datetime import datetime, timedelta
    from typing import List, Dict
    import pathlib
    import uuid

    import aiohttp
    import pandas as pd

    # Configuration
    HISTORICAL_START_DATE = customer['start_date']
    API_ACCESS_TOKEN = customer['access_token']
    API_ACCOUNT_IDS = customer['accounts_ids'].split(',')
    CONTA_BM = customer['conta_bm']
    print('HISTORICAL_START_DATE', HISTORICAL_START_DATE)
    print('API_ACCESS_TOKEN', API_ACCESS_TOKEN)
    print('API_ACCOUNT_IDS', API_ACCOUNT_IDS)
    print('CONTA_BM', CONTA_BM)

    SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'setup_automatico.json').as_posix()

    # Create a temporary directory for storing files
    TEMP_DIR = Path(tempfile.gettempdir()) / f"facebook_ads_{uuid.uuid4().hex}"
    TEMP_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Created temporary directory: {TEMP_DIR}")

    FIELDS = [
        'account_name', 'account_id', 'ad_id', 'ad_name', 'adset_name',
        'campaign_name', 'clicks', 'cost_per_action_type', 'ctr',
        'date_start', 'date_stop', 'frequency', 'impressions',
        'objective', 'reach', 'spend', 'actions', 'action_values'  # Added action_values
    ]

    ACTION_FIELDS = [
        'lead', 'offsite_conversion.fb_pixel_lead', 'onsite_conversion.lead_grouped',
        'onsite_conversion.messaging_conversation_started_7d', 'post_engagement', 'post_reaction'
    ]

    FIXED_COLUMNS = [
        'date', 'account_name', 'account_id', 'ad_id', 'ad_name', 'adset_name',
        'campaign', 'clicks', 'ctr', 'frequency', 'impressions', 'objective',
        'reach', 'totalcost', 'age', 'gender',
        'actions_lead', 'actions_offsite_conversion_fb_pixel_lead',
        'actions_onsite_conversion_lead_grouped',
        'actions_onsite_conversion_messaging_conversation_started_7d',
        'actions_post_engagement', 'actions_post_reaction',
        'cost_per_action_type_lead', 'cost_per_action_type_offsite_conversion_fb_pixel_lead',
        'cost_per_action_type_onsite_conversion_lead_grouped',
        'cost_per_action_type_onsite_conversion_messaging_conversation_started_7d',
        'cost_per_action_type_post_engagement', 'cost_per_action_type_post_reaction',
        'custom_conversion_action_name', 'custom_conversion_action_count', 'custom_conversion_action_value',
        'instagram_permalink_url', 'link', 'object_url'
    ]

    class FacebookAdsCollector:
        def __init__(self, access_token: str, account_ids: List[str], clickhouse_params: Dict):
            self.access_token = access_token
            self.account_ids = account_ids
            self.base_url = "https://graph.facebook.com/v19.0"
            self.session = None
            self.clickhouse_params = clickhouse_params
            self.temp_dir = TEMP_DIR
            self.semaphore = asyncio.Semaphore(500)  # Paralelismo de 500
            self.processed_files = []
            self.all_data_frames = []  # Store all DataFrames for combined upload
            self.clickhouse_client = clickhouse.get_client()

            # Cache para URLs dos criativos (evita buscar o mesmo ad_id múltiplas vezes)
            self.creative_urls_cache = {}
            self.custom_conversion_names = {}  # Cache para nomes das conversões personalizadas

        async def get_session(self):
            if not self.session:
                self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=300))
            return self.session

        async def close_session(self):
            if self.session:
                await self.session.close()
                self.session = None

        async def get_custom_conversion_names(self) -> Dict[str, str]:
            """Consulta os nomes das conversões personalizadas uma única vez e armazena em cache"""
            print("  ├─ Carregando nomes das conversões personalizadas...")
            session = await self.get_session()
            names = {}

            for account_id in self.account_ids:
                clean_account_id = account_id.replace('act_', '')
                url = f"{self.base_url}/act_{clean_account_id}/customconversions"
                params = {
                    "access_token": self.access_token,
                    "fields": "id,name",
                    "limit": 200
                }

                try:
                    async with self.semaphore:
                        async with session.get(url, params=params) as response:
                            if response.status == 200:
                                data = await response.json()
                                for item in data.get("data", []):
                                    names[item["id"]] = item["name"]
                                print(f"     └─ Account {account_id}: {len(data.get('data', []))} conversões personalizadas encontradas")
                            else:
                                print(f"     └─ Account {account_id}: Erro {response.status} ao buscar conversões personalizadas")
                except Exception as e:
                    print(f"     └─ Account {account_id}: Erro ao buscar conversões personalizadas: {e}")

            print(f"  └─ Total de conversões personalizadas carregadas: {len(names)}")
            return names

        async def validate_credentials(self) -> bool:
            """
            Valida as credenciais antes de iniciar a coleta
            """
            print("\n" + "="*60)
            print("VALIDANDO CREDENCIAIS")
            print("="*60)

            session = await self.get_session()

            # 1. Testar token básico
            url = f"{self.base_url}/me"
            params = {"access_token": self.access_token, "fields": "id,name"}

            async with session.get(url, params=params) as response:
                if response.status != 200:
                    error_data = await response.json()
                    msg = f"[ERRO] Token inválido: {error_data}"
                    raise Exception(msg)

                user_data = await response.json()
                print(f"[OK] Token válido para: {user_data.get('name', 'N/A')}")

            # 2. Testar acesso às contas
            valid_accounts = 0
            for account_id in self.account_ids:
                url = f"{self.base_url}/{account_id}"
                params = {"access_token": self.access_token, "fields": "account_id,name"}

                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        account_data = await response.json()
                        print(f"[OK] Acesso à conta: {account_data.get('name', account_id)}")
                        valid_accounts += 1
                    else:
                        error_data = await response.json()
                        msg = f"[ERRO] Sem acesso à conta {account_id}: {error_data}"
                        print(msg)
                        raise Exception(msg)

            # 3. Testar insights (SEM object_url)
            if valid_accounts > 0:
                test_date = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
                url = f"{self.base_url}/{self.account_ids[0]}/insights"
                params = {
                    "access_token": self.access_token,
                    "fields": "spend,impressions,ad_id",  # Campos válidos
                    "time_range": json.dumps({"since": test_date, "until": test_date}),
                    "level": "ad",
                    "limit": 1
                }

                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        print(f"[OK] Acesso a insights confirmado")
                        data = await response.json()
                        test_ads = data.get("data", [])

                        # 4. Testar busca de criativos se houver anúncios
                        if test_ads:
                            test_ad_id = test_ads[0].get("ad_id")
                            if test_ad_id:
                                creative_url = await self.get_ad_creative_urls(test_ad_id)
                                if creative_url:
                                    print(f"[OK] Acesso a criativos confirmado: {creative_url}")
                                else:
                                    print(f"[INFO] Nenhuma URL encontrada no criativo de teste")
                    else:
                        error_data = await response.json()
                        msg = f"[ERRO] Sem acesso a insights: {error_data}"
                        print(msg)
                        raise Exception(msg)

            print("="*60)
            if valid_accounts == len(self.account_ids):
                print("[SUCESSO] Todas as validações passaram!")
                return True
            else:
                print(f"[ERRO] Apenas {valid_accounts}/{len(self.account_ids)} contas válidas")
                return False

        async def get_ad_creative_urls(self, ad_id: str) -> Dict[str, str]:
            """
            Busca URLs dos criativos do anúncio
            Retorna um dict com: {'link': '...', 'instagram_permalink_url': '...', 'object_url': '...'}
            """
            if ad_id in self.creative_urls_cache:
                return self.creative_urls_cache[ad_id]

            session = await self.get_session()
            url = f"{self.base_url}/{ad_id}"
            params = {
                "access_token": self.access_token,
                "fields": "creative{object_story_spec,call_to_action,instagram_permalink_url}"
            }

            result = {
                'link': None,
                'instagram_permalink_url': None,
                'object_url': None
            }

            try:
                async with self.semaphore:
                    async with session.get(url, params=params) as response:
                        if response.status == 200:
                            data = await response.json()
                            creative = data.get("creative", {})

                            # 1. Instagram permalink
                            result['instagram_permalink_url'] = creative.get("instagram_permalink_url")

                            # 2. Object story spec (para link)
                            object_story = creative.get("object_story_spec", {})
                            link_data = object_story.get("link_data", {})
                            if link_data.get("link"):
                                result['link'] = link_data.get("link")
                                result['object_url'] = link_data.get("link")  # object_url = link principal

                            # 3. Call to action (alternativa)
                            if not result['link']:
                                cta = creative.get("call_to_action", {})
                                cta_value = cta.get("value", {})
                                if cta_value.get("link"):
                                    result['link'] = cta_value.get("link")
                                    result['object_url'] = cta_value.get("link")

                        elif response.status == 429:
                            await asyncio.sleep(int(response.headers.get("Retry-After", "60")))
                            return await self.get_ad_creative_urls(ad_id)

            except Exception as e:
                print(f"    └─ Erro ao buscar creative do ad {ad_id}: {e}")

            # Cache do resultado
            self.creative_urls_cache[ad_id] = result
            return result

        async def get_ad_details_with_demographics(self, ad_id: str, date_str: str) -> List[Dict]:
            """Get detailed metrics for ad_id broken down by age and gender"""
            session = await self.get_session()
            url = f"{self.base_url}/{ad_id}/insights"
            params = {
                "access_token": self.access_token,
                "fields": ",".join(FIELDS),  # SEM object_url
                "time_range": json.dumps({"since": date_str, "until": date_str}),
                "level": "ad",
                "breakdowns": "age,gender",
                "limit": 1000
            }

            all_data = []
            try:
                async with self.semaphore:
                    async with session.get(url, params=params) as response:
                        if response.status == 200:
                            data = await response.json()
                            results = data.get("data", [])
                            all_data.extend(results)
                            print(f"    └─ Ad {ad_id}: Got {len(results)} demographic breakdowns", end="")

                            # Handle pagination
                            page = 2
                            while "paging" in data and "next" in data["paging"]:
                                print(f", page {page}", end="")
                                await asyncio.sleep(0.5)
                                async with session.get(data["paging"]["next"]) as next_response:
                                    if next_response.status == 200:
                                        data = await next_response.json()
                                        all_data.extend(data.get("data", []))
                                        page += 1
                                    else:
                                        break
                            print()

                        elif response.status == 429:
                            print(f"    └─ Ad {ad_id}: Rate limited. Sleeping...")
                            await asyncio.sleep(int(response.headers.get("Retry-After", "60")))
                            return await self.get_ad_details_with_demographics(ad_id, date_str)

            except Exception as e:
                print(f"    └─ Ad {ad_id}: Error - {e}")

            return all_data

        async def get_active_ads(self, account_id: str, date_str: str) -> List[str]:
            """Get all ad_ids that had activity on the date"""
            print(f"  ├─ Step 1: Getting active ads for {date_str}")
            session = await self.get_session()
            url = f"{self.base_url}/{account_id}/insights"
            params = {
                "access_token": self.access_token,
                "fields": "ad_id",
                "time_range": json.dumps({"since": date_str, "until": date_str}),
                "level": "ad",
                "filtering": json.dumps([{"field": "spend", "operator": "GREATER_THAN", "value": "0"}]),
                "limit": 500
            }

            ad_ids = []
            try:
                async with self.semaphore:
                    async with session.get(url, params=params) as response:
                        if response.status == 200:
                            data = await response.json()
                            raw_data = data.get("data", [])

                            for item in raw_data:
                                ad_id = item.get("ad_id")
                                if ad_id:
                                    ad_ids.append(ad_id)
                            print(f"     └─ Page 1: Found {len(raw_data)} ads")

                            # Handle pagination
                            page = 2
                            while "paging" in data and "next" in data["paging"]:
                                await asyncio.sleep(1)
                                async with session.get(data["paging"]["next"]) as next_response:
                                    if next_response.status == 200:
                                        data = await next_response.json()
                                        raw_data = data.get("data", [])
                                        for item in raw_data:
                                            ad_id = item.get("ad_id")
                                            if ad_id:
                                                ad_ids.append(ad_id)
                                        print(f"     └─ Page {page}: Found {len(raw_data)} ads")
                                        page += 1
                                    else:
                                        break

            except Exception as e:
                print(f"    └─ Error: {e}")
                print(f"       Full traceback: {traceback.format_exc()}")

            unique_ads = list(set(ad_ids))
            print(f"  └─ Total unique active ads: {len(unique_ads)}")
            return unique_ads

        async def process_date(self, date_str: str) -> pd.DataFrame:
            """Process a single date by collecting data for all active ads with demographics"""
            print(f"\n{'=' * 60}")
            print(f"Processing date: {date_str}")
            print(f"{'=' * 60}")

            all_data = []

            for account_id in self.account_ids:
                print(f"\nAccount: {account_id}")

                # Step 1: Get all active ads
                ad_ids = await self.get_active_ads(account_id, date_str)

                # Step 2: Get details for each ad with age/gender breakdown
                print(f"  ├─ Step 2: Fetching demographic details for {len(ad_ids)} ads")
                start_time = time.time()

                tasks = [self.get_ad_details_with_demographics(ad_id, date_str) for ad_id in ad_ids]
                results = await asyncio.gather(*tasks)

                # Count how many ads have data
                ads_with_data = sum(1 for result_list in results if result_list)

                # Flatten results
                total_rows = 0
                for result_list in results:
                    all_data.extend(result_list)
                    total_rows += len(result_list)

                elapsed = time.time() - start_time
                print(f"  └─ Step 2 Complete: {ads_with_data}/{len(ad_ids)} ads with data")
                print(f"     └─ Total demographic rows: {total_rows}")
                print(f"     └─ Time elapsed: {elapsed:.1f}s")

            print(f"\n{'=' * 60}")
            print(f"Processing aggregation for {date_str}")
            print(f"Total raw rows to process: {len(all_data)}")

            df = await self.transform_data(all_data, date_str)
            print(f"Final aggregated rows: {len(df)}")
            print(f"{'=' * 60}")

            return df

        async def transform_data(self, data: List[Dict], date_str: str) -> pd.DataFrame:
            """Transform raw data to normalized DataFrame with fixed columns"""
            if not data:
                print("WARNING: No data to process")
                # Return empty DataFrame with fixed columns
                return pd.DataFrame(columns=FIXED_COLUMNS)

            print(f"  ├─ Transforming {len(data)} raw rows")

            # Coletar todos os ad_ids únicos para buscar URLs dos criativos
            unique_ad_ids = list(set(item.get("ad_id") for item in data if item.get("ad_id")))
            print(f"  ├─ Fetching creative URLs for {len(unique_ad_ids)} unique ads...")

            # Buscar URLs dos criativos em paralelo
            creative_tasks = [self.get_ad_creative_urls(ad_id) for ad_id in unique_ad_ids]
            creative_results = await asyncio.gather(*creative_tasks)

            # Criar mapeamento ad_id -> URLs
            ad_urls_map = dict(zip(unique_ad_ids, creative_results))

            # Transform all raw data
            rows = []
            for item in data:
                ad_id = item.get("ad_id")

                # Obter URLs do mapeamento
                ad_urls = ad_urls_map.get(ad_id, {})

                # Initialize custom conversion fields
                custom_conversion_name = None
                custom_conversion_count = 0
                custom_conversion_value = 0

                # Extract custom conversions - only the first one found
                for action in item.get("actions", []):
                    action_type = action.get("action_type", "")
                    if action_type.startswith("offsite_conversion.custom."):
                        custom_id = action_type.split(".")[-1]
                        custom_conversion_name = self.custom_conversion_names.get(custom_id, f"custom_{custom_id}")
                        custom_conversion_count = float(action.get("value", 0))

                        # Try to find corresponding value
                        for val in item.get("action_values", []):
                            if val.get("action_type") == action_type:
                                custom_conversion_value = float(val.get("value", 0))
                                break
                        break  # Only take the first custom conversion found

                # Base data
                row = {
                    "date": date_str,
                    "account_name": item.get("account_name"),
                    "account_id": item.get("account_id"),
                    "ad_id": ad_id,
                    "ad_name": item.get("ad_name"),
                    "adset_name": item.get("adset_name"),
                    "campaign": item.get("campaign_name"),
                    "clicks": float(item.get("clicks", 0)) if item.get("clicks") else 0,
                    "ctr": float(item.get("ctr", 0)) if item.get("ctr") else 0,
                    "frequency": float(item.get("frequency", 0)) if item.get("frequency") else 0,
                    "impressions": int(item.get("impressions", 0)) if item.get("impressions") else 0,
                    "objective": item.get("objective"),
                    "reach": int(item.get("reach", 0)) if item.get("reach") else 0,
                    "totalcost": float(item.get("spend", 0)) if item.get("spend") else 0,
                    "age": item.get("age"),
                    "gender": item.get("gender"),
                    "custom_conversion_action_name": custom_conversion_name,
                    "custom_conversion_action_count": custom_conversion_count,
                    "custom_conversion_action_value": custom_conversion_value,
                    # URLs dos criativos
                    "instagram_permalink_url": ad_urls.get("instagram_permalink_url"),
                    "link": ad_urls.get("link"),
                    "object_url": ad_urls.get("object_url")
                }

                # Extract actions
                for action_type in ACTION_FIELDS:
                    col_name = f"actions_{action_type.replace('.', '_')}"
                    row[col_name] = self._extract_value(item.get("actions", []), action_type)

                    cost_col = f"cost_per_action_type_{action_type.replace('.', '_')}"
                    row[cost_col] = self._extract_value(item.get("cost_per_action_type", []), action_type)

                rows.append(row)

            # Create DataFrame with fixed columns
            df = pd.DataFrame(rows)

            # Apply aggregation ONLY for ad_id + date + age + gender
            # This keeps the granularity of demographic data
            if len(df) > 0:
                agg_rules = {
                    "clicks": "sum",
                    "impressions": "sum",
                    "reach": "sum",
                    "totalcost": "sum",
                    "frequency": "mean",
                    "ctr": "mean",
                    "account_name": "first",
                    "ad_name": "first",
                    "adset_name": "first",
                    "campaign": "first",
                    "objective": "first",
                    "custom_conversion_action_name": "first",
                    "custom_conversion_action_count": "sum",
                    "custom_conversion_action_value": "sum",
                    "instagram_permalink_url": "first",
                    "link": "first",
                    "object_url": "first"
                }

                # Add aggregation rules for action fields
                for action_type in ACTION_FIELDS:
                    col_name = f"actions_{action_type.replace('.', '_')}"
                    cost_col = f"cost_per_action_type_{action_type.replace('.', '_')}"
                    if col_name in df.columns:
                        agg_rules[col_name] = "sum"
                    if cost_col in df.columns:
                        agg_rules[cost_col] = "mean"

                # Group by ad_id + date + age + gender to maintain full demographic granularity
                df = df.groupby(['ad_id', 'date', 'age', 'gender']).agg(agg_rules).reset_index()

            # Normalize columns to match fixed header
            df = self._ensure_fixed_columns(df)

            return df

        def _extract_value(self, items: List[Dict], action_type: str) -> float:
            """Extract value from items list"""
            for item in items:
                if item.get("action_type") == action_type:
                    try:
                        return float(item.get("value", 0))
                    except:
                        return 0
            return 0

        def _ensure_fixed_columns(self, df: pd.DataFrame) -> pd.DataFrame:
            """Ensure DataFrame has exactly the fixed columns in the correct order"""
            # Add missing columns
            for col in FIXED_COLUMNS:
                if col not in df.columns:
                    df[col] = None

            # Normalize column types
            int_cols = ["clicks", "impressions", "reach", "custom_conversion_action_count"] + [f"actions_{a.replace('.', '_')}" for a in ACTION_FIELDS]
            float_cols = ["frequency", "ctr", "totalcost", "custom_conversion_action_value"] + [f"cost_per_action_type_{a.replace('.', '_')}" for a in
                                                              ACTION_FIELDS]

            for col in int_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('int64')

            for col in float_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0).astype('float64')

            # Reorder columns
            df = df[FIXED_COLUMNS]

            return df

        async def save_to_temp_file(self, df: pd.DataFrame, date_str: str) -> str:
            """Save data to a temporary file with fixed columns"""
            if df.empty:
                print(f"WARNING: No data to save for {date_str}")
                # Still save an empty file with fixed columns
                df = pd.DataFrame(columns=FIXED_COLUMNS)

            # Ensure we have exactly the fixed columns
            df = self._ensure_fixed_columns(df)

            # Use facebook_ads_{date} format for filename
            filename = f"facebook_ads_{date_str}.csv"
            filepath = self.temp_dir / filename

            print(f"Saving to temporary file: {filepath}")
            df.to_csv(filepath, index=False)
            self.processed_files.append(filepath)
            print(f"Successfully saved {len(df)} records to {filepath}")

            return str(filepath)

        def upload_all_data_to_clickhouse(self):
            """Upload all collected data to ClickHouse in a single operation"""
            if not self.all_data_frames:
                print("WARNING: No data to upload to ClickHouse")
                return False

            # Combine all DataFrames into one
            combined_df = pd.concat(self.all_data_frames, ignore_index=True)
            if combined_df.empty:
                print("WARNING: Combined DataFrame is empty, nothing to upload")
                return False

            combined_df['conta_bm'] = CONTA_BM
            combined_df['date'] = pd.to_datetime(combined_df['date'])
            print(f"Uploading combined data to ClickHouse ({len(combined_df)} rows)")

            # Create a client with the provided connection parameters
            try:
                # Get database name from connection params or use default
                database = self.clickhouse_params.get('database', 'default')
                table_name = self.clickhouse_params.get('table_name', 'raw_facebook_ads')

                client = self.clickhouse_client
                client.command(f"DROP TABLE IF EXISTS {database}.facebook_ads_gold")
                create_table_query = f"""
                CREATE TABLE IF NOT EXISTS {database}.facebook_ads_gold
                (
                    `date` Date,
                    `account_name` String,
                    `account_id` String,
                    `ad_id` String,
                    `ad_name` String,
                    `adset_name` String,
                    `campaign` String,
                    `clicks` Int64,
                    `ctr` Float64,
                    `frequency` Float64,
                    `impressions` Int64,
                    `objective` String,
                    `reach` Int64,
                    `totalcost` Float64,
                    `age` String,
                    `gender` String,
                    `actions_lead` Int64,
                    `actions_offsite_conversion_fb_pixel_lead` Int64,
                    `actions_onsite_conversion_lead_grouped` Int64,
                    `actions_onsite_conversion_messaging_conversation_started_7d` Int64,
                    `actions_post_engagement` Int64,
                    `actions_post_reaction` Int64,
                    `cost_per_action_type_lead` Float64,
                    `cost_per_action_type_offsite_conversion_fb_pixel_lead` Float64,
                    `cost_per_action_type_onsite_conversion_lead_grouped` Float64,
                    `cost_per_action_type_onsite_conversion_messaging_conversation_started_7d` Float64,
                    `cost_per_action_type_post_engagement` Float64,
                    `cost_per_action_type_post_reaction` Float64,
                    `custom_conversion_action_name` String,
                    `custom_conversion_action_count` Int64,
                    `custom_conversion_action_value` Float64,
                    `instagram_permalink_url` String,
                    `link` String,
                    `object_url` String,
                    `conta_bm` String
                )
                ENGINE = MergeTree
                ORDER BY tuple()
                SETTINGS index_granularity = 8192;
                """
                # Execute CREATE TABLE query
                client.command(create_table_query)
                print(f"Created or verified table: {database}.{table_name}")

                # Insert data
                result = clickhouse.insert_df_to_clickhouse(combined_df, database, table_name, client)
                if result:
                    print(f"Successfully uploaded {len(combined_df)} records to ClickHouse table {database}.{table_name}")
                    return True
                else:
                    print(f"Failed to upload data to ClickHouse")
                    return False
            except Exception as e:
                print(f"Error uploading to ClickHouse: {e}")
                traceback.print_exc()
                raise e

        def is_date_processed(self, date_str: str) -> bool:
            """Check if date is already processed by checking ClickHouse"""
            try:
                database = self.clickhouse_params.get('database', 'default')
                table_name = self.clickhouse_params.get('table_name', 'raw_facebook_ads')

                client = self.clickhouse_client

                # Check if table exists
                check_table_query = f"EXISTS TABLE {database}.{table_name}"
                table_exists = client.command(check_table_query)

                if not table_exists:
                    return False

                # Check if data for this date exists
                check_query = f"SELECT count() FROM {database}.{table_name} WHERE date = '{date_str}' and conta_bm='{CONTA_BM}'"
                result = client.query(check_query)
                count = result.result_rows[0][0]

                return count > 0
            except Exception as e:
                print(f"Error checking if date is processed: {e}")
                return False

        def get_dates_to_process(self) -> List[str]:
            """Get list of dates to process"""
            print(f"\nChecking dates from {HISTORICAL_START_DATE} to yesterday...")
            start = datetime.strptime(HISTORICAL_START_DATE, "%Y-%m-%d").date()
            yesterday = datetime.now().date() - timedelta(days=1)  # Yesterday, not today

            dates = []
            current = start
            while current <= yesterday:  # Changed from 'today' to 'yesterday'
                date_str = current.strftime("%Y-%m-%d")
                if not self.is_date_processed(date_str):
                    dates.append(date_str)
                    print(f"   └─ {date_str}: Not processed [X]")
                else:
                    print(f"   └─ {date_str}: Already processed [✓]")
                current += timedelta(days=1)

            return dates

        def cleanup_temp_files(self):
            """Clean up temporary files and directory"""
            print(f"Cleaning up temporary files...")
            try:
                # Remove the temporary directory and all its contents
                if self.temp_dir.exists():
                    shutil.rmtree(self.temp_dir)
                    print(f"Removed temporary directory: {self.temp_dir}")
            except Exception as e:
                print(f"Error cleaning up temporary files: {e}")

        async def run(self):
            """Execute collection process"""
            try:
                # Load custom conversion names at the beginning
                self.custom_conversion_names = await self.get_custom_conversion_names()

                # VALIDAÇÃO ANTES DE COMEÇAR
                if not await self.validate_credentials():
                    print("\n[ERRO CRITICO] Validação falhou. Interrompendo execução.")
                    print("Verifique:")
                    print("1. ACCESS_TOKEN está correto e não expirou")
                    print("2. ACCOUNT_IDS estão corretos")
                    print("3. Permissões do app incluem 'ads_read'")
                    return

                dates = self.get_dates_to_process()
                if not dates:
                    print("\nAll dates already processed. Nothing to do!")
                    return

                print(f"\nStarting collection for {len(dates)} dates")
                print(f"Date range: {dates[0]} to {dates[-1]}")
                print("=" * 40)

                total_start_time = time.time()

                # Process all dates and collect data
                for i, date_str in enumerate(dates, 1):
                    print(f"\nProcessing ({i}/{len(dates)}): {date_str}")
                    df = await self.process_date(date_str)

                    # Save to temp file for reference
                    await self.save_to_temp_file(df, date_str)

                    # Add to the list of DataFrames for combined upload
                    if not df.empty:
                        self.all_data_frames.append(df)

                # Upload all collected data at once
                result = self.upload_all_data_to_clickhouse()

                total_elapsed = time.time() - total_start_time
                print(f"\nCollection Complete!")
                print(f"Total execution time: {total_elapsed:.2f} seconds")
                print(f"Average time per date: {total_elapsed / len(dates):.2f} seconds")
                print("=" * 40)

                return result

            except Exception as e:
                print(f"\nCritical error: {e}")
                traceback.print_exc()
                raise e
            finally:
                await self.close_session()
                self.cleanup_temp_files()

    async def main():
        print("\n" + "=" * 60)
        print("FACEBOOK ADS COLLECTOR - VERSÃO CORRIGIDA (SEM object_url nos insights)")
        print("=" * 60)
        print(f"Accounts: {', '.join(API_ACCOUNT_IDS)}")
        print(f"Start Date: {HISTORICAL_START_DATE}")
        print("URLs dos criativos serão buscadas separadamente!")
        print("=" * 60)

        collector = FacebookAdsCollector(
            access_token=API_ACCESS_TOKEN,
            account_ids=API_ACCOUNT_IDS,
            clickhouse_params=customer['clickhouse_connection_params']
        )

        await collector.run()

    # START
    asyncio.run(main())
