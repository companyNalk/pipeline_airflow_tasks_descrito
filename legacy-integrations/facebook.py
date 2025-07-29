"""
Facebook Ads module for data extraction functions.
This module contains functions specific to the Facebook Ads integration.
"""

from core import gcs


def run(customer):
    import json
    import time
    import asyncio
    import aiohttp
    import pandas as pd
    from datetime import datetime, timedelta
    from typing import List, Dict
    import traceback
    from google.cloud import storage
    import pathlib

    # Configuration
    BUCKET_NAME = customer['bucket_name']
    SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'setup_automatico.json').as_posix()
    HISTORICAL_START_DATE = customer['start_date']
    ACCESS_TOKEN = customer['access_token']

    accounts_ids_raw = customer['accounts_ids']
    if isinstance(accounts_ids_raw, str):
        ACCOUNT_IDS = [acc.strip() for acc in accounts_ids_raw.split(',') if acc.strip()]
    else:
        ACCOUNT_IDS = accounts_ids_raw

    FIELDS = [
        'account_name', 'account_id', 'ad_id', 'ad_name', 'adset_name',
        'campaign_name', 'clicks', 'cost_per_action_type', 'ctr',
        'date_start', 'date_stop', 'frequency', 'impressions',
        'objective', 'reach', 'spend', 'actions', 'action_values'
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
        'instagram_permalink_url', 'link'
    ]

    class FacebookAdsCollector:
        def __init__(self, access_token: str, account_ids: List[str], bucket_name: str):
            self.access_token = access_token
            self.account_ids = account_ids
            self.base_url = "https://graph.facebook.com/v19.0"
            self.session = None
            self.bucket = storage.Client.from_service_account_json(SERVICE_ACCOUNT_PATH).bucket(bucket_name)
            self.semaphore = asyncio.Semaphore(500)  # Paralelismo de 500
            self.custom_conversion_names = {}  # Cache para nomes das conversões personalizadas

        async def get_session(self):
            if not self.session:
                self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=300))
            return self.session

        async def close_session(self):
            if self.session:
                await self.session.close()
                self.session = None

        async def debug_account_permissions(self, account_id: str):
            """Debug para verificar permissões da conta"""
            session = await self.get_session()
            clean_account_id = account_id.replace('act_', '')

            # Testar acesso básico à conta
            url = f"{self.base_url}/act_{clean_account_id}"
            params = {
                "access_token": self.access_token,
                "fields": "account_id,name,account_status,capabilities"
            }

            try:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        print(f"     ├─ Account Access: OK")
                        print(f"     ├─ Account Name: {data.get('name', 'N/A')}")
                        print(f"     ├─ Account Status: {data.get('account_status', 'N/A')}")
                        print(f"     └─ Capabilities: {data.get('capabilities', [])}")
                        return True
                    else:
                        error_data = await response.json()
                        print(f"     └─ Account Access Error {response.status}: {error_data}")
                        return False
            except Exception as e:
                print(f"     └─ Account Access Exception: {e}")
                return False

        async def get_custom_conversion_names(self) -> Dict[str, str]:
            """Consulta os nomes das conversões personalizadas com melhor tratamento de erros"""
            print("  ├─ Carregando nomes das conversões personalizadas...")
            session = await self.get_session()
            names = {}

            for account_id in self.account_ids:
                print(f"    ├─ Processando account: {account_id}")

                # Debug das permissões da conta
                has_access = await self.debug_account_permissions(account_id)
                if not has_access:
                    print(f"    └─ Pulando conversões personalizadas para {account_id} (sem acesso)")
                    continue

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
                                conversions = data.get("data", [])
                                for item in conversions:
                                    names[item["id"]] = item["name"]
                                print(f"    └─ Success: {len(conversions)} conversões personalizadas encontradas")
                            else:
                                # Melhor tratamento de erros
                                error_text = await response.text()
                                try:
                                    error_data = json.loads(error_text)
                                    error_msg = error_data.get("error", {}).get("message", "Unknown error")
                                    error_code = error_data.get("error", {}).get("code", "Unknown")
                                    print(f"    └─ API Error {response.status}: Code {error_code} - {error_msg}")
                                except:
                                    print(f"    └─ HTTP Error {response.status}: {error_text[:200]}...")

                                # Se erro 400, pode ser que não tenha conversões personalizadas
                                if response.status == 400:
                                    print(f"    └─ Nota: Conta pode não ter conversões personalizadas configuradas")

                except Exception as e:
                    print(f"    └─ Exception: {e}")

            print(f"  └─ Total de conversões personalizadas carregadas: {len(names)}")
            return names

        async def get_ad_details_with_demographics(self, ad_id: str, date_str: str) -> List[Dict]:
            """Get detailed metrics for ad_id broken down by age and gender"""
            session = await self.get_session()
            url = f"{self.base_url}/{ad_id}/insights"
            params = {
                "access_token": self.access_token,
                "fields": ",".join(FIELDS),
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
                        else:
                            error_text = await response.text()
                            print(f"    └─ Ad {ad_id}: Error {response.status} - {error_text[:100]}...")

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
                        else:
                            error_text = await response.text()
                            print(f"    └─ Error {response.status}: {error_text[:200]}...")

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

                if not ad_ids:
                    print(f"  └─ No active ads found for {date_str}")
                    continue

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

            df = self.transform_data(all_data, date_str)
            print(f"Final aggregated rows: {len(df)}")
            print(f"{'=' * 60}")

            return df

        def transform_data(self, data: List[Dict], date_str: str) -> pd.DataFrame:
            """Transform raw data to normalized DataFrame with fixed columns"""
            if not data:
                print("WARNING: No data to process")
                # Return empty DataFrame with fixed columns
                return pd.DataFrame(columns=FIXED_COLUMNS)

            print(f"  ├─ Transforming {len(data)} raw rows")

            # First, transform all raw data
            rows = []
            for item in data:
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
                    "ad_id": item.get("ad_id"),
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
                    "instagram_permalink_url": None,
                    "link": None
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
                    "link": "first"
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
            int_cols = ["clicks", "impressions", "reach", "custom_conversion_action_count"] + [
                f"actions_{a.replace('.', '_')}" for a in ACTION_FIELDS]
            float_cols = ["frequency", "ctr", "totalcost", "custom_conversion_action_value"] + [
                f"cost_per_action_type_{a.replace('.', '_')}" for a in ACTION_FIELDS]

            for col in int_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('int64')

            for col in float_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0).astype('float64')

            # Reorder columns
            df = df[FIXED_COLUMNS]

            return df

        async def save_to_gcs(self, df: pd.DataFrame, date_str: str):
            """Save data to Google Cloud Storage with fixed columns"""
            if df.empty:
                print(f"WARNING: No data to save for {date_str}")
                # Still save an empty file with fixed columns
                df = pd.DataFrame(columns=FIXED_COLUMNS)

            # Ensure we have exactly the fixed columns
            df = self._ensure_fixed_columns(df)

            # Use facebook_ads_{date} format for filename
            blob_name = f"facebook_ads_{date_str}.csv"
            blob = self.bucket.blob(blob_name)

            print(f"Saving to GCS: {blob_name}")
            csv_data = df.to_csv(index=False).encode('utf-8')
            blob.upload_from_string(csv_data, content_type='text/csv')
            print(f"Successfully saved {len(df)} records")

        def is_date_processed(self, date_str: str) -> bool:
            """Check if date is already processed"""
            blob_name = f"facebook_ads_{date_str}.csv"
            return self.bucket.blob(blob_name).exists()

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

        async def test_token_and_permissions(self):
            """Testa o token e permissões básicas"""
            print("\n" + "=" * 60)
            print("TESTANDO TOKEN E PERMISSÕES")
            print("=" * 60)

            session = await self.get_session()

            # Test 1: Token validation
            url = f"{self.base_url}/me"
            params = {"access_token": self.access_token}

            try:
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        print(f"✓ Token válido - User: {data.get('name', 'N/A')} (ID: {data.get('id', 'N/A')})")
                    else:
                        error_data = await response.json()
                        print(f"✗ Token inválido - Error: {error_data}")
                        return False
            except Exception as e:
                print(f"✗ Erro ao validar token: {e}")
                return False

            # Test 2: Account access
            for account_id in self.account_ids:
                print(f"\nTestando acesso à conta: {account_id}")
                await self.debug_account_permissions(account_id)

            return True

        async def run(self):
            """Execute collection process"""
            try:
                # Primeiro, testar token e permissões
                if not await self.test_token_and_permissions():
                    print("\nERRO: Problemas com token ou permissões. Abortando...")
                    return

                # Load custom conversion names at the beginning
                self.custom_conversion_names = await self.get_custom_conversion_names()

                dates = self.get_dates_to_process()
                if not dates:
                    print("\nAll dates already processed. Nothing to do!")
                    return

                print(f"\nStarting collection for {len(dates)} dates")
                print(f"Date range: {dates[0]} to {dates[-1]}")
                print("=" * 40)

                total_start_time = time.time()

                for i, date_str in enumerate(dates, 1):
                    print(f"\nProcessing ({i}/{len(dates)}): {date_str}")
                    df = await self.process_date(date_str)
                    await self.save_to_gcs(df, date_str)

                total_elapsed = time.time() - total_start_time
                print(f"\nCollection Complete!")
                print(f"Total execution time: {total_elapsed:.2f} seconds")
                print(f"Average time per date: {total_elapsed / len(dates):.2f} seconds")
                print("=" * 40)

            except Exception as e:
                print(f"\nCritical error: {e}")
                traceback.print_exc()
            finally:
                await self.close_session()

    async def main():
        print("\n" + "=" * 60)
        print("FACEBOOK ADS COLLECTOR - WITH CUSTOM CONVERSIONS")
        print("=" * 60)
        print(f"Accounts: {ACCOUNT_IDS}")
        print(f"Bucket: {BUCKET_NAME}")
        print(f"Start Date: {HISTORICAL_START_DATE}")
        print("=" * 60)

        collector = FacebookAdsCollector(access_token=ACCESS_TOKEN, account_ids=ACCOUNT_IDS, bucket_name=BUCKET_NAME)

        await collector.run()

    # START
    asyncio.run(main())


def get_extraction_tasks():
    """
    Get the list of data extraction tasks for Facebook Ads.

    Returns:
        list: List of task configurations
    """
    return [
        {
            'task_id': 'run',
            'python_callable': run
        }
    ]
