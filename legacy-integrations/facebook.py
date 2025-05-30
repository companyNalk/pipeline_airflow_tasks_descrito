"""
Facebook Ads module for data extraction functions.
This module contains functions specific to the Facebook Ads integration.
"""

import os
import tempfile
import shutil
from pathlib import Path
from core import clickhouse


def run(customer):
    import asyncio
    import json
    import time
    import traceback
    from datetime import datetime, timedelta
    from typing import List, Dict
    import os
    import pathlib
    import uuid
    import secrets
    import string

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
        'objective', 'reach', 'spend', 'actions'
    ]

    ACTION_FIELDS = [
        'lead', 'offsite_conversion.fb_pixel_lead', 'onsite_conversion.lead_grouped',
        'onsite_conversion.messaging_conversation_started_7d', 'post_engagement', 'post_reaction'
    ]

    # FIXED HEADER - Garantir que todos os arquivos tenham exatamente o mesmo header
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
        'instagram_permalink_url', 'link'
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

        async def get_session(self):
            if not self.session:
                self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=300))
            return self.session

        async def close_session(self):
            if self.session:
                await self.session.close()
                self.session = None

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
                    print(f"[ERRO] Token inválido: {error_data}")
                    return False
                
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
                        print(f"[ERRO] Sem acesso à conta {account_id}: {error_data}")
            
            # 3. Testar insights
            if valid_accounts > 0:
                test_date = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
                url = f"{self.base_url}/{self.account_ids[0]}/insights"
                params = {
                    "access_token": self.access_token,
                    "fields": "spend,impressions",
                    "time_range": json.dumps({"since": test_date, "until": test_date}),
                    "level": "account",
                    "limit": 1
                }
                
                async with session.get(url, params=params) as response:
                    if response.status == 200:
                        print(f"[OK] Acesso a insights confirmado")
                    else:
                        error_data = await response.json()
                        print(f"[ERRO] Sem acesso a insights: {error_data}")
                        return False
            
            print("="*60)
            if valid_accounts == len(self.account_ids):
                print("[SUCESSO] Todas as validações passaram!")
                return True
            else:
                print(f"[ERRO] Apenas {valid_accounts}/{len(self.account_ids)} contas válidas")
                return False

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
            int_cols = ["clicks", "impressions", "reach"] + [f"actions_{a.replace('.', '_')}" for a in ACTION_FIELDS]
            float_cols = ["frequency", "ctr", "totalcost"] + [f"cost_per_action_type_{a.replace('.', '_')}" for a in
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
                return
            
            # Combine all DataFrames into one
            combined_df = pd.concat(self.all_data_frames, ignore_index=True)
            if combined_df.empty:
                print("WARNING: Combined DataFrame is empty, nothing to upload")
                return
            
            combined_df['conta_bm'] = CONTA_BM
            combined_df['date'] = pd.to_datetime(combined_df['date']).dt.date
            print(f"Uploading combined data to ClickHouse ({len(combined_df)} rows)")
            
            # Create a client with the provided connection parameters
            try:
                # Get database name from connection params or use default
                database = self.clickhouse_params.get('database', 'default')
                table_name = self.clickhouse_params.get('table_name', 'raw_facebook_ads')
                
                client = self.clickhouse_client
                
                # Generate CREATE TABLE query
                create_table_query = clickhouse.get_create_table_query(combined_df, database, table_name)
                # Execute CREATE TABLE query
                client.command(create_table_query)
                print(f"Created or verified table: {database}.{table_name}")
                
                # Insert data
                result = clickhouse.insert_df_to_clickhouse(combined_df, database, table_name, client)
                if result:
                    print(f"Successfully uploaded {len(combined_df)} records to ClickHouse table {database}.{table_name}")
                else:
                    print(f"Failed to upload data to ClickHouse")
            
            except Exception as e:
                print(f"Error uploading to ClickHouse: {e}")
                traceback.print_exc()

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
                self.upload_all_data_to_clickhouse()

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
                self.cleanup_temp_files()

    async def main():
        print("\n" + "=" * 60)
        print("FACEBOOK ADS COLLECTOR - WITH CREDENTIAL VALIDATION")
        print("=" * 60)
        print(f"Accounts: {', '.join(API_ACCOUNT_IDS)}")
        print(f"Start Date: {HISTORICAL_START_DATE}")
        print("=" * 60)

        collector = FacebookAdsCollector(
            access_token=API_ACCESS_TOKEN,
            account_ids=API_ACCOUNT_IDS,
            clickhouse_params=customer['clickhouse_connection_params']
        )

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
