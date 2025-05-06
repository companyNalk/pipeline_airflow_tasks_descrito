"""
Facebook Ads module for data extraction functions.
This module contains functions specific to the Facebook Ads integration.
"""

from core import gcs


def run(customer):
    import os
    import pathlib
    import json
    import time
    import asyncio
    import aiohttp
    import pandas as pd
    from datetime import datetime, timedelta
    from typing import List, Dict
    import traceback
    from google.cloud import storage

    # Configuration
    BUCKET_NAME = customer['bucket_name']
    # HISTORICAL_START_DATE = "2024-01-01"  # Removido conforme solicitado
    API_ACCESS_TOKEN = customer['api_access_token']
    API_ACCOUNT_IDS = ['api_account_ids']

    SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH

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
        def __init__(self, access_token: str, account_ids: List[str], bucket_name: str):
            self.access_token = access_token
            self.account_ids = account_ids
            self.base_url = "https://graph.facebook.com/v19.0"
            self.session = None
            storage_client = storage.Client(project=customer['project_id'])
            self.bucket = storage_client.bucket(bucket_name)
            self.semaphore = asyncio.Semaphore(500)  # Paralelismo de 500

        async def get_session(self):
            if not self.session:
                self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=300))
            return self.session

        async def close_session(self):
            if self.session:
                await self.session.close()
                self.session = None

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
                raise e

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
                raise e

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
            """Get list of dates to process (últimos 14 dias excluindo hoje)"""
            print(f"\nChecking dates for the last 14 days (excluding today)...")

            # Calcula os últimos 14 dias, excluindo hoje
            today = datetime.now().date()
            start = today - timedelta(days=14)  # 14 dias atrás
            end = today - timedelta(days=1)  # ontem

            print(f"Date range: {start.strftime('%Y-%m-%d')} to {end.strftime('%Y-%m-%d')}")

            dates = []
            current = start
            while current <= end:
                date_str = current.strftime("%Y-%m-%d")
                if not self.is_date_processed(date_str):
                    dates.append(date_str)
                    print(f"   └─ {date_str}: Not processed [X]")
                else:
                    print(f"   └─ {date_str}: Already processed [✓]")
                current += timedelta(days=1)

            return dates

        async def run(self):
            """Execute collection process"""
            try:
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
                raise e
            finally:
                await self.close_session()

    async def main():
        print("\n" + "=" * 60)
        print("FACEBOOK ADS COLLECTOR - ÚLTIMOS 14 DIAS")
        print("=" * 60)
        print(f"Accounts: {', '.join(API_ACCOUNT_IDS)}")
        print(f"Bucket: {BUCKET_NAME}")
        print(f"Collection: Last 14 days (excluding today)")
        print("=" * 60)

        collector = FacebookAdsCollector(
            access_token=API_ACCESS_TOKEN,
            account_ids=API_ACCOUNT_IDS,
            bucket_name=BUCKET_NAME
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
