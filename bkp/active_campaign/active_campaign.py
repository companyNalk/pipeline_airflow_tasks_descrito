"""
Active Campaign module for data extraction functions.
This module contains functions specific to the Active Campaign integration.
"""

from core import gcs

def run_active_campaign_extraction(customer):
    """
    Main function to extract all data from Active Campaign.
    This function collects data from various endpoints and saves them to GCS.
    """
    import csv
    import io
    import json
    import os
    import unicodedata
    import re
    import time
    from datetime import datetime
    import concurrent.futures

    import requests
    import pandas as pd
    from google.api_core.exceptions import GoogleAPIError
    from google.cloud import storage

    # Variables from customer config
    API_TOKEN = customer['token']
    BASE_URL = customer['base_url']
    BUCKET_NAME = customer['bucket_name']
    FOLDER_PATH = 'active-campaign'

    # Shared API settings
    API_HEADERS = {
        "Api-Token": API_TOKEN,
        "Content-Type": "application/json"
    }

    # Function to normalize column names
    def normalize_column_name(name):
        nfkd = unicodedata.normalize('NFKD', name)
        ascii_name = nfkd.encode('ASCII', 'ignore').decode('ASCII')
        cleaned = re.sub(r"[^\w\s]", "", ascii_name)
        return re.sub(r"\s+", "_", cleaned).lower()

    # Initialize Google Cloud Storage client
    def get_storage_client():
        """Initializes the Google Cloud Storage client using default GCP credentials."""
        try:
            return storage.Client()
        except GoogleAPIError as e:
            print(f"[ERROR] Error initializing Google Cloud Storage client: {e}")
            raise

    # Upload file to GCS
    def upload_to_gcs(bucket_name, destination_blob_name, content, content_type='text/csv'):
        """Uploads a string as blob to Google Cloud Storage (GCS)."""
        try:
            credentials = gcs.load_credentials_from_env()
            gcs.write_string_to_gcs(
                bucket_name=bucket_name,
                content=content,
                destination_name=destination_blob_name,
                content_type=content_type,
                credentials=credentials
            )
            print(f"[SUCCESS] File {destination_blob_name} uploaded successfully to bucket {bucket_name}.")
        except Exception as e:
            print(f"[ERROR] Failed to upload to {destination_blob_name}: {str(e)}")
            raise

    # Function to make API requests with error handling and pagination
    def make_api_request(endpoint, params=None, pagination_key='offset'):
        """Makes a request to the API with error handling and pagination."""
        url = f"{BASE_URL}/{endpoint}"
        all_items = []
        
        if pagination_key == 'offset':
            # For APIs that use offset/limit
            offset = 0
            limit = 100
            
            while True:
                request_params = params.copy() if params else {}
                request_params.update({
                    "limit": limit,
                    "offset": offset
                })
                
                try:
                    response = requests.get(url, headers=API_HEADERS, params=request_params)
                    response.raise_for_status()
                    
                    data = response.json()
                    # Determine the key to access items (usually the plural of the endpoint)
                    items_key = endpoint if endpoint.endswith('s') else f"{endpoint}s"
                    items = data.get(items_key, [])
                    
                    if not items:
                        break
                        
                    all_items.extend(items)
                    offset += len(items)
                    print(f"[INFO] Retrieved {len(all_items)} records from {items_key} so far.")
                    
                except requests.RequestException as e:
                    print(f"[ERROR] Error in API request to {url}: {e}")
                    break
        
        elif pagination_key == 'page':
            # For APIs that use page-based pagination
            page = 1
            
            while True:
                request_params = params.copy() if params else {}
                request_params.update({
                    "page": page
                })
                
                try:
                    response = requests.get(url, headers=API_HEADERS, params=request_params)
                    response.raise_for_status()
                    
                    data = response.json()
                    # Determine the key to access items (usually the plural of the endpoint)
                    items_key = endpoint if endpoint.endswith('s') else f"{endpoint}s"
                    items = data.get(items_key, [])
                    
                    if not items:
                        break
                        
                    all_items.extend(items)
                    
                    # Check if there's a next page
                    has_next = False
                    if 'meta' in data and data['meta'].get('next_page_url'):
                        has_next = True
                    
                    if not has_next:
                        break
                        
                    page += 1
                    print(f"[INFO] Retrieved {len(all_items)} records from {items_key} so far (page {page-1}).")
                    
                except requests.RequestException as e:
                    print(f"[ERROR] Error in API request to {url}: {e}")
                    break
        
        return all_items

    # Function to process and save data to GCS
    def process_and_save_data(data, filename, format='csv'):
        """Processes data and saves to GCS in CSV or JSON format."""
        if not data:
            print(f"[WARNING] No data to save in {filename}.")
            return
        
        # Complete file path
        full_path = f"{FOLDER_PATH}/{filename}"
        
        if format.lower() == 'csv':
            # Normalize column names if data is dictionaries
            if isinstance(data[0], dict):
                # Get all possible keys
                all_keys = set()
                for item in data:
                    all_keys.update(item.keys())
                
                # Normalize keys
                normalized_keys = {key: normalize_column_name(key) for key in all_keys}
                normalized_data = []
                
                for item in data:
                    normalized_item = {}
                    for key, value in item.items():
                        normalized_item[normalized_keys[key]] = value
                    normalized_data.append(normalized_item)
                
                # Use pandas to create CSV
                df = pd.DataFrame(normalized_data)
                csv_data = df.to_csv(index=False)
                
                # Upload to GCS
                upload_to_gcs(BUCKET_NAME, full_path, csv_data, 'text/csv')
                
            else:
                # If not dictionaries, convert to DataFrame directly
                df = pd.DataFrame(data)
                csv_data = df.to_csv(index=False)
                upload_to_gcs(BUCKET_NAME, full_path, csv_data, 'text/csv')
                
        elif format.lower() == 'json':
            # For JSON, simply convert and upload
            json_data = json.dumps(data, indent=4)
            upload_to_gcs(BUCKET_NAME, full_path, json_data, 'application/json')
        
        print(f"[SUCCESS] {len(data)} records saved to {full_path}.")

    # Function to measure performance
    def measure_performance(func):
        """Decorator to measure execution time of a function."""
        def wrapper(*args, **kwargs):
            start_time = time.time()
            print(f"[INFO] Starting {func.__name__}...")
            result = func(*args, **kwargs)
            end_time = time.time()
            print(f"[INFO] {func.__name__} completed in {end_time - start_time:.2f} seconds.")
            return result
        return wrapper

    # Specific functions for each endpoint
    @measure_performance
    def collect_contact_automations():
        """Collects data from contactAutomations endpoint."""
        contact_automations = make_api_request('contactAutomations')
        process_and_save_data(contact_automations, 'contact-automations.csv')
        return len(contact_automations)

    @measure_performance
    def collect_lists():
        """Collects data from lists endpoint."""
        lists = make_api_request('lists')
        process_and_save_data(lists, 'lists.csv')
        return len(lists)

    @measure_performance
    def collect_accounts():
        """Collects data from accounts endpoint."""
        accounts = make_api_request('accounts')
        process_and_save_data(accounts, 'accounts.csv')
        return len(accounts)

    @measure_performance
    def collect_automations():
        """Collects data from automations endpoint."""
        # Using page-based pagination instead of offset
        automations = make_api_request('automations', pagination_key='page')
        process_and_save_data(automations, 'automations.csv')
        return len(automations)

    @measure_performance
    def collect_campaigns():
        """Collects data from campaigns endpoint."""
        # Using page-based pagination instead of offset
        campaigns = make_api_request('campaigns', pagination_key='page')
        process_and_save_data(campaigns, 'campaigns.csv')
        return len(campaigns)

    @measure_performance
    def collect_contacts():
        """Collects data from contacts endpoint."""
        params = {"sort": "created_timestamp ASC"}
        contacts = make_api_request('contacts', params)
        process_and_save_data(contacts, 'contacts.csv')
        return len(contacts)

    @measure_performance
    def collect_messages():
        """Collects data from messages endpoint."""
        params = {"sort": "created_timestamp ASC"}
        messages = make_api_request('messages', params)
        process_and_save_data(messages, 'messages.json', format='json')
        return len(messages)

    # Main function to execute all collections sequentially
    def run_sequential():
        """Executes all data collections sequentially."""
        print("[START] Starting sequential collection of Active Campaign data")
        global_start = time.time()
        
        total_records = 0
        total_records += collect_contact_automations()
        total_records += collect_lists()
        total_records += collect_accounts()
        total_records += collect_automations()
        total_records += collect_campaigns()
        total_records += collect_contacts()
        total_records += collect_messages()
        
        global_end = time.time()
        print(f"[COMPLETE] Process finished. Total records: {total_records}. Total time: {global_end - global_start:.2f} seconds")

    # Execute the sequential collection
    run_sequential()

def get_extraction_tasks():
    """
    Get the list of data extraction tasks for Active Campaign.
    
    Returns:
        list: List of task configurations
    """
    return [
        {
            'task_id': 'run_active_campaign_extraction',
            'python_callable': run_active_campaign_extraction
        }
    ]
