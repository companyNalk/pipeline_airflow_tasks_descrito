from datetime import datetime, timedelta
from core import gcs
import time
import pandas as pd
import requests
import json

# Common constants
BASE_URL = 'https://developers.hotmart.com'
AUTH_URL = 'https://api-sec-vlc.hotmart.com/security/oauth/token'

# List of transaction statuses for comprehensive data collection
TRANSACTION_STATUSES = [
    'APPROVED', 'BLOCKED', 'CANCELLED', 'CHARGEBACK', 'COMPLETE',
    'EXPIRED', 'NO_FUNDS', 'OVERDUE', 'PARTIALLY_REFUNDED',
    'PRE_ORDER', 'PRINTED_BILLET', 'PROCESSING_TRANSACTION',
    'PROTESTED', 'REFUNDED', 'STARTED', 'UNDER_ANALISYS', 'WAITING_PAYMENT'
]

# Function to get authentication token
def get_token(basic_auth):
    """Obtains an access token from the Hotmart API."""
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Authorization': f'Basic {basic_auth}'
    }
    response = requests.post(AUTH_URL, headers=headers, params={'grant_type': 'client_credentials'})
    if response.status_code != 200:
        raise Exception(f"Error obtaining token: {response.status_code} - {response.text}")
    return response.json()['access_token']

# Function to calculate date range for the specified days
def get_date_range(days=365):
    """Returns start and end dates for fetching historical data."""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)

    # Format dates as timestamps in milliseconds
    start_timestamp = int(start_date.timestamp() * 1000)
    end_timestamp = int(end_date.timestamp() * 1000)

    print(f"Fetching data from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    return start_timestamp, end_timestamp

# Function to recursively flatten nested JSON
def flatten_json_recursive(nested_json, prefix=''):
    """Recursively extracts all fields from a nested JSON."""
    flattened = {}
    for key, value in nested_json.items():
        new_key = f"{prefix}_{key}" if prefix else key
        if isinstance(value, dict):
            flattened.update(flatten_json_recursive(value, new_key))
        elif isinstance(value, list):
            for i, item in enumerate(value):
                if isinstance(item, dict):
                    flattened.update(flatten_json_recursive(item, f"{new_key}_{i}"))
                else:
                    flattened[f"{new_key}_{i}"] = item
        else:
            flattened[new_key] = value
    return flattened

# Function to convert epoch timestamps to readable dates
def convert_dates(df):
    """Converts timestamps in epoch format to a readable format."""
    date_indicators = ['date', '_at', 'time']
    date_cols = [col for col in df.columns if any(indicator in col.lower() for indicator in date_indicators) and df[col].dtype in ['int64', 'float64']]

    for col in date_cols:
        df[f"{col}_readable"] = df[col].apply(
            lambda x: datetime.fromtimestamp(x / 1000).strftime('%Y-%m-%d %H:%M:%S') if pd.notnull(x) and x > 0 else None
        )
    return df

# Function to extract products
def extract_products(customer):
    """Main function to extract products and save to GCS."""
    start_time = time.time()
    print(f"Starting product extraction for {customer['project_id']}...")

    # Define the CSV file path within the "products" folder
    csv_filename = "products/products.csv"

    # Get access token
    token = get_token(customer['basic_auth'])

    # Get products
    print("Getting all products...")
    products_data = get_all_products(token, customer)

    if not products_data or not products_data.get('items'):
        print("No products found")
        return

    # Process and save CSV
    print("Processing product data for CSV...")
    df = process_products(products_data)

    if df is not None and not df.empty:
        csv_data = df.to_csv(index=False)
        
        # Use the GCS module to upload
        credentials = gcs.load_credentials_from_env()
        local_file_path = f"/tmp/{customer['project_id']}.hotmart.products.csv"
        df.to_csv(local_file_path, index=False)
        
        gcs.write_file_to_gcs(
            bucket_name=customer['bucket_name'],
            local_file_path=local_file_path,
            destination_name=csv_filename,
            credentials=credentials
        )

        print(f"Total products obtained: {len(products_data['items'])}")
        print(f"Process completed in {time.time() - start_time:.2f} seconds")
    else:
        print("No data to process after product analysis")

# Function to get all products with pagination
def get_all_products(token, customer):
    """Gets all products from the Hotmart API with pagination."""
    all_products = []
    url = f"{BASE_URL}/products/api/v1/products"
    headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
    next_page = None
    page_count = 0

    while True:
        page_count += 1
        params = {'max_results': 100}
        if next_page:
            params['page_token'] = next_page

        print(f"Getting page {page_count} of products...")
        response = requests.get(url, headers=headers, params=params)

        if response.status_code == 429:  # Rate limit
            print("Rate limit reached. Waiting 60 seconds...")
            time.sleep(60)
            continue

        if response.status_code != 200:
            print(f"Error getting products: {response.status_code} - {response.text}")
            break

        data = response.json()
        products = data.get('items', [])

        if not products:
            print("No more products to get.")
            break

        all_products.extend(products)
        print(f"Received {len(products)} items on this page. Total: {len(all_products)}")

        # Check if there's a next page
        if 'page_info' in data and data['page_info'].get('next_page_token'):
            next_page = data['page_info']['next_page_token']
        else:
            break

    print(f"Total products obtained: {len(all_products)}")
    return {'items': all_products}

# Function to process products
def process_products(data):
    """Processes product data and returns a DataFrame."""
    if 'items' not in data or not data['items']:
        print("No product items found in the data")
        return None

    items = data['items']
    flat_items = [flatten_json_recursive(item) for item in items]
    df = pd.DataFrame(flat_items)
    df = convert_dates(df)
    return df

# Function to extract sales history
def extract_sales_history(customer):
    """Main function to extract sales history and save to GCS."""
    start_time = time.time()
    days = int(customer.get('days_to_fetch', 365))
    print(f"Starting sales history extraction for {customer['project_id']} for the last {days} days...")

    # Define the CSV file path within the "sales_history" folder
    csv_filename = "sales_history/sales_history.csv"

    # Get access token
    token = get_token(customer['basic_auth'])

    # Get sales history
    print(f"Getting all sales for the last {days} days...")
    all_sales = get_all_sales_history(token, customer)

    if not all_sales:
        print("No sales found")
        return

    # Process and save CSV
    print("Processing sales data for CSV...")
    df = process_sales(all_sales)

    if df is not None and not df.empty:
        # Use the GCS module to upload
        credentials = gcs.load_credentials_from_env()
        local_file_path = f"/tmp/{customer['project_id']}.hotmart.sales_history.csv"
        df.to_csv(local_file_path, index=False)
        
        gcs.write_file_to_gcs(
            bucket_name=customer['bucket_name'],
            local_file_path=local_file_path,
            destination_name=csv_filename,
            credentials=credentials
        )

        print(f"Total sales obtained: {len(all_sales)}")
        print(f"Process completed in {time.time() - start_time:.2f} seconds")
    else:
        print("No data to process after sales analysis")

# Function to get all sales history with pagination
def get_all_sales_history(token, customer):
    """Gets all sales history with pagination for the specified days."""
    days = int(customer.get('days_to_fetch', 365))
    
    # Get date range
    start_date, end_date = get_date_range(days)

    # Complete list of transaction statuses
    transaction_status_param = ",".join(TRANSACTION_STATUSES)

    url = f"{BASE_URL}/payments/api/v1/sales/history"
    headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}

    all_sales = []
    next_page = None
    page_count = 0

    while True:
        page_count += 1
        params = {
            'max_results': 500,
            'start_date': start_date,
            'end_date': end_date,
            'transaction_status': transaction_status_param
        }

        if next_page:
            params['page_token'] = next_page

        print(f"Getting page {page_count} of sales history...")
        response = requests.get(url, headers=headers, params=params)

        if response.status_code == 429:  # Rate limit
            print("Rate limit reached. Waiting 60 seconds...")
            time.sleep(60)
            continue

        if response.status_code != 200:
            print(f"Error getting sales: {response.status_code} - {response.text}")
            break

        data = response.json()
        sales = data.get('items', [])

        if not sales:
            print("No more sales to get.")
            break

        all_sales.extend(sales)
        print(f"Received {len(sales)} items on this page. Total: {len(all_sales)}")

        # Check if there's a next page
        if 'page_info' in data and data['page_info'].get('next_page_token'):
            next_page = data['page_info']['next_page_token']
        else:
            break

    print(f"Total sales obtained: {len(all_sales)}")
    return all_sales

# Function to extract nested fields from sales data
def extract_nested_fields(sales):
    """Extracts nested fields to flat fields from sales data."""
    flat_data = []

    for item in sales:
        flat_item = {}

        # First level fields
        for key, value in item.items():
            if not isinstance(value, dict) and not isinstance(value, list):
                flat_item[key] = value

        # Extract product fields
        if 'product' in item and isinstance(item['product'], dict):
            for key, value in item['product'].items():
                flat_item[f'product_{key}'] = value

        # Extract buyer fields
        if 'buyer' in item and isinstance(item['buyer'], dict):
            for key, value in item['buyer'].items():
                flat_item[f'buyer_{key}'] = value

        # Extract producer fields
        if 'producer' in item and isinstance(item['producer'], dict):
            for key, value in item['producer'].items():
                flat_item[f'producer_{key}'] = value

        # Extract purchase fields and nested objects
        if 'purchase' in item and isinstance(item['purchase'], dict):
            for key, value in item['purchase'].items():
                if not isinstance(value, dict):
                    flat_item[f'purchase_{key}'] = value

            # Extract nested objects from purchase
            nested_objects = [
                ('price', 'purchase_price_'),
                ('payment', 'purchase_payment_'),
                ('tracking', 'purchase_tracking_'),
                ('offer', 'purchase_offer_'),
                ('hotmart_fee', 'purchase_hotmart_fee_')
            ]

            for obj_name, prefix in nested_objects:
                if obj_name in item['purchase'] and isinstance(item['purchase'][obj_name], dict):
                    for key, value in item['purchase'][obj_name].items():
                        flat_item[f'{prefix}{key}'] = value

        flat_data.append(flat_item)

    return flat_data

# Function to process sales data
def process_sales(sales):
    """Processes sales data and returns a DataFrame."""
    if not sales:
        print("No sales data to process")
        return None

    # Extract nested fields
    flat_sales = extract_nested_fields(sales)

    # Create DataFrame
    df = pd.DataFrame(flat_sales)

    # Convert dates
    df = convert_dates(df)

    return df

# Function to extract sales users
def extract_sales_users(customer):
    """Main function to extract sales users and save to GCS."""
    start_time = time.time()
    days = int(customer.get('days_to_fetch', 365))
    print(f"Starting sales users extraction for {customer['project_id']} for the last {days} days...")

    # Define the CSV file path
    csv_filename = "sales_users/sales_users.csv"

    # Get access token
    token = get_token(customer['basic_auth'])

    # Get sales users
    print(f"Getting all sales users for the last {days} days...")
    all_sales_users = get_all_sales_users(token, customer)

    if not all_sales_users:
        print("No sales users found")
        return

    print(f"Total: {len(all_sales_users)} sales users records")

    # Process and save CSV
    print("Processing sales users data for CSV...")
    df = process_sales_users(all_sales_users)

    if df is not None and not df.empty:
        # Use the GCS module to upload
        credentials = gcs.load_credentials_from_env()
        local_file_path = f"/tmp/{customer['project_id']}.hotmart.sales_users.csv"
        df.to_csv(local_file_path, index=False)
        
        gcs.write_file_to_gcs(
            bucket_name=customer['bucket_name'],
            local_file_path=local_file_path,
            destination_name=csv_filename,
            credentials=credentials
        )

        print(f"Process completed in {time.time() - start_time:.2f} seconds")
        print(f"Total records processed: {len(all_sales_users)}")
        print(f"Total fields extracted: {len(df.columns)}")
    else:
        print("No data to process after sales users analysis")

# Function to get all sales users with pagination
def get_all_sales_users(token, customer):
    """Gets all sales users with pagination for the specified days."""
    days = int(customer.get('days_to_fetch', 365))
    
    # Get date range
    start_date, end_date = get_date_range(days)

    # Complete list of transaction statuses
    transaction_status_param = ",".join(TRANSACTION_STATUSES)

    url = f"{BASE_URL}/payments/api/v1/sales/users"
    headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}

    all_sales_users = []
    next_page = None
    page_count = 0

    while True:
        page_count += 1
        params = {
            'max_results': 500,
            'start_date': start_date,
            'end_date': end_date,
            'transaction_status': transaction_status_param
        }

        if next_page:
            params['page_token'] = next_page

        print(f"Getting page {page_count} of sales users...")

        # Implement exponential backoff retry
        max_retries = 5
        retry_delay = 10  # seconds

        for retry in range(max_retries):
            try:
                response = requests.get(url, headers=headers, params=params, timeout=30)

                # Handle rate limit
                if response.status_code == 429:
                    wait_time = 60 * (retry + 1)  # Increase wait time for each retry
                    print(f"Rate limit reached. Waiting {wait_time} seconds...")
                    time.sleep(wait_time)
                    continue

                # Handle other errors
                if response.status_code != 200:
                    print(f"Error on page {page_count}: {response.status_code} - {response.text}")
                    if retry < max_retries - 1:
                        wait_time = retry_delay * (2 ** retry)  # Exponential backoff
                        print(f"Attempt {retry + 1}/{max_retries}. Waiting {wait_time} seconds...")
                        time.sleep(wait_time)
                        continue
                    else:
                        print(f"Failed after {max_retries} attempts.")
                        break

                # Successful request
                data = response.json()
                users = data.get('items', [])

                if not users:
                    print("No more sales users to get.")
                    break

                all_sales_users.extend(users)
                print(f"Received {len(users)} items on this page. Total: {len(all_sales_users)}")

                # Check if there's a next page
                if 'page_info' in data and data['page_info'].get('next_page_token'):
                    next_page = data['page_info']['next_page_token']
                    print(f"Next page token: {next_page[:20]}...")
                else:
                    print("No more pages to get.")
                    break

                # If we got here, the request was successful, exit the retry loop
                break

            except requests.exceptions.RequestException as e:
                if retry < max_retries - 1:
                    wait_time = retry_delay * (2 ** retry)
                    print(f"Connection error: {str(e)}. Attempt {retry + 1}/{max_retries}. Waiting {wait_time} seconds...")
                    time.sleep(wait_time)
                else:
                    print(f"Connection failure after {max_retries} attempts: {str(e)}")
                    return all_sales_users

        # Exit the main loop if there are no more pages or if there was an error
        if 'page_info' not in data or not data['page_info'].get('next_page_token') or (
                retry == max_retries - 1 and response.status_code != 200) or not users:
            break

    print(f"Total sales users obtained: {len(all_sales_users)}")
    return all_sales_users

# Function to extract nested fields from sales users data
def extract_nested_fields_users(sales_users):
    """Extracts nested fields to flat fields, creating a record for each user."""
    flat_data = []

    for item in sales_users:
        # Base transaction and product data
        base_item = {
            'transaction': item.get('transaction', ''),
        }

        # Extract product fields preserving the original key
        if 'product' in item and isinstance(item['product'], dict):
            base_item['product'] = json.dumps(item['product'], ensure_ascii=False)
            # Also extract individual fields for easier analysis
            for key, value in item['product'].items():
                base_item[f'product_{key}'] = value

        # For each user (participant), create a separate record
        if 'users' in item and isinstance(item['users'], list):
            for user_item in item['users']:
                flat_user = base_item.copy()  # Copy the base data

                # Add the user's role (PRODUCER, AFFILIATE, etc.) and preserve the original
                flat_user['role'] = user_item.get('role', '')
                flat_user['user_role'] = user_item.get('role', '')

                # Keep the complete user object as JSON
                if 'user' in user_item:
                    flat_user['user'] = json.dumps(user_item['user'], ensure_ascii=False)

                # Extract user data
                if 'user' in user_item and isinstance(user_item['user'], dict):
                    user_data = user_item['user']

                    # Simple user fields
                    for key in ['ucode', 'locale', 'name', 'trade_name', 'cellphone', 'phone', 'email']:
                        flat_user[f'user_{key}'] = user_data.get(key, '')

                    # User documents (may have multiple)
                    if 'documents' in user_data and isinstance(user_data['documents'], list):
                        # Preserve the complete list of documents as JSON
                        flat_user['user_documents'] = json.dumps(user_data['documents'], ensure_ascii=False)

                        # Create a dictionary to map document types to values
                        doc_map = {}
                        for doc in user_data['documents']:
                            doc_type = doc.get('type', '')
                            doc_value = doc.get('value', '')

                            # If the document type already exists, transform it into a list
                            if doc_type in doc_map:
                                if isinstance(doc_map[doc_type], list):
                                    doc_map[doc_type].append(doc_value)
                                else:
                                    doc_map[doc_type] = [doc_map[doc_type], doc_value]
                            else:
                                doc_map[doc_type] = doc_value

                        # Add documents to the flat item
                        for doc_type, doc_value in doc_map.items():
                            if isinstance(doc_value, list):
                                flat_user[f'user_document_{doc_type.lower()}'] = json.dumps(doc_value,
                                                                                        ensure_ascii=False)
                            else:
                                flat_user[f'user_document_{doc_type.lower()}'] = doc_value

                    # User address
                    if 'address' in user_data and isinstance(user_data['address'], dict):
                        # Preserve the complete address as JSON
                        flat_user['user_address'] = json.dumps(user_data['address'], ensure_ascii=False)

                        # Also extract individual fields
                        address = user_data['address']
                        for key, value in address.items():
                            flat_user[f'user_address_{key}'] = value

                # Add the processed record to the list
                flat_data.append(flat_user)

    return flat_data

# Function to process sales users data
def process_sales_users(sales_users):
    """Processes sales users data and returns a DataFrame."""
    if not sales_users:
        print("No sales users data to process")
        return None

    # Extract nested fields
    flat_data = extract_nested_fields_users(sales_users)

    # Create DataFrame
    df = pd.DataFrame(flat_data)

    return df

# Function to extract subscriptions
def extract_subscriptions(customer):
    """Main function to extract subscriptions and save to GCS."""
    start_time = time.time()
    print(f"Starting subscriptions extraction for {customer['project_id']}...")

    # Define the CSV file path
    csv_filename = "subscriptions/subscriptions.csv"

    # Get access token
    token = get_token(customer['basic_auth'])

    # Get subscriptions
    print("Getting all subscriptions...")
    all_subscriptions = get_all_subscriptions(token)

    if not all_subscriptions:
        print("No subscriptions found")
        return

    # Process and save CSV
    print("Processing subscriptions data for CSV...")
    df = process_subscriptions(all_subscriptions)

    if df is not None and not df.empty:
        # Use the GCS module to upload
        credentials = gcs.load_credentials_from_env()
        local_file_path = f"/tmp/{customer['project_id']}.hotmart.subscriptions.csv"
        df.to_csv(local_file_path, index=False)
        
        gcs.write_file_to_gcs(
            bucket_name=customer['bucket_name'],
            local_file_path=local_file_path,
            destination_name=csv_filename,
            credentials=credentials
        )

        print(f"Total subscriptions obtained: {len(all_subscriptions)}")
        print(f"Process completed in {time.time() - start_time:.2f} seconds")
    else:
        print("No data to process after subscriptions analysis")

# Function to get all subscriptions with pagination
def get_all_subscriptions(token):
    """Gets all subscriptions with pagination."""
    all_subscriptions = []
    url = f"{BASE_URL}/payments/api/v1/subscriptions"
    headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}
    next_page = None
    page_count = 0

    # List of subscription statuses
    subscription_statuses = [
        'ACTIVE', 'CANCELLED', 'DELAYED', 'INACTIVE', 'OVERDUE'
    ]
    status_param = ",".join(subscription_statuses)

    while True:
        page_count += 1
        params = {
            'max_results': 500,
            'status': status_param
        }

        if next_page:
            params['page_token'] = next_page

        print(f"Getting page {page_count} of subscriptions...")
        response = requests.get(url, headers=headers, params=params)

        if response.status_code == 429:  # Rate limit
            print("Rate limit reached. Waiting 60 seconds...")
            time.sleep(60)
            continue

        if response.status_code != 200:
            print(f"Error getting subscriptions: {response.status_code} - {response.text}")
            break

        data = response.json()
        subscriptions = data.get('items', [])

        if not subscriptions:
            print("No more subscriptions to get.")
            break

        all_subscriptions.extend(subscriptions)
        print(f"Received {len(subscriptions)} items on this page. Total: {len(all_subscriptions)}")

        # Check if there's a next page
        if 'page_info' in data and data['page_info'].get('next_page_token'):
            next_page = data['page_info']['next_page_token']
        else:
            break

    print(f"Total subscriptions obtained: {len(all_subscriptions)}")
    return all_subscriptions

# Function to process subscriptions data
def process_subscriptions(subscriptions):
    """Processes subscriptions data and returns a DataFrame."""
    if not subscriptions:
        print("No subscriptions data to process")
        return None

    # Flatten the data
    flat_items = [flatten_json_recursive(item) for item in subscriptions]
    
    # Create DataFrame
    df = pd.DataFrame(flat_items)
    
    # Convert dates
    df = convert_dates(df)
    
    return df

# Function to extract subscriptions summary
def extract_subscriptions_summary(customer):
    """Main function to extract subscriptions summary and save to GCS."""
    start_time = time.time()
    print(f"Starting subscriptions summary extraction for {customer['project_id']}...")

    # Define the CSV file path
    csv_filename = "subscriptions_summary/subscriptions_summary.csv"

    # Get access token
    token = get_token(customer['basic_auth'])

    # Get subscriptions summary
    print("Getting subscriptions summary...")
    summary_data = get_subscriptions_summary(token)

    if not summary_data:
        print("No subscriptions summary found")
        return

    # Process and save CSV
    print("Processing subscriptions summary data for CSV...")
    df = process_subscriptions_summary(summary_data)

    if df is not None and not df.empty:
        # Use the GCS module to upload
        credentials = gcs.load_credentials_from_env()
        local_file_path = f"/tmp/{customer['project_id']}.hotmart.subscriptions_summary.csv"
        df.to_csv(local_file_path, index=False)
        
        gcs.write_file_to_gcs(
            bucket_name=customer['bucket_name'],
            local_file_path=local_file_path,
            destination_name=csv_filename,
            credentials=credentials
        )

        print(f"Process completed in {time.time() - start_time:.2f} seconds")
    else:
        print("No data to process after subscriptions summary analysis")

# Function to get subscriptions summary
def get_subscriptions_summary(token):
    """Gets subscriptions summary from the Hotmart API."""
    url = f"{BASE_URL}/payments/api/v1/subscriptions/summary"
    headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}

    print("Getting subscriptions summary...")
    response = requests.get(url, headers=headers)

    if response.status_code == 429:  # Rate limit
        print("Rate limit reached. Waiting 60 seconds...")
        time.sleep(60)
        return get_subscriptions_summary(token)  # Retry after waiting

    if response.status_code != 200:
        print(f"Error getting subscriptions summary: {response.status_code} - {response.text}")
        return None

    return response.json()

# Function to process subscriptions summary data
def process_subscriptions_summary(summary_data):
    """Processes subscriptions summary data and returns a DataFrame."""
    if not summary_data:
        print("No subscriptions summary data to process")
        return None

    # Create a DataFrame with the summary data
    df = pd.DataFrame([summary_data])
    
    return df

# Function to extract subscriptions transactions
def extract_subscriptions_transactions(customer):
    """Main function to extract subscriptions transactions and save to GCS."""
    start_time = time.time()
    days = int(customer.get('days_to_fetch', 365))
    print(f"Starting subscriptions transactions extraction for {customer['project_id']} for the last {days} days...")

    # Define the CSV file path
    csv_filename = "subscriptions_transactions/subscriptions_transactions.csv"

    # Get access token
    token = get_token(customer['basic_auth'])

    # Get subscriptions transactions
    print(f"Getting all subscriptions transactions for the last {days} days...")
    all_transactions = get_all_subscriptions_transactions(token, customer)

    if not all_transactions:
        print("No subscriptions transactions found")
        return

    # Process and save CSV
    print("Processing subscriptions transactions data for CSV...")
    df = process_subscriptions_transactions(all_transactions)

    if df is not None and not df.empty:
        # Use the GCS module to upload
        credentials = gcs.load_credentials_from_env()
        local_file_path = f"/tmp/{customer['project_id']}.hotmart.subscriptions_transactions.csv"
        df.to_csv(local_file_path, index=False)
        
        gcs.write_file_to_gcs(
            bucket_name=customer['bucket_name'],
            local_file_path=local_file_path,
            destination_name=csv_filename,
            credentials=credentials
        )

        print(f"Total subscriptions transactions obtained: {len(all_transactions)}")
        print(f"Process completed in {time.time() - start_time:.2f} seconds")
    else:
        print("No data to process after subscriptions transactions analysis")

# Function to get all subscriptions transactions with pagination
def get_all_subscriptions_transactions(token, customer):
    """Gets all subscriptions transactions with pagination for the specified days."""
    days = int(customer.get('days_to_fetch', 365))
    
    # Get date range
    start_date, end_date = get_date_range(days)

    url = f"{BASE_URL}/payments/api/v1/subscriptions/transactions"
    headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}

    all_transactions = []
    next_page = None
    page_count = 0

    while True:
        page_count += 1
        params = {
            'max_results': 500,
            'start_date': start_date,
            'end_date': end_date
        }

        if next_page:
            params['page_token'] = next_page

        print(f"Getting page {page_count} of subscriptions transactions...")
        response = requests.get(url, headers=headers, params=params)

        if response.status_code == 429:  # Rate limit
            print("Rate limit reached. Waiting 60 seconds...")
            time.sleep(60)
            continue

        if response.status_code != 200:
            print(f"Error getting subscriptions transactions: {response.status_code} - {response.text}")
            break

        data = response.json()
        transactions = data.get('items', [])

        if not transactions:
            print("No more subscriptions transactions to get.")
            break

        all_transactions.extend(transactions)
        print(f"Received {len(transactions)} items on this page. Total: {len(all_transactions)}")

        # Check if there's a next page
        if 'page_info' in data and data['page_info'].get('next_page_token'):
            next_page = data['page_info']['next_page_token']
        else:
            break

    print(f"Total subscriptions transactions obtained: {len(all_transactions)}")
    return all_transactions

# Function to process subscriptions transactions data
def process_subscriptions_transactions(transactions):
    """Processes subscriptions transactions data and returns a DataFrame."""
    if not transactions:
        print("No subscriptions transactions data to process")
        return None

    # Flatten the data
    flat_items = [flatten_json_recursive(item) for item in transactions]
    
    # Create DataFrame
    df = pd.DataFrame(flat_items)
    
    # Convert dates
    df = convert_dates(df)
    
    return df

# Function to extract sales summary
def extract_sales_summary(customer):
    """Main function to extract sales summary and save to GCS."""
    start_time = time.time()
    print(f"Starting sales summary extraction for {customer['project_id']}...")

    # Define the CSV file path
    csv_filename = "sales_summary/sales_summary.csv"

    # Get access token
    token = get_token(customer['basic_auth'])

    # Get sales summary
    print("Getting sales summary...")
    summary_data = get_sales_summary(token)

    if not summary_data:
        print("No sales summary found")
        return

    # Process and save CSV
    print("Processing sales summary data for CSV...")
    df = process_sales_summary(summary_data)

    if df is not None and not df.empty:
        # Use the GCS module to upload
        credentials = gcs.load_credentials_from_env()
        local_file_path = f"/tmp/{customer['project_id']}.hotmart.sales_summary.csv"
        df.to_csv(local_file_path, index=False)
        
        gcs.write_file_to_gcs(
            bucket_name=customer['bucket_name'],
            local_file_path=local_file_path,
            destination_name=csv_filename,
            credentials=credentials
        )

        print(f"Process completed in {time.time() - start_time:.2f} seconds")
    else:
        print("No data to process after sales summary analysis")

# Function to get sales summary
def get_sales_summary(token):
    """Gets sales summary from the Hotmart API."""
    url = f"{BASE_URL}/payments/api/v1/sales/summary"
    headers = {'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'}

    print("Getting sales summary...")
    response = requests.get(url, headers=headers)

    if response.status_code == 429:  # Rate limit
        print("Rate limit reached. Waiting 60 seconds...")
        time.sleep(60)
        return get_sales_summary(token)  # Retry after waiting

    if response.status_code != 200:
        print(f"Error getting sales summary: {response.status_code} - {response.text}")
        return None

    return response.json()

# Function to process sales summary data
def process_sales_summary(summary_data):
    """Processes sales summary data and returns a DataFrame."""
    if not summary_data:
        print("No sales summary data to process")
        return None

    # Create a DataFrame with the summary data
    df = pd.DataFrame([summary_data])
    
    return df


def get_extraction_tasks():
    """
    Get the list of data extraction tasks for Hotmart.
    
    Returns:
        list: List of task configurations
    """
    return [
        {'task_id': 'extract_products', 'python_callable': extract_products},
        {'task_id': 'extract_sales_history', 'python_callable': extract_sales_history},
        {'task_id': 'extract_sales_users', 'python_callable': extract_sales_users},
        {'task_id': 'extract_subscriptions', 'python_callable': extract_subscriptions},
        {'task_id': 'extract_subscriptions_summary', 'python_callable': extract_subscriptions_summary},
        {'task_id': 'extract_subscriptions_transactions', 'python_callable': extract_subscriptions_transactions},
        {'task_id': 'extract_sales_summary', 'python_callable': extract_sales_summary},
    ]