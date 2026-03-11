"""
Clickup NALK module for data extraction functions.
This module contains functions specific to the Clickup NALK integration.
"""

import os
import pathlib

import pandas as pd
import requests
from core import gcs
from google.cloud import storage

BASE_URL = "https://api.clickup.com/api/v2"


def _fetch_all_view_tasks(view_id, api_token):
    headers = {"Authorization": api_token}
    tasks = []
    page = 0

    while True:
        response = requests.get(
            f"{BASE_URL}/view/{view_id}/task",
            headers=headers,
            params={"page": page, "include_closed": "true"},
        )
        if not response.ok:
            raise RuntimeError(
                f"Erro ao buscar tasks (página {page}): {response.status_code} - {response.text}"
            )

        page_tasks = response.json().get("tasks", [])
        if not page_tasks:
            break

        tasks.extend(page_tasks)
        print(f"[INFO] Página {page}: {len(page_tasks)} tasks (total: {len(tasks)})")
        page += 1

    return tasks


def _flatten_task(task):
    row = {
        "id": task.get("id", ""),
        "name": task.get("name", ""),
        "status": (task.get("status") or {}).get("status", ""),
        "priority": (task.get("priority") or {}).get("priority", ""),
        "due_date": task.get("due_date", ""),
        "start_date": task.get("start_date", ""),
        "date_created": task.get("date_created", ""),
        "date_updated": task.get("date_updated", ""),
        "date_closed": task.get("date_closed", ""),
        "creator": (task.get("creator") or {}).get("username", ""),
        "assignees": ", ".join(
            a.get("username", "") for a in task.get("assignees", [])
        ),
        "description": (task.get("description") or "").replace("\n", " ").replace("\r", " "),
        "url": task.get("url", ""),
        "list": (task.get("list") or {}).get("name", ""),
        "folder": (task.get("folder") or {}).get("name", ""),
        "space": (task.get("space") or {}).get("id", ""),
    }

    for cf in task.get("custom_fields", []):
        col = "cf_" + cf.get("name", "").replace(" ", "_").lower()
        value = cf.get("value")
        if value is None:
            row[col] = ""
        elif isinstance(value, list):
            row[col] = ", ".join(
                v.get("name", str(v)) if isinstance(v, dict) else str(v) for v in value
            )
        elif isinstance(value, dict):
            row[col] = value.get("name") or str(value)
        else:
            row[col] = str(value).replace("\n", " ").replace("\r", " ")

    return row


def run(customer):
    API_TOKEN = customer['api_bearer_token']
    BUCKET_NAME = customer['bucket_name']
    VIEW_ID = customer.get('view_id', '8chnhk3-81151')
    SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH
    credentials = gcs.load_credentials_from_env()
    storage_client = storage.Client(credentials=credentials)
    bucket = storage_client.bucket(BUCKET_NAME)

    print("[INFO] Buscando tasks via API oficial ClickUp...")
    tasks = _fetch_all_view_tasks(VIEW_ID, API_TOKEN)

    if not tasks:
        print("[WARN] Nenhuma task encontrada na view.")
        return

    print(f"[INFO] Total de tasks: {len(tasks)}")

    df = pd.DataFrame([_flatten_task(t) for t in tasks])
    for col in df.select_dtypes(include=["object"]).columns:
        df[col] = df[col].astype(str).str.replace('\n', ' ').str.replace('\r', ' ').str.strip()

    csv_content = df.to_csv(index=False, sep=';', lineterminator='\n')

    destination_path = "clientes/clientes.csv"
    blob = bucket.blob(destination_path)
    blob.upload_from_string(csv_content.encode('utf-8'), content_type="text/csv")
    print(f"✅ CSV salvo em gs://{BUCKET_NAME}/{destination_path} ({len(df)} linhas)")


def get_extraction_tasks():
    return [
        {
            'task_id': 'run',
            'python_callable': run
        }
    ]
