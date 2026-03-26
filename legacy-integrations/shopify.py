"""
Shopify module for data extraction functions.
This module contains functions specific to the Shopify integration.

Endpoints:
    - orders (+ line_items extraídos como order_items)
    - customers

Auth: X-Shopify-Access-Token header
Pagination: Link header cursor-based (RFC 5988)
API version: 2024-01
"""

import io
import csv
import logging
import re
import time

import pandas as pd
import requests
from core import gcs

BASE_URL_TEMPLATE = "https://{shop_name}.myshopify.com/admin/api/{api_version}"
DEFAULT_API_VERSION = "2024-01"
PAGE_LIMIT = 250  # Shopify max per page


def _get_headers(access_token):
    return {
        "X-Shopify-Access-Token": access_token,
        "Content-Type": "application/json",
    }


def _fetch_all_pages(base_url, endpoint, headers, resource_key, params=None):
    """
    Busca todas as páginas via Link header (cursor-based pagination).
    Shopify retorna o próximo cursor como URL completa no header Link rel="next".
    """
    all_items = []
    url = f"{base_url}/{endpoint}.json"
    request_params = dict(params or {})
    request_params["limit"] = PAGE_LIMIT

    while url:
        try:
            response = requests.get(url, headers=headers, params=request_params, timeout=60)

            # Rate limit (429)
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", 2))
                logging.warning(f"Rate limit (429). Aguardando {retry_after}s...")
                time.sleep(retry_after)
                continue

            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logging.error(f"Erro ao buscar {endpoint}: {e}")
            break

        data = response.json()
        items = data.get(resource_key, [])
        all_items.extend(items)

        # Parse Link header para próxima página
        link_header = response.headers.get("Link", "")
        match = re.search(r'<([^>]+)>;\s*rel="next"', link_header)
        if match:
            url = match.group(1)
            request_params = None  # Params já embutidos na URL
        else:
            url = None

        time.sleep(0.5)  # Respeitar rate limit básico do Shopify (2 req/s)

    logging.info(f"[Shopify] {endpoint}: {len(all_items)} registros")
    return all_items


def _map_orders(raw_orders):
    """Mapeia campos dos orders para formato flat."""
    orders = []
    for o in raw_orders:
        customer = o.get("customer") or {}
        orders.append({
            "id": str(o.get("id", "")),
            "order_number": o.get("order_number"),
            "email": o.get("email"),
            "total_price": o.get("total_price", 0),
            "subtotal_price": o.get("subtotal_price", 0),
            "total_tax": o.get("total_tax", 0),
            "total_discounts": o.get("total_discounts", 0),
            "currency": o.get("currency"),
            "financial_status": o.get("financial_status"),
            "fulfillment_status": o.get("fulfillment_status"),
            "created_at": o.get("created_at"),
            "updated_at": o.get("updated_at"),
            "customer_id": str(customer.get("id", "")) if customer.get("id") else None,
        })
    return orders


def _extract_order_items(raw_orders):
    """Extrai line_items de cada order em registros separados."""
    items = []
    for order in raw_orders:
        order_id = str(order.get("id", ""))
        for line in order.get("line_items", []):
            items.append({
                "id": str(line.get("id", "")),
                "order_id": order_id,
                "variant_id": str(line.get("variant_id", "")),
                "product_id": str(line.get("product_id", "")),
                "title": line.get("title"),
                "quantity": line.get("quantity", 1),
                "price": line.get("price", 0),
                "sku": line.get("sku"),
            })
    return items


def _map_customers(raw_customers):
    """Mapeia campos dos customers para formato flat."""
    customers = []
    for c in raw_customers:
        customers.append({
            "id": str(c.get("id", "")),
            "email": c.get("email"),
            "first_name": c.get("first_name"),
            "last_name": c.get("last_name"),
            "orders_count": c.get("orders_count", 0),
            "total_spent": c.get("total_spent", 0),
            "created_at": c.get("created_at"),
            "updated_at": c.get("updated_at"),
        })
    return customers


def _save_to_gcs(customer, data, file_name):
    """Salva lista de dicts como CSV no GCS."""
    if not data:
        logging.warning(f"[Shopify] Sem dados para {file_name}, pulando upload.")
        return

    df = pd.DataFrame(data)
    local_path = f"/tmp/{customer['project_id']}.shopify.{file_name}.csv"
    df.to_csv(local_path, index=False)

    credentials = gcs.load_credentials_from_env()
    gcs.write_file_to_gcs(
        bucket_name=customer["bucket_name"],
        local_file_path=local_path,
        destination_name=f"{file_name}/{file_name}.csv",
        credentials=credentials,
    )
    logging.info(f"[Shopify] {file_name}: {len(data)} registros salvos no GCS")


def extract_orders(customer):
    """Extrai orders e order_items do Shopify e salva no GCS."""
    start_time = time.time()
    shop_name = customer["shop_name"]
    access_token = customer["access_token"]
    api_version = customer.get("api_version", DEFAULT_API_VERSION)
    base_url = BASE_URL_TEMPLATE.format(shop_name=shop_name, api_version=api_version)
    headers = _get_headers(access_token)

    logging.info(f"[Shopify] Extraindo orders para {customer['project_id']}...")

    # Buscar orders (status=any inclui cancelados)
    raw_orders = _fetch_all_pages(base_url, "orders", headers, "orders", params={"status": "any"})

    # Mapear e salvar orders
    orders = _map_orders(raw_orders)
    _save_to_gcs(customer, orders, "orders")

    # Extrair e salvar order_items (line_items de cada order)
    order_items = _extract_order_items(raw_orders)
    _save_to_gcs(customer, order_items, "order_items")

    logging.info(f"[Shopify] Orders: {len(orders)} pedidos, {len(order_items)} itens em {time.time() - start_time:.2f}s")


def extract_customers(customer):
    """Extrai customers do Shopify e salva no GCS."""
    start_time = time.time()
    shop_name = customer["shop_name"]
    access_token = customer["access_token"]
    api_version = customer.get("api_version", DEFAULT_API_VERSION)
    base_url = BASE_URL_TEMPLATE.format(shop_name=shop_name, api_version=api_version)
    headers = _get_headers(access_token)

    logging.info(f"[Shopify] Extraindo customers para {customer['project_id']}...")

    raw_customers = _fetch_all_pages(base_url, "customers", headers, "customers")
    customers = _map_customers(raw_customers)
    _save_to_gcs(customer, customers, "customers")

    logging.info(f"[Shopify] Customers: {len(customers)} registros em {time.time() - start_time:.2f}s")


def get_extraction_tasks():
    """
    Get the list of data extraction tasks for Shopify.

    Returns:
        list: List of task configurations
    """
    return [
        {"task_id": "extract_orders", "python_callable": extract_orders},
        {"task_id": "extract_customers", "python_callable": extract_customers},
    ]
