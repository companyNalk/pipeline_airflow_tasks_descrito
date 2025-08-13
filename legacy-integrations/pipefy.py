"""
Pipefy module for data extraction functions.
This module contains functions specific to the Pipefy integration.
"""

from core import gcs


def run_cards(customer):
    import os
    import sys
    import csv
    import time
    import json
    import io
    from typing import Optional, List, Dict
    from datetime import datetime
    from dateutil import tz
    import requests
    from google.cloud import storage
    import pathlib

    # ============== Configurações de Ambiente ==============
    PIPEFY_TOKEN = customer['token']
    GCP_BUCKET_NAME = customer['bucket_name']
    GCP_SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GCP_SERVICE_ACCOUNT_PATH
    PIPEFY_ENDPOINT = "https://api.pipefy.com/graphql"

    # ============== Utils ==============
    def log(msg: str) -> None:
        now = datetime.now(tz=tz.gettz("America/Sao_Paulo")).strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{now}] {msg}", flush=True)

    def graphql_request(session: requests.Session, query: str, variables: Optional[Dict] = None,
                        max_retries: int = 5) -> Dict:
        payload = {"query": query}
        if variables:
            payload["variables"] = variables
        attempt = 0
        backoff = 1.5
        while True:
            attempt += 1
            r = session.post(PIPEFY_ENDPOINT, json=payload, timeout=60)
            if r.status_code == 200:
                data = r.json()
                if "errors" in data:
                    raise RuntimeError(f"GraphQL errors: {json.dumps(data['errors'], ensure_ascii=False)}")
                return data["data"]
            elif r.status_code in (429, 500, 502, 503, 504):
                if attempt >= max_retries:
                    raise RuntimeError(f"HTTP {r.status_code} após {attempt} tentativas: {r.text[:500]}")
                wait = backoff ** attempt
                log(f"⚠️ HTTP {r.status_code}. Retentando em {wait:.1f}s (tentativa {attempt}/{max_retries})...")
                time.sleep(wait)
            else:
                raise RuntimeError(f"Falha HTTP {r.status_code}: {r.text[:800]}")

    def init_gcp_client() -> storage.Client:
        """Inicializa cliente do Google Cloud Storage"""
        try:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GCP_SERVICE_ACCOUNT_PATH
            client = storage.Client()
            log(f"✅ Cliente GCP inicializado. Service Account: {GCP_SERVICE_ACCOUNT_PATH}")
            return client
        except Exception as e:
            log(f"❌ Falha ao inicializar cliente GCP: {e}")
            raise

    def upload_csv_to_gcs(client: storage.Client, bucket_name: str, folder_path: str,
                          filename: str, rows: List[Dict], header: List[str]) -> None:
        """Upload CSV diretamente para Google Cloud Storage"""
        try:
            bucket = client.bucket(bucket_name)
            blob_path = f"{folder_path}/{filename}"
            blob = bucket.blob(blob_path)

            # Criar CSV em memória
            csv_buffer = io.StringIO()
            writer = csv.DictWriter(csv_buffer, fieldnames=header, extrasaction="ignore")
            writer.writeheader()
            for row in rows:
                writer.writerow({k: row.get(k, "") for k in header})

            # Upload
            csv_content = csv_buffer.getvalue()
            blob.upload_from_string(csv_content, content_type='text/csv')

            log(f"📤 Upload concluído: gs://{bucket_name}/{blob_path} ({len(rows)} registros)")

        except Exception as e:
            log(f"❌ Falha no upload {filename}: {e}")
            raise

    # ============== Queries ==============
    QUERY_ME = """
    { me { id name email username } }
    """

    QUERY_ORGANIZATIONS = """
    { organizations { id name pipes { id name } } }
    """

    # Cards por pipe (paginação cursor-based)
    QUERY_CARDS_PAGE = """
    query ($pipeId: ID!, $after: String) {
      cards(pipe_id: $pipeId, first: 100, after: $after) {
        edges {
          node {
            id
            title
            suid
            url
            createdAt
            updated_at
            due_date
            done
            late
            overdue
            pipe { id name }
            current_phase { id name }
            assignees { id name email }
            labels { id name color }
            fields {
              field { id label type }
              name
              native_value
              array_value
              date_value
              datetime_value
              float_value
              assignee_values { id name email }
              label_values { id name color }
              filled_at
            }
          }
          cursor
        }
        pageInfo {
          hasNextPage
          endCursor
        }
      }
    }
    """

    # ============== Main ==============
    def main():
        # Validar configurações
        if not PIPEFY_TOKEN:
            log("❌ PIPEFY_TOKEN não configurado!")
            sys.exit(1)
        if not os.path.exists(GCP_SERVICE_ACCOUNT_PATH):
            log(f"❌ Arquivo de service account não encontrado: {GCP_SERVICE_ACCOUNT_PATH}")
            sys.exit(1)

        # Inicializar clientes
        session = requests.Session()
        session.headers.update({
            "Authorization": f"Bearer {PIPEFY_TOKEN}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        })

        gcp_client = init_gcp_client()

        # 1) Validar token
        log("🔐 Validando token (me)...")
        try:
            me = graphql_request(session, QUERY_ME)["me"]
            log(f"✅ Token OK. Usuário: {me.get('name')} ({me.get('email')}) | ID={me.get('id')}")
        except Exception as e:
            log(f"❌ Falha na validação do token: {e}")
            sys.exit(2)

        # 2) Listar organizações e pipes
        log("📦 Buscando organizações e pipes...")
        root = graphql_request(session, QUERY_ORGANIZATIONS)
        orgs = root.get("organizations", []) or []
        total_pipes = sum(len(o.get("pipes") or []) for o in orgs)
        log(f"🔎 Orgs: {len(orgs)} | Pipes totais: {total_pipes}")
        if total_pipes == 0:
            log("⚠️ Nenhum pipe encontrado com este token.")
            sys.exit(0)

        # 3) Iterar pipes e paginar cards
        folder_path = "cards"
        log(f"📁 Pasta de destino: {folder_path}")

        # Buffers CSV
        cards_rows: List[Dict] = []
        card_assignees_rows: List[Dict] = []
        card_labels_rows: List[Dict] = []
        card_fields_rows: List[Dict] = []

        pipe_idx = 0
        for org in orgs:
            org_id = str(org.get("id"))
            org_name = org.get("name")
            pipes = org.get("pipes") or []
            for pipe in pipes:
                pipe_idx += 1
                pipe_id = str(pipe.get("id"))
                pipe_name = pipe.get("name")
                log(f"➡️ Pipe [{pipe_idx}/{total_pipes}] {pipe_name} (ID={pipe_id}) — coletando cards...")

                after = None
                fetched = 0
                while True:
                    data = graphql_request(session, QUERY_CARDS_PAGE, {"pipeId": pipe_id, "after": after})
                    conn = data["cards"]
                    edges = conn.get("edges") or []

                    for edge in edges:
                        c = edge.get("node") or {}
                        card_id = str(c.get("id"))

                        # Linha principal do card
                        cards_rows.append({
                            "card_id": card_id,
                            "title": c.get("title"),
                            "suid": c.get("suid"),
                            "url": c.get("url"),
                            "pipe_id": pipe_id,
                            "pipe_name": pipe_name,
                            "org_id": org_id,
                            "org_name": org_name,
                            "current_phase_id": (c.get("current_phase") or {}).get("id"),
                            "current_phase_name": (c.get("current_phase") or {}).get("name"),
                            "created_at": c.get("createdAt"),
                            "updated_at": c.get("updated_at"),
                            "due_date": c.get("due_date"),
                            "done": c.get("done"),
                            "late": c.get("late"),
                            "overdue": c.get("overdue"),
                        })

                        # Assignees
                        for a in (c.get("assignees") or []):
                            card_assignees_rows.append({
                                "card_id": card_id,
                                "user_id": a.get("id"),
                                "user_name": a.get("name"),
                                "user_email": a.get("email"),
                            })

                        # Labels
                        for lb in (c.get("labels") or []):
                            card_labels_rows.append({
                                "card_id": card_id,
                                "label_id": lb.get("id"),
                                "label_name": lb.get("name"),
                                "label_color": lb.get("color"),
                            })

                        # Fields
                        for f in (c.get("fields") or []):
                            fld = f.get("field") or {}
                            card_fields_rows.append({
                                "card_id": card_id,
                                "field_id": fld.get("id"),
                                "field_label": fld.get("label"),
                                "field_type": fld.get("type"),
                                "name": f.get("name"),
                                "native_value": f.get("native_value"),
                                "array_value": "|".join(f.get("array_value") or []),
                                "date_value": f.get("date_value"),
                                "datetime_value": f.get("datetime_value"),
                                "float_value": f.get("float_value"),
                                "assignee_values": "|".join(
                                    [av.get("id") or "" for av in (f.get("assignee_values") or [])]),
                                "label_values": "|".join([lv.get("id") or "" for lv in (f.get("label_values") or [])]),
                                "filled_at": f.get("filled_at"),
                            })
                        fetched += 1

                    pi = conn.get("pageInfo") or {}
                    if pi.get("hasNextPage"):
                        after = pi.get("endCursor")
                        log(f"   • Cards coletados até agora: {fetched} (paginando...)")
                    else:
                        break

                log(f"   ✅ Cards coletados neste pipe: {fetched}")

        # 4) Upload CSVs para GCS
        log(f"☁️ Fazendo upload para gs://{GCP_BUCKET_NAME}/{folder_path}/...")

        upload_csv_to_gcs(
            gcp_client, GCP_BUCKET_NAME, folder_path, "cards.csv",
            cards_rows,
            [
                "card_id", "title", "suid", "url", "pipe_id", "pipe_name", "org_id", "org_name",
                "current_phase_id", "current_phase_name", "created_at", "updated_at", "due_date",
                "done", "late", "overdue",
            ]
        )

        upload_csv_to_gcs(
            gcp_client, GCP_BUCKET_NAME, folder_path, "card_assignees.csv",
            card_assignees_rows,
            ["card_id", "user_id", "user_name", "user_email"]
        )

        upload_csv_to_gcs(
            gcp_client, GCP_BUCKET_NAME, folder_path, "card_labels.csv",
            card_labels_rows,
            ["card_id", "label_id", "label_name", "label_color"]
        )

        upload_csv_to_gcs(
            gcp_client, GCP_BUCKET_NAME, folder_path, "card_fields.csv",
            card_fields_rows,
            [
                "card_id", "field_id", "field_label", "field_type", "name", "native_value", "array_value",
                "date_value", "datetime_value", "float_value", "assignee_values", "label_values", "filled_at",
            ]
        )

        log("✅ Upload concluído!")
        log(f"   • cards.csv:          {len(cards_rows)}")
        log(f"   • card_assignees.csv: {len(card_assignees_rows)}")
        log(f"   • card_labels.csv:    {len(card_labels_rows)}")
        log(f"   • card_fields.csv:    {len(card_fields_rows)}")

        log("🎉 Processo completo! Todos os dados foram enviados para o GCP Storage.")

    # START
    try:
        main()
    except KeyboardInterrupt:
        log("⏹ Interrompido pelo usuário.")
        sys.exit(130)
    except Exception as e:
        log(f"💥 Erro fatal: {e}")
        sys.exit(1)


def run_fields(customer):
    pass


def run_organizations(customer):
    pass


def run_phases(customer):
    pass


def run_users(customer):
    pass


def get_extraction_tasks():
    """
    Get the list of data extraction tasks for Moskit.

    Returns:
        list: List of task configurations
    """
    return [
        {
            'task_id': 'extract_cards',
            'python_callable': run_cards
        },
        {
            'task_id': 'extract_fields',
            'python_callable': run_fields
        },
        {
            'task_id': 'extract_organizations',
            'python_callable': run_organizations
        },
        {
            'task_id': 'extract_phases',
            'python_callable': run_phases
        },
        {
            'task_id': 'extract_users',
            'python_callable': run_users
        }
    ]
