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
    QUERY_ME = "{ me { id name email username } }"

    # Org + Pipes (para buscar fields de pipes)
    QUERY_ORGS_WITH_PIPES = """
    {
      organizations {
        id
        name
        pipes {
          id
          name
        }
      }
    }
    """

    # Start form fields do pipe
    QUERY_PIPE_START_FORM_FIELDS = """
    query ($pipeId: ID!) {
      pipe(id: $pipeId) {
        id
        name
        start_form_fields {
          id
          label
          type
          required
        }
      }
    }
    """

    # Phase fields (todas as phases do pipe)
    QUERY_PIPE_PHASE_FIELDS = """
    query ($pipeId: ID!) {
      pipe(id: $pipeId){
        id
        name
        phases {
          id
          name
          fields {
            id
            label
            type
            required
          }
        }
      }
    }
    """

    # Tabelas por organização (paginado)
    QUERY_ORG_TABLES_PAGE = """
    query ($orgId: ID!, $after: String) {
      organization(id: $orgId) {
        tables(first: 100, after: $after) {
          edges {
            node {
              id
              name
            }
          }
          pageInfo {
            hasNextPage
            endCursor
          }
        }
      }
    }
    """

    # Fields de uma tabela
    QUERY_TABLE_FIELDS = """
    query ($tableId: ID!) {
      table(id: $tableId) {
        id
        name
        table_fields {
          id
          label
          type
          required
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

        # 1) Validação do token
        log("🔐 Validando token (me)...")
        try:
            me = graphql_request(session, QUERY_ME)["me"]
            log(f"✅ Token OK. Usuário: {me.get('name')} ({me.get('email')}) | ID={me.get('id')}")
        except Exception as e:
            log(f"❌ Falha na validação do token: {e}")
            sys.exit(2)

        # 2) Orgs e Pipes
        log("📦 Buscando organizações e pipes...")
        root = graphql_request(session, QUERY_ORGS_WITH_PIPES)
        orgs = root.get("organizations", []) or []
        total_pipes = sum(len(o.get("pipes") or []) for o in orgs)
        log(f"🔎 Orgs: {len(orgs)} | Pipes totais: {total_pipes}")

        # Pasta de destino
        folder_path = "fields"
        log(f"📁 Pasta de destino: {folder_path}")

        # Buffers CSV
        start_form_rows: List[Dict] = []
        phase_fields_rows: List[Dict] = []
        table_fields_rows: List[Dict] = []

        # 3) Pipes → Start Form & Phase Fields
        pidx = 0
        for org in orgs:
            org_id = str(org.get("id"))
            org_name = org.get("name")
            for pipe in (org.get("pipes") or []):
                pidx += 1
                pipe_id = str(pipe.get("id"))
                pipe_name = pipe.get("name")
                log(f"➡️ Pipe [{pidx}/{total_pipes}] {pipe_name} (ID={pipe_id}) — coletando fields...")

                # Start form fields
                try:
                    sf = graphql_request(session, QUERY_PIPE_START_FORM_FIELDS, {"pipeId": pipe_id})["pipe"]
                    for f in (sf.get("start_form_fields") or []):
                        start_form_rows.append({
                            "organization_id": org_id,
                            "organization_name": org_name,
                            "pipe_id": pipe_id,
                            "pipe_name": pipe_name,
                            "field_id": f.get("id"),
                            "field_label": f.get("label"),
                            "field_type": f.get("type"),
                            "required": f.get("required"),
                        })
                except Exception as e:
                    log(f"   ⚠️ Erro ao coletar start_form_fields do pipe {pipe_id}: {e}")

                # Phase fields (todas as phases)
                try:
                    pf = graphql_request(session, QUERY_PIPE_PHASE_FIELDS, {"pipeId": pipe_id})["pipe"]
                    for ph in (pf.get("phases") or []):
                        phase_id = ph.get("id")
                        phase_name = ph.get("name")
                        for f in (ph.get("fields") or []):
                            phase_fields_rows.append({
                                "organization_id": org_id,
                                "organization_name": org_name,
                                "pipe_id": pipe_id,
                                "pipe_name": pipe_name,
                                "phase_id": phase_id,
                                "phase_name": phase_name,
                                "field_id": f.get("id"),
                                "field_label": f.get("label"),
                                "field_type": f.get("type"),
                                "required": f.get("required"),
                            })
                except Exception as e:
                    log(f"   ⚠️ Erro ao coletar phase fields do pipe {pipe_id}: {e}")

        # 4) Tables → Table Fields (paginado por organização)
        log("🗄️ Coletando tables e seus fields (paginado)...")
        for i, org in enumerate(orgs, start=1):
            org_id = str(org.get("id"))
            org_name = org.get("name")
            after = None
            total_tables = 0
            while True:
                page = graphql_request(session, QUERY_ORG_TABLES_PAGE, {"orgId": org_id, "after": after})
                conn = page["organization"]["tables"]
                edges = conn.get("edges") or []
                if not edges and not conn.get("pageInfo", {}).get("hasNextPage"):
                    break
                for edge in edges:
                    node = edge.get("node") or {}
                    table_id = node.get("id")
                    table_name = node.get("name")
                    total_tables += 1
                    # Table fields
                    try:
                        tf = graphql_request(session, QUERY_TABLE_FIELDS, {"tableId": table_id})["table"]
                        for f in (tf.get("table_fields") or []):
                            table_fields_rows.append({
                                "organization_id": org_id,
                                "organization_name": org_name,
                                "table_id": table_id,
                                "table_name": table_name,
                                "field_id": f.get("id"),
                                "field_label": f.get("label"),
                                "field_type": f.get("type"),
                                "required": f.get("required"),
                            })
                    except Exception as e:
                        log(f"   ⚠️ Erro ao coletar fields da tabela {table_id}: {e}")
                pi = conn.get("pageInfo") or {}
                if pi.get("hasNextPage"):
                    after = pi.get("endCursor")
                    log(f"   • Org {org_name}: paginando tables... coletadas {total_tables}")
                else:
                    break
            log(f"   ✅ Org {org_name}: tables processadas = {total_tables}")

        # 5) Upload CSVs para GCS
        log(f"☁️ Fazendo upload para gs://{GCP_BUCKET_NAME}/{folder_path}/...")

        upload_csv_to_gcs(
            gcp_client, GCP_BUCKET_NAME, folder_path, "pipe_start_form_fields.csv",
            start_form_rows,
            ["organization_id", "organization_name", "pipe_id", "pipe_name", "field_id", "field_label", "field_type",
             "required"]
        )

        upload_csv_to_gcs(
            gcp_client, GCP_BUCKET_NAME, folder_path, "pipe_phase_fields.csv",
            phase_fields_rows,
            ["organization_id", "organization_name", "pipe_id", "pipe_name", "phase_id", "phase_name", "field_id",
             "field_label", "field_type", "required"]
        )

        upload_csv_to_gcs(
            gcp_client, GCP_BUCKET_NAME, folder_path, "table_fields.csv",
            table_fields_rows,
            ["organization_id", "organization_name", "table_id", "table_name", "field_id", "field_label", "field_type",
             "required"]
        )

        log("✅ Upload concluído!")
        log(f"   • pipe_start_form_fields.csv: {len(start_form_rows)}")
        log(f"   • pipe_phase_fields.csv:      {len(phase_fields_rows)}")
        log(f"   • table_fields.csv:           {len(table_fields_rows)}")

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


def run_organizations(customer):
    import os
    import sys
    import csv
    import time
    import json
    import io
    from datetime import datetime
    from dateutil import tz
    from typing import Optional, List, Dict
    import requests
    from google.cloud import storage
    import pathlib

    # ============== Configurações de Ambiente ==============
    PIPEFY_TOKEN = customer['token']
    GCP_BUCKET_NAME = customer['bucket_name']
    GCP_SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GCP_SERVICE_ACCOUNT_PATH
    PIPEFY_ENDPOINT = "https://api.pipefy.com/graphql"

    # ---------- Utilidades ----------
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
            resp = session.post(PIPEFY_ENDPOINT, json=payload, timeout=60)
            if resp.status_code == 200:
                data = resp.json()
                if "errors" in data:
                    raise RuntimeError(f"GraphQL errors: {json.dumps(data['errors'], ensure_ascii=False)}")
                return data["data"]
            elif resp.status_code in (429, 500, 502, 503, 504):
                if attempt >= max_retries:
                    raise RuntimeError(f"HTTP {resp.status_code} após {attempt} tentativas: {resp.text[:500]}")
                wait = backoff ** attempt
                log(f"⚠️ HTTP {resp.status_code}. Tentando novamente em {wait:.1f}s (tentativa {attempt}/{max_retries})...")
                time.sleep(wait)
            else:
                raise RuntimeError(f"Falha HTTP {resp.status_code}: {resp.text[:800]}")

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

    # ---------- Queries ----------
    QUERY_ME = """
    {
      me {
        id
        name
        email
        username
      }
    }
    """

    QUERY_ORGANIZATIONS = """
    {
      organizations {
        id
        name
      }
    }
    """

    QUERY_ORG_DETAILS = """
    query ($orgId: ID!) {
      organization(id: $orgId) {
        id
        name
        planName
        createdAt
        pipes {
          id
          name
        }
        members {
          role_name
          user {
            id
            name
            email
          }
        }
        webhooks {
          id
          name
        }
      }
    }
    """

    QUERY_ORG_TABLES_PAGE = """
    query ($orgId: ID!, $after: String) {
      organization(id: $orgId) {
        tables(first: 100, after: $after) {
          edges {
            node {
              id
              name
              internal_id
            }
            cursor
          }
          pageInfo {
            endCursor
            hasNextPage
          }
        }
      }
    }
    """

    # ---------- Coleta principal ----------
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

        # 1) Validação do token
        log("🔐 Validando token com query 'me'...")
        try:
            me = graphql_request(session, QUERY_ME)["me"]
            log(f"✅ Token válido. Usuário: {me.get('name')} ({me.get('email')}) | ID: {me.get('id')}")
        except Exception as e:
            log(f"❌ Falha ao validar token: {e}")
            sys.exit(2)

        # 2) Obter organizações
        log("📦 Coletando lista de organizações...")
        data_orgs = graphql_request(session, QUERY_ORGANIZATIONS)
        orgs = data_orgs.get("organizations", []) or []
        log(f"🔎 Organizações encontradas: {len(orgs)}")
        if not orgs:
            log("⚠️ Nenhuma organização acessível com este token.")
            sys.exit(0)

        # Pasta de destino
        folder_path = "organizations"
        log(f"📁 Pasta de destino: {folder_path}")

        organizations_rows: List[Dict] = []
        org_pipes_rows: List[Dict] = []
        org_tables_rows: List[Dict] = []
        org_members_rows: List[Dict] = []
        org_webhooks_rows: List[Dict] = []

        for idx, org in enumerate(orgs, start=1):
            org_id = str(org.get("id"))
            org_name = org.get("name")
            log(f"➡️ [{idx}/{len(orgs)}] Organização {org_name} (ID={org_id})")

            details = graphql_request(session, QUERY_ORG_DETAILS, {"orgId": org_id})["organization"]
            organizations_rows.append({
                "organization_id": details.get("id"),
                "organization_name": details.get("name"),
                "plan_name": details.get("planName"),
                "created_at": details.get("createdAt"),
            })

            for p in details.get("pipes") or []:
                org_pipes_rows.append({
                    "organization_id": org_id,
                    "pipe_id": p.get("id"),
                    "pipe_name": p.get("name"),
                })

            for m in details.get("members") or []:
                user = m.get("user") or {}
                org_members_rows.append({
                    "organization_id": org_id,
                    "role_name": m.get("role_name"),
                    "user_id": user.get("id"),
                    "user_name": user.get("name"),
                    "user_email": user.get("email"),
                })

            for w in details.get("webhooks") or []:
                org_webhooks_rows.append({
                    "organization_id": org_id,
                    "webhook_id": w.get("id"),
                    "webhook_name": w.get("name"),
                })

            log("   ↳ Coletando tables com paginação...")
            after = None
            total_tables = 0
            while True:
                page = graphql_request(session, QUERY_ORG_TABLES_PAGE, {"orgId": org_id, "after": after})
                tables_conn = page["organization"]["tables"]
                edges = tables_conn.get("edges") or []
                for edge in edges:
                    node = edge.get("node") or {}
                    org_tables_rows.append({
                        "organization_id": org_id,
                        "table_id": node.get("id"),
                        "table_name": node.get("name"),
                        "table_internal_id": node.get("internal_id"),
                        "cursor": edge.get("cursor"),
                    })
                    total_tables += 1
                page_info = tables_conn.get("pageInfo") or {}
                if page_info.get("hasNextPage"):
                    after = page_info.get("endCursor")
                    log(f"      • Paginando... já coletadas {total_tables} tables (after={after})")
                else:
                    break
            log(f"   ✅ Tables coletadas: {total_tables}")

        # 4) Upload CSVs para GCS
        log(f"☁️ Fazendo upload para gs://{GCP_BUCKET_NAME}/{folder_path}/...")

        upload_csv_to_gcs(
            gcp_client, GCP_BUCKET_NAME, folder_path, "organizations.csv",
            organizations_rows,
            ["organization_id", "organization_name", "plan_name", "created_at"]
        )

        upload_csv_to_gcs(
            gcp_client, GCP_BUCKET_NAME, folder_path, "org_pipes.csv",
            org_pipes_rows,
            ["organization_id", "pipe_id", "pipe_name"]
        )

        upload_csv_to_gcs(
            gcp_client, GCP_BUCKET_NAME, folder_path, "org_tables.csv",
            org_tables_rows,
            ["organization_id", "table_id", "table_name", "table_internal_id", "cursor"]
        )

        upload_csv_to_gcs(
            gcp_client, GCP_BUCKET_NAME, folder_path, "org_members.csv",
            org_members_rows,
            ["organization_id", "role_name", "user_id", "user_name", "user_email"]
        )

        upload_csv_to_gcs(
            gcp_client, GCP_BUCKET_NAME, folder_path, "org_webhooks.csv",
            org_webhooks_rows,
            ["organization_id", "webhook_id", "webhook_name"]
        )

        log("✅ Upload concluído!")
        log(f"   • organizations.csv: {len(organizations_rows)} linhas")
        log(f"   • org_pipes.csv:     {len(org_pipes_rows)} linhas")
        log(f"   • org_tables.csv:    {len(org_tables_rows)} linhas")
        log(f"   • org_members.csv:   {len(org_members_rows)} linhas")
        log(f"   • org_webhooks.csv:  {len(org_webhooks_rows)} linhas")

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


def run_phases(customer):
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
    QUERY_ME = "{ me { id name email username } }"

    # Orgs + Pipes
    QUERY_ORGS_WITH_PIPES = """
    {
      organizations {
        id
        name
        pipes {
          id
          name
        }
      }
    }
    """

    # Fases de um pipe
    QUERY_PIPE_PHASES = """
    query ($pipeId: ID!) {
      pipe(id: $pipeId) {
        id
        name
        phases {
          id
          name
          index
          description
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

        # 2) Buscar organizações e pipes
        log("📦 Buscando organizações e pipes...")
        root = graphql_request(session, QUERY_ORGS_WITH_PIPES)
        orgs = root.get("organizations", []) or []
        total_pipes = sum(len(o.get("pipes") or []) for o in orgs)
        log(f"🔎 Orgs: {len(orgs)} | Pipes totais: {total_pipes}")
        if total_pipes == 0:
            log("⚠️ Nenhum pipe encontrado com este token.")
            sys.exit(0)

        # Pasta de destino
        folder_path = "phases"
        log(f"📁 Pasta de destino: {folder_path}")

        # Buffer CSV
        phase_rows: List[Dict] = []

        # 3) Iterar pipes e coletar phases
        pidx = 0
        for org in orgs:
            org_id = str(org.get("id"))
            org_name = org.get("name")
            for pipe in (org.get("pipes") or []):
                pidx += 1
                pipe_id = str(pipe.get("id"))
                pipe_name = pipe.get("name")
                log(f"➡️ Pipe [{pidx}/{total_pipes}] {pipe_name} (ID={pipe_id}) — coletando phases...")

                try:
                    data = graphql_request(session, QUERY_PIPE_PHASES, {"pipeId": pipe_id})
                    phases = (data.get("pipe") or {}).get("phases") or []
                    for ph in phases:
                        phase_rows.append({
                            "organization_id": org_id,
                            "organization_name": org_name,
                            "pipe_id": pipe_id,
                            "pipe_name": pipe_name,
                            "phase_id": ph.get("id"),
                            "phase_name": ph.get("name"),
                            "phase_index": ph.get("index"),
                            "phase_description": ph.get("description"),
                        })
                    log(f"   ✅ Fases coletadas neste pipe: {len(phases)}")
                except Exception as e:
                    log(f"   ⚠️ Erro ao coletar phases do pipe {pipe_id}: {e}")

        # 4) Upload CSV para GCS
        log(f"☁️ Fazendo upload para gs://{GCP_BUCKET_NAME}/{folder_path}/...")

        upload_csv_to_gcs(
            gcp_client, GCP_BUCKET_NAME, folder_path, "pipe_phases.csv",
            phase_rows,
            [
                "organization_id", "organization_name",
                "pipe_id", "pipe_name",
                "phase_id", "phase_name", "phase_index", "phase_description",
            ]
        )

        log("✅ Upload concluído!")
        log(f"   • pipe_phases.csv: {len(phase_rows)} linhas")

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


def run_users(customer):
    import os
    import sys
    import csv
    import time
    import json
    import io
    from datetime import datetime
    from dateutil import tz
    from typing import Optional, List, Dict
    import requests
    from google.cloud import storage
    import pathlib

    # ============== Configurações de Ambiente ==============
    PIPEFY_TOKEN = customer['token']
    GCP_BUCKET_NAME = customer['bucket_name']
    GCP_SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GCP_SERVICE_ACCOUNT_PATH
    PIPEFY_ENDPOINT = "https://api.pipefy.com/graphql"

    # ---------- Utils ----------
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
            resp = session.post(PIPEFY_ENDPOINT, json=payload, timeout=60)
            if resp.status_code == 200:
                data = resp.json()
                if "errors" in data:
                    raise RuntimeError(f"GraphQL errors: {json.dumps(data['errors'], ensure_ascii=False)}")
                return data["data"]
            elif resp.status_code in (429, 500, 502, 503, 504):
                if attempt >= max_retries:
                    raise RuntimeError(f"HTTP {resp.status_code} após {attempt} tentativas: {resp.text[:500]}")
                wait = backoff ** attempt
                log(f"⚠️ HTTP {resp.status_code}. Retentando em {wait:.1f}s (tentativa {attempt}/{max_retries})...")
                time.sleep(wait)
            else:
                raise RuntimeError(f"Falha HTTP {resp.status_code}: {resp.text[:800]}")

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

    # ---------- Queries ----------
    QUERY_ME = """
    {
      me {
        id
        name
        email
        username
      }
    }
    """

    QUERY_ORGANIZATIONS = """
    {
      organizations {
        id
        name
      }
    }
    """

    # Membros por organização (users / lastSignInAt via membro.user)
    QUERY_ORG_MEMBERS = """
    query ($orgId: ID!) {
      organization(id: $orgId) {
        id
        name
        members {
          billable
          role_name
          user {
            id
            name
            email
            username
            lastSignInAt
            avatar_url
            created_at
          }
        }
      }
    }
    """

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

        # 1) Validação do token
        log("🔐 Validando token (me)...")
        try:
            me = graphql_request(session, QUERY_ME)["me"]
            log(f"✅ Token OK. Usuário: {me.get('name')} ({me.get('email')}) | ID={me.get('id')}")
        except Exception as e:
            log(f"❌ Falha ao validar token: {e}")
            sys.exit(2)

        # 2) Organizações
        log("📦 Buscando organizações...")
        orgs_data = graphql_request(session, QUERY_ORGANIZATIONS)
        orgs = orgs_data.get("organizations", []) or []
        log(f"🔎 Organizações encontradas: {len(orgs)}")
        if not orgs:
            log("⚠️ Nenhuma organização acessível.")
            sys.exit(0)

        # Pasta de destino
        folder_path = "users"
        log(f"📁 Pasta de destino: {folder_path}")

        # Buffers normalizados
        users_map: Dict[str, Dict] = {}  # dedupe por user_id
        org_members_rows: List[Dict] = []

        # 3) Iterar organizações e coletar members
        for i, org in enumerate(orgs, start=1):
            org_id = str(org.get("id"))
            org_name = org.get("name")
            log(f"➡️ [{i}/{len(orgs)}] Organização: {org_name} (ID={org_id})")

            details = graphql_request(session, QUERY_ORG_MEMBERS, {"orgId": org_id})["organization"]
            members = details.get("members") or []
            log(f"   • Members: {len(members)}")

            for m in members:
                u = m.get("user") or {}
                user_id = str(u.get("id") or "")
                # Guardar user deduplicado
                if user_id:
                    users_map[user_id] = {
                        "user_id": user_id,
                        "name": u.get("name"),
                        "email": u.get("email"),
                        "username": u.get("username"),
                        "avatar_url": u.get("avatar_url"),
                        "created_at": u.get("created_at"),
                        "last_sign_in_at": u.get("lastSignInAt"),
                    }
                # Relação org-user (members)
                org_members_rows.append({
                    "organization_id": org_id,
                    "organization_name": org_name,
                    "user_id": user_id,
                    "role_name": m.get("role_name"),
                    "billable": m.get("billable"),
                    "last_sign_in_at": u.get("lastSignInAt"),
                    "user_email": u.get("email"),
                    "user_name": u.get("name"),
                })

        # 4) Upload CSVs para GCS
        log(f"☁️ Fazendo upload para gs://{GCP_BUCKET_NAME}/{folder_path}/...")

        users_rows = list(users_map.values())

        upload_csv_to_gcs(
            gcp_client, GCP_BUCKET_NAME, folder_path, "users.csv",
            users_rows,
            ["user_id", "name", "email", "username", "avatar_url", "created_at", "last_sign_in_at"]
        )

        upload_csv_to_gcs(
            gcp_client, GCP_BUCKET_NAME, folder_path, "org_users_members.csv",
            org_members_rows,
            ["organization_id", "organization_name", "user_id", "role_name", "billable", "last_sign_in_at",
             "user_email",
             "user_name"]
        )

        log("✅ Upload concluído!")
        log(f"   • users.csv: {len(users_rows)} linhas (deduplicado por user_id)")
        log(f"   • org_users_members.csv: {len(org_members_rows)} linhas")

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
