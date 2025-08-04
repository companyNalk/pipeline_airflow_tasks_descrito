"""
Local-X module for data extraction functions.
This module contains functions specific to the Local-X integration.
"""

from core import gcs


def run_submissions(customer):
    import csv
    import io
    import json
    import os
    import requests
    import time
    from datetime import datetime
    from google.cloud import storage
    from typing import List, Dict, Optional, Any
    import pathlib


    BASE_URL = customer['base_url']
    LOCATION_ID = customer['location_id']
    API_VERSION = customer['api_version']
    BUCKET_NAME = customer['bucket_name']
    TOKEN_FILE_PATH = "TOKEN/token.json"
    SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH

    access_token = None

    def get_token_from_bucket() -> str:
        print("Obtendo token do bucket GCS...")
        try:
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = SERVICE_ACCOUNT_PATH
            client = storage.Client()
            bucket = client.bucket(BUCKET_NAME)
            blob = bucket.blob(TOKEN_FILE_PATH)

            if not blob.exists():
                print(f"Arquivo {TOKEN_FILE_PATH} nao encontrado no bucket")
                return ""

            token_data = json.loads(blob.download_as_text())
            access_token = token_data.get('access_token', '')
            print("Token obtido do bucket com sucesso")
            return access_token
        except Exception as e:
            print(f"Erro ao obter token do bucket: {e}")
            return ""

    def flatten_nested_dict(data: Any, prefix: str = '') -> Dict[str, Any]:
        """
        Achata dicionários aninhados para CSV
        """
        flattened = {}
        if isinstance(data, dict):
            for key, value in data.items():
                new_key = f"{prefix}_{key}" if prefix else key
                if isinstance(value, (dict, list)):
                    flattened.update(flatten_nested_dict(value, new_key))
                else:
                    flattened[new_key] = value
        elif isinstance(data, list):
            for i, item in enumerate(data):
                new_key = f"{prefix}_{i}" if prefix else str(i)
                if isinstance(item, (dict, list)):
                    flattened.update(flatten_nested_dict(item, new_key))
                else:
                    flattened[new_key] = item
        else:
            flattened[prefix] = data
        return flattened

    def get_forms_submissions_page(page: int = 1, limit: int = 100) -> Optional[Dict]:
        """
        Coleta uma página de form submissions (todos os dados disponíveis)
        """
        global access_token

        if not access_token:
            access_token = get_token_from_bucket()
            if not access_token:
                return None

        headers = {
            'Cache-Control': 'no-cache',
            'Host': 'services.leadconnectorhq.com',
            'User-Agent': 'PostmanRuntime/7.43.0',
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Version': API_VERSION,
            'Authorization': f'Bearer {access_token}'
        }

        params = {
            'locationId': LOCATION_ID,
            'page': page,
            'limit': limit
        }

        try:
            response = requests.get(f"{BASE_URL}/forms/submissions/", headers=headers, params=params)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 401:
                print("Token expirado, obtendo novo token do bucket...")
                new_token = get_token_from_bucket()
                if new_token:
                    access_token = new_token
                    headers['Authorization'] = f'Bearer {new_token}'
                    response = requests.get(f"{BASE_URL}/forms/submissions/", headers=headers, params=params)
                    if response.status_code == 200:
                        return response.json()
                return None
            elif response.status_code == 429:
                print("Rate limit atingido, aguardando...")
                time.sleep(10)
                return get_forms_submissions_page(page, limit)
            else:
                print(f"Erro na requisicao - Status: {response.status_code}")
                return None
        except Exception as e:
            print(f"Erro de conexao: {e}")
            return None

    def collect_all_submissions() -> List[Dict]:
        """
        Coleta todas as form submissions disponíveis (sem filtro de data)
        """
        print("=" * 60)
        print("GoHighLevel - Coletor de Form Submissions (TODOS OS DADOS)")
        print("=" * 60)
        print(f"Execucao: {datetime.now().strftime('%d/%m/%Y as %H:%M:%S')}")
        print(f"Location ID: {LOCATION_ID}")
        print("=" * 60)
        print("Coletando TODAS as form submissions disponiveis (sem filtro de data)")
        print("=" * 60)

        all_submissions = []
        submissions_by_date = {}
        page = 1

        while True:
            print(f"Processando pagina {page}...")

            page_data = get_forms_submissions_page(page=page, limit=100)

            if not page_data:
                print("Erro ao buscar pagina, interrompendo coleta")
                break

            submissions = page_data.get('submissions', [])
            meta = page_data.get('meta', {})

            print(f"  Meta info: Total={meta.get('total', 0)}, CurrentPage={meta.get('currentPage', 0)}")

            if not submissions:
                print("Nenhuma submission encontrada, finalizando coleta")
                break

            # Processa submissions da página
            for submission in submissions:
                # Achata os dados aninhados
                flattened_submission = flatten_nested_dict(submission)
                all_submissions.append(flattened_submission)

                # Agrupa por data para relatório
                created_at = submission.get('createdAt', '')
                if created_at:
                    try:
                        submission_date = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                        date_key = submission_date.strftime('%d/%m/%Y')
                        if date_key not in submissions_by_date:
                            submissions_by_date[date_key] = 0
                        submissions_by_date[date_key] += 1
                    except:
                        pass

            print(f"  Pagina {page}: {len(submissions)} submissions coletadas")
            print(f"  Total acumulado: {len(all_submissions)} submissions")

            # Verifica se há próxima página
            if meta.get('nextPage') is None:
                print("  Ultima pagina processada")
                break

            page += 1
            time.sleep(0.1)  # Rate limiting

        # Mostra relatório por data
        print("=" * 60)
        print("RELATORIO POR DIA:")
        print("=" * 60)

        if submissions_by_date:
            total_shown = 0
            for date_key in sorted(submissions_by_date.keys(), key=lambda x: datetime.strptime(x, '%d/%m/%Y')):
                count = submissions_by_date[date_key]
                total_shown += count
                print(f"coleta de dados {date_key} {count} registros total {total_shown}")
        else:
            print("Nenhuma submission encontrada")

        # Análise do período coletado
        if all_submissions:
            dates = []
            for submission in all_submissions:
                created_at = submission.get('createdAt', '')
                if created_at:
                    try:
                        submission_date = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                        dates.append(submission_date)
                    except:
                        pass

            if dates:
                oldest_date = min(dates)
                newest_date = max(dates)
                period_info = f"{oldest_date.strftime('%d/%m/%Y')} ate {newest_date.strftime('%d/%m/%Y')}"
            else:
                period_info = "Nao foi possivel determinar o periodo"
        else:
            period_info = "Nenhum dado coletado"

        print("=" * 60)
        print("COLETA FINALIZADA!")
        print(f"Total de submissions coletadas: {len(all_submissions)}")
        print(f"Periodo real dos dados: {period_info}")
        print(f"Finalizado em: {datetime.now().strftime('%d/%m/%Y as %H:%M:%S')}")
        print("=" * 60)

        return all_submissions

    def save_submissions_to_bucket(submissions: List[Dict]) -> str:
        """
        Salva as form submissions no bucket GCS
        """
        if not submissions:
            print("Nenhuma submission para salvar")
            return ""

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"submissions.csv"
        bucket_path = f"forms/submissions/{filename}"

        print("=" * 60)
        print("SALVANDO ARQUIVO NO BUCKET")
        print("=" * 60)
        print(f"Endpoint: /forms/submissions/")
        print(f"Arquivo: {filename}")
        print(f"Registros: {len(submissions)} submissions")
        print(f"Destino: gs://{BUCKET_NAME}/{bucket_path}")

        try:
            # Prepara dados CSV em memória
            all_keys = set()
            for submission in submissions:
                all_keys.update(submission.keys())

            print(f"Campos detectados: {len(all_keys)} colunas")

            # Cria CSV em string
            csv_buffer = io.StringIO()
            writer = csv.DictWriter(csv_buffer, fieldnames=sorted(all_keys), delimiter=';')
            writer.writeheader()
            writer.writerows(submissions)
            csv_content = csv_buffer.getvalue()
            csv_buffer.close()

            # Envia para o bucket
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = SERVICE_ACCOUNT_PATH
            client = storage.Client()
            bucket = client.bucket(BUCKET_NAME)
            blob = bucket.blob(bucket_path)
            blob.upload_from_string(csv_content, content_type='text/csv')

            print("Arquivo enviado com sucesso!")
            print(f"Caminho: {bucket_path}")
            print("=" * 60)
            return bucket_path
        except Exception as e:
            print(f"Erro ao enviar CSV para o bucket: {e}")
            print("=" * 60)
            return ""

    def main():
        print("INICIANDO COLETOR DE FORM SUBMISSIONS - GoHighLevel")
        print("=" * 60)

        try:
            global access_token
            print("Obtendo token de acesso...")
            access_token = get_token_from_bucket()
            if not access_token:
                print("Falha ao obter token inicial")
                return
            print("Token obtido com sucesso")

            # Inicia coleta
            submissions = collect_all_submissions()
            if not submissions:
                print("Nenhuma submission coletada")
                return

            # Salva no bucket
            csv_bucket_path = save_submissions_to_bucket(submissions)
            if csv_bucket_path:
                print("PROCESSO CONCLUIDO COM SUCESSO!")
                print(f"Arquivo: {csv_bucket_path}")
                print(f"Total: {len(submissions)} form submissions")
                print(f"Finalizado: {datetime.now().strftime('%d/%m/%Y as %H:%M:%S')}")
            else:
                print("Erro ao salvar arquivo no bucket")

        except KeyboardInterrupt:
            print("\nProcesso interrompido pelo usuario")
        except Exception as e:
            print(f"Erro inesperado: {e}")

        print("=" * 60)

    # START
    main()


def run_contacts(customer):
    import csv
    import io
    import json
    import os
    import requests
    import time
    from datetime import datetime
    from google.cloud import storage
    from typing import List, Dict, Optional, Any
    import pathlib

    LOCATION_ID = customer['location_id']
    BASE_URL = customer['base_url']
    API_VERSION = customer['api_version']
    BUCKET_NAME = customer['bucket_name']
    TOKEN_FILE_PATH = "TOKEN/token.json"
    SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH

    access_token = None

    def get_token_from_bucket() -> str:
        print("Obtendo token do bucket GCS...")
        try:
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = SERVICE_ACCOUNT_PATH
            client = storage.Client()
            bucket = client.bucket(BUCKET_NAME)
            blob = bucket.blob(TOKEN_FILE_PATH)

            if not blob.exists():
                print(f"Arquivo {TOKEN_FILE_PATH} nao encontrado no bucket")
                return ""

            token_data = json.loads(blob.download_as_text())
            access_token = token_data.get('access_token', '')
            print("Token obtido do bucket com sucesso")
            return access_token
        except Exception as e:
            print(f"Erro ao obter token do bucket: {e}")
            return ""

    def flatten_nested_dict(data: Any, prefix: str = '') -> Dict[str, Any]:
        """
        Achata dicionários aninhados para CSV
        """
        flattened = {}
        if isinstance(data, dict):
            for key, value in data.items():
                new_key = f"{prefix}_{key}" if prefix else key
                if isinstance(value, (dict, list)):
                    flattened.update(flatten_nested_dict(value, new_key))
                else:
                    flattened[new_key] = value
        elif isinstance(data, list):
            for i, item in enumerate(data):
                new_key = f"{prefix}_{i}" if prefix else str(i)
                if isinstance(item, (dict, list)):
                    flattened.update(flatten_nested_dict(item, new_key))
                else:
                    flattened[new_key] = item
        else:
            flattened[prefix] = data
        return flattened

    def test_filter_formats() -> Optional[Dict]:
        """
        Testa diferentes formatos de filtro para encontrar o correto
        """
        global access_token

        if not access_token:
            access_token = get_token_from_bucket()
            if not access_token:
                return None

        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Authorization': f'Bearer {access_token}',
            'Version': API_VERSION
        }

        # Testa diferentes formatos
        test_formats = [
            # Formato 1: sem filtro (baseline)
            {
                "name": "sem filtro",
                "body": {
                    "locationId": LOCATION_ID,
                    "pageLimit": 5
                }
            },
            # Formato 2: filtro simples sem data
            {
                "name": "filtro existe email",
                "body": {
                    "locationId": LOCATION_ID,
                    "pageLimit": 5,
                    "filters": [
                        {
                            "field": "email",
                            "operator": "exists"
                        }
                    ]
                }
            },
            # Formato 3: filtro com date_added
            {
                "name": "date_added com gte",
                "body": {
                    "locationId": LOCATION_ID,
                    "pageLimit": 5,
                    "filters": [
                        {
                            "field": "date_added",
                            "operator": "gte",
                            "value": "2024-07-01"
                        }
                    ]
                }
            },
            # Formato 4: dateAdded
            {
                "name": "dateAdded com gte",
                "body": {
                    "locationId": LOCATION_ID,
                    "pageLimit": 5,
                    "filters": [
                        {
                            "field": "dateAdded",
                            "operator": "gte",
                            "value": "2024-07-01"
                        }
                    ]
                }
            },
            # Formato 5: operador diferente
            {
                "name": "date_added com >=",
                "body": {
                    "locationId": LOCATION_ID,
                    "pageLimit": 5,
                    "filters": [
                        {
                            "field": "date_added",
                            "operator": ">=",
                            "value": "2024-07-01"
                        }
                    ]
                }
            }
        ]

        for test_format in test_formats:
            print(f"Testando formato: {test_format['name']}")

            try:
                response = requests.post(f"{BASE_URL}/contacts/search",
                                         headers=headers,
                                         json=test_format['body'])

                print(f"  Status: {response.status_code}")

                if response.status_code == 200:
                    result = response.json()
                    contacts = result.get('contacts', [])
                    print(f"  ✓ Sucesso! {len(contacts)} contatos retornados")
                    print(f"  Total: {result.get('total', 0)}")
                    return test_format  # Retorna o formato que funcionou
                else:
                    print(f"  ✗ Erro: {response.text[:200]}")

            except Exception as e:
                print(f"  ✗ Excecao: {e}")

            time.sleep(0.5)

        return None

    def search_contacts_page(search_after: Optional[List] = None, page_limit: int = 100) -> Optional[Dict]:
        """
        Busca contatos usando o novo endpoint POST /contacts/search
        """
        global access_token

        if not access_token:
            access_token = get_token_from_bucket()
            if not access_token:
                return None

        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'Authorization': f'Bearer {access_token}',
            'Version': API_VERSION
        }

        # Usa formato básico primeiro (sem filtro de data)
        request_body = {
            "locationId": LOCATION_ID,
            "pageLimit": page_limit
        }

        # Adiciona cursor pagination se fornecido
        if search_after:
            request_body["searchAfter"] = search_after
        else:
            # Para primeira página, usa paginação padrão
            request_body["page"] = 1

        try:
            response = requests.post(f"{BASE_URL}/contacts/search",
                                     headers=headers,
                                     json=request_body)

            if response.status_code == 200:
                return response.json()
            elif response.status_code == 401:
                print("Token expirado, obtendo novo token do bucket...")
                new_token = get_token_from_bucket()
                if new_token:
                    access_token = new_token
                    headers['Authorization'] = f'Bearer {new_token}'
                    response = requests.post(f"{BASE_URL}/contacts/search",
                                             headers=headers,
                                             json=request_body)
                    if response.status_code == 200:
                        return response.json()
                return None
            elif response.status_code == 429:
                print("Rate limit atingido, aguardando...")
                time.sleep(10)
                return search_contacts_page(search_after, page_limit)
            else:
                print(f"Erro na requisicao - Status: {response.status_code}")
                print(f"Response: {response.text[:500]}")
                return None
        except Exception as e:
            print(f"Erro de conexao: {e}")
            return None

    def collect_all_contacts() -> List[Dict]:
        """
        Coleta todos os contatos usando o novo endpoint (primeiro testa formatos)
        """
        print("=" * 60)
        print("GoHighLevel - Coletor de Contatos (Novo Endpoint)")
        print("=" * 60)
        print(f"Execucao: {datetime.now().strftime('%d/%m/%Y as %H:%M:%S')}")
        print(f"Location ID: {LOCATION_ID}")
        print("=" * 60)
        print("Endpoint: POST /contacts/search")
        print("Testando formatos de filtro primeiro...")
        print("=" * 60)

        # Primeiro testa os formatos para ver qual funciona
        working_format = test_filter_formats()

        if not working_format:
            print("Nenhum formato de filtro funcionou, usando coleta sem filtro")
            print("=" * 60)
        else:
            print(f"Formato que funcionou: {working_format['name']}")
            print("=" * 60)

        all_contacts = []
        contacts_by_date = {}
        search_after = None
        page_number = 1
        total_from_api = 0

        while True:
            print(f"Processando pagina {page_number}...")

            page_data = search_contacts_page(search_after=search_after, page_limit=100)

            if not page_data:
                print("Erro ao buscar pagina, interrompendo coleta")
                break

            contacts = page_data.get('contacts', [])
            total_from_api = page_data.get('total', 0)

            print(f"  Total na API: {total_from_api}")
            print(f"  Contatos nesta pagina: {len(contacts)}")

            if not contacts:
                print("Nenhum contato encontrado, finalizando coleta")
                break

            # Processa contatos da página e filtra por data no código
            page_contacts_filtered = 0

            for contact in contacts:
                # Filtra por data >= 01/07/2024 no código
                date_added = contact.get('dateAdded', '')
                if date_added:
                    try:
                        contact_date = datetime.fromisoformat(date_added.replace('Z', '+00:00'))
                        # Se for antes de 01/07/2024, pula
                        if contact_date < datetime(2024, 7, 1, tzinfo=contact_date.tzinfo):
                            continue
                    except:
                        pass  # Se erro na data, inclui mesmo assim

                # Achata os dados aninhados
                flattened_contact = flatten_nested_dict(contact)
                all_contacts.append(flattened_contact)
                page_contacts_filtered += 1

                # Agrupa por data para relatório
                if date_added:
                    try:
                        contact_date = datetime.fromisoformat(date_added.replace('Z', '+00:00'))
                        date_key = contact_date.strftime('%d/%m/%Y')
                        if date_key not in contacts_by_date:
                            contacts_by_date[date_key] = 0
                        contacts_by_date[date_key] += 1
                    except:
                        pass

            print(f"  Contatos >= 01/07/2024: {page_contacts_filtered}")
            print(f"  Total acumulado: {len(all_contacts)} contatos")

            # Verifica se há mais páginas
            if len(contacts) < 100:
                print("  Ultima pagina processada (menos de 100 contatos)")
                break

            # Prepara searchAfter para próxima página
            last_contact = contacts[-1]
            search_after = last_contact.get('searchAfter')

            if not search_after:
                print("  Sem searchAfter, finalizando coleta")
                break

            print(f"  SearchAfter para proxima pagina: {search_after}")

            page_number += 1
            time.sleep(0.1)  # Rate limiting

            # Proteção contra loop infinito
            if page_number > 1000:
                print("  Limite de paginas atingido (1000), finalizando")
                break

        # Mostra relatório por data
        print("=" * 60)
        print("RELATORIO POR DIA (>= 01/07/2024):")
        print("=" * 60)

        if contacts_by_date:
            total_shown = 0
            for date_key in sorted(contacts_by_date.keys(), key=lambda x: datetime.strptime(x, '%d/%m/%Y')):
                count = contacts_by_date[date_key]
                total_shown += count
                print(f"coleta de dados {date_key} {count} registros total {total_shown}")
        else:
            print("Nenhum contato encontrado no periodo especificado")

        # Análise do período coletado
        if all_contacts:
            dates = []
            for contact in all_contacts:
                date_added = contact.get('dateAdded', '')
                if date_added:
                    try:
                        contact_date = datetime.fromisoformat(date_added.replace('Z', '+00:00'))
                        dates.append(contact_date)
                    except:
                        pass

            if dates:
                oldest_date = min(dates)
                newest_date = max(dates)
                period_info = f"{oldest_date.strftime('%d/%m/%Y')} ate {newest_date.strftime('%d/%m/%Y')}"
            else:
                period_info = "Nao foi possivel determinar o periodo"
        else:
            period_info = "Nenhum dado coletado"

        print("=" * 60)
        print("COLETA FINALIZADA!")
        print(f"Total de contatos coletados: {len(all_contacts)}")
        print(f"Total informado pela API: {total_from_api}")
        print(f"Periodo real dos dados: {period_info}")
        print(f"Finalizado em: {datetime.now().strftime('%d/%m/%Y as %H:%M:%S')}")
        print("=" * 60)

        return all_contacts

    def save_contacts_to_bucket(contacts: List[Dict]) -> str:
        """
        Salva os contatos no bucket GCS
        """
        if not contacts:
            print("Nenhum contato para salvar")
            return ""

        filename = "search.csv"
        bucket_path = f"contacts/{filename}"

        print("=" * 60)
        print("SALVANDO ARQUIVO NO BUCKET")
        print("=" * 60)
        print(f"Endpoint: POST /contacts/search")
        print(f"Arquivo: {filename}")
        print(f"Registros: {len(contacts)} contatos")
        print(f"Destino: gs://{BUCKET_NAME}/{bucket_path}")

        try:
            # Prepara dados CSV em memória
            all_keys = set()
            for contact in contacts:
                all_keys.update(contact.keys())

            print(f"Campos detectados: {len(all_keys)} colunas")

            # Cria CSV em string
            csv_buffer = io.StringIO()
            writer = csv.DictWriter(csv_buffer, fieldnames=sorted(all_keys), delimiter=';')
            writer.writeheader()
            writer.writerows(contacts)
            csv_content = csv_buffer.getvalue()
            csv_buffer.close()

            # Envia para o bucket
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = SERVICE_ACCOUNT_PATH
            client = storage.Client()
            bucket = client.bucket(BUCKET_NAME)
            blob = bucket.blob(bucket_path)
            blob.upload_from_string(csv_content, content_type='text/csv')

            print("Arquivo enviado com sucesso!")
            print(f"Caminho: {bucket_path}")
            print("=" * 60)
            return bucket_path
        except Exception as e:
            print(f"Erro ao enviar CSV para o bucket: {e}")
            print("=" * 60)
            return ""

    def main():
        print("INICIANDO COLETOR DE CONTATOS (NOVO ENDPOINT) - GoHighLevel")
        print("=" * 60)

        try:
            global access_token
            print("Obtendo token de acesso...")
            access_token = get_token_from_bucket()
            if not access_token:
                print("Falha ao obter token inicial")
                return
            print("Token obtido com sucesso")

            # Inicia coleta
            contacts = collect_all_contacts()
            if not contacts:
                print("Nenhum contato coletado")
                return

            # Salva no bucket
            csv_bucket_path = save_contacts_to_bucket(contacts)
            if csv_bucket_path:
                print("PROCESSO CONCLUIDO COM SUCESSO!")
                print(f"Arquivo: {csv_bucket_path}")
                print(f"Total: {len(contacts)} contatos")
                print(f"Finalizado: {datetime.now().strftime('%d/%m/%Y as %H:%M:%S')}")
            else:
                print("Erro ao salvar arquivo no bucket")

        except KeyboardInterrupt:
            print("\nProcesso interrompido pelo usuario")
        except Exception as e:
            print(f"Erro inesperado: {e}")

        print("=" * 60)

    # START
    main()


def run_funnels(customer):
    import requests
    import csv
    import time
    import json
    import io
    from datetime import datetime
    from typing import List, Dict, Optional, Any
    from google.cloud import storage
    import os
    import pathlib

    BASE_URL = customer['base_url']
    LOCATION_ID = customer['location_id']
    API_VERSION = customer['api_version']
    BUCKET_NAME = customer['bucket_name']
    TOKEN_FILE_PATH = "TOKEN/token.json"
    SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH

    access_token = None

    def get_token_from_bucket() -> str:
        print("Obtendo token do bucket GCS...")
        try:
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = SERVICE_ACCOUNT_PATH
            client = storage.Client()
            bucket = client.bucket(BUCKET_NAME)
            blob = bucket.blob(TOKEN_FILE_PATH)

            if not blob.exists():
                print(f"Arquivo {TOKEN_FILE_PATH} nao encontrado no bucket")
                return ""

            token_data = json.loads(blob.download_as_text())
            access_token = token_data.get('access_token', '')
            print("Token obtido do bucket com sucesso")
            return access_token
        except Exception as e:
            print(f"Erro ao obter token do bucket: {e}")
            return ""

    def flatten_nested_dict(data: Any, prefix: str = '') -> Dict[str, Any]:
        """
        Achata dicionários aninhados para CSV
        """
        flattened = {}
        if isinstance(data, dict):
            for key, value in data.items():
                new_key = f"{prefix}_{key}" if prefix else key
                if isinstance(value, (dict, list)):
                    flattened.update(flatten_nested_dict(value, new_key))
                else:
                    flattened[new_key] = value
        elif isinstance(data, list):
            for i, item in enumerate(data):
                new_key = f"{prefix}_{i}" if prefix else str(i)
                if isinstance(item, (dict, list)):
                    flattened.update(flatten_nested_dict(item, new_key))
                else:
                    flattened[new_key] = item
        else:
            flattened[prefix] = data
        return flattened

    def get_funnels_page(offset: str = "0", limit: str = "100") -> Optional[Dict]:
        """
        Coleta uma página de funnels
        """
        global access_token

        if not access_token:
            access_token = get_token_from_bucket()
            if not access_token:
                return None

        headers = {
            'Cache-Control': 'no-cache',
            'Host': 'services.leadconnectorhq.com',
            'User-Agent': 'PostmanRuntime/7.43.0',
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Version': API_VERSION,
            'Authorization': f'Bearer {access_token}'
        }

        params = {
            'locationId': LOCATION_ID,
            'offset': offset,
            'limit': limit
        }

        try:
            response = requests.get(f"{BASE_URL}/funnels/funnel/list", headers=headers, params=params)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 401:
                print("Token expirado, obtendo novo token do bucket...")
                new_token = get_token_from_bucket()
                if new_token:
                    access_token = new_token
                    headers['Authorization'] = f'Bearer {new_token}'
                    response = requests.get(f"{BASE_URL}/funnels/funnel/list", headers=headers, params=params)
                    if response.status_code == 200:
                        return response.json()
                return None
            elif response.status_code == 429:
                print("Rate limit atingido, aguardando...")
                time.sleep(10)
                return get_funnels_page(offset, limit)
            else:
                print(f"Erro na requisicao - Status: {response.status_code}")
                return None
        except Exception as e:
            print(f"Erro de conexao: {e}")
            return None

    def collect_all_funnels() -> List[Dict]:
        """
        Coleta todos os funnels disponíveis
        """
        print("=" * 60)
        print("GoHighLevel - Coletor de Funnels")
        print("=" * 60)
        print(f"Execucao: {datetime.now().strftime('%d/%m/%Y as %H:%M:%S')}")
        print(f"Location ID: {LOCATION_ID}")
        print("=" * 60)
        print("Coletando TODOS os funnels disponiveis")
        print("=" * 60)

        all_funnels = []
        funnels_by_date = {}
        offset = 0
        limit = 100

        while True:
            print(f"Processando offset {offset}...")

            page_data = get_funnels_page(offset=str(offset), limit=str(limit))

            if not page_data:
                print("Erro ao buscar pagina, interrompendo coleta")
                break

            funnels_data = page_data.get('funnels', [])
            count = page_data.get('count', 0)

            print(f"  Total no endpoint: {count}")

            # Se funnels_data é um dicionário único, transforma em lista
            if isinstance(funnels_data, dict):
                funnels_list = [funnels_data]
            elif isinstance(funnels_data, list):
                funnels_list = funnels_data
            else:
                print("  Formato inesperado dos dados, finalizando coleta")
                break

            if not funnels_list:
                print("Nenhum funnel encontrado, finalizando coleta")
                break

            # Processa funnels da página
            for funnel in funnels_list:
                # Achata os dados aninhados
                flattened_funnel = flatten_nested_dict(funnel)
                all_funnels.append(flattened_funnel)

                # Agrupa por data para relatório
                date_added = funnel.get('dateAdded', '')
                if date_added:
                    try:
                        funnel_date = datetime.fromisoformat(date_added.replace('Z', '+00:00'))
                        date_key = funnel_date.strftime('%d/%m/%Y')
                        if date_key not in funnels_by_date:
                            funnels_by_date[date_key] = 0
                        funnels_by_date[date_key] += 1
                    except:
                        pass

            print(f"  Funnels coletados nesta pagina: {len(funnels_list)}")
            print(f"  Total acumulado: {len(all_funnels)} funnels")

            # Se coletou menos que o limite, é a última página
            if len(funnels_list) < limit:
                print("  Ultima pagina processada")
                break

            offset += limit
            time.sleep(0.1)  # Rate limiting

        # Mostra relatório por data
        print("=" * 60)
        print("RELATORIO POR DIA:")
        print("=" * 60)

        if funnels_by_date:
            total_shown = 0
            for date_key in sorted(funnels_by_date.keys(), key=lambda x: datetime.strptime(x, '%d/%m/%Y')):
                count = funnels_by_date[date_key]
                total_shown += count
                print(f"coleta de dados {date_key} {count} registros total {total_shown}")
        else:
            print("Nenhum funnel encontrado")

        # Análise do período coletado
        if all_funnels:
            dates = []
            for funnel in all_funnels:
                date_added = funnel.get('dateAdded', '')
                if date_added:
                    try:
                        funnel_date = datetime.fromisoformat(date_added.replace('Z', '+00:00'))
                        dates.append(funnel_date)
                    except:
                        pass

            if dates:
                oldest_date = min(dates)
                newest_date = max(dates)
                period_info = f"{oldest_date.strftime('%d/%m/%Y')} ate {newest_date.strftime('%d/%m/%Y')}"
            else:
                period_info = "Nao foi possivel determinar o periodo"
        else:
            period_info = "Nenhum dado coletado"

        print("=" * 60)
        print("COLETA FINALIZADA!")
        print(f"Total de funnels coletados: {len(all_funnels)}")
        print(f"Periodo real dos dados: {period_info}")
        print(f"Finalizado em: {datetime.now().strftime('%d/%m/%Y as %H:%M:%S')}")
        print("=" * 60)

        return all_funnels

    def save_funnels_to_bucket(funnels: List[Dict]) -> str:
        """
        Salva os funnels no bucket GCS
        """
        if not funnels:
            print("Nenhum funnel para salvar")
            return ""

        filename = "list.csv"
        bucket_path = f"funnels/funnel/{filename}"

        print("=" * 60)
        print("SALVANDO ARQUIVO NO BUCKET")
        print("=" * 60)
        print(f"Endpoint: /funnels/funnel/list")
        print(f"Arquivo: {filename}")
        print(f"Registros: {len(funnels)} funnels")
        print(f"Destino: gs://{BUCKET_NAME}/{bucket_path}")

        try:
            # Prepara dados CSV em memória
            all_keys = set()
            for funnel in funnels:
                all_keys.update(funnel.keys())

            print(f"Campos detectados: {len(all_keys)} colunas")

            # Cria CSV em string
            csv_buffer = io.StringIO()
            writer = csv.DictWriter(csv_buffer, fieldnames=sorted(all_keys), delimiter=';')
            writer.writeheader()
            writer.writerows(funnels)
            csv_content = csv_buffer.getvalue()
            csv_buffer.close()

            # Envia para o bucket
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = SERVICE_ACCOUNT_PATH
            client = storage.Client()
            bucket = client.bucket(BUCKET_NAME)
            blob = bucket.blob(bucket_path)
            blob.upload_from_string(csv_content, content_type='text/csv')

            print("Arquivo enviado com sucesso!")
            print(f"Caminho: {bucket_path}")
            print("=" * 60)
            return bucket_path
        except Exception as e:
            print(f"Erro ao enviar CSV para o bucket: {e}")
            print("=" * 60)
            return ""

    def main():
        print("INICIANDO COLETOR DE FUNNELS - GoHighLevel")
        print("=" * 60)

        try:
            global access_token
            print("Obtendo token de acesso...")
            access_token = get_token_from_bucket()
            if not access_token:
                print("Falha ao obter token inicial")
                return
            print("Token obtido com sucesso")

            # Inicia coleta
            funnels = collect_all_funnels()
            if not funnels:
                print("Nenhum funnel coletado")
                return

            # Salva no bucket
            csv_bucket_path = save_funnels_to_bucket(funnels)
            if csv_bucket_path:
                print("PROCESSO CONCLUIDO COM SUCESSO!")
                print(f"Arquivo: {csv_bucket_path}")
                print(f"Total: {len(funnels)} funnels")
                print(f"Finalizado: {datetime.now().strftime('%d/%m/%Y as %H:%M:%S')}")
            else:
                print("Erro ao salvar arquivo no bucket")

        except KeyboardInterrupt:
            print("\nProcesso interrompido pelo usuario")
        except Exception as e:
            print(f"Erro inesperado: {e}")

        print("=" * 60)

    # START
    main()


def run_opps(customer):
    import requests
    import csv
    import time
    import json
    import io
    from datetime import datetime
    from typing import List, Dict, Optional, Any
    from google.cloud import storage
    import os
    import pathlib

    BASE_URL = customer['base_url']
    LOCATION_ID = customer['location_id']
    API_VERSION = customer['api_version']
    BUCKET_NAME = customer['bucket_name']
    TOKEN_FILE_PATH = "TOKEN/token.json"
    SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH

    access_token = None

    def get_token_from_bucket() -> str:
        print("Obtendo token do bucket GCS...")
        try:
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = SERVICE_ACCOUNT_PATH
            client = storage.Client()
            bucket = client.bucket(BUCKET_NAME)
            blob = bucket.blob(TOKEN_FILE_PATH)

            if not blob.exists():
                print(f"Arquivo {TOKEN_FILE_PATH} nao encontrado no bucket")
                return ""

            token_data = json.loads(blob.download_as_text())
            access_token = token_data.get('access_token', '')
            print("Token obtido do bucket com sucesso")
            return access_token
        except Exception as e:
            print(f"Erro ao obter token do bucket: {e}")
            return ""

    def flatten_nested_dict(data: Any, prefix: str = '') -> Dict[str, Any]:
        """
        Achata dicionários aninhados para CSV
        """
        flattened = {}
        if isinstance(data, dict):
            for key, value in data.items():
                new_key = f"{prefix}_{key}" if prefix else key
                if isinstance(value, (dict, list)):
                    flattened.update(flatten_nested_dict(value, new_key))
                else:
                    flattened[new_key] = value
        elif isinstance(data, list):
            for i, item in enumerate(data):
                new_key = f"{prefix}_{i}" if prefix else str(i)
                if isinstance(item, (dict, list)):
                    flattened.update(flatten_nested_dict(item, new_key))
                else:
                    flattened[new_key] = item
        else:
            flattened[prefix] = data
        return flattened

    def get_opportunities_page(page: int = 1, limit: int = 100, start_after_id: Optional[str] = None) -> Optional[Dict]:
        """
        Coleta uma página de opportunities
        """
        global access_token

        if not access_token:
            access_token = get_token_from_bucket()
            if not access_token:
                return None

        headers = {
            'Cache-Control': 'no-cache',
            'Host': 'services.leadconnectorhq.com',
            'User-Agent': 'PostmanRuntime/7.43.0',
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Version': API_VERSION,
            'Authorization': f'Bearer {access_token}'
        }

        params = {
            'location_id': LOCATION_ID,
            'page': page,
            'limit': limit,
            'status': 'all'  # Pega todas: open, won, lost, abandoned
        }

        if start_after_id:
            params['startAfterId'] = start_after_id

        try:
            response = requests.get(f"{BASE_URL}/opportunities/search", headers=headers, params=params)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 401:
                print("Token expirado, obtendo novo token do bucket...")
                new_token = get_token_from_bucket()
                if new_token:
                    access_token = new_token
                    headers['Authorization'] = f'Bearer {new_token}'
                    response = requests.get(f"{BASE_URL}/opportunities/search", headers=headers, params=params)
                    if response.status_code == 200:
                        return response.json()
                return None
            elif response.status_code == 429:
                print("Rate limit atingido, aguardando...")
                time.sleep(10)
                return get_opportunities_page(page, limit, start_after_id)
            else:
                print(f"Erro na requisicao - Status: {response.status_code}")
                print(f"Response: {response.text[:300]}")
                return None
        except Exception as e:
            print(f"Erro de conexao: {e}")
            return None

    def collect_all_opportunities() -> List[Dict]:
        """
        Coleta todas as opportunities disponíveis
        """
        print("=" * 60)
        print("GoHighLevel - Coletor de Opportunities")
        print("=" * 60)
        print(f"Execucao: {datetime.now().strftime('%d/%m/%Y as %H:%M:%S')}")
        print(f"Location ID: {LOCATION_ID}")
        print("=" * 60)
        print("Coletando TODAS as opportunities disponiveis (status: all)")
        print("=" * 60)

        all_opportunities = []
        opportunities_by_date = {}
        opportunities_by_status = {}
        page = 1
        start_after_id = None

        while True:
            print(f"Processando pagina {page}...")

            page_data = get_opportunities_page(page=page, limit=100, start_after_id=start_after_id)

            if not page_data:
                print("Erro ao buscar pagina, interrompendo coleta")
                break

            opportunities = page_data.get('opportunities', [])
            meta = page_data.get('meta', {})

            total = meta.get('total', 0)
            current_page = meta.get('currentPage', page)
            next_page = meta.get('nextPage')

            print(f"  Meta info: Total={total}, CurrentPage={current_page}, NextPage={next_page}")

            if not opportunities:
                print("Nenhuma opportunity encontrada, finalizando coleta")
                break

            # Processa opportunities da página
            for opportunity in opportunities:
                # Achata os dados aninhados
                flattened_opportunity = flatten_nested_dict(opportunity)
                all_opportunities.append(flattened_opportunity)

                # Agrupa por data para relatório
                created_at = opportunity.get('createdAt', '')
                if created_at:
                    try:
                        opportunity_date = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                        date_key = opportunity_date.strftime('%d/%m/%Y')
                        if date_key not in opportunities_by_date:
                            opportunities_by_date[date_key] = 0
                        opportunities_by_date[date_key] += 1
                    except:
                        pass

                # Agrupa por status
                status = opportunity.get('status', 'unknown')
                if status not in opportunities_by_status:
                    opportunities_by_status[status] = 0
                opportunities_by_status[status] += 1

            print(f"  Pagina {page}: {len(opportunities)} opportunities coletadas")
            print(f"  Total acumulado: {len(all_opportunities)} opportunities")

            # Verifica se há próxima página
            if not next_page:
                print("  Ultima pagina processada")
                break

            # Prepara para próxima página
            if opportunities:
                start_after_id = opportunities[-1].get('id')

            page += 1
            time.sleep(0.1)  # Rate limiting

        # Mostra relatório por data
        print("=" * 60)
        print("RELATORIO POR DIA:")
        print("=" * 60)

        if opportunities_by_date:
            total_shown = 0
            for date_key in sorted(opportunities_by_date.keys(), key=lambda x: datetime.strptime(x, '%d/%m/%Y')):
                count = opportunities_by_date[date_key]
                total_shown += count
                print(f"coleta de dados {date_key} {count} registros total {total_shown}")
        else:
            print("Nenhuma opportunity encontrada")

        # Mostra relatório por status
        print("=" * 60)
        print("RELATORIO POR STATUS:")
        print("=" * 60)

        for status, count in opportunities_by_status.items():
            print(f"Status {status}: {count} opportunities")

        # Análise do período coletado
        if all_opportunities:
            dates = []
            for opportunity in all_opportunities:
                created_at = opportunity.get('createdAt', '')
                if created_at:
                    try:
                        opportunity_date = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                        dates.append(opportunity_date)
                    except:
                        pass

            if dates:
                oldest_date = min(dates)
                newest_date = max(dates)
                period_info = f"{oldest_date.strftime('%d/%m/%Y')} ate {newest_date.strftime('%d/%m/%Y')}"
            else:
                period_info = "Nao foi possivel determinar o periodo"
        else:
            period_info = "Nenhum dado coletado"

        print("=" * 60)
        print("COLETA FINALIZADA!")
        print(f"Total de opportunities coletadas: {len(all_opportunities)}")
        print(f"Periodo real dos dados: {period_info}")
        print(f"Finalizado em: {datetime.now().strftime('%d/%m/%Y as %H:%M:%S')}")
        print("=" * 60)

        return all_opportunities

    def save_opportunities_to_bucket(opportunities: List[Dict]) -> str:
        """
        Salva as opportunities no bucket GCS
        """
        if not opportunities:
            print("Nenhuma opportunity para salvar")
            return ""

        filename = "search_opps.csv"
        bucket_path = f"opportunities/{filename}"

        print("=" * 60)
        print("SALVANDO ARQUIVO NO BUCKET")
        print("=" * 60)
        print(f"Endpoint: /opportunities/search")
        print(f"Arquivo: {filename}")
        print(f"Registros: {len(opportunities)} opportunities")
        print(f"Destino: gs://{BUCKET_NAME}/{bucket_path}")

        try:
            # Prepara dados CSV em memória
            all_keys = set()
            for opportunity in opportunities:
                all_keys.update(opportunity.keys())

            print(f"Campos detectados: {len(all_keys)} colunas")

            # Cria CSV em string
            csv_buffer = io.StringIO()
            writer = csv.DictWriter(csv_buffer, fieldnames=sorted(all_keys), delimiter=';')
            writer.writeheader()
            writer.writerows(opportunities)
            csv_content = csv_buffer.getvalue()
            csv_buffer.close()

            # Envia para o bucket
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = SERVICE_ACCOUNT_PATH
            client = storage.Client()
            bucket = client.bucket(BUCKET_NAME)
            blob = bucket.blob(bucket_path)
            blob.upload_from_string(csv_content, content_type='text/csv')

            print("Arquivo enviado com sucesso!")
            print(f"Caminho: {bucket_path}")
            print("=" * 60)
            return bucket_path
        except Exception as e:
            print(f"Erro ao enviar CSV para o bucket: {e}")
            print("=" * 60)
            return ""

    def main():
        print("INICIANDO COLETOR DE OPPORTUNITIES - GoHighLevel")
        print("=" * 60)

        try:
            global access_token
            print("Obtendo token de acesso...")
            access_token = get_token_from_bucket()
            if not access_token:
                print("Falha ao obter token inicial")
                return
            print("Token obtido com sucesso")

            # Inicia coleta
            opportunities = collect_all_opportunities()
            if not opportunities:
                print("Nenhuma opportunity coletada")
                return

            # Salva no bucket
            csv_bucket_path = save_opportunities_to_bucket(opportunities)
            if csv_bucket_path:
                print("PROCESSO CONCLUIDO COM SUCESSO!")
                print(f"Arquivo: {csv_bucket_path}")
                print(f"Total: {len(opportunities)} opportunities")
                print(f"Finalizado: {datetime.now().strftime('%d/%m/%Y as %H:%M:%S')}")
            else:
                print("Erro ao salvar arquivo no bucket")

        except KeyboardInterrupt:
            print("\nProcesso interrompido pelo usuario")
        except Exception as e:
            print(f"Erro inesperado: {e}")

        print("=" * 60)

    # START
    main()


def run_pipelines_opps(customer):
    import requests
    import csv
    import time
    import json
    import io
    from datetime import datetime
    from typing import List, Dict, Optional, Any
    from google.cloud import storage
    import os
    import pathlib

    BASE_URL = customer['base_url']
    LOCATION_ID = customer['location_id']
    API_VERSION = customer['api_version']
    BUCKET_NAME = customer['bucket_name']
    TOKEN_FILE_PATH = "TOKEN/token.json"
    SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH

    access_token = None

    def get_token_from_bucket() -> str:
        print("Obtendo token do bucket GCS...")
        try:
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = SERVICE_ACCOUNT_PATH
            client = storage.Client()
            bucket = client.bucket(BUCKET_NAME)
            blob = bucket.blob(TOKEN_FILE_PATH)

            if not blob.exists():
                print(f"Arquivo {TOKEN_FILE_PATH} nao encontrado no bucket")
                return ""

            token_data = json.loads(blob.download_as_text())
            access_token = token_data.get('access_token', '')
            print("Token obtido do bucket com sucesso")
            return access_token
        except Exception as e:
            print(f"Erro ao obter token do bucket: {e}")
            return ""

    def flatten_nested_dict(data: Any, prefix: str = '') -> Dict[str, Any]:
        """
        Achata dicionários aninhados para CSV
        """
        flattened = {}
        if isinstance(data, dict):
            for key, value in data.items():
                new_key = f"{prefix}_{key}" if prefix else key
                if isinstance(value, (dict, list)):
                    flattened.update(flatten_nested_dict(value, new_key))
                else:
                    flattened[new_key] = value
        elif isinstance(data, list):
            for i, item in enumerate(data):
                new_key = f"{prefix}_{i}" if prefix else str(i)
                if isinstance(item, (dict, list)):
                    flattened.update(flatten_nested_dict(item, new_key))
                else:
                    flattened[new_key] = item
        else:
            flattened[prefix] = data
        return flattened

    def get_pipelines() -> Optional[Dict]:
        """
        Coleta todos os pipelines (não tem paginação)
        """
        global access_token

        if not access_token:
            access_token = get_token_from_bucket()
            if not access_token:
                return None

        headers = {
            'Cache-Control': 'no-cache',
            'Host': 'services.leadconnectorhq.com',
            'User-Agent': 'PostmanRuntime/7.43.0',
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Version': API_VERSION,
            'Authorization': f'Bearer {access_token}'
        }

        params = {
            'locationId': LOCATION_ID
        }

        try:
            response = requests.get(f"{BASE_URL}/opportunities/pipelines", headers=headers, params=params)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 401:
                print("Token expirado, obtendo novo token do bucket...")
                new_token = get_token_from_bucket()
                if new_token:
                    access_token = new_token
                    headers['Authorization'] = f'Bearer {new_token}'
                    response = requests.get(f"{BASE_URL}/opportunities/pipelines", headers=headers, params=params)
                    if response.status_code == 200:
                        return response.json()
                return None
            elif response.status_code == 429:
                print("Rate limit atingido, aguardando...")
                time.sleep(10)
                return get_pipelines()
            else:
                print(f"Erro na requisicao - Status: {response.status_code}")
                print(f"Response: {response.text[:300]}")
                return None
        except Exception as e:
            print(f"Erro de conexao: {e}")
            return None

    def collect_all_pipelines() -> List[Dict]:
        """
        Coleta todos os pipelines disponíveis
        """
        print("=" * 60)
        print("GoHighLevel - Coletor de Pipelines")
        print("=" * 60)
        print(f"Execucao: {datetime.now().strftime('%d/%m/%Y as %H:%M:%S')}")
        print(f"Location ID: {LOCATION_ID}")
        print("=" * 60)
        print("Coletando TODOS os pipelines disponiveis")
        print("=" * 60)

        # Faz a requisição única (sem paginação)
        print("Fazendo requisicao para /opportunities/pipelines...")

        response_data = get_pipelines()

        if not response_data:
            print("Erro ao buscar pipelines")
            return []

        pipelines_data = response_data.get('pipelines', [])

        if not pipelines_data:
            print("Nenhum pipeline encontrado")
            return []

        print(f"Encontrados: {len(pipelines_data)} pipelines")

        all_pipelines = []

        # Processa todos os pipelines
        for i, pipeline in enumerate(pipelines_data):
            print(f"  Pipeline {i + 1}: {pipeline.get('name', 'Sem nome')} (ID: {pipeline.get('id', 'N/A')})")

            # Achata os dados aninhados
            flattened_pipeline = flatten_nested_dict(pipeline)
            all_pipelines.append(flattened_pipeline)

        print("=" * 60)
        print("DETALHES DOS PIPELINES:")
        print("=" * 60)

        for i, pipeline in enumerate(pipelines_data):
            name = pipeline.get('name', 'Sem nome')
            pipeline_id = pipeline.get('id', 'N/A')
            stages = pipeline.get('stages', [])
            show_funnel = pipeline.get('showInFunnel', False)
            show_pie = pipeline.get('showInPieChart', False)
            location_id = pipeline.get('locationId', 'N/A')

            print(f"Pipeline {i + 1}:")
            print(f"  Nome: {name}")
            print(f"  ID: {pipeline_id}")
            print(f"  Location ID: {location_id}")
            print(f"  Stages: {len(stages)} configurados")
            print(f"  Mostrar no Funil: {show_funnel}")
            print(f"  Mostrar no Grafico Pizza: {show_pie}")
            print("-" * 40)

        print("=" * 60)
        print("COLETA FINALIZADA!")
        print(f"Total de pipelines coletados: {len(all_pipelines)}")
        print(f"Finalizado em: {datetime.now().strftime('%d/%m/%Y as %H:%M:%S')}")
        print("=" * 60)

        return all_pipelines

    def save_pipelines_to_bucket(pipelines: List[Dict]) -> str:
        """
        Salva os pipelines no bucket GCS
        """
        if not pipelines:
            print("Nenhum pipeline para salvar")
            return ""

        filename = "pipelines_opps.csv"
        bucket_path = f"opportunities/{filename}"

        print("=" * 60)
        print("SALVANDO ARQUIVO NO BUCKET")
        print("=" * 60)
        print(f"Endpoint: /opportunities/pipelines")
        print(f"Arquivo: {filename}")
        print(f"Registros: {len(pipelines)} pipelines")
        print(f"Destino: gs://{BUCKET_NAME}/{bucket_path}")

        try:
            # Prepara dados CSV em memória
            all_keys = set()
            for pipeline in pipelines:
                all_keys.update(pipeline.keys())

            print(f"Campos detectados: {len(all_keys)} colunas")

            # Cria CSV em string
            csv_buffer = io.StringIO()
            writer = csv.DictWriter(csv_buffer, fieldnames=sorted(all_keys), delimiter=';')
            writer.writeheader()
            writer.writerows(pipelines)
            csv_content = csv_buffer.getvalue()
            csv_buffer.close()

            # Envia para o bucket
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = SERVICE_ACCOUNT_PATH
            client = storage.Client()
            bucket = client.bucket(BUCKET_NAME)
            blob = bucket.blob(bucket_path)
            blob.upload_from_string(csv_content, content_type='text/csv')

            print("Arquivo enviado com sucesso!")
            print(f"Caminho: {bucket_path}")
            print("=" * 60)
            return bucket_path
        except Exception as e:
            print(f"Erro ao enviar CSV para o bucket: {e}")
            print("=" * 60)
            return ""

    def main():
        print("INICIANDO COLETOR DE PIPELINES - GoHighLevel")
        print("=" * 60)

        try:
            global access_token
            print("Obtendo token de acesso...")
            access_token = get_token_from_bucket()
            if not access_token:
                print("Falha ao obter token inicial")
                return
            print("Token obtido com sucesso")

            # Inicia coleta
            pipelines = collect_all_pipelines()
            if not pipelines:
                print("Nenhum pipeline coletado")
                return

            # Salva no bucket
            csv_bucket_path = save_pipelines_to_bucket(pipelines)
            if csv_bucket_path:
                print("PROCESSO CONCLUIDO COM SUCESSO!")
                print(f"Arquivo: {csv_bucket_path}")
                print(f"Total: {len(pipelines)} pipelines")
                print(f"Finalizado: {datetime.now().strftime('%d/%m/%Y as %H:%M:%S')}")
            else:
                print("Erro ao salvar arquivo no bucket")

        except KeyboardInterrupt:
            print("\nProcesso interrompido pelo usuario")
        except Exception as e:
            print(f"Erro inesperado: {e}")

        print("=" * 60)

    # START
    main()


def run_products_price(customer):
    import requests
    import csv
    import time
    import json
    import io
    from datetime import datetime
    from typing import List, Dict, Optional, Any
    from google.cloud import storage
    import os
    import pathlib

    BASE_URL = customer['base_url']
    LOCATION_ID = customer['location_id']
    API_VERSION = customer['api_version']
    BUCKET_NAME = customer['bucket_name']
    TOKEN_FILE_PATH = "TOKEN/token.json"
    SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH

    # Configurações específicas para coleta
    SEARCH_PRODUCT_NAME = None  # Ex: "Awesome product" ou None para todos
    COLLECTION_IDS_FILTER = None  # Ex: "65d71377c326ea78e1c47df5,65d71377c326ea78e1c47d34" ou None
    AVAILABLE_IN_STORE_ONLY = None  # True, False ou None
    COLLECT_PRICES_FOR_PRODUCTS = True  # Se deve coletar preços para cada produto encontrado

    access_token = None

    def get_token_from_bucket() -> str:
        """
        Obtém token de acesso do bucket GCS
        """
        print("Obtendo token do bucket GCS...")
        try:
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = SERVICE_ACCOUNT_PATH
            client = storage.Client()
            bucket = client.bucket(BUCKET_NAME)
            blob = bucket.blob(TOKEN_FILE_PATH)

            if not blob.exists():
                print(f"Arquivo {TOKEN_FILE_PATH} nao encontrado no bucket")
                return ""

            token_data = json.loads(blob.download_as_text())
            access_token = token_data.get('access_token', '')
            print("Token obtido do bucket com sucesso")
            return access_token
        except Exception as e:
            print(f"Erro ao obter token do bucket: {e}")
            return ""

    def flatten_nested_dict(data: Any, prefix: str = '') -> Dict[str, Any]:
        """
        Achata dicionários aninhados para CSV
        """
        flattened = {}
        if isinstance(data, dict):
            for key, value in data.items():
                new_key = f"{prefix}_{key}" if prefix else key
                if isinstance(value, (dict, list)):
                    flattened.update(flatten_nested_dict(value, new_key))
                else:
                    flattened[new_key] = value
        elif isinstance(data, list):
            for i, item in enumerate(data):
                new_key = f"{prefix}_{i}" if prefix else str(i)
                if isinstance(item, (dict, list)):
                    flattened.update(flatten_nested_dict(item, new_key))
                else:
                    flattened[new_key] = item
        else:
            flattened[prefix] = data
        return flattened

    def get_products_page(offset: int = 0, limit: int = 100, debug: bool = False) -> Optional[Dict]:
        """
        Busca produtos usando GET /products/
        """
        global access_token

        if not access_token:
            access_token = get_token_from_bucket()
            if not access_token:
                return None

        headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {access_token}',
            'Version': API_VERSION
        }

        # Monta parâmetros da query
        params = {
            'locationId': LOCATION_ID,
            'limit': limit,
            'offset': offset
        }

        # Remove expand inicialmente para simplificar
        # 'expand': ['tax', 'stripe', 'paypal']  # Busca dados completos

        # Adiciona filtros opcionais
        if SEARCH_PRODUCT_NAME:
            params['search'] = SEARCH_PRODUCT_NAME

        if COLLECTION_IDS_FILTER:
            params['collectionIds'] = COLLECTION_IDS_FILTER

        if AVAILABLE_IN_STORE_ONLY is not None:
            params['availableInStore'] = AVAILABLE_IN_STORE_ONLY

        try:
            url = f"{BASE_URL}/products/"

            if debug:
                print(f"DEBUG - URL: {url}")
                print(f"DEBUG - Params: {params}")
                print(f"DEBUG - Headers: {headers}")

            response = requests.get(url, headers=headers, params=params)

            if debug:
                print(f"DEBUG - Status Code: {response.status_code}")
                print(f"DEBUG - Response Text: {response.text[:1000]}")

            if response.status_code == 200:
                return response.json()
            elif response.status_code == 401:
                print("Token expirado, obtendo novo token do bucket...")
                new_token = get_token_from_bucket()
                if new_token:
                    access_token = new_token
                    headers['Authorization'] = f'Bearer {new_token}'
                    response = requests.get(url, headers=headers, params=params)
                    if response.status_code == 200:
                        return response.json()
                return None
            elif response.status_code == 429:
                print("Rate limit atingido, aguardando...")
                time.sleep(10)
                return get_products_page(offset, limit, debug)
            else:
                print(f"Erro na requisicao - Status: {response.status_code}")
                print(f"Response: {response.text[:500]}")
                return None
        except Exception as e:
            print(f"Erro de conexao: {e}")
            return None

    def get_product_prices_page(product_id: str, offset: int = 0, limit: int = 100) -> Optional[Dict]:
        """
        Busca preços de um produto específico usando GET /products/{productId}/price
        """
        global access_token

        if not access_token:
            access_token = get_token_from_bucket()
            if not access_token:
                return None

        headers = {
            'Accept': 'application/json',
            'Authorization': f'Bearer {access_token}',
            'Version': API_VERSION
        }

        params = {
            'locationId': LOCATION_ID,
            'limit': limit,
            'offset': offset
        }

        try:
            url = f"{BASE_URL}/products/{product_id}/price"
            response = requests.get(url, headers=headers, params=params)

            if response.status_code == 200:
                return response.json()
            elif response.status_code == 401:
                print("Token expirado, obtendo novo token do bucket...")
                new_token = get_token_from_bucket()
                if new_token:
                    access_token = new_token
                    headers['Authorization'] = f'Bearer {new_token}'
                    response = requests.get(url, headers=headers, params=params)
                    if response.status_code == 200:
                        return response.json()
                return None
            elif response.status_code == 429:
                print("Rate limit atingido, aguardando...")
                time.sleep(10)
                return get_product_prices_page(product_id, offset, limit)
            else:
                print(f"Erro na requisicao de precos - Status: {response.status_code}")
                if response.status_code != 404:  # 404 é normal se produto não tem preços
                    print(f"Response: {response.text[:500]}")
                return None
        except Exception as e:
            print(f"Erro de conexao ao buscar precos: {e}")
            return None

    def test_products_connection() -> bool:
        """
        Testa diferentes configurações para encontrar produtos
        """
        print("=" * 60)
        print("TESTANDO CONEXAO COM ENDPOINT DE PRODUTOS")
        print("=" * 60)

        # Teste 1: Requisição básica
        print("Teste 1: Requisicao basica...")
        test_data = get_products_page(offset=0, limit=5, debug=True)

        if test_data:
            products = test_data.get('products', [])
            total_info = test_data.get('total', [])
            total = total_info[0].get('total', 0) if total_info and isinstance(total_info, list) and len(
                total_info) > 0 else 0

            print(f"✓ Conexao bem-sucedida!")
            print(f"  Produtos retornados: {len(products)}")
            print(f"  Total disponivel: {total}")

            if len(products) == 0 and total == 0:
                print("  ⚠️  Nenhum produto encontrado na conta")

                # Teste 2: Sem locationId para ver se é um problema de location
                print("\nTeste 2: Verificando se o locationId esta correto...")
                global access_token
                headers = {
                    'Accept': 'application/json',
                    'Authorization': f'Bearer {access_token}',
                    'Version': API_VERSION
                }

                # Testa sem locationId
                try:
                    response = requests.get(f"{BASE_URL}/products/", headers=headers, params={'limit': 5})
                    print(f"  Sem locationId - Status: {response.status_code}")
                    if response.status_code == 200:
                        data = response.json()
                        print(f"  Produtos sem filtro: {len(data.get('products', []))}")
                    else:
                        print(f"  Response: {response.text[:300]}")
                except Exception as e:
                    print(f"  Erro: {e}")

                # Teste 3: Diferentes valores de locationId (teste comum)
                print("\nTeste 3: Verificando se locationId precisa ser diferente...")
                test_locations = [
                    None,  # sem locationId
                    "",  # vazio
                    "3SwdhCsvxI8Au3KsPJt6",  # exemplo da documentação
                ]

                for loc in test_locations:
                    try:
                        params = {'limit': 5}
                        if loc is not None:
                            params['locationId'] = loc

                        response = requests.get(f"{BASE_URL}/products/", headers=headers, params=params)
                        if response.status_code == 200:
                            data = response.json()
                            products_count = len(data.get('products', []))
                            total_count = data.get('total', [{}])[0].get('total', 0) if data.get('total') else 0
                            print(f"  LocationId '{loc}': {products_count} produtos (total: {total_count})")
                        else:
                            print(f"  LocationId '{loc}': Status {response.status_code}")
                    except Exception as e:
                        print(f"  LocationId '{loc}': Erro {e}")

            return len(products) > 0
        else:
            print("✗ Erro na conexao com o endpoint")
            return False

    def collect_all_products() -> List[Dict]:
        """
        Coleta todos os produtos
        """
        print("=" * 60)
        print("GoHighLevel - Coletor de Produtos")
        print("=" * 60)
        print(f"Execucao: {datetime.now().strftime('%d/%m/%Y as %H:%M:%S')}")
        print(f"Location ID: {LOCATION_ID}")
        if SEARCH_PRODUCT_NAME:
            print(f"Filtro nome: {SEARCH_PRODUCT_NAME}")
        if COLLECTION_IDS_FILTER:
            print(f"Filtro collections: {COLLECTION_IDS_FILTER}")
        if AVAILABLE_IN_STORE_ONLY is not None:
            print(f"Apenas loja: {AVAILABLE_IN_STORE_ONLY}")
        print("=" * 60)
        print("Endpoint: GET /products/")
        print("=" * 60)

        # Primeiro testa a conexão
        if not test_products_connection():
            print("Falha no teste de conexao, mas continuando...")

        print("=" * 60)
        print("INICIANDO COLETA DE PRODUTOS")
        print("=" * 60)

        all_products = []
        products_by_type = {}
        offset = 0
        limit = 100
        page_number = 1
        total_from_api = 0

        while True:
            print(f"Processando pagina {page_number} (offset: {offset})...")

            page_data = get_products_page(offset=offset, limit=limit)

            if not page_data:
                print("Erro ao buscar pagina, interrompendo coleta")
                break

            products = page_data.get('products', [])
            total_info = page_data.get('total', [])

            if total_info and isinstance(total_info, list) and len(total_info) > 0:
                total_from_api = total_info[0].get('total', 0)

            print(f"  Total na API: {total_from_api}")
            print(f"  Produtos nesta pagina: {len(products)}")

            if not products:
                print("Nenhum produto encontrado, finalizando coleta")
                break

            # Processa produtos da página
            for product in products:
                # Achata os dados aninhados
                flattened_product = flatten_nested_dict(product)
                all_products.append(flattened_product)

                # Agrupa por tipo para relatório
                product_type = product.get('productType', 'unknown')
                if product_type not in products_by_type:
                    products_by_type[product_type] = 0
                products_by_type[product_type] += 1

            print(f"  Total acumulado: {len(all_products)} produtos")

            # Verifica se há mais páginas
            if len(products) < limit:
                print("  Ultima pagina processada (menos que o limite)")
                break

            # Verifica se coletou todos os produtos disponíveis
            if total_from_api > 0 and len(all_products) >= total_from_api:
                print("  Todos os produtos coletados")
                break

            offset += limit
            page_number += 1
            time.sleep(0.1)  # Rate limiting

            # Proteção contra loop infinito
            if page_number > 100:
                print("  Limite de paginas atingido (100), finalizando")
                break

        # Mostra relatório por tipo
        print("=" * 60)
        print("RELATORIO POR TIPO DE PRODUTO:")
        print("=" * 60)

        if products_by_type:
            for product_type, count in sorted(products_by_type.items()):
                print(f"{product_type}: {count} produtos")
        else:
            print("Nenhum produto encontrado")

        print("=" * 60)
        print("COLETA DE PRODUTOS FINALIZADA!")
        print(f"Total de produtos coletados: {len(all_products)}")
        print(f"Total informado pela API: {total_from_api}")
        print(f"Finalizado em: {datetime.now().strftime('%d/%m/%Y as %H:%M:%S')}")
        print("=" * 60)

        return all_products

    def collect_all_prices_for_products(products: List[Dict]) -> List[Dict]:
        """
        Coleta preços para todos os produtos fornecidos
        """
        if not products:
            print("Nenhum produto fornecido para coleta de precos")
            return []

        print("=" * 60)
        print("GoHighLevel - Coletor de Preços por Produto")
        print("=" * 60)
        print(f"Produtos para processar: {len(products)}")
        print("Endpoint: GET /products/{productId}/price")
        print("=" * 60)

        all_prices = []
        products_with_prices = 0
        products_without_prices = 0

        for i, product in enumerate(products, 1):
            product_id = product.get('_id')
            product_name = product.get('name', 'Sem nome')

            if not product_id:
                print(f"Produto {i}/{len(products)}: ID não encontrado, pulando...")
                continue

            print(f"Produto {i}/{len(products)}: {product_name[:50]}... (ID: {product_id})")

            # Coleta preços do produto
            product_prices = []
            offset = 0
            limit = 100

            while True:
                page_data = get_product_prices_page(product_id, offset, limit)

                if not page_data:
                    break

                prices = page_data.get('prices', [])

                if not prices:
                    break

                # Adiciona informações do produto aos preços
                for price in prices:
                    price_with_product = price.copy()
                    price_with_product['product_id'] = product_id
                    price_with_product['product_name'] = product_name
                    price_with_product['product_type'] = product.get('productType', '')

                    # Achata os dados aninhados
                    flattened_price = flatten_nested_dict(price_with_product)
                    product_prices.append(flattened_price)

                # Verifica se há mais páginas de preços
                if len(prices) < limit:
                    break

                offset += limit

            if product_prices:
                all_prices.extend(product_prices)
                products_with_prices += 1
                print(f"  ✓ {len(product_prices)} precos coletados")
            else:
                products_without_prices += 1
                print(f"  - Nenhum preco encontrado")

            time.sleep(0.1)  # Rate limiting entre produtos

        print("=" * 60)
        print("RELATORIO FINAL DE PRECOS:")
        print("=" * 60)
        print(f"Produtos processados: {len(products)}")
        print(f"Produtos com precos: {products_with_prices}")
        print(f"Produtos sem precos: {products_without_prices}")
        print(f"Total de precos coletados: {len(all_prices)}")
        print("=" * 60)

        return all_prices

    def save_data_to_bucket(data: List[Dict], data_type: str, suffix: str = "") -> str:
        """
        Salva os dados no bucket GCS
        """
        if not data:
            print(f"Nenhum {data_type} para salvar")
            return ""

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f"{data_type}_{timestamp}{suffix}.csv"
        bucket_path = f"{data_type}/{filename}"

        print("=" * 60)
        print(f"SALVANDO {data_type.upper()} NO BUCKET")
        print("=" * 60)
        print(f"Arquivo: {filename}")
        print(f"Registros: {len(data)} {data_type}")
        print(f"Destino: gs://{BUCKET_NAME}/{bucket_path}")

        try:
            # Prepara dados CSV em memória
            all_keys = set()
            for item in data:
                all_keys.update(item.keys())

            print(f"Campos detectados: {len(all_keys)} colunas")

            # Cria CSV em string
            csv_buffer = io.StringIO()
            writer = csv.DictWriter(csv_buffer, fieldnames=sorted(all_keys), delimiter=';')
            writer.writeheader()
            writer.writerows(data)
            csv_content = csv_buffer.getvalue()
            csv_buffer.close()

            # Envia para o bucket
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = SERVICE_ACCOUNT_PATH
            client = storage.Client()
            bucket = client.bucket(BUCKET_NAME)
            blob = bucket.blob(bucket_path)
            blob.upload_from_string(csv_content, content_type='text/csv')

            print("Arquivo enviado com sucesso!")
            print(f"Caminho: {bucket_path}")
            print("=" * 60)
            return bucket_path
        except Exception as e:
            print(f"Erro ao enviar CSV para o bucket: {e}")
            print("=" * 60)
            return ""

    def main():
        print("INICIANDO COLETOR DE PRODUTOS E PREÇOS - GoHighLevel")
        print("=" * 60)

        try:
            global access_token
            print("Obtendo token de acesso...")
            access_token = get_token_from_bucket()
            if not access_token:
                print("Falha ao obter token inicial")
                return
            print("Token obtido com sucesso")

            # 1. Coleta produtos
            print("\n" + "=" * 60)
            print("ETAPA 1: COLETANDO PRODUTOS")
            print("=" * 60)

            products = collect_all_products()
            if not products:
                print("Nenhum produto coletado, finalizando processo")
                return

            # 2. Salva produtos
            products_path = save_data_to_bucket(products, "products")

            # 3. Coleta preços (se habilitado)
            prices_path = ""
            if COLLECT_PRICES_FOR_PRODUCTS:
                print("\n" + "=" * 60)
                print("ETAPA 2: COLETANDO PREÇOS DOS PRODUTOS")
                print("=" * 60)

                prices = collect_all_prices_for_products(products)
                if prices:
                    prices_path = save_data_to_bucket(prices, "prices", "_with_products")
                else:
                    print("Nenhum preco coletado")

            # 4. Relatório final
            print("\n" + "=" * 60)
            print("PROCESSO CONCLUIDO COM SUCESSO!")
            print("=" * 60)
            if products_path:
                print(f"Produtos: {products_path}")
                print(f"Total produtos: {len(products)}")

            if prices_path:
                print(f"Precos: {prices_path}")
                print(f"Total precos: {len(prices) if 'prices' in locals() else 0}")

            print(f"Finalizado: {datetime.now().strftime('%d/%m/%Y as %H:%M:%S')}")
            print("=" * 60)

        except KeyboardInterrupt:
            print("\nProcesso interrompido pelo usuario")
        except Exception as e:
            print(f"Erro inesperado: {e}")

        print("=" * 60)

    # START
    main()


def get_extraction_tasks():
    """
    Get the list of data extraction tasks for VistaCRM.

    Returns:
        list: List of task configurations
    """
    return [
        {
            'task_id': 'run_submissions',
            'python_callable': run_submissions
        },
        {
            'task_id': 'run_contacts',
            'python_callable': run_contacts
        },
        {
            'task_id': 'run_funnels',
            'python_callable': run_funnels
        },
        {
            'task_id': 'run_opps',
            'python_callable': run_opps
        },
        {
            'task_id': 'run_pipelines_opps',
            'python_callable': run_pipelines_opps
        },
        {
            'task_id': 'run_products_price',
            'python_callable': run_products_price
        },

    ]
