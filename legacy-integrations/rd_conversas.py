"""
RD Conversas module for data extraction functions.
This module contains functions specific to the RD Conversas integration.
"""

import json

from core import gcs


def safe_csv_value(value):
    """
    Converte um valor para string segura para CSV, escapando separadores.
    """
    if value is None:
        return ""

    # Converte para string
    str_value = str(value)

    # Remove ou substitui caracteres problemáticos
    str_value = str_value.replace(';', ',')  # Substitui ; por ,
    str_value = str_value.replace('\n', ' ')  # Remove quebras de linha
    str_value = str_value.replace('\r', ' ')  # Remove retorno de carro
    str_value = str_value.replace('"', "'")  # Substitui aspas duplas por simples

    return str_value.strip()


def flatten_data(data):
    """
    Achata estruturas aninhadas nos dados.
    """
    flattened = {}

    for key, value in data.items():
        if isinstance(value, dict):
            # Se o valor é um dicionário, achata suas chaves
            for nested_key, nested_value in value.items():
                flattened_key = f"{key}_{nested_key}"
                flattened[flattened_key] = nested_value if nested_value is not None else ""
        elif isinstance(value, list):
            # Se o valor é uma lista, converta para string JSON
            flattened[key] = json.dumps(value) if value else ""
        else:
            # Para valores simples, use como está
            flattened[key] = value if value is not None else ""

    return flattened


def save_dynamic_csv_to_gcs(storage_client, bucket_name, folder, filename, data):
    """
    Função genérica para salvar dados com estrutura dinâmica no GCS.
    """
    if not data:
        print("Nenhum dado para salvar.")
        return

    try:
        print("Iniciando o upload para o GCS...")
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(f"{folder}/{filename}")

        from io import StringIO
        csv_buffer = StringIO()

        # Coletar todas as chaves possíveis dos dados achatados
        all_keys = set()
        for row in data:
            all_keys.update(row.keys())

        # Ordenar as chaves para consistência
        header = sorted(list(all_keys))

        # Escrever o cabeçalho
        csv_buffer.write(";".join(header) + "\n")

        # Escrever os dados
        for row in data:
            row_values = []
            for key in header:
                value = row.get(key, "")
                safe_value = safe_csv_value(value)
                row_values.append(safe_value)

            csv_buffer.write(";".join(row_values) + "\n")

        csv_buffer.seek(0)

        # Carregar o CSV para o GCS
        blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')
        print(f"Arquivo enviado para gs://{bucket_name}/{folder}/{filename} com sucesso.")
    except Exception as e:
        print(f"Erro ao enviar o arquivo para o GCS: {e}")
        raise


def save_simple_csv_to_gcs(storage_client, bucket_name, folder, filename, header, data, field_mapping):
    """
    Função genérica para salvar dados com estrutura simples no GCS.
    """
    if not data:
        print("Nenhum dado para salvar.")
        return

    try:
        print("Iniciando o upload para o GCS...")
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(f"{folder}/{filename}")

        from io import StringIO
        csv_buffer = StringIO()

        # Escrever o cabeçalho
        csv_buffer.write(header + "\n")

        # Escrever os dados
        for item in data:
            values = []
            for field in field_mapping:
                if callable(field):
                    value = field(item)
                else:
                    value = item.get(field, "Não especificado")
                safe_value = safe_csv_value(value)
                values.append(safe_value)

            csv_buffer.write(";".join(values) + "\n")

        csv_buffer.seek(0)

        # Carregar o CSV para o GCS
        blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')
        print(f"Arquivo enviado para gs://{bucket_name}/{folder}/{filename} com sucesso.")
    except Exception as e:
        print(f"Erro ao enviar o arquivo para o GCS: {e}")
        raise


def run_customers(customer):
    import os
    import requests
    import time
    from google.cloud import storage
    import pathlib

    # Configurações
    TOKEN = customer['api_token']
    BASE_URL = customer['api_base_url']
    URL = f'{BASE_URL}/v2/customers'
    BUCKET_NAME = customer['bucket_name']

    HEADERS = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {TOKEN}"
    }

    # Configuração do GCP
    SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH

    def fetch_customers():
        """Busca os dados de clientes do endpoint."""
        try:
            page = 1
            limit = 1000
            all_data = []
            total_collected = 0

            while True:
                # Configura os parâmetros da requisição
                params = {"page": page, "limit": limit}
                response = requests.get(URL, headers=HEADERS, params=params)

                # Verifica o status da resposta
                if response.status_code != 200:
                    print(f"Erro na requisição. Código: {response.status_code}, Detalhes: {response.text}")
                    break

                # Processa os dados
                data = response.json()
                quantidade = len(data)
                if quantidade == 0:  # Se não há mais dados, encerra a coleta
                    break

                # Achata os dados aninhados
                flattened_data = [flatten_data(item) for item in data]
                all_data.extend(flattened_data)

                total_collected += quantidade
                print(f"Coletados {total_collected} registros até agora (registros nesta página: {quantidade}).")

                # Incrementa a página para a próxima requisição
                page += 1

                # Adiciona um atraso para evitar sobrecarregar o servidor
                time.sleep(1)

            return all_data
        except Exception as e:
            print(e)
            raise

    def main():
        """Função principal para executar a coleta e envio dos dados."""
        print("Iniciando a coleta de dados de clientes do RD Station Conversas...")
        customers = fetch_customers()
        print(f"Coleta finalizada. Total de registros coletados: {len(customers)}")

        storage_client = storage.Client(project=customer['project_id'])
        save_dynamic_csv_to_gcs(storage_client, BUCKET_NAME, 'customers', 'customers.csv', customers)

    # START
    main()


def run_flows(customer):
    import os
    import requests
    from google.cloud import storage
    import pathlib

    # Configurações
    TOKEN = customer['api_token']
    BASE_URL = customer['api_base_url']
    URL = f'{BASE_URL}/v2/flows'
    BUCKET_NAME = customer['bucket_name']

    HEADERS = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {TOKEN}"
    }

    # Configuração do GCP
    SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH

    def fetch_flows():
        """Busca os dados de flows do endpoint."""
        try:
            response = requests.get(URL, headers=HEADERS)

            # Verifica o status da resposta
            if response.status_code != 200:
                print(f"Erro na requisição. Código: {response.status_code}, Detalhes: {response.text}")
                return []

            # Processa os dados
            data = response.json().get("flows", [])
            print(f"Total de flows coletados: {len(data)}")
            return data
        except Exception as e:
            print(e)
            raise

    def main():
        """Função principal para executar a coleta e envio dos dados."""
        print("Iniciando a coleta de flows...")
        flows = fetch_flows()

        storage_client = storage.Client(project=customer['project_id'])
        save_simple_csv_to_gcs(
            storage_client,
            BUCKET_NAME,
            'flows',
            'flows.csv',
            "ID;Título",
            flows,
            ['id', 'title']
        )

    # START
    main()


def run_integrations(customer):
    import requests
    import os
    from google.cloud import storage
    import pathlib

    # Configurações
    TOKEN = customer['api_token']
    BASE_URL = customer['api_base_url']
    URL = f'{BASE_URL}/v2/whatsapp/integrations'
    BUCKET_NAME = customer['bucket_name']

    HEADERS = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {TOKEN}"
    }

    # Configuração do GCP
    SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH

    def fetch_integrations():
        """Busca as integrações do WhatsApp."""
        response = requests.get(URL, headers=HEADERS)

        # Verifica o status da resposta
        if response.status_code != 200:
            print(f"Erro na requisição. Código: {response.status_code}, Detalhes: {response.text}")
            return []

        # Processa os dados diretamente como uma lista
        try:
            data = response.json()
            print(f"Total de integrações coletadas: {len(data)}")
            return data
        except ValueError:
            print("Erro ao processar a resposta JSON.")
            return []
        except Exception as e:
            print(e)
            raise

    def main():
        """Função principal para executar a coleta e envio dos dados."""
        print("Iniciando a coleta de integrações do WhatsApp...")
        integrations = fetch_integrations()

        storage_client = storage.Client(project=customer['project_id'])
        save_simple_csv_to_gcs(
            storage_client,
            BUCKET_NAME,
            'whatsapp/integrations',
            'whatsapp_integrations.csv',
            "ID da Integração;Descrição",
            integrations,
            ['id', 'description']
        )

    # START
    main()


def run_integrations_official(customer):
    import requests
    import os
    from google.cloud import storage
    import pathlib

    # Configurações
    TOKEN = customer['api_token']
    BASE_URL = customer['api_base_url']
    URL = f'{BASE_URL}/v2/whatsapp/integrations/official'
    BUCKET_NAME = customer['bucket_name']

    HEADERS = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {TOKEN}"
    }

    # Configuração do GCP
    SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH

    def fetch_integrations_official():
        """Busca as integrações oficiais do WhatsApp."""
        response = requests.get(URL, headers=HEADERS)

        # Verifica o status da resposta
        if response.status_code != 200:
            print(f"Erro na requisição. Código: {response.status_code}, Detalhes: {response.text}")
            return []

        # Trata a resposta como uma lista diretamente
        try:
            data = response.json()
            print(f"Total de integrações oficiais coletadas: {len(data)}")
            return data
        except ValueError:
            print("Erro ao processar a resposta JSON.")
            return []
        except Exception as e:
            print(e)
            raise

    def main():
        """Função principal para executar a coleta e envio dos dados."""
        print("Iniciando a coleta de integrações oficiais do WhatsApp...")
        integrations = fetch_integrations_official()

        storage_client = storage.Client(project=customer['project_id'])
        save_simple_csv_to_gcs(
            storage_client,
            BUCKET_NAME,
            'whatsapp/integrations/official',
            'whatsapp_integrations_official.csv',
            "Chave;Descrição",
            integrations,
            ['key', 'label']
        )

    # START
    main()


def run_reports(customer):
    import requests
    import os
    import time
    from datetime import datetime
    from google.cloud import storage
    import pathlib

    # Configurações
    TOKEN = customer['api_token']
    BASE_URL = customer['api_base_url']
    URL = f'{BASE_URL}/v4/reports'
    BUCKET_NAME = customer['bucket_name']

    HEADERS = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {TOKEN}"
    }

    # Configuração do GCP
    SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH

    # Intervalo de datas
    START_DATE = "2024-12-01T00:00:00.000Z"  # Data de início no formato ISO 8601
    END_DATE = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000Z")  # Data atual em UTC no formato ISO 8601

    def fetch_reports():
        """Busca relatórios de atendimentos."""
        try:
            take = 200
            skip = 0
            all_data = []
            total_collected = 0

            while True:
                # Configura os parâmetros da requisição
                params = {
                    "start_date": START_DATE,
                    "end_date": END_DATE,
                    "take": take,
                    "skip": skip
                }
                response = requests.get(URL, headers=HEADERS, params=params)

                # Verifica o status da resposta
                if response.status_code != 200:
                    print(f"Erro na requisição. Código: {response.status_code}, Detalhes: {response.text}")
                    break

                # Processa os dados
                data = response.json().get("reports", [])
                quantidade = len(data)
                if quantidade == 0:  # Se não há mais dados, encerra a coleta
                    break

                # Achata os dados aninhados
                flattened_data = [flatten_data(item) for item in data]
                all_data.extend(flattened_data)

                total_collected += quantidade
                print(f"Coletados {total_collected} registros até agora (registros nesta página: {quantidade}).")

                # Incrementa o `skip` para a próxima página
                skip += take

                # Adiciona um atraso para evitar sobrecarregar o servidor
                time.sleep(1)

            return all_data
        except Exception as e:
            print(e)
            raise

    def main():
        """Função principal para executar a coleta e envio dos relatórios."""
        print("Iniciando a coleta de relatórios de atendimentos...")
        reports = fetch_reports()
        print(f"Coleta finalizada. Total de registros coletados: {len(reports)}")

        storage_client = storage.Client(project=customer['project_id'])
        save_dynamic_csv_to_gcs(storage_client, BUCKET_NAME, 'reports', 'relatorios_atendimentos.csv', reports)

    # START
    main()


def run_templates(customer):
    import requests
    import os
    from google.cloud import storage
    import pathlib

    # Configurações
    TOKEN = customer['api_token']
    BASE_URL = customer['api_base_url']
    URL = f'{BASE_URL}/v2/template/all'
    BUCKET_NAME = customer['bucket_name']

    HEADERS = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {TOKEN}"
    }

    # Configuração do GCP
    SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH

    def fetch_templates():
        """Busca mensagens de template."""
        response = requests.get(URL, headers=HEADERS)

        # Verifica o status da resposta
        if response.status_code != 200:
            print(f"Erro na requisição. Código: {response.status_code}, Detalhes: {response.text}")
            return []

        # Processa os dados
        try:
            data = response.json().get("templates", [])
            print(f"Total de mensagens de template coletadas: {len(data)}")
            return data
        except ValueError:
            print("Erro ao processar a resposta JSON.")
            return []
        except Exception as e:
            print(e)
            raise

    def main():
        """Função principal para executar a coleta e envio das mensagens de template."""
        print("Iniciando a coleta de mensagens de template...")
        templates = fetch_templates()

        storage_client = storage.Client(project=customer['project_id'])
        save_simple_csv_to_gcs(
            storage_client,
            BUCKET_NAME,
            'template',
            'mensagens_template.csv',
            "ID;Conteúdo;URL da Mídia",
            templates,
            ['id', 'content', 'content_media']
        )

    # START
    main()


def run_wallets(customer):
    import requests
    import os
    from google.cloud import storage
    import pathlib

    # Configurações
    TOKEN = customer['api_token']
    BASE_URL = customer['api_base_url']
    URL = f'{BASE_URL}/v2/wallets'
    BUCKET_NAME = customer['bucket_name']

    HEADERS = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {TOKEN}"
    }

    # Configuração do GCP
    SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH

    def fetch_wallets():
        """Busca carteiras na API."""
        response = requests.get(URL, headers=HEADERS)

        # Verifica o status da resposta
        if response.status_code != 200:
            print(f"Erro na requisição. Código: {response.status_code}, Detalhes: {response.text}")
            return []

        # Processa os dados
        try:
            data = response.json().get("wallets", [])
            print(f"Total de carteiras coletadas: {len(data)}")
            return data
        except ValueError:
            print("Erro ao processar a resposta JSON.")
            return []
        except Exception as e:
            print(e)
            raise

    def main():
        """Função principal para executar a coleta e envio das carteiras."""
        print("Iniciando a coleta de carteiras...")
        wallets = fetch_wallets()

        storage_client = storage.Client(project=customer['project_id'])
        save_simple_csv_to_gcs(
            storage_client,
            BUCKET_NAME,
            'wallets',
            'carteiras.csv',
            "Carteiras",
            wallets,
            [lambda x: x]  # Para dados simples, apenas retorna o valor
        )

    # START
    main()


def run_workflows(customer):
    import requests
    import os
    from google.cloud import storage
    import pathlib

    # Configurações
    TOKEN = customer['api_token']
    BASE_URL = customer['api_base_url']
    URL = f'{BASE_URL}/v2/workflows'
    BUCKET_NAME = customer['bucket_name']

    HEADERS = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {TOKEN}"
    }

    # Configuração do GCP
    SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH

    def fetch_workflows():
        """Busca workflows na API."""
        response = requests.get(URL, headers=HEADERS)

        # Verifica o status da resposta
        if response.status_code != 200:
            print(f"Erro na requisição. Código: {response.status_code}, Detalhes: {response.text}")
            return []

        # Processa os dados
        try:
            data = response.json().get("workflows", [])
            print(f"Total de workflows coletados: {len(data)}")
            return data
        except ValueError:
            print("Erro ao processar a resposta JSON.")
            return []
        except Exception as e:
            print(e)
            raise

    def process_workflows(workflows):
        """Processa workflows para expandir estágios em linhas separadas."""
        processed_data = []
        for workflow in workflows:
            company = workflow.get("company", "Não especificado")
            wallet_stages = workflow.get("wallet", [])

            if wallet_stages:
                for stage in wallet_stages:
                    processed_data.append({
                        "company": company,
                        "stage": stage
                    })
            else:
                # Se não há estágios, cria uma linha com empresa e estágio vazio
                processed_data.append({
                    "company": company,
                    "stage": "Não especificado"
                })

        return processed_data

    def main():
        """Função principal para executar a coleta e envio dos workflows."""
        print("Iniciando a coleta de workflows...")
        workflows = fetch_workflows()
        processed_workflows = process_workflows(workflows)

        storage_client = storage.Client(project=customer['project_id'])
        save_simple_csv_to_gcs(
            storage_client,
            BUCKET_NAME,
            'workflows',
            'workflows.csv',
            "Empresa;Estágio do Workflow",
            processed_workflows,
            ['company', 'stage']
        )

    # START
    main()


def run_contact_by_phone(customer):
    """
    Busca contatos por telefone a partir de uma página específica de clientes.
    Só executa se 'start_page_by_contact_phone' estiver configurado no customer.
    """
    import os
    import requests
    import time
    from google.cloud import storage
    import pathlib

    # Verifica se o parâmetro de página inicial está configurado
    if 'start_page_by_contact_phone' not in customer:
        print("Parâmetro 'start_page_by_contact_phone' não encontrado. Função não será executada.")
        return

    # Configurações
    TOKEN = customer['api_token']
    BASE_URL = customer['api_base_url']
    CUSTOMERS_URL = f'{BASE_URL}/v2/customers'
    CONTACTS_URL = f'{BASE_URL}/v2/contacts'
    BUCKET_NAME = customer['bucket_name']
    START_PAGE = customer['start_page_by_contact_phone']

    HEADERS = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {TOKEN}"
    }

    # Configuração do GCP
    SERVICE_ACCOUNT_PATH = pathlib.Path('config', 'gcp.json').as_posix()
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_PATH

    def fetch_customers_phones():
        """Busca os telefones dos clientes do endpoint customers a partir da página especificada."""
        try:
            page = START_PAGE  # Começa da página especificada
            # limit = 1000
            all_phones = []
            total_collected = 0

            while True:
                # Configura os parâmetros da requisição
                params = {"page": page}
                response = requests.get(CUSTOMERS_URL, headers=HEADERS, params=params)

                # Verifica o status da resposta
                if response.status_code != 200:
                    print(f"Erro na requisição de clientes. Código: {response.status_code}, Detalhes: {response.text}")
                    break

                # Processa os dados
                data = response.json()
                quantidade = len(data)
                if quantidade == 0:  # Se não há mais dados, encerra a coleta
                    break

                # Extrai telefones válidos
                for customer_data in data:
                    cel_phone = customer_data.get('cel_phone')
                    if cel_phone and cel_phone.strip():  # Verifica se o telefone não é None ou vazio
                        all_phones.append({
                            'customer_id': customer_data.get('_id'),
                            'full_name': customer_data.get('full_name'),
                            'cel_phone': cel_phone.strip()
                        })

                total_collected += quantidade
                print(
                    f"Processados {total_collected} clientes até agora (clientes nesta página: {quantidade}), página atual: {page}.")

                # Incrementa a página para a próxima requisição
                page += 1

                # Adiciona um atraso para evitar sobrecarregar o servidor
                time.sleep(1)

            print(f"Total de telefones válidos encontrados: {len(all_phones)}")
            return all_phones
        except Exception as e:
            print(f"Erro ao buscar telefones dos clientes: {e}")
            raise

    def check_contact_exists(phone):
        """Verifica se um contato existe para o telefone fornecido."""
        try:
            url = f"{CONTACTS_URL}/{phone}/exists"
            response = requests.get(url, headers=HEADERS)

            if response.status_code == 200:
                return response.json()
            else:
                print(f"Erro ao verificar contato para {phone}. Código: {response.status_code}")
                return {"exists": False, "error": f"HTTP {response.status_code}"}
        except Exception as e:
            print(f"Erro ao verificar contato para {phone}: {e}")
            return {"exists": False, "error": str(e)}

    def fetch_contacts_by_phone(phones_data):
        """Verifica a existência de contatos para cada telefone."""
        try:
            contacts_results = []
            total_phones = len(phones_data)

            for i, phone_info in enumerate(phones_data, 1):
                phone = phone_info['cel_phone']
                customer_id = phone_info['customer_id']
                full_name = phone_info['full_name']

                print(f"Verificando contato {i}/{total_phones} para telefone: {phone}")

                # Verifica se o contato existe
                contact_result = check_contact_exists(phone)

                # Monta o resultado
                result = {
                    'customer_id': customer_id,
                    'full_name': full_name,
                    'cel_phone': phone,
                    'contact_exists': contact_result.get('exists', False),
                    'contact_data': contact_result if contact_result.get('exists') else None,
                    'error': contact_result.get('error', None)
                }

                contacts_results.append(result)

                # Adiciona um atraso para evitar sobrecarregar o servidor
                time.sleep(0.5)  # Meio segundo entre requisições

            return contacts_results
        except Exception as e:
            print(f"Erro ao verificar contatos por telefone: {e}")
            raise

    def main():
        """Função principal para executar a coleta e envio dos dados de contatos por telefone."""
        print(f"Iniciando a coleta de telefones dos clientes a partir da página {START_PAGE}...")
        phones_data = fetch_customers_phones()

        if not phones_data:
            print("Nenhum telefone válido encontrado nos clientes.")
            return

        print("Iniciando verificação de contatos por telefone...")
        contacts_results = fetch_contacts_by_phone(phones_data)

        # Achata os dados para o CSV
        flattened_results = [flatten_data(result) for result in contacts_results]

        print(f"Verificação finalizada. Total de registros: {len(flattened_results)}")

        storage_client = storage.Client(project=customer['project_id'])
        save_dynamic_csv_to_gcs(
            storage_client,
            BUCKET_NAME,
            'contact_by_phone',
            'contact_by_phone.csv',
            flattened_results
        )

    # START
    main()


def get_extraction_tasks():
    """
    Get the list of data extraction tasks for RD Conversas.

    Returns:
        list: List of task configurations
    """
    return [
        # {
        #     'task_id': 'run_customers',
        #     'python_callable': run_customers
        # },
        # {
        #     'task_id': 'run_flows',
        #     'python_callable': run_flows
        # },
        # {
        #     'task_id': 'run_integrations',
        #     'python_callable': run_integrations
        # },
        # {
        #     'task_id': 'run_integrations_official',
        #     'python_callable': run_integrations_official
        # },
        # {
        #     'task_id': 'run_reports',
        #     'python_callable': run_reports
        # },
        # {
        #     'task_id': 'run_templates',
        #     'python_callable': run_templates
        # },
        # {
        #     'task_id': 'run_wallets',
        #     'python_callable': run_wallets
        # },
        # {
        #     'task_id': 'run_workflows',
        #     'python_callable': run_workflows
        # },
        {
            'task_id': 'run_contact_by_phone',
            'python_callable': run_contact_by_phone
        }
    ]
