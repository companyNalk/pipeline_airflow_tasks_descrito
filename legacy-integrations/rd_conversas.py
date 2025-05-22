"""
RD Conversas module for data extraction functions.
This module contains functions specific to the RD Conversas integration.
"""

from core import gcs


def run_customers(customer):
    import os
    import requests
    import time
    from google.cloud import storage
    from io import StringIO
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

                all_data.extend(data)
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

    def save_to_gcs(bucket_name, folder, data):
        """Salva os dados coletados no GCS."""
        if not data:
            print("Nenhum dado para salvar.")
            return

        try:
            print("Iniciando o upload para o GCS...")
            storage_client = storage.Client(project=customer['project_id'])
            bucket = storage_client.bucket(BUCKET_NAME)
            blob = bucket.blob(f"{folder}/customers.csv")

            # Converte os dados para CSV em memória
            csv_buffer = StringIO()

            # Escrever o cabeçalho
            header = data[0].keys()
            csv_buffer.write(";".join(header) + "\n")

            # Escrever os dados
            for row in data:
                csv_buffer.write(";".join(map(str, row.values())) + "\n")

            csv_buffer.seek(0)

            # Carregar o CSV para o GCS
            blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')
            print(f"Arquivo enviado para gs://{bucket_name}/{folder}/customers.csv com sucesso.")
        except Exception as e:
            print(f"Erro ao enviar o arquivo para o GCS: {e}")
            raise

    def main():
        """Função principal para executar a coleta e envio dos dados."""
        print("Iniciando a coleta de dados de clientes do RD Station Conversas...")
        customers = fetch_customers()
        print(f"Coleta finalizada. Total de registros coletados: {len(customers)}")
        save_to_gcs(BUCKET_NAME, 'customers', customers)

    # START
    main()


def run_flows(customer):
    import os
    import requests
    from google.cloud import storage
    from io import StringIO
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

    def save_to_gcs(bucket_name, folder, data):
        """Salva os dados coletados no GCS."""
        if not data:
            print("Nenhum dado para salvar.")
            return

        try:
            print("Iniciando o upload para o GCS...")
            storage_client = storage.Client(project=customer['project_id'])
            bucket = storage_client.bucket(BUCKET_NAME)
            blob = bucket.blob(f"{folder}/flows.csv")

            # Converte os dados para CSV em memória
            csv_buffer = StringIO()

            # Escreve o cabeçalho
            csv_buffer.write("ID;Título\n")

            # Escreve os dados
            for flow in data:
                csv_buffer.write(f"{flow.get('id', 'Não especificado')};{flow.get('title', 'Não especificado')}\n")

            csv_buffer.seek(0)

            # Carregar o CSV para o GCS
            blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')
            print(f"Arquivo enviado para gs://{bucket_name}/{folder}/flows.csv com sucesso.")
        except Exception as e:
            print(f"Erro ao enviar o arquivo para o GCS: {e}")
            raise

    def main():
        """Função principal para executar a coleta e envio dos dados."""
        print("Iniciando a coleta de flows...")
        flows = fetch_flows()
        save_to_gcs(BUCKET_NAME, 'flows', flows)

    # START
    main()


def run_integrations(customer):
    import requests
    import os
    from io import StringIO
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

    def save_to_gcs(bucket_name, folder, data):
        """Salva os dados coletados no GCS."""
        if not data:
            print("Nenhum dado para salvar.")
            return

        try:
            print("Iniciando o upload para o GCS...")
            storage_client = storage.Client(project=customer['project_id'])
            bucket = storage_client.bucket(BUCKET_NAME)
            blob = bucket.blob(f"{folder}/whatsapp_integrations.csv")

            # Converte os dados para CSV em memória
            csv_buffer = StringIO()

            # Escreve o cabeçalho
            csv_buffer.write("ID da Integração;Descrição\n")

            # Escreve os dados
            for integration in data:
                id_integration = integration.get("id", "Não especificado")
                description = integration.get("description", "Não especificado")
                csv_buffer.write(f"{id_integration};{description}\n")

            csv_buffer.seek(0)

            # Carregar o CSV para o GCS
            blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')
            print(f"Arquivo enviado para gs://{bucket_name}/{folder}/whatsapp_integrations.csv com sucesso.")
        except Exception as e:
            print(f"Erro ao enviar o arquivo para o GCS: {e}")
            raise

    def main():
        """Função principal para executar a coleta e envio dos dados."""
        print("Iniciando a coleta de integrações do WhatsApp...")
        integrations = fetch_integrations()
        save_to_gcs(BUCKET_NAME, 'whatsapp/integrations', integrations)

    # START
    main()


def run_integrations_official(customer):
    import requests
    import os
    from io import StringIO
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

    def save_to_gcs(bucket_name, folder, data):
        """Salva os dados coletados no GCS."""
        if not data:
            print("Nenhum dado para salvar.")
            return

        try:
            print("Iniciando o upload para o GCS...")
            storage_client = storage.Client(project=customer['project_id'])
            bucket = storage_client.bucket(BUCKET_NAME)
            blob = bucket.blob(f"{folder}/whatsapp_integrations_official.csv")

            # Converte os dados para CSV em memória
            csv_buffer = StringIO()

            # Escreve o cabeçalho
            csv_buffer.write("Chave;Descrição\n")

            # Escreve os dados
            for integration in data:
                csv_buffer.write(
                    f"{integration.get('key', 'Não especificado')};{integration.get('label', 'Não especificado')}\n")

            csv_buffer.seek(0)

            # Carregar o CSV para o GCS
            blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')
            print(f"Arquivo enviado para gs://{bucket_name}/{folder}/whatsapp_integrations_official.csv com sucesso.")
        except Exception as e:
            print(f"Erro ao enviar o arquivo para o GCS: {e}")
            raise

    def main():
        """Função principal para executar a coleta e envio dos dados."""
        print("Iniciando a coleta de integrações oficiais do WhatsApp...")
        integrations = fetch_integrations_official()
        save_to_gcs(BUCKET_NAME, 'whatsapp/integrations/official', integrations)

    # START
    main()


def run_reports(customer):
    import requests
    import os
    import time
    from datetime import datetime
    from io import StringIO
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

            all_data.extend(data)
            total_collected += quantidade
            print(f"Coletados {total_collected} registros até agora (registros nesta página: {quantidade}).")

            # Incrementa o `skip` para a próxima página
            skip += take

            # Adiciona um atraso para evitar sobrecarregar o servidor
            time.sleep(1)

        return all_data

    def save_to_gcs(bucket_name, folder, data):
        """Salva os relatórios coletados no GCS."""
        if not data:
            print("Nenhum dado para salvar.")
            return

        try:
            print("Iniciando o upload para o GCS...")
            storage_client = storage.Client(project=customer['project_id'])
            bucket = storage_client.bucket(BUCKET_NAME)
            blob = bucket.blob(f"{folder}/relatorios_atendimentos.csv")

            # Converte os dados para CSV em memória
            csv_buffer = StringIO()

            # Escreve o cabeçalho
            header = data[0].keys()
            csv_buffer.write(";".join(header) + "\n")

            # Escreve os dados
            for row in data:
                csv_buffer.write(";".join(str(row[key]) for key in header) + "\n")

            csv_buffer.seek(0)

            # Carregar o CSV para o GCS
            blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')
            print(f"Arquivo enviado para gs://{bucket_name}/{folder}/relatorios_atendimentos.csv com sucesso.")
        except Exception as e:
            print(f"Erro ao enviar o arquivo para o GCS: {e}")
            raise

    def main():
        """Função principal para executar a coleta e envio dos relatórios."""
        print("Iniciando a coleta de relatórios de atendimentos...")
        reports = fetch_reports()
        print(f"Coleta finalizada. Total de registros coletados: {len(reports)}")
        save_to_gcs(BUCKET_NAME, 'reports', reports)

    # START
    main()


def run_templates(customer):
    import requests
    import os
    from io import StringIO
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

    def save_to_gcs(bucket_name, folder, data):
        """Salva os templates no GCS."""
        if not data:
            print("Nenhum dado para salvar.")
            return

        try:
            print("Iniciando o upload para o GCS...")
            storage_client = storage.Client(project=customer['project_id'])
            bucket = storage_client.bucket(BUCKET_NAME)
            blob = bucket.blob(f"{folder}/mensagens_template.csv")

            # Converte os dados para CSV em memória
            csv_buffer = StringIO()

            # Escreve o cabeçalho
            csv_buffer.write("ID;Conteúdo;URL da Mídia\n")

            # Escreve os dados
            for template in data:
                csv_buffer.write(f"{template.get('id', 'Não especificado')};"
                                 f"{template.get('content', 'Não especificado')};"
                                 f"{template.get('content_media', 'Não especificado')}\n")

            csv_buffer.seek(0)

            # Carregar o CSV para o GCS
            blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')
            print(f"Arquivo enviado para gs://{bucket_name}/{folder}/mensagens_template.csv com sucesso.")
        except Exception as e:
            print(f"Erro ao enviar o arquivo para o GCS: {e}")
            raise

    def main():
        """Função principal para executar a coleta e envio das mensagens de template."""
        print("Iniciando a coleta de mensagens de template...")
        templates = fetch_templates()
        save_to_gcs(BUCKET_NAME, 'template', templates)

    # START
    main()


def run_wallets(customer):
    import requests
    import os
    from io import StringIO
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

    def save_to_gcs(bucket_name, folder, data):
        """Salva as carteiras no GCS."""
        if not data:
            print("Nenhum dado para salvar.")
            return

        try:
            print("Iniciando o upload para o GCS...")
            storage_client = storage.Client(project=customer['project_id'])
            bucket = storage_client.bucket(BUCKET_NAME)
            blob = bucket.blob(f"{folder}/carteiras.csv")

            # Converte os dados para CSV em memória
            csv_buffer = StringIO()

            # Escreve o cabeçalho
            csv_buffer.write("Carteiras\n")

            # Escreve os dados
            for wallet in data:
                csv_buffer.write(f"{wallet}\n")

            csv_buffer.seek(0)

            # Carrega o CSV para o GCS
            blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')
            print(f"Arquivo enviado para gs://{bucket_name}/{folder}/carteiras.csv com sucesso.")
        except Exception as e:
            print(f"Erro ao enviar o arquivo para o GCS: {e}")

    def main():
        """Função principal para executar a coleta e envio das carteiras."""
        print("Iniciando a coleta de carteiras...")
        wallets = fetch_wallets()
        save_to_gcs(BUCKET_NAME, 'wallets', wallets)

    # START
    main()


def run_workflows(customer):
    import requests
    import os
    from io import StringIO
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

    def save_to_gcs(bucket_name, folder, data):
        """Salva os workflows no GCS."""
        if not data:
            print("Nenhum dado para salvar.")
            return

        try:
            print("Iniciando o upload para o GCS...")
            storage_client = storage.Client(project=customer['project_id'])
            bucket = storage_client.bucket(BUCKET_NAME)
            blob = bucket.blob(f"{folder}/workflows.csv")

            # Converte os dados para CSV em memória
            csv_buffer = StringIO()

            # Escreve o cabeçalho
            csv_buffer.write("Empresa;Estágio do Workflow\n")

            # Escreve os dados
            for workflow in data:
                company = workflow.get("company", "Não especificado")
                wallet_stages = workflow.get("wallet", [])
                for stage in wallet_stages:
                    csv_buffer.write(f"{company};{stage}\n")

            csv_buffer.seek(0)

            # Carrega o CSV para o GCS
            blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')
            print(f"Arquivo enviado para gs://{bucket_name}/{folder}/workflows.csv com sucesso.")
        except Exception as e:
            print(f"Erro ao enviar o arquivo para o GCS: {e}")
            raise

    def main():
        """Função principal para executar a coleta e envio dos workflows."""
        print("Iniciando a coleta de workflows...")
        workflows = fetch_workflows()
        save_to_gcs(BUCKET_NAME, 'workflows', workflows)

    # START
    main()


def get_extraction_tasks():
    """
    Get the list of data extraction tasks for RD Conversas.

    Returns:
        list: List of task configurations
    """
    return [
        {
            'task_id': 'run_customers',
            'python_callable': run_customers
        },
        {
            'task_id': 'run_flows',
            'python_callable': run_flows
        },
        {
            'task_id': 'run_integrations',
            'python_callable': run_integrations
        },
        {
            'task_id': 'run_integrations_official',
            'python_callable': run_integrations_official
        },
        {
            'task_id': 'run_reports',
            'python_callable': run_reports
        },
        {
            'task_id': 'run_templates',
            'python_callable': run_templates
        },
        {
            'task_id': 'run_wallets',
            'python_callable': run_wallets
        },
        {
            'task_id': 'run_workflows',
            'python_callable': run_workflows
        },
    ]
