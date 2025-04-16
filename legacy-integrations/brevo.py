"""
Brevo module for data extraction functions.
This module contains functions specific to the Brevo integration.
"""

import os
import time
import csv
import io
import logging
import requests
from core import gcs

def run(customer):
    """
    Extract campaign data from Brevo API and load it to GCS.
    
    Args:
        customer (dict): Customer information dictionary
    """
    # Configuração do Logger com nível INFO
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    # Configuração do Google Cloud Storage
    BUCKET_NAME = customer['bucket_name']
    FOLDER_NAME = customer['folder_name']

    # Obtendo API Key de variável de ambiente
    API_KEY = customer['api_key']
    if not API_KEY:
        raise ValueError("Erro: A chave da API da Brevo não foi encontrada nas variáveis de ambiente.")

    # ===========================
    # Função de Autenticação
    # ===========================
    def authenticate():
        """Verifica se a autenticação na API da Brevo está funcionando."""
        logging.info("Verificando autenticação na API da Brevo...")

        url = "https://api.brevo.com/v3/emailCampaigns"
        headers = {"accept": "application/json", "api-key": API_KEY}

        try:
            response = requests.get(url, headers=headers, params={"limit": 1})
            logging.info(f"Tentativa de autenticação na API. Status: {response.status_code}")

            if response.status_code == 200:
                logging.info("Autenticação bem-sucedida!")
                return True
            else:
                logging.error(f"Falha na autenticação! Código {response.status_code}: {response.text}")
                return False
        except requests.exceptions.RequestException as e:
            logging.error(f"Erro de conexão ao autenticar: {e}")
            return False

    # ===================================
    # Função para Coletar Campanhas
    # ===================================
    def fetch_campaigns(campaign_type):
        """Busca campanhas de email ou SMS na API da Brevo."""
        logging.info(f"Buscando campanhas de {campaign_type.upper()}...")

        url = f"https://api.brevo.com/v3/{campaign_type}Campaigns"
        headers = {"accept": "application/json", "api-key": API_KEY}
        params = {"limit": 100, "offset": 0, "sort": "desc"}
        campaigns = []

        while True:
            try:
                response = requests.get(url, headers=headers, params=params)
                logging.info(f"Tentativa de busca de {campaign_type.upper()} campaigns. Status: {response.status_code}")

                if response.status_code != 200:
                    logging.error(f"Erro ao buscar {campaign_type}Campaigns: {response.status_code} - {response.text}")
                    break

                data = response.json().get("campaigns", [])
                if not data:
                    logging.info(f"Nenhuma campanha encontrada para {campaign_type.upper()}.")
                    break

                logging.info(f"{len(data)} campanhas de {campaign_type.upper()} coletadas nesta requisição.")
                campaigns.extend(data)
                params["offset"] += params["limit"]

                # Pequeno delay para evitar bloqueios da API
                time.sleep(0.5)

            except requests.exceptions.RequestException as e:
                logging.error(f"Erro ao buscar {campaign_type}Campaigns: {e}")
                break

        logging.info(f"Total de {len(campaigns)} campanhas de {campaign_type.upper()} coletadas.")
        return campaigns

    # ===================================
    # Função para Salvar no GCS
    # ===================================
    def upload_to_gcs(file_name, data):
        """Salva arquivos CSV no Google Cloud Storage."""
        logging.info(f"Enviando arquivo {file_name} para o Google Cloud Storage...")

        csv_file = open(customer['project_id'] + '_' + file_name + '_brevo.csv', 'w+')
        csv_file.write(data)
        credentials = gcs.load_credentials_from_env()
        gcs.write_file_to_gcs(
            bucket_name=BUCKET_NAME,
            local_file_path=csv_file.name,
            destination_name=f"{FOLDER_NAME}/{file_name}",
            credentials=credentials
        )
        logging.info(f"Arquivo {file_name} salvo com sucesso no bucket {BUCKET_NAME}/{FOLDER_NAME}!")

    # ===================================
    # Função para Criar e Salvar CSV
    # ===================================
    def save_campaigns_to_csv(campaigns, file_name):
        """Cria e envia arquivo CSV para GCS."""
        logging.info(f"Criando CSV para {file_name}...")

        if not campaigns:
            logging.warning(f"Nenhuma campanha disponível para {file_name}, pulando upload.")
            return

        output = io.StringIO()
        csv_writer = csv.writer(output, delimiter=';', quotechar='"', quoting=csv.QUOTE_MINIMAL)

        # Escreve cabeçalho e dados
        csv_writer.writerow(campaigns[0].keys())
        for campaign in campaigns:
            csv_writer.writerow(campaign.values())

        upload_to_gcs(file_name, output.getvalue())

    # ===================================
    # Função Principal
    # ===================================
    def main():
        """Função principal para execução."""
        logging.info("Iniciando processamento...")

        if not authenticate():
            return "Falha na autenticação", 403

        logging.info("Coletando campanhas...")
        
        email_campaigns = fetch_campaigns("email")
        sms_campaigns = fetch_campaigns("sms")

        logging.info("Criando e salvando arquivos CSV...")
        
        save_campaigns_to_csv(email_campaigns, "email_campaigns.csv")
        save_campaigns_to_csv(sms_campaigns, "sms_campaigns.csv")

        logging.info("Execução concluída com sucesso!")
        
        return "Execução concluída", 200

    main()


def get_extraction_tasks():
    """
    Get the list of data extraction tasks for Brevo.
    
    Returns:
        list: List of task configurations
    """
    return [
        {
            'task_id': 'run',
            'python_callable': run
        }
    ]
