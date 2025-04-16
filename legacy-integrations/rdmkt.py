"""
RD Marketing module for data extraction functions.
This module contains functions specific to the RD Marketing integration.
"""

from core import gcs

def run_conversions(customer):
  """
  Extract conversion data from RD Marketing API.
  
  Args:
      customer (dict): Customer configuration containing API credentials and settings
  """
  print(customer)
  import requests
  import pandas as pd
  import datetime
  import os
  import io

  # Configurações Iniciais
  CLIENT_ID = customer['client_id']
  CLIENT_SECRET = customer['client_secret']
  TOKEN_URL = 'https://api.rd.services/auth/token'
  CONVERSION_STATS_URL = 'https://api.rd.services/platform/analytics/conversions'
  refresh_token = customer['refresh_token']
  BUCKET_NAME = customer['bucket_name']
  SAVE_DIR = customer['save_dir_conversoes']

  def get_access_token():
      """Obtém o token de acesso da API RD Station."""
      data = {
          'client_id': CLIENT_ID,
          'client_secret': CLIENT_SECRET,
          'refresh_token': refresh_token,
          'grant_type': 'refresh_token'
      }
      response = requests.post(TOKEN_URL, data=data)
      response.raise_for_status()
      return response.json()['access_token']


  def fetch_conversion_stats(start_date, end_date, access_token):
      """Busca estatísticas de conversões da API RD Station."""
      headers = {
          'Authorization': f'Bearer {access_token}',
          'Content-Type': 'application/json'
      }
      params = {
          'start_date': start_date,
          'end_date': end_date
      }
      response = requests.get(CONVERSION_STATS_URL, headers=headers, params=params)
      response.raise_for_status()
      return response.json()


  def save_to_csv_and_upload(data, bucket_name, destination_blob_name):
      """Salva os dados em um arquivo CSV e faz o upload para o GCS."""
      if 'conversions' in data:
          df = pd.json_normalize(data['conversions'])
          if not df.empty:
              credentials = gcs.load_credentials_from_env()
              local_file_path = f"/tmp/{customer['project_id']}.rdmkt.run_conversions.csv"
              df.to_csv(local_file_path, index=False)
              gcs.write_file_to_gcs(
                bucket_name=bucket_name,
                local_file_path=local_file_path,
                destination_name=destination_blob_name,
                credentials=credentials
              )
              print(f"Arquivo salvo em no GCS: {destination_blob_name}.")
          else:
              print("DataFrame está vazio. Nenhum dado para salvar.")
      else:
          print("Nenhum dado para salvar.")


  def main():
      """Função principal para execução local."""
      access_token = get_access_token()
      # Coletar dados dos últimos 40 dias
      end_date = datetime.date.today() - datetime.timedelta(days=1)
      start_date = end_date - datetime.timedelta(days=40)
      
      current_date = start_date
      while current_date <= end_date:
          next_date = current_date + datetime.timedelta(days=1)
          date_str = current_date.strftime('%Y-%m-%d')
          file_name = f"dados_conversoes_{current_date.strftime('%d_%m_%Y')}.csv"
          destination_blob_name = f"{SAVE_DIR}/{file_name}"
          
          print(f"Coletando dados de {date_str}")
          conversion_stats = fetch_conversion_stats(date_str, date_str, access_token)
          save_to_csv_and_upload(conversion_stats, BUCKET_NAME, destination_blob_name)
          
          current_date = next_date

      return 'Data collection and upload completed'

  main()


def run_emails(customer):
  """
  Extract email data from RD Marketing API.
  
  Args:
      customer (dict): Customer configuration containing API credentials and settings
  """
  import requests
  import pandas as pd
  import datetime
  import io

  # Configurações Iniciais
  CLIENT_ID = customer['client_id']
  CLIENT_SECRET = customer['client_secret']
  TOKEN_URL = 'https://api.rd.services/auth/token'
  EMAIL_STATS_URL = 'https://api.rd.services/platform/analytics/emails'
  refresh_token = customer['refresh_token']
  BUCKET_NAME = customer['bucket_name']
  SAVE_DIR = customer['save_dir_email']

  def get_access_token():
      """Obtém o token de acesso da API RD Station."""
      data = {
          'client_id': CLIENT_ID,
          'client_secret': CLIENT_SECRET,
          'refresh_token': refresh_token,
          'grant_type': 'refresh_token'
      }
      response = requests.post(TOKEN_URL, data=data)
      response.raise_for_status()
      return response.json()['access_token']


  def fetch_email_stats(start_date, end_date, access_token):
      """Busca estatísticas de e-mails da API RD Station."""
      headers = {
          'Authorization': f'Bearer {access_token}',
          'Content-Type': 'application/json'
      }
      params = {
          'start_date': start_date,
          'end_date': end_date
      }
      response = requests.get(EMAIL_STATS_URL, headers=headers, params=params)
      response.raise_for_status()
      return response.json()


  def save_to_csv_and_upload(data, bucket_name, destination_blob_name):
      """Salva os dados em um arquivo CSV e faz o upload para o GCS."""
      try:
          if 'emails' in data:
              df = pd.json_normalize(data['emails'])
              if not df.empty:
                  print(f"Arquivo salvo em {destination_blob_name}.")
                  credentials = gcs.load_credentials_from_env()
                  local_file_path = f"/tmp/{customer['project_id']}.rdmkt.run_emails.csv"
                  df.to_csv(local_file_path, index=False)
                  gcs.write_file_to_gcs(
                    bucket_name=bucket_name,
                    local_file_path=local_file_path,
                    destination_name=destination_blob_name,
                    credentials=credentials
                  )
              else:
                  print("DataFrame está vazio. Nenhum dado para salvar.")
          else:
              print("Nenhum dado para salvar.")
      except Exception as e:
          print(f"Erro ao salvar dados no GCS: {str(e)}")
          raise


  def main():
      """Função principal para execução local."""
      access_token = get_access_token()
      # Coletar dados dos últimos 7 dias
      end_date = datetime.date.today() - datetime.timedelta(days=1)
      start_date = end_date - datetime.timedelta(days=43)
      
      current_date = start_date
      while current_date <= end_date:
          next_date = current_date + datetime.timedelta(days=1)
          date_str = current_date.strftime('%Y-%m-%d')
          file_name = f"dados_emails_{current_date.strftime('%d_%m_%Y')}.csv"
          destination_blob_name = f"{SAVE_DIR}/{file_name}"
          
          print(f"Coletando dados de {date_str}")
          email_stats = fetch_email_stats(date_str, date_str, access_token)
          save_to_csv_and_upload(email_stats, BUCKET_NAME, destination_blob_name)
          
          current_date = next_date

  main()


def get_extraction_tasks():
    """
    Get the list of data extraction tasks for RD Marketing.
    
    Returns:
        list: List of task configurations
    """
    return [
        {
            'task_id': 'run_conversions',
            'python_callable': run_conversions
        },
        {
            'task_id': 'run_emails',
            'python_callable': run_emails
        }
    ]
