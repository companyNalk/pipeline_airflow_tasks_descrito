"""
Kommo module for data extraction functions.
This module contains functions specific to the Kommo integration.
"""

from core import gcs

def run_leads(customer):
  import requests
  import csv
  from datetime import datetime, timezone
  from google.cloud import storage
  import os

  # Token de longa duração
  access_token = customer['token']

  # Subdomínio do Kommo
  subdomain = customer['subdomain']

  # URL base da API
  base_url = f'https://{subdomain}.kommo.com'

  # Nome do bucket e caminho do arquivo no GCS
  bucket_name = customer['bucket_name']
  gcs_file_path = 'leads/leads.csv'

  # Função para desaninhamento dos campos personalizados
  def flatten_custom_fields(custom_fields):
      flattened_fields = {}
      if custom_fields:
          for field in custom_fields:
              field_name = field['field_name'].rstrip(':')  # Remove os dois pontos do final do nome do campo
              values = field['values']
              flattened_fields[field_name] = '; '.join([str(value['value']) for value in values])
      return flattened_fields

  # Função para converter timestamps Unix para datas legíveis com reconhecimento de fuso horário
  def convert_timestamp_to_date(timestamp):
      if isinstance(timestamp, int) or (isinstance(timestamp, str) and timestamp.isdigit()):
          return datetime.fromtimestamp(int(timestamp), timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
      return timestamp

  # Função para desaninhamento dos embeddeds
  def flatten_embedded_data(lead):
      embedded_data = {}

      # Desaninhamento de contatos
      if '_embedded' in lead and 'contacts' in lead['_embedded']:
          contacts = lead['_embedded']['contacts']
          embedded_data['contacts'] = '; '.join([str(contact['id']) for contact in contacts])

      # Desaninhamento de empresas
      if '_embedded' in lead and 'companies' in lead['_embedded']:
          companies = lead['_embedded']['companies']
          embedded_data['companies'] = '; '.join([str(company['id']) for company in companies])

      # Desaninhamento de tags
      if '_embedded' in lead and 'tags' in lead['_embedded']:
          tags = lead['_embedded']['tags']
          embedded_data['tags'] = '; '.join([str(tag['name']) for tag in tags])

      # Desaninhamento de loss_reason
      if '_embedded' in lead and 'loss_reason' in lead['_embedded']:
          loss_reasons = lead['_embedded']['loss_reason']
          embedded_data['loss_reason'] = '; '.join([str(loss_reason['name']) for loss_reason in loss_reasons])

      return embedded_data

  # Função para fazer a coleta de leads
  def get_leads():
      headers = {
          'Authorization': f'Bearer {access_token}'
      }
      leads = []
      offset = 0
      while True:
          params = {
              'limit': 250,
              'page': offset // 250 + 1,
              'with': 'contacts,companies,tags,loss_reason,catalog_elements'
          }
          response = requests.get(f'{base_url}/api/v4/leads', headers=headers, params=params)
          print(f"Solicitando leads, página: {offset // 250 + 1}")
          if response.status_code == 204:  # Nenhum conteúdo
              print("Nenhum lead adicional foi encontrado.")
              break
          elif response.status_code == 401:  # Erro de autenticação
              print("Erro de autenticação. Verifique se o token de longa duração é válido.")
              break
          elif response.status_code != 200:
              print(f"Erro ao coletar leads: {response.status_code}")
              print(response.json())  # Mostra a mensagem de erro detalhada
              break

          data = response.json()['_embedded']['leads']
          for lead in data:
              # Converter campos de data
              for date_field in ['closest_task_at', 'created_at', 'closed_at', 'updated_at']:
                  if date_field in lead:
                      lead[date_field] = convert_timestamp_to_date(lead[date_field])

              # Converter campos de data nos campos personalizados, se existirem
              if 'custom_fields_values' in lead:
                  custom_fields_flat = flatten_custom_fields(lead['custom_fields_values'])
                  lead.update(custom_fields_flat)
                  del lead['custom_fields_values']  # Remove o campo aninhado original

              # Desaninhamento dos campos embeddeds
              embedded_data = flatten_embedded_data(lead)
              lead.update(embedded_data)

          leads.extend(data)
          print(f"Coletados {len(leads)} leads até agora...")

          if len(data) < 250:  # Se o número de leads retornados for menor que o limite, não há mais dados
              break

          offset += 250

      return leads

  # Função para salvar os leads em um arquivo CSV e fazer upload para o GCS
  def save_leads_to_gcs(leads):
      # Obtenha todos os cabeçalhos únicos
      all_keys = set()
      for lead in leads:
          all_keys.update(lead.keys())

      # Salva o arquivo CSV localmente
      local_file_path = f"/tmp/{customer['project_id']}_leads.csv"
      with open(local_file_path, 'w', newline='', encoding='utf-8') as output_file:
          dict_writer = csv.DictWriter(output_file, fieldnames=list(all_keys), delimiter=';')
          dict_writer.writeheader()
          dict_writer.writerows(leads)

      credentials = gcs.load_credentials_from_env()
      gcs.write_file_to_gcs(
        bucket_name=bucket_name,
        local_file_path=local_file_path,
        destination_name=gcs_file_path,
        credentials=credentials
      )
      print(f"Arquivo CSV salvo no GCS: gs://{bucket_name}/{gcs_file_path}")

  # Função principal
  print("Iniciando a coleta de leads...")
  leads = get_leads()
  if leads:
      save_leads_to_gcs(leads)
      print(f"Coleta concluída. Um total de {len(leads)} leads foi salvo em leads.csv no GCS.")
  else:
      print("Nenhum lead foi coletado.")
def run_pipelines(customer):
  import requests
  import csv
  from google.cloud import storage
  import os

  # Token de acesso de longa duração
  access_token = customer['token']

  # Subdomínio do Kommo
  subdomain = customer['subdomain']

  #Bucket
  bucket_name = customer['bucket_name']


  # URL base da API
  base_url = f'https://{subdomain}.kommo.com'

  # Função para fazer a requisição dos pipelines
  def get_pipelines():
      headers = {
          'Authorization': f'Bearer {access_token}'
      }
      response = requests.get(f'{base_url}/api/v4/leads/pipelines', headers=headers)
      
      if response.status_code == 200:
          return response.json()["_embedded"]["pipelines"]
      elif response.status_code == 401:
          print("Erro de autenticação. Verifique se o token de longa duração é válido.")
      else:
          print(f"Erro ao obter os pipelines: {response.status_code}")
          print(response.json())
      return None

  # Função para salvar os pipelines em um arquivo CSV
  def save_pipelines_to_csv(pipelines_data):
      if not pipelines_data:
          print("Nenhum pipeline encontrado ou erro na requisição.")
          return

      with open(f"/tmp/{customer['project_id']}_pipelines.csv", mode="w", newline='', encoding='utf-8') as file:
          writer = csv.writer(file, delimiter=';')
          
          # Cabeçalho do CSV
          writer.writerow(["Pipeline ID", "Pipeline Name", "Status ID", "Status Name"])

          # Escrevendo os dados dos pipelines
          for pipeline in pipelines_data:
              pipeline_id = pipeline["id"]
              pipeline_name = pipeline["name"]
              for status in pipeline["_embedded"]["statuses"]:
                  status_id = status["id"]
                  status_name = status["name"]
                  writer.writerow([pipeline_id, pipeline_name, status_id, status_name])

      print("Arquivo pipelines.csv gerado com sucesso.")

  # Função para fazer upload do arquivo CSV para o Google Cloud Storage
  def upload_to_gcs():
      # client = storage.Client()
      # bucket = client.bucket(bucket_name)
      # blob = bucket.blob("pipelines/pipelines.csv")
      # blob.upload_from_filename("/tmp/pipelines.csv")
      credentials = gcs.load_credentials_from_env()
      gcs.write_file_to_gcs(
        bucket_name=bucket_name,
        local_file_path=f"/tmp/{customer['project_id']}_pipelines.csv",
        destination_name="pipelines/pipelines.csv",
        credentials=credentials
      )
      print(f"Arquivo pipelines.csv enviado para o bucket {bucket_name} com sucesso.")

  # Função principal para ser executada na Cloud Function
  def main():
      print("Iniciando a execução do script...")
      pipelines_data = get_pipelines()
      if pipelines_data:
          save_pipelines_to_csv(pipelines_data)
          upload_to_gcs()

  main()
def run_users(customer):
  import requests
  import csv
  from google.cloud import storage
  import os

  # Token de acesso de longa duração
  access_token = customer['token']

  # Subdomínio do Kommo
  subdomain = customer['subdomain']


  # Bucket
  bucket_name = customer['bucket_name']

  # URL base da API
  base_url = f'https://{subdomain}.kommo.com'

  # Função para coletar dados da lista de usuários
  def get_users_list():
      url = f"{base_url}/api/v4/users"
      headers = {
          "Authorization": f"Bearer {access_token}",
          "Content-Type": "application/hal+json",
      }
      
      response = requests.get(url, headers=headers)
      
      if response.status_code == 200:
          data = response.json()
          return data["_embedded"]["users"]
      elif response.status_code == 401:
          print("Erro de autenticação. Verifique se o token de longa duração é válido.")
      else:
          print(f"Erro ao coletar a lista de usuários: {response.status_code}")
          print(response.json())
      return None

  # Função para salvar dados no CSV
  def save_to_csv(users):
      if not users:
          print("Nenhum dado de usuário para salvar.")
          return

      with open(f"/tmp/{customer['project_id']}_users_list.csv", mode="w", newline='', encoding="utf-8") as file:
          writer = csv.writer(file, delimiter=';')
          writer.writerow([
              "ID", "Name", "Email", "Language", "Is Admin", 
              "Is Free", "Is Active", "Group ID", "Role ID"
          ])
          
          for user in users:
              writer.writerow([
                  user["id"],
                  user["name"],
                  user["email"],
                  user["lang"],
                  user["rights"]["is_admin"],
                  user["rights"]["is_free"],
                  user["rights"]["is_active"],
                  user["rights"].get("group_id", "N/A"),
                  user["rights"].get("role_id", "N/A")
              ])

      print("Arquivo users_list.csv gerado com sucesso.")

  # Função para fazer upload do arquivo CSV para o Google Cloud Storage
  def upload_to_gcs():
      # client = storage.Client()
      # bucket = client.bucket(bucket_name)
      # blob = bucket.blob("users/users_list.csv")
      # blob.upload_from_filename("/tmp/users_list.csv")
      credentials = gcs.load_credentials_from_env()
      gcs.write_file_to_gcs(
        bucket_name=bucket_name,
        local_file_path=f"/tmp/{customer['project_id']}_users_list.csv",
        destination_name="users/users_list.csv",
        credentials=credentials
      )
      print(f"Arquivo users_list.csv enviado para o bucket {bucket_name} com sucesso.")

  # Função principal para ser executada na Cloud Function
  def main():
      print("Coletando lista de usuários...")
      users = get_users_list()
      
      if users:
          print("Salvando dados no CSV...")
          save_to_csv(users)
          upload_to_gcs()
          print("Dados salvos com sucesso e enviados para o Google Cloud Storage.")
      else:
          print("Nenhum usuário encontrado ou erro na requisição.")

  main()




def get_extraction_tasks():
    """
    Get the list of data extraction tasks for Kommo.
    
    Returns:
        list: List of task configurations
    """
    return [
        {
            'task_id': 'run_leads',
            'python_callable': run_leads
        },
        {
            'task_id': 'run_pipelines',
            'python_callable': run_pipelines
        },
        {
            'task_id': 'run_users',
            'python_callable': run_users
        }
    ]
