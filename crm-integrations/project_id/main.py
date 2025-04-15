import csv
import json
import os
import sys
import time

import requests
from dotenv import load_dotenv

load_dotenv()

# Obtém as variáveis de ambiente com verificação
BASE_URL = os.getenv('BASE_URL')
X_CUSTOM_TOKEN = os.getenv('X_CUSTOM_TOKEN')

# Verifica se as variáveis necessárias estão definidas
if not BASE_URL:
    sys.exit("Erro: A variável BASE_URL não está definida no arquivo .env")
if not X_CUSTOM_TOKEN:
    sys.exit("Erro: A variável X_CUSTOM_TOKEN não está definida no arquivo .env")

BACKOFFICE_ENDPOINT = BASE_URL
BACKOFFICE_HEADERS = {
    "accept": "application/json",
    "x-custom-token": X_CUSTOM_TOKEN
}


def get_all_company_data():
    """
    Função para coletar os IDs e nomes de todas as empresas disponíveis no endpoint,
    considerando a paginação.
    """
    all_companies = []
    page = 1
    total_pages = 1

    try:
        while page <= total_pages:
            print(f"Coletando página {page} de {total_pages}...")

            # Faz a requisição para o endpoint que lista as empresas com getAll=true
            response = requests.get(
                url=f"{BACKOFFICE_ENDPOINT}/companies",
                params={
                    "getAll": "true",
                    "page": page,
                    "limit": 100  # Solicita o máximo de registros por página
                },
                headers=BACKOFFICE_HEADERS
            )

            # Verifica se a requisição foi bem-sucedida
            response.raise_for_status()

            # Converte a resposta para JSON
            data = response.json()

            # Atualiza o número total de páginas (se a API fornecer essa informação)
            # Os nomes exatos dos campos podem variar, ajuste conforme necessário
            if "pagination" in data:
                total_pages = data["pagination"].get("totalPages", total_pages)
            elif "meta" in data and "pagination" in data["meta"]:
                total_pages = data["meta"]["pagination"].get("totalPages", total_pages)

            # Extrai a lista de empresas
            companies = data.get('data', [])

            # Adiciona os dados das empresas à lista geral
            for company in companies:
                all_companies.append({
                    'id': company['id'],
                    'name': company.get('name', ''),
                    'normalizedName': company.get('normalizedName', '')
                })

            # Avança para a próxima página
            page += 1

            # Pausa curta para evitar sobrecarga da API
            time.sleep(0.5)

    except requests.exceptions.RequestException as e:
        print(f"Erro ao fazer requisição: {e}")
    except (KeyError, json.JSONDecodeError) as e:
        print(f"Erro ao processar dados: {e}")

    print(f"Total de empresas encontradas: {len(all_companies)}")
    return all_companies


def main():
    # Obter os dados das empresas
    companies = get_all_company_data()

    # Mostrar os IDs
    if companies:
        print(f"\nColetadas {len(companies)} empresas no total.")

        # Salvar os dados em um arquivo CSV
        csv_filename = "companies_data.csv"
        with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['id', 'name', 'normalizedName']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            for company in companies:
                writer.writerow(company)
        print(f"Dados completos salvos no arquivo '{csv_filename}'")


if __name__ == "__main__":
    main()
