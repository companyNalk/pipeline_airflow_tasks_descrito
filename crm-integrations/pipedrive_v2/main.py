import glob
import json
import os
import re
import shutil
import time
import unicodedata

import requests

from commons.app_inicializer import AppInitializer
from commons.big_query import BigQuery
from commons.memory_monitor import MemoryMonitor
from commons.utils import Utils
from generic.argument_manager import ArgumentManager

logger = AppInitializer.initialize()
Utils.clean_output_folder(logger)


def get_arguments():
    """Configura e retorna os argumentos da linha de comando."""
    return (ArgumentManager("Script para coletar e processar dados da API ArboCRM")
            .add("API_DOMAIN", "URL base", required=True)
            .add("API_TOKEN", "Token de autenticação", required=True)
            .add("PROJECT_ID", "ID do projeto GCS", required=True)
            .add("CRM_TYPE", "Ferramenta: Nome aba sheets", required=True)
            .add("GOOGLE_APPLICATION_CREDENTIALS", "Credenciais GCS", required=True)
            .parse())


args = get_arguments()

BASE_URL = f'https://{args.API_DOMAIN}.pipedrive.com/api'
OUTPUT_DIR = './original_data'
NORMALIZED_DIR = './normalized'
CONVERTED_DIR = './converted'
CSV_DIR = './output'


def clean_and_create_directories():
    """Remove todos os arquivos e subpastas das pastas de trabalho e as recria"""
    logger.info("🧹 INICIANDO LIMPEZA DAS PASTAS DE TRABALHO")

    directories = [OUTPUT_DIR, NORMALIZED_DIR, CONVERTED_DIR, CSV_DIR]

    for directory in directories:
        if os.path.exists(directory):
            logger.info(f"🗑️ Removendo conteúdo de: {directory}")
            # Remove todo o conteúdo da pasta
            for filename in os.listdir(directory):
                file_path = os.path.join(directory, filename)
                try:
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.unlink(file_path)
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
                except Exception as e:
                    logger.error(f"❌ Erro ao remover {file_path}: {e}")

        # Recria a pasta
        os.makedirs(directory, exist_ok=True)
        logger.info(f"📁 Pasta recriada: {directory}")

    logger.info("✅ Limpeza das pastas concluída")


def normalize_name(name):
    """Normaliza o nome removendo acentos, espaços e caracteres especiais"""
    name = unicodedata.normalize('NFD', name)
    name = ''.join(char for char in name if unicodedata.category(char) != 'Mn')
    name = name.lower()
    name = re.sub(r'\s+', ' ', name.strip())
    name = name.replace(' ', '_')
    name = re.sub(r'[^a-z0-9_]', '', name)
    return name


def clean_text_data(value):
    """Remove quebras de linha e caracteres problemáticos"""
    if isinstance(value, str):
        # Remove quebras de linha simples e múltiplas
        value = re.sub(r'\r?\n+', ' ', value)
        # Remove espaços extras
        value = re.sub(r'\s+', ' ', value.strip())
    return value


# def flatten_dict(d, parent_key='', sep='_'):
#     """Achata dicionário recursivamente, incluindo listas"""
#     items = []
#
#     if isinstance(d, dict):
#         for k, v in d.items():
#             new_key = f"{parent_key}{sep}{k}" if parent_key else k
#
#             if isinstance(v, dict):
#                 items.extend(flatten_dict(v, new_key, sep=sep).items())
#             elif isinstance(v, list):
#                 # Para listas, criar uma coluna para cada índice com número no final
#                 for i, item in enumerate(v):
#                     list_key = f"{new_key}_{i}"
#                     if isinstance(item, (dict, list)):
#                         items.extend(flatten_dict(item, list_key, sep=sep).items())
#                     else:
#                         # Limpar texto antes de adicionar
#                         clean_item = clean_text_data(item)
#                         items.append((list_key, clean_item))
#                 # Também salvar o tamanho da lista
#                 items.append((f"{new_key}_length", len(v)))
#             else:
#                 # Limpar texto antes de adicionar
#                 clean_value = clean_text_data(v)
#                 items.append((new_key, clean_value))
#     elif isinstance(d, list):
#         # Se o item raiz for uma lista
#         for i, item in enumerate(d):
#             list_key = f"{parent_key}_{i}" if parent_key else str(i)
#             if isinstance(item, (dict, list)):
#                 items.extend(flatten_dict(item, list_key, sep=sep).items())
#             else:
#                 # Limpar texto antes de adicionar
#                 clean_item = clean_text_data(item)
#                 items.append((list_key, clean_item))
#         items.append((f"{parent_key}_length" if parent_key else "length", len(d)))
#     else:
#         # Limpar texto antes de adicionar
#         clean_value = clean_text_data(d)
#         items.append((parent_key, clean_value))
#
#     return dict(items)


def flatten_json_to_csv():
    """FASE 5: Converte JSONs para CSV achatados"""
    logger.info("🔄 INICIANDO CONVERSÃO JSON PARA CSV")
    logger.info("=" * 50)

    # Encontrar todos os arquivos JSON da pasta original
    original_files = glob.glob(os.path.join(OUTPUT_DIR, '*.json'))

    if not original_files:
        logger.warning("⚠️ Nenhum arquivo JSON encontrado para converter")
        return

    for original_file in original_files:
        filename = os.path.basename(original_file)
        base_name = filename.replace('.json', '')
        # csv_name = f"{base_name}.csv"

        # ORDEM: original_data → normalized → converted
        # 1. Verificar se existe versão convertida (prioridade máxima)
        converted_file = os.path.join(CONVERTED_DIR, f"{base_name}_converted.json")

        # 2. Verificar se existe versão normalizada
        normalized_file = os.path.join(NORMALIZED_DIR, f"{base_name}_normalized.json")

        # 3. Usar a melhor versão disponível
        if os.path.exists(converted_file):
            # Usar versão convertida (final)
            json_file = converted_file
            logger.info(f"🔄 Convertendo (versão convertida): {filename}")
        elif os.path.exists(normalized_file):
            # Usar versão normalizada (intermediária)
            json_file = normalized_file
            logger.info(f"🔄 Convertendo (versão normalizada): {filename}")
        else:
            # Usar versão original
            json_file = original_file
            logger.info(f"🔄 Convertendo (versão original): {filename}")

        try:
            # Carregar JSON
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)

            # Extrair os dados (assumindo estrutura com 'data')
            records = data.get('data', [])

            if not records:
                logger.warning(f"⚠️ Nenhum registro encontrado em {filename}")
                continue

            # Achatar cada registro completamente
            # flattened_records = []
            # for record in records:
            # flattened = flatten_dict(record)
            # flattened_records.append(flattened)

            # # Converter para DataFrame
            # df = pd.DataFrame(flattened_records)
            #
            # # Criar pasta para o CSV (ex: deals/ para deals.csv)
            # csv_folder = os.path.join(CSV_DIR, base_name)
            # os.makedirs(csv_folder, exist_ok=True)
            #
            # # Salvar CSV dentro da pasta específica
            # csv_path = os.path.join(csv_folder, csv_name)
            # df.to_csv(csv_path, index=False, encoding='utf-8')
            #
            # logger.info(f"✅ {base_name}/{csv_name} salvo: {len(df)} linhas x {len(df.columns)} colunas")
            Utils.process_and_save_data(records, base_name)

        except Exception as e:
            logger.error(f"❌ Erro ao converter {filename}: {e}")

    logger.info("✅ Conversão JSON para CSV concluída!")


def fetch_and_save(endpoint, filename, version='v1'):
    """Função genérica para fazer requisição com paginação e salvar JSON"""
    try:
        logger.info(f"🔄 Coletando {filename}...")

        all_data = []
        url = f"{BASE_URL}/{version}/{endpoint}"

        if version == 'v1':
            # Paginação V1: start/limit
            start = 0
            while True:
                params = {
                    'start': start,
                    'limit': 50,
                    'api_token': args.API_TOKEN
                }

                response = requests.get(url, params=params)
                response.raise_for_status()
                data = response.json()

                if data.get('data'):
                    all_data.extend(data['data'])

                # Verificar se há mais dados
                pagination = data.get('additional_data', {}).get('pagination', {})
                if not pagination.get('more_items_in_collection'):
                    break

                start = pagination.get('next_start', start + 50)
                time.sleep(0.2)  # Pausa entre páginas

        else:  # version == 'v2'
            # Paginação V2: cursor
            cursor = None
            while True:
                params = {
                    'limit': 50,
                    'api_token': args.API_TOKEN
                }
                if cursor:
                    params['cursor'] = cursor

                response = requests.get(url, params=params)
                response.raise_for_status()
                data = response.json()

                if data.get('data'):
                    all_data.extend(data['data'])

                # Verificar se há próximo cursor
                cursor = data.get('additional_data', {}).get('next_cursor')
                if not cursor:
                    break

                time.sleep(0.2)  # Pausa entre páginas

        # Salvar dados completos
        final_data = {
            'success': True,
            'data': all_data,
            'total_count': len(all_data)
        }

        filepath = os.path.join(OUTPUT_DIR, f"{filename}.json")
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(final_data, f, indent=2, ensure_ascii=False)

        logger.info(f"✅ {filename} salvo: {len(all_data)} registros")
        return final_data

    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Erro ao coletar {filename}: {e}")
        return None
    except Exception as e:
        logger.error(f"❌ Erro geral {filename}: {e}")
        return None


def normalize_field_files():
    """Normaliza os nomes nos arquivos *_fields.json e salva na pasta normalized"""
    logger.info("🔄 INICIANDO NORMALIZAÇÃO DOS CAMPOS")
    logger.info("=" * 50)

    # Encontrar todos os arquivos *_fields.json
    field_files = glob.glob(os.path.join(OUTPUT_DIR, '*_fields.json'))

    if not field_files:
        logger.warning("⚠️ Nenhum arquivo *_fields.json encontrado para normalizar")
        return

    for file_path in field_files:
        filename = os.path.basename(file_path)
        logger.info(f"🔄 Processando: {filename}")

        # Carregar o arquivo
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # Contador de campos normalizados
        normalized_count = 0

        # Normalizar os nomes nos campos
        for field in data.get('data', []):
            if 'name' in field:
                original_name = field['name']
                normalized_name = normalize_name(original_name)

                if original_name != normalized_name:
                    field['name'] = normalized_name
                    logger.debug(f"  {original_name} -> {normalized_name}")
                    normalized_count += 1

        # Salvar o arquivo normalizado na pasta normalized
        base_name = filename.replace('.json', '_normalized.json')
        output_path = os.path.join(NORMALIZED_DIR, base_name)

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        logger.info(f"✅ {base_name} salvo em /normalized: {normalized_count} campos normalizados")


def convert_lead_keys_to_field_names():
    """Converte leads usando campos normalizados"""
    logger.info("🔄 INICIANDO CONVERSÃO DOS LEADS")
    logger.info("=" * 50)

    leads_path = os.path.join(OUTPUT_DIR, 'leads.json')
    fields_path = os.path.join(NORMALIZED_DIR, 'lead_fields_normalized.json')

    # Verificar se os arquivos existem
    if not os.path.exists(leads_path):
        logger.warning(f"⚠️ Arquivo não encontrado: {leads_path}")
        return

    if not os.path.exists(fields_path):
        logger.warning(f"⚠️ Arquivo não encontrado: {fields_path}")
        return

    logger.info("🔄 Carregando arquivos...")

    # Carregar os arquivos
    with open(leads_path, 'r', encoding='utf-8') as f:
        leads = json.load(f)

    with open(fields_path, 'r', encoding='utf-8') as f:
        lead_fields = json.load(f)

    # Criar mapa key -> name
    field_map = {field['key']: field['name'] for field in lead_fields['data']}
    logger.info(f"📊 Mapeamento criado com {len(field_map)} campos")

    # Converter leads
    converted_count = 0
    total_conversions = 0

    for lead in leads['data']:
        # Encontrar chaves que são SHA-1 (40 caracteres hexadecimais)
        sha1_keys = [key for key in lead.keys()
                     if len(key) == 40 and all(c in '0123456789abcdef' for c in key.lower())]

        # Substituir apenas as chaves SHA-1 que existem no mapeamento
        lead_conversions = 0
        for key in sha1_keys:
            if key in field_map:
                field_name = field_map[key]
                value = lead[key]
                del lead[key]  # Remove a chave SHA-1
                lead[field_name] = value  # Adiciona com o nome do campo
                lead_conversions += 1
                total_conversions += 1

        if lead_conversions > 0:
            converted_count += 1

    # Salvar resultado na pasta converted
    output_path = os.path.join(CONVERTED_DIR, 'leads_converted.json')
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(leads, f, indent=2, ensure_ascii=False)

    logger.info("✅ Conversão concluída:")
    logger.info(f"   • {converted_count} leads convertidos")
    logger.info(f"   • {total_conversions} campos SHA-1 substituídos")
    logger.info(f"   • Arquivo salvo: {output_path}")


def normalize_custom_fields_across_entities():
    """Converte custom_fields para todos os outros tipos de dados"""
    logger.info("🔄 INICIANDO CONVERSÃO DOS CUSTOM FIELDS")
    logger.info("=" * 50)

    # Encontrar todos os arquivos *_fields_normalized.json exceto lead_fields
    field_files = glob.glob(os.path.join(NORMALIZED_DIR, '*_fields_normalized.json'))
    field_files = [f for f in field_files if 'lead_fields' not in f]

    if not field_files:
        logger.warning("⚠️ Nenhum arquivo *_fields_normalized.json encontrado para conversão")
        return

    for field_file in field_files:
        # Extrair o tipo base (ex: deal_fields_normalized.json -> deal)
        base_name = os.path.basename(field_file)
        data_type = base_name.replace('_fields_normalized.json', '')

        # Caminhos dos arquivos
        data_file = os.path.join(OUTPUT_DIR, f'{data_type}s.json')  # deals.json, organizations.json, etc.

        # Verificar se o arquivo de dados existe
        if not os.path.exists(data_file):
            logger.warning(f"⚠️ Arquivo de dados não encontrado: {data_file}")
            continue

        logger.info(f"🔄 Processando: {data_type}s")

        # Carregar os arquivos
        with open(data_file, 'r', encoding='utf-8') as f:
            data = json.load(f)

        with open(field_file, 'r', encoding='utf-8') as f:
            fields = json.load(f)

        # Criar mapa key -> name
        field_map = {field['key']: field['name'] for field in fields['data']}

        # Converter custom_fields
        converted_count = 0
        total_conversions = 0

        for item in data['data']:
            if 'custom_fields' in item and item['custom_fields']:
                new_custom_fields = {}
                item_conversions = 0

                for key, value in item['custom_fields'].items():
                    field_name = field_map.get(key, key)  # Se não encontrar, usa a key original
                    new_custom_fields[field_name] = value
                    if key in field_map:
                        item_conversions += 1
                        total_conversions += 1

                item['custom_fields'] = new_custom_fields
                if item_conversions > 0:
                    converted_count += 1

        # Salvar resultado na pasta converted
        output_path = os.path.join(CONVERTED_DIR, f'{data_type}s_converted.json')
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        logger.info(f"✅ {data_type}s convertido:")
        logger.info(f"   • {converted_count} itens com custom_fields convertidos")
        logger.info(f"   • {total_conversions} campos convertidos")
        logger.info(f"   • Arquivo salvo: {output_path}")


def collect_organization_fields():
    """01. GET /api/v1/organizationFields"""
    return fetch_and_save('organizationFields', 'organization_fields', 'v1')


def collect_deal_fields():
    """02. GET /api/v1/dealFields"""
    return fetch_and_save('dealFields', 'deal_fields', 'v1')


def collect_person_fields():
    """03. GET /api/v1/personFields"""
    return fetch_and_save('personFields', 'person_fields', 'v1')


def collect_activity_fields():
    """04. GET /api/v1/activityFields"""
    return fetch_and_save('activityFields', 'activity_fields', 'v1')


def collect_product_fields():
    """05. GET /api/v1/productFields"""
    return fetch_and_save('productFields', 'product_fields', 'v1')


def collect_lead_fields():
    """06. GET /api/v1/leadFields"""
    return fetch_and_save('leadFields', 'lead_fields', 'v1')


def collect_pipelines():
    """07. GET /api/v2/pipelines"""
    return fetch_and_save('pipelines', 'pipelines', 'v2')


def collect_stages():
    """08. GET /api/v2/stages"""
    return fetch_and_save('stages', 'stages', 'v2')


def collect_organizations():
    """09. GET /api/v2/organizations"""
    return fetch_and_save('organizations', 'organizations', 'v2')


def collect_persons():
    """10. GET /api/v2/persons"""
    return fetch_and_save('persons', 'persons', 'v2')


def collect_products():
    """11. GET /api/v2/products"""
    return fetch_and_save('products', 'products', 'v2')


def collect_users():
    """12. GET /api/v1/users"""
    return fetch_and_save('users', 'users', 'v1')


def collect_deals():
    """13. GET /api/v2/deals"""
    return fetch_and_save('deals', 'deals', 'v2')


def collect_activities():
    """14. GET /api/v2/activities"""
    return fetch_and_save('activities', 'activities', 'v2')


def collect_leads():
    """15. GET /api/v1/leads"""
    return fetch_and_save('leads', 'leads', 'v1')


def main():
    """Executar coleta e normalização completa"""
    logger.info("🚀 INICIANDO COLETA E NORMALIZAÇÃO PIPEDRIVE")
    logger.info("=" * 50)

    start_time = time.time()

    # FASE 0: LIMPEZA DAS PASTAS
    clean_and_create_directories()

    # FASE 1: COLETA DOS DADOS
    logger.info("FASE 1: COLETANDO DADOS DO PIPEDRIVE")
    functions = [
        collect_organization_fields,
        collect_deal_fields,
        collect_person_fields,
        collect_activity_fields,
        collect_product_fields,
        collect_lead_fields,
        collect_pipelines,
        collect_stages,
        collect_organizations,
        collect_persons,
        collect_products,
        collect_users,
        collect_deals,
        collect_activities,
        collect_leads
    ]

    results = []
    for func in functions:
        result = func()
        results.append(result)
        time.sleep(0.5)

    # FASE 2: NORMALIZAÇÃO DOS CAMPOS
    normalize_field_files()

    # FASE 3: CONVERSÃO DOS LEADS
    convert_lead_keys_to_field_names()

    # FASE 4: CONVERSÃO DOS CUSTOM FIELDS
    normalize_custom_fields_across_entities()

    # FASE 5: CONVERSÃO PARA CSV
    flatten_json_to_csv()

    end_time = time.time()
    duration = end_time - start_time

    logger.info("=" * 50)
    logger.info(f"✅ PROCESSO COMPLETO CONCLUÍDO em {duration:.2f} segundos")
    logger.info(f"📁 Dados originais salvos em: {OUTPUT_DIR}")
    logger.info(f"📁 Campos normalizados salvos em: {NORMALIZED_DIR}")
    logger.info(f"📁 Dados convertidos (JSON) salvos em: {CONVERTED_DIR}")
    logger.info(f"📁 Arquivos CSV achatados salvos em: {CSV_DIR}")

    # Resumo
    successful = sum(1 for r in results if r is not None)
    logger.info(f"📊 {successful}/{len(functions)} endpoints coletados com sucesso")
    logger.info("📊 Arquivos *_fields.json normalizados com sucesso")
    logger.info("📊 Leads convertidos com campos legíveis")
    logger.info("📊 Custom fields convertidos para todos os tipos de dados")
    logger.info("📊 Todos os arquivos convertidos para CSV achatados")

    with MemoryMonitor(logger):
        BigQuery.process_csv_files()

    tables = Utils.get_existing_folders(logger)
    for table in tables:
        BigQuery.start_pipeline(args.PROJECT_ID, args.CRM_TYPE, table_name=table,
                                credentials_path=args.GOOGLE_APPLICATION_CREDENTIALS)


if __name__ == "__main__":
    main()
