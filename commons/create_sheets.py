# -*- coding: utf-8 -*-

import argparse
import logging
import os

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s: %(message)s'
)
logger = logging.getLogger(__name__)


def read_env_file(file_path):
    """Lê o arquivo .env e retorna um dicionário com as variáveis"""
    env_vars = {}
    try:
        with open(file_path, 'r') as file:
            for line in file:
                line = line.strip()
                if line and not line.startswith('#'):
                    key, value = line.split('=', 1)
                    env_vars[key] = value
        return env_vars
    except Exception as e:
        logger.error(f"Erro ao ler o arquivo .env: {e}")
        return {}


def generate_sql(project_id, bucket_name, tool, data_types):
    """Gera o SQL com base nas variáveis de ambiente"""
    data_types_list = data_types.split(',')
    sql = ""

    # Gera a primeira parte do SQL (EXTERNAL TABLES)
    for data_type in data_types_list:
        data_type_upper = data_type.upper()
        sql += f"# {data_type_upper}\n"
        sql += f"CREATE OR REPLACE EXTERNAL TABLE {{project_id}}.{tool}.{data_type}\n"
        sql += "OPTIONS (\n"
        sql += "  format = 'CSV',\n"
        sql += "  field_delimiter=';',\n"
        sql += "  skip_leading_rows=1,\n"
        sql += "  allow_quoted_newlines=true,\n"
        sql += f"  uris = ['gs://{{bucket_name}}/{data_type}/{data_type}.csv']);\n\n"

    # Adiciona o separador da seção GOLD
    sql += "-- GOLD\n"

    # Gera a segunda parte do SQL (GOLD TABLES)
    for data_type in data_types_list:
        data_type_upper = data_type.upper()
        sql += f"# {data_type_upper}\n"
        sql += f"CREATE OR REPLACE TABLE `{{project_id}}.vendas.{tool}_{data_type}_gold`\n"
        sql += "AS\n"
        sql += f"SELECT *\n"
        sql += f"FROM `{{project_id}}.{tool}.{data_type}`;\n\n"

    return sql.rstrip()


def main():
    # Configurar o parser de argumentos
    parser = argparse.ArgumentParser(description='Gera SQL a partir de arquivo .env')
    parser.add_argument('dir_path', help='Caminho para a pasta contendo o arquivo .env')
    parser.add_argument('-o', '--output', default='sheet.sql', help='Nome do arquivo de saída SQL')
    args = parser.parse_args()

    # Constrói o caminho completo para o arquivo .env
    env_file_path = os.path.join(args.dir_path, '.env')

    # Verifica se o arquivo .env existe
    if not os.path.isfile(env_file_path):
        logger.error(f"Arquivo .env não encontrado em '{args.dir_path}'")
        return

    # Lê as variáveis do arquivo .env
    env_vars = read_env_file(env_file_path)

    # Verifica se todas as variáveis necessárias existem
    required_vars = ['PROJECT_ID', 'BUCKET_NAME', 'TOOL', 'DATA_TYPES']
    for var in required_vars:
        if var not in env_vars:
            logger.error(f"Variável {var} não encontrada no arquivo .env")
            return

    # Obtém os valores das variáveis
    project_id = env_vars['PROJECT_ID']
    bucket_name = env_vars['BUCKET_NAME']
    tool = env_vars['TOOL']
    data_types = env_vars['DATA_TYPES']

    # Gera o SQL
    sql = generate_sql(project_id, bucket_name, tool, data_types)

    # Salva o SQL em um arquivo no mesmo diretório do .env
    output_path = os.path.join(args.dir_path, args.output)
    with open(output_path, 'w') as f:
        f.write(sql)

    logger.info(f"SQL gerado em '{output_path}'")


if __name__ == "__main__":
    main()
