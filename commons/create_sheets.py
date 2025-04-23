import argparse
import logging
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s: %(message)s'
)
logger = logging.getLogger(__name__)


def read_config_file(file_path):
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
        logger.error(f"Erro ao ler o arquivo de configuração: {e}")
        return {}


def generate_sql(tool, endpoints):
    """Gera o SQL com base nas variáveis de ambiente"""
    endpoints_list = endpoints.split(',')
    sql = ""

    # Gera a primeira parte do SQL (EXTERNAL TABLES)
    for endpoint in endpoints_list:
        endpoint_upper = endpoint.upper()
        sql += f"# {endpoint_upper}\n"
        sql += f"CREATE OR REPLACE EXTERNAL TABLE {{project_id}}.{tool}.{endpoint}\n"
        sql += "OPTIONS (\n"
        sql += "  format = 'CSV',\n"
        sql += "  field_delimiter=';',\n"
        sql += "  skip_leading_rows=1,\n"
        sql += "  allow_quoted_newlines=true,\n"
        sql += f"  uris = ['gs://{{bucket_name}}/{endpoint}/{endpoint}.csv']);\n\n"

    # Adiciona o separador da seção GOLD
    sql += "-- GOLD\n"

    # Gera a segunda parte do SQL (GOLD TABLES)
    for endpoint in endpoints_list:
        endpoint_upper = endpoint.upper()
        sql += f"# {endpoint_upper}\n"
        sql += f"CREATE OR REPLACE TABLE `{{project_id}}.vendas.{tool}_{endpoint}_gold`\n"
        sql += "AS\n"
        sql += "SELECT *\n"
        sql += f"FROM `{{project_id}}.{tool}.{endpoint}`;\n\n"

    return sql.rstrip()


def main():
    # Configurar o parser de argumentos
    parser = argparse.ArgumentParser(description='Gera SQL a partir de arquivo de configuração')
    parser.add_argument('dir_path', help='Caminho para a pasta contendo o arquivo de configuração')
    parser.add_argument('-o', '--output', default='sheet.sql', help='Nome do arquivo de saída SQL')
    args = parser.parse_args()

    # Procura por arquivos .env
    env_file_path = None
    for ext in ['.env']:
        temp_path = os.path.join(args.dir_path, f"config{ext}")
        if os.path.isfile(temp_path):
            env_file_path = temp_path
            break

    # Se não encontrou arquivo com nome padrão, procura qualquer arquivo .env
    if not env_file_path:
        for file in os.listdir(args.dir_path):
            if file.endswith('.env'):
                env_file_path = os.path.join(args.dir_path, file)
                break

    # Verifica se encontrou algum arquivo
    if not env_file_path:
        logger.error(f"Nenhum arquivo .env encontrado em '{args.dir_path}'")
        return

    # Lê as variáveis do arquivo
    env_vars = read_config_file(env_file_path)

    # Verifica se todas as variáveis necessárias existem
    required_vars = ['TOOL', 'ENDPOINTS']
    for var in required_vars:
        if var not in env_vars:
            logger.error(f"Variável {var} não encontrada no arquivo de configuração")
            return

    # Obtém os valores das variáveis
    tool = env_vars['TOOL']
    endpoints = env_vars['ENDPOINTS']

    # Gera o SQL
    sql = generate_sql(tool, endpoints)

    # Salva o SQL em um arquivo no mesmo diretório do arquivo de configuração
    output_path = os.path.join(args.dir_path, args.output)
    with open(output_path, 'w') as f:
        f.write(sql)

    logger.info(f"SQL gerado em '{output_path}'")


if __name__ == "__main__":
    main()
