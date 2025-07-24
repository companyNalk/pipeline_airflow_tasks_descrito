import csv
import datetime
import os
import subprocess
import time
import urllib.parse
import urllib.request

from commons.app_inicializer import AppInitializer
from commons.report_generator import ReportGenerator
from generic.argument_manager import ArgumentManager

logger = AppInitializer.initialize()

CONFIG = {
    "base_url": "https://www.corretaimoveisaracatuba.com.br/bkp_dbs/",
    "backup_prefix": "KSI_MYSQL_2fae20faed2006fcd6e6d17038b7758a067a01a9a0f9e44f30981ed46e502469_",
    "target_tables": [
        'clientes',
        'clientes_imoveis',
        'ordem_atendimento',
        'contrato_venda_documentos',
        'contrato_distrato',
        'imoveis',
        'ocorrencias',
        'rd_station_leads',
        'automacao_leads_integracao'
    ],
    "mysql_config": {
        "container_name": "mysql-kurole",
        "password": "temp123",
        "database": "ksi_correta",
        "port": "3308"
    }
}


def get_arguments():
    """Configura e retorna os argumentos da linha de comando."""
    return (ArgumentManager("Script Kurole para extração de dados MySQL")
            .add("API_USERNAME", "Username para autenticação no servidor de backup", required=True)
            .add("API_PASSWORD", "Password para autenticação no servidor de backup", required=True)
            .add("PROJECT_ID", "ID do projeto Google Cloud", required=True)
            .add("CRM_TYPE", "Nome da ferramenta (kurole)", required=True)
            .add("GOOGLE_APPLICATION_CREDENTIALS", "Credencial GCS", required=True)
            .parse())


def get_current_day_backup():
    """Retorna o nome do arquivo de backup baseado no dia da semana"""
    dias_semana = {
        0: 'segunda',  # Segunda-feira
        1: 'terca',  # Terça-feira
        2: 'quarta',  # Quarta-feira
        3: 'quinta',  # Quinta-feira
        4: 'sexta',  # Sexta-feira
        5: 'sabado',  # Sábado
        6: 'domingo'  # Domingo
    }

    hoje = datetime.datetime.now().weekday()
    dia_nome = dias_semana[hoje]

    return f"{dia_nome}.sql.gz"


def download_backup(filename, username, password):
    """Baixa o arquivo de backup do servidor"""
    full_filename = CONFIG["backup_prefix"] + filename
    url = CONFIG["base_url"] + full_filename

    # Salvar backup na pasta output
    backup_path = os.path.join('output', filename)

    logger.info(f"📥 Baixando backup: {filename}")
    logger.info(f"🌐 URL: {url}")
    logger.info(f"👤 Usuário: {username}")

    try:
        # Criar requisição com autenticação básica
        password_mgr = urllib.request.HTTPPasswordMgrWithDefaultRealm()
        password_mgr.add_password(None, url, username, password)

        auth_handler = urllib.request.HTTPBasicAuthHandler(password_mgr)
        opener = urllib.request.build_opener(auth_handler)

        # Fazer o download
        logger.info("⏳ Iniciando download...")

        # Garantir que o diretório output existe
        os.makedirs('output', exist_ok=True)

        request = urllib.request.Request(url)
        with opener.open(request) as response:
            # Verificar se o arquivo existe
            if response.getcode() != 200:
                logger.error(f"❌ Erro ao baixar: HTTP {response.getcode()}")
                return False, None

            # Verificar se é um arquivo válido
            content_type = response.headers.get('Content-Type', '')
            content_length = response.headers.get('Content-Length', 0)

            logger.info(f"📋 Content-Type: {content_type}")
            logger.info(f"📊 Content-Length: {content_length} bytes")

            # Salvar arquivo na pasta output
            file_path = os.path.abspath(backup_path)
            logger.info(f"💾 Salvando em: {file_path}")

            with open(file_path, 'wb') as f:
                file_size = 0
                chunk_size = 8192

                while True:
                    chunk = response.read(chunk_size)
                    if not chunk:
                        break
                    f.write(chunk)
                    file_size += len(chunk)

                    # Mostrar progresso a cada MB
                    if file_size % (1024 * 1024) == 0:
                        logger.info(f"   📊 Baixado: {file_size // (1024 * 1024)} MB")

        # Verificar se o arquivo foi criado corretamente
        if not os.path.exists(file_path):
            logger.error(f"❌ Arquivo não foi criado: {file_path}")
            return False, None

        final_size = os.path.getsize(file_path)
        if final_size == 0:
            logger.error(f"❌ Arquivo criado mas está vazio: {file_path}")
            return False, None

        logger.info(f"✅ Download concluído: {filename} ({final_size:,} bytes - {final_size / 1024 / 1024:.1f} MB)")
        logger.info(f"📁 Arquivo salvo em: {file_path}")
        return True, backup_path

    except Exception as e:
        logger.error(f"❌ Erro no download: {e}")
        return False, None


def run_command(cmd, shell=False, encoding='utf-8'):
    """Executa comando e retorna resultado"""
    try:
        result = subprocess.run(cmd, shell=shell, capture_output=True, text=True, timeout=3000, encoding=encoding,
                                errors='replace')
        return result.returncode == 0, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        return False, "", "Timeout na execução"
    except UnicodeDecodeError:
        try:
            result = subprocess.run(cmd, shell=shell, capture_output=True, text=True, timeout=3000, encoding='latin-1')
            return result.returncode == 0, result.stdout, result.stderr
        except Exception:
            return False, "", "Erro de codificação"


def setup_mysql():
    """Sobe container MySQL"""
    logger.info("🐳 Configurando MySQL com Docker...")

    # Limpar container anterior
    run_command(['docker', 'stop', CONFIG["mysql_config"]["container_name"]])
    run_command(['docker', 'rm', CONFIG["mysql_config"]["container_name"]])

    # Subir MySQL com configurações para caracteres brasileiros
    cmd = [
        'docker', 'run', '--name', CONFIG["mysql_config"]["container_name"],
        '-e', f'MYSQL_ROOT_PASSWORD={CONFIG["mysql_config"]["password"]}',
        '-e', f'MYSQL_DATABASE={CONFIG["mysql_config"]["database"]}',
        '-p', f'{CONFIG["mysql_config"]["port"]}:3306',
        '--tmpfs', '/var/lib/mysql-files:rw,noexec,nosuid,size=1g',
        '-d', 'mysql:8.0',
        '--log-bin-trust-function-creators=1',
        '--secure-file-priv=/var/lib/mysql-files',
        '--character-set-server=utf8mb4',
        '--collation-server=utf8mb4_unicode_ci',
        '--default-time-zone=America/Sao_Paulo'
    ]

    success, stdout, stderr = run_command(cmd)
    if not success:
        logger.error(f"❌ Erro ao iniciar MySQL: {stderr}")
        return False

    logger.info("⏳ Aguardando MySQL inicializar...")
    time.sleep(20)

    # Testar conexão
    for i in range(15):
        logger.info(f"  Tentativa {i + 1}/15...")
        success, _, _ = run_command([
            'docker', 'exec', CONFIG["mysql_config"]["container_name"],
            'mysql', '-uroot', f'-p{CONFIG["mysql_config"]["password"]}', '-e', 'SELECT 1'
        ])
        if success:
            break
        time.sleep(5)
    else:
        logger.error("❌ MySQL não inicializou corretamente")
        return False

    logger.info("✅ MySQL iniciado!")
    return True


def restore_dump(dump_filename):
    """Restaura o dump SQL"""
    logger.info(f"📦 Restaurando dump: {dump_filename}")

    if not os.path.exists(dump_filename):
        logger.error(f"❌ Arquivo {dump_filename} não encontrado!")
        return False

    # Configurar variáveis do MySQL antes da restauração
    logger.info("⚙️  Configurando MySQL...")
    config_commands = [
        "SET GLOBAL log_bin_trust_function_creators = 1;",
        "SET GLOBAL sql_mode = 'NO_AUTO_VALUE_ON_ZERO';",
        "SET foreign_key_checks = 0;",
        f"CREATE DATABASE IF NOT EXISTS {CONFIG['mysql_config']['database']};",
        f"USE {CONFIG['mysql_config']['database']};"
    ]

    for cmd in config_commands:
        success, stdout, stderr = run_command([
            'docker', 'exec', CONFIG["mysql_config"]["container_name"],
            'mysql', '-uroot', f'-p{CONFIG["mysql_config"]["password"]}',
            '-e', cmd
        ])
        if not success and "already exists" not in stderr:
            logger.warning(f"⚠️  Aviso na configuração: {stderr}")

    # Restaurar dump
    logger.info("📥 Restaurando dados...")
    cmd = f'''
    zcat {dump_filename} | docker exec -i {CONFIG["mysql_config"]["container_name"]} mysql -uroot -p{CONFIG["mysql_config"]["password"]} \
    --init-command="SET SESSION sql_mode='NO_AUTO_VALUE_ON_ZERO'; SET SESSION foreign_key_checks=0;" \
    {CONFIG["mysql_config"]["database"]}
    '''

    success, stdout, stderr = run_command(cmd, shell=True)

    if not success:
        logger.error(f"❌ Erro ao restaurar dump: {stderr}")

        # Tentativa alternativa
        logger.warning("⚠️  Tentando método alternativo...")
        cmd = f'''
        zcat {dump_filename} | docker exec -i {CONFIG["mysql_config"]["container_name"]} mysql -uroot -p{CONFIG["mysql_config"]["password"]} \
        --init-command="SET SESSION sql_mode='NO_AUTO_VALUE_ON_ZERO'; SET SESSION foreign_key_checks=0; SET SESSION log_bin_trust_function_creators=1;"
        '''

        success, stdout, stderr = run_command(cmd, shell=True)

        if not success:
            logger.error(f"❌ Erro final na restauração: {stderr}")
            return False

    logger.info("✅ Dump restaurado!")
    return True


def discover_database():
    """Descobre qual database usar"""
    logger.info("🔍 Descobrindo databases...")

    # Listar todas as databases
    success, stdout, stderr = run_command([
        'docker', 'exec', CONFIG["mysql_config"]["container_name"],
        'mysql', '-uroot', f'-p{CONFIG["mysql_config"]["password"]}',
        '-e', 'SHOW DATABASES'
    ])

    if not success:
        logger.error(f"❌ Erro ao listar databases: {stderr}")
        return None

    logger.info(f"📊 Databases disponíveis:\n{stdout}")

    # Tentar encontrar a database com tabelas
    databases = []
    for line in stdout.strip().split('\n')[1:]:
        db = line.strip()
        if db and db not in ['information_schema', 'performance_schema', 'mysql', 'sys']:
            databases.append(db)

    # Testar cada database para ver qual tem tabelas
    for db in databases:
        logger.info(f"🔍 Testando database: {db}")
        success, stdout, stderr = run_command([
            'docker', 'exec', CONFIG["mysql_config"]["container_name"],
            'mysql', '-uroot', f'-p{CONFIG["mysql_config"]["password"]}', db,
            '-e', 'SHOW TABLES'
        ])

        if success and len(stdout.strip().split('\n')) > 1:
            logger.info(f"✅ Database com dados encontrada: {db}")
            return db

    logger.warning(f"⚠️  Nenhuma database com tabelas encontrada, usando {CONFIG['mysql_config']['database']}")
    return CONFIG['mysql_config']['database']


def export_table_to_csv(table_name, database):
    """Exporta uma tabela específica para CSV"""
    logger.info(f"📄 Exportando tabela: {table_name}")

    # Criar estrutura de pastas
    output_dir = os.path.join('output', table_name)
    os.makedirs(output_dir, exist_ok=True)
    csv_file_path = os.path.join(output_dir, f'{table_name}.csv')

    # Verificar se a tabela existe
    success, stdout, stderr = run_command([
        'docker', 'exec', CONFIG["mysql_config"]["container_name"],
        'mysql', '-uroot', f'-p{CONFIG["mysql_config"]["password"]}', database,
        '-e', f'SHOW TABLES LIKE "{table_name}"'
    ])

    if not success:
        logger.error(f"❌ Erro ao verificar tabela {table_name}: {stderr}")
        return False, 0

    # Verificar se retornou alguma tabela
    lines = stdout.strip().split('\n')
    if len(lines) < 2:
        logger.error(f"❌ Tabela {table_name} não encontrada!")
        return False, 0

    # Verificar quantos registros tem
    success, stdout, stderr = run_command([
        'docker', 'exec', CONFIG["mysql_config"]["container_name"],
        'mysql', '-uroot', f'-p{CONFIG["mysql_config"]["password"]}', database,
        '-e', f'SELECT COUNT(*) FROM {table_name}'
    ])

    record_count = 0
    if success:
        count = stdout.strip().split('\n')[-1].strip()
        record_count = int(count) if count.isdigit() else 0
        logger.info(f"📊 Tabela {table_name} tem {record_count} registros")

        if record_count == 0:
            logger.warning(f"⚠️  Tabela {table_name} está vazia, criando arquivo vazio")
            return create_empty_csv(table_name, database, csv_file_path), 0

    # Método robusto: extrair via SELECT com diferentes codificações
    encodings_to_try = ['utf8mb4', 'utf8', 'latin1']

    for encoding in encodings_to_try:
        logger.info(f"🔤 Tentando codificação: {encoding}")

        cmd = [
            'docker', 'exec', CONFIG["mysql_config"]["container_name"],
            'mysql', '-uroot', f'-p{CONFIG["mysql_config"]["password"]}', database,
            '--default-character-set=' + encoding,
            '--batch', '--raw',
            '-e', f'SELECT * FROM {table_name}'
        ]

        # Tentar diferentes codificações Python
        for py_encoding in ['utf-8', 'latin-1', 'cp1252']:
            try:
                success, stdout, stderr = run_command(cmd, encoding=py_encoding)

                if success and stdout:
                    logger.info(f"✅ Dados obtidos com {encoding}/{py_encoding}")
                    break

            except Exception as e:
                logger.warning(f"⚠️  Erro com {encoding}/{py_encoding}: {e}")
                continue
        else:
            continue
        break
    else:
        logger.error(f"❌ Erro ao extrair dados de {table_name}")
        return False, 0

    # Processar saída do MySQL
    lines = stdout.strip().split('\n')
    if len(lines) < 1:
        logger.warning(f"⚠️  Tabela {table_name} vazia")
        return create_empty_csv(table_name, database, csv_file_path), 0

    # Primeira linha são os headers
    headers = lines[0].split('\t')

    # Criar CSV
    try:
        with open(csv_file_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(headers)

            data_rows = 0
            for line in lines[1:]:
                if line.strip():
                    row = []
                    for cell in line.split('\t'):
                        if cell == 'NULL':
                            row.append('')
                        else:
                            try:
                                clean_cell = cell.encode('utf-8', errors='ignore').decode('utf-8')
                                row.append(clean_cell)
                            except Exception:
                                row.append(str(cell))
                    writer.writerow(row)
                    data_rows += 1

        file_size = os.path.getsize(csv_file_path)
        logger.info(f"✅ {csv_file_path} criado! ({file_size:,} bytes, {data_rows} registros)")

        return True, data_rows

    except Exception as e:
        logger.error(f"❌ Erro ao criar CSV para {table_name}: {e}")
        return False, 0


def create_empty_csv(table_name, database, csv_file_path):
    """Cria CSV vazio com headers para tabelas vazias"""
    # Pegar estrutura da tabela
    success, stdout, stderr = run_command([
        'docker', 'exec', CONFIG["mysql_config"]["container_name"],
        'mysql', '-uroot', f'-p{CONFIG["mysql_config"]["password"]}', database,
        '-e', f'DESCRIBE {table_name}'
    ])

    if not success:
        logger.error(f"❌ Erro ao obter estrutura de {table_name}: {stderr}")
        return False

    # Extrair nomes das colunas
    lines = stdout.strip().split('\n')[1:]  # Pular header
    headers = [line.split('\t')[0] for line in lines if line.strip()]

    # Criar CSV apenas com headers
    with open(csv_file_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(headers)

    logger.info(f"✅ {csv_file_path} criado (vazio)!")
    return True


def cleanup():
    """Remove container de forma robusta"""
    logger.info("🧹 Limpando...")

    container_name = CONFIG["mysql_config"]["container_name"]

    # Parar container se estiver rodando
    logger.info(f"⏹️  Parando container {container_name}...")
    success, stdout, stderr = run_command(['docker', 'stop', container_name])
    if success:
        logger.info(f"✅ Container {container_name} parado com sucesso")
    else:
        logger.info(f"ℹ️  Container {container_name} já estava parado")

    # Remover container
    logger.info(f"🗑️  Removendo container {container_name}...")
    success, stdout, stderr = run_command(['docker', 'rm', container_name])
    if success:
        logger.info(f"✅ Container {container_name} removido com sucesso")
    else:
        logger.info(f"ℹ️  Container {container_name} já estava removido")

    # Verificar se realmente foi removido
    success, stdout, stderr = run_command(
        ['docker', 'ps', '-a', '--filter', f'name={container_name}', '--format', '{{.Names}}'])
    if success and not stdout.strip():
        logger.info("✅ Limpeza concluída com sucesso!")
    else:
        logger.warning(f"⚠️  Container ainda existe: {stdout.strip()}")

        # Forçar remoção se necessário
        logger.info("🔨 Forçando remoção...")
        run_command(['docker', 'rm', '-f', container_name])
        logger.info("✅ Limpeza forçada concluída!")


def check_docker_availability():
    """Verifica se o Docker está disponível e funcionando"""
    logger.info("🐳 Verificando disponibilidade do Docker...")

    # Verificar se o socket do Docker existe
    if not os.path.exists('/var/run/docker.sock'):
        logger.error("❌ Socket do Docker não encontrado em /var/run/docker.sock")
        logger.error("💡 Certifique-se de montar o socket: -v /var/run/docker.sock:/var/run/docker.sock")
        raise Exception("Socket do Docker não disponível")

    # Verificar se consegue se conectar ao Docker
    success, stdout, stderr = run_command(['docker', '--version'])
    if not success:
        logger.error("❌ Não foi possível executar comando docker")
        logger.error(f"💡 Erro: {stderr}")
        raise Exception("Docker CLI não disponível")

    # Verificar conexão com daemon
    success, stdout, stderr = run_command(['docker', 'version'])
    if not success:
        logger.error("❌ Não foi possível conectar ao Docker daemon")
        logger.error(f"💡 Erro: {stderr}")
        logger.error("💡 Verifique as permissões do socket do Docker")
        raise Exception("Docker daemon não acessível")

    logger.info("✅ Docker disponível e funcionando")
    logger.info(f"📋 Versão: {stdout.split()[2] if len(stdout.split()) > 2 else 'N/A'}")


def main():
    """Função principal para coleta e processamento de dados"""
    args = get_arguments()

    global_start_time = ReportGenerator.init_report(logger)
    all_table_stats = {}

    logger.info("🚀 Iniciando sistema Kurole de extração MySQL")

    try:
        # Verificar Docker antes de qualquer coisa
        check_docker_availability()

        # Identificar dia da semana e arquivo de backup
        hoje = datetime.datetime.now()
        logger.info(f"📅 Hoje é: {hoje.strftime('%A, %d/%m/%Y')}")

        backup_filename = get_current_day_backup()
        logger.info(f"🎯 Arquivo de backup do dia: {backup_filename}")
        logger.info(f"🎯 Tabelas a serem exportadas: {', '.join(CONFIG['target_tables'])}")

        # Criar pasta output
        if not os.path.exists('output'):
            os.makedirs('output')
            logger.info("📁 Pasta 'output' criada!")

        # 1. Baixar arquivo de backup (retorna sucesso e caminho do arquivo)
        download_success, backup_file_path = download_backup(backup_filename, args.API_USERNAME, args.API_PASSWORD)
        if not download_success:
            raise Exception("Falha no download do backup")

        # 2. Setup MySQL
        if not setup_mysql():
            raise Exception("Falha na configuração do MySQL")

        # 3. Restaurar dump (usar o caminho correto)
        if not restore_dump(backup_file_path):
            cleanup()
            raise Exception("Falha na restauração do dump")

        # 4. Descobrir qual database usar
        database = discover_database()
        if not database:
            cleanup()
            raise Exception("Database não encontrada")

        # 5. Exportar tabelas específicas
        exported = 0
        total_records = 0

        logger.info(f"\n🎯 Iniciando exportação de {len(CONFIG['target_tables'])} tabelas específicas...")

        for i, table in enumerate(CONFIG['target_tables'], 1):
            logger.info(f"\n📋 Processando tabela {i}/{len(CONFIG['target_tables'])}: {table}")
            start_time = time.time()

            try:
                success, record_count = export_table_to_csv(table, database)

                stats = {
                    "registros": record_count,
                    "status": "Sucesso" if success else "Falha",
                    "tempo": time.time() - start_time,
                    "table_name": table
                }

                if success:
                    exported += 1
                    total_records += record_count
                    logger.info(f"   ✅ Sucesso! ({exported}/{len(CONFIG['target_tables'])} concluídas)")
                else:
                    logger.error("   ❌ Falhou!")
                    stats["status"] = "Falha na exportação"

                all_table_stats[table] = stats

            except Exception as e:
                logger.exception(f"   ❌ Erro inesperado: {e}")
                all_table_stats[table] = {
                    "registros": 0,
                    "status": f"Falha: {type(e).__name__}: {str(e)}",
                    "tempo": time.time() - start_time,
                    "table_name": table
                }

        # Relatório final
        logger.info("\n🎉 Exportação concluída!")
        logger.info(f"✅ Exportadas: {exported}/{len(CONFIG['target_tables'])} tabelas")
        logger.info(f"📊 Total de registros: {total_records:,}")

        # Relatório final consolidado
        if not ReportGenerator.final_summary(logger, all_table_stats, global_start_time):
            raise Exception("Falhas encontradas na execução")

        logger.info(f"🎉 Integração Kurole concluída com sucesso! {len(CONFIG['target_tables'])} tabelas processadas.")

        # Limpar arquivo de backup baixado (agora na pasta output)
        if backup_file_path and os.path.exists(backup_file_path):
            os.remove(backup_file_path)
            logger.info(f"🗑️  Arquivo {backup_file_path} removido")

    except KeyboardInterrupt:
        logger.warning("\n⚠️  Operação cancelada pelo usuário")
        cleanup()
        raise
    except Exception as e:
        logger.exception(f"❌ ERRO CRÍTICO: {e}")
        cleanup()
        raise
    finally:
        # Cleanup sempre executado em caso de erro
        cleanup()


if __name__ == "__main__":
    main()
