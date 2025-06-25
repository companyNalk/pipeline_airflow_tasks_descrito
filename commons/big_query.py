import glob
import json
import logging
import os
import re
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from decimal import Decimal
from multiprocessing import cpu_count
from pathlib import Path

import dateutil.parser as date_parser
import pandas as pd
from google.auth import load_credentials_from_file
from google.cloud import bigquery
from google.oauth2 import service_account


class BigQuery:
    DEFAULT_OUTPUT_DIR = './output'
    LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
    CSV_DELIMITER = ';'

    # Valores para classificação de tipos
    BOOLEAN_VALUES = {
        'true', 'false'
    }

    # Mapeamento de tipos para BigQuery
    TYPE_MAPPING = {
        'integer': 'INT64',
        'numeric': 'NUMERIC',
        'float': 'FLOAT64',
        'date': 'DATE',
        'datetime': 'TIMESTAMP',
        'timestamp': 'TIMESTAMP',
        'time': 'TIME',
        'boolean': 'BOOLEAN',
        'string': 'STRING'
    }

    def __init__(self, output_dir=DEFAULT_OUTPUT_DIR, log_level=logging.INFO, max_workers=None):
        """Inicializa a classe BigQuery."""
        self.output_dir = Path(output_dir)
        self.max_workers = max_workers or cpu_count()
        self._setup_logging(log_level)
        self._lock = threading.Lock()  # Para thread-safe logging

    def _setup_logging(self, log_level):
        """Configura o sistema de logging."""
        logging.basicConfig(
            level=log_level,
            format=self.LOG_FORMAT,
            handlers=[
                logging.StreamHandler(),
            ]
        )
        self.logger = logging.getLogger(__name__)

    def _thread_safe_log(self, level, message):
        """Thread-safe logging."""
        with self._lock:
            getattr(self.logger, level)(message)

    @classmethod
    def process_csv_files(cls, output_dir=DEFAULT_OUTPUT_DIR, max_workers=None):
        """Processa todos os arquivos CSV no diretório especificado."""
        start_time = time.time()
        generator = cls(output_dir, max_workers=max_workers)
        generator.logger.info(f"INICINAOD PROCESSAMENTO DO CSV - COM {generator.max_workers} WORKERS")

        generator._process_all_csv_files()

        total_time = time.time() - start_time
        generator.logger.info(
            f"✅ Processamento completo finalizado em {total_time / 60:.2f} minutos ({total_time:.2f} segundos)")

    def _find_csv_files(self):
        """Encontra todos os arquivos CSV no diretório de saída."""
        return [Path(f) for f in glob.glob(str(self.output_dir / "*" / "*.csv"), recursive=True)]

    def _process_all_csv_files(self):
        """Processamento paralelo de múltiplos arquivos CSV."""
        files = self._find_csv_files()
        if not files:
            self.logger.warning(f"Nenhum arquivo CSV encontrado em {self.output_dir}")
            return

        self.logger.info(f"Encontrados {len(files)} arquivo(s). Processando com {self.max_workers} workers...")

        # Processamento paralelo de arquivos
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_file = {
                executor.submit(self._process_single_csv_safe, file_path): file_path
                for file_path in files
            }

            for future in as_completed(future_to_file):
                file_path = future_to_file[future]
                try:
                    future.result()
                    self._thread_safe_log('info', f"✅ Concluído: {file_path.name}")
                except Exception as e:
                    self._thread_safe_log('error', f"❌ Erro ao processar {file_path}: {e}")

    def _process_single_csv_safe(self, csv_path):
        """Wrapper thread-safe para processamento de CSV individual."""
        try:
            self._process_single_csv(csv_path)
        except Exception as e:
            self._thread_safe_log('error', f"Erro ao processar {csv_path}: {e}")
            raise

    def _process_single_csv(self, csv_path):
        """Processa um único arquivo CSV."""
        try:
            # Usar with para gerenciar recursos
            df = pd.read_csv(csv_path, delimiter=self.CSV_DELIMITER, dtype=str, low_memory=False)
            self._thread_safe_log('info',
                                  f"Arquivo {csv_path.name} carregado: {len(df)} linhas, {len(df.columns)} colunas")

            schema, report = self._generate_schema_parallel(df)
            self._generate_inconsistency_report(report)

            # Usar with para salvar o schema
            schema_path = csv_path.parent / "schema.json"
            self._save_schema(schema, schema_path)

        except pd.errors.EmptyDataError:
            self._thread_safe_log('error', f"Arquivo vazio: {csv_path}")
            raise
        except Exception as e:
            self._thread_safe_log('error', f"Erro no processamento do arquivo {csv_path}: {str(e)}")
            raise

    def _generate_schema_parallel(self, df):
        """Análise paralela de colunas usando multiprocessing."""
        schema = []
        report = []
        optimized = 0
        safe = 0
        total_columns = len(df.columns)
        start_time = time.time()

        self.logger.info(f"Iniciando análise paralela de {total_columns} colunas com {self.max_workers} processes...")

        # Preparar dados para processamento paralelo
        column_data = [(col_name, df[col_name].to_list()) for col_name in df.columns]

        # Usar ProcessPoolExecutor para análise intensiva de CPU
        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            # Submeter todas as tarefas
            future_to_column = {
                executor.submit(analyze_column_worker, col_name, col_data, self.BOOLEAN_VALUES,
                                self.TYPE_MAPPING): col_name
                for col_name, col_data in column_data
            }

            # Coletar resultados conforme completam
            completed = 0
            for future in as_completed(future_to_column):
                col_name = future_to_column[future]
                try:
                    analysis = future.result()
                    report.append(analysis)

                    # Determinar tipo final
                    if analysis['inconsistent_percent'] == 0:
                        final_type = analysis['suggested_type']
                        analysis['type_reason'] = "OTIMIZADO"
                        optimized += 1
                    else:
                        final_type = 'STRING'
                        analysis['type_reason'] = "SEGURO"
                        safe += 1

                    analysis['final_type'] = final_type
                    schema.append({'name': col_name, 'type': final_type, 'mode': 'NULLABLE'})

                    completed += 1
                    if completed % 10 == 0:
                        elapsed = time.time() - start_time
                        eta = (elapsed / completed) * (total_columns - completed)
                        self.logger.info(f"Progresso: {completed}/{total_columns} - ETA: {eta / 60:.1f} min")

                except Exception as e:
                    self.logger.error(f"Erro ao analisar coluna {col_name}: {e}")

        # Ordenar resultados pela ordem original das colunas
        column_order = {name: idx for idx, name in enumerate(df.columns)}
        report.sort(key=lambda x: column_order[x['column']])
        schema.sort(key=lambda x: column_order[x['name']])

        self.logger.info(f"Análise paralela concluída em {(time.time() - start_time) / 60:.2f} minutos")
        self.logger.info(f"RESUMO: {optimized} colunas otimizadas, {safe} colunas seguras (STRING)")
        return schema, report

    def _detect_pattern(self, value):
        """Detecta o padrão do valor e retorna o tipo correspondente."""
        if not isinstance(value, str):
            return 'string'

        value_lower = value.lower().strip()
        try:
            if self._is_integer(value):
                return 'integer'
            if self._is_numeric(value):
                return 'numeric'
            if self._is_float(value):
                return 'float'
            if value_lower in self.BOOLEAN_VALUES:
                return 'boolean'
            if self._is_time(value):
                return 'time'

            date_type = self._detect_date(value)
            if date_type:
                return date_type
        except Exception:
            pass

        return 'string'

    def _is_integer(self, v):
        """Verifica se o valor é um inteiro válido."""
        return bool(re.fullmatch(r"-?\d+", v)) and -9223372036854775808 <= int(v) <= 9223372036854775807

    def _is_numeric(self, v):
        """Verifica se o valor é um numérico de precisão alta."""
        if not re.fullmatch(r"-?\d+\.\d+", v):
            return False
        d = Decimal(v)
        return len(str(d).split('.')[-1]) > 6 and len(str(d).replace('.', '').replace('-', '')) <= 38

    def _is_float(self, v):
        """Verifica se o valor é um float válido."""
        return bool(re.fullmatch(r"-?\d+(\.\d+)?([eE][+-]?\d+)?", v))

    def _is_time(self, v):
        """Verifica se o valor é um horário válido - VERSÃO CORRIGIDA."""
        # Padrão mais flexível que aceita HH:MM e HH:MM:SS
        pattern_with_seconds = r"^([01]?\d|2[0-3]):([0-5]\d):([0-5]\d)(\.\d+)?$"
        pattern_without_seconds = r"^([01]?\d|2[0-3]):([0-5]\d)$"

        # Se tem segundos, é TIME válido
        if re.match(pattern_with_seconds, v):
            try:
                parts = v.split(':')
                hours = int(parts[0])
                minutes = int(parts[1])
                seconds = float(parts[2]) if '.' in parts[2] else int(parts[2])
                return (0 <= hours <= 23 and 0 <= minutes <= 59 and 0 <= seconds < 60)
            except (ValueError, IndexError):
                return False

        # Se só tem HH:MM, retornar FALSE para forçar STRING
        # BigQuery TIME precisa de segundos obrigatoriamente
        elif re.match(pattern_without_seconds, v):
            return False  # Força STRING em vez de TIME

        return False

    def _detect_date(self, v):
        """Detecta e classifica o tipo de data no valor - VERSÃO CORRIGIDA."""

        weekday_names = {
            'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday',
            'segunda', 'terça', 'quarta', 'quinta', 'sexta', 'sabado', 'domingo',
            'mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun',
            'seg', 'ter', 'qua', 'qui', 'sex', 'sab', 'dom'
        }

        # Se é só nome de dia da semana, retornar None (será STRING)
        if v.lower().strip() in weekday_names:
            return None

        # Filtrar outros valores problemáticos
        invalid_date_patterns = [
            r'^\d{1,2}:\d{2}$',  # HH:MM (horário sem segundos)
            r'^[a-zA-Z]+$',  # Só letras (provavelmente não é data)
        ]

        for pattern in invalid_date_patterns:
            if re.match(pattern, v.strip()):
                return None

        try:
            dt = date_parser.parse(v, fuzzy=False)

            # Verificar se parsing resultou em algo válido
            # Se ano está muito no futuro ou passado, provavelmente não é data real
            if dt.year < 1900 or dt.year > 2100:
                return None

            if dt.tzinfo or 'z' in v.lower():
                return 'timestamp'
            if dt.hour or dt.minute or dt.second:
                return 'datetime'
            return 'date'
        except Exception:
            return None

    def _generate_inconsistency_report(self, report, threshold=0.01):
        """Gera um relatório detalhado de inconsistências."""
        self.logger.info("=" * 100)
        self.logger.info("RELATÓRIO DE INCONSISTÊNCIAS E TIPOS APLICADOS")
        self.logger.info("=" * 100)

        try:
            # Estatísticas de tipos
            type_stats = Counter(col['final_type'] for col in report)
            optimized_count = len([col for col in report if col['type_reason'] == 'OTIMIZADO'])
            safe_count = len([col for col in report if col['type_reason'] == 'SEGURO'])

            self.logger.info("ESTATÍSTICAS DE TIPOS APLICADOS:")
            for type_name, count in type_stats.most_common():
                self.logger.info(f"  {type_name}: {count} colunas")
            self.logger.info(f"  Total otimizado: {optimized_count} colunas")
            self.logger.info(f"  Total seguro (STRING): {safe_count} colunas")
            self.logger.info("-" * 100)

            # Verificação de inconsistências
            columns_with_issues = [col for col in report if col['inconsistent_percent'] >= threshold]

            if not columns_with_issues:
                self.logger.info("Nenhuma inconsistência significativa encontrada!")
                return

            # Resumo executivo
            total_inconsistent = sum(col['inconsistent_count'] for col in columns_with_issues)
            self.logger.info("RESUMO EXECUTIVO:")
            self.logger.info(f"  Total de registros inconsistentes: {total_inconsistent:,}")
            self.logger.info(f"  Colunas afetadas: {len(columns_with_issues)}")
            self.logger.info(f"  Tipos mais afetados: {self._most_affected_types(columns_with_issues)}")
            self.logger.info("-" * 100)

            # Detalhes por coluna
            for col in columns_with_issues:
                self.logger.info(f"COLUNA: {col['column']}")
                self.logger.info(
                    f"  Tipo sugerido: {col['suggested_type']} → Aplicado: {col['final_type']} ({col['type_reason']})")
                self.logger.info(f"  Registros: {col['non_null_records']:,} válidos, {col['null_records']:,} nulos")
                self.logger.info(
                    f"  Inconsistências: {col['inconsistent_count']:,} ({col['inconsistent_percent']:.4f}%)")
                self.logger.info(f"  Confiança: {col['confidence_score']:.2f}%")
                if col['inconsistent_values']:
                    self.logger.info(f"  ⚠️  EXEMPLOS INCONSISTENTES: {col['inconsistent_values'][:5]}")
                self.logger.info(f"  Ação sugerida: Verificar e corrigir {col['inconsistent_count']:,} registros\n")

        except Exception as e:
            self.logger.error(f"Erro ao gerar relatório de inconsistências: {e}")

    def _most_affected_types(self, columns):
        """Identifica os tipos mais afetados por inconsistências."""
        try:
            type_counts = Counter(col['suggested_type'] for col in columns)
            return ', '.join(f"{t} ({c})" for t, c in type_counts.most_common(3))
        except Exception as e:
            self.logger.error(f"Erro ao calcular tipos mais afetados: {e}")
            return "Erro ao calcular"

    def _save_schema(self, schema, path):
        """Salva o esquema em um arquivo JSON."""
        try:
            schema_simple = [{"name": f["name"], "type": f["type"]} for f in schema]
            with open(path, 'w', encoding='utf-8') as f:
                json.dump(schema_simple, f, indent=2, ensure_ascii=False)
            self.logger.info(f"📄 Schema salvo em {path}")
        except Exception as e:
            self.logger.error(f"Erro ao salvar schema: {e}")
            raise

    @staticmethod
    def _convert_timezone_offsets(csv_path, schema_json, logger):
        """Converte timezone offsets para timestamps válidos do BigQuery."""
        try:
            logger.info("🔄 Convertendo timezone offsets...")

            df = pd.read_csv(csv_path, delimiter=';', dtype=str, low_memory=False)
            conversions = 0

            # Buscar colunas com 'timezone' no nome
            timezone_cols = [col for col in df.columns if 'timezone' in col.lower()]

            for col in timezone_cols:
                logger.info(f"Processando coluna: {col}")

                for idx, value in enumerate(df[col]):
                    if pd.isna(value):
                        continue

                    value_str = str(value).strip()

                    # Se é timezone offset (-03:00, +05:30)
                    if re.match(r'^[+-]([01]?\d|2[0-3]):([0-5]\d)$', value_str):
                        # Converter para timestamp válido: "2000-01-01 00:00:00-03:00"
                        timestamp_with_tz = f"2000-01-01 00:00:00{value_str}"
                        df.at[idx, col] = timestamp_with_tz
                        conversions += 1

                # Atualizar schema para TIMESTAMP
                for field in schema_json:
                    if field['name'] == col:
                        field['type'] = 'TIMESTAMP'

            if conversions > 0:
                # Salvar CSV atualizado
                df.to_csv(csv_path, sep=';', index=False, encoding='utf-8')
                logger.info(f"✅ {conversions} timezone offsets convertidos para TIMESTAMP")
            else:
                logger.info("ℹ️  Nenhum timezone offset encontrado")

        except Exception as e:
            logger.error(f"❌ Erro na conversão de timezone: {e}")
            raise

    @staticmethod
    def _convert_date_br_to_iso(csv_path, schema_json, logger):
        """
        Preprocessa dados do CSV para compatibilidade com BigQuery.
        Converte APENAS datas no formato D/MM/AAAA ou DD/MM/AAAA para AAAA-MM-DD.
        NUNCA converte se houver componente de hora.
        """
        try:
            logger.info("🔄 Iniciando preprocessamento de dados...")

            BigQuery._convert_timezone_offsets(csv_path, schema_json, logger)

            # Identificar campos de data
            date_fields = [field['name'] for field in schema_json if field['type'] == 'DATE']
            datetime_fields = [field['name'] for field in schema_json if field['type'] in ['TIMESTAMP', 'DATETIME']]

            if not date_fields and not datetime_fields:
                logger.info("ℹ️  Nenhum campo de data/datetime encontrado")
                return

            logger.info(f"📅 Campos DATE: {date_fields}")
            logger.info(f"🕐 Campos DATETIME/TIMESTAMP: {datetime_fields}")

            # Processar CSV
            df = pd.read_csv(csv_path, delimiter=';', dtype=str, low_memory=False)
            total_conversions = 0

            # Converter campos DATE
            for field in date_fields:
                if field in df.columns:
                    before = df[field].copy()

                    for idx, date_str in enumerate(df[field]):
                        # Converte APENAS D/MM/AAAA ou DD/MM/AAAA para AAAA-MM-DD (SEM HORA)
                        if not date_str or pd.isna(date_str) or str(date_str).strip() == '':
                            df.at[idx, field] = None
                            continue

                        date_str = str(date_str).strip()

                        # REJEITA se contém espaço (indica hora)
                        if ' ' in date_str:
                            df.at[idx, field] = date_str
                            continue

                        # Aceita APENAS D/MM/AAAA ou DD/MM/AAAA
                        if re.match(r'^\d{1,2}/\d{2}/\d{4}$', date_str):
                            parts = date_str.split('/')
                            day, month, year = parts

                            # Validar se mês e ano têm tamanho correto
                            if len(month) != 2 or len(year) != 4:
                                df.at[idx, field] = date_str
                                continue

                            # Validar se dia tem 1 ou 2 dígitos
                            if len(day) not in [1, 2]:
                                df.at[idx, field] = date_str
                                continue

                            # Validar valores
                            try:
                                day_int, month_int, year_int = int(day), int(month), int(year)
                                if not (1 <= day_int <= 31 and 1 <= month_int <= 12 and 1900 <= year_int <= 2100):
                                    df.at[idx, field] = None
                                    continue
                            except ValueError:
                                df.at[idx, field] = None
                                continue

                            # Converter para ISO
                            df.at[idx, field] = f"{year}-{month}-{day.zfill(2)}"
                        else:
                            df.at[idx, field] = date_str

                    converted = sum(1 for old, new in zip(before, df[field])
                                    if pd.notna(old) and pd.notna(new) and old != new)
                    if converted > 0:
                        total_conversions += converted
                        logger.info(f"   ✅ {field}: {converted} conversões")

            # Processar campos DATETIME (sem converter - NUNCA converte datetime)
            for field in datetime_fields:
                if field in df.columns:
                    for idx, datetime_str in enumerate(df[field]):
                        if not datetime_str or pd.isna(datetime_str):
                            df.at[idx, field] = None
                        else:
                            df.at[idx, field] = str(datetime_str).strip()

            # Salvar se houve conversões
            if total_conversions > 0:
                df.to_csv(csv_path, sep=';', index=False, encoding='utf-8')
                logger.info(f"✅ {total_conversions} conversão(ões) realizada(s)")
            else:
                logger.info("ℹ️  Nenhuma conversão necessária")

        except Exception as e:
            logger.error(f"❌ Erro no preprocessamento: {e}")
            raise

    @staticmethod
    def _create_externa_table(project_id, tool_name, table_name, credentials_path):
        """Carrega dados do CSV diretamente para uma tabela BigQuery."""
        import warnings
        # Suprimir warning específico do Google Auth sobre quota project
        warnings.filterwarnings("ignore", message="Your application has authenticated using end user credentials")

        start_time = time.time()

        # Setup de logging para método estático
        logger = logging.getLogger(__name__)
        logger.info(f"Iniciando criação da tabela externa: {project_id}.{tool_name}.{table_name}")

        try:
            # Detectar tipo de credencial automaticamente
            logger.info(f"Carregando credenciais de: {credentials_path}")

            # Ler arquivo para detectar o tipo
            with open(credentials_path, 'r') as f:
                cred_info = json.load(f)

            cred_type = cred_info.get('type', 'unknown')
            logger.info(f"Tipo de credencial detectado: {cred_type}")

            # Carregar credenciais baseado no tipo
            if cred_type == 'service_account':
                credentials = service_account.Credentials.from_service_account_file(credentials_path)
                client = bigquery.Client(project=project_id, credentials=credentials)
                logger.info("Usando credenciais de Service Account")
            elif cred_type == 'authorized_user':
                credentials, _ = load_credentials_from_file(credentials_path)
                # Configurar quota project para evitar warnings
                credentials = credentials.with_quota_project(project_id)
                client = bigquery.Client(project=project_id, credentials=credentials)
                logger.info("Usando credenciais de Authorized User (OAuth2) com quota project")
            else:
                logger.warning(f"Tipo de credencial desconhecido: {cred_type}. Tentando carregamento genérico...")
                credentials, _ = load_credentials_from_file(credentials_path)
                client = bigquery.Client(project=project_id, credentials=credentials)

            schema_path = os.path.join("./output", table_name, "schema.json")
            csv_path = os.path.join("./output", table_name, f"{table_name}.csv")

            if not os.path.exists(schema_path):
                logger.error(f"Arquivo de esquema não encontrado: {schema_path}")
                raise FileNotFoundError(f"Arquivo de esquema não encontrado: {schema_path}")
            if not os.path.exists(csv_path):
                logger.error(f"Arquivo CSV não encontrado: {csv_path}")
                raise FileNotFoundError(f"Arquivo CSV não encontrado: {csv_path}")

            logger.info(f"Carregando schema de: {schema_path}")
            logger.info(f"Carregando dados de: {csv_path}")

            with open(schema_path, 'r', encoding='utf-8') as f:
                schema_json = json.load(f)

            # Reprocessamento
            BigQuery._convert_date_br_to_iso(csv_path, schema_json, logger)

            schema = [
                bigquery.SchemaField(field["name"], field["type"], mode=field.get("mode", "NULLABLE"))
                for field in schema_json
            ]

            table_id_full = f"{project_id}.{tool_name}.{table_name}"
            logger.info(f"Configurando job de carga para tabela: {table_id_full}")

            job_config = bigquery.LoadJobConfig(
                schema=schema,
                skip_leading_rows=1,
                field_delimiter=";",
                source_format=bigquery.SourceFormat.CSV,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                max_bad_records=10,
                allow_jagged_rows=True,
                allow_quoted_newlines=True
            )

            logger.info("Removendo tabela existente (se houver)")
            client.delete_table(table_id_full, not_found_ok=True)

            logger.info("Iniciando upload do arquivo CSV para BigQuery")
            with open(csv_path, "rb") as source_file:
                job = client.load_table_from_file(source_file, table_id_full, job_config=job_config)

            logger.info("Aguardando conclusão do job de carga...")
            job.result()

            table = client.get_table(table_id_full)
            total_time = time.time() - start_time
            logger.info(f"✅ Tabela '{table_id_full}' criada com {table.num_rows:,} linhas em {total_time:.2f} segundos")
            logger.info(f"📁 Arquivo CSV processado: {csv_path}")

        except FileNotFoundError as e:
            logger.error(f"Erro de arquivo não encontrado: {e}")
            raise
        except Exception as e:
            logger.error(f"Erro ao criar tabela externa: {str(e)}")
            raise

    @staticmethod
    def _create_gold_table(project_id, tool_name, table_name, credentials_path):
        """Cria uma tabela gold no dataset 'vendas'."""
        start_time = time.time()

        # Setup de logging para método estático
        logger = logging.getLogger(__name__)
        logger.info(f"Iniciando criação da tabela gold para: {tool_name}_{table_name}_gold")

        try:
            # Detectar tipo de credencial automaticamente
            logger.info(f"Carregando credenciais de: {credentials_path}")

            # Ler arquivo para detectar o tipo
            with open(credentials_path, 'r') as f:
                cred_info = json.load(f)

            cred_type = cred_info.get('type', 'unknown')
            logger.info(f"Tipo de credencial detectado: {cred_type}")

            # Carregar credenciais baseado no tipo
            if cred_type == 'service_account':
                credentials = service_account.Credentials.from_service_account_file(credentials_path)
                client = bigquery.Client(project=project_id, credentials=credentials)
                logger.info("Usando credenciais de Service Account")
            elif cred_type == 'authorized_user':
                credentials, _ = load_credentials_from_file(credentials_path)
                credentials = credentials.with_quota_project(project_id)
                client = bigquery.Client(project=project_id, credentials=credentials)
                logger.info("Usando credenciais de Authorized User (OAuth2) com quota project")
            else:
                logger.warning(f"Tipo de credencial desconhecido: {cred_type}. Tentando carregamento genérico...")
                credentials, _ = load_credentials_from_file(credentials_path)
                client = bigquery.Client(project=project_id, credentials=credentials)

            source_table_id = f"{project_id}.{tool_name}.{table_name}"
            gold_table_id = f"{project_id}.vendas.{tool_name}_{table_name}_gold"

            logger.info(f"Origem: {source_table_id}")
            logger.info(f"Destino: {gold_table_id}")

            query = f"""
            CREATE OR REPLACE TABLE {gold_table_id}
            AS
            SELECT * FROM {source_table_id}
            """

            logger.info("Executando query para criação da tabela gold")
            query_job = client.query(query)
            query_job.result()

            table = client.get_table(gold_table_id)
            total_time = time.time() - start_time
            logger.info(f"✅ Tabela gold '{gold_table_id}' criada com sucesso em {total_time:.2f} segundos!")
            logger.info(f"📊 Linhas copiadas: {table.num_rows:,}")
            logger.info(f"📋 Colunas: {len(table.schema)}")
            logger.info(f"🔗 Origem: {source_table_id}")

        except Exception as e:
            total_time = time.time() - start_time
            logger.error(f"❌ Erro ao criar tabela gold após {total_time:.2f} segundos: {e}")
            raise

    @staticmethod
    def start_pipeline(project_id, tool_name, table_name, credentials_path):
        """Pipeline para criar as tabelas no BQ."""
        start_time = time.time()

        # Setup de logging para método estático
        logger = logging.getLogger(__name__)

        # Log de início com destaque
        logger.info("=" * 80)
        logger.info("🚀 INICIANDO PIPELINE BIGQUERY")
        logger.info(f"📋 Projeto: {project_id}")
        logger.info(f"🔧 Ferramenta: {tool_name}")
        logger.info(f"📊 Tabela: {table_name}")
        logger.info("=" * 80)

        try:
            logger.info("📥 [1/2] Criando tabela externa...")
            BigQuery._create_externa_table(project_id, tool_name, table_name, credentials_path)
            logger.info("✅ Tabela externa criada com sucesso")

            logger.info("🏆 [2/2] Criando tabela gold...")
            BigQuery._create_gold_table(project_id, tool_name, table_name, credentials_path)
            logger.info("✅ Tabela gold criada com sucesso")

            total_time = time.time() - start_time
            logger.info("=" * 80)
            logger.info("🎉 PIPELINE CONCLUÍDO COM SUCESSO!")
            logger.info(f"📊 Tabela: {table_name}")
            logger.info(f"⏱️  Tempo total: {total_time:.2f}s\n")

        except Exception as e:
            total_time = time.time() - start_time
            logger.error("=" * 80)
            logger.error("💥 PIPELINE FALHOU!")
            logger.error(f"📊 Tabela: {table_name}")
            logger.error(f"⏱️  Tempo até falha: {total_time:.2f}s")
            logger.error(f"🔥 Erro: {str(e)}")
            logger.error("=" * 80)
            raise


# Função worker para processamento paralelo de colunas - VERSÃO CORRIGIDA
def analyze_column_worker(column_name, column_data, boolean_values, type_mapping):
    """Worker function para análise paralela de colunas - VERSÃO CORRIGIDA."""

    def detect_pattern(value, boolean_values):
        """Função auxiliar para detectar padrões - VERSÃO CORRIGIDA."""
        if not isinstance(value, str):
            return 'string'

        value_lower = value.lower().strip()

        # Verificar se é inteiro
        if bool(re.fullmatch(r"-?\d+", value)) and -9223372036854775808 <= int(value) <= 9223372036854775807:
            return 'integer'

        # Verificar se é numérico
        if re.fullmatch(r"-?\d+\.\d+", value):
            d = Decimal(value)
            if len(str(d).split('.')[-1]) > 6 and len(str(d).replace('.', '').replace('-', '')) <= 38:
                return 'numeric'

        # Verificar se é float
        if bool(re.fullmatch(r"-?\d+(\.\d+)?([eE][+-]?\d+)?", value)):
            return 'float'

        # Verificar se é booleano
        if value_lower in boolean_values:
            return 'boolean'

        # CORREÇÃO: Verificar TIME com segundos obrigatórios
        time_pattern_valid = r"^([01]?\d|2[0-3]):([0-5]\d):([0-5]\d)(\.\d+)?$"
        if re.match(time_pattern_valid, value):
            return 'time'

        # CORREÇÃO: Filtrar dias da semana e HH:MM
        weekday_names = {
            'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday',
            'segunda', 'terça', 'quarta', 'quinta', 'sexta', 'sabado', 'domingo',
            'mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun',
            'seg', 'ter', 'qua', 'qui', 'sex', 'sab', 'dom'
        }

        # Se é nome de dia da semana, é STRING
        if value_lower in weekday_names:
            return 'string'

        # Se é só HH:MM (sem segundos), é STRING
        if re.match(r'^\d{1,2}:\d{2}$', value):
            return 'string'

        # Detecção de data filtrada
        try:
            dt = date_parser.parse(value, fuzzy=False)
            # Validar se é uma data realista
            if dt.year < 1900 or dt.year > 2100:
                return 'string'

            if dt.tzinfo or 'z' in value.lower():
                return 'timestamp'
            if dt.hour or dt.minute or dt.second:
                return 'datetime'
            return 'date'
        except Exception:
            return 'string'

        return 'string'

    try:
        # Análise da coluna
        total = len(column_data)
        non_null = [str(val).strip() for val in column_data if pd.notna(val)]

        # Se não tiver valores não-nulos, retorna tipo STRING
        if not non_null:
            return {
                'column': column_name,
                'suggested_type': 'STRING',
                'total_records': total,
                'non_null_records': 0,
                'null_records': total,
                'inconsistent_count': 0,
                'inconsistent_percent': 0,
                'confidence_score': 0,
                'inconsistent_values': []
            }

        # Contar padrões e identificar o dominante
        patterns = Counter(detect_pattern(val, boolean_values) for val in non_null)
        dominant, count = patterns.most_common(1)[0]

        # Calcular inconsistências
        inconsistent = len(non_null) - count
        percent_inconsistent = (inconsistent / len(non_null)) * 100
        confidence = (count / len(non_null)) * 100

        # Coletar amostras de valores inconsistentes
        inconsistent_samples = [v for v in non_null if detect_pattern(v, boolean_values) != dominant][:20]

        # Retornar análise completa
        return {
            'column': column_name,
            'suggested_type': type_mapping[dominant],
            'total_records': total,
            'non_null_records': len(non_null),
            'null_records': total - len(non_null),
            'inconsistent_count': inconsistent,
            'inconsistent_percent': round(percent_inconsistent, 4),
            'confidence_score': round(confidence, 2),
            'inconsistent_values': inconsistent_samples
        }
    except Exception as e:
        # Em caso de erro, retornar um tipo seguro (STRING)
        return {
            'column': column_name,
            'suggested_type': 'STRING',
            'total_records': len(column_data),
            'non_null_records': 0,
            'null_records': len(column_data),
            'inconsistent_count': 0,
            'inconsistent_percent': 0,
            'confidence_score': 0,
            'inconsistent_values': [],
            'error': str(e)
        }
