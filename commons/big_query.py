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


class DataTypeDetector:
    """Handles data type detection and pattern matching logic."""

    BOOLEAN_VALUES = {'true', 'false'}
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

    WEEKDAY_NAMES = {
        'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday',
        'segunda', 'terça', 'quarta', 'quinta', 'sexta', 'sabado', 'domingo',
        'mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun',
        'seg', 'ter', 'qua', 'qui', 'sex', 'sab', 'dom'
    }

    @classmethod
    def detect_pattern(cls, value):
        """Detecta o padrão do valor e retorna o tipo correspondente."""
        if not isinstance(value, str):
            return 'string'

        value_stripped = value.strip()
        value_lower = value_stripped.lower()

        # Timezone patterns
        if re.match(r'^[A-Za-z]+/[A-Za-z_]+$', value_stripped):  # America/Sao_Paulo
            return 'string'
        if re.match(r'^[+-]\d{2}:\d{2}$', value_stripped):  # -03:00, +02:00
            return 'string'

        # Numeric types
        if cls._is_integer(value):
            return 'integer'
        if cls._is_numeric(value):
            return 'numeric'
        if cls._is_float(value):
            return 'float'

        # Boolean
        if value_lower in cls.BOOLEAN_VALUES:
            return 'boolean'

        # Time (with seconds required)
        if cls._is_time(value):
            return 'time'

        # TIMESTAMP patterns - verificar ANTES da detecção genérica de data
        # ISO 8601 com T (YYYY-MM-DDTHH:MM:SS) -> sempre TIMESTAMP
        if re.match(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$', value_stripped):
            return 'timestamp'

        # Date/DateTime patterns
        date_type = cls._detect_date(value)
        if date_type:
            return date_type

        return 'string'

    @classmethod
    def _is_integer(cls, v):
        """Verifica se o valor é um inteiro válido."""
        return bool(re.fullmatch(r"-?\d+", v)) and -9223372036854775808 <= int(v) <= 9223372036854775807

    @classmethod
    def _is_numeric(cls, v):
        """Verifica se o valor é um numérico de precisão alta."""
        if not re.fullmatch(r"-?\d+\.\d+", v):
            return False
        d = Decimal(v)
        return len(str(d).split('.')[-1]) > 6 and len(str(d).replace('.', '').replace('-', '')) <= 38

    @classmethod
    def _is_float(cls, v):
        """Verifica se o valor é um float válido."""
        return bool(re.fullmatch(r"-?\d+(\.\d+)?([eE][+-]?\d+)?", v))

    @classmethod
    def _is_time(cls, v):
        """Verifica se o valor é um horário válido - VERSÃO CORRIGIDA."""
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
        elif re.match(pattern_without_seconds, v):
            return False  # Força STRING em vez de TIME

        return False

    @classmethod
    def _detect_date(cls, v):
        """Detecta e classifica o tipo de data no valor - VERSÃO CORRIGIDA."""
        # Se é só nome de dia da semana, retornar None (será STRING)
        if v.lower().strip() in cls.WEEKDAY_NAMES:
            return None

        # Filtrar outros valores problemáticos
        invalid_date_patterns = [
            r'^\d{1,2}:\d{2}$',  # HH:MM (horário sem segundos)
            r'^[a-zA-Z]+$',  # Só letras (provavelmente não é data)
        ]

        for pattern in invalid_date_patterns:
            if re.match(pattern, v.strip()):
                return None

        # PADRÕES DE TIMESTAMP - VERIFICAR PRIMEIRO (mais específico)
        # ISO 8601 com T (YYYY-MM-DDTHH:MM:SS) -> sempre TIMESTAMP
        if re.match(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$', v.strip()):
            return 'timestamp'

        # US timestamp format (MM/DD/YYYY HH:MM:SS) -> sempre TIMESTAMP
        if re.match(r'^\d{1,2}/\d{1,2}/\d{4} \d{1,2}:\d{2}:\d{2}$', v.strip()):
            return 'timestamp'

        # YYYY-MM-DD HH:MM:SS -> sempre TIMESTAMP
        if re.match(r'^\d{4}-\d{2}-\d{2} \d{1,2}:\d{2}(:\d{2})?$', v.strip()):
                return 'timestamp'

        try:
            dt = date_parser.parse(v, fuzzy=False)

            # Verificar se parsing resultou em algo válido
            if dt.year < 1900 or dt.year > 2100:
                return None

            if dt.tzinfo or 'z' in v.lower():
                return 'timestamp'
            if dt.hour or dt.minute or dt.second:
                return 'timestamp'
            return 'date'
        except Exception:
            return None


class DataPreprocessor:
    """Handles data preprocessing and cleaning operations."""

    @staticmethod
    def convert_date_br_to_iso(csv_path, schema_json, logger):
        """
        Preprocessa dados do CSV para compatibilidade com BigQuery.
        Converte APENAS datas no formato D/MM/AAAA ou DD/MM/AAAA para AAAA-MM-DD.
        NUNCA converte se houver componente de hora.
        """
        try:
            logger.info("🔄 Iniciando preprocessamento de dados...")

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
    def convert_timestamp_br_to_iso(csv_path, schema_json=None, logger=None, delimiter=';'):
        """
        Converte timestamps no formato DD/MM/YYYY HH:MM:SS para YYYY-MM-DD HH:MM:SS
        para compatibilidade com BigQuery (formato brasileiro).
        """

        if logger is None:
            logger = logging.getLogger(__name__)

        try:
            logger.info("🔄 Iniciando conversão de timestamps BR para ISO...")

            # Carregar CSV
            df = pd.read_csv(csv_path, delimiter=delimiter, dtype=str, low_memory=False)
            logger.info(f"📄 Arquivo carregado: {len(df)} linhas, {len(df.columns)} colunas")

            # Identificar campos de timestamp do schema (se fornecido)
            timestamp_fields = []
            if schema_json:
                timestamp_fields = [
                    field['name'] for field in schema_json
                    if field['type'] in ['TIMESTAMP', 'DATETIME']
                ]
                logger.info(f"🕐 Campos TIMESTAMP identificados no schema: {timestamp_fields}")

            # Se não tiver schema, detectar automaticamente colunas com timestamp
            if not timestamp_fields:
                timestamp_fields = []
                for col in df.columns:
                    # Verificar se a coluna tem padrão de timestamp BR
                    sample_values = df[col].dropna().head(10)
                    br_timestamp_count = 0

                    for val in sample_values:
                        if isinstance(val, str) and re.match(r'^\d{1,2}/\d{1,2}/\d{4} \d{1,2}:\d{2}:\d{2}$',
                                                             val.strip()):
                            br_timestamp_count += 1

                    # Se mais de 50% das amostras são timestamps BR, incluir
                    if br_timestamp_count >= len(sample_values) * 0.5:
                        timestamp_fields.append(col)

                logger.info(f"🔍 Campos TIMESTAMP detectados automaticamente: {timestamp_fields}")

            if not timestamp_fields:
                logger.info("ℹ️  Nenhum campo de timestamp encontrado")
                return 0

            total_conversions = 0

            # Processar cada campo de timestamp
            for field in timestamp_fields:
                if field not in df.columns:
                    logger.warning(f"⚠️  Campo '{field}' não encontrado no CSV")
                    continue

                field_conversions = 0
                logger.info(f"🔄 Processando campo: {field}")

                for idx, timestamp_str in enumerate(df[field]):
                    # Pular valores vazios/nulos
                    if not timestamp_str or pd.isna(timestamp_str):
                        continue

                    timestamp_str = str(timestamp_str).strip()

                    # Verificar se está no formato DD/MM/YYYY HH:MM:SS
                    match = re.match(r'^(\d{1,2})/(\d{1,2})/(\d{4}) (\d{1,2}):(\d{2}):(\d{2})$', timestamp_str)

                    if match:
                        day, month, year, hour, minute, second = match.groups()

                        # Validar valores
                        try:
                            day_int = int(day)
                            month_int = int(month)
                            year_int = int(year)
                            hour_int = int(hour)
                            minute_int = int(minute)
                            second_int = int(second)

                            # Verificar se os valores são válidos
                            if not (
                                    1 <= day_int <= 31 and 1 <= month_int <= 12 and 1900 <= year_int <= 2100 and 0 <= hour_int <= 23 and 0 <= minute_int <= 59 and 0 <= second_int <= 59):  # noqa
                                continue

                            # Converter para formato ISO: YYYY-MM-DD HH:MM:SS
                            iso_timestamp = f"{year}-{month.zfill(2)}-{day.zfill(2)} {hour.zfill(2)}:{minute}:{second}"
                            df.at[idx, field] = iso_timestamp
                            field_conversions += 1

                        except ValueError:
                            # Se algum valor não puder ser convertido para int, pular
                            continue

                if field_conversions > 0:
                    total_conversions += field_conversions
                    logger.info(f"   ✅ {field}: {field_conversions} conversões realizadas")
                else:
                    logger.info(f"   ℹ️  {field}: nenhuma conversão necessária")

            # Salvar arquivo se houve conversões
            if total_conversions > 0:
                # Fazer backup do arquivo original
                backup_path = f"{csv_path}.backup_br"
                if not Path(backup_path).exists():
                    df_original = pd.read_csv(csv_path, delimiter=delimiter, dtype=str, low_memory=False)
                    df_original.to_csv(backup_path, sep=delimiter, index=False, encoding='utf-8')
                    logger.info(f"💾 Backup criado: {backup_path}")

                # Salvar arquivo convertido
                df.to_csv(csv_path, sep=delimiter, index=False, encoding='utf-8')
                logger.info(f"✅ {total_conversions} timestamp(s) BR convertido(s) com sucesso!")
                logger.info(f"📁 Arquivo atualizado: {csv_path}")
            else:
                logger.info("ℹ️  Nenhuma conversão de timestamp BR necessária")

            return total_conversions

        except Exception as e:
            logger.error(f"❌ Erro na conversão de timestamps BR: {e}")
            raise

    @staticmethod
    def convert_timestamp_us_to_iso(csv_path, schema_json=None, logger=None, delimiter=';'):
        """
        Converte timestamps no formato MM/DD/YYYY HH:MM:SS para YYYY-MM-DD HH:MM:SS
        para compatibilidade com BigQuery.
        """
        if logger is None:
            logger = logging.getLogger(__name__)

        try:
            logger.info("🔄 Iniciando conversão de timestamps US para ISO...")

            # Carregar CSV
            df = pd.read_csv(csv_path, delimiter=delimiter, dtype=str, low_memory=False)
            logger.info(f"📄 Arquivo carregado: {len(df)} linhas, {len(df.columns)} colunas")

            # Identificar campos de timestamp do schema (se fornecido)
            timestamp_fields = []
            if schema_json:
                timestamp_fields = [
                    field['name'] for field in schema_json
                    if field['type'] in ['TIMESTAMP', 'DATETIME']
                ]
                logger.info(f"🕐 Campos TIMESTAMP identificados no schema: {timestamp_fields}")

            # Se não tiver schema, detectar automaticamente colunas com timestamp
            if not timestamp_fields:
                timestamp_fields = []
                for col in df.columns:
                    # Verificar se a coluna tem padrão de timestamp US
                    sample_values = df[col].dropna().head(10)
                    us_timestamp_count = 0

                    for val in sample_values:
                        if isinstance(val, str) and re.match(r'^\d{1,2}/\d{1,2}/\d{4} \d{1,2}:\d{2}:\d{2}$',
                                                             val.strip()):
                            us_timestamp_count += 1

                    # Se mais de 50% das amostras são timestamps US, incluir
                    if us_timestamp_count >= len(sample_values) * 0.5:
                        timestamp_fields.append(col)

                logger.info(f"🔍 Campos TIMESTAMP detectados automaticamente: {timestamp_fields}")

            if not timestamp_fields:
                logger.info("ℹ️  Nenhum campo de timestamp encontrado")
                return 0

            total_conversions = 0

            # Processar cada campo de timestamp
            for field in timestamp_fields:
                if field not in df.columns:
                    logger.warning(f"⚠️  Campo '{field}' não encontrado no CSV")
                    continue

                field_conversions = 0
                logger.info(f"🔄 Processando campo: {field}")

                for idx, timestamp_str in enumerate(df[field]):
                    # Pular valores vazios/nulos
                    if not timestamp_str or pd.isna(timestamp_str):
                        continue

                    timestamp_str = str(timestamp_str).strip()

                    # Verificar se está no formato MM/DD/YYYY HH:MM:SS
                    match = re.match(r'^(\d{1,2})/(\d{1,2})/(\d{4}) (\d{1,2}):(\d{2}):(\d{2})$', timestamp_str)

                    if match:
                        month, day, year, hour, minute, second = match.groups()

                        # Validar valores
                        try:
                            month_int = int(month)
                            day_int = int(day)
                            year_int = int(year)
                            hour_int = int(hour)
                            minute_int = int(minute)
                            second_int = int(second)

                            # Verificar se os valores são válidos
                            if not (1 <= month_int <= 12 and 1 <= day_int <= 31 and 1900 <= year_int <= 2100 and 0 <= hour_int <= 23 and 0 <= minute_int <= 59 and 0 <= second_int <= 59):  # noqa
                                continue

                            # Converter para formato ISO: YYYY-MM-DD HH:MM:SS
                            iso_timestamp = f"{year}-{month.zfill(2)}-{day.zfill(2)} {hour.zfill(2)}:{minute}:{second}"
                            df.at[idx, field] = iso_timestamp
                            field_conversions += 1

                        except ValueError:
                            # Se algum valor não puder ser convertido para int, pular
                            continue

                if field_conversions > 0:
                    total_conversions += field_conversions
                    logger.info(f"   ✅ {field}: {field_conversions} conversões realizadas")
                else:
                    logger.info(f"   ℹ️  {field}: nenhuma conversão necessária")

            # Salvar arquivo se houve conversões
            if total_conversions > 0:
                # Fazer backup do arquivo original
                backup_path = f"{csv_path}.backup"
                if not Path(backup_path).exists():
                    df_original = pd.read_csv(csv_path, delimiter=delimiter, dtype=str, low_memory=False)
                    df_original.to_csv(backup_path, sep=delimiter, index=False, encoding='utf-8')
                    logger.info(f"💾 Backup criado: {backup_path}")

                # Salvar arquivo convertido
                df.to_csv(csv_path, sep=delimiter, index=False, encoding='utf-8')
                logger.info(f"✅ {total_conversions} timestamp(s) convertido(s) com sucesso!")
                logger.info(f"📁 Arquivo atualizado: {csv_path}")
            else:
                logger.info("ℹ️  Nenhuma conversão de timestamp necessária")

            return total_conversions

        except Exception as e:
            logger.error(f"❌ Erro na conversão de timestamps: {e}")
            raise

    @staticmethod
    def remove_timezone_offsets(csv_path, schema_json=None, logger=None, delimiter=';'):
        """
        Remove timezone offsets de timestamps para compatibilidade com BigQuery.
        Converte '2023-07-13 14:09:47 -0300' para '2023-07-13 14:09:47'
        E converte campos com apenas timezone offsets (ex: -03:00:00) para STRING no schema.
        """
        if logger is None:
            logger = logging.getLogger(__name__)

        try:
            logger.info("🕐 Iniciando remoção de timezone offsets...")

            # Carregar CSV
            df = pd.read_csv(csv_path, delimiter=delimiter, dtype=str, low_memory=False)
            logger.info(f"📄 Arquivo carregado: {len(df)} linhas, {len(df.columns)} colunas")

            # Identificar campos de timestamp do schema (se fornecido)
            timestamp_fields = []
            if schema_json:
                timestamp_fields = [
                    field['name'] for field in schema_json
                    if field['type'] in ['TIMESTAMP', 'DATETIME']
                ]
                logger.info(f"🕐 Campos TIMESTAMP identificados no schema: {timestamp_fields}")

            # Se não tiver schema, detectar automaticamente colunas com timezone
            if not timestamp_fields:
                timestamp_fields = []
                for col in df.columns:
                    # Verificar se a coluna tem timestamps com timezone
                    sample_values = df[col].dropna().head(10)
                    timezone_count = 0

                    for val in sample_values:
                        if isinstance(val, str):
                            # Verificar padrões com timezone offset
                            if re.search(r'\s[+-]\d{2,4}$', val.strip()):  # Termina com +0300 ou -03:00
                                timezone_count += 1
                            # NOVO: Verificar timezone offsets standalone
                            elif re.match(r'^[+-]\d{1,2}:?\d{0,2}:?\d{0,2}$', val.strip()):  # -03:00:00, -03:00, -03
                                timezone_count += 1

                    # Se mais de 50% das amostras têm timezone, incluir
                    if timezone_count >= len(sample_values) * 0.5:
                        timestamp_fields.append(col)

                logger.info(f"🔍 Campos com timezone detectados automaticamente: {timestamp_fields}")

            if not timestamp_fields:
                logger.info("ℹ️  Nenhum campo com timezone encontrado")
                return 0

            total_conversions = 0

            # Processar cada campo de timestamp
            for field in timestamp_fields:
                if field not in df.columns:
                    logger.warning(f"⚠️  Campo '{field}' não encontrado no CSV")
                    continue

                field_conversions = 0
                logger.info(f"🔄 Processando campo: {field}")

                # NOVO: Verificar se campo contém apenas timezone offsets standalone
                sample_values = df[field].dropna().astype(str).str.strip().head(10)
                has_standalone_timezone = any(
                    re.match(r'^[+-]\d{1,2}:?\d{0,2}:?\d{0,2}$', val) for val in sample_values
                )
                has_valid_timestamp = any(
                    re.match(r'^\d{4}-\d{2}-\d{2}[ T]\d{1,2}:\d{2}:\d{2}', val) for val in sample_values
                )

                # Se tem timezone standalone e não tem timestamps válidos, converter schema para STRING
                if has_standalone_timezone and not has_valid_timestamp and schema_json:
                    for schema_field in schema_json:
                        if schema_field['name'] == field:
                            old_type = schema_field['type']
                            schema_field['type'] = 'STRING'
                            logger.info(f"   🔧 {field}: {old_type} → STRING (timezone standalone detectado)")
                            total_conversions += 1
                            break
                    continue  # Pular processamento de dados para este campo

                for idx, timestamp_str in enumerate(df[field]):
                    # Pular valores vazios/nulos
                    if not timestamp_str or pd.isna(timestamp_str):
                        continue

                    timestamp_str = str(timestamp_str).strip()

                    # Padrões de timezone para remover
                    timezone_patterns = [
                        r'\s[+-]\d{4}$',  # -0300, +0200
                        r'\s[+-]\d{2}:\d{2}$',  # -03:00, +02:00
                        r'\s[+-]\d{2}$',  # -03, +02
                        r'\sZ$',  # Z (UTC)
                        r'\sUTC$',  # UTC
                        r'\sGMT$',  # GMT
                    ]

                    original_value = timestamp_str
                    converted = False

                    # Tentar remover cada padrão de timezone
                    for pattern in timezone_patterns:
                        if re.search(pattern, timestamp_str):
                            # Remover o timezone offset
                            clean_timestamp = re.sub(pattern, '', timestamp_str)

                            # Validar se o resultado é um timestamp válido
                            if re.match(r'^\d{4}-\d{2}-\d{2} \d{1,2}:\d{2}:\d{2}$', clean_timestamp):
                                df.at[idx, field] = clean_timestamp
                                field_conversions += 1
                                converted = True
                                break

                    # Se não foi convertido, manter valor original
                    if not converted:
                        df.at[idx, field] = original_value

                if field_conversions > 0:
                    total_conversions += field_conversions
                    logger.info(f"   ✅ {field}: {field_conversions} timezone offsets removidos")
                else:
                    logger.info(f"   ℹ️  {field}: nenhum timezone offset encontrado")

            if schema_json:
                schema_path = Path(csv_path).parent / "schema.json"
                with open(schema_path, 'w', encoding='utf-8') as f:
                    json.dump(schema_json, f, indent=2, ensure_ascii=False)

            # Salvar arquivo se houve conversões
            if total_conversions > 0:
                # Salvar arquivo convertido (sem backup)
                df.to_csv(csv_path, sep=delimiter, index=False, encoding='utf-8')
                logger.info(f"✅ {total_conversions} timezone offset(s) removido(s) com sucesso!")
                logger.info(f"📁 Arquivo atualizado: {csv_path}")
            else:
                logger.info("ℹ️  Nenhum timezone offset precisou ser removido")

            return total_conversions

        except Exception as e:
            logger.error(f"❌ Erro na remoção de timezone offsets: {e}")
            raise

    @staticmethod
    def fix_coordinates_schema(csv_path, schema_json=None, logger=None):
        """
        Corrige campos de coordenadas (latitude/longitude) que foram incorretamente
        detectados como DATE para FLOAT64.
        """
        if logger is None:
            logger = logging.getLogger(__name__)

        try:
            logger.info("🗺️  Verificando campos de coordenadas com tipo incorreto...")

            if not schema_json:
                logger.info("ℹ️  Nenhum schema fornecido")
                return 0

            # Carregar CSV para verificar conteúdo
            df = pd.read_csv(csv_path, delimiter=';', dtype=str, low_memory=False)
            corrections = 0

            # Identificar campos que podem ser coordenadas
            coordinate_field_names = [
                'latitude', 'longitude', 'lat', 'lng', 'lon',
                'coord_lat', 'coord_lng', 'geo_lat', 'geo_lng'
            ]

            # Verificar cada campo no schema
            for field in schema_json:
                field_name = field['name'].lower()

                # Se o campo tem nome de coordenada E está como DATE
                if any(coord in field_name for coord in coordinate_field_names) and field['type'] == 'DATE':

                    # Verificar se realmente contém coordenadas no CSV
                    if field['name'] in df.columns:
                        sample_values = df[field['name']].dropna().astype(str).str.strip().head(100)

                        # Contar valores que parecem coordenadas (números decimais negativos/positivos)
                        coordinate_pattern_count = 0
                        for val in sample_values:
                            # Padrão de coordenada: -XX.XXXXXX ou XX.XXXXXX
                            if re.match(r'^-?\d+[,.]?\d*$', val.replace(',', '.')):
                                try:
                                    # Tentar converter para float
                                    coord_val = float(val.replace(',', '.'))
                                    # Verificar se está em range válido de coordenadas
                                    if -180 <= coord_val <= 180:
                                        coordinate_pattern_count += 1
                                except ValueError:
                                    continue

                        # Se mais de 80% dos valores são coordenadas válidas
                        if coordinate_pattern_count >= len(sample_values) * 0.8:
                            field['type'] = 'FLOAT64'
                            corrections += 1
                            logger.info(f"   ✅ {field['name']}: DATE → FLOAT64 (coordenada detectada)")

            # Salvar schema atualizado se houve correções
            if corrections > 0:
                schema_path = Path(csv_path).parent / "schema.json"
                with open(schema_path, 'w', encoding='utf-8') as f:
                    json.dump(schema_json, f, indent=2, ensure_ascii=False)
                logger.info(f"✅ {corrections} campo(s) de coordenadas corrigido(s)")
            else:
                logger.info("ℹ️  Nenhum campo de coordenadas precisou ser corrigido")

            return corrections

        except Exception as e:
            logger.error(f"❌ Erro na correção de coordenadas: {e}")
            raise

    @staticmethod
    def clean_null_values(csv_path, logger):
        """Limpa valores 'null' string substituindo por NULL real."""
        try:
            logger.info("🧹 Limpando valores 'null' no CSV...")

            df = pd.read_csv(csv_path, delimiter=';', dtype=str, low_memory=False)

            # Substituir strings "null" por valores vazios
            df = df.replace('null', '')
            df = df.replace('NULL', '')
            df = df.replace('Null', '')

            # Salvar CSV limpo
            df.to_csv(csv_path, sep=';', index=False, encoding='utf-8')
            logger.info("✅ Valores 'null' limpos com sucesso")

        except Exception as e:
            logger.error(f"❌ Erro na limpeza de nulls: {e}")
            raise

    @staticmethod
    def fix_invalid_date_values(csv_path, schema_json=None, logger=None):
        """
        Converte campos DATE para STRING APENAS se contêm valores como "3/1", "11/1".
        """
        if logger is None:
            logger = logging.getLogger(__name__)

        try:
            logger.info("🔧 Verificando campos DATE com valores inválidos...")

            if not schema_json:
                return 0

            # Identificar campos DATE no schema
            date_fields = [field['name'] for field in schema_json if field['type'] == 'DATE']

            if not date_fields:
                logger.info("ℹ️  Nenhum campo DATE encontrado")
                return 0

            # Carregar CSV para verificar conteúdo
            df = pd.read_csv(csv_path, delimiter=';', dtype=str, low_memory=False)
            corrections = 0

            # Verificar cada campo DATE
            for field_name in date_fields:
                if field_name not in df.columns:
                    continue

                # Verificar se há valores como "3/1", "11/1" (sem ano)
                sample_values = df[field_name].dropna().astype(str).str.strip().head(100)
                invalid_count = sum(1 for val in sample_values if re.match(r'^\d{1,2}/\d{1,2}$', val))

                # Se há valores inválidos, converter para STRING
                if invalid_count > 0:
                    for field in schema_json:
                        if field['name'] == field_name:
                            field['type'] = 'STRING'
                            corrections += 1
                            logger.info(f"   ✅ {field_name}: DATE → STRING ({invalid_count} valores inválidos)")
                            break

            # Salvar schema atualizado se houve correções
            if corrections > 0:
                schema_path = Path(csv_path).parent / "schema.json"
                with open(schema_path, 'w', encoding='utf-8') as f:
                    json.dump(schema_json, f, indent=2, ensure_ascii=False)
                logger.info(f"✅ {corrections} campo(s) corrigido(s)")
            else:
                logger.info("ℹ️  Nenhum campo DATE precisou ser corrigido")

            return corrections

        except Exception as e:
            logger.error(f"❌ Erro na correção de datas: {e}")
            raise

    @staticmethod
    def normalize_decimal_precision(csv_path, schema_json=None, logger=None, delimiter=';', max_decimals=6):
        """
        Normaliza valores decimais para no máximo 6 casas decimais.
        Isso evita problemas de precisão excessiva que causam conversão para STRING.
        """
        if logger is None:
            logger = logging.getLogger(__name__)

        try:
            logger.info(f"🔢 Normalizando decimais para no máximo {max_decimals} casas...")

            # Carregar CSV
            df = pd.read_csv(csv_path, delimiter=delimiter, dtype=str, low_memory=False)
            logger.info(f"📄 Arquivo carregado: {len(df)} linhas, {len(df.columns)} colunas")

            total_normalizations = 0
            columns_affected = []

            # Processar cada coluna
            for col_name in df.columns:
                col_normalizations = 0

                for idx, value in enumerate(df[col_name]):
                    if pd.isna(value):
                        continue

                    original_value = str(value).strip()

                    # Pular valores que não são numéricos com decimais
                    if '.' not in original_value:
                        continue

                    # Tentar normalizar apenas valores que parecem decimais
                    try:
                        # Verificar se é um número válido
                        float_val = float(original_value)

                        # Contar casas decimais atuais
                        decimal_part = original_value.split('.')[1] if '.' in original_value else ''

                        # Se tem mais casas decimais que o limite, normalizar
                        if len(decimal_part) > max_decimals:
                            # Formatar com no máximo max_decimals casas decimais
                            normalized_value = f"{float_val:.{max_decimals}f}"
                            # Remover zeros desnecessários à direita
                            normalized_value = normalized_value.rstrip('0').rstrip('.')

                            # Se o valor mudou, aplicar
                            if original_value != normalized_value:
                                df.at[idx, col_name] = normalized_value
                                col_normalizations += 1

                    except (ValueError, OverflowError, IndexError):
                        # Se não conseguir processar, manter valor original
                        continue

                if col_normalizations > 0:
                    total_normalizations += col_normalizations
                    columns_affected.append(col_name)
                    logger.info(f"   ✅ {col_name}: {col_normalizations} valores normalizados")

            # Salvar se houve normalizações
            if total_normalizations > 0:
                # Fazer backup do arquivo original
                backup_path = f"{csv_path}.backup_decimals"
                if not Path(backup_path).exists():
                    # Carregar arquivo original para backup
                    df_original = pd.read_csv(csv_path, delimiter=delimiter, dtype=str, low_memory=False)
                    df_original.to_csv(backup_path, sep=delimiter, index=False, encoding='utf-8')
                    logger.info(f"💾 Backup criado: {backup_path}")

                # Salvar arquivo normalizado
                df.to_csv(csv_path, sep=delimiter, index=False, encoding='utf-8')
                logger.info(f"✅ {total_normalizations} valores decimais normalizados com sucesso!")
                logger.info(
                    f"📊 Colunas afetadas: {len(columns_affected)} ({', '.join(columns_affected[:5])}{'...' if len(columns_affected) > 5 else ''})")
                logger.info(f"📁 Arquivo atualizado: {csv_path}")
            else:
                logger.info("ℹ️  Nenhuma normalização decimal necessária")

            return total_normalizations

        except Exception as e:
            logger.error(f"❌ Erro na normalização de decimais: {e}")
            raise


class ReportGenerator:
    """Handles generation of inconsistency reports."""

    @staticmethod
    def generate_inconsistency_report(report, logger, threshold=0.01):
        """Gera um relatório detalhado de inconsistências."""
        logger.info("=" * 100)
        logger.info("RELATÓRIO DE INCONSISTÊNCIAS E TIPOS APLICADOS")
        logger.info("=" * 100)

        try:
            # Estatísticas de tipos
            type_stats = Counter(col['final_type'] for col in report)
            optimized_count = len([col for col in report if col['type_reason'] == 'OTIMIZADO'])
            safe_count = len([col for col in report if col['type_reason'] == 'SEGURO'])

            logger.info("ESTATÍSTICAS DE TIPOS APLICADOS:")
            for type_name, count in type_stats.most_common():
                logger.info(f"  {type_name}: {count} colunas")
            logger.info(f"  Total otimizado: {optimized_count} colunas")
            logger.info(f"  Total seguro (STRING): {safe_count} colunas")
            logger.info("-" * 100)

            # Verificação de inconsistências
            columns_with_issues = [col for col in report if col['inconsistent_percent'] >= threshold]

            if not columns_with_issues:
                logger.info("Nenhuma inconsistência significativa encontrada!")
                return

            # Resumo executivo
            total_inconsistent = sum(col['inconsistent_count'] for col in columns_with_issues)
            logger.info("RESUMO EXECUTIVO:")
            logger.info(f"  Total de registros inconsistentes: {total_inconsistent:,}")
            logger.info(f"  Colunas afetadas: {len(columns_with_issues)}")
            logger.info(f"  Tipos mais afetados: {ReportGenerator._most_affected_types(columns_with_issues, logger)}")
            logger.info("-" * 100)

            # Detalhes por coluna
            for col in columns_with_issues:
                logger.info(f"COLUNA: {col['column']}")
                logger.info(
                    f"  Tipo sugerido: {col['suggested_type']} → Aplicado: {col['final_type']} ({col['type_reason']})")
                logger.info(f"  Registros: {col['non_null_records']:,} válidos, {col['null_records']:,} nulos")
                logger.info(
                    f"  Inconsistências: {col['inconsistent_count']:,} ({col['inconsistent_percent']:.4f}%)")
                logger.info(f"  Confiança: {col['confidence_score']:.2f}%")
                if col['inconsistent_values']:
                    logger.info(f"  ⚠️  EXEMPLOS INCONSISTENTES: {col['inconsistent_values'][:5]}")
                logger.info(f"  Ação sugerida: Verificar e corrigir {col['inconsistent_count']:,} registros\n")

        except Exception as e:
            logger.error(f"Erro ao gerar relatório de inconsistências: {e}")

    @staticmethod
    def _most_affected_types(columns, logger):
        """Identifica os tipos mais afetados por inconsistências."""
        try:
            type_counts = Counter(col['suggested_type'] for col in columns)
            return ', '.join(f"{t} ({c})" for t, c in type_counts.most_common(3))
        except Exception as e:
            logger.error(f"Erro ao calcular tipos mais afetados: {e}")
            return "Erro ao calcular"


class BigQueryTableManager:
    """Handles BigQuery table operations."""

    @staticmethod
    def _get_credentials_and_client(project_id, credentials_path, logger):
        """Helper method to get credentials and BigQuery client."""
        import warnings
        warnings.filterwarnings("ignore", message="Your application has authenticated using end user credentials")

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

        return client

    @staticmethod
    def create_external_table(project_id, tool_name, table_name, credentials_path):
        """Carrega dados do CSV diretamente para uma tabela BigQuery."""
        start_time = time.time()
        logger = logging.getLogger(__name__)
        logger.info(f"Iniciando criação da tabela externa: {project_id}.{tool_name}.{table_name}")

        try:
            client = BigQueryTableManager._get_credentials_and_client(project_id, credentials_path, logger)

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
            DataPreprocessor.normalize_decimal_precision(csv_path, schema_json, logger)
            DataPreprocessor.fix_invalid_date_values(csv_path, schema_json, logger)
            DataPreprocessor.convert_date_br_to_iso(csv_path, schema_json, logger)
            DataPreprocessor.convert_timestamp_br_to_iso(csv_path, schema_json, logger)
            DataPreprocessor.convert_timestamp_us_to_iso(csv_path, schema_json, logger)
            DataPreprocessor.remove_timezone_offsets(csv_path, schema_json, logger)
            DataPreprocessor.fix_coordinates_schema(csv_path, schema_json, logger)
            DataPreprocessor.clean_null_values(csv_path, logger)

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
    def create_gold_table(project_id, tool_name, table_name, credentials_path):
        """Cria uma tabela gold no dataset 'vendas'."""
        start_time = time.time()
        logger = logging.getLogger(__name__)
        logger.info(f"Iniciando criação da tabela gold para: {tool_name}_{table_name}_gold")

        try:
            client = BigQueryTableManager._get_credentials_and_client(project_id, credentials_path, logger)

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


class BigQuery:
    """Main class for BigQuery schema generation and processing."""

    DEFAULT_OUTPUT_DIR = './output'
    LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
    CSV_DELIMITER = ';'

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
        generator.logger.info(f"INICIANDO PROCESSAMENTO DO CSV - COM {generator.max_workers} WORKERS")

        generator._process_all_csv_files()

        total_time = time.time() - start_time
        generator.logger.info(
            f"✅ Processamento completo finalizado em {total_time / 60:.2f} minutos ({total_time:.2f} segundos)")

    def _find_csv_files(self):
        """Encontra todos os arquivos CSV no diretório de saída."""
        import glob
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
            DataPreprocessor.normalize_decimal_precision(csv_path, logger=self.logger)

            # Usar with para gerenciar recursos
            df = pd.read_csv(csv_path, delimiter=self.CSV_DELIMITER, dtype=str, low_memory=False)
            self._thread_safe_log('info',
                                  f"Arquivo {csv_path.name} carregado: {len(df)} linhas, {len(df.columns)} colunas")

            schema, report = self._generate_schema_parallel(df)
            ReportGenerator.generate_inconsistency_report(report, self.logger)

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
                executor.submit(
                    analyze_column_worker,
                    col_name,
                    col_data,
                    DataTypeDetector.BOOLEAN_VALUES,
                    DataTypeDetector.TYPE_MAPPING
                ): col_name
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
    def start_pipeline(project_id, tool_name, table_name, credentials_path):
        """Pipeline para criar as tabelas no BQ."""
        start_time = time.time()
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
            BigQueryTableManager.create_external_table(project_id, tool_name, table_name, credentials_path)
            logger.info("✅ Tabela externa criada com sucesso")

            logger.info("🏆 [2/2] Criando tabela gold...")
            BigQueryTableManager.create_gold_table(project_id, tool_name, table_name, credentials_path)
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


def analyze_column_worker(column_name, column_data, boolean_values, type_mapping):
    """Worker function para análise paralela de colunas - VERSÃO CORRIGIDA."""
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

        # Contar padrões e identificar o dominante usando a classe DataTypeDetector
        patterns = Counter(DataTypeDetector.detect_pattern(val) for val in non_null)
        dominant, count = patterns.most_common(1)[0]

        # Calcular inconsistências
        inconsistent = len(non_null) - count
        percent_inconsistent = (inconsistent / len(non_null)) * 100
        confidence = (count / len(non_null)) * 100

        # Coletar amostras de valores inconsistentes
        inconsistent_samples = [v for v in non_null if DataTypeDetector.detect_pattern(v) != dominant][:20]

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
