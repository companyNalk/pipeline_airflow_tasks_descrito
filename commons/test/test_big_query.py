import json
import os
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch, mock_open

import pandas as pd
import pytest

from commons.big_query import DataTypeDetector, DataPreprocessor, BigQuery, analyze_column_worker, BigQueryTableManager, \
    ReportGenerator


class TestDataTypeDetectorCoverage:
    """Testes para garantir cobertura completa da classe DataTypeDetector."""

    def test_constants_defined(self):
        """Testa se todas as constantes estão definidas."""
        # GIVEN & WHEN & THEN
        assert 'true' in DataTypeDetector.BOOLEAN_VALUES
        assert 'false' in DataTypeDetector.BOOLEAN_VALUES
        assert 'monday' in DataTypeDetector.WEEKDAY_NAMES
        assert 'seg' in DataTypeDetector.WEEKDAY_NAMES
        assert DataTypeDetector.TYPE_MAPPING['integer'] == 'INT64'
        assert DataTypeDetector.TYPE_MAPPING['string'] == 'STRING'

    def test_detect_pattern_timezone_strings(self):
        """Testa detecção de padrões de timezone."""
        # GIVEN & WHEN & THEN
        assert DataTypeDetector.detect_pattern("America/Sao_Paulo") == 'string'
        assert DataTypeDetector.detect_pattern("Europe/London") == 'string'
        assert DataTypeDetector.detect_pattern("-03:00") == 'string'
        assert DataTypeDetector.detect_pattern("+02:00") == 'string'

    def test_detect_pattern_numeric_types(self):
        """Testa detecção de tipos numéricos."""
        # GIVEN & WHEN & THEN
        assert DataTypeDetector.detect_pattern("123") == 'integer'
        assert DataTypeDetector.detect_pattern("-456") == 'integer'
        assert DataTypeDetector.detect_pattern("123.4567890") == 'numeric'
        assert DataTypeDetector.detect_pattern("1.5e10") == 'float'

    def test_detect_pattern_boolean_and_time(self):
        """Testa detecção de booleanos e horários."""
        # GIVEN & WHEN & THEN
        assert DataTypeDetector.detect_pattern("true") == 'boolean'
        assert DataTypeDetector.detect_pattern("FALSE") == 'boolean'
        assert DataTypeDetector.detect_pattern("14:30:45") == 'time'
        assert DataTypeDetector.detect_pattern("14:30") == 'string'  # Sem segundos

    def test_detect_pattern_dates_and_timestamps(self):
        """Testa detecção de datas e timestamps."""
        # GIVEN & WHEN & THEN
        assert DataTypeDetector.detect_pattern("2023-12-25") == 'date'
        assert DataTypeDetector.detect_pattern("12/25/2023 14:30:45") == 'timestamp'
        assert DataTypeDetector.detect_pattern("2023-12-25T14:30:45") == 'timestamp'
        assert DataTypeDetector.detect_pattern("monday") == 'string'

    def test_detect_pattern_edge_cases(self):
        """Testa casos extremos."""
        # GIVEN & WHEN & THEN
        assert DataTypeDetector.detect_pattern(123) == 'string'  # Não-string
        assert DataTypeDetector.detect_pattern("") == 'string'
        assert DataTypeDetector.detect_pattern("  ") == 'string'
        assert DataTypeDetector.detect_pattern("invalid_date") == 'string'

    def test_is_integer_bounds(self):
        """Testa limites de inteiros."""
        # GIVEN & WHEN & THEN
        assert DataTypeDetector._is_integer("9223372036854775807")
        assert DataTypeDetector._is_integer("-9223372036854775808")
        assert not DataTypeDetector._is_integer("9223372036854775808")
        assert not DataTypeDetector._is_integer("abc")

    def test_is_numeric_precision(self):
        """Testa detecção de numéricos de alta precisão."""
        # GIVEN & WHEN & THEN
        assert DataTypeDetector._is_numeric("123.1234567890")  # Mais de 6 dígitos decimais
        assert not DataTypeDetector._is_numeric("123.12")  # Menos de 6 dígitos decimais
        assert not DataTypeDetector._is_numeric("123")  # Sem ponto decimal

    def test_is_float_patterns(self):
        """Testa padrões de float."""
        # GIVEN & WHEN & THEN
        assert DataTypeDetector._is_float("123.45")
        assert DataTypeDetector._is_float("1.5e10")
        assert DataTypeDetector._is_float("-2.3E-5")
        assert not DataTypeDetector._is_float("abc")

    def test_is_time_validation(self):
        """Testa validação de horários."""
        # GIVEN & WHEN & THEN
        assert DataTypeDetector._is_time("23:59:59")
        assert DataTypeDetector._is_time("00:00:00.123")
        assert not DataTypeDetector._is_time("25:00:00")  # Hora inválida
        assert not DataTypeDetector._is_time("12:60:00")  # Minuto inválido
        assert not DataTypeDetector._is_time("12:30")  # Sem segundos

    def test_detect_date_filtering(self):
        """Testa filtragem de datas."""
        # GIVEN & WHEN & THEN
        assert DataTypeDetector._detect_date("segunda") is None  # Dia da semana
        assert DataTypeDetector._detect_date("14:30") is None  # Só horário
        assert DataTypeDetector._detect_date("abc") is None  # Só letras
        assert DataTypeDetector._detect_date("2023-12-25") == 'date'


class TestDataPreprocessorCoverage:
    """Testes para garantir cobertura completa da classe DataPreprocessor."""

    def test_convert_date_br_to_iso_no_fields(self):
        """Testa conversão quando não há campos de data."""
        # GIVEN
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("col1;col2\n1;2\n")
            csv_path = f.name

        schema_json = [{'name': 'col1', 'type': 'STRING'}]
        logger = Mock()

        # WHEN
        DataPreprocessor.convert_date_br_to_iso(csv_path, schema_json, logger)

        # THEN
        logger.info.assert_any_call("ℹ️  Nenhum campo de data/datetime encontrado")
        os.unlink(csv_path)

    def test_convert_timestamp_br_to_iso_basic(self):
        """Testa conversão básica de timestamp BR."""
        # GIVEN
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("timestamp_col;other_col\n25/12/2023 14:30:45;data\n")
            csv_path = f.name

        schema_json = [{'name': 'timestamp_col', 'type': 'TIMESTAMP'}]
        logger = Mock()

        # WHEN
        result = DataPreprocessor.convert_timestamp_br_to_iso(csv_path, schema_json, logger)

        # THEN
        assert result > 0
        df = pd.read_csv(csv_path, delimiter=';', dtype=str)
        assert df.iloc[0, 0] == "2023-12-25 14:30:45"
        os.unlink(csv_path)

    def test_convert_timestamp_us_to_iso_basic(self):
        """Testa conversão básica de timestamp US."""
        # GIVEN
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("timestamp_col;other_col\n12/25/2023 14:30:45;data\n")
            csv_path = f.name

        schema_json = [{'name': 'timestamp_col', 'type': 'TIMESTAMP'}]
        logger = Mock()

        # WHEN
        result = DataPreprocessor.convert_timestamp_us_to_iso(csv_path, schema_json, logger)

        # THEN
        assert result > 0
        df = pd.read_csv(csv_path, delimiter=';', dtype=str)
        assert df.iloc[0, 0] == "2023-12-25 14:30:45"
        os.unlink(csv_path)

    def test_convert_timestamp_auto_detection(self):
        """Testa detecção automática de campos de timestamp."""
        # GIVEN
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("timestamp_col;normal_col\n12/25/2023 14:30:45;abc\n")
            csv_path = f.name

        logger = Mock()

        # WHEN
        result = DataPreprocessor.convert_timestamp_us_to_iso(csv_path, None, logger)

        # THEN
        assert result > 0
        os.unlink(csv_path)

    def test_clean_null_values_all_variants(self):
        """Testa limpeza de todas as variantes de null."""
        # GIVEN
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("col1;col2\nnull;NULL\nNull;valid\n")
            csv_path = f.name

        logger = Mock()

        # WHEN
        DataPreprocessor.clean_null_values(csv_path, logger)

        # THEN
        df = pd.read_csv(csv_path, delimiter=';', dtype=str, na_filter=False)
        assert df.iloc[0, 0] == ''
        assert df.iloc[0, 1] == ''
        assert df.iloc[1, 1] == 'valid'
        os.unlink(csv_path)


class TestReportGeneratorCoverage:
    """Testes para garantir cobertura completa da classe ReportGenerator."""

    def test_generate_inconsistency_report_no_issues(self):
        """Testa relatório quando não há inconsistências."""
        # GIVEN
        report = [
            {'inconsistent_percent': 0, 'final_type': 'INT64', 'type_reason': 'OTIMIZADO'}
        ]
        logger = Mock()

        # WHEN
        ReportGenerator.generate_inconsistency_report(report, logger)

        # THEN
        logger.info.assert_any_call("Nenhuma inconsistência significativa encontrada!")

    def test_generate_inconsistency_report_with_issues(self):
        """Testa relatório com inconsistências."""
        # GIVEN
        report = [
            {
                'inconsistent_percent': 5.0, 'final_type': 'STRING', 'type_reason': 'SEGURO',
                'column': 'test_col', 'suggested_type': 'INT64', 'non_null_records': 100,
                'null_records': 0, 'inconsistent_count': 5, 'confidence_score': 95.0,
                'inconsistent_values': ['abc', 'def']
            }
        ]
        logger = Mock()

        # WHEN
        ReportGenerator.generate_inconsistency_report(report, logger, threshold=1.0)

        # THEN
        logger.info.assert_any_call("COLUNA: test_col")

    def test_most_affected_types_error_handling(self):
        """Testa tratamento de erro no cálculo de tipos afetados."""
        # GIVEN
        columns = [{'suggested_type': None}]
        logger = Mock()

        # WHEN
        result = ReportGenerator._most_affected_types(columns, logger)

        # THEN
        assert "None (1)" in result


class TestBigQueryTableManagerCoverage:
    """Testes para garantir cobertura completa da classe BigQueryTableManager."""

    @patch('commons.big_query.service_account')
    @patch('commons.big_query.bigquery')
    @patch('builtins.open', new_callable=mock_open, read_data='{"type": "service_account"}')
    def test_get_credentials_and_client_service_account(self, mock_file, mock_bq, mock_sa):
        """Testa criação de cliente com service account."""
        # GIVEN
        logger = Mock()
        mock_credentials = Mock()
        mock_sa.Credentials.from_service_account_file.return_value = mock_credentials

        # WHEN
        BigQueryTableManager._get_credentials_and_client("project", "path", logger)

        # THEN
        mock_sa.Credentials.from_service_account_file.assert_called_once()
        logger.info.assert_any_call("Usando credenciais de Service Account")

    @patch('commons.big_query.load_credentials_from_file')
    @patch('commons.big_query.bigquery')
    @patch('builtins.open', new_callable=mock_open, read_data='{"type": "authorized_user"}')
    def test_get_credentials_and_client_authorized_user(self, mock_file, mock_bq, mock_load_creds):
        """Testa criação de cliente com authorized user."""
        # GIVEN
        logger = Mock()
        mock_credentials = Mock()
        mock_credentials.with_quota_project.return_value = mock_credentials
        mock_load_creds.return_value = (mock_credentials, None)

        # WHEN
        BigQueryTableManager._get_credentials_and_client("project", "path", logger)

        # THEN
        mock_credentials.with_quota_project.assert_called_once_with("project")
        logger.info.assert_any_call("Usando credenciais de Authorized User (OAuth2) com quota project")

    @patch('commons.big_query.load_credentials_from_file')
    @patch('commons.big_query.bigquery')
    @patch('builtins.open', new_callable=mock_open, read_data='{"type": "unknown"}')
    def test_get_credentials_and_client_unknown_type(self, mock_file, mock_bq, mock_load_creds):
        """Testa criação de cliente com tipo desconhecido."""
        # GIVEN
        logger = Mock()
        mock_credentials = Mock()
        mock_load_creds.return_value = (mock_credentials, None)

        # WHEN
        BigQueryTableManager._get_credentials_and_client("project", "path", logger)

        # THEN
        logger.warning.assert_called_once()

    @patch('commons.big_query.BigQueryTableManager._get_credentials_and_client')
    @patch('os.path.exists')
    def test_create_external_table_missing_files(self, mock_exists, mock_client):
        """Testa criação de tabela externa com arquivos ausentes."""
        # GIVEN
        mock_exists.return_value = False

        # WHEN & THEN
        with pytest.raises(FileNotFoundError):
            BigQueryTableManager.create_external_table("project", "tool", "table", "creds.json")


class TestBigQueryMainClassCoverage:
    """Testes para garantir cobertura completa da classe BigQuery principal."""

    def test_constants_defined(self):
        """Testa se todas as constantes estão definidas."""
        # GIVEN & WHEN & THEN
        assert BigQuery.DEFAULT_OUTPUT_DIR == './output'
        assert BigQuery.CSV_DELIMITER == ';'
        assert BigQuery.LOG_FORMAT is not None

    def test_initialization_with_defaults(self):
        """Testa inicialização com valores padrão."""
        # GIVEN & WHEN
        bq = BigQuery()

        # THEN
        assert bq.output_dir == Path('./output')
        assert bq.max_workers > 0
        assert bq._lock is not None

    def test_initialization_with_custom_values(self):
        """Testa inicialização com valores customizados."""
        # GIVEN & WHEN
        bq = BigQuery(output_dir='/custom', max_workers=4)

        # THEN
        assert bq.output_dir == Path('/custom')
        assert bq.max_workers == 4

    def test_thread_safe_log(self):
        """Testa logging thread-safe."""
        # GIVEN
        bq = BigQuery()
        bq.logger = Mock()

        # WHEN
        bq._thread_safe_log('info', 'test message')

        # THEN
        bq.logger.info.assert_called_once_with('test message')

    def test_find_csv_files_empty_directory(self):
        """Testa busca de arquivos CSV em diretório vazio."""
        # GIVEN
        with tempfile.TemporaryDirectory() as temp_dir:
            bq = BigQuery(output_dir=temp_dir)

            # WHEN
            files = bq._find_csv_files()

            # THEN
            assert len(files) == 0

    def test_process_all_csv_files_no_files(self):
        """Testa processamento quando não há arquivos."""
        # GIVEN
        with tempfile.TemporaryDirectory() as temp_dir:
            bq = BigQuery(output_dir=temp_dir)

            # WHEN
            bq._process_all_csv_files()

            # THEN - Deve completar sem erro

    def test_save_schema_format(self):
        """Testa formato de salvamento do schema."""
        # GIVEN
        bq = BigQuery()
        schema = [
            {'name': 'col1', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'col2', 'type': 'INT64', 'mode': 'NULLABLE'}
        ]

        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            schema_path = f.name

        # WHEN
        bq._save_schema(schema, schema_path)

        # THEN
        with open(schema_path, 'r') as f:
            saved_schema = json.load(f)

        assert len(saved_schema) == 2
        assert saved_schema[0]['name'] == 'col1'
        assert saved_schema[0]['type'] == 'STRING'
        assert 'mode' not in saved_schema[0]  # Deve ser removido

        os.unlink(schema_path)


class TestAnalyzeColumnWorkerCoverage:
    """Testes para garantir cobertura completa da função analyze_column_worker."""

    def test_analyze_column_worker_empty_data(self):
        """Testa worker com dados vazios."""
        # GIVEN & WHEN
        result = analyze_column_worker("empty_col", [],
                                       DataTypeDetector.BOOLEAN_VALUES,
                                       DataTypeDetector.TYPE_MAPPING)

        # THEN
        assert result['column'] == 'empty_col'
        assert result['suggested_type'] == 'STRING'
        assert result['total_records'] == 0
        assert result['inconsistent_count'] == 0

    def test_analyze_column_worker_all_null(self):
        """Testa worker com todos valores nulos."""
        # GIVEN & WHEN
        result = analyze_column_worker("null_col", [None, pd.NA, ''],
                                       DataTypeDetector.BOOLEAN_VALUES,
                                       DataTypeDetector.TYPE_MAPPING)

        # THEN
        assert result['column'] == 'null_col'
        assert result['suggested_type'] == 'STRING'
        assert result['null_records'] > 0

    def test_analyze_column_worker_consistent_integers(self):
        """Testa worker com inteiros consistentes."""
        # GIVEN & WHEN
        result = analyze_column_worker("int_col", ["1", "2", "3", "4"],
                                       DataTypeDetector.BOOLEAN_VALUES,
                                       DataTypeDetector.TYPE_MAPPING)

        # THEN
        assert result['column'] == 'int_col'
        assert result['suggested_type'] == 'INT64'
        assert result['inconsistent_count'] == 0
        assert result['confidence_score'] == 100.0

    def test_analyze_column_worker_mixed_data(self):
        """Testa worker com dados mistos."""
        # GIVEN & WHEN
        result = analyze_column_worker("mixed_col", ["1", "2", "abc", "4"],
                                       DataTypeDetector.BOOLEAN_VALUES,
                                       DataTypeDetector.TYPE_MAPPING)

        # THEN
        assert result['column'] == 'mixed_col'
        assert result['inconsistent_count'] == 1
        assert 'abc' in result['inconsistent_values']
        assert result['confidence_score'] < 100.0

    def test_analyze_column_worker_error_handling(self):
        """Testa worker com tratamento de erro."""
        # GIVEN & WHEN
        result = analyze_column_worker("robust_col", [object()],
                                       DataTypeDetector.BOOLEAN_VALUES,
                                       DataTypeDetector.TYPE_MAPPING)

        # THEN
        assert result['column'] == 'robust_col'
        assert result['suggested_type'] == 'STRING'  # Fallback seguro


class TestEdgeCasesAndErrorHandling:
    """Testes para casos extremos e tratamento de erros."""

    def test_process_csv_files_class_method(self):
        """Testa método de classe process_csv_files."""
        # GIVEN
        with tempfile.TemporaryDirectory() as temp_dir:
            # WHEN & THEN - Deve completar sem erro
            BigQuery.process_csv_files(output_dir=temp_dir, max_workers=1)

    @patch('commons.big_query.BigQueryTableManager.create_external_table')
    @patch('commons.big_query.BigQueryTableManager.create_gold_table')
    def test_start_pipeline_success(self, mock_gold, mock_external):
        """Testa pipeline completo com sucesso."""
        # GIVEN
        mock_external.return_value = None
        mock_gold.return_value = None

        # WHEN & THEN - Deve completar sem erro
        BigQuery.start_pipeline("project", "tool", "table", "creds.json")

    @patch('commons.big_query.BigQueryTableManager.create_external_table')
    def test_start_pipeline_failure(self, mock_external):
        """Testa pipeline com falha."""
        # GIVEN
        mock_external.side_effect = Exception("Test error")

        # WHEN & THEN
        with pytest.raises(Exception, match="Test error"):
            BigQuery.start_pipeline("project", "tool", "table", "creds.json")

    def test_data_type_detector_with_invalid_numeric(self):
        """Testa DataTypeDetector com valor numérico inválido."""
        # GIVEN & WHEN & THEN
        assert DataTypeDetector._is_numeric("invalid_decimal") is False

    def test_timestamp_conversion_invalid_values(self):
        """Testa conversão de timestamp com valores inválidos."""
        # GIVEN
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            f.write("timestamp_col\n32/12/2023 14:30:45\n")  # Data inválida
            csv_path = f.name

        logger = Mock()

        # WHEN
        result = DataPreprocessor.convert_timestamp_br_to_iso(csv_path, None, logger)

        # THEN
        assert result == 0  # Nenhuma conversão realizada
        os.unlink(csv_path)
