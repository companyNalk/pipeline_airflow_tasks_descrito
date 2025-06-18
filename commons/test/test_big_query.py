import json
import os
import shutil
from pathlib import Path
from unittest.mock import patch, mock_open, MagicMock, ANY

import pandas as pd
import pytest

from commons.big_query import BigQuery, analyze_column_worker


@pytest.fixture(autouse=True)
def cleanup_output_directory():
    """Fixture que limpa o diretório output antes e depois de cada teste."""
    output_dir = "./output"
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    yield
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)


class TestBigQueryInit:
    """Testes para inicialização da classe BigQuery."""

    def test_init_default_values(self):
        """Testa inicialização com valores padrão."""
        # GIVEN

        # WHEN
        bq = BigQuery()

        # THEN
        assert bq.output_dir == Path('./output')
        assert bq.max_workers > 0
        assert hasattr(bq, 'logger')

    def test_init_custom_values(self):
        """Testa inicialização com valores personalizados."""
        # GIVEN
        custom_dir = "/custom/path"
        custom_workers = 4

        # WHEN
        bq = BigQuery(output_dir=custom_dir, max_workers=custom_workers)

        # THEN
        assert bq.output_dir == Path(custom_dir)
        assert bq.max_workers == custom_workers

    def test_thread_safe_logging(self):
        """Testa se o logging é thread-safe."""
        # GIVEN
        bq = BigQuery()

        # WHEN
        with patch.object(bq.logger, 'info') as mock_log:
            bq._thread_safe_log('info', 'test message')

        # THEN
        mock_log.assert_called_once_with('test message')


class TestPatternDetection:
    """Testes para detecção de padrões em dados."""

    def test_pattern_detection_basic_types(self):
        """Testa detecção de tipos básicos."""
        # GIVEN
        bq = BigQuery()

        # WHEN & THEN
        assert bq._detect_pattern("123") == "integer"
        assert bq._detect_pattern("-456") == "integer"
        assert bq._detect_pattern("123.456789") == "float"
        assert bq._detect_pattern("123.45") == "float"
        assert bq._detect_pattern("123.45e2") == "float"
        assert bq._detect_pattern("true") == "boolean"
        assert bq._detect_pattern("false") == "boolean"
        assert bq._detect_pattern("yes") == "boolean"
        assert bq._detect_pattern("12:30:45") == "time"
        assert bq._detect_pattern("hello") == "string"

    def test_integer_validation(self):
        """Testa validação específica de inteiros."""
        # GIVEN
        bq = BigQuery()

        # WHEN & THEN
        assert bq._is_integer("123")
        assert bq._is_integer("-456")
        assert bq._is_integer("0")
        assert not bq._is_integer("123.45")
        assert not bq._is_integer("abc")
        assert not bq._is_integer("9223372036854775808")

    def test_numeric_validation(self):
        """Testa validação de números de alta precisão."""
        # GIVEN
        bq = BigQuery()

        # WHEN & THEN
        assert bq._is_numeric("123.1234567")
        assert not bq._is_numeric("123.123")
        assert not bq._is_numeric("123")
        assert not bq._is_numeric("abc.123")

    def test_boolean_detection(self):
        """Testa detecção de valores booleanos."""
        # GIVEN
        bq = BigQuery()
        text_booleans = ['true', 'false', 'yes', 'no', 'sim', 'não']
        numeric_values = ['1', '0']

        # WHEN & THEN
        for value in text_booleans:
            assert bq._detect_pattern(value) == 'boolean'

        for value in numeric_values:
            assert bq._detect_pattern(value) in ['integer', 'boolean']

    def test_date_detection(self):
        """Testa detecção de diferentes tipos de data."""
        # GIVEN
        bq = BigQuery()
        test_cases = [
            ("2023-01-01", "date"),
            ("2023-01-01 12:30:45", "datetime"),
            ("2023-01-01T12:30:45Z", "timestamp"),
            ("2023-01-01T12:30:45+00:00", "timestamp"),
        ]

        # WHEN & THEN
        for value, expected_type in test_cases:
            assert bq._detect_pattern(value) == expected_type

    def test_time_validation(self):
        """Testa validação de horários."""
        # GIVEN
        bq = BigQuery()
        valid_times = ["12:30:45", "23:59:59", "12:30:45.123"]
        invalid_times = ["abc:30:45", "12:70:45"]

        # WHEN & THEN
        for time_val in valid_times:
            assert bq._is_time(time_val)

        for time_val in invalid_times:
            assert not bq._is_time(time_val)


class TestColumnAnalysis:
    """Testes para análise de colunas."""

    def test_analyze_column_worker_integers(self):
        """Testa análise de coluna com inteiros."""
        # GIVEN
        column_data = ["1", "2", "3", "4", "5"]

        # WHEN
        result = analyze_column_worker(
            "test_col", column_data,
            BigQuery.BOOLEAN_VALUES, BigQuery.TYPE_MAPPING
        )

        # THEN
        assert result['column'] == 'test_col'
        assert result['suggested_type'] == 'INT64'
        assert result['non_null_records'] == 5
        assert result['inconsistent_count'] == 0
        assert result['confidence_score'] == 100.0

    def test_analyze_column_worker_empty_data(self):
        """Testa análise de coluna vazia."""
        # GIVEN
        column_data = [None, None, None]

        # WHEN
        result = analyze_column_worker(
            "empty_col", column_data,
            BigQuery.BOOLEAN_VALUES, BigQuery.TYPE_MAPPING
        )

        # THEN
        assert result['suggested_type'] == 'STRING'
        assert result['non_null_records'] == 0
        assert result['null_records'] == 3
        assert result['confidence_score'] == 0

    def test_analyze_column_worker_with_nulls(self):
        """Testa análise de coluna com valores nulos misturados."""
        # GIVEN
        column_data = ["1", "2", None, "4", None]

        # WHEN
        result = analyze_column_worker(
            "with_nulls", column_data,
            BigQuery.BOOLEAN_VALUES, BigQuery.TYPE_MAPPING
        )

        # THEN
        assert result['suggested_type'] == 'INT64'
        assert result['non_null_records'] == 3
        assert result['null_records'] == 2
        assert result['inconsistent_count'] == 0


class TestCsvProcessing:
    """Testes para processamento de arquivos CSV."""

    def test_find_csv_files_empty_directory(self):
        """Testa busca em diretório vazio."""
        # GIVEN
        bq = BigQuery()

        # WHEN
        with patch('glob.glob', return_value=[]):
            files = bq._find_csv_files()

        # THEN
        assert files == []

    def test_find_csv_files_with_files(self):
        """Testa busca com arquivos presentes."""
        # GIVEN
        bq = BigQuery()
        mock_files = ["/path/file1.csv", "/path/file2.csv"]

        # WHEN
        with patch('glob.glob', return_value=mock_files):
            files = bq._find_csv_files()

        # THEN
        assert len(files) == 2
        assert all(isinstance(f, Path) for f in files)

    def test_process_single_csv_success(self):
        """Testa processamento bem-sucedido de um CSV."""
        # GIVEN
        bq = BigQuery()
        csv_path = Path("test.csv")
        mock_df = pd.DataFrame({
            "col1": ["1", "2", "3"],
            "col2": ["a", "b", "c"]
        })
        mock_schema = [{"name": "col1", "type": "INT64", "mode": "NULLABLE"}]
        mock_report = [{"column": "col1", "suggested_type": "INT64"}]

        # WHEN
        with patch('pandas.read_csv', return_value=mock_df), \
                patch.object(bq, '_generate_schema_parallel', return_value=(mock_schema, mock_report)), \
                patch.object(bq, '_generate_inconsistency_report'), \
                patch.object(bq, '_save_schema'), \
                patch.object(bq, '_thread_safe_log'):
            bq._process_single_csv(csv_path)

        # THEN - Não há erros se chegou aqui

    def test_process_single_csv_empty_file(self):
        """Testa processamento de arquivo vazio."""
        # GIVEN
        bq = BigQuery()
        csv_path = Path("empty.csv")

        # WHEN & THEN
        with patch('pandas.read_csv', side_effect=pd.errors.EmptyDataError()), \
                patch.object(bq, '_thread_safe_log'):
            with pytest.raises(pd.errors.EmptyDataError):
                bq._process_single_csv(csv_path)

    def test_process_all_csv_files_no_files(self):
        """Testa processamento quando não há arquivos CSV."""
        # GIVEN
        bq = BigQuery()

        # WHEN
        with patch.object(bq, '_find_csv_files', return_value=[]):
            bq._process_all_csv_files()

        # THEN - Deve completar sem erro

    def test_process_all_csv_files_with_files(self):
        """Testa processamento paralelo de múltiplos arquivos."""
        # GIVEN
        bq = BigQuery()
        mock_files = [Path("file1.csv"), Path("file2.csv")]

        # WHEN
        with patch.object(bq, '_find_csv_files', return_value=mock_files), \
                patch.object(bq, '_process_single_csv_safe') as mock_process:
            bq._process_all_csv_files()

        # THEN
        assert mock_process.call_count == 2


class TestSchemaGeneration:
    """Testes para geração de esquemas."""

    def test_generate_schema_parallel_simple(self):
        """Testa geração de schema com dados simples."""
        # GIVEN
        bq = BigQuery()
        df = pd.DataFrame({
            "int_col": ["1", "2", "3"],
            "str_col": ["a", "b", "c"]
        })
        mock_results = [
            {"column": "int_col", "suggested_type": "INT64", "inconsistent_percent": 0, "type_reason": "OTIMIZADO"},
            {"column": "str_col", "suggested_type": "STRING", "inconsistent_percent": 10, "type_reason": "SEGURO"}
        ]

        # WHEN
        with patch('concurrent.futures.ProcessPoolExecutor') as mock_executor:
            mock_future = MagicMock()
            mock_future.result.side_effect = mock_results
            mock_executor.return_value.__enter__.return_value.submit.return_value = mock_future
            mock_executor.return_value.__enter__.return_value.__iter__ = lambda self: iter([mock_future, mock_future])
            schema, report = bq._generate_schema_parallel(df)

        # THEN
        assert len(schema) == 2
        assert len(report) == 2
        assert schema[0]['name'] == 'int_col'
        assert schema[0]['type'] == 'INT64'

    def test_save_schema_success(self):
        """Testa salvamento bem-sucedido do schema."""
        # GIVEN
        bq = BigQuery()
        schema = [
            {"name": "col1", "type": "INT64", "mode": "NULLABLE"},
            {"name": "col2", "type": "STRING", "mode": "NULLABLE"}
        ]
        expected_content = [
            {"name": "col1", "type": "INT64"},
            {"name": "col2", "type": "STRING"}
        ]

        # WHEN
        with patch('builtins.open', mock_open()), \
                patch('json.dump') as mock_json_dump:
            bq._save_schema(schema, "test_schema.json")

        # THEN
        mock_json_dump.assert_called_once_with(
            expected_content, ANY, indent=2, ensure_ascii=False
        )

    def test_save_schema_error(self):
        """Testa tratamento de erro no salvamento do schema."""
        # GIVEN
        bq = BigQuery()
        schema = [{"name": "col1", "type": "INT64"}]

        # WHEN & THEN
        with patch('builtins.open', side_effect=IOError("Permission denied")):
            with pytest.raises(IOError):
                bq._save_schema(schema, "test_schema.json")


class TestReporting:
    """Testes para geração de relatórios."""

    def test_generate_inconsistency_report_no_issues(self):
        """Testa relatório quando não há inconsistências."""
        # GIVEN
        bq = BigQuery()
        report = [
            {
                "column": "col1", "final_type": "INT64", "type_reason": "OTIMIZADO",
                "inconsistent_percent": 0, "inconsistent_count": 0
            }
        ]

        # WHEN
        with patch.object(bq.logger, 'info') as mock_log:
            bq._generate_inconsistency_report(report)
            log_messages = [call[0][0] for call in mock_log.call_args_list]

        # THEN
        assert any("Nenhuma inconsistência" in msg for msg in log_messages)

    def test_generate_inconsistency_report_with_issues(self):
        """Testa relatório com inconsistências encontradas."""
        # GIVEN
        bq = BigQuery()
        report = [
            {
                "column": "problematic_col", "final_type": "STRING", "type_reason": "SEGURO",
                "suggested_type": "INT64", "inconsistent_percent": 15.5,
                "inconsistent_count": 100, "non_null_records": 1000, "null_records": 50,
                "confidence_score": 84.5, "inconsistent_values": ["abc", "def"]
            }
        ]

        # WHEN
        with patch.object(bq.logger, 'info') as mock_log:
            bq._generate_inconsistency_report(report, threshold=10.0)
            log_messages = [call[0][0] for call in mock_log.call_args_list]

        # THEN
        assert any("problematic_col" in msg for msg in log_messages)
        assert any("15.5" in str(msg) for msg in log_messages)

    def test_most_affected_types(self):
        """Testa identificação dos tipos mais afetados."""
        # GIVEN
        bq = BigQuery()
        columns = [
            {"suggested_type": "INT64"},
            {"suggested_type": "INT64"},
            {"suggested_type": "FLOAT64"},
        ]

        # WHEN
        result = bq._most_affected_types(columns)

        # THEN
        assert "INT64 (2)" in result
        assert "FLOAT64 (1)" in result


class TestBigQueryOperations:
    """Testes para operações do BigQuery."""

    def test_create_externa_table_success(self):
        """Testa criação bem-sucedida de tabela externa."""
        # GIVEN
        schema_content = [{"name": "col1", "type": "INT64"}]
        credentials_content = {"type": "service_account", "project_id": "test-project"}

        # WHEN
        with patch('os.path.exists', return_value=True), \
                patch('builtins.open') as mock_open_file, \
                patch('google.oauth2.service_account.Credentials.from_service_account_file'), \
                patch('google.cloud.bigquery.Client') as mock_client:

            def side_effect(file_path, *args, **kwargs):
                if 'credentials.json' in str(file_path):
                    return mock_open(read_data=json.dumps(credentials_content))()
                else:
                    return mock_open(read_data=json.dumps(schema_content))()

            mock_open_file.side_effect = side_effect

            mock_job = MagicMock()
            mock_table = MagicMock()
            mock_table.num_rows = 1000

            mock_client_instance = mock_client.return_value
            mock_client_instance.load_table_from_file.return_value = mock_job
            mock_client_instance.get_table.return_value = mock_table

            BigQuery._create_externa_table(
                "test-project", "test_tool", "test_table", "credentials.json"
            )

        # THEN
        mock_client_instance.delete_table.assert_called_once()
        mock_client_instance.load_table_from_file.assert_called_once()
        mock_job.result.assert_called_once()

    def test_create_externa_table_missing_files(self):
        """Testa erro quando arquivos estão ausentes."""
        # GIVEN & WHEN & THEN
        with patch('os.path.exists', return_value=False):
            with pytest.raises(FileNotFoundError):
                BigQuery._create_externa_table(
                    "test-project", "test_tool", "test_table", "credentials.json"
                )

    def test_create_gold_table_success(self):
        """Testa criação bem-sucedida de tabela gold."""
        # GIVEN
        credentials_content = {"type": "service_account", "project_id": "test-project"}

        # WHEN
        with patch('builtins.open', mock_open(read_data=json.dumps(credentials_content))), \
                patch('google.oauth2.service_account.Credentials.from_service_account_file'), \
                patch('google.cloud.bigquery.Client') as mock_client:
            mock_job = MagicMock()
            mock_table = MagicMock()
            mock_table.num_rows = 1000
            mock_table.schema = ["col1", "col2"]

            mock_client_instance = mock_client.return_value
            mock_client_instance.query.return_value = mock_job
            mock_client_instance.get_table.return_value = mock_table

            BigQuery._create_gold_table(
                "test-project", "test_tool", "test_table", "credentials.json"
            )

        # THEN
        mock_client_instance.query.assert_called_once()
        mock_job.result.assert_called_once()

    def test_start_pipeline_success(self):
        """Testa pipeline completo bem-sucedido."""
        # GIVEN & WHEN
        with patch.object(BigQuery, '_create_externa_table') as mock_externa, \
                patch.object(BigQuery, '_create_gold_table') as mock_gold:
            BigQuery.start_pipeline(
                "test-project", "test_tool", "test_table", "credentials.json"
            )

        # THEN
        mock_externa.assert_called_once()
        mock_gold.assert_called_once()

    def test_start_pipeline_failure(self):
        """Testa falha no pipeline."""
        # GIVEN & WHEN & THEN
        with patch.object(BigQuery, '_create_externa_table', side_effect=Exception("BigQuery error")):
            with pytest.raises(Exception):
                BigQuery.start_pipeline(
                    "test-project", "test_tool", "test_table", "credentials.json"
                )


class TestStaticMethods:
    """Testes para métodos estáticos."""

    def test_process_csv_files_class_method(self):
        """Testa método de classe para processar arquivos CSV."""
        # GIVEN & WHEN
        with patch.object(BigQuery, '_process_all_csv_files') as mock_process:
            BigQuery.process_csv_files("./test_output", max_workers=2)

        # THEN
        mock_process.assert_called_once()


class TestExceptionHandling:
    """Testes para verificar tratamento de exceções."""

    def test_pattern_detection_exception(self):
        """Testa tratamento de exceção na detecção de padrões."""
        # GIVEN
        bq = BigQuery()

        # WHEN
        with patch.object(bq, '_is_integer', side_effect=Exception("Parse error")):
            result = bq._detect_pattern("123")

        # THEN
        assert result == 'string'

    def test_analyze_column_worker_exception(self):
        """Testa tratamento de exceção no worker de análise."""
        # GIVEN & WHEN
        with patch('pandas.notna', side_effect=Exception("Pandas error")):
            result = analyze_column_worker(
                "error_col", ["1", "2", "3"],
                BigQuery.BOOLEAN_VALUES, BigQuery.TYPE_MAPPING
            )

        # THEN
        assert result['suggested_type'] == 'STRING'
        assert 'error' in result

    def test_generate_inconsistency_report_exception(self):
        """Testa tratamento de exceção no relatório de inconsistências."""
        # GIVEN
        bq = BigQuery()
        invalid_report = [{"invalid": "data"}]

        # WHEN
        with patch.object(bq.logger, 'error') as mock_error:
            bq._generate_inconsistency_report(invalid_report)

        # THEN
        mock_error.assert_called()

    def test_most_affected_types_exception(self):
        """Testa tratamento de exceção na identificação de tipos afetados."""
        # GIVEN
        bq = BigQuery()
        invalid_columns = [{"wrong_key": "value"}]

        # WHEN
        result = bq._most_affected_types(invalid_columns)

        # THEN
        assert result == "Erro ao calcular"

    def test_bigquery_operations_exceptions(self):
        """Testa tratamento de exceções nas operações do BigQuery."""
        # GIVEN & WHEN & THEN
        with patch('os.path.exists', return_value=True), \
                patch('builtins.open', mock_open(read_data='[{"name": "col1", "type": "INT64"}]')), \
                patch('google.oauth2.service_account.Credentials.from_service_account_file'), \
                patch('google.cloud.bigquery.Client', side_effect=Exception("BigQuery connection error")):
            with pytest.raises(Exception):
                BigQuery._create_externa_table(
                    "test-project", "test_tool", "test_table", "credentials.json"
                )

        with patch('google.oauth2.service_account.Credentials.from_service_account_file'), \
                patch('google.cloud.bigquery.Client', side_effect=Exception("BigQuery error")):
            with pytest.raises(Exception):
                BigQuery._create_gold_table(
                    "test-project", "test_tool", "test_table", "credentials.json"
                )


class TestAdditionalCoverage:
    """Testes para garantir cobertura completa."""

    def test_csv_delimiter_constant(self):
        """Testa se a constante do delimitador está definida."""
        # GIVEN & WHEN & THEN
        assert BigQuery.CSV_DELIMITER == ';'

    def test_type_mapping_constant(self):
        """Testa se o mapeamento de tipos está completo."""
        # GIVEN
        expected_types = [
            'integer', 'numeric', 'float', 'date', 'datetime',
            'timestamp', 'time', 'boolean', 'string'
        ]

        # WHEN & THEN
        for type_name in expected_types:
            assert type_name in BigQuery.TYPE_MAPPING

    def test_boolean_values_constant(self):
        """Testa se os valores booleanos estão definidos."""
        # GIVEN & WHEN & THEN
        assert 'true' in BigQuery.BOOLEAN_VALUES
        assert 'false' in BigQuery.BOOLEAN_VALUES
        assert '1' in BigQuery.BOOLEAN_VALUES
        assert '0' in BigQuery.BOOLEAN_VALUES

    def test_edge_case_integer_bounds(self):
        """Testa limites de inteiros."""
        # GIVEN
        bq = BigQuery()

        # WHEN & THEN
        assert bq._is_integer("9223372036854775807")
        assert bq._is_integer("-9223372036854775808")
        assert not bq._is_integer("9223372036854775808")

    def test_edge_case_empty_strings(self):
        """Testa comportamento com strings vazias."""
        # GIVEN
        bq = BigQuery()

        # WHEN & THEN
        assert bq._detect_pattern("") == 'string'
        assert bq._detect_pattern("   ") == 'string'

    def test_worker_function_direct_call(self):
        """Testa chamada direta da função worker."""
        # GIVEN & WHEN
        result = analyze_column_worker(
            "direct_test", ["1", "2", "3"],
            BigQuery.BOOLEAN_VALUES, BigQuery.TYPE_MAPPING
        )

        # THEN
        assert result['column'] == 'direct_test'
        assert result['suggested_type'] == 'INT64'
        assert result['inconsistent_count'] == 0
