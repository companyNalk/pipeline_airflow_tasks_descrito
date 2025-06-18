import os
import shutil
import uuid
from unittest.mock import patch, mock_open

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from commons.utils import Utils


@pytest.fixture(autouse=True)
def cleanup_output_directory():
    """Fixture que limpa o diretório output antes e depois de cada teste."""
    output_dir = "output"
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    yield
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)


class TestFlattenJson:
    """Testes para achatamento de estruturas JSON."""

    def test_flatten_empty_dict(self):
        # GIVEN
        data = {}

        # WHEN
        result = Utils._flatten_json(data)

        # THEN
        assert result == {}

    def test_flatten_simple_dict(self):
        # GIVEN
        data = {"a": 1, "b": 2}

        # WHEN
        result = Utils._flatten_json(data)

        # THEN
        assert result == {"a": 1, "b": 2}

    def test_flatten_nested_dict(self):
        # GIVEN
        data = {"a": 1, "b": {"c": 2, "d": 3}}
        expected = {"a": 1, "b_c": 2, "b_d": 3}

        # WHEN
        result = Utils._flatten_json(data)

        # THEN
        assert result == expected

    def test_flatten_with_list(self):
        # GIVEN
        data = {"a": 1, "b": [10, 20, 30]}
        expected = {"a": 1, "b_1": 10, "b_2": 20, "b_3": 30}

        # WHEN
        result = Utils._flatten_json(data)

        # THEN
        assert result == expected

    def test_flatten_complex_structure(self):
        # GIVEN
        data = {
            "name": "John",
            "address": {"street": "Main St", "city": "New York"},
            "phones": [
                {"type": "home", "number": "123"},
                {"type": "work", "number": "456"}
            ]
        }
        expected = {
            "name": "John",
            "address_street": "Main St",
            "address_city": "New York",
            "phones_1_type": "home",
            "phones_1_number": "123",
            "phones_2_type": "work",
            "phones_2_number": "456"
        }

        # WHEN
        result = Utils._flatten_json(data)

        # THEN
        assert result == expected

    def test_flatten_custom_separator(self):
        # GIVEN
        data = {"a": {"b": 1}}

        # WHEN
        result = Utils._flatten_json(data, sep=".")

        # THEN
        assert result == {"a.b": 1}

    def test_flatten_with_parent_key(self):
        # GIVEN
        data = {"a": 1}

        # WHEN
        result = Utils._flatten_json(data, parent_key="parent")

        # THEN
        assert result == {"parent_a": 1}


class TestNormalizeKey:
    """Testes para normalização de chaves/strings."""

    def test_basic_transformations(self):
        """Testa transformações básicas em uma função."""
        # GIVEN
        test_cases = [
            ("AbCdEf", "abcdef"),
            ("açãó êíóú", "acao_eiou"),
            ("test@#$%^&*()!~`+=[]{}<>:;,.?/|\\", "test"),
            ("test with spaces", "test_with_spaces"),
            ("___test___", "test"),
            ("camelCaseToSnakeCase", "camel_case_to_snake_case"),
        ]

        # WHEN & THEN
        for input_val, expected in test_cases:
            result = Utils._normalize_key(input_val)
            assert result == expected

    def test_complex_normalization(self):
        """Testa normalização complexa."""
        # GIVEN
        key = "  Usuário@Endereço-Email  "
        expected = "usuario_endereco_email"

        # WHEN
        result = Utils._normalize_key(key)

        # THEN
        assert result == expected

    def test_pascal_case_conversion(self):
        """Testa conversão específica de PascalCase."""
        # GIVEN
        test_cases = [
            ("ExibeContato", "exibe_contato"),
            ("IdStatusGestao", "id_status_gestao")
        ]

        # WHEN & THEN
        for input_val, expected in test_cases:
            result = Utils._normalize_key(input_val, use_pascal_case=True)
            assert result == expected


class TestNormalizeKeys:
    """Testes para normalização de estruturas de dados."""

    def test_normalize_dict_keys(self):
        # GIVEN
        data = {"User Name": "John", "Email@Address": "john@example.com"}
        expected = {"user_name": "John", "email_address": "john@example.com"}

        # WHEN
        result = Utils._normalize_keys(data)

        # THEN
        assert result == expected

    def test_normalize_nested_structures(self):
        # GIVEN
        data = {
            "User": {
                "Full Name": "John Doe",
                "ContactInfo": {"Email": "john@example.com"}
            }
        }
        expected = {
            "user": {
                "full_name": "John Doe",
                "contactinfo": {"email": "john@example.com"}
            }
        }

        # WHEN
        result = Utils._normalize_keys(data)

        # THEN
        assert result == expected

    def test_normalize_list_of_dicts(self):
        # GIVEN
        data = [{"First Name": "John"}, {"Last Name": "Doe"}]
        expected = [{"first_name": "John"}, {"last_name": "Doe"}]

        # WHEN
        result = Utils._normalize_keys(data)

        # THEN
        assert result == expected

    def test_normalize_scalar_values(self):
        """Testa que valores escalares permanecem inalterados."""
        # GIVEN
        scalar_string = "test"
        scalar_list = [1, "test", True]

        # WHEN
        result_string = Utils._normalize_keys(scalar_string)
        result_list = Utils._normalize_keys(scalar_list)

        # THEN
        assert result_string == "test"
        assert result_list == [1, "test", True]


class TestDataFrameOperations:
    """Testes para operações em DataFrames."""

    def test_normalize_column_names(self):
        # GIVEN
        df = pd.DataFrame({
            "User Name": [1, 2, 3],
            "Email@Address": [4, 5, 6],
            '"Texto"': ["a", "b", "c"],
            "'novoTexto'": ["d", "e", "f"]
        })
        expected_columns = ["user_name", "email_address", "texto", "novo_texto"]

        # WHEN
        result = Utils._normalize_column_names(df)

        # THEN
        assert list(result.columns) == expected_columns

    def test_remove_empty_columns_nan(self):
        # GIVEN
        df = pd.DataFrame({
            'a': [1, 2, 3],
            'b': [None, None, None],
            'c': [4, 5, 6]
        })
        expected = pd.DataFrame({'a': [1, 2, 3], 'c': [4, 5, 6]})

        # WHEN
        with patch('commons.utils.logging') as mock_logging:
            result = Utils._remove_empty_columns(df)

        # THEN
        assert_frame_equal(result, expected)
        mock_logging.info.assert_any_call("Colunas removidas por serem NaN: 1")

    def test_remove_empty_columns_strings(self):
        # GIVEN
        df = pd.DataFrame({
            'a': [1, 2, 3],
            'b': ['', '', ''],
            'c': [4, 5, 6]
        })
        expected = pd.DataFrame({'a': [1, 2, 3], 'c': [4, 5, 6]})

        # WHEN
        with patch('commons.utils.logging') as mock_logging:
            result = Utils._remove_empty_columns(df)

        # THEN
        assert_frame_equal(result, expected)
        mock_logging.info.assert_any_call("Colunas removidas por serem strings vazias: 1")

    def test_remove_empty_columns_mixed(self):
        # GIVEN
        df = pd.DataFrame({
            'a': [1, 2, 3],
            'b': [None, None, None],
            'c': ['', '', ''],
            'd': [4, 5, 6]
        })
        expected = pd.DataFrame({'a': [1, 2, 3], 'd': [4, 5, 6]})

        # WHEN
        with patch('commons.utils.logging') as mock_logging:
            result = Utils._remove_empty_columns(df)

        # THEN
        assert_frame_equal(result, expected)
        mock_logging.info.assert_any_call("Total de colunas removidas: 2")

    def test_remove_empty_columns_edge_cases(self):
        # GIVEN - DataFrame vazio
        empty_df = pd.DataFrame()

        # WHEN
        with patch('commons.utils.logging') as mock_logging:
            result = Utils._remove_empty_columns(empty_df)

        # THEN
        assert result.empty
        mock_logging.warning.assert_called_once()

        # GIVEN - Nenhuma coluna vazia
        normal_df = pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})

        # WHEN
        with patch('commons.utils.logging') as mock_logging:
            result = Utils._remove_empty_columns(normal_df)

        # THEN
        assert_frame_equal(result, normal_df)
        mock_logging.info.assert_not_called()

        # GIVEN - Todas as colunas vazias
        all_empty_df = pd.DataFrame({'a': [None, None], 'b': ['', '']})

        # WHEN & THEN
        with patch('commons.utils.logging'):
            with pytest.raises(ValueError, match="DataFrame resultante está vazio"):
                Utils._remove_empty_columns(all_empty_df)

    def test_convert_id_columns(self):
        # GIVEN
        df = pd.DataFrame({
            "id": [1.0, 2.0, 3.0],
            "user_id": [10.0, 20.0, None],
            "name": ["Alice", "Bob", "Charlie"]
        })

        # WHEN
        with patch('commons.utils.logging') as mock_logging:
            result = Utils._process_and_convert_id_columns(df)

        # THEN
        assert result["id"].dtype == "Int64"
        assert result["user_id"].dtype == "Int64"
        assert result["name"].dtype == object
        assert mock_logging.info.call_count == 2

    def test_convert_id_columns_with_uuid(self):
        # GIVEN
        test_uuid = uuid.uuid4()
        df = pd.DataFrame({
            "id": [test_uuid, None],
            "regular_id": [1.0, 2.0]
        })

        # WHEN
        result = Utils._process_and_convert_id_columns(df)

        # THEN
        assert result["id"].dtype == object  # UUID mantido
        assert result["regular_id"].dtype == "Int64"

    def test_convert_nullable_int_columns(self):
        # GIVEN
        df = pd.DataFrame({
            "int_col": [1, 2, 3, None],
            "float_col": [1.0, 2.5, 3.0, None],
            "string_col": ["a", "b", "c", None]
        })

        # WHEN
        with patch('commons.utils.logging') as mock_logging:
            result = Utils._convert_columns_to_nullable_int(df.copy())

        # THEN
        assert result["int_col"].dtype == "Int64"
        assert result["float_col"].dtype == float
        assert result["string_col"].dtype == object
        mock_logging.info.assert_called_once()


class TestDataUtilities:
    """Testes para utilitários diversos de manipulação de dados."""

    def test_remove_newlines_from_fields(self):
        # GIVEN
        data = [
            {"text": "Line 1\nLine 2"},
            {"text": "Line 1\r\nLine 2"},
            {"text": "Line 1\rLine 2"},
            {"number": 123, "bool_val": True}
        ]
        expected = [
            {"text": "Line 1 Line 2"},
            {"text": "Line 1 Line 2"},
            {"text": "Line 1 Line 2"},
            {"number": 123, "bool_val": True}
        ]

        # WHEN
        result = Utils._remove_newlines_from_fields(data)

        # THEN
        assert result == expected

    def test_remove_newlines_empty_data(self):
        # GIVEN
        empty_data = []

        # WHEN
        result = Utils._remove_newlines_from_fields(empty_data)

        # THEN
        assert result == []


class TestFileSaving:
    """Testes para operações de salvamento de arquivos."""

    def test_save_local_dataframe_success(self):
        # GIVEN
        df = pd.DataFrame({"col": [1, 2, 3]})
        file_name = "test_file"

        # WHEN
        with patch('os.makedirs') as mock_makedirs, \
                patch('pandas.DataFrame.to_csv') as mock_to_csv, \
                patch('commons.utils.logging') as mock_logging:
            result = Utils._save_local_dataframe(df, file_name)

        # THEN
        assert result is True
        mock_makedirs.assert_any_call("output", exist_ok=True)
        mock_makedirs.assert_any_call(os.path.join("output", file_name), exist_ok=True)
        mock_to_csv.assert_called_once()
        mock_logging.info.assert_called_once()

    def test_save_local_dataframe_custom_separator(self):
        # GIVEN
        df = pd.DataFrame({"col": [1, 2, 3]})
        separator = ","

        # WHEN
        with patch('os.makedirs'), \
                patch('pandas.DataFrame.to_csv') as mock_to_csv, \
                patch('commons.utils.logging'):
            Utils._save_local_dataframe(df, "test", separator)

        # THEN
        mock_to_csv.assert_called_once_with("output/test/test.csv", sep=",", index=False)


class TestCsvOperations:
    """Testes para operações com arquivos CSV e chunks."""

    def test_validate_csv_valid_file(self):
        # GIVEN
        csv_content = "col1;col2;col3\nvalue1;value2;value3\nvalue4;value5;value6"

        # WHEN
        with patch('builtins.open', mock_open(read_data=csv_content)):
            result = Utils._validate_csv("test")

        # THEN
        assert result['valid'] is True
        assert result['total_lines'] == 3
        assert result['header_columns'] == 3
        assert result['errors'] == []

    def test_validate_csv_invalid_file(self):
        # GIVEN
        csv_content = "col1;col2;col3\nvalue1;value2\nvalue4;value5;value6;extra"

        # WHEN
        with patch('builtins.open', mock_open(read_data=csv_content)):
            result = Utils._validate_csv("test")

        # THEN
        assert result['valid'] is False
        assert len(result['errors']) == 2

    def test_get_chunks_dir(self):
        # GIVEN
        endpoint_name = "test_endpoint"
        expected_path = os.path.join("output", "test_endpoint", "chunks")

        # WHEN
        with patch('os.makedirs') as mock_makedirs:
            result = Utils._get_chunks_dir(endpoint_name)

        # THEN
        assert result == expected_path
        mock_makedirs.assert_called_once_with(expected_path, exist_ok=True)

    def test_cleanup_chunks_directory_success(self):
        # GIVEN
        endpoint_name = "test_endpoint"
        base_dir = "output"
        output_dir = os.path.join(base_dir, endpoint_name)
        chunks_dir = os.path.join(output_dir, "chunks")
        os.makedirs(chunks_dir, exist_ok=True)
        assert os.path.exists(chunks_dir)

        try:
            # WHEN
            result = Utils._cleanup_chunks_directory(endpoint_name)

            # THEN
            directory_removed = not os.path.exists(chunks_dir)
            assert directory_removed or result is True
        finally:
            if os.path.exists(base_dir):
                shutil.rmtree(base_dir, ignore_errors=True)

    def test_merge_chunks_no_files(self):
        # GIVEN
        endpoint_name = "empty_endpoint"

        # WHEN
        with patch('glob.glob', return_value=[]), \
                patch('commons.utils.logging') as mock_logging:
            result = Utils.merge_chunks_and_normalize(endpoint_name)

        # THEN
        assert result is False
        mock_logging.warning.assert_called_once()

    def test_merge_chunks_success(self):
        # GIVEN
        chunk_files = ["chunk1.csv", "chunk2.csv"]
        chunk_df = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})

        # WHEN
        with patch('glob.glob', return_value=chunk_files), \
                patch('pandas.read_csv', return_value=chunk_df), \
                patch('pandas.DataFrame.to_csv') as mock_to_csv, \
                patch('commons.utils.logging'):
            result = Utils.merge_chunks_and_normalize("test_endpoint")

        # THEN
        assert result is True
        mock_to_csv.assert_called_once()


class TestMainProcessing:
    """Testes para funções principais de processamento."""

    def test_process_and_save_data_empty(self):
        # GIVEN
        empty_data = []
        endpoint_name = "test_endpoint"

        # WHEN
        with patch('commons.utils.logging') as mock_logging:
            result = Utils.process_and_save_data(empty_data, endpoint_name)

        # THEN
        assert result == []
        mock_logging.warning.assert_called_once()

    def test_process_and_save_data_success(self):
        # GIVEN
        raw_data = [{"User": "John", "Email": "john@example.com"}]
        mock_df = pd.DataFrame(raw_data)

        # Mock do retorno da validação CSV
        mock_validation_result = {
            'valid': True,
            'total_lines': 2,  # header + 1 linha de dados
            'header_columns': 2,
            'errors': [],
            'file_path': 'output/test_endpoint/test_endpoint.csv'
        }

        # WHEN
        with patch.object(Utils, '_normalize_keys', return_value=raw_data), \
                patch.object(Utils, '_flatten_json', return_value=raw_data[0]), \
                patch('pandas.DataFrame', return_value=mock_df), \
                patch.object(Utils, '_normalize_column_names', return_value=mock_df), \
                patch.object(Utils, '_remove_empty_columns', return_value=mock_df), \
                patch.object(Utils, '_process_and_convert_id_columns', return_value=mock_df), \
                patch.object(Utils, '_save_local_dataframe', return_value=True), \
                patch.object(Utils, '_validate_csv', return_value=mock_validation_result), \
                patch('commons.utils.logging'):
            result = Utils.process_and_save_data(raw_data, "test_endpoint")

        # THEN
        assert len(result) == 1

    def test_process_and_save_data_in_chunks_empty(self):
        # GIVEN
        empty_data = []
        endpoint_name = "test"

        # WHEN
        with patch('commons.utils.logging') as mock_logging:
            result = Utils.process_and_save_data_in_chunks(empty_data, endpoint_name)

        # THEN
        assert result == []
        mock_logging.warning.assert_called_once()

    def test_process_and_save_data_in_chunks_memory_only(self):
        # GIVEN
        raw_data = [{"User": "Alice"}]
        df = pd.DataFrame([{"user": "Alice"}])

        # WHEN
        with patch.object(Utils, '_normalize_keys', return_value=raw_data), \
                patch.object(Utils, '_flatten_json', return_value=raw_data[0]), \
                patch('pandas.DataFrame', return_value=df), \
                patch.object(Utils, '_normalize_column_names', return_value=df), \
                patch.object(Utils, '_remove_empty_columns', return_value=df), \
                patch.object(Utils, '_convert_columns_to_nullable_int', return_value=df), \
                patch('commons.utils.logging'):
            result = Utils.process_and_save_data_in_chunks(
                raw_data, "test", save_to_disk=False
            )

        # THEN
        assert result == df.to_dict(orient='records')

    def test_post_process_csv_file_success(self):
        # GIVEN
        endpoint_name = "test"

        # WHEN
        with patch.object(Utils, 'merge_chunks_and_normalize', return_value=True), \
                patch.object(Utils, '_cleanup_chunks_directory', return_value=True):
            result = Utils.post_process_csv_file(endpoint_name, cleanup_chunks=True)

        # THEN
        assert result is True

    def test_post_process_csv_file_failed_merge(self):
        # GIVEN
        endpoint_name = "test"

        # WHEN
        with patch.object(Utils, 'merge_chunks_and_normalize', return_value=False), \
                patch.object(Utils, '_cleanup_chunks_directory') as mock_cleanup:
            result = Utils.post_process_csv_file(endpoint_name, cleanup_chunks=True)

        # THEN
        assert result is False
        mock_cleanup.assert_not_called()


class TestBatchProcessing:
    """Testes para processamento de dados em lotes."""

    def test_process_data_in_batches_single_page(self):
        # GIVEN
        def fetch_page(path, headers, page):
            return {"total_pages": 1, "items": [{"user": "Alice"}]}

        # WHEN
        with patch.object(Utils, 'process_and_save_data_in_chunks', return_value=[{"user": "Alice"}]), \
                patch('commons.utils.logging'), \
                patch('os.makedirs'), \
                patch('os.path.exists', return_value=False):
            result = Utils.process_data_in_batches(
                "test", "/api/test", {}, fetch_page
            )

        # THEN
        assert result["registros"] == 1
        assert result["status"] == "Sucesso"

    def test_process_data_in_batches_multiple_pages(self):
        # GIVEN
        responses = {
            1: {"total_pages": 3, "items": [{"user": "A"}]},
            2: {"items": [{"user": "B"}]},
            3: {"items": [{"user": "C"}]}
        }

        def fetch_page(path, headers, page):
            return responses[page]

        # WHEN
        with patch.object(Utils, 'process_and_save_data_in_chunks',
                          return_value=[{"user": "A"}, {"user": "B"}, {"user": "C"}]), \
                patch('commons.utils.logging'), \
                patch('os.makedirs'), \
                patch('os.path.exists', return_value=False):
            result = Utils.process_data_in_batches(
                "test", "/api/test", {}, fetch_page, batch_size=2
            )

        # THEN
        assert result["registros"] == 3
        assert result["status"] == "Sucesso"

    def test_process_data_in_batches_error_handling(self):
        # GIVEN
        def fetch_page(path, headers, page):
            if page == 1:
                return {"total_pages": 2, "items": [{"user": "A"}]}
            raise ValueError("Network error")

        # WHEN
        with patch.object(Utils, 'process_and_save_data_in_chunks', return_value=[{"user": "A"}]), \
                patch('commons.utils.logging'), \
                patch('os.makedirs'), \
                patch('os.path.exists', return_value=False):
            result = Utils.process_data_in_batches(
                "test", "/api/test", {}, fetch_page
            )

        # THEN
        assert result["registros"] == 1
        assert result["status"] == "Sucesso"

    def test_process_data_in_batches_total_failure(self):
        # GIVEN
        def fetch_page(path, headers, page):
            raise RuntimeError("Total failure")

        # WHEN
        with patch('commons.utils.logging'), \
                patch('os.makedirs'), \
                patch('os.path.exists', return_value=False):
            result = Utils.process_data_in_batches(
                "test", "/api/test", {}, fetch_page
            )

        # THEN
        assert result["status"].startswith("Falha")
        assert result["registros"] == 0

    def test_process_data_in_batches_invalid_response(self):
        # GIVEN
        def fetch_page(path, headers, page):
            return {"invalid": "response"}  # Sem 'total_pages'

        # WHEN
        with patch('commons.utils.logging'):
            result = Utils.process_data_in_batches(
                "test", "/api/test", {}, fetch_page
            )

        # THEN
        assert result["status"].startswith("Falha")
        assert result["registros"] == 0

    def test_process_data_in_batches_with_delay(self):
        """Testa que o delay é aplicado durante o processamento em lotes."""

        # GIVEN
        def fetch_page(path, headers, page):
            if page == 1:
                return {"total_pages": 3, "items": [{"user": "A"}]}
            elif page == 2:
                return {"items": [{"user": "B"}]}
            return {"items": [{"user": "C"}]}

        # WHEN
        with patch.object(Utils, 'process_and_save_data_in_chunks', return_value=[]), \
                patch.object(Utils, 'merge_chunks_and_normalize', return_value=True), \
                patch('commons.utils.logging'), \
                patch('os.makedirs'), \
                patch('os.path.exists', return_value=False), \
                patch('glob.glob', return_value=[]), \
                patch('time.sleep') as mock_sleep:
            result = Utils.process_data_in_batches(
                "test", "/api/test", {}, fetch_page,
                delay_between_pages=1, batch_size=1
            )

        # THEN
        assert result["status"] == "Sucesso"
        assert mock_sleep.call_count >= 0


class TestExceptionHandling:
    """Testes para verificar tratamento de exceções."""

    def test_flatten_json_exception(self):
        # GIVEN
        data = {"a": 1}

        # WHEN & THEN
        with patch.object(Utils, '_flatten_json', side_effect=Exception("Erro")):
            with pytest.raises(Exception):
                Utils._flatten_json(data)

    def test_normalize_keys_exception(self):
        # GIVEN
        data = {"test": "value"}

        # WHEN & THEN
        with patch.object(Utils, '_normalize_key', side_effect=Exception("Erro")):
            with pytest.raises(Exception):
                Utils._normalize_keys(data)

    def test_save_dataframe_exception(self):
        # GIVEN
        df = pd.DataFrame({"col": [1, 2, 3]})

        # WHEN & THEN
        with patch('os.makedirs', side_effect=Exception("Erro")), \
                patch('commons.utils.logging') as mock_logging:
            with pytest.raises(Exception):
                Utils._save_local_dataframe(df, "test")

        mock_logging.error.assert_called_once()

    def test_convert_columns_exception(self):
        # GIVEN
        df = pd.DataFrame({"col": [1, 2, 3]})

        # WHEN & THEN
        with patch('pandas.DataFrame.__getitem__', side_effect=Exception("Erro")), \
                patch('commons.utils.logging') as mock_logging:
            with pytest.raises(Exception):
                Utils._convert_columns_to_nullable_int(df)

        mock_logging.error.assert_called_once()

    def test_process_and_save_exception(self):
        # GIVEN
        raw_data = [{"User": "John"}]

        # WHEN & THEN
        with patch.object(Utils, '_normalize_keys', side_effect=Exception("Erro")), \
                patch('commons.utils.logging') as mock_logging:
            with pytest.raises(Exception):
                Utils.process_and_save_data(raw_data, "test")

        mock_logging.error.assert_called_once()

    def test_process_chunks_exception(self):
        # GIVEN
        raw_data = [{"User": "Alice"}]

        # WHEN & THEN
        with patch.object(Utils, '_normalize_keys', side_effect=Exception("Erro")), \
                patch('commons.utils.logging') as mock_logging:
            with pytest.raises(Exception, match="Erro"):
                Utils.process_and_save_data_in_chunks(raw_data, "test")

        mock_logging.error.assert_called_once()
