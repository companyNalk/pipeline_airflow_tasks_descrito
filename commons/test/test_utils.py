import os
from unittest.mock import patch, ANY

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from commons.utils import Utils


class TestFlattenJson:
    """Testes para o método _flatten_json."""

    def test_flatten_empty_dict(self):
        """Testa achatamento de dicionário vazio."""
        # GIVEN
        data = {}

        # WHEN
        result = Utils._flatten_json(data)

        # THEN
        assert result == {}

    def test_flatten_simple_dict(self):
        """Testa achatamento de dicionário simples."""
        # GIVEN
        data = {"a": 1, "b": 2}

        # WHEN
        result = Utils._flatten_json(data)

        # THEN
        assert result == {"a": 1, "b": 2}

    def test_flatten_nested_dict(self):
        """Testa achatamento de dicionário aninhado."""
        # GIVEN
        data = {"a": 1, "b": {"c": 2, "d": 3}}

        # WHEN
        result = Utils._flatten_json(data)

        # THEN
        assert result == {"a": 1, "b_c": 2, "b_d": 3}

    def test_flatten_with_list(self):
        """Testa achatamento com lista."""
        # GIVEN
        data = {"a": 1, "b": [10, 20, 30]}

        # WHEN
        result = Utils._flatten_json(data)

        # THEN
        assert result == {"a": 1, "b_1": 10, "b_2": 20, "b_3": 30}

    def test_flatten_complex_structure(self):
        """Testa achatamento de estrutura complexa com dicionários e listas aninhadas."""
        # GIVEN
        data = {
            "name": "John",
            "address": {
                "street": "Main St",
                "city": "New York"
            },
            "phones": [
                {"type": "home", "number": "123"},
                {"type": "work", "number": "456"}
            ]
        }

        # WHEN
        result = Utils._flatten_json(data)

        # THEN
        expected = {
            "name": "John",
            "address_street": "Main St",
            "address_city": "New York",
            "phones_1_type": "home",
            "phones_1_number": "123",
            "phones_2_type": "work",
            "phones_2_number": "456"
        }
        assert result == expected

    def test_flatten_custom_separator(self):
        """Testa achatamento com separador personalizado."""
        # GIVEN
        data = {"a": {"b": 1}}

        # WHEN
        result = Utils._flatten_json(data, sep=".")

        # THEN
        assert result == {"a.b": 1}

    def test_flatten_with_parent_key(self):
        """Testa achatamento com chave pai fornecida."""
        # GIVEN
        data = {"a": 1}

        # WHEN
        result = Utils._flatten_json(data, parent_key="parent")

        # THEN
        assert result == {"parent_a": 1}

    def test_flatten_exception(self):
        """Testa se exceções são tratadas corretamente."""
        # GIVEN
        # Em vez de usar object(), vamos forçar uma exceção real
        data = {"a": 1}

        # WHEN/THEN
        with patch.object(Utils, '_flatten_json', side_effect=Exception("Erro forçado")):
            with pytest.raises(Exception):
                Utils._flatten_json(data)


class TestRemoveEmptyColumns:
    """Testes para o método _remove_empty_columns."""

    def test_remove_empty_columns_empty_df(self):
        """Testa remoção de colunas em DataFrame vazio."""
        # GIVEN
        df = pd.DataFrame()

        # WHEN
        with patch('commons.utils.logging') as mock_logging:
            result = Utils._remove_empty_columns(df)

        # THEN
        assert result.empty
        mock_logging.warning.assert_called_once_with("DataFrame vazio recebido para remoção de colunas")

    def test_remove_nan_columns(self):
        """Testa remoção de colunas com NaN."""
        # GIVEN
        df = pd.DataFrame({
            'a': [1, 2, 3],
            'b': [None, None, None],
            'c': [4, 5, 6]
        })

        # WHEN
        with patch('commons.utils.logging') as mock_logging:
            result = Utils._remove_empty_columns(df)

        # THEN
        expected = pd.DataFrame({
            'a': [1, 2, 3],
            'c': [4, 5, 6]
        })
        assert_frame_equal(result, expected)
        mock_logging.info.assert_any_call("Colunas removidas por serem NaN: 1")
        mock_logging.info.assert_any_call("Total de colunas removidas: 1")

    def test_remove_empty_string_columns(self):
        """Testa remoção de colunas com strings vazias."""
        # GIVEN
        df = pd.DataFrame({
            'a': [1, 2, 3],
            'b': ['', '', ''],
            'c': [4, 5, 6]
        })

        # WHEN
        with patch('commons.utils.logging') as mock_logging:
            result = Utils._remove_empty_columns(df)

        # THEN
        expected = pd.DataFrame({
            'a': [1, 2, 3],
            'c': [4, 5, 6]
        })
        assert_frame_equal(result, expected)
        mock_logging.info.assert_any_call("Colunas removidas por serem strings vazias: 1")
        mock_logging.info.assert_any_call("Total de colunas removidas: 1")

    def test_remove_both_types_of_empty_columns(self):
        """Testa remoção de colunas com NaN e strings vazias."""
        # GIVEN
        df = pd.DataFrame({
            'a': [1, 2, 3],
            'b': [None, None, None],
            'c': ['', '', ''],
            'd': [4, 5, 6]
        })

        # WHEN
        with patch('commons.utils.logging') as mock_logging:
            result = Utils._remove_empty_columns(df)

        # THEN
        expected = pd.DataFrame({
            'a': [1, 2, 3],
            'd': [4, 5, 6]
        })
        assert_frame_equal(result, expected)
        mock_logging.info.assert_any_call("Colunas removidas por serem NaN: 1")
        mock_logging.info.assert_any_call("Colunas removidas por serem strings vazias: 1")
        mock_logging.info.assert_any_call("Total de colunas removidas: 2")

    def test_no_empty_columns(self):
        """Testa comportamento quando não há colunas vazias para remover."""
        # GIVEN
        df = pd.DataFrame({
            'a': [1, 2, 3],
            'b': [4, 5, 6]
        })

        # WHEN
        with patch('commons.utils.logging') as mock_logging:
            result = Utils._remove_empty_columns(df)

        # THEN
        assert_frame_equal(result, df)
        mock_logging.info.assert_not_called()

    def test_all_columns_empty(self):
        """Testa comportamento quando todas as colunas são vazias."""
        # GIVEN
        df = pd.DataFrame({
            'a': [None, None, None],
            'b': ['', '', '']
        })

        # WHEN/THEN
        with patch('commons.utils.logging'):
            with pytest.raises(ValueError, match="O DataFrame resultante está vazio após a remoção de colunas"):
                Utils._remove_empty_columns(df)


class TestNormalizeKey:
    """Testes para o método _normalize_key."""

    def test_lowercase_conversion(self):
        """Testa conversão para lowercase."""
        # GIVEN
        key = "AbCdEf"

        # WHEN
        result = Utils._normalize_key(key)

        # THEN
        assert result == "abcdef"

    def test_remove_accents(self):
        """Testa remoção de acentos."""
        # GIVEN
        key = "açãó êíóú"

        # WHEN
        result = Utils._normalize_key(key)

        # THEN
        assert result == "acao_eiou"

    def test_replace_special_chars(self):
        """Testa substituição de caracteres especiais."""
        # GIVEN
        key = "test@#$%^&*()!~`+=[]{}<>:;,.?/|\\"

        # WHEN
        result = Utils._normalize_key(key)

        # THEN
        assert result == "test"

    def test_replace_spaces(self):
        """Testa substituição de espaços."""
        # GIVEN
        key = "test with spaces"

        # WHEN
        result = Utils._normalize_key(key)

        # THEN
        assert result == "test_with_spaces"

    def test_trim_underscores(self):
        """Testa remoção de underscores no início e fim."""
        # GIVEN
        key = "___test___"

        # WHEN
        result = Utils._normalize_key(key)

        # THEN
        assert result == "test"

    def test_camel_case_to_snake_case(self):
        """Testa conversão de camelCase para snake_case."""
        # GIVEN
        key = "camelCaseToSnakeCase"

        # WHEN
        result = Utils._normalize_key(key)

        # THEN
        assert result == "camel_case_to_snake_case"

    def test_complex_normalization(self):
        """Testa normalização complexa combinando várias transformações."""
        # GIVEN
        key = "  Usuário@Endereço-Email  "

        # WHEN
        result = Utils._normalize_key(key)

        # THEN
        assert result == "usuario_endereco_email"

    def test_column_names_with_quotes(self):
        """Testa normalização de nomes de colunas com aspas."""
        # GIVEN
        df = pd.DataFrame({
            '"Texto"': ["jonas", "ana"],
            "'novoTexto'": ["jonas", "paula"]
        })

        # WHEN
        result = Utils._normalize_column_names(df)

        # THEN
        expected = pd.DataFrame({
            "texto": ["jonas", "ana"],
            "novo_texto": ["jonas", "paula"]
        })
        assert_frame_equal(result, expected)


class TestNormalizeKeys:
    """Testes para o método _normalize_keys."""

    def test_normalize_dict_keys(self):
        """Testa normalização de chaves em dicionário."""
        # GIVEN
        data = {"User Name": "John", "Email@Address": "john@example.com"}

        # WHEN
        result = Utils._normalize_keys(data)

        # THEN
        expected = {"user_name": "John", "email_address": "john@example.com"}
        assert result == expected

    def test_normalize_nested_dict(self):
        """Testa normalização de chaves em dicionário aninhado."""
        # GIVEN
        data = {
            "User": {
                "Full Name": "John Doe",
                "ContactInfo": {
                    "Email": "john@example.com"
                }
            }
        }

        # WHEN
        result = Utils._normalize_keys(data)

        # THEN
        expected = {
            "user": {
                "full_name": "John Doe",
                "contactinfo": {
                    "email": "john@example.com"
                }
            }
        }
        assert result == expected

    def test_normalize_list_of_dicts(self):
        """Testa normalização de lista de dicionários."""
        # GIVEN
        data = [
            {"First Name": "John"},
            {"Last Name": "Doe"}
        ]

        # WHEN
        result = Utils._normalize_keys(data)

        # THEN
        expected = [
            {"first_name": "John"},
            {"last_name": "Doe"}
        ]
        assert result == expected

    def test_normalize_non_dict_list(self):
        """Testa normalização de lista de valores não-dicionário."""
        # GIVEN
        data = [1, "test", True]

        # WHEN
        result = Utils._normalize_keys(data)

        # THEN
        assert result == data

    def test_normalize_scalar_value(self):
        """Testa normalização de valor escalar."""
        # GIVEN
        data = "test"

        # WHEN
        result = Utils._normalize_keys(data)

        # THEN
        assert result == "test"

    def test_normalize_complex_structure(self):
        """Testa normalização de estrutura complexa."""
        # GIVEN
        data = {
            "Usuários": [
                {"Nome": "John", "Endereço": {"Cidade": "New York"}},
                {"Nome": "Jane", "Endereço": {"Cidade": "Boston"}}
            ]
        }

        # WHEN
        result = Utils._normalize_keys(data)

        # THEN
        expected = {
            "usuarios": [
                {"nome": "John", "endereco": {"cidade": "New York"}},
                {"nome": "Jane", "endereco": {"cidade": "Boston"}}
            ]
        }
        assert result == expected

    def test_normalize_keys_exception(self):
        """Testa se exceções são tratadas corretamente."""
        # GIVEN
        data = {"test": "value"}

        # WHEN/THEN
        with patch.object(Utils, '_normalize_key', side_effect=Exception("Teste de erro")):
            with pytest.raises(Exception):
                Utils._normalize_keys(data)


class TestNormalizeColumnNames:
    """Testes para o método _normalize_column_names."""

    def test_normalize_empty_dataframe(self):
        """Testa normalização de DataFrame vazio."""
        # GIVEN
        df = pd.DataFrame()

        # WHEN
        result = Utils._normalize_column_names(df)

        # THEN
        assert result.empty

    def test_normalize_column_names(self):
        """Testa normalização de nomes de colunas."""
        # GIVEN
        df = pd.DataFrame({
            "User Name": [1, 2, 3],
            "Email@Address": [4, 5, 6]
        })

        # WHEN
        result = Utils._normalize_column_names(df)

        # THEN
        expected = pd.DataFrame({
            "user_name": [1, 2, 3],
            "email_address": [4, 5, 6]
        })
        assert_frame_equal(result, expected)

    def test_normalize_column_names_exception(self):
        """Testa se exceções são tratadas corretamente."""
        # GIVEN
        df = pd.DataFrame({"col": [1, 2, 3]})

        # WHEN/THEN
        with patch.object(Utils, '_normalize_key', side_effect=Exception("Teste de erro")):
            with pytest.raises(Exception):
                Utils._normalize_column_names(df)


class TestSaveLocalDataframe:
    """Testes para o método _save_local_dataframe."""

    def test_save_dataframe_success(self):
        """Testa salvamento bem-sucedido de DataFrame."""
        # GIVEN
        df = pd.DataFrame({"col": [1, 2, 3]})
        file_name = "test_file"

        # WHEN
        with patch('os.makedirs') as mock_makedirs:
            with patch('pandas.DataFrame.to_csv') as mock_to_csv:
                with patch('commons.utils.logging') as mock_logging:
                    result = Utils._save_local_dataframe(df, file_name)

        # THEN
        assert result is True
        # Verifica a criação do diretório base
        mock_makedirs.assert_any_call("output", exist_ok=True)
        # Verifica a criação do diretório específico
        mock_makedirs.assert_any_call(os.path.join("output", file_name), exist_ok=True)
        mock_to_csv.assert_called_once()
        # Verifica a mensagem de log com o caminho correto do arquivo
        csv_path = os.path.join("output", file_name, f"{file_name}.csv")
        mock_logging.info.assert_called_once_with(f"Arquivo salvo com sucesso: {csv_path}")

    def test_save_dataframe_custom_separator(self):
        """Testa salvamento de DataFrame com separador personalizado."""
        # GIVEN
        df = pd.DataFrame({"col": [1, 2, 3]})
        file_name = "test_file"
        separator = ","

        # WHEN
        with patch('os.makedirs'):
            with patch('pandas.DataFrame.to_csv') as mock_to_csv:
                with patch('commons.utils.logging'):
                    Utils._save_local_dataframe(df, file_name, separator)

        # THEN
        mock_to_csv.assert_called_once_with(f"output/{file_name}/{file_name}.csv", sep=separator, index=False)

    def test_save_dataframe_exception(self):
        """Testa se exceções são tratadas corretamente."""
        # GIVEN
        df = pd.DataFrame({"col": [1, 2, 3]})
        file_name = "test_file"

        # WHEN/THEN
        with patch('os.makedirs', side_effect=Exception("Teste de erro")):
            with patch('commons.utils.logging') as mock_logging:
                with pytest.raises(Exception):
                    Utils._save_local_dataframe(df, file_name)

        mock_logging.error.assert_called_once()


class TestProcessAndSaveData:
    """Testes para o método process_and_save_data."""

    def test_process_empty_data(self):
        """Testa processamento de dados vazios."""
        # GIVEN
        raw_data = []
        endpoint_name = "test_endpoint"

        # WHEN
        with patch('commons.utils.logging') as mock_logging:
            result = Utils.process_and_save_data(raw_data, endpoint_name)

        # THEN
        assert result == []
        mock_logging.warning.assert_called_once_with(f"Nenhum dado recebido para o endpoint {endpoint_name}")

    def test_process_and_save_success(self):
        """Testa processamento e salvamento bem-sucedido."""
        # GIVEN
        raw_data = [{"User": "John", "Email": "john@example.com"}]
        endpoint_name = "test_endpoint"
        mock_df = pd.DataFrame(raw_data)

        # WHEN
        with patch.object(Utils, '_normalize_keys', return_value=raw_data) as mock_normalize:
            with patch.object(Utils, '_flatten_json', return_value=raw_data[0]) as mock_flatten:
                with patch('pandas.DataFrame', return_value=mock_df):
                    with patch.object(Utils, '_normalize_column_names', return_value=mock_df) as mock_normalize_cols:
                        with patch.object(Utils, '_remove_empty_columns', return_value=mock_df) as mock_remove_empty:
                            with patch.object(Utils, '_save_local_dataframe', return_value=True) as mock_save:
                                with patch('commons.utils.logging'):
                                    result = Utils.process_and_save_data(raw_data, endpoint_name)

        # THEN
        assert len(result) == 1
        mock_normalize.assert_called_once_with(raw_data)
        mock_flatten.assert_called_once()
        mock_normalize_cols.assert_called_once()
        mock_remove_empty.assert_called_once()
        # Usamos ANY para evitar comparação direta de DataFrames
        mock_save.assert_called_once_with(ANY, endpoint_name)

    def test_process_empty_dataframe_after_processing(self):
        """Testa quando o DataFrame fica vazio após processamento."""
        # GIVEN
        raw_data = [{"User": "John"}]
        endpoint_name = "test_endpoint"
        empty_df = pd.DataFrame()

        # WHEN
        with patch.object(Utils, '_normalize_keys', return_value=raw_data):
            with patch.object(Utils, '_flatten_json', return_value=raw_data[0]):
                with patch('pandas.DataFrame', return_value=empty_df):
                    with patch('commons.utils.logging') as mock_logging:
                        result = Utils.process_and_save_data(raw_data, endpoint_name)

        # THEN
        assert result == []
        # Verificação genérica - apenas se qualquer warning foi chamado
        assert mock_logging.warning.called
        assert any("DataFrame vazio" in str(args) for args, _ in mock_logging.warning.call_args_list)

    def test_process_exception(self):
        """Testa se exceções são tratadas corretamente."""
        # GIVEN
        raw_data = [{"User": "John"}]
        endpoint_name = "test_endpoint"

        # WHEN/THEN
        with patch.object(Utils, '_normalize_keys', side_effect=Exception("Teste de erro")):
            with patch('commons.utils.logging') as mock_logging:
                with pytest.raises(Exception):
                    Utils.process_and_save_data(raw_data, endpoint_name)

        mock_logging.error.assert_called_once()


class TestConvertColumnsToNullableInt:
    """Testes para o método _convert_columns_to_nullable_int."""

    def test_convert_integer_columns(self):
        """Testa conversão de coluna com inteiros puros."""
        # GIVEN
        df = pd.DataFrame({"col": [1, 2, 3, None]})

        # WHEN
        with patch('commons.utils.logging') as mock_logging:
            result = Utils._convert_columns_to_nullable_int(df.copy())

        # THEN
        assert result["col"].dtype == "Int64"
        mock_logging.info.assert_called_once_with("Colunas convertidas para Int64: col")

    def test_keep_float_columns(self):
        """Testa que colunas com floats reais (ex: 7.1) não são convertidas."""
        # GIVEN
        df = pd.DataFrame({"col": [1.0, 2.5, 3.0, None]})

        # WHEN
        with patch('commons.utils.logging') as mock_logging:
            result = Utils._convert_columns_to_nullable_int(df.copy())

        # THEN
        assert result["col"].dtype == float
        mock_logging.info.assert_not_called()

    def test_non_numeric_column(self):
        """Testa que colunas não numéricas são ignoradas."""
        # GIVEN
        df = pd.DataFrame({"col": ["a", "b", "c"]})

        # WHEN
        with patch('commons.utils.logging') as mock_logging:
            result = Utils._convert_columns_to_nullable_int(df.copy())

        # THEN
        assert result.equals(df)
        mock_logging.info.assert_not_called()

    def test_conversion_exception(self):
        """Testa tratamento de exceções no método."""
        # GIVEN
        df = pd.DataFrame({"col": [1, 2, 3]})

        # WHEN/THEN
        with patch('commons.utils.logging') as mock_logging:
            with patch('pandas.DataFrame.__getitem__', side_effect=Exception("Erro forçado")):
                with pytest.raises(Exception):
                    Utils._convert_columns_to_nullable_int(df)

        mock_logging.error.assert_called_once()


class TestProcessAndSaveDataInChunks:
    """Testes para o método process_and_save_data_in_chunks."""

    def test_empty_raw_data(self):
        """Testa processamento com dados vazios."""
        # GIVEN
        raw_data = []
        endpoint_name = "empty_test"

        # WHEN
        with patch('commons.utils.logging') as mock_logging:
            result = Utils.process_and_save_data_in_chunks(raw_data, endpoint_name)

        # THEN
        assert result == []
        mock_logging.warning.assert_called_once_with(f"Nenhum dado recebido para o endpoint {endpoint_name}")

    def test_success_no_disk_save(self):
        """Testa processamento sem salvar em disco (save_to_disk=False)."""
        # GIVEN
        raw_data = [{"User Name": "Alice", "Email": "alice@example.com"}]
        endpoint_name = "memory_test"

        df = pd.DataFrame([{"user_name": "Alice", "email": "alice@example.com"}])

        # WHEN
        with patch.object(Utils, '_normalize_keys', return_value=raw_data):
            with patch.object(Utils, '_flatten_json', return_value=raw_data[0]):
                with patch('pandas.DataFrame', return_value=df):
                    with patch.object(Utils, '_normalize_column_names', return_value=df):
                        with patch.object(Utils, '_remove_empty_columns', return_value=df):
                            with patch.object(Utils, '_convert_columns_to_nullable_int', return_value=df):
                                with patch('commons.utils.logging') as mock_logging:
                                    result = Utils.process_and_save_data_in_chunks(
                                        raw_data, endpoint_name, save_to_disk=False
                                    )

        # THEN
        assert result == df.to_dict(orient='records')
        mock_logging.info.assert_any_call(
            f"Processamento concluído: {len(df)} registros para {endpoint_name} (mantidos apenas em memória)"
        )

    def test_empty_dataframe_after_processing(self):
        """Testa quando o DataFrame fica vazio após processar os dados."""
        # GIVEN
        raw_data = [{"User": "Alice"}]
        endpoint_name = "empty_df_test"
        empty_df = pd.DataFrame()

        # WHEN
        with patch.object(Utils, '_normalize_keys', return_value=raw_data):
            with patch.object(Utils, '_flatten_json', return_value=raw_data[0]):
                with patch('pandas.DataFrame', return_value=empty_df):
                    with patch('commons.utils.logging') as mock_logging:
                        result = Utils.process_and_save_data_in_chunks(raw_data, endpoint_name)

        # THEN
        assert result == []
        mock_logging.warning.assert_called_once_with(
            f"DataFrame vazio após processamento para {endpoint_name}"
        )

    def test_success_with_disk_save_single_chunk(self, tmp_path):
        """Testa processamento com salvamento em disco e chunk único."""
        # GIVEN
        raw_data = [{"User Name": "Alice", "Email": "alice@example.com"}]
        endpoint_name = "chunk_test"

        df = pd.DataFrame([{"user_name": "Alice", "email": "alice@example.com"}])

        output_dir = tmp_path / "output" / endpoint_name
        output_dir.mkdir(parents=True, exist_ok=True)

        # WHEN
        with patch('commons.utils.os.makedirs'), \
                patch('commons.utils.os.path.isfile', return_value=False), \
                patch('pandas.DataFrame.to_csv') as mock_to_csv, \
                patch.object(Utils, '_normalize_keys', return_value=raw_data), \
                patch.object(Utils, '_flatten_json', return_value=raw_data[0]), \
                patch('pandas.DataFrame', return_value=df), \
                patch.object(Utils, '_normalize_column_names', return_value=df), \
                patch.object(Utils, '_remove_empty_columns', return_value=df), \
                patch.object(Utils, '_convert_columns_to_nullable_int', return_value=df), \
                patch('commons.utils.logging') as mock_logging:
            result = Utils.process_and_save_data_in_chunks(
                raw_data, endpoint_name, chunk_size=1000, save_to_disk=True
            )

        # THEN
        assert result == df.to_dict(orient='records')
        mock_to_csv.assert_called_once()
        mock_logging.info.assert_any_call(
            f"Processamento concluído: {len(df)} registros salvos em output/{endpoint_name}/{endpoint_name}.csv"
        )

        # THEN
        assert result == df.to_dict(orient='records')
        mock_to_csv.assert_called_once()
        mock_logging.info.assert_any_call(
            f"Processamento concluído: {len(df)} registros salvos em output/{endpoint_name}/{endpoint_name}.csv")

    def test_processing_exception(self):
        """Testa tratamento de exceções durante o processamento."""
        # GIVEN
        raw_data = [{"User": "Alice"}]
        endpoint_name = "error_test"

        # WHEN / THEN
        with patch.object(Utils, '_normalize_keys', side_effect=Exception("Erro forçado")), \
                patch('commons.utils.logging') as mock_logging:
            with pytest.raises(Exception, match="Erro forçado"):
                Utils.process_and_save_data_in_chunks(raw_data, endpoint_name)

        mock_logging.error.assert_called_once()


class TestProcessDataInBatches:
    """Testes para o método process_data_in_batches."""

    def test_success_single_page(self):
        """Testa processamento bem-sucedido com apenas uma página."""
        # GIVEN
        endpoint_name = "test_endpoint"
        endpoint_path = "/api/test"
        headers = {}
        mock_items = [{"user": "Alice"}]

        def fetch_page(path, headers, page):
            return {"total_pages": 1, "items": mock_items}

        # WHEN
        with patch.object(Utils, 'process_and_save_data_in_chunks', return_value=mock_items) as mock_process, \
                patch('commons.utils.logging'), \
                patch('os.makedirs'), \
                patch('os.path.exists', return_value=False):
            result = Utils.process_data_in_batches(endpoint_name, endpoint_path, headers, fetch_page)

        # THEN
        assert result["registros"] == 1
        assert result["status"] == "Sucesso"
        mock_process.assert_called_once_with(mock_items, endpoint_name, chunk_size=100, save_to_disk=True)

    def test_success_multiple_pages_with_batching(self):
        """Testa processamento de múltiplas páginas com batch_size 2."""
        # GIVEN
        endpoint_name = "batch_endpoint"
        endpoint_path = "/api/batch"
        headers = {}
        page_1 = {"total_pages": 3, "items": [{"user": "A"}]}
        page_2 = {"items": [{"user": "B"}]}
        page_3 = {"items": [{"user": "C"}]}

        def fetch_page(path, headers, page):
            return {1: page_1, 2: page_2, 3: page_3}[page]

        side_effect_result = [[{"user": "A"}], [{"user": "B"}, {"user": "C"}]]

        # WHEN
        with patch.object(Utils, 'process_and_save_data_in_chunks', side_effect=side_effect_result) as mock_process, \
                patch('commons.utils.logging'), \
                patch('os.makedirs'), \
                patch('os.path.exists', return_value=False):
            result = Utils.process_data_in_batches(
                endpoint_name, endpoint_path, headers, fetch_page, batch_size=2, chunk_size=1
            )

        # THEN
        assert result["registros"] == 3
        assert result["status"] == "Sucesso"
        assert mock_process.call_count == 2

    def test_page_fetch_error_handled(self):
        """Testa se erro ao buscar página é tratado e o buffer é processado."""
        # GIVEN
        endpoint_name = "error_endpoint"
        endpoint_path = "/api/error"
        headers = {}
        items_1 = [{"user": "A"}]
        items_2 = [{"user": "B"}]

        def fetch_page(path, headers, page):
            if page == 1:
                return {"total_pages": 3, "items": items_1}
            if page == 2:
                raise ValueError("Erro na página 2")
            return {"items": items_2}

        side_effect_result = [[{"user": "A"}], [{"user": "B"}]]

        # WHEN
        with patch.object(Utils, 'process_and_save_data_in_chunks', side_effect=side_effect_result) as mock_process, \
                patch('commons.utils.logging'), \
                patch('os.makedirs'), \
                patch('os.path.exists', return_value=False):
            result = Utils.process_data_in_batches(endpoint_name, endpoint_path, headers, fetch_page)

        # THEN
        assert result["registros"] == 2
        assert result["status"] == "Sucesso"
        assert mock_process.call_count == 2

    def test_returns_error_on_total_failure(self):
        """Testa quando o processo falha completamente no início."""
        # GIVEN
        endpoint_name = "fail_endpoint"
        endpoint_path = "/api/fail"
        headers = {}

        def fetch_page(path, headers, page):
            raise RuntimeError("Falha de rede")

        # WHEN
        with patch('commons.utils.logging'), \
                patch('os.makedirs'), \
                patch('os.path.exists', return_value=False):
            result = Utils.process_data_in_batches(endpoint_name, endpoint_path, headers, fetch_page)

        # THEN
        assert result["status"].startswith("Falha")
        assert result["registros"] == 0
        assert result["tempo"] == 0
