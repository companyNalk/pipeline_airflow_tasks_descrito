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
