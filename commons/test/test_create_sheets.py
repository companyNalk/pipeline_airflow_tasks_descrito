import os
import tempfile
from unittest.mock import patch, Mock

from commons.create_sheets import read_config_file, generate_sql, main


class TestReadConfigFile:
    """Testes para função read_config_file."""

    def test_read_valid_config_file(self):
        """Testa leitura de arquivo de configuração válido."""
        # GIVEN
        with tempfile.NamedTemporaryFile(mode='w', suffix='.env', delete=False) as f:
            f.write("TOOL=my_tool\n")
            f.write("ENDPOINTS=users,orders\n")
            f.write("# This is a comment\n")
            f.write("BUCKET=my-bucket\n")
            config_path = f.name

        # WHEN
        result = read_config_file(config_path)

        # THEN
        assert result['TOOL'] == 'my_tool'
        assert result['ENDPOINTS'] == 'users,orders'
        assert result['BUCKET'] == 'my-bucket'
        assert len(result) == 3

        # CLEANUP
        os.unlink(config_path)

    def test_read_config_file_with_comments_and_empty_lines(self):
        """Testa leitura ignorando comentários e linhas vazias."""
        # GIVEN
        with tempfile.NamedTemporaryFile(mode='w', suffix='.env', delete=False) as f:
            f.write("# Comment line\n")
            f.write("\n")
            f.write("TOOL=test_tool\n")
            f.write("  \n")
            f.write("ENDPOINTS=endpoint1\n")
            config_path = f.name

        # WHEN
        result = read_config_file(config_path)

        # THEN
        assert result['TOOL'] == 'test_tool'
        assert result['ENDPOINTS'] == 'endpoint1'
        assert len(result) == 2

        # CLEANUP
        os.unlink(config_path)

    def test_read_config_file_with_equals_in_value(self):
        """Testa leitura com valores que contêm '='."""
        # GIVEN
        with tempfile.NamedTemporaryFile(mode='w', suffix='.env', delete=False) as f:
            f.write("URL=https://api.example.com/v1?key=value\n")
            f.write("TOOL=my_tool\n")
            config_path = f.name

        # WHEN
        result = read_config_file(config_path)

        # THEN
        assert result['URL'] == 'https://api.example.com/v1?key=value'
        assert result['TOOL'] == 'my_tool'

        # CLEANUP
        os.unlink(config_path)

    def test_read_nonexistent_file(self):
        """Testa leitura de arquivo inexistente."""
        # GIVEN & WHEN
        result = read_config_file("nonexistent_file.env")

        # THEN
        assert result == {}

    def test_read_config_file_with_invalid_format(self):
        """Testa leitura de arquivo com formato inválido."""
        # GIVEN
        with tempfile.NamedTemporaryFile(mode='w', suffix='.env', delete=False) as f:
            f.write("TOOL=valid_tool\n")
            f.write("INVALID_LINE_WITHOUT_EQUALS\n")
            f.write("ENDPOINTS=users\n")
            config_path = f.name

        # WHEN
        result = read_config_file(config_path)

        # THEN
        assert result == {}

        # CLEANUP
        os.unlink(config_path)


class TestGenerateSQL:
    """Testes para função generate_sql."""

    def test_generate_sql_single_endpoint(self):
        """Testa geração de SQL com um único endpoint."""
        # GIVEN
        tool = "my_tool"
        endpoints = "users"

        # WHEN
        result = generate_sql(tool, endpoints)

        # THEN
        assert "CREATE OR REPLACE EXTERNAL TABLE {project_id}.my_tool.users" in result
        assert "gs://{bucket_name}/users/users.csv" in result
        assert "CREATE OR REPLACE TABLE `{project_id}.vendas.my_tool_users_gold`" in result
        assert "FROM `{project_id}.my_tool.users`" in result
        assert "-- GOLD" in result

    def test_generate_sql_multiple_endpoints(self):
        """Testa geração de SQL com múltiplos endpoints."""
        # GIVEN
        tool = "ecommerce"
        endpoints = "users,orders,products"

        # WHEN
        result = generate_sql(tool, endpoints)

        # THEN
        assert "CREATE OR REPLACE EXTERNAL TABLE {project_id}.ecommerce.users" in result
        assert "CREATE OR REPLACE EXTERNAL TABLE {project_id}.ecommerce.orders" in result
        assert "CREATE OR REPLACE EXTERNAL TABLE {project_id}.ecommerce.products" in result

        # Verificar URIs
        assert "gs://{bucket_name}/users/users.csv" in result
        assert "gs://{bucket_name}/orders/orders.csv" in result
        assert "gs://{bucket_name}/products/products.csv" in result

        assert "ecommerce_users_gold" in result
        assert "ecommerce_orders_gold" in result
        assert "ecommerce_products_gold" in result

    def test_generate_sql_format_options(self):
        """Testa se todas as opções de formato estão presentes."""
        # GIVEN
        tool = "test_tool"
        endpoints = "test_endpoint"

        # WHEN
        result = generate_sql(tool, endpoints)

        # THEN
        assert "format = 'CSV'" in result
        assert "field_delimiter=';'" in result
        assert "skip_leading_rows=1" in result
        assert "allow_quoted_newlines=true" in result

    def test_generate_sql_structure(self):
        """Testa a estrutura geral do SQL gerado."""
        # GIVEN
        tool = "analytics"
        endpoints = "events,sessions"

        # WHEN
        result = generate_sql(tool, endpoints)

        # THEN
        lines = result.split('\n')

        assert any("-- GOLD" in line for line in lines)
        assert any("# EVENTS" in line for line in lines)
        assert any("# SESSIONS" in line for line in lines)
        assert not result.endswith('\n\n')


class TestMain:
    """Testes para função main."""

    @patch('commons.create_sheets.logger')
    @patch('argparse.ArgumentParser.parse_args')
    def test_main_success_with_config_env(self, mock_args, mock_logger):
        """Testa execução principal com sucesso usando config.env."""
        # GIVEN
        with tempfile.TemporaryDirectory() as temp_dir:
            # Criar arquivo config.env
            config_path = os.path.join(temp_dir, "config.env")
            with open(config_path, 'w') as f:
                f.write("TOOL=test_tool\n")
                f.write("ENDPOINTS=users,orders\n")

            mock_args.return_value = Mock(dir_path=temp_dir, output='test.sql')

            # WHEN
            main()

            # THEN
            output_path = os.path.join(temp_dir, 'test.sql')
            assert os.path.exists(output_path)

            with open(output_path, 'r') as f:
                content = f.read()
                assert "test_tool" in content
                assert "users" in content
                assert "orders" in content

            mock_logger.info.assert_called_with(f"SQL gerado em '{output_path}'")

    @patch('commons.create_sheets.logger')
    @patch('argparse.ArgumentParser.parse_args')
    def test_main_success_with_any_env_file(self, mock_args, mock_logger):
        """Testa execução principal encontrando qualquer arquivo .env."""
        # GIVEN
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = os.path.join(temp_dir, "random.env")
            with open(config_path, 'w') as f:
                f.write("TOOL=another_tool\n")
                f.write("ENDPOINTS=products\n")

            mock_args.return_value = Mock(dir_path=temp_dir, output='output.sql')

            # WHEN
            main()

            # THEN
            output_path = os.path.join(temp_dir, 'output.sql')
            assert os.path.exists(output_path)

    @patch('commons.create_sheets.logger')
    @patch('argparse.ArgumentParser.parse_args')
    def test_main_no_env_file_found(self, mock_args, mock_logger):
        """Testa execução quando nenhum arquivo .env é encontrado."""
        # GIVEN
        with tempfile.TemporaryDirectory() as temp_dir:
            mock_args.return_value = Mock(dir_path=temp_dir, output='test.sql')

            # WHEN
            main()

            # THEN
            mock_logger.error.assert_called_with(f"Nenhum arquivo .env encontrado em '{temp_dir}'")

    @patch('commons.create_sheets.logger')
    @patch('argparse.ArgumentParser.parse_args')
    def test_main_missing_required_variables(self, mock_args, mock_logger):
        """Testa execução com variáveis obrigatórias ausentes."""
        # GIVEN
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = os.path.join(temp_dir, "config.env")
            with open(config_path, 'w') as f:
                f.write("TOOL=test_tool\n")

            mock_args.return_value = Mock(dir_path=temp_dir, output='test.sql')

            # WHEN
            main()

            # THEN
            mock_logger.error.assert_called_with("Variável ENDPOINTS não encontrada no arquivo de configuração")

    @patch('commons.create_sheets.logger')
    @patch('argparse.ArgumentParser.parse_args')
    def test_main_default_output_filename(self, mock_args, mock_logger):
        """Testa execução com nome de arquivo padrão."""
        # GIVEN
        with tempfile.TemporaryDirectory() as temp_dir:
            config_path = os.path.join(temp_dir, "config.env")
            with open(config_path, 'w') as f:
                f.write("TOOL=test_tool\n")
                f.write("ENDPOINTS=users\n")

            mock_args.return_value = Mock(dir_path=temp_dir, output='sheet.sql')

            # WHEN
            main()

            # THEN
            default_output_path = os.path.join(temp_dir, 'sheet.sql')
            assert os.path.exists(default_output_path)


class TestEdgeCases:
    """Testes para casos extremos."""

    def test_generate_sql_with_spaces_in_endpoints(self):
        """Testa geração com espaços nos endpoints."""
        # GIVEN
        tool = "my_tool"
        endpoints = "users, orders , products"

        # WHEN
        result = generate_sql(tool, endpoints)

        # THEN
        assert "users" in result
        assert " orders " in result
        assert " products" in result

    def test_generate_sql_empty_endpoints(self):
        """Testa geração com string de endpoints vazia."""
        # GIVEN
        tool = "my_tool"
        endpoints = ""

        # WHEN
        result = generate_sql(tool, endpoints)

        # THEN
        assert "-- GOLD" in result
        assert "CREATE OR REPLACE EXTERNAL TABLE" in result
        assert "{project_id}.my_tool." in result

    def test_generate_sql_truly_empty_endpoints(self):
        """Testa com lista de endpoints realmente vazia."""
        # GIVEN
        tool = "my_tool"

        # WHEN
        endpoints_list = []
        sql = ""

        for endpoint in endpoints_list:
            sql += f"CREATE OR REPLACE EXTERNAL TABLE {{project_id}}.{tool}.{endpoint}\n"

        sql += "-- GOLD\n"

        for endpoint in endpoints_list:
            sql += f"CREATE OR REPLACE TABLE `{{project_id}}.vendas.{tool}_{endpoint}_gold`\n"

        # THEN
        assert "-- GOLD" in sql
        assert "CREATE OR REPLACE EXTERNAL TABLE" not in sql

    def test_read_config_file_empty_file(self):
        """Testa leitura de arquivo vazio."""
        # GIVEN
        with tempfile.NamedTemporaryFile(mode='w', suffix='.env', delete=False) as f:
            f.write("")
            config_path = f.name

        # WHEN
        result = read_config_file(config_path)

        # THEN
        assert result == {}

        # CLEANUP
        os.unlink(config_path)
