import os
from unittest.mock import patch, MagicMock

import pytest

from generic.argument_manager import ArgumentManager


class TestArgumentManager:
    """Testes simplificados para a classe ArgumentManager."""

    def test_init_debug_mode(self):
        """Testa inicialização no modo debug."""
        with patch('sys.argv', ['script.py']):
            arg_manager = ArgumentManager()
            assert arg_manager.is_debug is True
            assert arg_manager.script_name == 'script'

    def test_init_normal_mode(self):
        """Testa inicialização no modo normal."""
        with patch('sys.argv', ['script.py', '--arg', 'value']):
            arg_manager = ArgumentManager()
            assert arg_manager.is_debug is False

    def test_add_argument(self):
        """Testa a adição de argumentos."""
        arg_manager = ArgumentManager()
        arg_manager.add('TEST_VAR', 'Test help text')

        assert len(arg_manager.args) == 1
        arg = arg_manager.args[0]
        assert arg['name'] == 'test-var'
        assert arg['env'] == 'TEST_VAR'
        assert arg['help'] == 'Test help text'

    def test_parse_debug_mode(self):
        """Testa o parsing no modo debug usando variáveis de ambiente."""
        with patch('sys.argv', ['script.py']):
            with patch.dict(os.environ, {'TEST_VAR': 'env_value'}):
                arg_manager = ArgumentManager()
                arg_manager.add('TEST_VAR', 'Test help')
                args = arg_manager.parse()

                assert args.TEST_VAR == 'env_value'

    def test_parse_debug_with_type_conversion(self):
        """Testa a conversão de tipo no modo debug."""
        with patch('sys.argv', ['script.py']):
            with patch.dict(os.environ, {'NUM_VAR': '42'}):
                arg_manager = ArgumentManager()
                arg_manager.add('NUM_VAR', 'Number', arg_type=int)
                args = arg_manager.parse()

                assert args.NUM_VAR == 42
                assert isinstance(args.NUM_VAR, int)

    def test_parse_debug_required_missing(self):
        """Testa erro quando variável obrigatória está faltando no modo debug."""
        with patch('sys.argv', ['script.py']):
            arg_manager = ArgumentManager()
            arg_manager.add('REQUIRED_VAR', 'Required var', required=True)

            with pytest.raises(ValueError, match="Variável de ambiente obrigatória"):
                arg_manager.parse()

    def test_parse_normal_mode(self):
        """Testa o parsing no modo normal usando argumentos de linha de comando."""
        with patch('sys.argv', ['script.py', '--test-var', 'cli_value']):
            with patch('argparse.ArgumentParser.parse_args') as mock_parse_args:
                # Simula o retorno do parse_args
                mock_args = MagicMock()
                mock_args.TEST_VAR = 'cli_value'
                mock_parse_args.return_value = mock_args

                arg_manager = ArgumentManager()
                arg_manager.add('TEST_VAR', 'Test help')
                args = arg_manager.parse()

                assert args.TEST_VAR == 'cli_value'

    def test_folder_path_default(self):
        """Testa o comportamento especial para folder-path."""
        with patch('sys.argv', ['myscript.py']):
            arg_manager = ArgumentManager()
            arg_manager.add('FOLDER_PATH', 'Folder path')
            args = arg_manager.parse()

            assert args.FOLDER_PATH == 'myscript'

    def test_method_chaining(self):
        """Testa o encadeamento de métodos."""
        arg_manager = ArgumentManager()
        result = arg_manager.add('VAR1', 'Help 1').add('VAR2', 'Help 2')

        assert result is arg_manager
        assert len(arg_manager.args) == 2
