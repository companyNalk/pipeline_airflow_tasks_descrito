import os
from pathlib import Path
from unittest.mock import patch, MagicMock

from commons.validate_env import load_env_file


class TestLoadEnvFileDockerEnvironment:
    def test_load_env_file_in_docker_environment(self):
        """Verifica se a função ignora o carregamento quando executada em Docker."""
        # GIVEN
        with patch.dict(os.environ, {"RUNNING_IN_DOCKER": "true"}):
            with patch('commons.validate_env.logger') as mock_logger:
                # WHEN
                result = load_env_file()

                # THEN
                assert result is False
                mock_logger.info.assert_called_once_with(
                    "Running in Docker environment. Skipping .env file loading."
                )

    def test_load_env_file_without_dotenv_module(self):
        """Verifica o comportamento quando o módulo python-dotenv não está instalado."""
        # GIVEN
        with patch.dict(os.environ, {}, clear=True):  # Limpa os envs para garantir que RUNNING_IN_DOCKER não existe
            with patch('commons.validate_env.load_dotenv', None):
                with patch('commons.validate_env.logger') as mock_logger:
                    # WHEN
                    result = load_env_file()

                    # THEN
                    assert result is False
                    mock_logger.warning.assert_called_once_with(
                        "python-dotenv not installed. Cannot load .env files."
                    )

    def test_load_env_file_env_file_exists(self):
        """Verifica o carregamento quando o arquivo .env existe."""
        # GIVEN
        mock_env_path = Path('.env')
        mock_dotenv = MagicMock(return_value=True)

        with patch.dict(os.environ, {}, clear=True):
            with patch('commons.validate_env.load_dotenv', mock_dotenv):
                with patch('commons.validate_env.logger') as mock_logger:
                    with patch.object(Path, 'exists', return_value=True):
                        with patch.object(Path, 'absolute', return_value=mock_env_path):
                            # WHEN
                            result = load_env_file()

                            # THEN
                            assert result is True
                            mock_logger.info.assert_called_once_with(
                                f"Loading environment variables from {mock_env_path}"
                            )
                            mock_dotenv.assert_called_once_with(dotenv_path=mock_env_path)

    def test_load_env_file_no_env_file_found(self):
        """Verifica o comportamento quando nenhum arquivo .env é encontrado."""
        # GIVEN
        with patch.dict(os.environ, {}, clear=True):
            with patch('commons.validate_env.load_dotenv') as mock_dotenv:
                with patch('commons.validate_env.logger') as mock_logger:
                    with patch.object(Path, 'exists', return_value=False):
                        # WHEN
                        result = load_env_file()

                        # THEN
                        assert result is False
                        mock_logger.warning.assert_called_once_with(
                            "No .env file found. Using system environment variables only."
                        )
                        mock_dotenv.assert_not_called()

    def test_load_env_file_with_custom_paths(self):
        """Verifica o carregamento com caminhos personalizados."""
        # GIVEN
        custom_path = Path('/custom/path/.env')
        another_path = Path('/another/path/.env')
        mock_dotenv = MagicMock(return_value=True)

        with patch.dict(os.environ, {}, clear=True):
            with patch('commons.validate_env.load_dotenv', mock_dotenv):
                with patch('commons.validate_env.logger') as mock_logger:
                    # Simula que apenas o segundo arquivo existe
                    with patch.object(Path, 'exists', side_effect=[False, True]):
                        with patch.object(Path, 'absolute', return_value=another_path):
                            # WHEN
                            result = load_env_file([custom_path, another_path])

                            # THEN
                            assert result is True
                            mock_logger.info.assert_called_once_with(
                                f"Loading environment variables from {another_path}"
                            )
                            mock_dotenv.assert_called_once_with(dotenv_path=another_path)

    def test_load_env_file_multiple_paths_none_exist(self):
        """Verifica o comportamento quando vários caminhos são fornecidos, mas nenhum existe."""
        # GIVEN
        path1 = Path('/path1/.env')
        path2 = Path('/path2/.env')

        with patch.dict(os.environ, {}, clear=True):
            with patch('commons.validate_env.load_dotenv') as mock_dotenv:
                with patch('commons.validate_env.logger') as mock_logger:
                    with patch.object(Path, 'exists', return_value=False):
                        # WHEN
                        result = load_env_file([path1, path2])

                        # THEN
                        assert result is False
                        mock_logger.warning.assert_called_once_with(
                            "No .env file found. Using system environment variables only."
                        )
                        mock_dotenv.assert_not_called()

    def test_load_env_file_stops_at_first_found(self):
        """Verifica se a função para no primeiro arquivo .env encontrado."""
        # GIVEN
        path1 = Path('/path1/.env')
        path2 = Path('/path2/.env')
        mock_dotenv = MagicMock(return_value=True)

        with patch.dict(os.environ, {}, clear=True):
            with patch('commons.validate_env.load_dotenv', mock_dotenv):
                with patch('commons.validate_env.logger'):
                    # Simula que ambos os arquivos existem
                    with patch.object(Path, 'exists', return_value=True):
                        with patch.object(Path, 'absolute', return_value=path1):
                            # WHEN
                            result = load_env_file([path1, path2])

                            # THEN
                            assert result is True
                            # Deve ter carregado apenas o primeiro arquivo
                            mock_dotenv.assert_called_once_with(dotenv_path=path1)
                            # Não deve ter tentado carregar o segundo arquivo
                            assert mock_dotenv.call_count == 1
