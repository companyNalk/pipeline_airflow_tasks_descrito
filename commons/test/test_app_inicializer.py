import logging
import sys
from unittest.mock import patch, MagicMock

from commons.app_inicializer import AppInitializer


class TestSetupLogging:
    """Testes para o método _setup_logging da classe AppInitializer."""

    def test_returns_logger_instance(self):
        """Verifica se _setup_logging retorna uma instância de logger."""
        assert isinstance(AppInitializer._setup_logging(), logging.Logger)

    def test_default_params(self):
        """Verifica os parâmetros padrão do setup de logging."""
        # GIVEN
        expected_name = "commons.app_inicializer"
        expected_format = '[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s'

        # WHEN
        with patch('logging.getLogger') as mock_get_logger:
            AppInitializer._setup_logging()

            # THEN
            mock_get_logger.assert_called_once()
            assert mock_get_logger.call_args[0][0] == expected_name

        # WHEN
        with patch('logging.basicConfig') as mock_basic_config:
            AppInitializer._setup_logging()

            # THEN
            assert mock_basic_config.call_args[1]['level'] == logging.INFO
            assert mock_basic_config.call_args[1]['format'] == expected_format

    def test_custom_logger_name(self):
        """Verifica o setup de logging com nome de logger personalizado."""
        # GIVEN
        custom_name = "custom_logger"

        # WHEN
        with patch('logging.getLogger') as mock_get_logger:
            AppInitializer._setup_logging(logger_name=custom_name)

            # THEN
            mock_get_logger.assert_called_once_with(custom_name)

    def test_custom_level(self):
        """Verifica o setup de logging com nível personalizado."""
        # GIVEN
        custom_level = logging.DEBUG

        # WHEN
        with patch('logging.basicConfig') as mock_basic_config:
            AppInitializer._setup_logging(level=custom_level)

            # THEN
            assert mock_basic_config.call_args[1]['level'] == custom_level

    def test_handlers(self):
        """Verifica se os handlers são configurados corretamente."""
        # GIVEN
        mock_handler = MagicMock()

        # WHEN
        with patch('logging.basicConfig') as mock_basic_config:
            with patch('logging.StreamHandler') as mock_stream_handler:
                mock_stream_handler.return_value = mock_handler
                AppInitializer._setup_logging()

                # THEN
                mock_stream_handler.assert_called_once_with(sys.stdout)
                assert mock_handler in mock_basic_config.call_args[1]['handlers']


class TestLoadEnvironment:
    """Testes para o método _load_environment da classe AppInitializer."""

    def test_calls_load_env_file(self):
        """Verifica se _load_environment chama load_env_file."""
        # WHEN
        with patch('commons.app_inicializer.load_env_file') as mock_load_env:
            AppInitializer._load_environment()

            # THEN
            mock_load_env.assert_called_once()


class TestInitialize:
    """Testes para o método initialize da classe AppInitializer."""

    def test_default_params(self):
        """Verifica o método initialize com parâmetros padrão."""
        # GIVEN
        mock_logger = MagicMock()

        # WHEN
        with patch.object(AppInitializer, '_setup_logging', return_value=mock_logger) as mock_setup_logging:
            with patch.object(AppInitializer, '_load_environment') as mock_load_env:
                result = AppInitializer.initialize()

                # THEN
                mock_setup_logging.assert_called_with("commons.app_inicializer", logging.INFO)
                mock_load_env.assert_called_once()
                assert result is mock_logger

    def test_custom_params(self):
        """Verifica o método initialize com parâmetros personalizados."""
        # GIVEN
        mock_logger = MagicMock()
        logger_name = "test_logger"
        log_level = logging.WARNING

        # WHEN
        with patch.object(AppInitializer, '_setup_logging', return_value=mock_logger) as mock_setup_logging:
            with patch.object(AppInitializer, '_load_environment') as mock_load_env:
                result = AppInitializer.initialize(logger_name=logger_name, level=log_level)

                # THEN
                mock_setup_logging.assert_called_with(logger_name, log_level)
                mock_load_env.assert_called_once()
                assert result is mock_logger

    def test_integration(self):
        """Teste integrado de initialize."""
        # GIVEN
        mock_logger = MagicMock()

        # WHEN
        with patch('logging.getLogger', return_value=mock_logger):
            with patch('logging.basicConfig'):
                with patch('commons.app_inicializer.load_env_file') as mock_load_env:
                    logger = AppInitializer.initialize(logger_name="test_integration")

                    # THEN
                    mock_load_env.assert_called_once()
                    assert logger is mock_logger
