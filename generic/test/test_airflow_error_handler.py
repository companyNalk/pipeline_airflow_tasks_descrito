from unittest.mock import MagicMock, patch

try:
    from generic.airflow_error_handler import airflow_error_handler
except ImportError:
    raise ImportError(
        "Não foi possível importar airflow_error_handler do pacote generic. Verifique a estrutura do projeto.")


class TestAirflowErrorHandler:
    """Testes para o decorador airflow_error_handler."""

    def test_successful_execution(self):
        """Verifica se a função decorada executa com sucesso e registra logs corretos."""
        # GIVEN
        logger = MagicMock()
        mock_func = MagicMock(return_value="success")
        mock_func.__name__ = "mocked_function"
        decorated_func = airflow_error_handler(mock_func)
        start_time = 1234567890.0

        # WHEN
        with patch('generic.airflow_error_handler.logger', logger):
            with patch('time.time', side_effect=[start_time, start_time + 2.5]):
                result = decorated_func()

        # THEN
        assert result == "success"
        logger.debug.assert_any_call("⏱️ Iniciando mocked_function")
        logger.debug.assert_any_call("✓ mocked_function concluído em 2.50s")
        logger.error.assert_not_called()

    def test_rate_limit_with_retry_success(self):
        """Verifica se o decorador lida com erro de rate limit e tem sucesso após retry."""
        # GIVEN
        logger = MagicMock()
        mock_func = MagicMock(side_effect=[Exception("429 Too Many Requests"), "success"])
        mock_func.__name__ = "mocked_function"
        decorated_func = airflow_error_handler(mock_func)
        start_time = 1234567890.0

        # WHEN
        with patch('generic.airflow_error_handler.logger', logger):
            with patch('time.time', side_effect=[start_time, start_time + 1, start_time + 6]):
                with patch('time.sleep') as mock_sleep:
                    result = decorated_func()

        # THEN
        assert result == "success"
        logger.warning.assert_any_call("⏳ RATE LIMIT em mocked_function - Tentativa 1/3")
        logger.warning.assert_any_call("⏳ Aguardando 5s antes de tentar novamente...")
        mock_sleep.assert_called_once_with(5)
        logger.debug.assert_any_call("✓ mocked_function concluído em 6.00s")
        logger.error.assert_not_called()

    def test_rate_limit_exceeds_max_retries(self):
        """Verifica se o decorador falha após exceder as tentativas de retry para rate limit."""
        # GIVEN
        logger = MagicMock()
        mock_func = MagicMock(side_effect=Exception("429 Too Many Requests"))
        mock_func.__name__ = "mocked_function"
        decorated_func = airflow_error_handler(mock_func)
        start_time = 1234567890.0
        time_sequence = [start_time, start_time + 1, start_time + 6, start_time + 16, start_time + 16]

        # WHEN
        with patch('generic.airflow_error_handler.logger', logger):
            with patch('time.time', side_effect=time_sequence):
                with patch('time.sleep') as mock_sleep:
                    try:
                        decorated_func()
                    except Exception as e:
                        error = e

        # THEN
        assert str(error) == "Rate limit excedido em mocked_function após 3 tentativas"
        logger.warning.assert_any_call("⏳ RATE LIMIT em mocked_function - Tentativa 1/3")
        logger.warning.assert_any_call("⏳ RATE LIMIT em mocked_function - Tentativa 2/3")
        logger.warning.assert_any_call("⏳ RATE LIMIT em mocked_function - Tentativa 3/3")
        mock_sleep.assert_any_call(5)
        mock_sleep.assert_any_call(10)
        mock_sleep.assert_any_call(20)
        logger.error.assert_any_call("⛔ LIMITE DE TAXA EXCEDIDO em mocked_function após 16.00s")
        logger.error.assert_any_call("⚠️ Tentativas realizadas: 3/3")

    def test_non_rate_limit_error(self):
        """Verifica se o decorador lida corretamente com erros que não são de rate limit."""
        # GIVEN
        logger = MagicMock()
        mock_func = MagicMock(side_effect=ValueError("Invalid input"))
        mock_func.__name__ = "mocked_function"
        decorated_func = airflow_error_handler(mock_func)
        start_time = 1234567890.0

        # WHEN
        with patch('generic.airflow_error_handler.logger', logger):
            with patch('time.time', side_effect=[start_time, start_time + 1]):
                try:
                    decorated_func()
                except ValueError as e:
                    error = e

        # THEN
        assert str(error) == "Invalid input"
        logger.error.assert_any_call("❌ ERRO em mocked_function após 1.00s: ValueError: Invalid input")
        # Verifica se há uma chamada ao logger.error que começa com '📋 TRACEBACK:\n'
        traceback_calls = [call for call in logger.error.call_args_list if call.args[0].startswith("📋 TRACEBACK:\n")]
        assert len(traceback_calls) == 1, "Chamada com '📋 TRACEBACK:\\n' não encontrada"
        logger.warning.assert_not_called()

    def test_rate_limit_error_case_insensitive(self):
        """Verifica se o decorador detecta erro de rate limit com mensagem em letras minúsculas."""
        # GIVEN
        logger = MagicMock()
        mock_func = MagicMock(side_effect=[Exception("too many requests"), "success"])
        mock_func.__name__ = "mocked_function"
        decorated_func = airflow_error_handler(mock_func)
        start_time = 1234567890.0

        # WHEN
        with patch('generic.airflow_error_handler.logger', logger):
            with patch('time.time', side_effect=[start_time, start_time + 1, start_time + 6]):
                with patch('time.sleep') as mock_sleep:
                    result = decorated_func()

        # THEN
        assert result == "success"
        logger.warning.assert_any_call("⏳ RATE LIMIT em mocked_function - Tentativa 1/3")
        mock_sleep.assert_called_once_with(5)
        logger.debug.assert_any_call("✓ mocked_function concluído em 6.00s")
