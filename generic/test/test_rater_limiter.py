import logging
import time
from unittest.mock import patch, MagicMock

import pytest

from generic.rate_limiter import RateLimitFilter, RateLimiter


class TestRateLimitFilter:
    """Testes simplificados para a classe RateLimitFilter."""

    def setup_method(self):
        """Configura o ambiente para cada teste."""
        # Resetar o last_logged para garantir isolamento entre testes
        RateLimitFilter.last_logged = 0

    def test_filter_non_rate_limit_message(self):
        """Testa que mensagens normais sempre passam pelo filtro."""
        filter_instance = RateLimitFilter()

        # Criar um record simulado
        record = MagicMock()
        record.getMessage.return_value = "Mensagem normal de log"

        # O filtro deve deixar passar mensagens normais
        assert filter_instance.filter(record) is True

    def test_filter_rate_limit_message_first_time(self):
        """Testa que a primeira mensagem de rate limit passa pelo filtro."""
        filter_instance = RateLimitFilter()

        # Criar um record simulado com mensagem de rate limit
        record = MagicMock()
        record.getMessage.return_value = "Rate limit atingido"

        # A primeira mensagem de rate limit deve passar
        assert filter_instance.filter(record) is True

    def test_filter_rate_limit_message_within_timeout(self):
        """Testa que mensagens de rate limit são bloqueadas dentro do período de timeout."""
        filter_instance = RateLimitFilter()

        # Simular que já foi logado recentemente
        RateLimitFilter.last_logged = time.time()

        # Criar um record simulado com mensagem de rate limit
        record = MagicMock()
        record.getMessage.return_value = "Rate limit atingido"

        # Mensagem dentro do timeout deve ser bloqueada
        assert filter_instance.filter(record) is False

    def test_filter_rate_limit_message_after_timeout(self):
        """Testa que mensagens de rate limit passam após o período de timeout."""
        filter_instance = RateLimitFilter()

        # Simular que foi logado há mais de 60 segundos
        RateLimitFilter.last_logged = time.time() - 61

        # Criar um record simulado com mensagem de rate limit
        record = MagicMock()
        record.getMessage.return_value = "Rate limit atingido"

        # Mensagem após timeout deve passar
        assert filter_instance.filter(record) is True

    def test_multiple_rate_limit_patterns(self):
        """Testa que diferentes padrões de mensagens de rate limit são reconhecidos."""
        filter_instance = RateLimitFilter()

        # Resetar o last_logged para cada teste de padrão
        patterns = ["Rate limit", "RATE LIMIT", "429", "too many requests", "Too Many Requests",
                    "Aguardando", "⏳"]

        for pattern in patterns:
            RateLimitFilter.last_logged = 0
            record = MagicMock()
            record.getMessage.return_value = f"Erro: {pattern} detectado"

            # A primeira mensagem deve passar
            assert filter_instance.filter(record) is True

            # A próxima mensagem (dentro do timeout) deve ser bloqueada
            assert filter_instance.filter(record) is False


class TestRateLimiter:
    """Testes simplificados para a classe RateLimiter."""

    def test_init(self):
        """Testa a inicialização do RateLimiter."""
        logger = logging.getLogger("test")
        limiter = RateLimiter(
            requests_per_window=100,
            window_seconds=60,
            check_every=5,
            logger=logger
        )

        assert limiter.requests_per_window == 100
        assert limiter.window_seconds == 60
        assert limiter.check_every == 5
        assert limiter.logger == logger
        assert limiter.request_counter == 0
        # Verificar se o lock existe, sem verificar o tipo específico
        assert hasattr(limiter, 'lock')
        assert limiter.lock is not None

    def test_filter_added_to_logger(self):
        """Testa se o filtro é adicionado ao logger."""
        logger = logging.getLogger("test_filter")
        handler = logging.StreamHandler()
        logger.addHandler(handler)

        RateLimiter(logger=logger)

        # Verificar se o filtro foi adicionado ao logger e ao handler
        filter_added_to_logger = any(isinstance(f, RateLimitFilter) for f in logger.filters)
        filter_added_to_handler = any(isinstance(f, RateLimitFilter) for f in handler.filters)

        assert filter_added_to_logger
        assert filter_added_to_handler

    def test_check_under_limit(self):
        """Testa o comportamento quando estamos abaixo do limite de requisições."""
        with patch('time.time', return_value=1000):
            limiter = RateLimiter(requests_per_window=10, window_seconds=60)

            # Simular chamadas abaixo do limite
            for _ in range(9):
                with patch('time.sleep') as mock_sleep:
                    limiter.check()
                    mock_sleep.assert_not_called()

            assert limiter.request_counter == 9

    def test_check_reset_window(self):
        """Testa o reset da janela de tempo."""
        current_time = 1000

        with patch('time.time', return_value=current_time):
            limiter = RateLimiter(requests_per_window=10, window_seconds=60, check_every=1)

            # Avançar o tempo para depois do reset_time
            with patch('time.time', return_value=current_time + 61):
                limiter.check()

                # O contador deve ter sido resetado
                assert limiter.request_counter == 1
                assert limiter.reset_time > current_time + 60

    @patch('time.sleep')
    @patch('time.time')
    def test_check_at_limit(self, mock_time, mock_sleep):
        """Testa o comportamento quando atingimos o limite de requisições."""
        # Configurar o mock de tempo para retornar valores incrementais
        mock_time.side_effect = [1000, 1000, 1000, 1060]

        limiter = RateLimiter(requests_per_window=5, window_seconds=60, check_every=5)
        limiter.request_counter = 5  # Já no limite
        limiter.reset_time = 1060  # Reset em 1 minuto

        # Esta chamada deve ativar o sleep
        limiter.check()

        # Verificar se o sleep foi chamado com o tempo correto
        mock_sleep.assert_called_once_with(60)
        # Após o sleep, o contador deve ser resetado
        assert limiter.request_counter == 1

    def test_get_remaining(self):
        """Testa o cálculo de requisições restantes."""
        with patch('time.time', return_value=1000):
            limiter = RateLimiter(requests_per_window=10)
            limiter.request_counter = 3

            assert limiter.get_remaining() == 7

    def test_get_remaining_after_reset(self):
        """Testa o cálculo de requisições restantes após o reset da janela."""
        with patch('time.time', return_value=1000):
            limiter = RateLimiter(requests_per_window=10, window_seconds=60)
            limiter.request_counter = 7
            limiter.reset_time = 1030

            # Simular que passamos do tempo de reset
            with patch('time.time', return_value=1031):
                assert limiter.get_remaining() == 10

    def test_get_reset_time(self):
        """Testa o cálculo do tempo restante até o reset."""
        with patch('time.time', return_value=1000):
            limiter = RateLimiter(window_seconds=60)
            limiter.reset_time = 1040

            assert limiter.get_reset_time() == 40

    def test_get_reset_time_after_reset(self):
        """Testa o cálculo do tempo restante quando já passou do reset."""
        with patch('time.time', return_value=1000):
            limiter = RateLimiter(window_seconds=60)
            limiter.reset_time = 990  # Já passou

            assert limiter.get_reset_time() == 0


if __name__ == "__main__":
    pytest.main(["-v"])
