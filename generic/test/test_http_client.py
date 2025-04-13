import json
import logging
from unittest.mock import MagicMock, patch

import requests  # Import adicionado para corrigir NameError

from generic.http_client import HttpClient


class TestHttpClient:
    """Testes para a classe HttpClient."""

    def test_initialization(self):
        """Verifica se o HttpClient é inicializado corretamente com valores padrão."""
        # GIVEN
        base_url = "https://api.example.com"
        rate_limiter = MagicMock()
        logger = MagicMock()
        timeout = 60
        user_agent = "CustomClient/2.0"

        # WHEN
        client = HttpClient(base_url, rate_limiter, logger, timeout, user_agent)

        # THEN
        assert client.base_url == "https://api.example.com"
        assert client.rate_limiter == rate_limiter
        assert client.logger == logger
        assert client.default_timeout == timeout
        assert client.session.headers["User-Agent"] == user_agent
        assert client.session.headers["Accept"] == "application/json"
        assert client.default_retry_codes == [429, 500, 502, 503, 504]
        assert client.max_retries == 3
        assert client.retry_delay_base == 1

    def test_initialization_defaults(self):
        """Verifica se o HttpClient usa valores padrão quando não especificados."""
        # WHEN
        client = HttpClient(base_url="https://api.example.com")

        # THEN
        assert client.base_url == "https://api.example.com"
        assert client.rate_limiter is not None
        assert isinstance(client.logger, logging.Logger)
        assert client.default_timeout == 30
        assert client.session.headers["User-Agent"] == "APIClient/1.0"

    def test_add_default_header(self):
        """Verifica se um cabeçalho padrão é adicionado corretamente."""
        # GIVEN
        client = HttpClient(base_url="https://api.example.com")

        # WHEN
        client.add_default_header("Authorization", "Bearer token")

        # THEN
        assert client.session.headers["Authorization"] == "Bearer token"

    def test_add_hooks(self):
        """Verifica se hooks são adicionados corretamente."""
        # GIVEN
        client = HttpClient(base_url="https://api.example.com")
        request_hook = MagicMock()
        response_hook = MagicMock()
        error_hook = MagicMock()

        # WHEN
        client.add_request_hook(request_hook)
        client.add_response_hook(response_hook)
        client.add_error_hook(error_hook)

        # THEN
        assert request_hook in client.request_hooks
        assert response_hook in client.response_hooks
        assert error_hook in client.error_hooks

    def test_build_url_relative(self):
        """Verifica se URLs relativas são construídas corretamente."""
        # GIVEN
        client = HttpClient(base_url="https://api.example.com")

        # WHEN
        full_url = client._build_url("endpoint")

        # THEN
        assert full_url == "https://api.example.com/endpoint"

    def test_build_url_absolute(self):
        """Verifica se URLs absolutas são retornadas sem modificação."""
        # GIVEN
        client = HttpClient(base_url="https://api.example.com")
        absolute_url = "https://other.api.com/endpoint"

        # WHEN
        full_url = client._build_url(absolute_url)

        # THEN
        assert full_url == absolute_url

    def test_execute_hooks_success(self):
        """Verifica se hooks são executados corretamente."""
        # GIVEN
        client = HttpClient(base_url="https://api.example.com")
        hook = MagicMock()
        client.request_hooks = [hook]

        # WHEN
        client._execute_hooks(client.request_hooks, "GET", "url", {}, None, None)

        # THEN
        hook.assert_called_once_with("GET", "url", {}, None, None)

    def test_execute_hooks_with_error(self):
        """Verifica se erros em hooks são capturados e logados."""
        # GIVEN
        client = HttpClient(base_url="https://api.example.com")
        logger = MagicMock()
        client.logger = logger
        hook = MagicMock(side_effect=ValueError("Hook error"))
        client.request_hooks = [hook]

        # WHEN
        client._execute_hooks(client.request_hooks, "GET", "url")

        # THEN
        logger.warning.assert_called_once_with("Erro ao executar hook: Hook error")

    def test_request_success_json(self):
        """Verifica se uma requisição bem-sucedida retorna JSON."""
        # GIVEN
        client = HttpClient(base_url="https://api.example.com")
        logger = MagicMock()
        rate_limiter = MagicMock()
        client.logger = logger
        client.rate_limiter = rate_limiter
        response_mock = MagicMock()
        response_mock.status_code = 200
        response_mock.json.return_value = {"data": "success"}
        session_mock = MagicMock()
        session_mock.request.return_value = response_mock

        # WHEN
        with patch.object(client, 'session', session_mock):
            result = client.request("GET", "endpoint")

        # THEN
        assert result == {"data": "success"}
        rate_limiter.check.assert_called_once()
        logger.debug.assert_any_call("🔄 Requisição GET:endpoint - Tentativa 1/3")
        # Verifica se há uma chamada ao logger.debug que começa com o prefixo esperado
        debug_calls = [call for call in logger.debug.call_args_list if
                       call.args[0].startswith("⏱️ Resposta GET:endpoint - Status: 200 - Tempo: ")]
        assert len(debug_calls) == 1, "Chamada com '⏱️ Resposta GET:endpoint - Status: 200 - Tempo: ' não encontrada"

    def test_request_success_text(self):
        """Verifica se uma requisição bem-sucedida retorna texto quando não é JSON."""
        # GIVEN
        client = HttpClient(base_url="https://api.example.com")
        logger = MagicMock()
        client.logger = logger
        response_mock = MagicMock()
        response_mock.status_code = 200
        response_mock.json.side_effect = json.JSONDecodeError("Invalid JSON", "", 0)
        response_mock.text = "Plain text response"
        session_mock = MagicMock()
        session_mock.request.return_value = response_mock

        # WHEN
        with patch.object(client, 'session', session_mock):
            result = client.request("GET", "endpoint")

        # THEN
        assert result == "Plain text response"
        logger.debug.assert_any_call("🔄 Requisição GET:endpoint - Tentativa 1/3")

    def test_request_404(self):
        """Verifica se uma resposta 404 retorna um objeto padrão."""
        # GIVEN
        client = HttpClient(base_url="https://api.example.com")
        logger = MagicMock()
        client.logger = logger
        response_mock = MagicMock()
        response_mock.status_code = 404
        session_mock = MagicMock()
        session_mock.request.return_value = response_mock

        # WHEN
        with patch.object(client, 'session', session_mock):
            result = client.request("GET", "endpoint")

        # THEN
        assert result == {"data": [], "meta": {"totalPages": 0}}
        logger.debug.assert_any_call("ℹ️ Endpoint não encontrado: https://api.example.com/endpoint")

    def test_request_retry_on_error(self):
        """Verifica se o cliente faz retry em códigos de erro configurados."""
        # GIVEN
        client = HttpClient(base_url="https://api.example.com")
        logger = MagicMock()
        client.logger = logger
        response_mock = MagicMock()
        response_mock.status_code = 429
        response_mock.json.return_value = {"message": "Rate limit exceeded"}
        session_mock = MagicMock()
        session_mock.request.side_effect = [response_mock, response_mock,
                                            MagicMock(status_code=200, json=lambda: {"data": "success"})]

        # WHEN
        with patch.object(client, 'session', session_mock):
            with patch('time.sleep') as mock_sleep:
                result = client.request("GET", "endpoint")

        # THEN
        assert result == {"data": "success"}
        assert session_mock.request.call_count == 3
        logger.warning.assert_any_call("⚠️ Erro HTTP 429 para GET:endpoint. Tentando novamente em 1s")
        logger.warning.assert_any_call("⚠️ Erro HTTP 429 para GET:endpoint. Tentando novamente em 2s")
        mock_sleep.assert_any_call(1)
        mock_sleep.assert_any_call(2)

    def test_request_exceeds_retries(self):
        """Verifica se uma exceção é levantada após exceder retries."""
        # GIVEN
        client = HttpClient(base_url="https://api.example.com")
        logger = MagicMock()
        client.logger = logger
        response_mock = MagicMock()
        response_mock.status_code = 500
        response_mock.json.return_value = {"message": "Server error"}
        session_mock = MagicMock()
        session_mock.request.return_value = response_mock

        # WHEN
        with patch.object(client, 'session', session_mock):
            with patch('time.sleep'):
                try:
                    client.request("GET", "endpoint")
                except Exception as e:
                    error = e

        # THEN
        assert str(error) == "Erro HTTP 500: Server error"
        assert session_mock.request.call_count == 3
        logger.error.assert_any_call("❌ Erro HTTP 500: Server error")

    def test_request_network_error(self):
        """Verifica se erros de rede são tratados com retries."""
        # GIVEN
        client = HttpClient(base_url="https://api.example.com")
        logger = MagicMock()
        client.logger = logger
        session_mock = MagicMock()
        session_mock.request.side_effect = [requests.ConnectionError("Network error"),
                                            MagicMock(status_code=200, json=lambda: {"data": "success"})]

        # WHEN
        with patch.object(client, 'session', session_mock):
            with patch('time.sleep') as mock_sleep:
                result = client.request("GET", "endpoint")

        # THEN
        assert result == {"data": "success"}
        logger.warning.assert_any_call(
            "⚠️ Erro de rede na requisição GET:endpoint: Network error. Tentando novamente em 1s")
        mock_sleep.assert_called_once_with(1)

    def test_get_method(self):
        """Verifica se o método get chama request corretamente."""
        # GIVEN
        client = HttpClient(base_url="https://api.example.com")
        with patch.object(client, 'request', return_value={"data": "success"}) as mock_request:
            # WHEN
            result = client.get("endpoint", params={"key": "value"})

        # THEN
        assert result == {"data": "success"}
        mock_request.assert_called_once_with("GET", "endpoint", params={"key": "value"})

    def test_post_method(self):
        """Verifica se o método post chama request corretamente."""
        # GIVEN
        client = HttpClient(base_url="https://api.example.com")
        with patch.object(client, 'request', return_value={"data": "success"}) as mock_request:
            # WHEN
            result = client.post("endpoint", json_data={"key": "value"})

        # THEN
        assert result == {"data": "success"}
        mock_request.assert_called_once_with("POST", "endpoint", json_data={"key": "value"}, data=None)
