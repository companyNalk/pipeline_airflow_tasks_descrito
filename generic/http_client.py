import json
import logging
import time
from typing import Any, Dict, List, Optional, Callable

import requests

from generic.airflow_error_handler import airflow_error_handler
from generic.rate_limiter import RateLimiter


class HttpClient:
    def __init__(self, base_url: str, rate_limiter: Optional[RateLimiter] = None,
                 logger: Optional[logging.Logger] = None, default_timeout: int = 30, user_agent: str = "APIClient/1.0"):
        self.base_url = base_url.rstrip('/')
        self.rate_limiter = rate_limiter or RateLimiter()
        self.logger = logger or logging.getLogger(__name__)
        self.default_timeout = default_timeout

        # Inicializa a sessão para reutilização de conexões
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": user_agent,
            "Accept": "application/json"
        })

        # Configurações para retentativas
        self.default_retry_codes = [429, 500, 502, 503, 504]
        self.max_retries = 3
        self.retry_delay_base = 1

        # Callbacks que podem ser registrados
        self.request_hooks = []
        self.response_hooks = []
        self.error_hooks = []

    def add_default_header(self, name: str, value: str) -> None:
        """
        Adiciona um header padrão para todas as requisições.
        """
        self.session.headers.update({name: value})

    def add_request_hook(self, hook: Callable) -> None:
        """
        Adiciona um hook que será executado antes de cada requisição.
        """
        self.request_hooks.append(hook)

    def add_response_hook(self, hook: Callable) -> None:
        """
        Adiciona um hook que será executado após cada resposta bem-sucedida.
        """
        self.response_hooks.append(hook)

    def add_error_hook(self, hook: Callable) -> None:
        """
        Adiciona um hook que será executado quando ocorrer um erro.
        """
        self.error_hooks.append(hook)

    def _build_url(self, url: str) -> str:
        """
        Constrói a URL completa a partir de um path relativo ou URL completa.
        """
        if url.startswith(('http://', 'https://')):
            return url
        return f"{self.base_url}/{url.lstrip('/')}"

    def _execute_hooks(self, hooks: List[Callable], *args, **kwargs) -> None:
        """
        Executa uma lista de hooks com os argumentos fornecidos.
        """
        for hook in hooks:
            try:
                hook(*args, **kwargs)
            except Exception as e:
                self.logger.warning(f"Erro ao executar hook: {str(e)}")

    @airflow_error_handler
    def request(self, method: str, url: str, headers: Dict[str, str] = None, data: Any = None,
                params: Dict[str, Any] = None, json_data: Dict[str, Any] = None, timeout: int = None,
                retry_on_status: List[int] = None, max_retries: int = None, debug_info: str = None) -> Any:
        """
        Realiza uma requisição HTTP com suporte a rate limit, retry e logging.
        """
        # Aplicar rate limiting
        if self.rate_limiter:
            self.rate_limiter.check()

        # Preparar parâmetros
        full_url = self._build_url(url)
        request_headers = headers or {}
        request_timeout = timeout or self.default_timeout
        retry_codes = retry_on_status or self.default_retry_codes
        retries = max_retries or self.max_retries

        # Criar ID para debugging
        request_id = f"{method}:{url.split('/')[-1]}"
        if debug_info:
            request_id = f"{request_id}:{debug_info}"

        # Executar hooks de pré-requisição
        self._execute_hooks(self.request_hooks, method, full_url, request_headers, data or json_data, params)

        # Tentativas com backoff exponencial
        retry_delay = self.retry_delay_base

        for attempt in range(retries):
            try:
                self.logger.debug(f"🔄 Requisição {request_id} - Tentativa {attempt + 1}/{retries}")
                start_time = time.time()

                # Realizar a requisição
                response = self.session.request(method=method.upper(), url=full_url, headers=request_headers, data=data,
                                                json=json_data, params=params, timeout=request_timeout)

                # Calcular duração
                duration = time.time() - start_time
                self.logger.debug(f"⏱️ Resposta {request_id} - Status: {response.status_code} - Tempo: {duration:.2f}s")

                # Executar hooks de resposta
                self._execute_hooks(self.response_hooks, response, duration)

                # Verificar se é 404 (tratamento especial)
                if response.status_code == 404:
                    self.logger.debug(f"ℹ️ Endpoint não encontrado: {full_url}")
                    return {"data": [], "meta": {"totalPages": 0}}

                # Verificar se é um código de erro
                if response.status_code >= 400:
                    # Verificar se deve fazer retry
                    if response.status_code in retry_codes and attempt < retries - 1:
                        self.logger.warning(
                            f"⚠️ Erro HTTP {response.status_code} para {request_id}. Tentando novamente em {retry_delay}s")
                        time.sleep(retry_delay)
                        retry_delay *= 2  # Backoff exponencial
                        continue

                    # Extrair mensagem de erro
                    error_msg = f"Erro HTTP {response.status_code}"
                    try:
                        error_data = response.json()
                        if isinstance(error_data, dict):
                            if "message" in error_data:
                                error_msg += f": {error_data['message']}"
                            elif "error" in error_data:
                                error_msg += f": {error_data['error']}"
                    except Exception:
                        error_msg += f": {response.text[:200]}"

                    # Registrar e lançar exceção
                    self.logger.error(f"❌ {error_msg}")
                    self._execute_hooks(self.error_hooks, Exception(error_msg),
                                        {"request_id": request_id, "status": response.status_code})
                    raise Exception(error_msg)

                # Processar resposta bem-sucedida
                try:
                    return response.json()
                except json.JSONDecodeError:
                    # Se não for JSON, retorna o texto
                    return response.text

            except requests.RequestException as e:
                # Erro de rede
                if attempt < retries - 1:
                    self.logger.warning(
                        f"⚠️ Erro de rede na requisição {request_id}: {str(e)}. Tentando novamente em {retry_delay}s")
                    time.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    self.logger.error(f"❌ Falha na requisição {request_id} após {retries} tentativas: {str(e)}")
                    self._execute_hooks(self.error_hooks, e, {"request_id": request_id})
                    raise

            except Exception as e:
                # Outros erros
                if attempt < retries - 1:
                    self.logger.warning(
                        f"⚠️ Erro na requisição {request_id}: {str(e)}. Tentando novamente em {retry_delay}s")
                    time.sleep(retry_delay)
                    retry_delay *= 2
                else:
                    self.logger.error(f"❌ Falha na requisição {request_id}: {str(e)}")
                    self._execute_hooks(self.error_hooks, e, {"request_id": request_id})
                    raise

    def get(self, url: str, params: Dict[str, Any] = None, **kwargs) -> Any:
        return self.request("GET", url, params=params, **kwargs)

    def post(self, url: str, json_data: Dict[str, Any] = None, data: Any = None, **kwargs) -> Any:
        return self.request("POST", url, json_data=json_data, data=data, **kwargs)
