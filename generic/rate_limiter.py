import logging
import threading
import time
from typing import Optional


class RateLimitFilter(logging.Filter):
    last_logged = 0

    def filter(self, record):
        message = record.getMessage()

        rate_limit_patterns = ["Rate limit", "RATE LIMIT", "429", "too many requests", "Too Many Requests",
                               "Aguardando", "⏳"]

        # Verifica se a mensagem contém algum dos padrões
        is_rate_limit_message = any(pattern in message for pattern in rate_limit_patterns)

        if is_rate_limit_message:
            # Apenas logar mensagens de rate limit uma vez por minuto para evitar spam
            current_time = time.time()
            if current_time - RateLimitFilter.last_logged > 60:
                RateLimitFilter.last_logged = current_time
                return True
            return False
        return True


class RateLimiter:
    def __init__(self, requests_per_window: int = 100, window_seconds: int = 60, check_every: int = 5,
                 logger: Optional[logging.Logger] = None):
        self.requests_per_window = requests_per_window
        self.window_seconds = window_seconds
        self.check_every = check_every
        self.logger = logger or logging.getLogger(__name__)

        # Aplicar o filtro de rate limit ao logger
        # Importante: se o logger já tiver handlers, aplicamos o filtro a cada handler
        # e também ao próprio logger para capturar todos os casos
        self.logger.addFilter(RateLimitFilter())

        # Também aplicar o filtro a todos os handlers existentes
        for handler in self.logger.handlers:
            handler.addFilter(RateLimitFilter())

        # Variáveis de controle
        self.lock = threading.Lock()
        self.request_counter = 0
        self.reset_time = time.time() + window_seconds

    def check(self) -> None:
        """
        Verifica se o limite de requisições foi atingido e aguarda se necessário.
        Esta função deve ser chamada antes de cada requisição.
        """
        with self.lock:
            current_time = time.time()

            # Verificações periódicas para reduzir overhead
            if self.request_counter % self.check_every == 0:
                # Reinicia contador se a janela de tempo acabou
                if current_time >= self.reset_time:
                    self.request_counter = 0
                    self.reset_time = current_time + self.window_seconds

                # Se atingiu o limite, aguarda até o final da janela
                if self.request_counter >= self.requests_per_window:
                    wait_time = self.reset_time - current_time
                    wait_time = max(0.1, wait_time)  # Pelo menos 100ms para evitar busy-waiting

                    self.logger.info(f"⏳ Rate limit atingido. Aguardando {wait_time:.2f}s...")
                    time.sleep(wait_time)

                    # Reinicia o contador e o timer
                    self.request_counter = 0
                    self.reset_time = time.time() + self.window_seconds
                    self.logger.info("▶️ Continuando após pausa de rate limit")

            # Incrementa o contador a cada chamada
            self.request_counter += 1

    def get_remaining(self) -> int:
        """
        Retorna o número de requisições restantes na janela de tempo atual.
        """
        with self.lock:
            # Verifica se a janela já acabou
            current_time = time.time()
            if current_time >= self.reset_time:
                return self.requests_per_window

            remaining = self.requests_per_window - self.request_counter
            return max(0, remaining)

    def get_reset_time(self) -> float:
        """
        Retorna o tempo restante em segundos até o reset do contador.
        """
        with self.lock:
            current_time = time.time()
            return max(0, self.reset_time - current_time)
