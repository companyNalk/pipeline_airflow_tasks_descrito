import functools
import logging
import time
import traceback

logger = logging.getLogger(__name__)


def airflow_error_handler(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        func_name = func.__name__
        start_time = time.time()

        # Configurações para tratamento de rate limit
        max_rate_limit_retries = 3
        rate_limit_retry_count = 0
        rate_limit_backoff = 5  # segundos iniciais

        while True:
            try:
                logger.debug(f"⏱️ Iniciando {func_name}")
                result = func(*args, **kwargs)
                duration = time.time() - start_time
                logger.debug(f"✓ {func_name} concluído em {duration:.2f}s")
                return result

            except Exception as e:
                duration = time.time() - start_time
                error_msg = str(e)
                error_type = type(e).__name__

                # Verificar se é um erro de rate limit (429)
                is_rate_limit = "429" in error_msg or "too many requests" in error_msg.lower()

                # Tentar novamente se for rate limit e não excedemos as tentativas
                if is_rate_limit and rate_limit_retry_count < max_rate_limit_retries:
                    # Aumentar tempo de espera exponencialmente
                    wait_time = rate_limit_backoff * (2 ** rate_limit_retry_count)
                    rate_limit_retry_count += 1

                    logger.warning(f"{'~' * 80}")
                    logger.warning(
                        f"⏳ RATE LIMIT em {func_name} - Tentativa {rate_limit_retry_count}/{max_rate_limit_retries}")
                    logger.warning(f"⏳ Aguardando {wait_time}s antes de tentar novamente...")
                    logger.warning(f"{'~' * 80}")

                    time.sleep(wait_time)
                    continue

                # Se chegamos aqui, ou não é rate limit ou excedemos as tentativas
                # Log simplificado para rate limit (evita poluição de logs)
                if is_rate_limit:
                    logger.error(f"{'-' * 80}")
                    logger.error(f"⛔ LIMITE DE TAXA EXCEDIDO em {func_name} após {duration:.2f}s")

                    if rate_limit_retry_count > 0:
                        logger.error(f"⚠️ Tentativas realizadas: {rate_limit_retry_count}/{max_rate_limit_retries}")

                    logger.error("⚙️ Recomendação: Reduzir MAX_WORKERS ou aumentar intervalo entre requisições")
                    logger.error(f"{'-' * 80}")

                    # Criar uma exceção personalizada para melhor tratamento pelo Airflow
                    rate_limit_ex = Exception(
                        f"Rate limit excedido em {func_name} após {rate_limit_retry_count} tentativas")
                    raise rate_limit_ex from None  # Suprimir a causa original (evita stacktrace duplicado)

                # Para outros tipos de erros, log detalhado com traceback
                else:
                    logger.error(f"\n{'=' * 80}")
                    logger.error(f"❌ ERRO em {func_name} após {duration:.2f}s: {error_type}: {error_msg}")
                    tb_str = traceback.format_exc()
                    logger.error(f"📋 TRACEBACK:\n{tb_str}")
                    logger.error(f"{'=' * 80}")
                    raise  # Deixa a exceção original propagar

    return wrapper
