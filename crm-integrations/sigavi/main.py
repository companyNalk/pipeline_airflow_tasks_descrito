import logging
import time

from commons.app_inicializer import AppInitializer
from commons.report_generator import ReportGenerator
from commons.utils import Utils
from generic.argument_manager import ArgumentManager
from generic.http_client import HttpClient
from generic.rate_limiter import RateLimiter

logger = AppInitializer.initialize()

RATE_LIMIT = 100
ENDPOINTS = {
    "fac_lista": "api/crm/fac/lista",
}


def get_arguments():
    """Configura e retorna os argumentos da linha de comando."""
    return (ArgumentManager("Script para coletar e processar dados da API Sigavi360")
            .add("API_BASE_URL", "URL base", required=True)
            .add("API_USERNAME", "Username para autenticação", required=True)
            .add("API_PASSWORD", "Senha para autenticação", required=True)
            .parse())


def get_auth_token(base_url, username, password, grant_type):
    """Obtém o token de autenticação da API."""
    logger.info("🔑 Obtendo token de autenticação")

    auth_url = f"{base_url}/Sigavi/api/Acesso/Token"
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    payload = {
        "username": username,
        "password": password,
        "grant_type": grant_type
    }

    try:
        response = http_client.post(
            url=auth_url,
            headers=headers,
            data=payload,
            debug_info="auth_token"
        )

        if not response or "access_token" not in response:
            raise Exception(f"Falha ao obter token: {response}")

        access_token = response.get("access_token")
        logger.info("✅ Token de autenticação obtido com sucesso")
        return access_token

    except Exception as e:
        logger.error(f"❌ Erro ao obter token de autenticação: {str(e)}")
        raise


def fetch_data(endpoint, token):
    """Busca dados de um endpoint específico da API."""
    headers = {
        "Authorization": f"bearer {token}",
        "Content-Type": "application/json"
    }

    try:
        logger.info(f"📡 Consultando endpoint: {endpoint}")
        response = http_client.post(
            url=endpoint,
            headers=headers,
            data={},
            debug_info=f"{endpoint}"
        )

        if not response:
            raise Exception("Resposta vazia ou inválida")

        logger.info(f"✅ Dados obtidos com sucesso do endpoint: {endpoint}")
        return response

    except Exception as e:
        logger.error(f"❌ Erro ao buscar dados para {endpoint}: {str(e)}")
        raise


def process_endpoint(endpoint_name, endpoint_path, token):
    """Busca, processa e salva todos os dados do endpoint."""
    try:
        logger.info(f"\n{'=' * 50}\n🔍 PROCESSANDO ENDPOINT: {endpoint_name.upper()}\n{'=' * 50}")

        endpoint_start = time.time()

        headers = {
            "Authorization": f"bearer {token}",
            "Content-Type": "application/json"
        }

        all_data = []
        pagina_atual = 1
        pagina_total = 1

        logger.info(f"📡 Iniciando consulta paginada no endpoint: {endpoint_path}")

        full_url = f"{http_client.base_url}/{endpoint_path}"
        logger.info(f"URL completa: {full_url}")

        while pagina_atual <= pagina_total:
            logger.debug(f"🔁 Página {pagina_atual} de {pagina_total}")

            response = http_client.post(
                url=endpoint_path,
                headers=headers,
                data={"pagina": pagina_atual},
                debug_info=f"{endpoint_path} - página {pagina_atual}"
            )

            if not response:
                logger.warning(f"Resposta vazia ou inválida para página {pagina_atual}")
                break

            # Adiciona log detalhado da resposta para diagnóstico
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Tipo de resposta: {type(response)}")

            # Captura a quantidade total de páginas
            if isinstance(response, dict) and "PaginaTotal" in response:
                pagina_total = response.get("PaginaTotal", 1)
                logger.info(f"Total de páginas encontrado: {pagina_total}")

            if isinstance(response, list):
                all_data.extend(response)
            elif isinstance(response, dict):
                if "data" in response and isinstance(response["data"], list):
                    all_data.extend(response["data"])
                elif "items" in response and isinstance(response["items"], list):
                    all_data.extend(response["items"])
                elif isinstance(response, dict):
                    all_data.append(response)

            pagina_atual += 1

        if not all_data:
            logger.warning("⚠️ Nenhum dado foi encontrado após processar todas as páginas")
            return {
                "registros": 0,
                "status": "Vazio",
                "tempo": time.time() - endpoint_start
            }

        logger.info(f"✅ Total de {len(all_data)} registros encontrados")

        # Usando Utils.process_and_save_data exatamente como no modelo base
        logger.info(f"💾 Processando e salvando {len(all_data)} registros para {endpoint_name}")
        processed_data = Utils.process_and_save_data(all_data, endpoint_name)

        endpoint_duration = time.time() - endpoint_start
        return {
            "registros": len(processed_data),
            "status": "Sucesso",
            "tempo": endpoint_duration
        }

    except Exception as e:
        logger.exception(f"❌ Falha no endpoint {endpoint_name}")
        return {
            "registros": 0,
            "status": f"Falha: {type(e).__name__}: {str(e)}",
            "tempo": 0
        }


def main():
    """Função principal para coleta de dados."""
    # 1. Obter argumentos de linha de comando
    args = get_arguments()

    # 2. Configurar cliente HTTP
    global http_client
    api_base_url = args.API_BASE_URL.rstrip('/')
    api_username = args.API_USERNAME
    api_password = args.API_PASSWORD
    api_grant_type = 'password'

    rate_limiter = RateLimiter(requests_per_window=RATE_LIMIT, logger=logger)
    http_client = HttpClient(base_url=api_base_url, rate_limiter=rate_limiter, logger=logger)

    # 3. Iniciar relatório
    global_start_time = ReportGenerator.init_report(logger)
    endpoint_stats = {}

    try:
        # 4. Obter token de autenticação
        auth_token = get_auth_token(api_base_url, api_username, api_password, api_grant_type)

        # 5. Processar todos os endpoints
        for endpoint_name, endpoint_path in ENDPOINTS.items():
            endpoint_stats[endpoint_name] = process_endpoint(endpoint_name, endpoint_path, auth_token)
            logger.info(
                f"✅ {endpoint_name}: {endpoint_stats[endpoint_name]['registros']} registros em {endpoint_stats[endpoint_name]['tempo']:.2f}s")

        # 6. Gerar resumo final
        success = ReportGenerator.final_summary(logger, endpoint_stats, global_start_time)

        # Se houver falhas, lançar exceção
        if not success:
            raise Exception(f"Falhas nos endpoints: {success}")

    except Exception as e:
        logger.exception(f"❌ ERRO CRÍTICO NA EXECUÇÃO: {e}")
        raise


if __name__ == "__main__":
    main()
