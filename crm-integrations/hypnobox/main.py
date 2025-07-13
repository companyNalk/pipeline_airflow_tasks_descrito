import json
import time

from commons.app_inicializer import AppInitializer
from commons.big_query import BigQuery
from commons.memory_monitor import MemoryMonitor
from commons.report_generator import ReportGenerator
from commons.utils import Utils
from generic.argument_manager import ArgumentManager
from generic.http_client import HttpClient
from generic.rate_limiter import RateLimiter

logger = AppInitializer.initialize()

CONFIG = {
    "rate_limit": 100,
    "base_url_template": "https://{subdomain}.hypnobox.com.br",
    "endpoints": {
        "products": {
            "path": "api/products",
            "data_key": "Produtos",
            "has_pagination": False
        },
        "clients": {
            "path": "api/clients-v2.json",
            "data_key": "Clientes",
            "has_pagination": True,
            "pagination_key": "Paginacao"
        },
        "visitas": {
            "path": "api/visitas",
            "data_key": "Visitas",
            "has_pagination": True,
            "pagination_key": "Paginacao"
        },
        "propostas": {
            "path": "api/consultaproposta",
            "data_key": "Propostas",
            "has_pagination": True,
            "pagination_key": "Paginacao"
        },
        "tarefas": {
            "path": "api/consultatarefa",
            "data_key": "Tarefas",
            "has_pagination": True,
            "pagination_key": "Paginacao"
        }
    }
}


def get_arguments():
    """Configura e retorna os argumentos da linha de comando."""
    return (ArgumentManager("Script para coletar e processar dados da API Planik")
            .add("API_SUBDOMAIN", "Subdomínio da API (ex: planik)", required=True)
            .add("API_LOGIN", "Login para autenticação", required=True)
            .add("API_PASSWORD", "Senha para autenticação", required=True)
            .add("PROJECT_ID", "ID do projeto GCS", required=True)
            .add("CRM_TYPE", "Ferramenta: Nome aba sheets", required=True)
            .add("GOOGLE_APPLICATION_CREDENTIALS", "Credenciais GCS", required=True)
            .parse())


def get_auth_token(http_client, login, password):
    """Obtém o token de autenticação."""
    logger.info("🔐 Obtendo token de autenticação")

    try:
        params = {
            "login": login,
            "password": password,
            "returnType": "json"
        }

        response = http_client.get("api/auth", params=params)

        # Parse da resposta JSON se necessário
        if isinstance(response, str):
            response = json.loads(response)

        token = response.get('token')
        if not token:
            raise Exception("Token não encontrado na resposta da autenticação")

        logger.info("✅ Token de autenticação obtido com sucesso")
        return token

    except Exception as e:
        logger.error(f"❌ Erro ao obter token: {str(e)}")
        raise


def fetch_all_data(http_client, endpoint_name, endpoint_config, token):
    """Busca todos os dados de um endpoint."""
    logger.info(f"📚 Buscando dados para: {endpoint_name}")
    start_time = time.time()
    all_items = []

    # Verificar se o endpoint tem paginação
    if not endpoint_config.get('has_pagination', True):
        # Endpoint sem paginação (como products)
        try:
            params = {"token": token, "returnType": "json"}
            data = http_client.get(endpoint_config['path'], params=params)

            # Parse da resposta JSON se necessário
            if isinstance(data, str):
                data = json.loads(data)

            items = data.get(endpoint_config['data_key'], [])
            all_items.extend(items)

            logger.info(f"📄 Endpoint {endpoint_name}: {len(items)} itens obtidos")

        except Exception as e:
            logger.error(f"❌ Erro ao buscar dados para {endpoint_name}: {str(e)}")
            raise
    else:
        # Endpoint com paginação
        page_num = 1

        while True:
            try:
                params = {"token": token, "pagina": page_num}
                data = http_client.get(endpoint_config['path'], params=params)

                # Parse da resposta JSON se necessário
                if isinstance(data, str):
                    data = json.loads(data)

                items = data.get(endpoint_config['data_key'], [])
                all_items.extend(items)

                # Verificar informações de paginação
                pagination_info = data.get(endpoint_config['pagination_key'], {})
                total_pages = int(pagination_info.get('NumerodePaginas', 1))

                if page_num == 1 or page_num == total_pages or page_num % 20 == 0:
                    logger.info(f"📄 Endpoint {endpoint_name}: página {page_num}/{total_pages} com {len(items)} itens")

                if page_num >= total_pages:
                    break

                page_num += 1
                time.sleep(0.5)  # Pausa entre requisições

            except Exception as e:
                logger.error(f"❌ Erro na página {page_num} para {endpoint_name}: {str(e)}")
                raise

    duration = time.time() - start_time
    logger.info(f"✅ Endpoint {endpoint_name}: {len(all_items)} itens obtidos em {duration:.2f}s")
    return all_items


def process_endpoint(endpoint_name, endpoint_config, token, http_client):
    """Processa um endpoint específico e retorna estatísticas."""
    logger.info(f"\n{'=' * 50}\n🔍 PROCESSANDO: {endpoint_name.upper()}\n{'=' * 50}")

    try:
        # Buscar e processar dados
        start_time = time.time()
        raw_data = fetch_all_data(http_client, endpoint_name, endpoint_config, token)

        # Processar dados
        logger.info(f"💾 Processando e salvando {len(raw_data)} registros para {endpoint_name}")
        processed_data = Utils.process_and_save_data(raw_data, endpoint_name, use_pascal_case_conversion=True)

        return {
            "registros": len(processed_data),
            "status": "Sucesso",
            "tempo": time.time() - start_time
        }

    except Exception as e:
        logger.exception(f"❌ Falha no endpoint {endpoint_name}")
        return {"registros": 0, "status": f"Falha: {type(e).__name__}: {str(e)}", "tempo": 0}


def main():
    """Função principal para coleta de dados."""
    args = get_arguments()
    global_start_time = ReportGenerator.init_report(logger)
    endpoint_stats = {}

    try:
        # Construir URL base com subdomínio
        base_url = CONFIG["base_url_template"].format(subdomain=args.API_SUBDOMAIN)

        # Configurar cliente HTTP
        rate_limiter = RateLimiter(requests_per_window=CONFIG["rate_limit"], logger=logger)
        http_client = HttpClient(base_url=base_url, rate_limiter=rate_limiter, logger=logger)

        # Obter token de autenticação
        token = get_auth_token(http_client, args.API_LOGIN, args.API_PASSWORD)

        # Processar cada endpoint
        for endpoint_name, endpoint_config in CONFIG["endpoints"].items():
            endpoint_stats[endpoint_name] = process_endpoint(endpoint_name, endpoint_config, token, http_client)
            logger.info(
                f"✅ {endpoint_name}: {endpoint_stats[endpoint_name]['registros']} registros em {endpoint_stats[endpoint_name]['tempo']:.2f}s")

        if not ReportGenerator.final_summary(logger, endpoint_stats, global_start_time):
            raise Exception("Falhas encontradas na execução")

        with MemoryMonitor(logger):
            BigQuery.process_csv_files()

        tables = Utils.get_existing_folders(logger)
        for table in tables:
            BigQuery.start_pipeline(args.PROJECT_ID, args.CRM_TYPE, table_name=table,
                                    credentials_path=args.GOOGLE_APPLICATION_CREDENTIALS)

    except Exception as e:
        logger.exception(f"❌ ERRO CRÍTICO: {e}")
        raise


if __name__ == "__main__":
    main()
