import time
import requests

from commons.app_inicializer import AppInitializer
from commons.big_query import BigQuery
from commons.memory_monitor import MemoryMonitor
from commons.report_generator import ReportGenerator
from commons.utils import Utils
from generic.argument_manager import ArgumentManager

logger = AppInitializer.initialize()

CONFIG = {
    "base_url": "https://expoconecta.native-infinity.com.br"
}


def get_arguments():
    """Configura e retorna os argumentos da linha de comando."""
    return (ArgumentManager("Script para coletar dados da API ExpoConecta")
            .add("API_USERNAME", "Username para autenticação", required=True)
            .add("API_PASSWORD", "Password para autenticação", required=True)
            .add("API_REPORT_IDS", "IDs dos reports separados por vírgula (ex: 1,4,5,6)", required=True)
            .add("PROJECT_ID", "ID do projeto GCS", required=True)
            .add("CRM_TYPE", "Ferramenta: Nome aba sheets", required=True)
            .add("GOOGLE_APPLICATION_CREDENTIALS", "Credenciais GCS", required=True)
            .parse())


def get_auth_token(username, password):
    """Obtém o token de autenticação."""
    logger.info("🔐 Obtendo token de autenticação...")

    auth_url = f"{CONFIG['base_url']}/api/token"
    payload = {
        "username": username,
        "password": password
    }

    try:
        response = requests.post(auth_url, json=payload)
        response.raise_for_status()

        token = response.json().get('token')
        if not token:
            raise Exception("Token não encontrado na resposta")

        logger.info("✅ Token obtido com sucesso")
        return token

    except Exception as e:
        logger.error(f"❌ Erro ao obter token: {str(e)}")
        raise


def fetch_endpoint_data(token, endpoint_id):
    """Busca dados de um endpoint específico."""
    logger.info(f"📚 Buscando dados do endpoint ID: {endpoint_id}")

    url = f"{CONFIG['base_url']}/api/apiReport/{endpoint_id}"
    headers = {"Authorization": f"Bearer {token}"}

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        data = response.json().get('data', [])
        logger.info(f"✅ Endpoint {endpoint_id}: {len(data)} registros obtidos")
        return data

    except Exception as e:
        logger.error(f"❌ Erro no endpoint {endpoint_id}: {str(e)}")
        raise


def process_and_save_data(data, endpoint_id):
    """Processa e salva os dados usando o padrão do script original."""
    if not data:
        logger.warning(f"⚠️ Nenhum dado para processar no endpoint {endpoint_id}")
        return []

    logger.info(f"💾 Processando e salvando {len(data)} registros para report_{endpoint_id}")

    # Usar o Utils.process_and_save_data do script original
    processed_data = Utils.process_and_save_data(data, f"report_{endpoint_id}")

    return processed_data


def process_endpoint(token, endpoint_id):
    """Processa um endpoint específico e retorna estatísticas."""
    try:
        start_time = time.time()

        # Buscar dados
        data = fetch_endpoint_data(token, endpoint_id)

        # Processar e salvar dados usando o padrão original
        processed_data = process_and_save_data(data, endpoint_id)

        return {
            "registros": len(processed_data),
            "status": "Sucesso",
            "tempo": time.time() - start_time
        }

    except Exception as e:
        logger.exception(f"❌ Falha no endpoint {endpoint_id}")
        return {
            "registros": 0,
            "status": f"Falha: {type(e).__name__}: {str(e)}",
            "tempo": 0
        }


def main():
    """Função principal para coleta de dados ExpoConecta."""
    args = get_arguments()
    global_start_time = ReportGenerator.init_report(logger)
    endpoint_stats = {}

    try:
        # Obter token de autenticação
        token = get_auth_token(args.API_USERNAME, args.API_PASSWORD)

        # Processar IDs dos reports
        report_ids = [int(id.strip()) for id in args.API_REPORT_IDS.split(',')]
        logger.info(f"📋 Reports a processar: {report_ids}")

        logger.info(f"\n{'=' * 50}\n🔍 PROCESSANDO ENDPOINTS EXPOCONECTA\n{'=' * 50}")

        # Processar cada endpoint
        for endpoint_id in report_ids:
            endpoint_stats[f"endpoint_{endpoint_id}"] = process_endpoint(token, endpoint_id)
            logger.info(
                f"✅ Endpoint {endpoint_id}: {endpoint_stats[f'endpoint_{endpoint_id}']['registros']} "
                f"registros em {endpoint_stats[f'endpoint_{endpoint_id}']['tempo']:.2f}s"
            )

            time.sleep(0.5)

        # Relatório final
        if not ReportGenerator.final_summary(logger, endpoint_stats, global_start_time):
            raise Exception("Falhas encontradas na execução")

        # Processar arquivos CSV com BigQuery
        with MemoryMonitor(logger):
            BigQuery.process_csv_files()

        # Pipeline BigQuery
        tables = Utils.get_existing_folders(logger)
        for table in tables:
            BigQuery.start_pipeline(
                args.PROJECT_ID,
                args.CRM_TYPE,
                table_name=table,
                credentials_path=args.GOOGLE_APPLICATION_CREDENTIALS
            )

    except Exception as e:
        logger.exception(f"❌ ERRO CRÍTICO: {e}")
        raise


if __name__ == "__main__":
    main()
