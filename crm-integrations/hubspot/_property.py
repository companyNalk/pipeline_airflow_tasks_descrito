import os
import time

import hubspot
from hubspot.crm.properties import ApiException

from commons.app_inicializer import AppInitializer
from commons.report_generator import ReportGenerator
from commons.utils import Utils
from generic.argument_manager import ArgumentManager

logger = AppInitializer.initialize()

MAX_IDS = 100
RATE_LIMIT = 100
MAX_WORKERS = min(10, os.cpu_count() or 5)


def get_arguments():
    """Configura e retorna os argumentos da linha de comando."""
    return (ArgumentManager("Script para coletar e processar propriedades da API HubSpot")
            .add("ACCESS_TOKEN", "Token de acesso para autenticação", required=True)
            .add("OBJECT_TYPE", "Tipo de objeto (deals, contacts, companies, etc)", required=False, default="deals")
            .parse())


def init_hubspot_client(access_token):
    """Inicializa o cliente HubSpot com o token de acesso."""
    logger.info("🔑 Inicializando cliente HubSpot")
    try:
        client = hubspot.Client.create(access_token=access_token)
        logger.info("✅ Cliente HubSpot inicializado com sucesso")
        return client
    except Exception as e:
        logger.error(f"❌ Erro ao inicializar cliente HubSpot: {str(e)}")
        raise


def get_all_properties(client, object_type="contacts"):
    """Busca todas as propriedades de um tipo de objeto do HubSpot."""
    logger.info(f"🔍 Buscando todas as propriedades de {object_type}")
    try:
        start_time = time.time()

        # Obter as propriedades
        properties_response = client.crm.properties.core_api.get_all(object_type)
        properties = properties_response.results

        duration = time.time() - start_time
        logger.info(f"✅ {len(properties)} propriedades obtidas em {duration:.2f}s")

        # Processar propriedades para o formato esperado (simplificado)
        processed_properties = []
        for prop in properties:
            # Simplificado para seguir o exemplo fornecido
            property_dict = {
                "property": prop.name,
                "label": prop.label
            }

            processed_properties.append(property_dict)

        return processed_properties
    except ApiException as e:
        logger.error(f"❌ API Exception ao buscar propriedades: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"❌ Erro ao buscar propriedades: {str(e)}")
        raise


def process_primary_endpoint(client, object_type, endpoint_name=None):
    """Processa o endpoint de propriedades como um endpoint principal."""
    if endpoint_name is None:
        endpoint_name = f"hubspot_{object_type}_properties"

    try:
        logger.info(f"\n{'=' * 50}\n🔍 PROCESSANDO ENDPOINT: {endpoint_name.upper()}\n{'=' * 50}")

        endpoint_start = time.time()

        # Buscar todas as propriedades
        raw_data = get_all_properties(client, object_type)

        # Processar e salvar usando o Utils do projeto
        logger.info(f"💾 Processando e salvando {len(raw_data)} registros para {endpoint_name}")
        processed_data = Utils.process_and_save_data(raw_data, endpoint_name)

        endpoint_duration = time.time() - endpoint_start

        # Estatísticas de processamento
        stats = {
            "registros": len(processed_data),
            "status": "Sucesso",
            "tempo": endpoint_duration
        }

        logger.info(f"✅ {endpoint_name}: {len(processed_data)} registros em {endpoint_duration:.2f}s")

        return processed_data, stats

    except Exception as e:
        logger.exception(f"❌ Falha no endpoint {endpoint_name}")

        # Estatísticas em caso de erro
        stats = {
            "registros": 0,
            "status": f"Falha: {type(e).__name__}: {str(e)}",
            "tempo": 0
        }

        return [], stats


def main():
    """Função principal para coleta de dados de propriedades do HubSpot."""
    # Iniciar relatório de processamento
    global_start_time = ReportGenerator.init_report(logger)

    try:
        # 1. Obter configurações
        args = get_arguments()

        access_token = args.ACCESS_TOKEN
        object_type = args.OBJECT_TYPE

        # 2. Inicializar cliente HubSpot
        hubspot_client = init_hubspot_client(access_token)

        # 3. Processamento de endpoints principais
        endpoint_stats = {}
        endpoint_name = f"hubspot_{object_type}_properties"

        # Processar endpoint principal
        processed_data, stats = process_primary_endpoint(hubspot_client, object_type, endpoint_name)
        endpoint_stats[endpoint_name] = stats

        # 4. Gerar resumo final
        success = ReportGenerator.final_summary(logger, endpoint_stats, global_start_time)

        # Se houver falhas, lançar exceção
        if not success:
            raise Exception(f"Falhas no processamento: {success}")

    except Exception as e:
        logger.exception(f"❌ ERRO CRÍTICO NA EXECUÇÃO: {e}")
        raise


if __name__ == "__main__":
    main()
