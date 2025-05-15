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
    return (ArgumentManager("Script para coletar e processar pipelines da API HubSpot")
            .add("ACCESS_TOKEN", "Token de acesso para autenticação", required=True)
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


def get_all_pipelines(client):
    """Busca todos os pipelines de deals do HubSpot."""
    logger.info("🔍 Buscando todos os pipelines de deals")
    try:
        start_time = time.time()

        # Obter os pipelines
        pipelines_response = client.crm.pipelines.pipelines_api.get_all("deals")
        pipelines = pipelines_response.results

        duration = time.time() - start_time
        logger.info(f"✅ {len(pipelines)} pipelines obtidos em {duration:.2f}s")

        # Processar pipelines para o formato esperado
        processed_pipelines = []
        for pipeline in pipelines:
            pipeline_dict = {
                "id": pipeline.id,
                "label": pipeline.label,
                "display_order": pipeline.display_order,
                "created_at": pipeline.created_at,
                "updated_at": pipeline.updated_at,
                "archived": pipeline.archived
            }

            # Adicionar estágios do pipeline, se disponíveis
            if hasattr(pipeline, 'stages') and pipeline.stages:
                # Criar uma representação dos estágios como propriedades adicionais
                for i, stage in enumerate(pipeline.stages):
                    prefix = f"stage_{i + 1}_"
                    pipeline_dict[f"{prefix}id"] = stage.id
                    pipeline_dict[f"{prefix}label"] = stage.label
                    pipeline_dict[f"{prefix}display_order"] = stage.display_order
                    pipeline_dict[f"{prefix}created_at"] = stage.created_at
                    pipeline_dict[f"{prefix}updated_at"] = stage.updated_at
                    pipeline_dict[f"{prefix}archived"] = stage.archived

                pipeline_dict["total_stages"] = len(pipeline.stages)
            else:
                pipeline_dict["total_stages"] = 0

            processed_pipelines.append(pipeline_dict)

        return processed_pipelines
    except ApiException as e:
        logger.error(f"❌ API Exception ao buscar pipelines: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"❌ Erro ao buscar pipelines: {str(e)}")
        raise


def process_primary_endpoint(client, endpoint_name="hubspot_pipelines"):
    """Processa o endpoint de pipelines como um endpoint principal."""
    try:
        logger.info(f"\n{'=' * 50}\n🔍 PROCESSANDO ENDPOINT: {endpoint_name.upper()}\n{'=' * 50}")

        endpoint_start = time.time()

        # Buscar todos os pipelines
        raw_data = get_all_pipelines(client)

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
    """Função principal para coleta de dados de pipelines do HubSpot."""
    # Iniciar relatório de processamento
    global_start_time = ReportGenerator.init_report(logger)

    try:
        # 1. Obter configurações
        args = get_arguments()

        access_token = args.ACCESS_TOKEN

        # 2. Inicializar cliente HubSpot
        hubspot_client = init_hubspot_client(access_token)

        # 3. Processamento de endpoints principais
        endpoint_stats = {}
        endpoint_name = "hubspot_pipelines"

        # Processar endpoint principal
        processed_data, stats = process_primary_endpoint(hubspot_client, endpoint_name)
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
