import os
import time

import hubspot
from hubspot.crm.deals import ApiException

from commons.app_inicializer import AppInitializer
from commons.report_generator import ReportGenerator
from commons.utils import Utils
from generic.argument_manager import ArgumentManager

logger = AppInitializer.initialize()

MAX_PROPERTIES_PER_REQUEST = 250
MAX_IDS = 100
RATE_LIMIT = 100
MAX_WORKERS = min(10, os.cpu_count() or 5)
LIMIT_PER_PAGE = 100


def get_arguments():
    """Configura e retorna os argumentos da linha de comando."""
    return (ArgumentManager("Script para coletar e processar dados da API HubSpot")
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


def get_all_deal_properties(client):
    """Busca todas as propriedades disponíveis para deals."""
    logger.info("🔍 Buscando todas as propriedades de deals")
    try:
        start_time = time.time()
        response = client.crm.properties.core_api.get_all('deals')

        all_properties = [prop.name for prop in response.results]

        duration = time.time() - start_time
        logger.info(f"✅ {len(all_properties)} propriedades de deals obtidas em {duration:.2f}s")
        return all_properties
    except Exception as e:
        logger.error(f"❌ Erro ao buscar propriedades de deals: {str(e)}")
        raise


def create_property_groups(properties, group_size=250):
    """Divide as propriedades em grupos para evitar limites da API."""
    return [properties[i:i + group_size] for i in range(0, len(properties), group_size)]


def fetch_deals_page(client, properties, limit, after=None):
    """Busca uma página de deals com as propriedades especificadas."""
    try:
        response = client.crm.deals.basic_api.get_page(
            limit=limit,
            properties=properties,
            after=after
        )
        return response
    except ApiException as e:
        logger.error(f"❌ API Exception ao buscar página de deals: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"❌ Erro ao buscar página de deals: {str(e)}")
        raise


def process_deals_batch(client, property_groups, limit, after=None):
    """Processa um lote de deals para cada grupo de propriedades."""
    all_items = []
    has_more = True
    current_after = after

    while has_more:
        batch_start_time = time.time()
        logger.info(f"🔄 Buscando página de deals com after={current_after}")

        for group_index, properties in enumerate(property_groups):
            try:
                api_response = fetch_deals_page(client, properties, limit, current_after)
                deals = api_response.results

                # Atualizar flag de paginação
                has_more = api_response.paging is not None
                current_after = api_response.paging.next.after if has_more else None

                # Processar cada deal
                for deal in deals:
                    deal_id = deal.id
                    # Verificar se já processamos este deal
                    existing_deal = next((item for item in all_items if item.get("hs_object_id") == deal_id), None)

                    if existing_deal:
                        # Adicionar propriedades ao deal existente
                        for property_name in properties:
                            existing_deal[property_name] = deal.properties.get(property_name, "")
                    else:
                        # Criar novo deal
                        new_deal = {"hs_object_id": deal_id}
                        for property_name in properties:
                            new_deal[property_name] = deal.properties.get(property_name, "")
                        all_items.append(new_deal)

                logger.info(f"✓ Grupo {group_index + 1}/{len(property_groups)} com {len(deals)} deals processados")

            except Exception as e:
                logger.error(f"❌ Erro no grupo {group_index + 1}: {str(e)}")
                # Continuar para o próximo grupo em caso de erro

        batch_duration = time.time() - batch_start_time
        logger.info(f"✓ Página de deals processada em {batch_duration:.2f}s, deals totais: {len(all_items)}")

        if not has_more:
            logger.info("🏁 Todas as páginas de deals processadas")
            break

    return all_items


def process_primary_endpoint(client, endpoint_name="hubspot_deals", limit=LIMIT_PER_PAGE):
    """Processa o endpoint de deals como um endpoint principal."""
    try:
        logger.info(f"\n{'=' * 50}\n🔍 PROCESSANDO ENDPOINT: {endpoint_name.upper()}\n{'=' * 50}")

        endpoint_start = time.time()

        # 1. Obter todas as propriedades
        all_properties = get_all_deal_properties(client)

        # 2. Dividir propriedades em grupos
        property_groups = create_property_groups(all_properties, MAX_PROPERTIES_PER_REQUEST)
        logger.info(f"📊 Propriedades divididas em {len(property_groups)} grupos de requisição")

        # 3. Processar todos os deals
        raw_data = process_deals_batch(client, property_groups, limit)

        # 4. Processar e salvar usando o Utils do projeto
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
    """Função principal para coleta de dados do HubSpot."""
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
        endpoint_name = "hubspot_deals"

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
