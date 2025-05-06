import asyncio
import os
import time

import aiohttp
import hubspot

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
    return (ArgumentManager("Script para coletar e processar contatos da API HubSpot")
            .add("ACCESS_TOKEN", "Token de acesso para autenticação", required=True)
            .add("AFTER_KEY", "Chave de paginação para continuar a partir do último processamento", required=False)
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


def get_all_contact_properties(client):
    """Busca todas as propriedades disponíveis para contatos."""
    logger.info("🔍 Buscando todas as propriedades de contatos")
    try:
        start_time = time.time()
        response = client.crm.properties.core_api.get_all('contacts')

        # Limitar a 700 propriedades para evitar problemas de performance
        all_properties = [prop.name for prop in response.results][:700]

        duration = time.time() - start_time
        logger.info(f"✅ {len(all_properties)} propriedades de contatos obtidas em {duration:.2f}s")
        return all_properties
    except Exception as e:
        logger.error(f"❌ Erro ao buscar propriedades de contatos: {str(e)}")
        raise


def create_property_groups(properties, group_size=250):
    """Divide as propriedades em grupos para evitar limites da API."""
    return [properties[i:i + group_size] for i in range(0, len(properties), group_size)]


async def fetch_contacts_page(access_token, after=None, session=None):
    """Busca uma página de contatos usando aiohttp."""
    url = "https://api.hubapi.com/crm/v3/objects/contacts"
    params = {"limit": LIMIT_PER_PAGE}
    if after:
        params["after"] = after

    headers = {"Authorization": f"Bearer {access_token}"}

    # Usar a sessão fornecida ou criar uma nova
    close_session = False
    if session is None:
        session = aiohttp.ClientSession()
        close_session = True

    try:
        async with session.get(url, params=params, headers=headers) as response:
            response.raise_for_status()
            data = await response.json()
            return data
    except aiohttp.ClientError as e:
        logger.error(f"❌ Erro HTTP ao buscar contatos: {str(e)}")
        return None
    finally:
        if close_session:
            await session.close()


async def process_contacts_batch_async(access_token, after=None, max_contacts=100000):
    """Processa lotes de contatos usando requisições assíncronas."""
    logger.info(f"🔄 Iniciando coleta de contatos a partir de: {after or 'início'}")

    all_contacts = []
    current_after = after
    batch_count = 0

    async with aiohttp.ClientSession() as session:
        while True:
            batch_start_time = time.time()
            data = await fetch_contacts_page(access_token, current_after, session)

            if data is None or 'results' not in data:
                logger.error("❌ Falha ao obter resultados da API")
                break

            contacts = data['results']
            all_contacts.extend(contacts)
            batch_count += 1

            # Log de progresso
            if batch_count % 10 == 0:
                logger.info(f"📊 Progresso: {len(all_contacts)} contatos coletados em {batch_count} lotes")

            # Verificar se temos paginação
            if 'paging' not in data or 'next' not in data['paging']:
                logger.info("🏁 Fim da paginação alcançado")
                break

            # Atualizar cursor de paginação
            current_after = data['paging']['next']['after']

            # Limitar número de contatos
            if len(all_contacts) >= max_contacts:
                logger.info(f"🛑 Limite de {max_contacts} contatos atingido")
                break

            batch_duration = time.time() - batch_start_time
            logger.info(f"✓ Lote {batch_count} processado em {batch_duration:.2f}s, {len(contacts)} contatos")

    # Transformar contatos para o formato esperado pelo Utils
    processed_contacts = []
    for contact in all_contacts:
        processed_contact = {
            'id': contact['id'],
            'createdAt': contact.get('createdAt', ''),
            'updatedAt': contact.get('updatedAt', ''),
            'archived': str(contact.get('archived', False))
        }
        # Adicionar todas as propriedades
        if 'properties' in contact:
            for prop_name, prop_value in contact['properties'].items():
                processed_contact[prop_name] = prop_value

        processed_contacts.append(processed_contact)

    return processed_contacts, current_after


def process_primary_endpoint(access_token, endpoint_name="hubspot_contacts", after_key=None):
    """Processa o endpoint de contatos como um endpoint principal."""
    try:
        logger.info(f"\n{'=' * 50}\n🔍 PROCESSANDO ENDPOINT: {endpoint_name.upper()}\n{'=' * 50}")

        endpoint_start = time.time()

        # Usar asyncio para processamento assíncrono
        loop = asyncio.get_event_loop()
        raw_data, last_after = loop.run_until_complete(
            process_contacts_batch_async(access_token, after_key)
        )

        # Processar e salvar usando o Utils do projeto
        logger.info(f"💾 Processando e salvando {len(raw_data)} registros para {endpoint_name}")
        processed_data = Utils.process_and_save_data(raw_data, endpoint_name)

        endpoint_duration = time.time() - endpoint_start

        # Estatísticas de processamento
        stats = {
            "registros": len(processed_data),
            "status": "Sucesso",
            "tempo": endpoint_duration,
            "ultimo_after": last_after
        }

        logger.info(f"✅ {endpoint_name}: {len(processed_data)} registros em {endpoint_duration:.2f}s")
        logger.info(f"📌 Último cursor de paginação: {last_after}")

        return processed_data, stats

    except Exception as e:
        logger.exception(f"❌ Falha no endpoint {endpoint_name}")

        # Estatísticas em caso de erro
        stats = {
            "registros": 0,
            "status": f"Falha: {type(e).__name__}: {str(e)}",
            "tempo": 0,
            "ultimo_after": after_key
        }

        return [], stats


def main():
    """Função principal para coleta de dados de contatos do HubSpot."""
    # Iniciar relatório de processamento
    global_start_time = ReportGenerator.init_report(logger)

    try:
        # 1. Obter configurações
        args = get_arguments()

        access_token = args.ACCESS_TOKEN
        after_key = getattr(args, 'AFTER_KEY', None)

        # 2. Processamento de endpoints principais
        endpoint_stats = {}
        endpoint_name = "hubspot_contacts"

        # Processar endpoint principal
        processed_data, stats = process_primary_endpoint(access_token, endpoint_name, after_key)
        endpoint_stats[endpoint_name] = stats

        # 3. Gerar resumo final
        success = ReportGenerator.final_summary(logger, endpoint_stats, global_start_time)

        # 4. Registrar último cursor de paginação para uso futuro
        if success and stats.get("ultimo_after"):
            logger.info(f"💾 Salvando último cursor de paginação: {stats['ultimo_after']}")
            # Aqui você pode implementar o salvamento do cursor em um arquivo ou banco de dados

        # Se houver falhas, lançar exceção
        if not success:
            raise Exception(f"Falhas no processamento: {success}")

    except Exception as e:
        logger.exception(f"❌ ERRO CRÍTICO NA EXECUÇÃO: {e}")
        raise


if __name__ == "__main__":
    main()
