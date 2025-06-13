import ast
import asyncio
import concurrent.futures
import os
import time

import aiohttp
import hubspot
from hubspot.crm.properties import ApiException

from commons.app_inicializer import AppInitializer
from commons.report_generator import ReportGenerator
from commons.utils import Utils
from generic.argument_manager import ArgumentManager

logger = AppInitializer.initialize()

MAX_IDS = 100
RATE_LIMIT = 100
MAX_WORKERS = min(6, os.cpu_count() or 4)
MAX_PROPERTIES_PER_REQUEST = 250
LIMIT_PER_PAGE = 100


def get_arguments():
    return (ArgumentManager("Script HubSpot - Coleta com Campos Configuráveis")
            .add("ACCESS_TOKEN", "Token de acesso para autenticação", required=True)
            .parse())


args = get_arguments()


def parse_env_list(env_var_name, default='[]'):
    try:
        env_value = os.getenv(env_var_name, default)
        env_value = env_value.strip()
        parsed_list = ast.literal_eval(env_value)

        if isinstance(parsed_list, list):
            return [item.strip() if isinstance(item, str) else item for item in parsed_list]
        return parsed_list

    except (ValueError, SyntaxError):
        logger.warning(f"⚠️ Erro ao fazer parse de {env_var_name}, usando lista vazia (todos os campos)")
        return []


FIELDS_CONTACTS = parse_env_list('FIELDS_CONTACTS')
FIELDS_DEALS = parse_env_list('FIELDS_DEALS')
FIELDS_OWNERS = parse_env_list('FIELDS_OWNERS')
FIELDS_PIPELINES = parse_env_list('FIELDS_PIPELINES')
FIELDS_PROPERTIES = parse_env_list('FIELDS_PROPERTIES')

ENDPOINT_FIELD_CONFIG = {
    "hubspot_contacts": {
        "mode": "specific" if FIELDS_CONTACTS else "all",
        "fields": FIELDS_CONTACTS
    },
    "hubspot_deals": {
        "mode": "specific" if FIELDS_DEALS else "all",
        "fields": FIELDS_DEALS
    },
    "hubspot_owners": {
        "mode": "specific" if FIELDS_OWNERS else "all",
        "fields": FIELDS_OWNERS
    },
    "hubspot_pipelines": {
        "mode": "specific" if FIELDS_PIPELINES else "all",
        "fields": FIELDS_PIPELINES
    },
    "hubspot_properties": {
        "mode": "specific" if FIELDS_PROPERTIES else "all",
        "fields": FIELDS_PROPERTIES
    }
}

ENDPOINTS = {
    "hubspot_contacts": "contacts",
    "hubspot_deals": "deals",
    "hubspot_owners": "owners",
    "hubspot_pipelines": "pipelines",
    "hubspot_properties": "properties"
}

ENDPOINT_PARAMS = {
    "hubspot_deals": {"limit": LIMIT_PER_PAGE},
    "hubspot_contacts": {"limit": LIMIT_PER_PAGE},
    "hubspot_owners": {},
    "hubspot_pipelines": {"object_type": "deals"},
    "hubspot_properties": {"object_type": "contacts"}
}


def init_hubspot_client(access_token):
    logger.info("🔑 Inicializando cliente HubSpot")
    try:
        client = hubspot.Client.create(access_token=access_token)
        logger.info("✅ Cliente HubSpot inicializado com sucesso")
        return client
    except Exception as e:
        logger.error(f"❌ Erro ao inicializar cliente HubSpot: {str(e)}")
        raise


def get_endpoint_properties(client, endpoint_name, object_type):
    config = ENDPOINT_FIELD_CONFIG.get(endpoint_name, {"mode": "all", "fields": []})

    logger.info(f"🔍 Configuração para {endpoint_name}: modo='{config['mode']}'")

    if config["mode"] == "all":
        logger.info(f"🔍 Buscando TODAS as propriedades de {object_type}")
        try:
            start_time = time.time()
            response = client.crm.properties.core_api.get_all(object_type)
            all_properties = [prop.name for prop in response.results]
            duration = time.time() - start_time
            logger.info(f"✅ {len(all_properties)} propriedades obtidas em {duration:.2f}s")
            return all_properties
        except Exception as e:
            logger.error(f"❌ Erro ao buscar propriedades: {str(e)}")
            raise

    elif config["mode"] == "specific":
        specified_fields = config["fields"]
        logger.info(f"🎯 Usando {len(specified_fields)} campos específicos")

        try:
            response = client.crm.properties.core_api.get_all(object_type)
            available_properties = [prop.name for prop in response.results]

            existing_fields = []
            missing_fields = []

            for field in specified_fields:
                if field in available_properties:
                    existing_fields.append(field)
                    logger.debug(f"✅ Campo encontrado: {field}")
                else:
                    missing_fields.append(field)
                    logger.warning(f"⚠️ Campo NÃO encontrado: {field}")

            if missing_fields:
                logger.warning(
                    f"⚠️ {len(missing_fields)} campos não encontrados: {', '.join(missing_fields[:10])}{'...' if len(missing_fields) > 10 else ''}")

            logger.info(f"🎯 {len(existing_fields)} campos válidos serão coletados")
            return existing_fields

        except Exception as e:
            logger.error(f"❌ Erro ao validar campos específicos: {str(e)}")
            return specified_fields

    else:
        logger.error(f"❌ Modo inválido '{config['mode']}' para {endpoint_name}")
        return []


def create_property_groups(properties, group_size=250):
    groups = [properties[i:i + group_size] for i in range(0, len(properties), group_size)]
    logger.info(f"📊 {len(properties)} propriedades divididas em {len(groups)} grupos de até {group_size}")
    return groups


def fetch_deals_page(client, properties, limit, after=None):
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

                if group_index == 0:
                    has_more = api_response.paging is not None
                    current_after = api_response.paging.next.after if has_more else None

                for deal in deals:
                    deal_id = deal.id
                    existing_deal = next((item for item in all_items if item.get("hs_object_id") == deal_id), None)

                    if existing_deal:
                        for property_name in properties:
                            existing_deal[property_name] = deal.properties.get(property_name, "")
                    else:
                        new_deal = {"hs_object_id": deal_id}
                        for property_name in properties:
                            new_deal[property_name] = deal.properties.get(property_name, "")
                        all_items.append(new_deal)

                logger.info(f"✓ Grupo {group_index + 1}/{len(property_groups)} processado")

            except Exception as e:
                logger.error(f"❌ Erro no grupo {group_index + 1}: {str(e)}")
                continue

        batch_duration = time.time() - batch_start_time
        logger.info(f"✓ Lote: {len(all_items)} deals em {batch_duration:.2f}s")

        if not has_more:
            logger.info("🏁 Todas as páginas de deals processadas")
            break

    return all_items


def process_deals_endpoint(client, endpoint_name="hubspot_deals", limit=LIMIT_PER_PAGE):
    try:
        logger.info(f"\n{'=' * 50}\n🔍 PROCESSANDO: {endpoint_name.upper()}\n{'=' * 50}")

        endpoint_start = time.time()

        all_properties = get_endpoint_properties(client, endpoint_name, "deals")
        property_groups = create_property_groups(all_properties, MAX_PROPERTIES_PER_REQUEST)
        raw_data = process_deals_batch(client, property_groups, limit)

        logger.info(f"💾 Processando e salvando {len(raw_data)} registros para {endpoint_name}")
        processed_data = Utils.process_and_save_data(raw_data, endpoint_name)

        endpoint_duration = time.time() - endpoint_start

        stats = {
            "registros": len(processed_data),
            "status": "Sucesso",
            "tempo": endpoint_duration,
            "total_properties": len(all_properties)
        }

        logger.info(f"✅ {endpoint_name}: {len(processed_data)} registros em {endpoint_duration:.2f}s")

        return processed_data, stats

    except Exception as e:
        logger.exception(f"❌ Falha no endpoint {endpoint_name}")

        stats = {
            "registros": 0,
            "status": f"Falha: {type(e).__name__}: {str(e)}",
            "tempo": 0,
            "total_properties": 0
        }

        return [], stats


async def fetch_contacts_page_with_properties(access_token, properties, after=None, session=None, max_retries=3):
    url = "https://api.hubapi.com/crm/v3/objects/contacts"

    params = {
        "limit": LIMIT_PER_PAGE,
        "properties": ",".join(properties)
    }
    if after:
        params["after"] = after

    headers = {"Authorization": f"Bearer {access_token}"}

    close_session = False
    if session is None:
        session = aiohttp.ClientSession()
        close_session = True

    for attempt in range(max_retries):
        try:
            async with session.get(url, params=params, headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    return data
                elif response.status == 429:
                    wait_time = 2 ** attempt
                    logger.warning(f"⚠️ Rate limited, aguardando {wait_time}s (tentativa {attempt + 1})")
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    logger.error(f"❌ Erro HTTP {response.status} na tentativa {attempt + 1}")
                    if attempt == max_retries - 1:
                        return None

        except Exception as e:
            logger.error(f"❌ Erro na tentativa {attempt + 1}: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(1)

        finally:
            if close_session:
                await session.close()

    return None


async def process_contacts_with_config(client, access_token, endpoint_name, log_details=False, after=None, max_contacts=None):
    logger.info(f"🔄 Iniciando coleta configurada para {endpoint_name} a partir de: {after or 'início'}")

    target_properties = get_endpoint_properties(client, endpoint_name, "contacts")

    if not target_properties:
        logger.error(f"❌ Nenhuma propriedade válida encontrada para {endpoint_name}")
        return [], None

    property_groups = create_property_groups(target_properties, MAX_PROPERTIES_PER_REQUEST)

    all_contacts = []
    current_after = after
    batch_count = 0
    has_more = True
    consecutive_errors = 0
    max_consecutive_errors = 5

    async with aiohttp.ClientSession() as session:
        while has_more:
            if max_contacts and len(all_contacts) >= max_contacts:
                logger.info(f"📊 Limite máximo de {max_contacts:,} contatos atingido")
                break

            batch_start_time = time.time()
            contact_objects = {}

            for group_index, properties in enumerate(property_groups):
                logger.info(
                    f"🔄 Buscando grupo {group_index + 1}/{len(property_groups)} com {len(properties)} propriedades")

                data = await fetch_contacts_page_with_properties(access_token, properties, current_after, session)

                if data is None:
                    consecutive_errors += 1
                    logger.error(f"❌ Falha no grupo {group_index + 1} (erro consecutivo {consecutive_errors})")

                    if consecutive_errors >= max_consecutive_errors:
                        logger.error(f"❌ Muitos erros consecutivos ({consecutive_errors}), parando coleta")
                        has_more = False
                        break
                    continue

                if 'results' not in data:
                    logger.error(f"❌ Resposta inválida para grupo {group_index + 1}")
                    continue

                consecutive_errors = 0
                contacts = data['results']

                for contact in contacts:
                    contact_id = contact['id']

                    if contact_id in contact_objects:
                        for prop_name, prop_value in contact['properties'].items():
                            contact_objects[contact_id]['properties'][prop_name] = prop_value
                    else:
                        contact_objects[contact_id] = contact

                if group_index == 0:
                    has_more = 'paging' in data and 'next' in data['paging']
                    if has_more:
                        current_after = data['paging']['next']['after']
                    else:
                        logger.info("🏁 Fim da paginação alcançado")

                logger.info(f"✓ Grupo {group_index + 1}/{len(property_groups)} processado")

            if contact_objects:
                all_contacts.extend(list(contact_objects.values()))
                batch_count += 1

                batch_duration = time.time() - batch_start_time
                logger.info(f"✓ Lote {batch_count}: {len(contact_objects)} contatos em {batch_duration:.2f}s")

                if batch_count % 5 == 0:
                    logger.info(f"📊 Progresso: {len(all_contacts)} contatos coletados")

            await asyncio.sleep(0.1)

    processed_contacts = []
    utm_found_count = 0

    for contact in all_contacts:
        processed_contact = {
            'id': contact['id'],
            'createdAt': contact.get('createdAt', ''),
            'updatedAt': contact.get('updatedAt', ''),
            'archived': str(contact.get('archived', False))
        }

        utm_data = {}
        if 'properties' in contact:
            for prop_name, prop_value in contact['properties'].items():
                processed_contact[prop_name] = prop_value or ''

                if prop_name.startswith('utm_') and prop_value:
                    utm_data[prop_name] = prop_value

        if utm_data:
            utm_found_count += 1
            if log_details:
                utm_summary = ', '.join([f"{k}={v}" for k, v in utm_data.items()])
                logger.info(f"🎯 Contato {contact['id']} UTMs: {utm_summary}")

        processed_contacts.append(processed_contact)

    logger.info(f"📊 RESUMO: {len(processed_contacts)} contatos processados")
    if utm_found_count > 0:
        logger.info(
            f"🎯 UTM: {utm_found_count} contatos com dados UTM ({utm_found_count / len(processed_contacts) * 100:.1f}%)")

    return processed_contacts, current_after


def process_contacts_endpoint(client, access_token, endpoint_name="hubspot_contacts", log_details=False,
                              after_key=None):
    try:
        logger.info(f"\n{'=' * 50}\n🔍 PROCESSANDO: {endpoint_name.upper()}\n{'=' * 50}")

        config = ENDPOINT_FIELD_CONFIG.get(endpoint_name, {})
        logger.info(f"🎯 Configuração: modo='{config.get('mode', 'all')}'")
        if config.get('mode') == 'specific':
            fields = config.get('fields', [])
            logger.info(f"🎯 Campos específicos: {', '.join(fields[:5])}{'...' if len(fields) > 5 else ''}")

        endpoint_start = time.time()

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        raw_data, last_after = loop.run_until_complete(
            process_contacts_with_config(client, access_token, endpoint_name, log_details, after_key, max_contacts=None)
        )

        loop.close()

        logger.info(f"💾 Processando e salvando {len(raw_data)} registros para {endpoint_name}")
        processed_data = Utils.process_and_save_data(raw_data, endpoint_name)

        endpoint_duration = time.time() - endpoint_start
        total_properties = len(raw_data[0].keys()) if raw_data else 0

        stats = {
            "registros": len(processed_data),
            "status": "Sucesso",
            "tempo": endpoint_duration,
            "ultimo_after": last_after,
            "total_properties": total_properties
        }

        logger.info(
            f"✅ {endpoint_name}: {len(processed_data)} registros com {total_properties} propriedades em {endpoint_duration:.2f}s")

        return processed_data, stats

    except Exception as e:
        logger.exception(f"❌ Falha no endpoint {endpoint_name}")

        stats = {
            "registros": 0,
            "status": f"Falha: {type(e).__name__}: {str(e)}",
            "tempo": 0,
            "ultimo_after": after_key,
            "total_properties": 0
        }

        return [], stats


def get_all_owners(client):
    logger.info("🔍 Buscando todos os owners")
    try:
        start_time = time.time()
        owners_response = client.crm.owners.owners_api.get_page()
        owners = owners_response.results

        duration = time.time() - start_time
        logger.info(f"✅ {len(owners)} owners obtidos em {duration:.2f}s")

        processed_owners = []
        for owner in owners:
            owner_dict = owner.to_dict()
            processed_owners.append(owner_dict)

        return processed_owners
    except Exception as e:
        logger.error(f"❌ Erro ao buscar owners: {str(e)}")
        raise


def process_owners_endpoint(client, endpoint_name="hubspot_owners"):
    try:
        logger.info(f"\n{'=' * 50}\n🔍 PROCESSANDO: {endpoint_name.upper()}\n{'=' * 50}")
        endpoint_start = time.time()

        raw_data = get_all_owners(client)
        logger.info(f"💾 Processando e salvando {len(raw_data)} registros para {endpoint_name}")
        processed_data = Utils.process_and_save_data(raw_data, endpoint_name)

        endpoint_duration = time.time() - endpoint_start
        total_properties = len(raw_data[0].keys()) if raw_data else 0

        stats = {
            "registros": len(processed_data),
            "status": "Sucesso",
            "tempo": endpoint_duration,
            "total_properties": total_properties
        }

        logger.info(f"✅ {endpoint_name}: {len(processed_data)} registros em {endpoint_duration:.2f}s")
        return processed_data, stats

    except Exception as e:
        logger.exception(f"❌ Falha no endpoint {endpoint_name}")
        return [], {
            "registros": 0,
            "status": f"Falha: {type(e).__name__}: {str(e)}",
            "tempo": 0,
            "total_properties": 0
        }


def get_all_pipelines(client, object_type="deals"):
    logger.info(f"🔍 Buscando todos os pipelines de {object_type}")
    try:
        start_time = time.time()
        pipelines_response = client.crm.pipelines.pipelines_api.get_all(object_type)
        pipelines = pipelines_response.results

        duration = time.time() - start_time
        logger.info(f"✅ {len(pipelines)} pipelines obtidos em {duration:.2f}s")

        processed_pipelines = []
        current_date = time.strftime('%Y-%m-%d %H:%M:%S')

        for pipeline in pipelines:
            pipeline_id = pipeline.id if hasattr(pipeline, 'id') else 'N/A'
            pipeline_name = pipeline.label if hasattr(pipeline, 'label') else 'N/A'

            if hasattr(pipeline, 'stages') and pipeline.stages:
                for stage in pipeline.stages:
                    pipeline_stage = {
                        "pipeline_id": pipeline_id,
                        "pipeline_name": pipeline_name,
                        "pipeline_stage_created_at": stage.created_at if hasattr(stage, 'created_at') else 'N/A',
                        "pipeline_stage_display_order": stage.display_order if hasattr(stage,
                                                                                       'display_order') else 'N/A',
                        "pipeline_stage_id": stage.id if hasattr(stage, 'id') else 'N/A',
                        "pipeline_stage_is_archived": stage.archived if hasattr(stage, 'archived') else 'N/A',
                        "pipeline_stage_is_closed": stage.metadata.get("closed", "N/A") if hasattr(stage,
                                                                                                   'metadata') else 'N/A',
                        "pipeline_stage_name": stage.label if hasattr(stage, 'label') else 'N/A',
                        "pipeline_stage_probability": stage.metadata.get("probability", "N/A") if hasattr(stage,
                                                                                                          'metadata') else 'N/A',
                        "pipeline_stage_updated_at": stage.updated_at if hasattr(stage, 'updated_at') else 'N/A',
                        "date": current_date
                    }
                    processed_pipelines.append(pipeline_stage)

        return processed_pipelines
    except Exception as e:
        logger.error(f"❌ Erro ao buscar pipelines: {str(e)}")
        raise


def process_pipelines_endpoint(client, endpoint_name="hubspot_pipelines", object_type="deals"):
    try:
        logger.info(f"\n{'=' * 50}\n🔍 PROCESSANDO: {endpoint_name.upper()}\n{'=' * 50}")
        endpoint_start = time.time()

        raw_data = get_all_pipelines(client, object_type)
        logger.info(f"💾 Processando e salvando {len(raw_data)} registros para {endpoint_name}")
        processed_data = Utils.process_and_save_data(raw_data, endpoint_name)

        endpoint_duration = time.time() - endpoint_start
        total_properties = len(raw_data[0].keys()) if raw_data else 0

        stats = {
            "registros": len(processed_data),
            "status": "Sucesso",
            "tempo": endpoint_duration,
            "total_properties": total_properties
        }

        logger.info(f"✅ {endpoint_name}: {len(processed_data)} registros em {endpoint_duration:.2f}s")
        return processed_data, stats

    except Exception as e:
        logger.exception(f"❌ Falha no endpoint {endpoint_name}")
        return [], {
            "registros": 0,
            "status": f"Falha: {type(e).__name__}: {str(e)}",
            "tempo": 0,
            "total_properties": 0
        }


def get_all_properties(client, object_type="contacts"):
    logger.info(f"🔍 Buscando todas as propriedades de {object_type}")
    try:
        start_time = time.time()
        properties_response = client.crm.properties.core_api.get_all(object_type)
        properties = properties_response.results

        duration = time.time() - start_time
        logger.info(f"✅ {len(properties)} propriedades obtidas em {duration:.2f}s")

        processed_properties = []
        for prop in properties:
            property_dict = {
                "property": prop.name,
                "label": prop.label,
                "type": getattr(prop, 'type', 'N/A'),
                "fieldType": getattr(prop, 'fieldType', 'N/A'),
                "description": getattr(prop, 'description', 'N/A'),
                "groupName": getattr(prop, 'groupName', 'N/A'),
                "hidden": getattr(prop, 'hidden', False),
                "calculated": getattr(prop, 'calculated', False),
                "externalOptions": getattr(prop, 'externalOptions', False)
            }
            processed_properties.append(property_dict)

        return processed_properties
    except Exception as e:
        logger.error(f"❌ Erro ao buscar propriedades: {str(e)}")
        raise


def process_properties_endpoint(client, endpoint_name="hubspot_properties", object_type="contacts"):
    try:
        logger.info(f"\n{'=' * 50}\n🔍 PROCESSANDO: {endpoint_name.upper()}\n{'=' * 50}")
        endpoint_start = time.time()

        raw_data = get_all_properties(client, object_type)
        logger.info(f"💾 Processando e salvando {len(raw_data)} registros para {endpoint_name}")
        processed_data = Utils.process_and_save_data(raw_data, endpoint_name)

        endpoint_duration = time.time() - endpoint_start
        total_properties = len(raw_data[0].keys()) if raw_data else 0

        stats = {
            "registros": len(processed_data),
            "status": "Sucesso",
            "tempo": endpoint_duration,
            "total_properties": total_properties
        }

        logger.info(f"✅ {endpoint_name}: {len(processed_data)} registros em {endpoint_duration:.2f}s")
        return processed_data, stats

    except Exception as e:
        logger.exception(f"❌ Falha no endpoint {endpoint_name}")
        return [], {
            "registros": 0,
            "status": f"Falha: {type(e).__name__}: {str(e)}",
            "tempo": 0,
            "total_properties": 0
        }


def main():
    global_start_time = ReportGenerator.init_report(logger)

    try:
        access_token = args.ACCESS_TOKEN
        log_details = getattr(args, 'LOG_PROPERTIES', False)
        if isinstance(log_details, str):
            log_details = log_details.lower() == 'true'

        endpoints_filter = getattr(args, 'ENDPOINTS', None)
        if endpoints_filter:
            endpoints_filter = endpoints_filter.split(',')

        max_parallel_str = getattr(args, 'MAX_PARALLEL', None)
        max_parallel = int(max_parallel_str) if max_parallel_str else MAX_WORKERS

        hubspot_client = init_hubspot_client(access_token)

        endpoints_to_process = {}
        for endpoint_name in ENDPOINTS:
            if endpoints_filter is None or endpoint_name in endpoints_filter:
                endpoints_to_process[endpoint_name] = ENDPOINTS[endpoint_name]

        logger.info(f"🔍 Endpoints a processar: {', '.join(endpoints_to_process.keys())}")
        logger.info("🎯 MODO: Coleta configurável por endpoint")

        for endpoint_name in endpoints_to_process.keys():
            config = ENDPOINT_FIELD_CONFIG.get(endpoint_name, {})
            mode = config.get('mode', 'all')
            if mode == 'specific':
                fields = config.get('fields', [])
                logger.info(
                    f"📋 {endpoint_name}: {len(fields)} campos específicos ({', '.join(fields[:5])}{'...' if len(fields) > 5 else ''})")
            else:
                logger.info(f"📋 {endpoint_name}: TODAS as propriedades")

        endpoint_stats = {}

        def process_endpoint_worker(endpoint_info):
            endpoint_name, endpoint_type = endpoint_info

            try:
                logger.info(f"🔄 Iniciando processamento paralelo do endpoint: {endpoint_name}")

                if endpoint_name == "hubspot_deals":
                    _, stats = process_deals_endpoint(hubspot_client, endpoint_name)

                elif endpoint_name == "hubspot_contacts":
                    _, stats = process_contacts_endpoint(hubspot_client, access_token, endpoint_name, log_details)

                elif endpoint_name == "hubspot_owners":
                    _, stats = process_owners_endpoint(hubspot_client, endpoint_name)

                elif endpoint_name == "hubspot_pipelines":
                    object_type = ENDPOINT_PARAMS[endpoint_name].get("object_type", "deals")
                    _, stats = process_pipelines_endpoint(hubspot_client, endpoint_name, object_type)

                elif endpoint_name == "hubspot_properties":
                    object_type = ENDPOINT_PARAMS[endpoint_name].get("object_type", "contacts")
                    _, stats = process_properties_endpoint(hubspot_client, endpoint_name, object_type)

                else:
                    logger.error(f"❌ Endpoint desconhecido: {endpoint_name}")
                    stats = {
                        "registros": 0,
                        "status": "Falha: Endpoint desconhecido",
                        "tempo": 0,
                        "total_properties": 0
                    }

                logger.info(f"✅ Processamento paralelo do endpoint {endpoint_name} concluído com sucesso")
                return endpoint_name, stats

            except Exception as e:
                logger.error(f"❌ Erro no processamento paralelo do endpoint {endpoint_name}: {str(e)}")
                return endpoint_name, {
                    "registros": 0,
                    "status": f"Falha: {type(e).__name__}: {str(e)}",
                    "tempo": 0,
                    "total_properties": 0
                }

        max_workers = min(len(endpoints_to_process), max_parallel)
        logger.info(f"🚀 Iniciando processamento paralelo com {max_workers} workers")

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_endpoint = {
                executor.submit(process_endpoint_worker, (endpoint_name, endpoint_type)):
                    endpoint_name for endpoint_name, endpoint_type in endpoints_to_process.items()
            }

            for future in concurrent.futures.as_completed(future_to_endpoint):
                endpoint_name, stats = future.result()
                endpoint_stats[endpoint_name] = stats

                logger.info(f"📊 {endpoint_name}: {stats['registros']} registros "
                            f"com {stats.get('total_properties', 0)} propriedades em {stats['tempo']:.2f}s")

        logger.info(f"\n{'=' * 60}\n🎯 RESUMO FINAL - COLETA CONFIGURÁVEL\n{'=' * 60}")

        total_records = sum(stats['registros'] for stats in endpoint_stats.values())
        total_properties = sum(stats.get('total_properties', 0) for stats in endpoint_stats.values())

        for endpoint_name, stats in endpoint_stats.items():
            config = ENDPOINT_FIELD_CONFIG.get(endpoint_name, {})
            mode_info = f"({config.get('mode', 'all')} mode)"

            logger.info(f"📈 {endpoint_name} {mode_info}:")
            logger.info(f"   - Registros: {stats['registros']}")
            logger.info(f"   - Propriedades coletadas: {stats.get('total_properties', 0)}")
            logger.info(f"   - Status: {stats['status']}")

        logger.info("\n🎯 RESUMO GERAL:")
        logger.info(f"   - Total de registros: {total_records}")
        logger.info(f"   - Total de campos coletados: {total_properties}")
        logger.info(f"   - Endpoints processados: {len(endpoint_stats)}")

        success = ReportGenerator.final_summary(logger, endpoint_stats, global_start_time)

        if not success:
            failed_endpoints = [name for name, stats in endpoint_stats.items() if 'Falha' in stats['status']]
            raise Exception(f"Falhas nos endpoints: {failed_endpoints}")

        logger.info("✅ SUCESSO: Coleta configurável concluída!")

    except Exception as e:
        logger.exception(f"❌ ERRO CRÍTICO NA EXECUÇÃO: {e}")
        raise


if __name__ == "__main__":
    main()
