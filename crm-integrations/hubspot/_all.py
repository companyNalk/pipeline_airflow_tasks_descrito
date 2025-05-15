# # import asyncio
# # import os
# # import time
# #
# # import aiohttp
# # import hubspot
# # from hubspot.crm.properties import ApiException
# #
# # from commons.app_inicializer import AppInitializer
# # from commons.report_generator import ReportGenerator
# # from commons.utils import Utils
# # from generic.argument_manager import ArgumentManager
# #
# # logger = AppInitializer.initialize()
# #
# # MAX_IDS = 100
# # RATE_LIMIT = 100
# # MAX_WORKERS = min(10, os.cpu_count() or 5)
# # MAX_PROPERTIES_PER_REQUEST = 250
# # LIMIT_PER_PAGE = 100
# #
# # # Definição dos endpoints da API HubSpot
# # ENDPOINTS = {
# #     "deals": "deals",
# #     "contacts": "contacts",
# #     "owners": "owners",
# #     "pipelines": "pipelines",
# #     "properties": "properties"
# # }
# #
# # # Parâmetros específicos para cada endpoint
# # ENDPOINT_PARAMS = {
# #     "deals": {"limit": LIMIT_PER_PAGE},
# #     "contacts": {"limit": LIMIT_PER_PAGE},
# #     "owners": {},
# #     "pipelines": {"object_type": "deals"},
# #     "properties": {"object_type": "deals"}
# # }
# #
# #
# # def get_arguments():
# #     """Configura e retorna os argumentos da linha de comando."""
# #     return (ArgumentManager("Script para coletar e processar dados da API HubSpot")
# #             .add("ACCESS_TOKEN", "Token de acesso para autenticação", required=True)
# #             .add("ENDPOINTS", "Lista de endpoints a processar (separados por vírgula). Se vazio, processa todos",
# #                  required=False)
# #             .parse())
# #
# #
# # def init_hubspot_client(access_token):
# #     """Inicializa o cliente HubSpot com o token de acesso."""
# #     logger.info("🔑 Inicializando cliente HubSpot")
# #     try:
# #         client = hubspot.Client.create(access_token=access_token)
# #         logger.info("✅ Cliente HubSpot inicializado com sucesso")
# #         return client
# #     except Exception as e:
# #         logger.error(f"❌ Erro ao inicializar cliente HubSpot: {str(e)}")
# #         raise
# #
# #
# # #################################
# # # Funções para processar Deals
# # #################################
# #
# # def get_all_deal_properties(client):
# #     """Busca todas as propriedades disponíveis para deals."""
# #     logger.info("🔍 Buscando todas as propriedades de deals")
# #     try:
# #         start_time = time.time()
# #         response = client.crm.properties.core_api.get_all('deals')
# #
# #         # Limitar a 700 propriedades para evitar problemas de performance
# #         all_properties = [prop.name for prop in response.results][:700]
# #
# #         duration = time.time() - start_time
# #         logger.info(f"✅ {len(all_properties)} propriedades de deals obtidas em {duration:.2f}s")
# #         return all_properties
# #     except Exception as e:
# #         logger.error(f"❌ Erro ao buscar propriedades de deals: {str(e)}")
# #         raise
# #
# #
# # def create_property_groups(properties, group_size=250):
# #     """Divide as propriedades em grupos para evitar limites da API."""
# #     return [properties[i:i + group_size] for i in range(0, len(properties), group_size)]
# #
# #
# # def fetch_deals_page(client, properties, limit, after=None):
# #     """Busca uma página de deals com as propriedades especificadas."""
# #     try:
# #         response = client.crm.deals.basic_api.get_page(
# #             limit=limit,
# #             properties=properties,
# #             after=after
# #         )
# #         return response
# #     except ApiException as e:
# #         logger.error(f"❌ API Exception ao buscar página de deals: {str(e)}")
# #         raise
# #     except Exception as e:
# #         logger.error(f"❌ Erro ao buscar página de deals: {str(e)}")
# #         raise
# #
# #
# # def process_deals_batch(client, property_groups, limit, after=None):
# #     """Processa um lote de deals para cada grupo de propriedades."""
# #     all_items = []
# #     has_more = True
# #     current_after = after
# #
# #     while has_more:
# #         batch_start_time = time.time()
# #         logger.info(f"🔄 Buscando página de deals com after={current_after}")
# #
# #         for group_index, properties in enumerate(property_groups):
# #             try:
# #                 api_response = fetch_deals_page(client, properties, limit, current_after)
# #                 deals = api_response.results
# #
# #                 # Atualizar flag de paginação
# #                 has_more = api_response.paging is not None
# #                 current_after = api_response.paging.next.after if has_more else None
# #
# #                 # Processar cada deal
# #                 for deal in deals:
# #                     deal_id = deal.id
# #                     # Verificar se já processamos este deal
# #                     existing_deal = next((item for item in all_items if item.get("hs_object_id") == deal_id), None)
# #
# #                     if existing_deal:
# #                         # Adicionar propriedades ao deal existente
# #                         for property_name in properties:
# #                             existing_deal[property_name] = deal.properties.get(property_name, "")
# #                     else:
# #                         # Criar novo deal
# #                         new_deal = {"hs_object_id": deal_id}
# #                         for property_name in properties:
# #                             new_deal[property_name] = deal.properties.get(property_name, "")
# #                         all_items.append(new_deal)
# #
# #                 logger.info(f"✓ Grupo {group_index + 1}/{len(property_groups)} com {len(deals)} deals processados")
# #
# #             except Exception as e:
# #                 logger.error(f"❌ Erro no grupo {group_index + 1}: {str(e)}")
# #                 # Continuar para o próximo grupo em caso de erro
# #
# #         batch_duration = time.time() - batch_start_time
# #         logger.info(f"✓ Página de deals processada em {batch_duration:.2f}s, deals totais: {len(all_items)}")
# #
# #         if not has_more:
# #             logger.info("🏁 Todas as páginas de deals processadas")
# #             break
# #
# #     return all_items
# #
# #
# # def process_deals_endpoint(client, endpoint_name="deals", limit=LIMIT_PER_PAGE):
# #     """Processa o endpoint de deals."""
# #     try:
# #         logger.info(f"\n{'=' * 50}\n🔍 PROCESSANDO ENDPOINT: {endpoint_name.upper()}\n{'=' * 50}")
# #
# #         endpoint_start = time.time()
# #
# #         # 1. Obter todas as propriedades
# #         all_properties = get_all_deal_properties(client)
# #
# #         # 2. Dividir propriedades em grupos
# #         property_groups = create_property_groups(all_properties, MAX_PROPERTIES_PER_REQUEST)
# #         logger.info(f"📊 Propriedades divididas em {len(property_groups)} grupos de requisição")
# #
# #         # 3. Processar todos os deals
# #         raw_data = process_deals_batch(client, property_groups, limit)
# #
# #         # 4. Processar e salvar usando o Utils do projeto
# #         logger.info(f"💾 Processando e salvando {len(raw_data)} registros para {endpoint_name}")
# #         processed_data = Utils.process_and_save_data(raw_data, endpoint_name)
# #
# #         endpoint_duration = time.time() - endpoint_start
# #
# #         # Estatísticas de processamento
# #         stats = {
# #             "registros": len(processed_data),
# #             "status": "Sucesso",
# #             "tempo": endpoint_duration
# #         }
# #
# #         logger.info(f"✅ {endpoint_name}: {len(processed_data)} registros em {endpoint_duration:.2f}s")
# #
# #         return processed_data, stats
# #
# #     except Exception as e:
# #         logger.exception(f"❌ Falha no endpoint {endpoint_name}")
# #
# #         # Estatísticas em caso de erro
# #         stats = {
# #             "registros": 0,
# #             "status": f"Falha: {type(e).__name__}: {str(e)}",
# #             "tempo": 0
# #         }
# #
# #         return [], stats
# #
# #
# # #################################
# # # Funções para processar Contacts
# # #################################
# #
# # async def fetch_contacts_page(access_token, after=None, session=None):
# #     """Busca uma página de contatos usando aiohttp."""
# #     url = "https://api.hubapi.com/crm/v3/objects/contacts"
# #     params = {"limit": LIMIT_PER_PAGE}
# #     if after:
# #         params["after"] = after
# #
# #     headers = {"Authorization": f"Bearer {access_token}"}
# #
# #     # Usar a sessão fornecida ou criar uma nova
# #     close_session = False
# #     if session is None:
# #         session = aiohttp.ClientSession()
# #         close_session = True
# #
# #     try:
# #         async with session.get(url, params=params, headers=headers) as response:
# #             response.raise_for_status()
# #             data = await response.json()
# #             return data
# #     except aiohttp.ClientError as e:
# #         logger.error(f"❌ Erro HTTP ao buscar contatos: {str(e)}")
# #         return None
# #     finally:
# #         if close_session:
# #             await session.close()
# #
# #
# # async def process_contacts_batch_async(access_token, after=None, max_contacts=100000):
# #     """Processa lotes de contatos usando requisições assíncronas."""
# #     logger.info(f"🔄 Iniciando coleta de contatos a partir de: {after or 'início'}")
# #
# #     all_contacts = []
# #     current_after = after
# #     batch_count = 0
# #
# #     async with aiohttp.ClientSession() as session:
# #         while True:
# #             batch_start_time = time.time()
# #             data = await fetch_contacts_page(access_token, current_after, session)
# #
# #             if data is None or 'results' not in data:
# #                 logger.error("❌ Falha ao obter resultados da API")
# #                 break
# #
# #             contacts = data['results']
# #             all_contacts.extend(contacts)
# #             batch_count += 1
# #
# #             # Log de progresso
# #             if batch_count % 10 == 0:
# #                 logger.info(f"📊 Progresso: {len(all_contacts)} contatos coletados em {batch_count} lotes")
# #
# #             # Verificar se temos paginação
# #             if 'paging' not in data or 'next' not in data['paging']:
# #                 logger.info("🏁 Fim da paginação alcançado")
# #                 break
# #
# #             # Atualizar cursor de paginação
# #             current_after = data['paging']['next']['after']
# #
# #             # Limitar número de contatos
# #             if len(all_contacts) >= max_contacts:
# #                 logger.info(f"🛑 Limite de {max_contacts} contatos atingido")
# #                 break
# #
# #             batch_duration = time.time() - batch_start_time
# #             logger.info(f"✓ Lote {batch_count} processado em {batch_duration:.2f}s, {len(contacts)} contatos")
# #
# #     # Transformar contatos para o formato esperado pelo Utils
# #     processed_contacts = []
# #     for contact in all_contacts:
# #         processed_contact = {
# #             'id': contact['id'],
# #             'createdAt': contact.get('createdAt', ''),
# #             'updatedAt': contact.get('updatedAt', ''),
# #             'archived': str(contact.get('archived', False))
# #         }
# #         # Adicionar todas as propriedades
# #         if 'properties' in contact:
# #             for prop_name, prop_value in contact['properties'].items():
# #                 processed_contact[prop_name] = prop_value
# #
# #         processed_contacts.append(processed_contact)
# #
# #     return processed_contacts, current_after
# #
# #
# # def process_contacts_endpoint(access_token, endpoint_name="contacts", after_key=None):
# #     """Processa o endpoint de contatos."""
# #     try:
# #         logger.info(f"\n{'=' * 50}\n🔍 PROCESSANDO ENDPOINT: {endpoint_name.upper()}\n{'=' * 50}")
# #
# #         endpoint_start = time.time()
# #
# #         # Usar asyncio para processamento assíncrono
# #         loop = asyncio.get_event_loop()
# #         raw_data, last_after = loop.run_until_complete(
# #             process_contacts_batch_async(access_token, after_key)
# #         )
# #
# #         # Processar e salvar usando o Utils do projeto
# #         logger.info(f"💾 Processando e salvando {len(raw_data)} registros para {endpoint_name}")
# #         processed_data = Utils.process_and_save_data(raw_data, endpoint_name)
# #
# #         endpoint_duration = time.time() - endpoint_start
# #
# #         # Estatísticas de processamento
# #         stats = {
# #             "registros": len(processed_data),
# #             "status": "Sucesso",
# #             "tempo": endpoint_duration,
# #             "ultimo_after": last_after
# #         }
# #
# #         logger.info(f"✅ {endpoint_name}: {len(processed_data)} registros em {endpoint_duration:.2f}s")
# #         logger.info(f"📌 Último cursor de paginação: {last_after}")
# #
# #         return processed_data, stats
# #
# #     except Exception as e:
# #         logger.exception(f"❌ Falha no endpoint {endpoint_name}")
# #
# #         # Estatísticas em caso de erro
# #         stats = {
# #             "registros": 0,
# #             "status": f"Falha: {type(e).__name__}: {str(e)}",
# #             "tempo": 0,
# #             "ultimo_after": after_key
# #         }
# #
# #         return [], stats
# #
# #
# # #################################
# # # Funções para processar Owners
# # #################################
# #
# # def get_all_owners(client):
# #     """Busca todos os owners do HubSpot."""
# #     logger.info("🔍 Buscando todos os owners")
# #     try:
# #         start_time = time.time()
# #
# #         # Obter a página de owners
# #         owners_response = client.crm.owners.owners_api.get_page()
# #         owners = owners_response.results
# #
# #         duration = time.time() - start_time
# #         logger.info(f"✅ {len(owners)} owners obtidos em {duration:.2f}s")
# #
# #         # Processar owners para o formato esperado
# #         processed_owners = []
# #         for owner in owners:
# #             owner_dict = owner.to_dict()
# #             processed_owners.append(owner_dict)
# #
# #         return processed_owners
# #     except ApiException as e:
# #         logger.error(f"❌ API Exception ao buscar owners: {str(e)}")
# #         raise
# #     except Exception as e:
# #         logger.error(f"❌ Erro ao buscar owners: {str(e)}")
# #         raise
# #
# #
# # def process_owners_endpoint(client, endpoint_name="owners"):
# #     """Processa o endpoint de owners."""
# #     try:
# #         logger.info(f"\n{'=' * 50}\n🔍 PROCESSANDO ENDPOINT: {endpoint_name.upper()}\n{'=' * 50}")
# #
# #         endpoint_start = time.time()
# #
# #         # Buscar todos os owners
# #         raw_data = get_all_owners(client)
# #
# #         # Processar e salvar usando o Utils do projeto
# #         logger.info(f"💾 Processando e salvando {len(raw_data)} registros para {endpoint_name}")
# #         processed_data = Utils.process_and_save_data(raw_data, endpoint_name)
# #
# #         endpoint_duration = time.time() - endpoint_start
# #
# #         # Estatísticas de processamento
# #         stats = {
# #             "registros": len(processed_data),
# #             "status": "Sucesso",
# #             "tempo": endpoint_duration
# #         }
# #
# #         logger.info(f"✅ {endpoint_name}: {len(processed_data)} registros em {endpoint_duration:.2f}s")
# #
# #         return processed_data, stats
# #
# #     except Exception as e:
# #         logger.exception(f"❌ Falha no endpoint {endpoint_name}")
# #
# #         # Estatísticas em caso de erro
# #         stats = {
# #             "registros": 0,
# #             "status": f"Falha: {type(e).__name__}: {str(e)}",
# #             "tempo": 0
# #         }
# #
# #         return [], stats
# #
# #
# # #################################
# # # Funções para processar Pipelines
# # #################################
# #
# # def get_all_pipelines(client, object_type="deals"):
# #     """Busca todos os pipelines de um tipo de objeto do HubSpot."""
# #     logger.info(f"🔍 Buscando todos os pipelines de {object_type}")
# #     try:
# #         start_time = time.time()
# #
# #         # Obter os pipelines
# #         pipelines_response = client.crm.pipelines.pipelines_api.get_all(object_type)
# #         pipelines = pipelines_response.results
# #
# #         duration = time.time() - start_time
# #         logger.info(f"✅ {len(pipelines)} pipelines obtidos em {duration:.2f}s")
# #
# #         # Processar pipelines para o formato esperado
# #         processed_pipelines = []
# #         for pipeline in pipelines:
# #             pipeline_dict = {
# #                 "id": pipeline.id,
# #                 "label": pipeline.label,
# #                 "display_order": pipeline.display_order,
# #                 "created_at": pipeline.created_at,
# #                 "updated_at": pipeline.updated_at,
# #                 "archived": pipeline.archived
# #             }
# #
# #             # Adicionar estágios do pipeline, se disponíveis
# #             if hasattr(pipeline, 'stages') and pipeline.stages:
# #                 # Criar uma representação dos estágios como propriedades adicionais
# #                 for i, stage in enumerate(pipeline.stages):
# #                     prefix = f"stage_{i + 1}_"
# #                     pipeline_dict[f"{prefix}id"] = stage.id
# #                     pipeline_dict[f"{prefix}label"] = stage.label
# #                     pipeline_dict[f"{prefix}display_order"] = stage.display_order
# #                     pipeline_dict[f"{prefix}created_at"] = stage.created_at
# #                     pipeline_dict[f"{prefix}updated_at"] = stage.updated_at
# #                     pipeline_dict[f"{prefix}archived"] = stage.archived
# #
# #                 pipeline_dict["total_stages"] = len(pipeline.stages)
# #             else:
# #                 pipeline_dict["total_stages"] = 0
# #
# #             processed_pipelines.append(pipeline_dict)
# #
# #         return processed_pipelines
# #     except ApiException as e:
# #         logger.error(f"❌ API Exception ao buscar pipelines: {str(e)}")
# #         raise
# #     except Exception as e:
# #         logger.error(f"❌ Erro ao buscar pipelines: {str(e)}")
# #         raise
# #
# #
# # def process_pipelines_endpoint(client, endpoint_name="pipelines", object_type="deals"):
# #     """Processa o endpoint de pipelines."""
# #     try:
# #         logger.info(f"\n{'=' * 50}\n🔍 PROCESSANDO ENDPOINT: {endpoint_name.upper()}\n{'=' * 50}")
# #
# #         endpoint_start = time.time()
# #
# #         # Buscar todos os pipelines
# #         raw_data = get_all_pipelines(client, object_type)
# #
# #         # Processar e salvar usando o Utils do projeto
# #         logger.info(f"💾 Processando e salvando {len(raw_data)} registros para {endpoint_name}")
# #         processed_data = Utils.process_and_save_data(raw_data, endpoint_name)
# #
# #         endpoint_duration = time.time() - endpoint_start
# #
# #         # Estatísticas de processamento
# #         stats = {
# #             "registros": len(processed_data),
# #             "status": "Sucesso",
# #             "tempo": endpoint_duration
# #         }
# #
# #         logger.info(f"✅ {endpoint_name}: {len(processed_data)} registros em {endpoint_duration:.2f}s")
# #
# #         return processed_data, stats
# #
# #     except Exception as e:
# #         logger.exception(f"❌ Falha no endpoint {endpoint_name}")
# #
# #         # Estatísticas em caso de erro
# #         stats = {
# #             "registros": 0,
# #             "status": f"Falha: {type(e).__name__}: {str(e)}",
# #             "tempo": 0
# #         }
# #
# #         return [], stats
# #
# #
# # #################################
# # # Funções para processar Properties
# # #################################
# #
# # def get_all_properties(client, object_type="deals"):
# #     """Busca todas as propriedades de um tipo de objeto do HubSpot."""
# #     logger.info(f"🔍 Buscando todas as propriedades de {object_type}")
# #     try:
# #         start_time = time.time()
# #
# #         # Obter as propriedades
# #         properties_response = client.crm.properties.core_api.get_all(object_type)
# #         properties = properties_response.results
# #
# #         duration = time.time() - start_time
# #         logger.info(f"✅ {len(properties)} propriedades obtidas em {duration:.2f}s")
# #
# #         # Processar propriedades para o formato esperado (simplificado)
# #         processed_properties = []
# #         for prop in properties:
# #             # Simplificado para seguir o exemplo fornecido
# #             property_dict = {
# #                 "property": prop.name,
# #                 "label": prop.label
# #             }
# #
# #             processed_properties.append(property_dict)
# #
# #         return processed_properties
# #     except ApiException as e:
# #         logger.error(f"❌ API Exception ao buscar propriedades: {str(e)}")
# #         raise
# #     except Exception as e:
# #         logger.error(f"❌ Erro ao buscar propriedades: {str(e)}")
# #         raise
# #
# #
# # def process_properties_endpoint(client, endpoint_name="properties", object_type="deals"):
# #     """Processa o endpoint de propriedades."""
# #     try:
# #         logger.info(f"\n{'=' * 50}\n🔍 PROCESSANDO ENDPOINT: {endpoint_name.upper()}\n{'=' * 50}")
# #
# #         endpoint_start = time.time()
# #
# #         # Buscar todas as propriedades
# #         raw_data = get_all_properties(client, object_type)
# #
# #         # Processar e salvar usando o Utils do projeto
# #         logger.info(f"💾 Processando e salvando {len(raw_data)} registros para {endpoint_name}")
# #         processed_data = Utils.process_and_save_data(raw_data, endpoint_name)
# #
# #         endpoint_duration = time.time() - endpoint_start
# #
# #         # Estatísticas de processamento
# #         stats = {
# #             "registros": len(processed_data),
# #             "status": "Sucesso",
# #             "tempo": endpoint_duration
# #         }
# #
# #         logger.info(f"✅ {endpoint_name}: {len(processed_data)} registros em {endpoint_duration:.2f}s")
# #
# #         return processed_data, stats
# #
# #     except Exception as e:
# #         logger.exception(f"❌ Falha no endpoint {endpoint_name}")
# #
# #         # Estatísticas em caso de erro
# #         stats = {
# #             "registros": 0,
# #             "status": f"Falha: {type(e).__name__}: {str(e)}",
# #             "tempo": 0
# #         }
# #
# #         return [], stats
# #
# #
# # def main():
# #     """Função principal para coleta de dados da API HubSpot."""
# #     # Iniciar relatório de processamento
# #     global_start_time = ReportGenerator.init_report(logger)
# #
# #     try:
# #         # 1. Obter configurações
# #         args = get_arguments()
# #
# #         access_token = args.ACCESS_TOKEN
# #         endpoints_filter = args.ENDPOINTS.split(',') if hasattr(args, 'ENDPOINTS') and args.ENDPOINTS else None
# #
# #         # 2. Inicializar cliente HubSpot
# #         hubspot_client = init_hubspot_client(access_token)
# #
# #         # 3. Filtrar endpoints a processar
# #         endpoints_to_process = {}
# #         for endpoint_name in ENDPOINTS:
# #             if endpoints_filter is None or endpoint_name in endpoints_filter:
# #                 endpoints_to_process[endpoint_name] = ENDPOINTS[endpoint_name]
# #
# #         logger.info(f"🔍 Endpoints a processar: {', '.join(endpoints_to_process.keys())}")
# #
# #         # 4. Processamento de endpoints
# #         endpoint_stats = {}
# #
# #         for endpoint_name, endpoint_type in endpoints_to_process.items():
# #             if endpoint_name == "deals":
# #                 # Processar endpoint de deals
# #                 _, stats = process_deals_endpoint(hubspot_client, endpoint_name)
# #                 endpoint_stats[endpoint_name] = stats
# #
# #             elif endpoint_name == "contacts":
# #                 # Processar endpoint de contatos (usando async)
# #                 _, stats = process_contacts_endpoint(access_token, endpoint_name)
# #                 endpoint_stats[endpoint_name] = stats
# #
# #             elif endpoint_name == "owners":
# #                 # Processar endpoint de owners
# #                 _, stats = process_owners_endpoint(hubspot_client, endpoint_name)
# #                 endpoint_stats[endpoint_name] = stats
# #
# #             elif endpoint_name == "pipelines":
# #                 # Processar endpoint de pipelines
# #                 object_type = ENDPOINT_PARAMS[endpoint_name].get("object_type", "deals")
# #                 _, stats = process_pipelines_endpoint(hubspot_client, endpoint_name, object_type)
# #                 endpoint_stats[endpoint_name] = stats
# #
# #             elif endpoint_name == "properties":
# #                 # Processar endpoint de propriedades
# #                 object_type = ENDPOINT_PARAMS[endpoint_name].get("object_type", "deals")
# #                 _, stats = process_properties_endpoint(hubspot_client, endpoint_name, object_type)
# #                 endpoint_stats[endpoint_name] = stats
# #
# #         # 5. Gerar resumo final
# #         success = ReportGenerator.final_summary(logger, endpoint_stats, global_start_time)
# #
# #         # Se houver falhas, lançar exceção
# #         if not success:
# #             raise Exception(f"Falhas nos endpoints: {success}")
# #
# #     except Exception as e:
# #         logger.exception(f"❌ ERRO CRÍTICO NA EXECUÇÃO: {e}")
# #         raise
# #
# #
# # if __name__ == "__main__":
# #     main()
# import asyncio
# import concurrent.futures
# import os
# import time
#
# import aiohttp
# import hubspot
# from hubspot.crm.properties import ApiException
#
# from commons.app_inicializer import AppInitializer
# from commons.report_generator import ReportGenerator
# from commons.utils import Utils
# from generic.argument_manager import ArgumentManager
#
# logger = AppInitializer.initialize()
#
# MAX_IDS = 100
# RATE_LIMIT = 100
# MAX_WORKERS = min(10, os.cpu_count() or 5)
# MAX_PROPERTIES_PER_REQUEST = 250
# LIMIT_PER_PAGE = 100
#
# # Definição dos endpoints da API HubSpot
# ENDPOINTS = {
#     "hubspot_deals": "deals",
#     "hubspot_contacts": "contacts",
#     "hubspot_owners": "owners",
#     "hubspot_pipelines": "pipelines",
#     "hubspot_properties": "properties"
# }
#
# # Parâmetros específicos para cada endpoint
# ENDPOINT_PARAMS = {
#     "hubspot_deals": {"limit": LIMIT_PER_PAGE},
#     "hubspot_contacts": {"limit": LIMIT_PER_PAGE},
#     "hubspot_owners": {},
#     "hubspot_pipelines": {"object_type": "deals"},
#     "hubspot_properties": {"object_type": "deals"}
# }
#
#
# def get_arguments():
#     """Configura e retorna os argumentos da linha de comando."""
#     return (ArgumentManager("Script para coletar e processar dados da API HubSpot")
#             .add("ACCESS_TOKEN", "Token de acesso para autenticação", required=True)
#             .add("ENDPOINTS", "Lista de endpoints a processar (separados por vírgula). Se vazio, processa todos",
#                  required=False)
#             .add("MAX_PARALLEL", "Número máximo de workers para paralelização", required=False,
#                  default=str(MAX_WORKERS))
#             .parse())
#
#
# def init_hubspot_client(access_token):
#     """Inicializa o cliente HubSpot com o token de acesso."""
#     logger.info("🔑 Inicializando cliente HubSpot")
#     try:
#         client = hubspot.Client.create(access_token=access_token)
#         logger.info("✅ Cliente HubSpot inicializado com sucesso")
#         return client
#     except Exception as e:
#         logger.error(f"❌ Erro ao inicializar cliente HubSpot: {str(e)}")
#         raise
#
#
# #################################
# # Funções para processar Deals
# #################################
#
# def get_all_deal_properties(client):
#     """Busca todas as propriedades disponíveis para deals."""
#     logger.info("🔍 Buscando todas as propriedades de deals")
#     try:
#         start_time = time.time()
#         response = client.crm.properties.core_api.get_all('deals')
#
#         # Limitar a 700 propriedades para evitar problemas de performance
#         all_properties = [prop.name for prop in response.results][:700]
#
#         duration = time.time() - start_time
#         logger.info(f"✅ {len(all_properties)} propriedades de deals obtidas em {duration:.2f}s")
#         return all_properties
#     except Exception as e:
#         logger.error(f"❌ Erro ao buscar propriedades de deals: {str(e)}")
#         raise
#
#
# def create_property_groups(properties, group_size=250):
#     """Divide as propriedades em grupos para evitar limites da API."""
#     return [properties[i:i + group_size] for i in range(0, len(properties), group_size)]
#
#
# def fetch_deals_page(client, properties, limit, after=None):
#     """Busca uma página de deals com as propriedades especificadas."""
#     try:
#         response = client.crm.deals.basic_api.get_page(
#             limit=limit,
#             properties=properties,
#             after=after
#         )
#         return response
#     except ApiException as e:
#         logger.error(f"❌ API Exception ao buscar página de deals: {str(e)}")
#         raise
#     except Exception as e:
#         logger.error(f"❌ Erro ao buscar página de deals: {str(e)}")
#         raise
#
#
# def process_deals_batch(client, property_groups, limit, after=None):
#     """Processa um lote de deals para cada grupo de propriedades."""
#     all_items = []
#     has_more = True
#     current_after = after
#
#     while has_more:
#         batch_start_time = time.time()
#         logger.info(f"🔄 Buscando página de deals com after={current_after}")
#
#         for group_index, properties in enumerate(property_groups):
#             try:
#                 api_response = fetch_deals_page(client, properties, limit, current_after)
#                 deals = api_response.results
#
#                 # Atualizar flag de paginação
#                 has_more = api_response.paging is not None
#                 current_after = api_response.paging.next.after if has_more else None
#
#                 # Processar cada deal
#                 for deal in deals:
#                     deal_id = deal.id
#                     # Verificar se já processamos este deal
#                     existing_deal = next((item for item in all_items if item.get("hs_object_id") == deal_id), None)
#
#                     if existing_deal:
#                         # Adicionar propriedades ao deal existente
#                         for property_name in properties:
#                             existing_deal[property_name] = deal.properties.get(property_name, "")
#                     else:
#                         # Criar novo deal
#                         new_deal = {"hs_object_id": deal_id}
#                         for property_name in properties:
#                             new_deal[property_name] = deal.properties.get(property_name, "")
#                         all_items.append(new_deal)
#
#                 logger.info(f"✓ Grupo {group_index + 1}/{len(property_groups)} com {len(deals)} deals processados")
#
#             except Exception as e:
#                 logger.error(f"❌ Erro no grupo {group_index + 1}: {str(e)}")
#                 # Continuar para o próximo grupo em caso de erro
#
#         batch_duration = time.time() - batch_start_time
#         logger.info(f"✓ Página de deals processada em {batch_duration:.2f}s, deals totais: {len(all_items)}")
#
#         if not has_more:
#             logger.info("🏁 Todas as páginas de deals processadas")
#             break
#
#     return all_items
#
#
# def process_deals_endpoint(client, endpoint_name="hubspot_deals", limit=LIMIT_PER_PAGE):
#     """Processa o endpoint de deals."""
#     try:
#         logger.info(f"\n{'=' * 50}\n🔍 PROCESSANDO ENDPOINT: {endpoint_name.upper()}\n{'=' * 50}")
#
#         endpoint_start = time.time()
#
#         # 1. Obter todas as propriedades
#         all_properties = get_all_deal_properties(client)
#
#         # 2. Dividir propriedades em grupos
#         property_groups = create_property_groups(all_properties, MAX_PROPERTIES_PER_REQUEST)
#         logger.info(f"📊 Propriedades divididas em {len(property_groups)} grupos de requisição")
#
#         # 3. Processar todos os deals
#         raw_data = process_deals_batch(client, property_groups, limit)
#
#         # 4. Processar e salvar usando o Utils do projeto
#         logger.info(f"💾 Processando e salvando {len(raw_data)} registros para {endpoint_name}")
#         processed_data = Utils.process_and_save_data(raw_data, endpoint_name)
#
#         endpoint_duration = time.time() - endpoint_start
#
#         # Estatísticas de processamento
#         stats = {
#             "registros": len(processed_data),
#             "status": "Sucesso",
#             "tempo": endpoint_duration
#         }
#
#         logger.info(f"✅ {endpoint_name}: {len(processed_data)} registros em {endpoint_duration:.2f}s")
#
#         return processed_data, stats
#
#     except Exception as e:
#         logger.exception(f"❌ Falha no endpoint {endpoint_name}")
#
#         # Estatísticas em caso de erro
#         stats = {
#             "registros": 0,
#             "status": f"Falha: {type(e).__name__}: {str(e)}",
#             "tempo": 0
#         }
#
#         return [], stats
#
#
# #################################
# # Funções para processar Contacts
# #################################
#
# async def fetch_contacts_page(access_token, after=None, session=None):
#     """Busca uma página de contatos usando aiohttp."""
#     url = "https://api.hubapi.com/crm/v3/objects/contacts"
#     params = {"limit": LIMIT_PER_PAGE}
#     if after:
#         params["after"] = after
#
#     headers = {"Authorization": f"Bearer {access_token}"}
#
#     # Usar a sessão fornecida ou criar uma nova
#     close_session = False
#     if session is None:
#         session = aiohttp.ClientSession()
#         close_session = True
#
#     try:
#         async with session.get(url, params=params, headers=headers) as response:
#             response.raise_for_status()
#             data = await response.json()
#             return data
#     except aiohttp.ClientError as e:
#         logger.error(f"❌ Erro HTTP ao buscar contatos: {str(e)}")
#         return None
#     finally:
#         if close_session:
#             await session.close()
#
#
# async def process_contacts_batch_async(access_token, after=None, max_contacts=100000):
#     """Processa lotes de contatos usando requisições assíncronas."""
#     logger.info(f"🔄 Iniciando coleta de contatos a partir de: {after or 'início'}")
#
#     all_contacts = []
#     current_after = after
#     batch_count = 0
#
#     async with aiohttp.ClientSession() as session:
#         while True:
#             batch_start_time = time.time()
#             data = await fetch_contacts_page(access_token, current_after, session)
#
#             if data is None or 'results' not in data:
#                 logger.error("❌ Falha ao obter resultados da API")
#                 break
#
#             contacts = data['results']
#             all_contacts.extend(contacts)
#             batch_count += 1
#
#             # Log de progresso
#             if batch_count % 10 == 0:
#                 logger.info(f"📊 Progresso: {len(all_contacts)} contatos coletados em {batch_count} lotes")
#
#             # Verificar se temos paginação
#             if 'paging' not in data or 'next' not in data['paging']:
#                 logger.info("🏁 Fim da paginação alcançado")
#                 break
#
#             # Atualizar cursor de paginação
#             current_after = data['paging']['next']['after']
#
#             # Limitar número de contatos
#             if len(all_contacts) >= max_contacts:
#                 logger.info(f"🛑 Limite de {max_contacts} contatos atingido")
#                 break
#
#             batch_duration = time.time() - batch_start_time
#             logger.info(f"✓ Lote {batch_count} processado em {batch_duration:.2f}s, {len(contacts)} contatos")
#
#     # Transformar contatos para o formato esperado pelo Utils
#     processed_contacts = []
#     for contact in all_contacts:
#         processed_contact = {
#             'id': contact['id'],
#             'createdAt': contact.get('createdAt', ''),
#             'updatedAt': contact.get('updatedAt', ''),
#             'archived': str(contact.get('archived', False))
#         }
#         # Adicionar todas as propriedades
#         if 'properties' in contact:
#             for prop_name, prop_value in contact['properties'].items():
#                 processed_contact[prop_name] = prop_value
#
#         processed_contacts.append(processed_contact)
#
#     return processed_contacts, current_after
#
#
# def process_contacts_endpoint(access_token, endpoint_name="hubspot_contacts", after_key=None):
#     """Processa o endpoint de contatos."""
#     try:
#         logger.info(f"\n{'=' * 50}\n🔍 PROCESSANDO ENDPOINT: {endpoint_name.upper()}\n{'=' * 50}")
#
#         endpoint_start = time.time()
#
#         # Criar um novo event loop para esta thread
#         loop = asyncio.new_event_loop()
#         asyncio.set_event_loop(loop)
#
#         # Usar o loop criado para executar a função assíncrona
#         raw_data, last_after = loop.run_until_complete(
#             process_contacts_batch_async(access_token, after_key)
#         )
#
#         # Fechar o loop quando terminar
#         loop.close()
#
#         # Processar e salvar usando o Utils do projeto
#         logger.info(f"💾 Processando e salvando {len(raw_data)} registros para {endpoint_name}")
#         processed_data = Utils.process_and_save_data(raw_data, endpoint_name)
#
#         endpoint_duration = time.time() - endpoint_start
#
#         # Estatísticas de processamento
#         stats = {
#             "registros": len(processed_data),
#             "status": "Sucesso",
#             "tempo": endpoint_duration,
#             "ultimo_after": last_after
#         }
#
#         logger.info(f"✅ {endpoint_name}: {len(processed_data)} registros em {endpoint_duration:.2f}s")
#         logger.info(f"📌 Último cursor de paginação: {last_after}")
#
#         return processed_data, stats
#
#     except Exception as e:
#         logger.exception(f"❌ Falha no endpoint {endpoint_name}")
#
#         # Estatísticas em caso de erro
#         stats = {
#             "registros": 0,
#             "status": f"Falha: {type(e).__name__}: {str(e)}",
#             "tempo": 0,
#             "ultimo_after": after_key
#         }
#
#         return [], stats
#
#
# #################################
# # Funções para processar Owners
# #################################
#
# def get_all_owners(client):
#     """Busca todos os owners do HubSpot."""
#     logger.info("🔍 Buscando todos os owners")
#     try:
#         start_time = time.time()
#
#         # Obter a página de owners
#         owners_response = client.crm.owners.owners_api.get_page()
#         owners = owners_response.results
#
#         duration = time.time() - start_time
#         logger.info(f"✅ {len(owners)} owners obtidos em {duration:.2f}s")
#
#         # Processar owners para o formato esperado
#         processed_owners = []
#         for owner in owners:
#             owner_dict = owner.to_dict()
#             processed_owners.append(owner_dict)
#
#         return processed_owners
#     except ApiException as e:
#         logger.error(f"❌ API Exception ao buscar owners: {str(e)}")
#         raise
#     except Exception as e:
#         logger.error(f"❌ Erro ao buscar owners: {str(e)}")
#         raise
#
#
# def process_owners_endpoint(client, endpoint_name="hubspot_owners"):
#     """Processa o endpoint de owners."""
#     try:
#         logger.info(f"\n{'=' * 50}\n🔍 PROCESSANDO ENDPOINT: {endpoint_name.upper()}\n{'=' * 50}")
#
#         endpoint_start = time.time()
#
#         # Buscar todos os owners
#         raw_data = get_all_owners(client)
#
#         # Processar e salvar usando o Utils do projeto
#         logger.info(f"💾 Processando e salvando {len(raw_data)} registros para {endpoint_name}")
#         processed_data = Utils.process_and_save_data(raw_data, endpoint_name)
#
#         endpoint_duration = time.time() - endpoint_start
#
#         # Estatísticas de processamento
#         stats = {
#             "registros": len(processed_data),
#             "status": "Sucesso",
#             "tempo": endpoint_duration
#         }
#
#         logger.info(f"✅ {endpoint_name}: {len(processed_data)} registros em {endpoint_duration:.2f}s")
#
#         return processed_data, stats
#
#     except Exception as e:
#         logger.exception(f"❌ Falha no endpoint {endpoint_name}")
#
#         # Estatísticas em caso de erro
#         stats = {
#             "registros": 0,
#             "status": f"Falha: {type(e).__name__}: {str(e)}",
#             "tempo": 0
#         }
#
#         return [], stats
#
#
# #################################
# # Funções para processar Pipelines
# #################################
#
# def get_all_pipelines(client, object_type="deals"):
#     """Busca todos os pipelines de um tipo de objeto do HubSpot."""
#     logger.info(f"🔍 Buscando todos os pipelines de {object_type}")
#     try:
#         start_time = time.time()
#
#         # Obter os pipelines
#         pipelines_response = client.crm.pipelines.pipelines_api.get_all(object_type)
#         pipelines = pipelines_response.results
#
#         duration = time.time() - start_time
#         logger.info(f"✅ {len(pipelines)} pipelines obtidos em {duration:.2f}s")
#
#         # Processar pipelines para o formato esperado
#         processed_pipelines = []
#         for pipeline in pipelines:
#             pipeline_dict = {
#                 "id": pipeline.id,
#                 "label": pipeline.label,
#                 "display_order": pipeline.display_order,
#                 "created_at": pipeline.created_at,
#                 "updated_at": pipeline.updated_at,
#                 "archived": pipeline.archived
#             }
#
#             # Adicionar estágios do pipeline, se disponíveis
#             if hasattr(pipeline, 'stages') and pipeline.stages:
#                 # Criar uma representação dos estágios como propriedades adicionais
#                 for i, stage in enumerate(pipeline.stages):
#                     prefix = f"stage_{i + 1}_"
#                     pipeline_dict[f"{prefix}id"] = stage.id
#                     pipeline_dict[f"{prefix}label"] = stage.label
#                     pipeline_dict[f"{prefix}display_order"] = stage.display_order
#                     pipeline_dict[f"{prefix}created_at"] = stage.created_at
#                     pipeline_dict[f"{prefix}updated_at"] = stage.updated_at
#                     pipeline_dict[f"{prefix}archived"] = stage.archived
#
#                 pipeline_dict["total_stages"] = len(pipeline.stages)
#             else:
#                 pipeline_dict["total_stages"] = 0
#
#             processed_pipelines.append(pipeline_dict)
#
#         return processed_pipelines
#     except ApiException as e:
#         logger.error(f"❌ API Exception ao buscar pipelines: {str(e)}")
#         raise
#     except Exception as e:
#         logger.error(f"❌ Erro ao buscar pipelines: {str(e)}")
#         raise
#
#
# def process_pipelines_endpoint(client, endpoint_name="hubspot_pipelines", object_type="deals"):
#     """Processa o endpoint de pipelines."""
#     try:
#         logger.info(f"\n{'=' * 50}\n🔍 PROCESSANDO ENDPOINT: {endpoint_name.upper()}\n{'=' * 50}")
#
#         endpoint_start = time.time()
#
#         # Buscar todos os pipelines
#         raw_data = get_all_pipelines(client, object_type)
#
#         # Processar e salvar usando o Utils do projeto
#         logger.info(f"💾 Processando e salvando {len(raw_data)} registros para {endpoint_name}")
#         processed_data = Utils.process_and_save_data(raw_data, endpoint_name)
#
#         endpoint_duration = time.time() - endpoint_start
#
#         # Estatísticas de processamento
#         stats = {
#             "registros": len(processed_data),
#             "status": "Sucesso",
#             "tempo": endpoint_duration
#         }
#
#         logger.info(f"✅ {endpoint_name}: {len(processed_data)} registros em {endpoint_duration:.2f}s")
#
#         return processed_data, stats
#
#     except Exception as e:
#         logger.exception(f"❌ Falha no endpoint {endpoint_name}")
#
#         # Estatísticas em caso de erro
#         stats = {
#             "registros": 0,
#             "status": f"Falha: {type(e).__name__}: {str(e)}",
#             "tempo": 0
#         }
#
#         return [], stats
#
#
# #################################
# # Funções para processar Properties
# #################################
#
# def get_all_properties(client, object_type="deals"):
#     """Busca todas as propriedades de um tipo de objeto do HubSpot."""
#     logger.info(f"🔍 Buscando todas as propriedades de {object_type}")
#     try:
#         start_time = time.time()
#
#         # Obter as propriedades
#         properties_response = client.crm.properties.core_api.get_all(object_type)
#         properties = properties_response.results
#
#         duration = time.time() - start_time
#         logger.info(f"✅ {len(properties)} propriedades obtidas em {duration:.2f}s")
#
#         # Processar propriedades para o formato esperado (simplificado)
#         processed_properties = []
#         for prop in properties:
#             # Simplificado para seguir o exemplo fornecido
#             property_dict = {
#                 "property": prop.name,
#                 "label": prop.label
#             }
#
#             processed_properties.append(property_dict)
#
#         return processed_properties
#     except ApiException as e:
#         logger.error(f"❌ API Exception ao buscar propriedades: {str(e)}")
#         raise
#     except Exception as e:
#         logger.error(f"❌ Erro ao buscar propriedades: {str(e)}")
#         raise
#
#
# def process_properties_endpoint(client, endpoint_name="hubspot_properties", object_type="deals"):
#     """Processa o endpoint de propriedades."""
#     try:
#         logger.info(f"\n{'=' * 50}\n🔍 PROCESSANDO ENDPOINT: {endpoint_name.upper()}\n{'=' * 50}")
#
#         endpoint_start = time.time()
#
#         # Buscar todas as propriedades
#         raw_data = get_all_properties(client, object_type)
#
#         # Processar e salvar usando o Utils do projeto
#         logger.info(f"💾 Processando e salvando {len(raw_data)} registros para {endpoint_name}")
#         processed_data = Utils.process_and_save_data(raw_data, endpoint_name)
#
#         endpoint_duration = time.time() - endpoint_start
#
#         # Estatísticas de processamento
#         stats = {
#             "registros": len(processed_data),
#             "status": "Sucesso",
#             "tempo": endpoint_duration
#         }
#
#         logger.info(f"✅ {endpoint_name}: {len(processed_data)} registros em {endpoint_duration:.2f}s")
#
#         return processed_data, stats
#
#     except Exception as e:
#         logger.exception(f"❌ Falha no endpoint {endpoint_name}")
#
#         # Estatísticas em caso de erro
#         stats = {
#             "registros": 0,
#             "status": f"Falha: {type(e).__name__}: {str(e)}",
#             "tempo": 0
#         }
#
#         return [], stats
#
#
# def main():
#     """Função principal para coleta de dados da API HubSpot."""
#     # Iniciar relatório de processamento
#     global_start_time = ReportGenerator.init_report(logger)
#
#     try:
#         # 1. Obter configurações
#         args = get_arguments()
#
#         access_token = args.ACCESS_TOKEN
#         endpoints_filter = args.ENDPOINTS.split(',') if hasattr(args, 'ENDPOINTS') and args.ENDPOINTS else None
#         max_parallel_str = getattr(args, 'MAX_PARALLEL', str(MAX_WORKERS))
#         max_parallel = int(max_parallel_str)
#
#         # 2. Inicializar cliente HubSpot
#         hubspot_client = init_hubspot_client(access_token)
#
#         # 3. Filtrar endpoints a processar
#         endpoints_to_process = {}
#         for endpoint_name in ENDPOINTS:
#             if endpoints_filter is None or endpoint_name in endpoints_filter:
#                 endpoints_to_process[endpoint_name] = ENDPOINTS[endpoint_name]
#
#         logger.info(f"🔍 Endpoints a processar: {', '.join(endpoints_to_process.keys())}")
#
#         # 4. Processamento paralelo de endpoints
#         endpoint_stats = {}
#
#         # Função para processar um endpoint e retornar suas estatísticas
#         def process_endpoint_worker(endpoint_info):
#             endpoint_name, endpoint_type = endpoint_info
#
#             try:
#                 logger.info(f"🔄 Iniciando processamento paralelo do endpoint: {endpoint_name}")
#
#                 if endpoint_name == "hubspot_deals":
#                     # Processar endpoint de deals
#                     _, stats = process_deals_endpoint(hubspot_client, endpoint_name)
#
#                 elif endpoint_name == "hubspot_contacts":
#                     # Processar endpoint de contatos (usando async)
#                     _, stats = process_contacts_endpoint(access_token, endpoint_name)
#
#                 elif endpoint_name == "hubspot_owners":
#                     # Processar endpoint de owners
#                     _, stats = process_owners_endpoint(hubspot_client, endpoint_name)
#
#                 elif endpoint_name == "hubspot_pipelines":
#                     # Processar endpoint de pipelines
#                     object_type = ENDPOINT_PARAMS[endpoint_name].get("object_type", "deals")
#                     _, stats = process_pipelines_endpoint(hubspot_client, endpoint_name, object_type)
#
#                 elif endpoint_name == "hubspot_properties":
#                     # Processar endpoint de propriedades
#                     object_type = ENDPOINT_PARAMS[endpoint_name].get("object_type", "deals")
#                     _, stats = process_properties_endpoint(hubspot_client, endpoint_name, object_type)
#
#                 logger.info(f"✅ Processamento paralelo do endpoint {endpoint_name} concluído com sucesso")
#                 return endpoint_name, stats
#
#             except Exception as e:
#                 logger.error(f"❌ Erro no processamento paralelo do endpoint {endpoint_name}: {str(e)}")
#                 # Em caso de erro, retornar estatísticas de falha
#                 return endpoint_name, {
#                     "registros": 0,
#                     "status": f"Falha: {type(e).__name__}: {str(e)}",
#                     "tempo": 0
#                 }
#
#         # Usar ThreadPoolExecutor para processar endpoints em paralelo
#         # Limitar número de workers conforme configuração
#         max_workers = min(len(endpoints_to_process), max_parallel)
#         logger.info(f"🚀 Iniciando processamento paralelo com {max_workers} workers")
#
#         with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
#             # Submeter todos os endpoints para processamento
#             future_to_endpoint = {
#                 executor.submit(process_endpoint_worker, (endpoint_name, endpoint_type)):
#                     endpoint_name for endpoint_name, endpoint_type in endpoints_to_process.items()
#             }
#
#             # Coletar resultados à medida que são concluídos
#             for future in concurrent.futures.as_completed(future_to_endpoint):
#                 endpoint_name, stats = future.result()
#                 endpoint_stats[endpoint_name] = stats
#                 logger.info(
#                     f"📊 Endpoint {endpoint_name} concluído: {stats['registros']} registros em {stats['tempo']:.2f}s")
#
#         # 5. Gerar resumo final
#         success = ReportGenerator.final_summary(logger, endpoint_stats, global_start_time)
#
#         # Se houver falhas, lançar exceção
#         if not success:
#             raise Exception(f"Falhas nos endpoints: {success}")
#
#     except Exception as e:
#         logger.exception(f"❌ ERRO CRÍTICO NA EXECUÇÃO: {e}")
#         raise
#
#
# if __name__ == "__main__":
#     main()
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
MAX_WORKERS = min(10, os.cpu_count() or 5)
MAX_PROPERTIES_PER_REQUEST = 250
LIMIT_PER_PAGE = 100

# Definição dos endpoints da API HubSpot
ENDPOINTS = {
    "hubspot_deals": "deals",
    "hubspot_contacts": "contacts",
    "hubspot_owners": "owners",
    "hubspot_pipelines": "pipelines",
    "hubspot_properties": "properties"
}

# Parâmetros específicos para cada endpoint
ENDPOINT_PARAMS = {
    "hubspot_deals": {"limit": LIMIT_PER_PAGE},
    "hubspot_contacts": {"limit": LIMIT_PER_PAGE},
    "hubspot_owners": {},
    "hubspot_pipelines": {"object_type": "deals"},
    "hubspot_properties": {"object_type": "deals"}
}


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


#################################
# Funções para processar Deals
#################################

def get_all_deal_properties(client):
    """Busca todas as propriedades disponíveis para deals."""
    logger.info("🔍 Buscando todas as propriedades de deals")
    try:
        start_time = time.time()
        response = client.crm.properties.core_api.get_all('deals')

        # Limitar a 700 propriedades para evitar problemas de performance
        all_properties = [prop.name for prop in response.results][:700]

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


def process_deals_endpoint(client, endpoint_name="hubspot_deals", limit=LIMIT_PER_PAGE):
    """Processa o endpoint de deals."""
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


#################################
# Funções para processar Contacts
#################################

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


async def fetch_contacts_page_with_properties(access_token, properties, after=None, session=None):
    """Busca uma página de contatos com as propriedades especificadas usando aiohttp."""
    url = "https://api.hubapi.com/crm/v3/objects/contacts"
    params = {
        "limit": LIMIT_PER_PAGE,
        "properties": properties
    }
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


async def process_contacts_batch_with_properties_async(client, access_token, after=None, max_contacts=100000):
    """Processa lotes de contatos com todas as propriedades usando requisições assíncronas."""
    logger.info(f"🔄 Iniciando coleta de contatos com propriedades a partir de: {after or 'início'}")

    # 1. Obter todas as propriedades de contatos
    all_properties = get_all_contact_properties(client)

    # 2. Dividir propriedades em grupos
    property_groups = create_property_groups(all_properties, MAX_PROPERTIES_PER_REQUEST)
    logger.info(f"📊 Propriedades de contatos divididas em {len(property_groups)} grupos de requisição")

    all_contacts = []
    current_after = after
    batch_count = 0
    has_more = True

    async with aiohttp.ClientSession() as session:
        while has_more and len(all_contacts) < max_contacts:
            batch_start_time = time.time()

            # Manter o controle do dicionário de objetos
            contact_objects = {}

            # Para cada grupo de propriedades, fazer uma requisição
            for group_index, properties in enumerate(property_groups):
                logger.info(f"🔄 Buscando contatos com grupo de propriedades {group_index + 1}/{len(property_groups)}")

                data = await fetch_contacts_page_with_properties(access_token, properties, current_after, session)

                if data is None or 'results' not in data:
                    logger.error(f"❌ Falha ao obter resultados da API para grupo {group_index + 1}")
                    continue

                contacts = data['results']

                # Processar cada contato
                for contact in contacts:
                    contact_id = contact['id']

                    # Verificar se já processamos este contato
                    if contact_id in contact_objects:
                        # Adicionar propriedades ao contato existente
                        for prop_name, prop_value in contact['properties'].items():
                            contact_objects[contact_id]['properties'][prop_name] = prop_value
                    else:
                        # Adicionar novo contato
                        contact_objects[contact_id] = contact

                # Atualizar flag de paginação (apenas na primeira iteração)
                if group_index == 0:
                    has_more = 'paging' in data and 'next' in data['paging']
                    if has_more:
                        current_after = data['paging']['next']['after']
                    else:
                        logger.info("🏁 Fim da paginação alcançado")

                logger.info(f"✓ Grupo {group_index + 1}/{len(property_groups)} processado")

            # Adicionar contatos processados ao resultado final
            all_contacts.extend(list(contact_objects.values()))
            batch_count += 1

            # Log de progresso
            if batch_count % 5 == 0:
                logger.info(f"📊 Progresso: {len(all_contacts)} contatos coletados em {batch_count} lotes")

            batch_duration = time.time() - batch_start_time
            logger.info(f"✓ Lote {batch_count} processado em {batch_duration:.2f}s, {len(contact_objects)} contatos")

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


def process_contacts_endpoint(client, access_token, endpoint_name="hubspot_contacts", after_key=None):
    """Processa o endpoint de contatos com todas as propriedades."""
    try:
        logger.info(f"\n{'=' * 50}\n🔍 PROCESSANDO ENDPOINT: {endpoint_name.upper()}\n{'=' * 50}")

        endpoint_start = time.time()

        # Criar um novo event loop para esta thread
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        # Usar o loop criado para executar a função assíncrona
        raw_data, last_after = loop.run_until_complete(
            process_contacts_batch_with_properties_async(client, access_token, after_key)
        )

        # Fechar o loop quando terminar
        loop.close()

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


#################################
# Funções para processar Owners
#################################

def get_all_owners(client):
    """Busca todos os owners do HubSpot."""
    logger.info("🔍 Buscando todos os owners")
    try:
        start_time = time.time()

        # Obter a página de owners
        owners_response = client.crm.owners.owners_api.get_page()
        owners = owners_response.results

        duration = time.time() - start_time
        logger.info(f"✅ {len(owners)} owners obtidos em {duration:.2f}s")

        # Processar owners para o formato esperado
        processed_owners = []
        for owner in owners:
            owner_dict = owner.to_dict()
            processed_owners.append(owner_dict)

        return processed_owners
    except ApiException as e:
        logger.error(f"❌ API Exception ao buscar owners: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"❌ Erro ao buscar owners: {str(e)}")
        raise


def process_owners_endpoint(client, endpoint_name="hubspot_owners"):
    """Processa o endpoint de owners."""
    try:
        logger.info(f"\n{'=' * 50}\n🔍 PROCESSANDO ENDPOINT: {endpoint_name.upper()}\n{'=' * 50}")

        endpoint_start = time.time()

        # Buscar todos os owners
        raw_data = get_all_owners(client)

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


#################################
# Funções para processar Pipelines
#################################

def get_all_pipelines(client, object_type="deals"):
    """Busca todos os pipelines de um tipo de objeto do HubSpot."""
    logger.info(f"🔍 Buscando todos os pipelines de {object_type}")
    try:
        start_time = time.time()

        # Obter os pipelines
        pipelines_response = client.crm.pipelines.pipelines_api.get_all(object_type)
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


def process_pipelines_endpoint(client, endpoint_name="hubspot_pipelines", object_type="deals"):
    """Processa o endpoint de pipelines."""
    try:
        logger.info(f"\n{'=' * 50}\n🔍 PROCESSANDO ENDPOINT: {endpoint_name.upper()}\n{'=' * 50}")

        endpoint_start = time.time()

        # Buscar todos os pipelines
        raw_data = get_all_pipelines(client, object_type)

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


#################################
# Funções para processar Properties
#################################

def get_all_properties(client, object_type="deals"):
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


def process_properties_endpoint(client, endpoint_name="hubspot_properties", object_type="deals"):
    """Processa o endpoint de propriedades."""
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
    """Função principal para coleta de dados da API HubSpot."""
    # Iniciar relatório de processamento
    global_start_time = ReportGenerator.init_report(logger)

    try:
        # 1. Obter configurações
        args = get_arguments()

        access_token = args.ACCESS_TOKEN
        endpoints_filter = args.ENDPOINTS.split(',') if hasattr(args, 'ENDPOINTS') and args.ENDPOINTS else None
        max_parallel_str = getattr(args, 'MAX_PARALLEL', str(MAX_WORKERS))
        max_parallel = int(max_parallel_str)

        # 2. Inicializar cliente HubSpot
        hubspot_client = init_hubspot_client(access_token)

        # 3. Filtrar endpoints a processar
        endpoints_to_process = {}
        for endpoint_name in ENDPOINTS:
            if endpoints_filter is None or endpoint_name in endpoints_filter:
                endpoints_to_process[endpoint_name] = ENDPOINTS[endpoint_name]

        logger.info(f"🔍 Endpoints a processar: {', '.join(endpoints_to_process.keys())}")

        # 4. Processamento paralelo de endpoints
        endpoint_stats = {}

        # Função para processar um endpoint e retornar suas estatísticas
        def process_endpoint_worker(endpoint_info):
            endpoint_name, endpoint_type = endpoint_info

            try:
                logger.info(f"🔄 Iniciando processamento paralelo do endpoint: {endpoint_name}")

                if endpoint_name == "hubspot_deals":
                    # Processar endpoint de deals
                    _, stats = process_deals_endpoint(hubspot_client, endpoint_name)

                elif endpoint_name == "hubspot_contacts":
                    # Processar endpoint de contatos com todas as propriedades
                    _, stats = process_contacts_endpoint(hubspot_client, access_token, endpoint_name)

                elif endpoint_name == "hubspot_owners":
                    # Processar endpoint de owners
                    _, stats = process_owners_endpoint(hubspot_client, endpoint_name)

                elif endpoint_name == "hubspot_pipelines":
                    # Processar endpoint de pipelines
                    object_type = ENDPOINT_PARAMS[endpoint_name].get("object_type", "deals")
                    _, stats = process_pipelines_endpoint(hubspot_client, endpoint_name, object_type)

                elif endpoint_name == "hubspot_properties":
                    # Processar endpoint de propriedades
                    object_type = ENDPOINT_PARAMS[endpoint_name].get("object_type", "deals")
                    _, stats = process_properties_endpoint(hubspot_client, endpoint_name, object_type)

                logger.info(f"✅ Processamento paralelo do endpoint {endpoint_name} concluído com sucesso")
                return endpoint_name, stats

            except Exception as e:
                logger.error(f"❌ Erro no processamento paralelo do endpoint {endpoint_name}: {str(e)}")
                # Em caso de erro, retornar estatísticas de falha
                return endpoint_name, {
                    "registros": 0,
                    "status": f"Falha: {type(e).__name__}: {str(e)}",
                    "tempo": 0
                }

        # Usar ThreadPoolExecutor para processar endpoints em paralelo
        # Limitar número de workers conforme configuração
        max_workers = min(len(endpoints_to_process), max_parallel)
        logger.info(f"🚀 Iniciando processamento paralelo com {max_workers} workers")

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submeter todos os endpoints para processamento
            future_to_endpoint = {
                executor.submit(process_endpoint_worker, (endpoint_name, endpoint_type)):
                    endpoint_name for endpoint_name, endpoint_type in endpoints_to_process.items()
            }

            # Coletar resultados à medida que são concluídos
            for future in concurrent.futures.as_completed(future_to_endpoint):
                endpoint_name, stats = future.result()
                endpoint_stats[endpoint_name] = stats
                logger.info(
                    f"📊 Endpoint {endpoint_name} concluído: {stats['registros']} registros em {stats['tempo']:.2f}s")

        # 5. Gerar resumo final
        success = ReportGenerator.final_summary(logger, endpoint_stats, global_start_time)

        # Se houver falhas, lançar exceção
        if not success:
            raise Exception(f"Falhas nos endpoints: {success}")

    except Exception as e:
        logger.exception(f"❌ ERRO CRÍTICO NA EXECUÇÃO: {e}")
        raise


if __name__ == "__main__":
    main()
