import time
from pathlib import Path

from commons.app_inicializer import AppInitializer
from commons.big_query import BigQuery
from commons.memory_monitor import MemoryMonitor
from commons.report_generator import ReportGenerator
from commons.utils import Utils
from generic.argument_manager import ArgumentManager
from generic.http_client import HttpClient
from generic.rate_limiter import RateLimiter

logger = AppInitializer.initialize()

RATE_LIMIT = 5
ENDPOINTS = {
    "deals": "api/3/deals",
    "deal_groups": "api/3/dealGroups",
    "deal_stages": "api/3/dealStages",
    "deal_custom_field_meta": "api/3/dealCustomFieldMeta",
    "contacts": "api/3/contacts",
}

# ENDPOINTS DEPENDENTES
DEPENDENT_ENDPOINTS = [
    "contact_tags",  # /api/3/contacts/{id}/contactTags
    "tags"  # /api/3/contactTags/{id}/tag
]


def get_arguments():
    """Configura e retorna os argumentos da linha de comando."""
    return (ArgumentManager("Script para coletar e processar dados da API ActiveCampaign")
            .add("ACCOUNT_NAME", "Nome da conta ActiveCampaign", required=True)
            .add("API_TOKEN", "Token de autenticação", required=True)
            .add("PROJECT_ID", "ID do projeto no BigQuery", required=True)
            .add("CRM_TYPE", "Tipo do CRM", required=True)
            .add("GOOGLE_APPLICATION_CREDENTIALS", "Caminho para as credenciais do Google", required=True)
            .parse())


def fetch_data(endpoint, token, offset=0, params=None, debug_info=None):
    """Busca dados de um endpoint específico."""
    headers = {"Api-Token": token}

    # Use valor padrão para parâmetros - limite máximo da ActiveCampaign
    params = params or {}
    params.update({"offset": offset, "limit": 100})

    debug_info = debug_info or f"{endpoint}:offset{offset}"

    return http_client.get(endpoint, headers=headers, params=params, debug_info=debug_info)


def fetch_single_item(endpoint, token, debug_info=None):
    """Busca um item único de um endpoint (para endpoints de dependência)."""
    headers = {"Api-Token": token}
    debug_info = debug_info or endpoint
    return http_client.get(endpoint, headers=headers, debug_info=debug_info)


def get_data_key_for_endpoint(endpoint_path):
    """Retorna a chave de dados baseada no endpoint."""
    if "contacts/" in endpoint_path and "contactTags" in endpoint_path:
        return "contactTags"
    elif "contactTags/" in endpoint_path and "tag" in endpoint_path:
        return "tag"
    elif endpoint_path == "api/3/contacts":
        return "scoreValues"
    else:
        # Para endpoints normais, pega o último segmento da URL
        return endpoint_path.split('/')[-1]


def fetch_page(endpoint, token, offset=0):
    """Busca uma página específica de um endpoint."""
    try:
        data = fetch_data(endpoint, token, offset)

        # Extrair dados baseado na estrutura do endpoint
        data_key = get_data_key_for_endpoint(endpoint)
        items = data.get(data_key, [])

        # Informações de paginação da ActiveCampaign
        meta = data.get('meta', {})
        total = int(meta.get('total', 0))
        current_offset = offset
        limit = 100

        # Calcular páginas baseado no offset
        current_page = (current_offset // limit) + 1
        total_pages = (total + limit - 1) // limit if total > 0 else 1

        # Simplificação da condição de log - mais frequente para acompanhar progresso
        if current_page == 1 or current_page == total_pages or current_page % 50 == 0:
            logger.info(f"📄 Endpoint {endpoint}: página {current_page}/{total_pages} com {len(items)} itens")

        return {
            'items': items,
            'meta': {
                'total': total,
                'limit': limit,
                'offset': current_offset,
                'current_page': current_page,
                'total_pages': total_pages
            },
            'total_pages': total_pages,
            'has_more': (current_offset + limit) < total
        }
    except Exception as e:
        logger.error(f"❌ Erro ao buscar offset {offset} para {endpoint}: {str(e)}")
        raise


def fetch_all_pages(endpoint, token):
    """Busca todas as páginas de um endpoint."""
    logger.info(f"📚 Buscando todas as páginas para: {endpoint}")
    start_time = time.time()

    # Buscar primeira página
    first_page = fetch_page(endpoint, token, 0)
    all_items = first_page['items'].copy()
    total_pages = first_page['total_pages']

    if total_pages <= 1 or not first_page['has_more']:
        logger.info(f"✓ Endpoint {endpoint}: 1 página com {len(all_items)} itens obtidos")
        return all_items

    # Buscar páginas restantes usando offset
    current_offset = 100  # Próximo offset
    page_num = 2

    while current_offset < first_page['meta']['total']:
        try:
            page_data = fetch_page(endpoint, token, current_offset)
            all_items.extend(page_data['items'])

            # Rate limiter já controla - sem sleep adicional

            # Próximo offset
            current_offset += 100
            page_num += 1

            # Se não há mais dados, parar
            if not page_data['has_more']:
                break

        except Exception as e:
            logger.error(f"❌ Erro no offset {current_offset} para {endpoint}: {str(e)}")
            break

    duration = time.time() - start_time
    logger.info(f"✅ Endpoint {endpoint}: {total_pages} páginas com {len(all_items)} itens obtidos em {duration:.2f}s")
    return all_items


def fetch_contact_tags_batch(contact_ids, token):
    """Busca contactTags para um batch de contact IDs sequencialmente."""
    contact_tags = []
    
    # Processamento 100% sequencial para evitar rate limit
    for contact_id in contact_ids:
        try:
            endpoint = f"api/3/contacts/{contact_id}/contactTags"
            data = fetch_single_item(endpoint, token, f"contactTags:{contact_id}")
            tags = data.get('contactTags', [])
            contact_tags.extend(tags)
            
            # Pausa controlada para respeitar 5 req/s (0.2s por request)
            time.sleep(0.25)  # Um pouco mais conservador
            
        except Exception as e:
            logger.error(f"❌ Erro ao buscar contactTags para contato {contact_id}: {str(e)}")
            continue
    
    return contact_tags


def process_contact_tags(contact_ids, token):
    """Processa contactTags para uma lista de contact IDs com paralelismo."""
    logger.info(f"\n🔍 PROCESSANDO CONTACT TAGS para {len(contact_ids)} contatos")
    start_time = time.time()

    all_contact_tags = []
    processed_count = 0
    batch_size = 10  # Lotes pequenos para processamento sequencial

    # Processar em batches
    for i in range(0, len(contact_ids), batch_size):
        batch = contact_ids[i:i + batch_size]
        
        try:
            batch_tags = fetch_contact_tags_batch(batch, token)
            all_contact_tags.extend(batch_tags)
            
            processed_count += len(batch)
            logger.info(f"📄 Processados {processed_count}/{len(contact_ids)} contatos")
            
            # Sem pausa entre batches - sleep interno já controla
                
        except Exception as e:
            logger.error(f"❌ Erro no batch {i}-{i+len(batch)}: {str(e)}")
            continue

    duration = time.time() - start_time
    logger.info(f"✅ ContactTags: {len(all_contact_tags)} registros de {len(contact_ids)} contatos em {duration:.2f}s")
    return all_contact_tags


def fetch_tags_batch(contact_tag_ids, token):
    """Busca tags para um batch de contactTag IDs sequencialmente."""
    tags = []
    unique_tag_ids = set()  # Para evitar duplicatas
    
    # Processamento 100% sequencial para evitar rate limit
    for contact_tag_id in contact_tag_ids:
        try:
            endpoint = f"api/3/contactTags/{contact_tag_id}/tag"
            data = fetch_single_item(endpoint, token, f"tag:{contact_tag_id}")
            tag_data = data.get('tag', {})
            
            if tag_data and tag_data.get('id'):
                if tag_data.get('id') not in unique_tag_ids:
                    unique_tag_ids.add(tag_data.get('id'))
                    tags.append(tag_data)
            
            # Pausa controlada para respeitar 5 req/s (0.2s por request)
            time.sleep(0.25)  # Um pouco mais conservador
                    
        except Exception as e:
            logger.error(f"❌ Erro ao buscar tag para contactTag {contact_tag_id}: {str(e)}")
            continue
    
    return tags


def process_tags(contact_tag_ids, token):
    """Processa tags para uma lista de contactTag IDs com paralelismo."""
    logger.info(f"\n🔍 PROCESSANDO TAGS para {len(contact_tag_ids)} contactTags")
    start_time = time.time()

    all_tags = []
    processed_count = 0
    batch_size = 10  # Lotes pequenos para processamento sequencial

    # Processar em batches
    for i in range(0, len(contact_tag_ids), batch_size):
        batch = contact_tag_ids[i:i + batch_size]
        
        try:
            batch_tags = fetch_tags_batch(batch, token)
            all_tags.extend(batch_tags)
            
            processed_count += len(batch)
            logger.info(f"📄 Processadas {processed_count}/{len(contact_tag_ids)} tags")
            
            # Sem pausa entre batches - rate limiter controla  
            # Rate limiter interno já gerencia o limite
                
        except Exception as e:
            logger.error(f"❌ Erro no batch {i}-{i+len(batch)}: {str(e)}")
            continue

    duration = time.time() - start_time
    logger.info(f"✅ Tags: {len(all_tags)} registros únicos de {len(contact_tag_ids)} contactTags em {duration:.2f}s")
    return all_tags


def process_endpoint(endpoint_name, endpoint_path, token, args):
    """Processa um endpoint específico e retorna estatísticas."""
    try:
        logger.info(f"\n{'=' * 50}\n🔍 PROCESSANDO ENDPOINT: {endpoint_name.upper()}\n{'=' * 50}")

        endpoint_start = time.time()
        raw_data = fetch_all_pages(endpoint_path, token)

        # Processar e salvar dados
        logger.info(f"💾 Processando e salvando {len(raw_data)} registros para {endpoint_name}")
        processed_data = Utils.process_and_save_data(raw_data, endpoint_name)

        # Processar schema e criar tabelas no BigQuery imediatamente
        if processed_data:
            logger.info(f"🔧 Criando schema e tabelas no BigQuery para {endpoint_name}")
            with MemoryMonitor(logger):
                # Processar apenas o CSV específico do endpoint
                csv_path = Path(f"output/{endpoint_name}/{endpoint_name}.csv")
                if csv_path.exists():
                    bigquery_generator = BigQuery()
                    bigquery_generator._process_single_csv(csv_path)
                    logger.info(f"✅ Schema gerado para {endpoint_name}")
                else:
                    logger.warning(f"⚠️ Arquivo CSV não encontrado: {csv_path}")
            
            BigQuery.start_pipeline(args.PROJECT_ID, args.CRM_TYPE, table_name=endpoint_name,
                                    credentials_path=args.GOOGLE_APPLICATION_CREDENTIALS)
            logger.info(f"✅ Schema e tabelas criadas para {endpoint_name}")

        endpoint_duration = time.time() - endpoint_start

        # Retornar estatísticas
        return {
            "registros": len(processed_data),
            "status": "Sucesso",
            "tempo": endpoint_duration,
            "data": raw_data  # Retorna os dados para uso em endpoints dependentes
        }

    except Exception as e:
        logger.exception(f"❌ Falha no endpoint {endpoint_name}")
        return {
            "registros": 0,
            "status": f"Falha: {type(e).__name__}: {str(e)}",
            "tempo": 0,
            "data": []
        }


def process_dependent_endpoints(contacts_data, token, args):
    """Processa os endpoints dependentes baseados nos dados de contatos."""
    dependent_stats = {}

    # Extrair IDs dos contatos
    contact_ids = [contact.get('contact') for contact in contacts_data if contact.get('contact')]

    if not contact_ids:
        logger.warning("⚠️  Nenhum contact ID encontrado para processar endpoints dependentes")
        return dependent_stats

    # 1. Processar contact_tags
    try:
        logger.info(f"\n{'=' * 50}\n🔍 PROCESSANDO ENDPOINT: CONTACT_TAGS\n{'=' * 50}")
        contact_tags_start = time.time()

        contact_tags_data = process_contact_tags(contact_ids, token)

        # Salvar dados
        logger.info(f"💾 Salvando {len(contact_tags_data)} registros para contact_tags")
        processed_contact_tags = Utils.process_and_save_data(contact_tags_data, "contact_tags")

        # Processar schema e criar tabelas no BigQuery imediatamente
        if processed_contact_tags:
            logger.info(f"🔧 Criando schema e tabelas no BigQuery para contact_tags")
            with MemoryMonitor(logger):
                # Processar apenas o CSV específico do endpoint
                csv_path = Path("output/contact_tags/contact_tags.csv")
                if csv_path.exists():
                    bigquery_generator = BigQuery()
                    bigquery_generator._process_single_csv(csv_path)
                    logger.info(f"✅ Schema gerado para contact_tags")
                else:
                    logger.warning(f"⚠️ Arquivo CSV não encontrado: {csv_path}")
            
            BigQuery.start_pipeline(args.PROJECT_ID, args.CRM_TYPE, table_name="contact_tags",
                                    credentials_path=args.GOOGLE_APPLICATION_CREDENTIALS)
            logger.info(f"✅ Schema e tabelas criadas para contact_tags")

        contact_tags_duration = time.time() - contact_tags_start
        dependent_stats["contact_tags"] = {
            "registros": len(processed_contact_tags),
            "status": "Sucesso",
            "tempo": contact_tags_duration
        }

    except Exception as e:
        logger.exception("❌ Falha no endpoint contact_tags")
        dependent_stats["contact_tags"] = {
            "registros": 0,
            "status": f"Falha: {type(e).__name__}: {str(e)}",
            "tempo": 0
        }
        contact_tags_data = []

    # 2. Processar tags
    try:
        logger.info(f"\n{'=' * 50}\n🔍 PROCESSANDO ENDPOINT: TAGS\n{'=' * 50}")
        tags_start = time.time()

        # Extrair IDs dos contactTags
        contact_tag_ids = [ct.get('id') for ct in contact_tags_data if ct.get('id')]

        if contact_tag_ids:
            tags_data = process_tags(contact_tag_ids, token)

            # Salvar dados
            logger.info(f"💾 Salvando {len(tags_data)} registros para tags")
            processed_tags = Utils.process_and_save_data(tags_data, "tags")

            # Processar schema e criar tabelas no BigQuery imediatamente
            if processed_tags:
                logger.info(f"🔧 Criando schema e tabelas no BigQuery para tags")
                with MemoryMonitor(logger):
                    # Processar apenas o CSV específico do endpoint
                    csv_path = Path("output/tags/tags.csv")
                    if csv_path.exists():
                        bigquery_generator = BigQuery()
                        bigquery_generator._process_single_csv(csv_path)
                        logger.info(f"✅ Schema gerado para tags")
                    else:
                        logger.warning(f"⚠️ Arquivo CSV não encontrado: {csv_path}")
                
                BigQuery.start_pipeline(args.PROJECT_ID, args.CRM_TYPE, table_name="tags",
                                        credentials_path=args.GOOGLE_APPLICATION_CREDENTIALS)
                logger.info(f"✅ Schema e tabelas criadas para tags")

            tags_duration = time.time() - tags_start
            dependent_stats["tags"] = {
                "registros": len(processed_tags),
                "status": "Sucesso",
                "tempo": tags_duration
            }
        else:
            logger.warning("⚠️  Nenhum contactTag ID encontrado para processar tags")
            dependent_stats["tags"] = {
                "registros": 0,
                "status": "Nenhum contactTag ID disponível",
                "tempo": 0
            }

    except Exception as e:
        logger.exception("❌ Falha no endpoint tags")
        dependent_stats["tags"] = {
            "registros": 0,
            "status": f"Falha: {type(e).__name__}: {str(e)}",
            "tempo": 0
        }

    return dependent_stats


def main():
    """Função principal para coleta de dados."""
    # 1. Obter argumentos de linha de comando
    args = get_arguments()

    # 2. Configurar cliente HTTP
    global http_client
    account_name = args.ACCOUNT_NAME
    api_base_url = f"https://{account_name}.api-us1.com"
    api_token = args.API_TOKEN

    rate_limiter = RateLimiter(requests_per_window=4, window_seconds=1, max_rate_limit_attempts=10000, logger=logger)  # Mais conservador
    http_client = HttpClient(base_url=api_base_url, rate_limiter=rate_limiter, logger=logger)

    # 3. Iniciar relatório
    global_start_time = ReportGenerator.init_report(logger)
    endpoint_stats = {}
    contacts_data = []

    try:
        # 4. Processar todos os endpoints principais
        for endpoint_name, endpoint_path in ENDPOINTS.items():
            result = process_endpoint(endpoint_name, endpoint_path, api_token, args)
            endpoint_stats[endpoint_name] = {
                "registros": result["registros"],
                "status": result["status"],
                "tempo": result["tempo"]
            }

            # Guardar dados de contatos para processar endpoints dependentes
            if endpoint_name == "contacts":
                contacts_data = result.get("data", [])

            logger.info(
                f"✅ {endpoint_name}: {endpoint_stats[endpoint_name]['registros']} registros em {endpoint_stats[endpoint_name]['tempo']:.2f}s")

        # 5. Processar endpoints dependentes
        dependent_stats = process_dependent_endpoints(contacts_data, api_token, args)

        # Adicionar estatísticas dos endpoints dependentes
        for endpoint_name, stats in dependent_stats.items():
            endpoint_stats[endpoint_name] = stats
            logger.info(f"✅ {endpoint_name}: {stats['registros']} registros em {stats['tempo']:.2f}s")

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
