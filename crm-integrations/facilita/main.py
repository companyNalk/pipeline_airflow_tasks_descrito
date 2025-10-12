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

RATE_LIMIT = 100

# Configuração dos relatórios disponíveis
REPORTS_CONFIG = {
    "negocios": {
        "pages": [
            "bi_completo_negocios_p1",
            "bi_completo_negocios_p2",
            "bi_completo_negocios_p3",
            "bi_completo_negocios_p4",
            "bi_completo_negocios_p5"
        ],
        "max_records": 6000,
        "description": "Relatório completo de negócios"
    },
    "leads": {
        "pages": [
            "bi_relatorio_evolucao_leads_p1",
            "bi_relatorio_evolucao_leads_p2",
            "bi_relatorio_evolucao_leads_p3"
        ],
        "max_records": 8000,
        "description": "Relatório de evolução de leads"
    }
}


def clean_cpf(cpf_value):
    """
    Limpa e valida o valor do CPF.
    """
    if not cpf_value or cpf_value is None:
        return ""

    # Converter para string
    cpf_str = str(cpf_value).strip()

    # Se for string vazia, retornar vazio
    if not cpf_str:
        return ""

    # Remover pontos e hífens
    cpf_cleaned = cpf_str.replace(".", "").replace("-", "")

    # Verificar se contém apenas números
    if cpf_cleaned.isdigit():
        return cpf_cleaned
    else:
        # Se contém outros caracteres, retornar vazio
        logger.warning(f"CPF inválido encontrado: '{cpf_value}' - será deixado vazio")
        return ""


def clean_data_fields(data_list):
    """
    Limpa campos específicos dos dados, incluindo CPF.
    """
    if not isinstance(data_list, list):
        return data_list

    cleaned_data = []
    cpf_issues_count = 0

    for item in data_list:
        if isinstance(item, dict):
            # Criar cópia do item para não modificar o original
            cleaned_item = item.copy()

            # Limpar campo CPF se existir
            if 'cpf' in cleaned_item:
                original_cpf = cleaned_item['cpf']
                cleaned_cpf = clean_cpf(original_cpf)

                if original_cpf != cleaned_cpf and original_cpf:
                    cpf_issues_count += 1

                cleaned_item['cpf'] = cleaned_cpf

            cleaned_data.append(cleaned_item)
        else:
            cleaned_data.append(item)

    if cpf_issues_count > 0:
        logger.info(f"🔧 {cpf_issues_count} CPFs foram limpos/corrigidos")

    return cleaned_data


def get_arguments():
    """Configura e retorna os argumentos da linha de comando."""
    return (ArgumentManager("Script para coletar dados da API AppFacilita")
            .add("API_BASE_URL", "URL base", required=True, default="https://bi.appfacilita.com")
            .add("FACILITA_TOKEN", "Token de autenticação", required=True)
            .add("FACILITA_INSTANCE", "Instância do AppFacilita", required=True)
            .add("PLATFORM_API_BASE_URL", "URL base da API Platform", required=False,
                 default="https://api.facilitaapp.com/platform/v1")
            .add("PLATFORM_API_INSTANCE", "API Instance para Platform API", required=False)
            .add("PLATFORM_API_KEY", "API Key para Platform API", required=False)
            .add("PLATFORM_TOKEN_USER", "Token User para Platform API", required=False)
            .add("PROJECT_ID", "ID do projeto GCP", required=True)
            .add("CRM_TYPE", "Tipo do CRM", required=True)
            .add("GOOGLE_APPLICATION_CREDENTIALS", "Credencial GCS", required=True)
            .parse())


def fetch_report_data(report_name, token, instance, debug_info=None):
    """Busca dados de um relatório específico."""
    headers = {
        "facilita_token": token,
        "facilita_instance": instance
    }

    # Parâmetros da requisição
    params = {"report": report_name}

    debug_info = debug_info or f"report:{report_name}"

    return http_client.get("analyses", headers=headers, params=params, debug_info=debug_info)


def fetch_report_page(report_name, token, instance):
    """Busca uma página específica de um relatório."""
    try:
        logger.info(f"📄 Buscando relatório: {report_name}")
        data = fetch_report_data(report_name, token, instance)

        # Como cada página é um relatório separado, retornamos os dados diretamente
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict) and 'data' in data:
            items = data['data'] if isinstance(data['data'], list) else [data['data']]
        else:
            items = [data] if data else []

        # Aplicar limpeza dos dados, incluindo CPF
        items = clean_data_fields(items)

        logger.info(f"✓ Relatório {report_name}: {len(items)} registros obtidos e limpos")

        return {
            'items': items,
            'report_name': report_name,
            'record_count': len(items)
        }

    except Exception as e:
        logger.error(f"❌ Erro ao buscar relatório {report_name}: {str(e)}")
        raise


def fetch_funnel_data(api_instance, api_key, token_user, platform_api_base_url):
    """Busca dados do endpoint /funnel da Platform API."""
    try:
        logger.info(f"📊 Buscando dados do funnel da Platform API")

        # Criar um cliente HTTP específico para a Platform API
        platform_rate_limiter = RateLimiter(requests_per_window=RATE_LIMIT, logger=logger)
        platform_http_client = HttpClient(base_url=platform_api_base_url, rate_limiter=platform_rate_limiter,
                                          logger=logger)

        headers = {
            "api-instance": api_instance,
            "api-key": api_key,
            "token-user": token_user
        }

        data = platform_http_client.get("funnel", headers=headers, debug_info="funnel")

        # Processar a resposta
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict) and 'data' in data:
            items = data['data'] if isinstance(data['data'], list) else [data['data']]
        else:
            items = [data] if data else []

        logger.info(f"✓ Funnel: {len(items)} registros obtidos")

        return items

    except Exception as e:
        logger.error(f"❌ Erro ao buscar funnel: {str(e)}")
        raise


def fetch_all_report_pages(report_type, token, instance):
    """Busca todas as páginas de um tipo de relatório."""
    logger.info(f"📚 Buscando todas as páginas para relatório: {report_type}")
    start_time = time.time()

    config = REPORTS_CONFIG.get(report_type)
    if not config:
        raise ValueError(f"Tipo de relatório '{report_type}' não configurado")

    all_items = []
    pages = config["pages"]

    logger.info(f"🔍 {config['description']} - {len(pages)} páginas para processar")

    for i, report_name in enumerate(pages, 1):
        try:
            page_data = fetch_report_page(report_name, token, instance)
            all_items.extend(page_data['items'])

            logger.info(f"📊 Página {i}/{len(pages)}: {page_data['record_count']} registros")

            # Pausa pequena entre requisições
            if i < len(pages):
                time.sleep(0.5)

        except Exception as e:
            logger.error(f"❌ Erro na página {report_name}: {str(e)}")
            # Continua com as outras páginas mesmo se uma falhar
            continue

    duration = time.time() - start_time
    max_expected = config["max_records"]

    logger.info(
        f"✅ Relatório {report_type}: {len(pages)} páginas com {len(all_items)} registros obtidos em {duration:.2f}s")
    logger.info(f"📈 Registros obtidos: {len(all_items)}/{max_expected} (máximo esperado)")

    return all_items


def process_report_type(report_type, token, instance):
    """Processa um tipo de relatório específico e retorna estatísticas."""
    try:
        logger.info(f"\n{'=' * 60}\n🔍 PROCESSANDO RELATÓRIO: {report_type.upper()}\n{'=' * 60}")

        config = REPORTS_CONFIG.get(report_type)
        if not config:
            raise ValueError(f"Tipo de relatório '{report_type}' não encontrado")

        logger.info(f"📋 {config['description']}")
        logger.info(f"📄 Páginas configuradas: {len(config['pages'])}")
        logger.info(f"📊 Registros esperados: até {config['max_records']}")

        report_start = time.time()
        raw_data = fetch_all_report_pages(report_type, token, instance)

        # Processar e salvar dados
        logger.info(f"💾 Processando e salvando {len(raw_data)} registros para {report_type}")
        processed_data = Utils.process_and_save_data(raw_data, report_type)

        report_duration = time.time() - report_start

        # Retornar estatísticas
        return {
            "registros": len(processed_data),
            "status": "Sucesso",
            "tempo": report_duration,
            "paginas": len(config['pages'])
        }

    except Exception as e:
        logger.exception(f"❌ Falha no relatório {report_type}")
        return {
            "registros": 0,
            "status": f"Falha: {type(e).__name__}: {str(e)}",
            "tempo": 0,
            "paginas": 0
        }


def main():
    """Função principal para coleta de dados."""
    # 1. Obter argumentos de linha de comando
    args = get_arguments()

    # 2. Configurar cliente HTTP
    global http_client
    api_base_url = args.API_BASE_URL.rstrip('/')
    facilita_token = args.FACILITA_TOKEN
    facilita_instance = args.FACILITA_INSTANCE

    # Adicionar o sufixo /api/v1/ à URL base
    api_base_url = f"{api_base_url}/api/v1"

    rate_limiter = RateLimiter(requests_per_window=RATE_LIMIT, logger=logger)
    http_client = HttpClient(base_url=api_base_url, rate_limiter=rate_limiter, logger=logger)

    # 3. Iniciar relatório
    global_start_time = ReportGenerator.init_report(logger)
    report_stats = {}

    try:
        logger.info("🚀 INICIANDO COLETA DE DADOS - APPFACILITA")
        logger.info(f"🔗 URL Base: {api_base_url}")
        logger.info(f"🏢 Instância: {facilita_instance}")
        logger.info(f"📊 Relatórios disponíveis: {', '.join(REPORTS_CONFIG.keys())}")

        # 4. Processar todos os tipos de relatório
        for report_type in REPORTS_CONFIG.keys():
            report_stats[report_type] = process_report_type(report_type, facilita_token, facilita_instance)

            stats = report_stats[report_type]
            logger.info(f"✅ {report_type}: {stats['registros']} registros "
                        f"({stats['paginas']} páginas) em {stats['tempo']:.2f}s")

        # 5. Processar funnel da Platform API (se as credenciais estiverem disponíveis)
        if args.PLATFORM_API_INSTANCE and args.PLATFORM_API_KEY and args.PLATFORM_TOKEN_USER:
            try:
                logger.info(f"\n{'=' * 60}\n🔍 PROCESSANDO FUNNEL - PLATFORM API\n{'=' * 60}")
                funnel_start = time.time()

                funnel_data = fetch_funnel_data(
                    args.PLATFORM_API_INSTANCE,
                    args.PLATFORM_API_KEY,
                    args.PLATFORM_TOKEN_USER,
                    args.PLATFORM_API_BASE_URL
                )

                # Processar e salvar dados
                logger.info(f"💾 Processando e salvando {len(funnel_data)} registros para funnel")
                processed_funnel = Utils.process_and_save_data(funnel_data, "funnel")

                funnel_duration = time.time() - funnel_start

                report_stats["funnel"] = {
                    "registros": len(processed_funnel),
                    "status": "Sucesso",
                    "tempo": funnel_duration,
                    "paginas": 1
                }

                logger.info(f"✅ funnel: {len(processed_funnel)} registros em {funnel_duration:.2f}s")

            except Exception as e:
                logger.error(f"❌ Erro ao processar funnel: {str(e)}")
                report_stats["funnel"] = {
                    "registros": 0,
                    "status": f"Falha: {type(e).__name__}: {str(e)}",
                    "tempo": 0,
                    "paginas": 0
                }
        else:
            logger.info("ℹ️  Credenciais da Platform API não fornecidas, pulando coleta do funnel")

        # 6. Gerar resumo final
        logger.info(f"\n{'=' * 60}\n📋 RESUMO FINAL DA COLETA\n{'=' * 60}")

        total_records = sum(stats['registros'] for stats in report_stats.values())
        total_pages = sum(stats['paginas'] for stats in report_stats.values())

        logger.info(f"📊 Total de registros coletados: {total_records}")
        logger.info(f"📄 Total de páginas processadas: {total_pages}")

        success = ReportGenerator.final_summary(logger, report_stats, global_start_time)

        # 7. BigQuery - processar e enviar dados
        with MemoryMonitor(logger):
            BigQuery.process_csv_files()

        tables = Utils.get_existing_folders(logger)
        for table in tables:
            BigQuery.start_pipeline(args.PROJECT_ID, args.CRM_TYPE, table_name=table,
                                    credentials_path=args.GOOGLE_APPLICATION_CREDENTIALS)

        # Verificar se houve falhas
        if not success:
            raise Exception(f"Falhas nos relatórios: {success}")

        logger.info("🎉 COLETA FINALIZADA COM SUCESSO!")

    except Exception as e:
        logger.exception(f"❌ ERRO CRÍTICO NA EXECUÇÃO: {e}")
        raise


if __name__ == "__main__":
    main()
