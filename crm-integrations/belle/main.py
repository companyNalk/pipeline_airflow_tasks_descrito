import time
from datetime import datetime

from dateutil.relativedelta import relativedelta

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
    "base_url": "https://app.bellesoftware.com.br/api/release/controller/IntegracaoExterna/v1.0",
    "endpoints": {
        "agendamentos": {
            "path": "agendamentos",
            "use_date_range": True,
            "start_date": "01/01/2024"
        },
        "agendamentos_finalizados": {
            "path": "agendamentos/finalizados",
            "use_date_range": True,
            "start_date": "01/01/2024"
        },
        "clientes": {
            "path": "clientes",
            "use_date_range": False
        },
        "servicos": {
            "path": "servico/listar",
            "use_date_range": False
        },
        "contas_receber": {
            "path": "contas_receber",
            "use_date_range": True,
            "start_date": "01/01/2025",
            "max_months": 3,
            "param_name": "estab"
        },
        "vendas": {
            "path": "vendas",
            "use_date_range": True,
            "start_date": "01/01/2025",
            "max_days": 2
        }
    }
}


def get_arguments():
    """Configura e retorna os argumentos da linha de comando."""
    return (ArgumentManager("Script para coletar e processar dados da API Belle Software")
            .add("API_AUTH_TOKEN", "Token de autenticação da API", required=True)
            .add("API_COD_ESTAB_LIST", "Lista de códigos de estabelecimento separados por vírgula (ex: 1,2,3,5,8)", required=True)
            .add("PROJECT_ID", "ID do projeto Google Cloud", required=True)
            .add("CRM_TYPE", "Nome da ferramenta", required=True)
            .add("GOOGLE_APPLICATION_CREDENTIALS", "Credencial GCS", required=True)
            .parse())


def get_current_date():
    """Retorna a data atual no formato DD/MM/YYYY."""
    return datetime.now().strftime("%d/%m/%Y")


def get_date_ranges(start_date_str, max_months=None, max_days=None):
    """
    Gera intervalos de datas considerando limite máximo de meses ou dias.
    Para contas_receber, divide em períodos de 3 meses.
    Para vendas, divide em períodos de 5 dias.
    """
    start_date = datetime.strptime(start_date_str, "%d/%m/%Y")
    end_date = datetime.now()

    if max_days:
        # Divide em períodos de dias (para vendas)
        date_ranges = []
        current_start = start_date

        while current_start < end_date:
            current_end = current_start + relativedelta(days=max_days)
            if current_end > end_date:
                current_end = end_date

            date_ranges.append((
                current_start.strftime("%d/%m/%Y"),
                current_end.strftime("%d/%m/%Y")
            ))

            current_start = current_end + relativedelta(days=1)

        return date_ranges

    elif max_months:
        # Divide em períodos de meses (para contas_receber)
        date_ranges = []
        current_start = start_date

        while current_start < end_date:
            current_end = current_start + relativedelta(months=max_months)
            if current_end > end_date:
                current_end = end_date

            date_ranges.append((
                current_start.strftime("%d/%m/%Y"),
                current_end.strftime("%d/%m/%Y")
            ))

            current_start = current_end + relativedelta(days=1)

        return date_ranges

    else:
        # Sem limite, retorna período único
        return [(start_date_str, get_current_date())]


def fetch_endpoint_data(http_client, endpoint_config, cod_estab, token, dt_inicio=None, dt_fim=None):
    """Busca dados de um endpoint específico."""
    headers = {"Authorization": token}

    # Parâmetros base
    params = {}

    # Adiciona código do estabelecimento
    if endpoint_config.get("param_name") == "estab":
        params["estab"] = cod_estab
    else:
        params["codEstab"] = cod_estab

    # Adiciona datas se necessário
    if endpoint_config.get("use_date_range") and dt_inicio and dt_fim:
        params["dtInicio"] = dt_inicio
        params["dtFim"] = dt_fim

    try:
        data = http_client.get(endpoint_config["path"], headers=headers, params=params)

        # Considera diferentes estruturas de resposta
        if isinstance(data, list):
            return data
        elif isinstance(data, dict):
            return data.get('data', data.get('items', [data]))
        else:
            return []

    except Exception as e:
        logger.error(f"❌ Erro ao buscar dados do endpoint {endpoint_config['path']}: {str(e)}")
        raise


def process_endpoint(endpoint_name, endpoint_config, cod_estab, token):
    """Processa um endpoint específico para um estabelecimento e retorna estatísticas."""
    # Nome do arquivo/tabela será cod_estab_endpoint
    table_name = f"cod_estab_{cod_estab}_{endpoint_name}"

    logger.info(f"\n{'=' * 50}\n🔍 PROCESSANDO: {table_name.upper()}\n{'=' * 50}")

    try:
        # Cliente HTTP
        rate_limiter = RateLimiter(requests_per_window=CONFIG["rate_limit"], logger=logger)
        http_client = HttpClient(base_url=CONFIG["base_url"], rate_limiter=rate_limiter, logger=logger)

        start_time = time.time()
        all_data = []

        if endpoint_config.get("use_date_range"):
            # Endpoints que usam intervalo de datas
            start_date = endpoint_config["start_date"]
            max_months = endpoint_config.get("max_months")
            max_days = endpoint_config.get("max_days")

            date_ranges = get_date_ranges(start_date, max_months, max_days)

            logger.info(f"📅 Coletando dados em {len(date_ranges)} período(s)")

            for i, (dt_inicio, dt_fim) in enumerate(date_ranges, 1):
                logger.info(f"📊 Período {i}/{len(date_ranges)}: {dt_inicio} até {dt_fim}")

                period_data = fetch_endpoint_data(
                    http_client, endpoint_config, cod_estab,
                    token, dt_inicio, dt_fim
                )

                all_data.extend(period_data)
                logger.info(f"✅ Coletados {len(period_data)} registros do período")

                if i < len(date_ranges):
                    time.sleep(0.5)  # Pausa entre períodos
        else:
            # Endpoints simples sem data
            logger.info(f"📊 Coletando dados do endpoint {endpoint_name}")
            all_data = fetch_endpoint_data(
                http_client, endpoint_config, cod_estab, token
            )

        # Processar e salvar dados usando o nome concatenado
        logger.info(f"💾 Processando e salvando {len(all_data)} registros para {table_name}")
        processed_data = Utils.process_and_save_data(all_data, table_name)

        return {
            "registros": len(processed_data),
            "status": "Sucesso",
            "tempo": time.time() - start_time,
            "table_name": table_name
        }

    except Exception as e:
        logger.exception(f"❌ Falha no endpoint {table_name}")
        return {"registros": 0, "status": f"Falha: {type(e).__name__}: {str(e)}", "tempo": 0, "table_name": table_name}


def validate_cod_estab_list(cod_estab_list_str):
    """Valida e converte a lista de códigos de estabelecimento."""
    valid_codes = ["1", "2", "3", "4", "5", "8"]

    # Remove espaços e divide por vírgula
    cod_estab_list = [code.strip() for code in cod_estab_list_str.split(',')]

    # Validar cada código
    for code in cod_estab_list:
        if code not in valid_codes:
            raise ValueError(f"COD_ESTAB '{code}' inválido. Valores válidos: {', '.join(valid_codes)}")

    return cod_estab_list


def main():
    """Função principal para coleta de dados."""
    args = get_arguments()

    # Validar e obter lista de códigos de estabelecimento
    cod_estab_list = validate_cod_estab_list(args.API_COD_ESTAB_LIST)

    global_start_time = ReportGenerator.init_report(logger)
    all_endpoint_stats = {}
    all_table_names = []

    logger.info(f"🏢 Iniciando coleta para estabelecimentos: {', '.join(cod_estab_list)}")

    try:
        # Processar cada estabelecimento
        for cod_estab in cod_estab_list:
            logger.info(f"\n{'=' * 60}\n🏬 ESTABELECIMENTO: {cod_estab}\n{'=' * 60}")

            estab_stats = {}

            # Processar cada endpoint para o estabelecimento atual
            for endpoint_name, endpoint_config in CONFIG["endpoints"].items():
                stats = process_endpoint(endpoint_name, endpoint_config, cod_estab, args.API_AUTH_TOKEN)

                # Usar o table_name como chave para estatísticas
                table_name = stats["table_name"]
                estab_stats[table_name] = stats
                all_endpoint_stats[table_name] = stats
                all_table_names.append(table_name)

                logger.info(
                    f"✅ {table_name}: {stats['registros']} registros em {stats['tempo']:.2f}s"
                )

            # Log resumo do estabelecimento
            total_records = sum(stat['registros'] for stat in estab_stats.values())
            logger.info(f"📊 Estabelecimento {cod_estab}: {total_records} registros totais")

        # Relatório final consolidado
        if not ReportGenerator.final_summary(logger, all_endpoint_stats, global_start_time):
            raise Exception("Falhas encontradas na execução")

        # Processar arquivos CSV
        with MemoryMonitor(logger):
            BigQuery.process_csv_files()

        tables = Utils.get_existing_folders(logger)
        for table in tables:
            BigQuery.start_pipeline(args.PROJECT_ID, args.CRM_TYPE, table_name=table,
                                    credentials_path=args.GOOGLE_APPLICATION_CREDENTIALS)

        logger.info(f"🎉 Integração concluída com sucesso! {len(all_table_names)} tabelas processadas.")

    except Exception as e:
        logger.exception(f"❌ ERRO CRÍTICO: {e}")
        raise


if __name__ == "__main__":
    main()
