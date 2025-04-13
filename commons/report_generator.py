import time
from datetime import datetime


class ReportGenerator:
    @staticmethod
    def init_report(logger, report_name="COLETA DE DADOS API"):
        """
        Inicia um novo relatório de execução.
        """
        global_start_time = time.time()

        logger.info(f"{'#' * 100}")
        logger.info(f"🚀 INICIANDO {report_name} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"{'#' * 100}")

        return global_start_time

    @staticmethod
    def final_summary(logger, endpoint_stats, global_start_time):
        """
        Gera o resumo final da execução dos endpoints.
        """
        total_time = time.time() - global_start_time
        minutes, seconds = divmod(int(total_time), 60)

        # Verificar se houve alguma falha
        falhas = [name for name, stats in endpoint_stats.items() if "Falha" in stats["status"]]

        # Formatar em tabela para facilitar visualização no Airflow
        logger.info(f"{'#' * 100}")
        logger.info(f"📋 RESUMO DA EXECUÇÃO - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"{'#' * 100}")
        logger.info(f"⏱️ Tempo total de execução: {minutes} minutos e {seconds} segundos")
        logger.info(f"🚦 Status geral: {'❌ Com falhas' if falhas else '✅ Sucesso'}")

        # Formatar como tabela
        logger.info(f"{'-' * 100}")
        logger.info(f"| {'ENDPOINT':<25} | {'STATUS':<30} | {'REGISTROS':<10} | {'TEMPO (s)':<10} |")
        logger.info(f"|{'-' * 27}|{'-' * 32}|{'-' * 12}|{'-' * 12}|")

        for endpoint_name, stats in endpoint_stats.items():
            status_icon = "✅" if "Sucesso" in stats["status"] else "❌"
            status_text = stats["status"]
            if len(status_text) > 28:
                status_text = status_text[:25] + "..."

            logger.info(
                f"| {endpoint_name:<25} | {status_icon} {status_text:<28} | {stats['registros']:<10} | {stats.get('tempo', 0):<10.2f} |")

        logger.info(f"{'-' * 100}")
        logger.info(f"{'#' * 100}\n")

        # Se houver falhas, retornar False e logar erro
        if falhas:
            falhas_str = ", ".join(falhas)
            logger.error(f"Falhas nos endpoints: {falhas_str}")
            return False

        logger.info("✅ Processamento concluído com sucesso")
        return True
