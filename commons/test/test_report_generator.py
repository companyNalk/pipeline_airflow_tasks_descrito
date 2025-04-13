import time
from datetime import datetime
from unittest.mock import MagicMock, patch

from commons.report_generator import ReportGenerator


class TestInitReport:
    """Testes para o método init_report da classe ReportGenerator."""

    def test_returns_start_time(self):
        """Verifica se init_report retorna o tempo de início corretamente."""
        # GIVEN
        logger = MagicMock()
        mock_time = 1234567890.0

        # WHEN
        with patch('time.time', return_value=mock_time):
            result = ReportGenerator.init_report(logger)

        # THEN
        assert result == mock_time

    def test_default_report_name(self):
        """Verifica se init_report usa o nome de relatório padrão quando não especificado."""
        # GIVEN
        logger = MagicMock()
        default_name = "COLETA DE DADOS API"

        # WHEN
        with patch('time.time'):
            ReportGenerator.init_report(logger)

        # THEN
        # Verifica que o nome padrão foi usado
        report_calls = [call for call in logger.info.call_args_list if "INICIANDO" in str(call)]
        assert len(report_calls) == 1
        assert default_name in report_calls[0].args[0]

    def test_custom_report_name(self):
        """Verifica se init_report usa o nome de relatório personalizado quando especificado."""
        # GIVEN
        logger = MagicMock()
        custom_name = "RELATÓRIO PERSONALIZADO"

        # WHEN
        with patch('time.time'):
            ReportGenerator.init_report(logger, custom_name)

        # THEN
        # Verifica que o nome personalizado foi usado
        report_calls = [call for call in logger.info.call_args_list if "INICIANDO" in str(call)]
        assert len(report_calls) == 1
        assert custom_name in report_calls[0].args[0]

    def test_logs_correct_messages(self):
        """Verifica se init_report registra as mensagens de log corretas."""
        # GIVEN
        logger = MagicMock()
        report_name = "TESTE RELATÓRIO"
        mock_datetime = datetime(2023, 1, 1, 12, 0, 0)

        # WHEN
        with patch('commons.report_generator.datetime') as mock_dt:
            mock_dt.now.return_value = mock_datetime
            ReportGenerator.init_report(logger, report_name)

        # THEN
        assert logger.info.call_count == 3
        logger.info.assert_any_call("#" * 100)
        logger.info.assert_any_call(f"🚀 INICIANDO {report_name} - 2023-01-01 12:00:00")


class TestFinalSummary:
    """Testes para o método final_summary da classe ReportGenerator."""

    def test_success_scenario(self):
        """Verifica o cenário de sucesso do resumo final."""
        # GIVEN
        logger = MagicMock()
        endpoint_stats = {
            "endpoint1": {"status": "Sucesso", "registros": 100, "tempo": 5.5},
            "endpoint2": {"status": "Sucesso - Processados", "registros": 200, "tempo": 3.2}
        }
        global_start_time = time.time() - 125  # 2 minutos e 5 segundos atrás

        # WHEN
        result = ReportGenerator.final_summary(logger, endpoint_stats, global_start_time)

        # THEN
        assert result is True
        assert logger.error.call_count == 0
        assert "✅ Processamento concluído com sucesso" in [call.args[0] for call in logger.info.call_args_list]
        assert "🚦 Status geral: ✅ Sucesso" in [call.args[0] for call in logger.info.call_args_list]

    def test_single_failure_scenario(self):
        """Verifica o cenário com uma única falha no resumo final."""
        # GIVEN
        logger = MagicMock()
        endpoint_stats = {
            "endpoint1": {"status": "Sucesso", "registros": 100, "tempo": 5.5},
            "endpoint2": {"status": "Falha - Erro na API", "registros": 0, "tempo": 1.2}
        }
        global_start_time = time.time() - 65  # 1 minuto e 5 segundos atrás

        # WHEN
        result = ReportGenerator.final_summary(logger, endpoint_stats, global_start_time)

        # THEN
        assert result is False
        logger.error.assert_called_once_with("Falhas nos endpoints: endpoint2")
        assert "🚦 Status geral: ❌ Com falhas" in [call.args[0] for call in logger.info.call_args_list]

    def test_multiple_failures_scenario(self):
        """Verifica o cenário com múltiplas falhas no resumo final."""
        # GIVEN
        logger = MagicMock()
        endpoint_stats = {
            "endpoint1": {"status": "Sucesso", "registros": 100, "tempo": 5.5},
            "endpoint2": {"status": "Falha - Timeout", "registros": 0, "tempo": 1.2},
            "endpoint3": {"status": "Falha - Erro 500", "registros": 0, "tempo": 2.1}
        }
        global_start_time = time.time() - 65

        # WHEN
        result = ReportGenerator.final_summary(logger, endpoint_stats, global_start_time)

        # THEN
        assert result is False
        # Verifica se ambos os endpoints com falha são listados na mensagem de erro
        logger.error.assert_called_once()
        error_msg = logger.error.call_args[0][0]
        assert "endpoint2" in error_msg
        assert "endpoint3" in error_msg
        assert ", " in error_msg  # Verifica que os nomes estão separados por vírgula e espaço

    def test_formats_table_correctly(self):
        """Verifica se a tabela de resumo é formatada corretamente."""
        # GIVEN
        logger = MagicMock()
        endpoint_stats = {
            "endpoint_curto": {"status": "Sucesso", "registros": 100, "tempo": 5.5},
            "endpoint_com_nome_muito_longo": {"status": "Falha - Mensagem de erro muito grande que deve ser truncada",
                                              "registros": 0, "tempo": 1.2}
        }
        global_start_time = time.time() - 65

        # WHEN
        with patch('commons.report_generator.datetime') as mock_dt:
            mock_dt.now.return_value = datetime(2023, 1, 1, 12, 0, 0)
            ReportGenerator.final_summary(logger, endpoint_stats, global_start_time)

        # THEN
        # Verifica cabeçalho
        header_call_args = [call.args[0] for call in logger.info.call_args_list]
        assert any(
            "| ENDPOINT                  | STATUS                         | REGISTROS  | TEMPO (s)  |" in arg for arg in
            header_call_args)

        # Verifica truncamento
        table_rows = [call.args[0] for call in logger.info.call_args_list if
                      "endpoint_com_nome_muito_longo" in call.args[0]]
        assert len(table_rows) == 1
        assert "..." in table_rows[0]

    def test_calculates_time_correctly(self):
        """Verifica se o tempo total é calculado e formatado corretamente."""
        # GIVEN
        logger = MagicMock()
        endpoint_stats = {"endpoint1": {"status": "Sucesso", "registros": 100, "tempo": 5.5}}
        global_start_time = time.time() - 125  # 2 minutos e 5 segundos

        # WHEN
        ReportGenerator.final_summary(logger, endpoint_stats, global_start_time)

        # THEN
        time_logs = [call.args[0] for call in logger.info.call_args_list if "Tempo total de execução" in call.args[0]]
        assert len(time_logs) == 1
        assert "2 minutos e 5 segundos" in time_logs[0]

    def test_formats_datetime_correctly(self):
        """Verifica se a data e hora no resumo final são formatadas corretamente."""
        # GIVEN
        logger = MagicMock()
        endpoint_stats = {"endpoint1": {"status": "Sucesso", "registros": 100, "tempo": 5.5}}
        global_start_time = time.time() - 65
        mock_datetime = datetime(2023, 1, 1, 12, 0, 0)

        # WHEN
        with patch('commons.report_generator.datetime') as mock_dt:
            mock_dt.now.return_value = mock_datetime
            ReportGenerator.final_summary(logger, endpoint_stats, global_start_time)

        # THEN
        datetime_logs = [call.args[0] for call in logger.info.call_args_list if "RESUMO DA EXECUÇÃO" in call.args[0]]
        assert len(datetime_logs) == 1
        assert "2023-01-01 12:00:00" in datetime_logs[0]

    def test_handles_missing_tempo(self):
        """Verifica se o resumo final lida corretamente com stats que não têm o campo 'tempo'."""
        # GIVEN
        logger = MagicMock()
        endpoint_stats = {
            "endpoint1": {"status": "Sucesso", "registros": 100}  # Sem campo 'tempo'
        }
        global_start_time = time.time() - 65

        # WHEN
        result = ReportGenerator.final_summary(logger, endpoint_stats, global_start_time)

        # THEN
        assert result is True
        # Verifica se a linha da tabela contém o endpoint1 e usa 0 para o tempo
        table_rows = [call.args[0] for call in logger.info.call_args_list if "endpoint1" in call.args[0]]
        assert len(table_rows) == 1
        assert "0.00" in table_rows[0]

    def test_handles_empty_endpoint_stats(self):
        """Verifica se o resumo final lida corretamente com um dicionário vazio de estatísticas."""
        # GIVEN
        logger = MagicMock()
        endpoint_stats = {}  # Dicionário vazio
        global_start_time = time.time() - 65

        # WHEN
        result = ReportGenerator.final_summary(logger, endpoint_stats, global_start_time)

        # THEN
        assert result is True  # Não deve ter falhas se não houver endpoints
        assert "🚦 Status geral: ✅ Sucesso" in [call.args[0] for call in logger.info.call_args_list]
