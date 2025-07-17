import time
from unittest.mock import Mock, patch

import pytest

from commons.memory_monitor import MemoryMonitor


def setup_memory_mocks(mock_virtual_memory, mock_process, system_percent=60.0, process_mb=300.0,
                       system_available_gb=6.0, system_used_gb=10.0, system_total_gb=16.0):
    """Helper para configurar mocks com valores numéricos reais."""
    mock_memory = Mock()
    mock_memory.percent = system_percent
    mock_memory.available = int(system_available_gb * (1024 ** 3))
    mock_memory.used = int(system_used_gb * (1024 ** 3))
    mock_memory.total = int(system_total_gb * (1024 ** 3))
    mock_virtual_memory.return_value = mock_memory

    mock_proc = Mock()
    mock_proc_memory = Mock()
    mock_proc_memory.rss = int(process_mb * (1024 ** 2))
    mock_proc.memory_info.return_value = mock_proc_memory
    mock_proc.memory_percent.return_value = (process_mb / 1024) / system_total_gb * 100
    mock_process.return_value = mock_proc


class TestMemoryMonitorInitialization:
    """Testes para inicialização da classe MemoryMonitor."""

    def test_init_with_defaults(self):
        """Testa inicialização com valores padrão."""
        # GIVEN
        logger = Mock()

        # WHEN
        monitor = MemoryMonitor(logger)

        # THEN
        assert monitor.logger == logger
        assert monitor.check_interval == 5.0
        assert monitor.memory_threshold == 80.0
        assert monitor.monitoring is False
        assert monitor.monitor_thread is None
        assert monitor.stats['peak_usage'] == 0.0
        assert monitor.stats['alerts_count'] == 0

    def test_init_with_custom_values(self):
        """Testa inicialização com valores customizados."""
        # GIVEN
        logger = Mock()

        # WHEN
        monitor = MemoryMonitor(logger, check_interval=2.0, memory_threshold=90.0)

        # THEN
        assert monitor.check_interval == 2.0
        assert monitor.memory_threshold == 90.0
        assert monitor.stats['peak_usage'] == 0.0


class TestMemoryInfo:
    """Testes para coleta de informações de memória."""

    @patch('commons.memory_monitor.psutil.Process')
    @patch('commons.memory_monitor.psutil.virtual_memory')
    def test_get_memory_info_basic(self, mock_virtual_memory, mock_process):
        """Testa coleta básica de informações de memória."""
        # GIVEN
        logger = Mock()
        monitor = MemoryMonitor(logger)

        setup_memory_mocks(mock_virtual_memory, mock_process, system_percent=75.5, process_mb=512.0,
                           system_available_gb=4.0, system_used_gb=12.0, system_total_gb=16.0)

        # WHEN
        result = monitor.get_memory_info()

        # THEN
        assert result['system_percent'] == 75.5
        assert result['system_available_gb'] == 4.0
        assert result['system_used_gb'] == 12.0
        assert result['system_total_gb'] == 16.0
        assert result['process_mb'] == 512.0

    @patch('commons.memory_monitor.psutil.Process')
    @patch('commons.memory_monitor.psutil.virtual_memory')
    def test_log_memory_status_basic(self, mock_virtual_memory, mock_process):
        """Testa logging básico do status de memória."""
        # GIVEN
        logger = Mock()
        monitor = MemoryMonitor(logger)

        setup_memory_mocks(mock_virtual_memory, mock_process, system_percent=65.3, process_mb=256.0)

        # WHEN
        monitor.log_memory_status("🔍")

        # THEN
        logger.info.assert_called_once()
        logged_message = logger.info.call_args[0][0]
        assert "🔍 Memória" in logged_message
        assert "65.3%" in logged_message
        assert "256.0MB" in logged_message

    @patch('commons.memory_monitor.psutil.Process')
    @patch('commons.memory_monitor.psutil.virtual_memory')
    def test_log_memory_status_updates_peak(self, mock_virtual_memory, mock_process):
        """Testa se o pico de memória é atualizado corretamente."""
        # GIVEN
        logger = Mock()
        monitor = MemoryMonitor(logger)

        setup_memory_mocks(mock_virtual_memory, mock_process, system_percent=50.0, process_mb=200.0)

        # WHEN
        monitor.log_memory_status()

        # THEN
        assert monitor.stats['peak_usage'] == 50.0
        assert monitor.stats['peak_usage_mb'] == 200.0

        # WHEN
        setup_memory_mocks(mock_virtual_memory, mock_process, system_percent=85.0, process_mb=400.0)

        monitor.log_memory_status()

        # THEN - verificar pico atualizado
        assert monitor.stats['peak_usage'] == 85.0
        assert monitor.stats['peak_usage_mb'] == 400.0


class TestMemoryThreshold:
    """Testes para verificação de limites de memória."""

    @patch('commons.memory_monitor.psutil.Process')
    @patch('commons.memory_monitor.psutil.virtual_memory')
    def test_check_memory_threshold_below_limit(self, mock_virtual_memory, mock_process):
        """Testa verificação quando memória está abaixo do limite."""
        # GIVEN
        logger = Mock()
        monitor = MemoryMonitor(logger, memory_threshold=80.0)

        setup_memory_mocks(mock_virtual_memory, mock_process, system_percent=70.0, process_mb=300.0)

        # WHEN
        result = monitor.check_memory_threshold()

        # THEN
        assert result is False
        assert monitor.stats['alerts_count'] == 0
        logger.warning.assert_not_called()

    @patch('commons.memory_monitor.psutil.Process')
    @patch('commons.memory_monitor.psutil.virtual_memory')
    def test_check_memory_threshold_above_limit(self, mock_virtual_memory, mock_process):
        """Testa verificação quando memória está acima do limite."""
        # GIVEN
        logger = Mock()
        monitor = MemoryMonitor(logger, memory_threshold=80.0)

        setup_memory_mocks(mock_virtual_memory, mock_process,
                           system_percent=85.0, process_mb=500.0)

        # WHEN
        result = monitor.check_memory_threshold()

        # THEN
        assert result is True
        assert monitor.stats['alerts_count'] == 1
        logger.warning.assert_called_once()
        warning_message = logger.warning.call_args[0][0]
        assert "ALERTA DE MEMÓRIA" in warning_message
        assert "85.0%" in warning_message
        assert "500.0MB" in warning_message


class TestMonitoringLifecycle:
    """Testes para ciclo de vida do monitoramento."""

    @patch('commons.memory_monitor.psutil.Process')
    @patch('commons.memory_monitor.psutil.virtual_memory')
    def test_start_monitoring_basic(self, mock_virtual_memory, mock_process):
        """Testa início básico do monitoramento."""
        # GIVEN
        logger = Mock()
        monitor = MemoryMonitor(logger, check_interval=0.1)

        setup_memory_mocks(mock_virtual_memory, mock_process, system_percent=60.0, process_mb=300.0)

        # WHEN
        monitor.start_monitoring()

        # THEN
        assert monitor.monitoring is True
        assert monitor.monitor_thread is not None
        assert monitor.monitor_thread.is_alive()
        assert monitor.stats['start_usage'] == 60.0
        assert monitor.stats['start_usage_mb'] == 300.0

        # Cleanup
        monitor.stop_monitoring()

    def test_start_monitoring_already_active(self):
        """Testa tentativa de iniciar monitoramento já ativo."""
        # GIVEN
        logger = Mock()
        monitor = MemoryMonitor(logger)
        monitor.monitoring = True

        # WHEN
        monitor.start_monitoring()

        # THEN
        logger.warning.assert_called_once_with("⚠️  Monitor de memória já está ativo")

    @patch('commons.memory_monitor.psutil.Process')
    @patch('commons.memory_monitor.psutil.virtual_memory')
    def test_stop_monitoring_basic(self, mock_virtual_memory, mock_process):
        """Testa parada básica do monitoramento."""
        # GIVEN
        logger = Mock()
        monitor = MemoryMonitor(logger, check_interval=0.1)

        setup_memory_mocks(mock_virtual_memory, mock_process, system_percent=60.0, process_mb=300.0)

        # WHEN
        monitor.start_monitoring()
        time.sleep(0.05)
        monitor.stop_monitoring()

        # THEN
        assert monitor.monitoring is False
        logger.info.assert_any_call("⏹️  Monitoramento de memória finalizado")

    def test_stop_monitoring_not_active(self):
        """Testa parada quando monitoramento não está ativo."""
        # GIVEN
        logger = Mock()
        monitor = MemoryMonitor(logger)

        # WHEN
        monitor.stop_monitoring()

        # THEN
        assert monitor.monitoring is False

    @patch('commons.memory_monitor.psutil.Process')
    @patch('commons.memory_monitor.psutil.virtual_memory')
    def test_log_checkpoint(self, mock_virtual_memory, mock_process):
        """Testa logging de checkpoint."""
        # GIVEN
        logger = Mock()
        monitor = MemoryMonitor(logger)

        setup_memory_mocks(mock_virtual_memory, mock_process, system_percent=65.0, process_mb=256.0)

        # WHEN
        monitor.log_checkpoint("Processo ETL")

        # THEN
        logger.info.assert_called_once()
        logged_message = logger.info.call_args[0][0]
        assert "🔍 Processo ETL" in logged_message


class TestFinalSummary:
    """Testes para resumo final de memória."""

    @patch('commons.memory_monitor.psutil.Process')
    @patch('commons.memory_monitor.psutil.virtual_memory')
    def test_log_final_summary_basic(self, mock_virtual_memory, mock_process):
        """Testa resumo final básico."""
        # GIVEN
        logger = Mock()
        monitor = MemoryMonitor(logger)

        monitor.stats['start_usage'] = 50.0
        monitor.stats['start_usage_mb'] = 200.0
        monitor.stats['peak_usage'] = 75.0
        monitor.stats['peak_usage_mb'] = 400.0
        monitor.stats['alerts_count'] = 2

        setup_memory_mocks(mock_virtual_memory, mock_process, system_percent=60.0, process_mb=350.0)

        # WHEN
        monitor.log_final_summary()

        # THEN
        assert logger.info.call_count >= 5

        logged_calls = [call[0][0] for call in logger.info.call_args_list]
        summary_content = ' '.join(logged_calls)

        assert "RESUMO DE MEMÓRIA" in summary_content
        assert "50.0%" in summary_content
        assert "200.0MB" in summary_content
        assert "60.0%" in summary_content
        assert "350.0MB" in summary_content
        assert "75.0%" in summary_content
        assert "400.0MB" in summary_content
        assert "+150.0MB" in summary_content
        assert "2" in summary_content


class TestContextManager:
    """Testes para uso como context manager."""

    @patch('commons.memory_monitor.psutil.Process')
    @patch('commons.memory_monitor.psutil.virtual_memory')
    def test_context_manager_basic(self, mock_virtual_memory, mock_process):
        """Testa uso básico como context manager."""
        # GIVEN
        logger = Mock()
        monitor = MemoryMonitor(logger, check_interval=0.1)

        setup_memory_mocks(mock_virtual_memory, mock_process, system_percent=55.0, process_mb=280.0)

        # WHEN
        with monitor as m:
            # THEN
            assert m.monitoring is True
            assert m is monitor
            time.sleep(0.05)

        # THEN
        assert monitor.monitoring is False

    @patch('commons.memory_monitor.psutil.Process')
    @patch('commons.memory_monitor.psutil.virtual_memory')
    def test_context_manager_with_exception(self, mock_virtual_memory, mock_process):
        """Testa context manager quando ocorre exceção."""
        # GIVEN
        logger = Mock()
        monitor = MemoryMonitor(logger, check_interval=0.1)

        setup_memory_mocks(mock_virtual_memory, mock_process, system_percent=55.0, process_mb=280.0)

        # WHEN & THEN
        with pytest.raises(ValueError):
            with monitor:
                assert monitor.monitoring is True
                raise ValueError("Test exception")

        assert monitor.monitoring is False


class TestErrorHandling:
    """Testes para tratamento de erros."""

    @patch('commons.memory_monitor.psutil.Process')
    @patch('commons.memory_monitor.psutil.virtual_memory')
    def test_monitor_loop_error_handling(self, mock_virtual_memory, mock_process):
        """Testa tratamento de erro no loop de monitoramento."""
        # GIVEN
        logger = Mock()
        monitor = MemoryMonitor(logger, check_interval=0.1)

        setup_memory_mocks(mock_virtual_memory, mock_process, system_percent=60.0, process_mb=300.0)

        # WHEN
        monitor.start_monitoring()
        time.sleep(0.05)

        mock_virtual_memory.side_effect = Exception("Test error")
        time.sleep(0.15)

        mock_virtual_memory.side_effect = None
        setup_memory_mocks(mock_virtual_memory, mock_process, system_percent=60.0, process_mb=300.0)

        monitor.stop_monitoring()

        # THEN
        error_logged = any("Erro no monitoramento de memória" in str(call)
                           for call in logger.error.call_args_list)
        assert error_logged


class TestEdgeCases:
    """Testes para casos extremos."""

    def test_memory_monitor_thread_safety(self):
        """Testa se múltiplas chamadas são thread-safe."""
        # GIVEN
        logger = Mock()
        monitor = MemoryMonitor(logger)

        # WHEN
        monitor.start_monitoring()
        monitor.start_monitoring()
        monitor.stop_monitoring()
        monitor.stop_monitoring()

        # THEN
        assert monitor.monitoring is False

    @patch('commons.memory_monitor.psutil.Process')
    @patch('commons.memory_monitor.psutil.virtual_memory')
    def test_peak_memory_not_updated_when_lower(self, mock_virtual_memory, mock_process):
        """Testa que pico não é atualizado quando valor é menor."""
        # GIVEN
        logger = Mock()
        monitor = MemoryMonitor(logger)
        monitor.stats['peak_usage'] = 80.0

        setup_memory_mocks(mock_virtual_memory, mock_process, system_percent=70.0, process_mb=300.0)

        # WHEN
        monitor.log_memory_status()

        # THEN
        assert monitor.stats['peak_usage'] == 80.0
