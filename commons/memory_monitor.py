import threading
import time
from typing import Dict, Optional

import psutil


class MemoryMonitor:
    def __init__(self, logger, check_interval: float = 5.0, memory_threshold: float = 80.0):
        self.logger = logger
        self.check_interval = check_interval
        self.memory_threshold = memory_threshold
        self.monitoring = False
        self.monitor_thread: Optional[threading.Thread] = None
        self.stats = {
            'peak_usage': 0.0,
            'peak_usage_mb': 0.0,
            'alerts_count': 0,
            'start_usage': 0.0,
            'start_usage_mb': 0.0
        }

    def get_memory_info(self) -> Dict[str, float]:
        """Retorna informações atuais de memória."""
        memory = psutil.virtual_memory()
        process = psutil.Process()
        process_memory = process.memory_info()

        return {
            'system_percent': memory.percent,
            'system_available_gb': memory.available / (1024 ** 3),
            'system_used_gb': memory.used / (1024 ** 3),
            'system_total_gb': memory.total / (1024 ** 3),
            'process_mb': process_memory.rss / (1024 ** 2),
            'process_percent': process.memory_percent()
        }

    def log_memory_status(self, prefix: str = "🧠"):
        """Registra o status atual da memória no logger."""
        info = self.get_memory_info()

        self.logger.info(
            f"{prefix} Memória - Sistema: {info['system_percent']:.1f}% "
            f"({info['system_used_gb']:.1f}GB/{info['system_total_gb']:.1f}GB) | "
            f"Processo: {info['process_mb']:.1f}MB ({info['process_percent']:.1f}%)"
        )

        # Atualizar estatísticas de pico
        if info['system_percent'] > self.stats['peak_usage']:
            self.stats['peak_usage'] = info['system_percent']
            self.stats['peak_usage_mb'] = info['process_mb']

    def check_memory_threshold(self):
        """Verifica se o uso de memória ultrapassou o limite e registra alerta."""
        info = self.get_memory_info()

        if info['system_percent'] > self.memory_threshold:
            self.stats['alerts_count'] += 1
            self.logger.warning(
                f"⚠️  ALERTA DE MEMÓRIA: {info['system_percent']:.1f}% "
                f"(limite: {self.memory_threshold}%) - "
                f"Processo usando {info['process_mb']:.1f}MB"
            )
            return True
        return False

    def _monitor_loop(self):
        """Loop de monitoramento executado em thread separada."""
        while self.monitoring:
            try:
                self.check_memory_threshold()
                time.sleep(self.check_interval)
            except Exception as e:
                self.logger.error(f"❌ Erro no monitoramento de memória: {e}")
                time.sleep(self.check_interval)

    def start_monitoring(self):
        """Inicia o monitoramento contínuo de memória."""
        if self.monitoring:
            self.logger.warning("⚠️  Monitor de memória já está ativo")
            return

        # Registrar estado inicial
        initial_info = self.get_memory_info()
        self.stats['start_usage'] = initial_info['system_percent']
        self.stats['start_usage_mb'] = initial_info['process_mb']

        self.logger.info(f"🚀 Iniciando monitoramento de memória (intervalo: {self.check_interval}s)")
        self.log_memory_status("📊")

        self.monitoring = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()

    def stop_monitoring(self):
        """Para o monitoramento de memória."""
        if not self.monitoring:
            return

        self.monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=2)

        self.logger.info("⏹️  Monitoramento de memória finalizado")
        self.log_final_summary()

    def log_final_summary(self):
        """Registra resumo final do uso de memória."""
        current_info = self.get_memory_info()

        memory_growth = current_info['process_mb'] - self.stats['start_usage_mb']

        self.logger.info("📈 RESUMO DE MEMÓRIA:")
        self.logger.info(f"   • Uso inicial: {self.stats['start_usage']:.1f}% ({self.stats['start_usage_mb']:.1f}MB)")
        self.logger.info(f"   • Uso final: {current_info['system_percent']:.1f}% ({current_info['process_mb']:.1f}MB)")
        self.logger.info(f"   • Pico de uso: {self.stats['peak_usage']:.1f}% ({self.stats['peak_usage_mb']:.1f}MB)")
        self.logger.info(f"   • Crescimento do processo: {memory_growth:+.1f}MB")
        self.logger.info(f"   • Alertas de memória: {self.stats['alerts_count']}\n")

    def log_checkpoint(self, checkpoint_name: str):
        """Registra um checkpoint de memória durante o processamento."""
        self.log_memory_status(f"🔍 {checkpoint_name}")

    def __enter__(self):
        """Context manager - inicia monitoramento."""
        self.start_monitoring()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager - para monitoramento."""
        self.stop_monitoring()
