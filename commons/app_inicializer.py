import logging
import sys

from commons.validate_env import load_env_file


class AppInitializer:
    @staticmethod
    def setup_logging(logger_name=__name__, level=logging.INFO):
        """
        Configura o logger da aplicação com formato padrão e saída para stdout.
        """
        logging.basicConfig(
            level=level,
            format='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(sys.stdout)
            ]
        )
        return logging.getLogger(logger_name)

    @staticmethod
    def load_environment():
        """
        Carrega as variáveis de ambiente a partir do arquivo .env
        """
        load_env_file()

    @classmethod
    def initialize(cls, logger_name=__name__, level=logging.INFO):
        """
        Inicializa a aplicação configurando o logger e carregando variáveis de ambiente.
        """
        logger = cls.setup_logging(logger_name, level)
        cls.load_environment()
        return logger
