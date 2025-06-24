import logging
import os
import sys

from commons.utils import Utils
from commons.validate_env import load_env_file


class AppInitializer:
    @classmethod
    def _setup_logging(cls, logger_name=__name__, level=logging.INFO):
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

    @classmethod
    def _load_environment(cls):
        """
        Carrega as variáveis de ambiente a partir do arquivo .env
        """
        load_env_file()

    @classmethod
    def initialize(cls, logger_name=__name__, level=logging.INFO):
        """
        Inicializa a aplicação configurando o logger e carregando variáveis de ambiente.
        """
        # Ampla permissão
        os.umask(0)

        logger = cls._setup_logging(logger_name, level)
        Utils.clean_output_folder(logger)

        cls._load_environment()
        return logger
