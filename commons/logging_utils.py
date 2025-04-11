import logging


def setup_logging(level=logging.INFO):
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    # Se já tiver handlers configurados (em caso de reload), não reconfigura
    if not logging.getLogger().handlers:
        console = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            "%Y-%m-%d %H:%M:%S"
        )
        console.setFormatter(formatter)
        logging.getLogger().addHandler(console)
