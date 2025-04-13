import logging
import os
from pathlib import Path
from typing import List

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

logger = logging.getLogger(__name__)


def load_env_file(env_paths: List[Path] = None) -> bool:
    if os.environ.get('RUNNING_IN_DOCKER') == 'true':
        logger.info("Running in Docker environment. Skipping .env file loading.")
        return False

    if not load_dotenv:
        logger.warning("python-dotenv not installed. Cannot load .env files.")
        return False

    # Default paths if none provided
    if env_paths is None:
        env_paths = [Path('.env')]

    for env_path in env_paths:
        if env_path.exists():
            logger.info(f"Loading environment variables from {env_path.absolute()}")
            load_dotenv(dotenv_path=env_path)
            return True

    logger.warning("No .env file found. Using system environment variables only.")
    return False
