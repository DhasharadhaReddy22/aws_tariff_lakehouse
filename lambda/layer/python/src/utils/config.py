import os
from pathlib import Path
from dotenv import load_dotenv

import boto3

from .logger import get_logger

# Create logger – in dev: file logger | in lambda: CloudWatch
logger = get_logger(__name__, caller_file_path=__file__)

def _load_dotenv_if_dev():
    stage = os.getenv("STAGE", "dev")  # default stage
    if stage == "dev":
        try:
            PROJECT_DIR = Path(__file__).resolve().parents[5]
            load_dotenv(PROJECT_DIR / ".env")
            logger.info("Loaded .env file for development stage")
            return
        except Exception:
            logger.warning("Failed to load .env file")
            pass  # can extend this to other ways of loading env variables is needed
    pass

_load_dotenv_if_dev()

class Config:
    """
    Loads configuration based on stage.
    - dev  => read values from .env / environment variables
    - prod => read values from SSM Parameter Store
    """

    def __init__(self):
        self._stage = os.getenv("STAGE", "dev")

        # Lazy: Only create SSM client when needed
        self._ssm_client = None

        logger.info(f"Config initialized for stage: {self._stage}")

    @property # this makes it read-only, you can't config.stage = "prod"
    def stage(self) -> str:
        return self._stage
    
    def _get_ssm_client(self):
        if self._ssm_client is None:
            self._ssm_client = boto3.client("ssm")
            logger.info("Initialized SSM client")
        return self._ssm_client

    
    def get(self, name: str, default=None):
        """
        Precedence:
        1. Environment variable   (includes .env loaded values)
        2. If dev: simply return default if not found in environment
        3. If prod: fetch from SSM (path: /tariff/<stage>/<name>)
        4. Fallback to default
        """

        env_key = name.upper()

        # Picks up environment variables set directly or via .env (using _load_dotenv_if_dev)
        if env_key in os.environ:
            logger.info(f"Loaded config '{env_key}' from environment variable")
            return os.environ[env_key]

        # If dev, and not found in .env, return default
        if self.stage == "dev":
            logger.warning(f"Returned default for missing config '{env_key}' in .env file")
            return default

        # Production — read from SSM
        ssm_path = f"/tariff/{self.stage}/{name}"
        try:
            client = self._get_ssm_client()
            resp = client.get_parameter(Name=ssm_path, WithDecryption=False)
            logger.info(f"Loaded config '{env_key}' from SSM Parameter Store at '{ssm_path}'")
            return resp["Parameter"]["Value"]
        except Exception:
            logger.error(f"Failed to load config '{env_key}' from SSM at '{ssm_path}', returning default")
            return default

config = Config()