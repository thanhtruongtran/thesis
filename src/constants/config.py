import os
from textwrap import dedent

import dspy
from dotenv import load_dotenv

load_dotenv()


class PostgresDBETLConfig:
    TRANSFER_EVENT_TABLE = os.environ.get(
        "POSTGRES_ETL_TRANSFER_EVENT_TABLE", "transfer_event"
    )
    CONNECTION_URL = os.environ.get(
        "POSTGRES_ETL_CONNECTION_URL",
        "postgresql://user:password@localhost:5432/database",
    )


class PostgreRAGConfig:
    TRANSFER_EVENT_TABLE = os.environ.get(
        "POSTGRES_RAG_TRANSFER_EVENT_TABLE", "transfer_event"
    )
    CONNECTION_URL = os.environ.get(
        "POSTGRES_RAG_CONNECTION_URL",
        "postgresql://user:password@localhost:5432/database",
    )


class MongoDBETLConfig:
    CONNECTION_URL = os.getenv("MONGODB_ETL_CONNECTION_URL")
    DATABASE = os.getenv("MONGODB_ETL_DATABASE")


class MongoDBKLGConfig:
    CONNECTION_URL = os.getenv("MONGODB_KLG_CONNECTION_URL")
    DATABASE = os.getenv("MONGODB_KLG_DATABASE", "knowledge_graph")


class MongoDBCDPConfig:
    CONNECTION_URL = os.getenv("MONGODB_CDP_CONNECTION_URL")
    DATABASE = os.getenv("MONGODB_CDP_DATABASE")


class MongoDBCenticConfig:
    CONNECTION_URL = os.getenv("MONGODB_CENTIC_CONNECTION_URL")
    DATABASE = os.getenv("MONGODB_CENTIC_DATABASE")


class MongoDBExtraConfig:
    CONNECTION_URL = os.getenv("MONGODB_EXTRA_CONNECTION_URL")
    DATABASE = os.getenv("MONGODB_EXTRA_DATABASE")


class MongoDBNewsConfig:
    CONNECTION_URL = os.getenv("MONGODB_NEWS_CONNECTION_URL")
    DATABASE = os.getenv("MONGODB_NEWS_DATABASE")


class MongoDBCommunityConfig:
    CONNECTION_URL = os.getenv("MONGODB_COMMUNITY_CONNECTION_URL")
    DATABASE = os.getenv("MONGODB_COMMUNITY_DATABASE")


class MongoDBCacheConfig:
    CONNECTION_URL = os.getenv("MONGODB_CACHE_CONNECTION_URL")
    DATABASE = os.getenv("MONGODB_CACHE_DATABASE")


class MongoDBTraderConfig:
    CONNECTION_URL = os.getenv("MONGODB_TRADER_CONNECTION_URL")
    DATABASE = os.getenv("MONGODB_TRADER_DATABASE")


class NetworkConfig:
    chain_id_to_db_prefix = {
        '0x38': '',
        '0x1': 'ethereum_'
    }


class EmbeddingModelConfig:
    MODEL_NAME = os.getenv("EMBEDDING_MODEL")
    EMBEDDING_DIMENSION = os.getenv("EMBEDDING_DIMENSION")


class LLMModelConfig:
    MODEL_NAME = os.getenv("LARGE_LANGUAGE_MODEL")
    MODEL_API = os.getenv("TOGETHER_AI_API")
    MODEL_KWARGS_TWITTER = {"temperature": 0.75, "max_length": 65, "top_p": 1}
    MODEL_KWARGS_TELEGRAM = {"temperature": 0.75, "max_length": 250, "top_p": 1}


class LLMModelConfigOpenAI:
    MODEL_NAME = os.getenv("LARGE_LANGUAGE_MODEL_OPENAI")
    MODEL_API = os.getenv("OPENAI_API")
    AZURE_ENDPOINT = os.getenv("AZURE_ENDPOINT")
    API_VERSION = os.getenv("API_VERSION")
    DEPLOYMENT_NAME = os.getenv("DEPLOYMENT_NAME")
    MODEL_KWARGS_TWITTER = {"temperature": 0.75, "max_length": 65, "top_p": 1}
    MODEL_KWARGS_TELEGRAM = {"temperature": 0.75, "max_length": 250, "top_p": 1}


class CenticApiKey:
    APIKEY = os.getenv("CENTIC_API")
    JWT = os.getenv("JWT")


class AzureOpenAIService:
    def __init__(self, model="gpt-4o", max_tokens=1000):
        self.model = model
        self.api_key = os.getenv("OPENAI_API")
        self.api_version = os.getenv("API_VERSION")
        self.api_base = os.getenv("AZURE_ENDPOINT")
        self.max_tokens = max_tokens
        self.turbo = self.initialize_turbo()

    def initialize_turbo(self):
        """Initialize the Azure OpenAI model and configure dspy settings."""
        turbo_instance = dspy.AzureOpenAI(
            model=self.model,
            api_key=self.api_key,
            api_version=self.api_version,
            api_base=self.api_base,
            max_tokens=self.max_tokens,
        )
        dspy.settings.configure(lm=turbo_instance)
        return turbo_instance


class Config:
    RUN_SETTING = {
        "host": os.environ.get("SERVER_HOST", "localhost"),
        "port": int(os.environ.get("SERVER_PORT", 8080)),
        "debug": os.getenv("DEBUG", False),
        "access_log": False,
        "auto_reload": True,
        "workers": int(os.getenv("SERVER_WORKERS", 4)),
    }

    SECRET = os.environ.get("SECRET_KEY", "example project")
    JWT_PASSWORD = os.getenv("JWT_PASSWORD", "dev123")
    EXPIRATION_JWT = 2592000  # 1 month
    RESPONSE_TIMEOUT = 30  # seconds
    REQUEST_TIMEOUT = 5

    SERVER_NAME = os.getenv("SERVER_NAME")

    # To reorder swagger tags
    raw = {}
    if SERVER_NAME:
        raw["servers"] = [{"url": SERVER_NAME}]

    FALLBACK_ERROR_FORMAT = "json"

    OAS_UI_DEFAULT = "swagger"
    SWAGGER_UI_CONFIGURATION = {
        "apisSorter": "alpha",
        "docExpansion": "list",
        "operationsSorter": "alpha",
    }

    API_HOST = os.getenv("API_HOST", "0.0.0.0:8096")
    API_BASEPATH = os.getenv("API_BASEPATH", "")
    API_SCHEMES = os.getenv("API_SCHEMES", "http")
    SWAGGER_API_VERSION = os.getenv("SWAGGER_API_VERSION", "0.1.0")
    API_TITLE = os.getenv("API_TITLE", "Centic Campaign")
    API_CONTACT_EMAIL = os.getenv("API_CONTACT_EMAIL", "example@gmail.com")

    API_DESCRIPTION = os.getenv(
        "API_DESCRIPTION",
        dedent(
            """
        ## Explore the Centic Campaign API
        """
        ),
    )
