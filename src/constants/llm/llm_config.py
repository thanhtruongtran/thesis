from typing import Dict, Optional

from openai import AzureOpenAI

from src.constants.config import LLMModelConfigOpenAI
from src.utils.logger import get_logger

logger = get_logger(__name__)


class BaseLLM:
    """Base class for Large Language Models with common functionality"""

    def __init__(
        self,
        model_name: Optional[str] = None,
        model_kwargs: Optional[Dict] = None,
    ):
        self.model_name = model_name
        self.model_kwargs = model_kwargs
        self._model = None

    def _validate_params(self) -> bool:
        """Validate that either both or neither of model_name and model_kwargs are provided"""
        return (self.model_name is None and self.model_kwargs is None) or (
            self.model_name is not None and self.model_kwargs is not None
        )

    @property
    def model(self):
        """Get the underlying language model"""
        return self._model


class AzureOpenAIModel(BaseLLM):
    """Azure OpenAI Language Model implementation"""

    def __init__(
        self,
        model_name: Optional[str] = None,
        model_kwargs: Optional[Dict] = None,
    ):
        super().__init__(model_name, model_kwargs)

        if not self._validate_params():
            logger.error("Invalid initialization parameters")
            raise ValueError(
                "Both or neither of model_name and model_kwargs must be provided"
            )

        self._model = AzureOpenAI(
            azure_endpoint=LLMModelConfigOpenAI.AZURE_ENDPOINT,
            azure_deployment=LLMModelConfigOpenAI.DEPLOYMENT_NAME,
            api_version=LLMModelConfigOpenAI.API_VERSION,
            api_key=LLMModelConfigOpenAI.MODEL_API,
        )

    def make_client(self):
        """Make the Azure OpenAI client"""
        return self._model
