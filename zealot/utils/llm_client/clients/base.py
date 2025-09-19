"""
Base LLM Client

Abstract base class for all LLM client implementations. This class provides
a common interface and shared functionality for all LLM providers (Cohere,
OpenAI, Anthropic, Google, etc.).

All concrete LLM client implementations must inherit from this class and
implement the abstract methods.

Example:
    class MyLLMClient(BaseLLMClient):
        def _initialize_client(self):
            # Initialize your specific LLM client
            pass
        
        def generate_text(self, prompt: str, **kwargs) -> str:
            # Implement text generation
            pass
"""

import os
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, Tuple

from ..configs import LLMConfig
from ..providers import LLMProvider


class BaseLLMClient(ABC):
    """
    Abstract base class for LLM clients.
    
    This class provides a common interface for all LLM providers and handles
    shared functionality like API key resolution and provider information.
    
    Attributes:
        config (LLMConfig): Configuration for the LLM provider
        provider (LLMProvider): The provider enum instance
        client: The initialized provider-specific client
    
    Example:
        # This is typically done through the factory, not directly
        config = get_llm_config("cohere")
        client = CohereClient(config)
        response = client.generate_text("Hello, world!")
    """
    
    def __init__(self, config: LLMConfig):
        """
        Initialize the base LLM client.
        
        Args:
            config (LLMConfig): Configuration containing provider settings,
                               API key, model, and other parameters.
        
        Raises:
            ValueError: If the provider in config is invalid
            ImportError: If the required provider package is not installed
        """
        self.config = config
        self.provider = LLMProvider.from_string(config.provider)
        self.client = self._initialize_client()
    
    @abstractmethod
    def _initialize_client(self):
        """
        Initialize the provider-specific client.
        
        This method must be implemented by each concrete LLM client class.
        It should handle the initialization of the actual LLM provider's
        client library (e.g., cohere.Client, openai, anthropic.Anthropic).
        
        Returns:
            The initialized provider-specific client instance.
        
        Raises:
            ImportError: If the required provider package is not installed.
                        The error message should include installation instructions.
        """
        pass
    
    @abstractmethod
    def generate_text(self, prompt: str, **kwargs) -> str:
        """
        Generate text using the configured LLM.
        
        This method must be implemented by each concrete LLM client class.
        It should handle the actual text generation using the provider's API.
        
        Args:
            prompt (str): The input prompt for text generation
            **kwargs: Additional parameters for text generation (temperature,
                     max_tokens, etc.). These will override config defaults.
        
        Returns:
            str: The generated text response from the LLM.
        
        Raises:
            Exception: If text generation fails (API errors, network issues, etc.)
        """
        pass
    
    def _get_api_key(self) -> str:
        """
        Get the actual API key, resolving environment variables if needed.
        
        This method handles both direct API keys and environment variable
        references. If the config contains an environment variable reference
        (e.g., "${COHERE_API_KEY}"), it will resolve it from the environment.
        
        Returns:
            str: The resolved API key for the provider.
        
        Raises:
            ValueError: If an environment variable reference is not set.
        """
        api_key = self.config.api_key
        
        # If API key is an environment variable reference
        if api_key.startswith('${') and api_key.endswith('}'):
            env_var = api_key[2:-1]
            actual_key = os.getenv(env_var)
            if not actual_key:
                raise ValueError(f"Environment variable {env_var} is not set")
            return actual_key
        
        return api_key
    
    def get_provider_info(self) -> Dict[str, Any]:
        """
        Get comprehensive provider information.
        
        Returns a dictionary containing all relevant information about the
        current provider and configuration.
        
        Returns:
            Dict[str, Any]: Dictionary containing:
                - provider: The provider name (e.g., "cohere")
                - display_name: Human-readable provider name (e.g., "Cohere")
                - model: The model being used
                - temperature: Current temperature setting
                - max_tokens: Current max tokens setting
                - base_url: The API base URL
        
        Example:
            info = client.get_provider_info()
            print(f"Using {info['display_name']} with model {info['model']}")
        """
        return {
            'provider': self.provider.value,
            'display_name': self.provider.value.title(),
            'model': self.config.model,
            'temperature': self.config.temperature,
            'max_tokens': self.config.max_tokens,
            'base_url': self.config.base_url
        }
    
    def validate_tokens(self, input_tokens: int, output_tokens: int) -> Tuple[bool, str]:
        """
        Validate token usage against model limits (if available)
        
        Args:
            input_tokens: Number of input tokens
            output_tokens: Number of output tokens
            
        Returns:
            tuple: (is_valid, error_message)
        """
        # Try to get model limits from OpenRouterModel if available
        try:
            from ..models import OpenRouterModel
            model = OpenRouterModel.from_string(self.config.model)
            return model.validate_tokens(input_tokens, output_tokens)
        except (ImportError, ValueError):
            # Fallback: basic validation using config max_tokens
            if output_tokens > self.config.max_tokens:
                return False, f"Output tokens ({output_tokens}) exceed configured max_tokens ({self.config.max_tokens})"
            return True, ""
    
    def get_token_limits(self) -> Optional[Dict[str, int]]:
        """
        Get token limits for the current model (if available)
        
        Returns:
            dict with token limits or None if not available
        """
        try:
            from ..models import OpenRouterModel
            model = OpenRouterModel.from_string(self.config.model)
            return {
                'max_input_tokens': model.get_max_input_tokens(),
                'max_output_tokens': model.get_max_output_tokens(),
                'max_total_tokens': model.get_max_total_tokens()
            }
        except (ImportError, ValueError):
            return None
