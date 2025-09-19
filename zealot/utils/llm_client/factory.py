"""
LLM Client Factory
Factory pattern implementation for creating LLM clients based on provider enum
"""

from typing import Dict, Any

from .providers import LLMProvider
from .configs import get_llm_config
from .clients import BaseLLMClient, CohereClient, OpenAIClient, AnthropicClient, GoogleClient, OpenRouterClient


class LLMClientFactory:
    """Factory class for creating LLM clients based on provider enum"""
    
    # Registry of client classes mapped to providers
    _client_registry: Dict[LLMProvider, type] = {
        LLMProvider.COHERE: CohereClient,
        LLMProvider.OPENAI: OpenAIClient,
        LLMProvider.ANTHROPIC: AnthropicClient,
        LLMProvider.GOOGLE: GoogleClient,
        LLMProvider.OPENROUTER: OpenRouterClient,
    }
    
    @classmethod
    def create_client(cls, provider: LLMProvider) -> BaseLLMClient:
        """
        Create an LLM client for the specified provider
        
        Args:
            provider: The LLM provider enum
            
        Returns:
            Configured LLM client instance
            
        Raises:
            ValueError: If provider is not supported
            ValueError: If no configuration found for provider
        """
        # Get configuration for the provider
        config = get_llm_config(provider.value)
        if not config:
            raise ValueError(f"No configuration found for provider: {provider.value}")
        
        # Get the appropriate client class
        client_class = cls._client_registry.get(provider)
        if not client_class:
            raise ValueError(f"Unsupported provider: {provider.value}")
        
        # Create and return the client
        return client_class(config)
    
    @classmethod
    def create_client_from_string(cls, provider_str: str) -> BaseLLMClient:
        """
        Create an LLM client from a provider string
        
        Args:
            provider_str: Provider name as string (e.g., "cohere", "openai")
            
        Returns:
            Configured LLM client instance
        """
        provider = LLMProvider.from_string(provider_str)
        return cls.create_client(provider)
    
    @classmethod
    def get_supported_providers(cls) -> list:
        """Get list of all supported providers"""
        return list(cls._client_registry.keys())
    
    @classmethod
    def register_provider(cls, provider: LLMProvider, client_class: type):
        """
        Register a new provider and its client class
        
        Args:
            provider: The LLM provider enum
            client_class: The client class that implements BaseLLMClient
        """
        if not issubclass(client_class, BaseLLMClient):
            raise ValueError("Client class must inherit from BaseLLMClient")
        
        cls._client_registry[provider] = client_class
    
    @classmethod
    def is_provider_supported(cls, provider: LLMProvider) -> bool:
        """Check if a provider is supported"""
        return provider in cls._client_registry


# =============================================================================
# CONVENIENCE FUNCTIONS - Simplified API for common use cases
# =============================================================================
# These functions provide a clean, simple interface for the most common
# LLM client creation patterns. Use these instead of the factory class
# directly for better readability and easier maintenance.

def create_llm_client(provider: LLMProvider) -> BaseLLMClient:
    """
    Create an LLM client for the specified provider using enum.
    
    This is the RECOMMENDED way to create clients when you know the provider
    at compile time. It provides type safety and IDE autocomplete.
    
    Args:
        provider: LLMProvider enum value (e.g., LLMProvider.COHERE)
        
    Returns:
        BaseLLMClient: Configured client instance for the provider
        
    Example:
        >>> from zealot.utils.llm_client import create_llm_client, LLMProvider
        >>> client = create_llm_client(LLMProvider.COHERE)
        >>> response = client.generate_text("Hello, world!")
        
    When to use:
        - You know the provider at development time
        - You want type safety and IDE support
        - You're building a specific application with a fixed provider
    """
    return LLMClientFactory.create_client(provider)


def create_llm_client_from_string(provider_str: str) -> BaseLLMClient:
    """
    Create an LLM client from a provider string (runtime configuration).
    
    This function is useful when the provider is determined at runtime,
    such as from configuration files, environment variables, or user input.
    It automatically handles case-insensitive provider names.
    
    Args:
        provider_str: Provider name as string (e.g., "cohere", "openai")
        
    Returns:
        BaseLLMClient: Configured client instance for the provider
        
    Example:
        >>> from zealot.utils.llm_client import create_llm_client_from_string
        >>> provider = os.getenv("LLM_PROVIDER", "cohere")
        >>> client = create_llm_client_from_string(provider)
        >>> response = client.generate_text("Hello, world!")
        
    When to use:
        - Provider is determined at runtime (config files, env vars)
        - Building dynamic applications that can switch providers
        - Creating CLI tools or web apps with user-selectable providers
        - Testing with different providers programmatically
    """
    return LLMClientFactory.create_client_from_string(provider_str)


def list_supported_providers() -> list:
    """
    List all supported LLM providers as strings.
    
    This function returns a list of all available provider names that can
    be used with create_llm_client_from_string(). Useful for validation,
    user interfaces, and dynamic provider selection.
    
    Returns:
        list: List of supported provider names (e.g., ['cohere', 'openai', ...])
        
    Example:
        >>> from zealot.utils.llm_client import list_supported_providers
        >>> providers = list_supported_providers()
        >>> print(f"Available providers: {providers}")
        >>> # Use in validation
        >>> if user_provider in list_supported_providers():
        ...     client = create_llm_client_from_string(user_provider)
        
    When to use:
        - Building user interfaces with provider selection
        - Validating user input for provider names
        - Creating help text or documentation
        - Dynamic provider discovery in applications
    """
    return LLMClientFactory.get_supported_providers()


# Example usage
if __name__ == "__main__":
    print("🏭 LLM Client Factory")
    print("=" * 30)
    
    # List supported providers
    providers = list_supported_providers()
    print(f"Supported providers: {[p.value for p in providers]}")
    
    # Example of creating clients
    try:
        # Using enum
        cohere_client = create_llm_client(LLMProvider.COHERE)
        print(f"✅ Created Cohere client: {type(cohere_client).__name__}")
        
        # Using string
        openai_client = create_llm_client_from_string("openai")
        print(f"✅ Created OpenAI client: {type(openai_client).__name__}")
        
    except Exception as e:
        print(f"❌ Error creating clients: {e}")