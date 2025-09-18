"""
Provider Configuration Enum
Defines default configurations for each LLM provider
"""

from enum import Enum
from dataclasses import dataclass
from typing import Dict, Any


@dataclass
class ProviderDefaults:
    """Default configuration for a provider"""
    base_url: str
    model: str
    temperature: float = 0.7
    max_tokens: int = 1000
    timeout: int = 30


class ProviderConfig(Enum):
    """Enum containing default configurations for each LLM provider"""
    
    COHERE = ProviderDefaults(
        base_url="https://api.cohere.ai/v1",
        model="command"
    )
    
    OPENAI = ProviderDefaults(
        base_url="https://api.openai.com/v1",
        model="gpt-3.5-turbo"
    )
    
    ANTHROPIC = ProviderDefaults(
        base_url="https://api.anthropic.com",
        model="claude-3-sonnet-20240229"
    )
    
    GOOGLE = ProviderDefaults(
        base_url="https://generativelanguage.googleapis.com/v1beta",
        model="gemini-pro"
    )
    
    @classmethod
    def get_defaults(cls, provider: str) -> ProviderDefaults:
        """Get default configuration for a provider"""
        provider_upper = provider.upper()
        try:
            return cls[provider_upper].value
        except KeyError:
            # Return minimal defaults for unknown providers
            return ProviderDefaults(
                base_url="",
                model="default"
            )
    
    @classmethod
    def list_providers(cls) -> list:
        """List all available provider names"""
        return [provider.name.lower() for provider in cls]
    
    @classmethod
    def get_provider_info(cls, provider: str) -> Dict[str, Any]:
        """Get provider information as dictionary"""
        defaults = cls.get_defaults(provider)
        return {
            'base_url': defaults.base_url,
            'model': defaults.model,
            'temperature': defaults.temperature,
            'max_tokens': defaults.max_tokens,
            'timeout': defaults.timeout
        }


# Example usage
if __name__ == "__main__":
    print("🔧 Provider Configuration Enum")
    print("=" * 40)
    
    # List all providers
    providers = ProviderConfig.list_providers()
    print(f"Available providers: {providers}")
    
    # Get defaults for each provider
    for provider in providers:
        defaults = ProviderConfig.get_defaults(provider)
        print(f"\n{provider.upper()}:")
        print(f"  Base URL: {defaults.base_url}")
        print(f"  Model: {defaults.model}")
        print(f"  Temperature: {defaults.temperature}")
        print(f"  Max Tokens: {defaults.max_tokens}")
        print(f"  Timeout: {defaults.timeout}")
