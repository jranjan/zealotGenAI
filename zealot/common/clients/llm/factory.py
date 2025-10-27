"""
Minimal LLM Client Factory
"""

from enum import Enum
from .client import LLMClient


class LLMClientType(Enum):
    """LLM client type enumeration."""
    OPENAI = "openai"
    COHERE = "cohere"
    ANTHROPIC = "anthropic"
    GOOGLE = "google"
    OPENROUTER = "openrouter"


class LLMClientFactory:
    """Factory for creating LLM client instances."""
    
    @staticmethod
    def create(provider: str, api_key: str, **kwargs) -> LLMClient:
        """Create LLM client."""
        return LLMClient(provider=provider, api_key=api_key, **kwargs)