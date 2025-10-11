"""
Minimal LLM Client Factory
"""

from client import LLMClient


class LLMClientFactory:
    """Factory for creating LLM client instances."""
    
    @staticmethod
    def create(provider: str, api_key: str, **kwargs) -> LLMClient:
        """Create LLM client."""
        return LLMClient(provider=provider, api_key=api_key, **kwargs)