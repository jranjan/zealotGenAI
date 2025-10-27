"""
LLM Client Implementations
Concrete implementations for different LLM providers
"""

from zealot.common.clients.llm.factory import LLMClientFactory, LLMClientType
from zealot.common.clients.llm.client import LLMClient

# Stub for backward compatibility
class OpenRouterModel:
    """Stub class for OpenRouterModel."""
    pass

__all__ = [
    'LLMClientFactory',
    'LLMClientType',
    'LLMClient',
    'OpenRouterModel'
]
