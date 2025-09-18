"""
LLM Client Module
Unified interface for multiple LLM providers using Factory pattern
"""

# Import the essentials
from .providers import LLMProvider
from .factory import create_llm_client, create_llm_client_from_string, list_supported_providers
from .configs import get_llm_config, config_manager, create_llm_config, ProviderConfig

# Version
__version__ = "1.0.0"