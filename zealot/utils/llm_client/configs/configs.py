"""
LLM Configuration Manager
Manages configurations for multiple LLM providers (Cohere, OpenAI, etc.)
"""

import json
from typing import Dict, Any, Optional
from dataclasses import dataclass
from pathlib import Path
from .provider_configs import ProviderConfig


@dataclass
class LLMConfig:
    """Configuration for a specific LLM provider"""
    provider: str
    api_key: str
    base_url: Optional[str] = None
    model: str = "default"
    temperature: float = 0.7
    max_tokens: int = 1000
    timeout: int = 30
    additional_params: Dict[str, Any] = None

    def __post_init__(self):
        if self.additional_params is None:
            self.additional_params = {}


class LLMConfigManager:
    """Manages LLM configurations for multiple providers"""
    
    def __init__(self, config_dir: str = "zealot/config/llm"):
        self.config_dir = Path(config_dir)
        self.config_dir.mkdir(parents=True, exist_ok=True)
        self._configs: Dict[str, LLMConfig] = {}
        self._loaded_providers: set = set()
    
    def _load_config(self, provider: str) -> Optional[LLMConfig]:
        """Load configuration for a specific provider (lazy loading)"""
        if provider in self._loaded_providers:
            return self._configs.get(provider)
        
        config_file = self.config_dir / f"{provider}.json"
        
        if not config_file.exists():
            return None
        
        try:
            with open(config_file, 'r') as f:
                config_data = json.load(f)
            
            config = LLMConfig(**config_data)
            self._configs[provider] = config
            self._loaded_providers.add(provider)
            print(f"✅ Loaded config for {provider}")
            return config
            
        except Exception as e:
            print(f"❌ Error loading config for {provider}: {e}")
            return None
    
    def get_config(self, provider: str) -> Optional[LLMConfig]:
        """Get configuration for a specific provider (lazy loading)"""
        return self._load_config(provider)
    
    def list_providers(self) -> list:
        """List all available providers (scans config directory)"""
        config_files = self.config_dir.glob("*.json")
        providers = []
        
        for config_file in config_files:
            providers.append(config_file.stem)
        
        return providers
    
    def add_config(self, config: LLMConfig):
        """Add or update a configuration"""
        self._configs[config.provider] = config
        self._save_config(config)
    
    def _save_config(self, config: LLMConfig):
        """Save configuration to file"""
        config_file = self.config_dir / f"{config.provider}.json"
        
        config_data = {
            'provider': config.provider,
            'api_key': config.api_key,
            'base_url': config.base_url,
            'model': config.model,
            'temperature': config.temperature,
            'max_tokens': config.max_tokens,
            'timeout': config.timeout,
            'additional_params': config.additional_params or {}
        }
        
        with open(config_file, 'w') as f:
            json.dump(config_data, f, indent=2)
        
        print(f"💾 Saved config for {config.provider}")
    
    def create_config(self, provider: str, api_key: str, **kwargs):
        """Create configuration for any provider using factory pattern"""
        # Get defaults from enum
        provider_defaults = ProviderConfig.get_defaults(provider)
        
        # Create config with provider-specific defaults
        config = LLMConfig(
            provider=provider,
            api_key=api_key,
            base_url=kwargs.get('base_url', provider_defaults.base_url),
            model=kwargs.get('model', provider_defaults.model),
            temperature=kwargs.get('temperature', provider_defaults.temperature),
            max_tokens=kwargs.get('max_tokens', provider_defaults.max_tokens),
            timeout=kwargs.get('timeout', provider_defaults.timeout),
            additional_params=kwargs.get('additional_params', {})
        )
        self.add_config(config)
        return config


# Global config manager instance
config_manager = LLMConfigManager()


def get_llm_config(provider: str) -> Optional[LLMConfig]:
    """Convenience function to get LLM config"""
    return config_manager.get_config(provider)


def list_available_providers() -> list:
    """Convenience function to list available providers"""
    return config_manager.list_providers()


def create_llm_config(provider: str, api_key: str, **kwargs):
    """Convenience function to create LLM config using factory pattern"""
    return config_manager.create_config(provider, api_key, **kwargs)


# Example usage and initialization
if __name__ == "__main__":
    # Example: Create configurations for different providers
    print("🔧 LLM Configuration Manager")
    print("=" * 50)
    
    # List available providers
    providers = list_available_providers()
    print(f"Available providers: {providers}")
    
    # Example of creating configs using factory pattern
    # config_manager.create_config("cohere", "your-api-key", model="command")
    # config_manager.create_config("openai", "your-api-key", model="gpt-4")
    # config_manager.create_config("anthropic", "your-api-key", model="claude-3-opus")
    # config_manager.create_config("google", "your-api-key", model="gemini-pro")
