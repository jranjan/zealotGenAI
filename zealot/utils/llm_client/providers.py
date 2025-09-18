"""
LLM Provider Enum
Defines all supported LLM providers as an enum for type safety
"""

from enum import Enum


class LLMProvider(Enum):
    """Enumeration of supported LLM providers"""
    
    COHERE = "cohere"
    OPENAI = "openai"
    ANTHROPIC = "anthropic"
    GOOGLE = "google"
    
    @classmethod
    def from_string(cls, provider_str: str) -> 'LLMProvider':
        """Create LLMProvider from string, case-insensitive"""
        provider_str = provider_str.lower().strip()
        
        # Try exact match first
        for provider in cls:
            if provider.value == provider_str:
                return provider
        
        # Try partial matches
        if "cohere" in provider_str:
            return cls.COHERE
        elif "openai" in provider_str or "gpt" in provider_str:
            return cls.OPENAI
        elif "anthropic" in provider_str or "claude" in provider_str:
            return cls.ANTHROPIC
        elif "google" in provider_str or "gemini" in provider_str:
            return cls.GOOGLE
        
        raise ValueError(f"Unknown LLM provider: {provider_str}")
    
    def __str__(self) -> str:
        return self.value
