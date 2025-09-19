"""
OpenRouter Model Enum
Defines supported OpenRouter models with token limits
"""

from enum import Enum
from dataclasses import dataclass


@dataclass
class ModelLimits:
    """Token limits for a model"""
    max_input_tokens: int
    max_output_tokens: int
    max_total_tokens: int = None
    
    @property
    def effective_max_total(self) -> int:
        """Get effective maximum total tokens"""
        if self.max_total_tokens:
            return min(self.max_total_tokens, self.max_input_tokens + self.max_output_tokens)
        return self.max_input_tokens + self.max_output_tokens


class OpenRouterModel(Enum):
    """Supported OpenRouter models with token limits"""
    
    # OpenAI
    GPT_4O = ("openai/gpt-4o", ModelLimits(128000, 16384, 128000))
    GPT_4O_MINI = ("openai/gpt-4o-mini", ModelLimits(128000, 16384, 128000))
    GPT_3_5_TURBO = ("openai/gpt-3.5-turbo", ModelLimits(16385, 4096, 16385))
    
    # Anthropic
    CLAUDE_3_5_SONNET = ("anthropic/claude-3.5-sonnet", ModelLimits(200000, 8192, 200000))
    CLAUDE_3_HAIKU = ("anthropic/claude-3-haiku", ModelLimits(200000, 4096, 200000))
    
    # Google
    GEMINI_PRO = ("google/gemini-2.5-flash", ModelLimits(1000000, 8192, 1000000))
    
    # Cohere
    COHERE_COMMAND = ("cohere/command-a", ModelLimits(128000, 4096, 128000))
    COHERE_COMMAND_LIGHT = ("cohere/command-light", ModelLimits(128000, 4096, 128000))
    COHERE_COMMAND_NIGHTLY = ("cohere/command-nightly", ModelLimits(128000, 4096, 128000))
    
    # Meta
    LLAMA_3_1_8B_INSTRUCT = ("meta-llama/llama-3.1-8b-instruct", ModelLimits(128000, 8192, 128000))
    
    # Mistral
    MISTRAL_7B_INSTRUCT = ("mistralai/mistral-nemo", ModelLimits(128000, 8192, 128000))
    
    def __init__(self, model_name: str, limits: ModelLimits):
        self.model_name = model_name
        self.limits = limits
    
    @property
    def value(self) -> str:
        return self.model_name
    
    @classmethod
    def from_string(cls, model_str: str) -> 'OpenRouterModel':
        """Create OpenRouterModel from string"""
        model_str = model_str.lower().strip()
        
        # Exact match
        for model in cls:
            if model.value == model_str:
                return model
        
        # Special handling for Cohere variants (check first)
        if "cohere" in model_str:
            if "light" in model_str:
                return cls.COHERE_COMMAND_LIGHT
            elif "nightly" in model_str:
                return cls.COHERE_COMMAND_NIGHTLY
            else:
                return cls.COHERE_COMMAND
        
        # Partial matches using dictionary lookup (ordered by specificity)
        partial_matches = [
            ("gpt-4o-mini", cls.GPT_4O_MINI),  # More specific first
            ("gpt-4o", cls.GPT_4O),
            ("claude-3.5", cls.CLAUDE_3_5_SONNET),
            ("claude-3-haiku", cls.CLAUDE_3_HAIKU),
            ("gemini", cls.GEMINI_PRO),
            ("llama", cls.LLAMA_3_1_8B_INSTRUCT),
            ("mistral", cls.MISTRAL_7B_INSTRUCT)
        ]
        
        # Check partial matches in order
        for key, model in partial_matches:
            if key in model_str:
                return model
        
        raise ValueError(f"Unknown OpenRouter model: {model_str}")
    
    @classmethod
    def get_by_provider(cls, provider: str) -> list:
        """Get models for a provider"""
        provider = provider.lower()
        providers = {
            "openai": [cls.GPT_4O, cls.GPT_4O_MINI, cls.GPT_3_5_TURBO],
            "anthropic": [cls.CLAUDE_3_5_SONNET, cls.CLAUDE_3_HAIKU],
            "google": [cls.GEMINI_PRO],
            "cohere": [cls.COHERE_COMMAND, cls.COHERE_COMMAND_LIGHT, cls.COHERE_COMMAND_NIGHTLY],
            "meta": [cls.LLAMA_3_1_8B_INSTRUCT],
            "mistral": [cls.MISTRAL_7B_INSTRUCT]
        }
        return providers.get(provider, [])
    
    @classmethod
    def get_all_models(cls) -> list:
        return list(cls)
    
    def get_provider(self) -> str:
        return self.value.split('/')[0]
    
    def get_model_name(self) -> str:
        return self.value.split('/')[1]
    
    def get_max_input_tokens(self) -> int:
        return self.limits.max_input_tokens
    
    def get_max_output_tokens(self) -> int:
        return self.limits.max_output_tokens
    
    def get_max_total_tokens(self) -> int:
        return self.limits.effective_max_total
    
    def validate_tokens(self, input_tokens: int, output_tokens: int) -> tuple[bool, str]:
        """Validate token usage against model limits"""
        if input_tokens > self.limits.max_input_tokens:
            return False, f"Input tokens ({input_tokens}) exceed maximum ({self.limits.max_input_tokens})"
        
        if output_tokens > self.limits.max_output_tokens:
            return False, f"Output tokens ({output_tokens}) exceed maximum ({self.limits.max_output_tokens})"
        
        total_tokens = input_tokens + output_tokens
        if total_tokens > self.limits.effective_max_total:
            return False, f"Total tokens ({total_tokens}) exceed maximum ({self.limits.effective_max_total})"
        
        return True, ""
    
    def get_token_info(self) -> dict:
        """Get token information for this model"""
        return {
            'model': self.value,
            'provider': self.get_provider(),
            'max_input_tokens': self.limits.max_input_tokens,
            'max_output_tokens': self.limits.max_output_tokens,
            'max_total_tokens': self.limits.effective_max_total,
            'max_input_tokens_formatted': f"{self.limits.max_input_tokens:,}",
            'max_output_tokens_formatted': f"{self.limits.max_output_tokens:,}",
            'max_total_tokens_formatted': f"{self.limits.effective_max_total:,}"
        }
    
    def __str__(self) -> str:
        return self.value
