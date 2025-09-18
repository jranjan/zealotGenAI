"""
Anthropic LLM Client
Implementation for Anthropic Claude API integration
"""

from .base import BaseLLMClient


class AnthropicClient(BaseLLMClient):
    """Anthropic LLM client implementation"""
    
    def _initialize_client(self):
        """Initialize Anthropic client"""
        try:
            import anthropic
            api_key = self._get_api_key()
            return anthropic.Anthropic(api_key=api_key)
        except ImportError:
            raise ImportError("Anthropic package not installed. Run: pip install anthropic")
    
    def generate_text(self, prompt: str, **kwargs) -> str:
        """Generate text using Anthropic Claude"""
        response = self.client.messages.create(
            model=self.config.model,
            max_tokens=kwargs.get('max_tokens', self.config.max_tokens),
            temperature=kwargs.get('temperature', self.config.temperature),
            messages=[{"role": "user", "content": prompt}],
            **self.config.additional_params
        )
        return response.content[0].text