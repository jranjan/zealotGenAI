"""
OpenAI LLM Client
Implementation for OpenAI API integration
"""

from .base import BaseLLMClient


class OpenAIClient(BaseLLMClient):
    """OpenAI LLM client implementation"""
    
    def _initialize_client(self):
        """Initialize OpenAI client"""
        try:
            import openai
            api_key = self._get_api_key()
            openai.api_key = api_key
            return openai
        except ImportError:
            raise ImportError("OpenAI package not installed. Run: pip install openai")
    
    def generate_text(self, prompt: str, **kwargs) -> str:
        """Generate text using OpenAI"""
        response = self.client.ChatCompletion.create(
            model=self.config.model,
            messages=[{"role": "user", "content": prompt}],
            temperature=kwargs.get('temperature', self.config.temperature),
            max_tokens=kwargs.get('max_tokens', self.config.max_tokens),
            **self.config.additional_params
        )
        return response.choices[0].message.content