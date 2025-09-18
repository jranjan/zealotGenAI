"""
Google LLM Client
Implementation for Google Gemini API integration
"""

from .base import BaseLLMClient


class GoogleClient(BaseLLMClient):
    """Google LLM client implementation"""
    
    def _initialize_client(self):
        """Initialize Google client"""
        try:
            import google.generativeai as genai
            api_key = self._get_api_key()
            genai.configure(api_key=api_key)
            return genai
        except ImportError:
            raise ImportError("Google Generative AI package not installed. Run: pip install google-generativeai")
    
    def generate_text(self, prompt: str, **kwargs) -> str:
        """Generate text using Google Gemini"""
        model = self.client.GenerativeModel(self.config.model)
        response = model.generate_content(
            prompt,
            generation_config={
                'temperature': kwargs.get('temperature', self.config.temperature),
                'max_output_tokens': kwargs.get('max_tokens', self.config.max_tokens),
                **self.config.additional_params
            }
        )
        return response.text