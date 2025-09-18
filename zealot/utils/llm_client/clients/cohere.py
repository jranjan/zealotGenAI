"""
Cohere LLM Client
Implementation for Cohere API integration
"""

from .base import BaseLLMClient


class CohereClient(BaseLLMClient):
    """
    Cohere LLM client implementation using Chat API
    
    Note: Migrated from deprecated Generate API to Chat API (September 2025)
    
    Supported additional parameters for Chat API:
    - stop_sequences: List of stop sequences
    - stream: Enable streaming response (boolean)
    
    Unsupported parameters (filtered out):
    - truncate: Not supported by Chat API
    """
    
    def _initialize_client(self):
        """Initialize Cohere client"""
        try:
            import cohere
            api_key = self._get_api_key()
            return cohere.Client(api_key=api_key)
        except ImportError:
            raise ImportError("Cohere package not installed. Run: pip install cohere")
    
    def generate_text(self, prompt: str, **kwargs) -> str:
        """Generate text using Cohere Chat API (migrated from deprecated Generate API)"""
        # Cohere Chat API parameters - only use supported parameters
        chat_params = {
            'model': self.config.model,
            'message': prompt,
            'temperature': kwargs.get('temperature', self.config.temperature),
            'max_tokens': kwargs.get('max_tokens', self.config.max_tokens),
        }
        
        # Add only parameters that are supported by Chat API
        # Chat API supports: model, message, temperature, max_tokens, stop_sequences, stream
        if 'stop_sequences' in self.config.additional_params:
            chat_params['stop_sequences'] = self.config.additional_params['stop_sequences']
        
        if 'stream' in self.config.additional_params:
            chat_params['stream'] = self.config.additional_params['stream']
        
        # Note: 'truncate' is not supported by Chat API, so we skip it
        
        # Use the new Chat API instead of deprecated Generate API
        response = self.client.chat(
            **chat_params
        )
        return response.text