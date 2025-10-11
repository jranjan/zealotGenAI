"""
Minimal LLM Client

Single client that handles all LLM providers.
"""

from enum import Enum
from typing import Dict, Any, Optional
import os
import json
import requests


class ProviderInfo(Enum):
    """Provider information."""
    
    OPENAI = ("openai", "https://api.openai.com/v1", "OPENAI_API_KEY")
    COHERE = ("cohere", "https://api.cohere.ai/v1", "COHERE_API_KEY")
    ANTHROPIC = ("anthropic", "https://api.anthropic.com", "ANTHROPIC_API_KEY")
    GOOGLE = ("google", "https://generativelanguage.googleapis.com/v1", "GOOGLE_API_KEY")
    OPENROUTER = ("openrouter", "https://openrouter.ai/api/v1", "OPENROUTER_API_KEY")
    
    def __init__(self, provider_name: str, endpoint: str, api_key_env: str):
        self.provider_name = provider_name
        self.endpoint = endpoint
        self.api_key_env = api_key_env


class LLMClient:
    """Minimal LLM client for all providers."""
    
    def __init__(
        self,
        provider: str,
        api_key: str,
        model: str = "gpt-3.5-turbo",
        temperature: float = 0.7,
        max_tokens: int = 1000,
        **kwargs
    ):
        """Initialize LLM client."""
        self.provider = provider
        self.api_key = api_key
        self.model = model
        self.temperature = temperature
        self.max_tokens = max_tokens
        self.kwargs = kwargs
        
        # Get provider info
        for info in ProviderInfo:
            if info.provider_name == provider:
                self.provider_info = info
                break
        else:
            raise ValueError(f"Unsupported provider: {provider}")
    
    def get_api_key(self) -> str:
        """Get API key, resolving environment variables."""
        if self.api_key.startswith('${') and self.api_key.endswith('}'):
            env_var = self.api_key[2:-1]
            actual_key = os.getenv(env_var)
            if not actual_key:
                raise ValueError(f"Environment variable {env_var} is not set")
            return actual_key
        return self.api_key
    
    def generate(self, prompt: str, **kwargs) -> str:
        """Generate text using the configured provider."""
        api_key = self.get_api_key()
        params = {
            'temperature': kwargs.get('temperature', self.temperature),
            'max_tokens': kwargs.get('max_tokens', self.max_tokens),
            **self.kwargs,
            **kwargs
        }
        
        if self.provider == "openai":
            return self._generate_openai(prompt, api_key, params)
        elif self.provider == "cohere":
            return self._generate_cohere(prompt, api_key, params)
        elif self.provider == "anthropic":
            return self._generate_anthropic(prompt, api_key, params)
        elif self.provider == "google":
            return self._generate_google(prompt, api_key, params)
        elif self.provider == "openrouter":
            return self._generate_openrouter(prompt, api_key, params)
        else:
            raise ValueError(f"Unsupported provider: {self.provider}")
    
    def _generate_openai(self, prompt: str, api_key: str, params: Dict[str, Any]) -> str:
        """Generate using OpenAI API."""
        try:
            import openai
            openai.api_key = api_key
            response = openai.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                temperature=params['temperature'],
                max_tokens=params['max_tokens']
            )
            return response.choices[0].message.content
        except ImportError:
            raise ImportError("OpenAI package not installed")
    
    def _generate_cohere(self, prompt: str, api_key: str, params: Dict[str, Any]) -> str:
        """Generate using Cohere API."""
        try:
            import cohere
            client = cohere.Client(api_key=api_key)
            response = client.chat(
                model=self.model,
                message=prompt,
                temperature=params['temperature'],
                max_tokens=params['max_tokens']
            )
            return response.text
        except ImportError:
            raise ImportError("Cohere package not installed")
    
    def _generate_anthropic(self, prompt: str, api_key: str, params: Dict[str, Any]) -> str:
        """Generate using Anthropic API."""
        try:
            import anthropic
            client = anthropic.Anthropic(api_key=api_key)
            response = client.messages.create(
                model=self.model,
                max_tokens=params['max_tokens'],
                temperature=params['temperature'],
                messages=[{"role": "user", "content": prompt}]
            )
            return response.content[0].text
        except ImportError:
            raise ImportError("Anthropic package not installed")
    
    def _generate_google(self, prompt: str, api_key: str, params: Dict[str, Any]) -> str:
        """Generate using Google Gemini API."""
        try:
            import google.generativeai as genai
            genai.configure(api_key=api_key)
            model = genai.GenerativeModel(self.model)
            response = model.generate_content(
                prompt,
                generation_config={
                    'temperature': params['temperature'],
                    'max_output_tokens': params['max_tokens']
                }
            )
            return response.text
        except ImportError:
            raise ImportError("Google Generative AI package not installed")
    
    def _generate_openrouter(self, prompt: str, api_key: str, params: Dict[str, Any]) -> str:
        """Generate using OpenRouter API."""
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        payload = {
            'model': self.model,
            'messages': [{"role": "user", "content": prompt}],
            'temperature': params['temperature'],
            'max_tokens': params['max_tokens']
        }
        
        try:
            response = requests.post(
                url=self.provider_info.endpoint + "/chat/completions",
                headers=headers,
                data=json.dumps(payload)
            )
            response.raise_for_status()
            result = response.json()
            return result['choices'][0]['message']['content']
        except requests.exceptions.RequestException as e:
            raise Exception(f"OpenRouter API request failed: {e}")