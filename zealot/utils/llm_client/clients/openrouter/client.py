"""
OpenRouter LLM Client
Implementation for OpenRouter API integration
"""

import requests
import json
from typing import Optional, Dict, Any
from ..base import BaseLLMClient
from .models import OpenRouterModel


class OpenRouterClient(BaseLLMClient):
    """
    OpenRouter LLM client implementation
    
    Supports multiple models through OpenRouter's unified API.
    Uses configuration dataset for model selection and token limits.
    """
    
    def _initialize_client(self):
        """Initialize OpenRouter client (no client object needed)"""
        return None  # We'll use requests directly
    
    def generate_text(self, prompt: str, model: Optional[str] = None, **kwargs) -> str:
        """
        Generate text using OpenRouter API
        
        Args:
            prompt: The input prompt
            model: Optional model override (uses config model if not provided)
            **kwargs: Additional parameters (temperature, max_tokens, etc.)
            
        Returns:
            Generated text response
        """
        api_key = self._get_api_key()
        
        # Use provided model or fall back to config model
        target_model = model or self.config.model
        
        # Validate model and get token limits if available
        if model:
            self._validate_model_and_tokens(target_model, kwargs)
        
        # OpenRouter API parameters
        openrouter_params = {
            'model': target_model,
            'messages': [
                {
                    'role': 'user',
                    'content': prompt
                }
            ],
            'temperature': kwargs.get('temperature', self.config.temperature),
            'max_tokens': kwargs.get('max_tokens', self.config.max_tokens),
        }
        
        # Add any additional parameters from config
        if self.config.additional_params:
            openrouter_params.update(self.config.additional_params)
        
        # Prepare headers
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": self.config.additional_params.get('http_referer', 'https://zealotgenai.com'),
            "X-Title": self.config.additional_params.get('x_title', 'ZealotGenAI'),
        }
        
        try:
            response = requests.post(
                url=self.config.base_url,
                headers=headers,
                data=json.dumps(openrouter_params),
                timeout=self.config.timeout
            )
            response.raise_for_status()
            
            result = response.json()
            return result['choices'][0]['message']['content']
            
        except requests.exceptions.RequestException as e:
            raise ConnectionError(f"OpenRouter API request failed: {e}")
        except KeyError as e:
            raise ValueError(f"Unexpected response format from OpenRouter: {e}")
        except Exception as e:
            raise Exception(f"OpenRouter API error: {e}")
    
    def _validate_model_and_tokens(self, model: str, kwargs: Dict[str, Any]) -> None:
        """
        Validate model and token usage against limits
        
        Args:
            model: The model to validate
            kwargs: Parameters including max_tokens
        """
        try:
            from ..models import OpenRouterModel
            model_enum = OpenRouterModel.from_string(model)
            
            # Get requested max_tokens
            requested_tokens = kwargs.get('max_tokens', self.config.max_tokens)
            
            # Validate against model limits
            is_valid, error_msg = model_enum.validate_tokens(0, requested_tokens)
            if not is_valid:
                raise ValueError(f"Token limit exceeded for {model}: {error_msg}")
                
        except ImportError:
            # OpenRouterModel not available, skip validation
            pass
        except ValueError as e:
            if "Unknown OpenRouter model" in str(e):
                # Model not in enum, but might still be valid
                pass
            else:
                raise e
    
    def get_available_models(self) -> list:
        """
        Get list of available models from configuration
        
        Returns:
            List of available model names
        """
        try:
            from ..models import OpenRouterModel
            return [model.value for model in OpenRouterModel.get_all_models()]
        except ImportError:
            # Fallback to config model if enum not available
            return [self.config.model]
    
    def get_model_info(self, model: Optional[str] = None) -> Dict[str, Any]:
        """
        Get information about a specific model
        
        Args:
            model: Model name (uses config model if not provided)
            
        Returns:
            Dictionary with model information
        """
        target_model = model or self.config.model
        
        try:
            from ..models import OpenRouterModel
            model_enum = OpenRouterModel.from_string(target_model)
            return model_enum.get_token_info()
        except (ImportError, ValueError):
            # Fallback to basic info
            return {
                'model': target_model,
                'provider': target_model.split('/')[0] if '/' in target_model else 'unknown',
                'max_input_tokens': 'Unknown',
                'max_output_tokens': 'Unknown',
                'max_total_tokens': 'Unknown'
            }
