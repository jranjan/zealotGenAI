"""
OpenRouter LLM App
LLM application with multiple model support
"""

from typing import Optional, Dict, Any
from zealot.utils.llm_client.clients.openrouter import OpenRouterModel
from zealot.utils.printer import OpenRouterPrinter
from zealot.apps.common.app import LLMApp


class OpenRouterApp(LLMApp):
    """OpenRouter LLM App with multiple model support"""
    
    def run(self, message: str, model: str, provider_params: Optional[Dict[str, Any]] = None):
        """Run the app with model selection using configuration dataset"""
        # Store the model and provider params for use in generate method
        self._current_model = model
        self._provider_params = provider_params or {}
        # Add model info to the message for display
        enhanced_message = f"{message}\n🔧 Model: {model}"
        # Use base class implementation
        return super().run(enhanced_message)
    
    def generate(self, message: str) -> str:
        """Generate response from LLM with model selection"""
        client = self._get_client()
        
        # Use the stored model and provider params
        return client.generate_text(message, model=self._current_model, **self._provider_params)
    
    def list_models(self):
        """List available OpenRouter models with token limits"""
        models = OpenRouterModel.get_all_models()
        OpenRouterPrinter.print_models_table(models)
        return [model.value for model in models]
    
    def list_models_by_provider(self, provider: str = None):
        """List models grouped by provider or for a specific provider"""
        if provider:
            models = OpenRouterModel.get_by_provider(provider)
            OpenRouterPrinter.print_models_by_provider(provider, models)
            return [model.value for model in models]
        else:
            OpenRouterPrinter.print_all_models_by_provider()
            return [model.value for model in OpenRouterModel.get_all_models()]
    
    def show_model_info(self, model_name: str):
        """Show detailed information for a specific model"""
        try:
            model = OpenRouterModel.from_string(model_name)
            OpenRouterPrinter.print_model_info(model)
        except ValueError as e:
            OpenRouterPrinter.print_error(str(e))
    
    def validate_request(self, model_name: str, input_tokens: int, output_tokens: int):
        """Validate token usage for a specific model"""
        try:
            model = OpenRouterModel.from_string(model_name)
            OpenRouterPrinter.print_model_validation(model, input_tokens, output_tokens)
        except ValueError as e:
            OpenRouterPrinter.print_error(str(e))
    