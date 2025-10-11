"""
OpenRouter Printer
Specialized printing utilities for OpenRouter applications
"""

from typing import List

from zealot.common.clients import OpenRouterModel


class OpenRouterPrinter:
    """
    Specialized printer for OpenRouter applications
    
    Provides static methods for printing OpenRouter-specific information
    including model listings, provider groupings, and model details.
    """
    
    @staticmethod
    def print_models_table(models: List[OpenRouterModel], title: str = "Available OpenRouter Models") -> None:
        """
        Print a formatted table of OpenRouter models
        
        Args:
            models: List of OpenRouterModel instances
            title: Title for the models table
        """
        if not models:
            OpenRouterPrinter.print_section("No models available", title, "🤖")
            return
        
        # Table header
        header = f"{'#':<3} {'Model':<35} {'Input':<12} {'Output':<12} {'Total':<12}"
        separator = "-" * 80
        
        # Table rows
        model_lines = []
        for i, model in enumerate(models, 1):
            input_tokens = f"{model.get_max_input_tokens():,}"
            output_tokens = f"{model.get_max_output_tokens():,}"
            total_tokens = f"{model.get_max_total_tokens():,}"
            model_lines.append(f"{i:<3} {model.value:<35} {input_tokens:<12} {output_tokens:<12} {total_tokens:<12}")
        
        content = f"{header}\n{separator}\n" + "\n".join(model_lines)
        OpenRouterPrinter.print_section(content, title, "🤖")
    
    @staticmethod
    def print_models_by_provider(provider: str, models: List[OpenRouterModel]) -> None:
        """
        Print models grouped by provider
        
        Args:
            provider: Provider name
            models: List of models for the provider
        """
        if not models:
            OpenRouterPrinter.print_section(f"No {provider} models available", f"{provider.title()} Models", "🤖")
            return
        
        # Table header
        header = f"{'Model':<35} {'Input':<12} {'Output':<12}"
        separator = "-" * 80
        
        # Table rows
        model_lines = []
        for model in models:
            input_tokens = f"{model.get_max_input_tokens():,}"
            output_tokens = f"{model.get_max_output_tokens():,}"
            model_lines.append(f"{model.value:<35} {input_tokens:<12} {output_tokens:<12}")
        
        content = f"{header}\n{separator}\n" + "\n".join(model_lines)
        OpenRouterPrinter.print_section(content, f"{provider.title()} Models", "🤖")
    
    @staticmethod
    def print_all_models_by_provider() -> None:
        """
        Print all models grouped by provider
        """
        providers = ["openai", "anthropic", "google", "cohere", "meta", "mistral"]
        
        all_models = []
        content_lines = []
        
        for provider_name in providers:
            models = OpenRouterModel.get_by_provider(provider_name)
            if models:
                content_lines.append(f"\n{provider_name.title()}:")
                for model in models:
                    input_tokens = f"{model.get_max_input_tokens():,}"
                    output_tokens = f"{model.get_max_output_tokens():,}"
                    content_lines.append(f"  • {model.value:<35} (Input: {input_tokens}, Output: {output_tokens})")
                    all_models.append(model.value)
        
        OpenRouterPrinter.print_section("\n".join(content_lines), "Available OpenRouter Models by Provider", "🤖")
    
    @staticmethod
    def print_model_info(model: OpenRouterModel) -> None:
        """
        Print detailed information for a specific model
        
        Args:
            model: OpenRouterModel instance
        """
        info = model.get_token_info()
        
        info_text = f"""Provider: {info['provider']}
Max Input Tokens: {info['max_input_tokens_formatted']}
Max Output Tokens: {info['max_output_tokens_formatted']}
Max Total Tokens: {info['max_total_tokens_formatted']}"""
        
        OpenRouterPrinter.print_section(info_text, f"Model Information | Provider: OpenRouter | Model: {info['model']}", "🤖")
    
    @staticmethod
    def print_model_validation(model: OpenRouterModel, input_tokens: int, output_tokens: int) -> None:
        """
        Print model validation results
        
        Args:
            model: OpenRouterModel instance
            input_tokens: Number of input tokens
            output_tokens: Number of output tokens
        """
        is_valid, error_msg = model.validate_tokens(input_tokens, output_tokens)
        
        if is_valid:
            content = f"✅ Token usage is valid\nInput: {input_tokens:,} tokens\nOutput: {output_tokens:,} tokens\nTotal: {input_tokens + output_tokens:,} tokens"
            OpenRouterPrinter.print_section(content, f"Token Validation | Provider: OpenRouter | Model: {model.value}", "✅")
        else:
            content = f"❌ Token usage exceeds limits\n{error_msg}\nInput: {input_tokens:,} tokens\nOutput: {output_tokens:,} tokens\nTotal: {input_tokens + output_tokens:,} tokens"
            OpenRouterPrinter.print_section(content, f"Token Validation | Provider: OpenRouter | Model: {model.value}", "❌")
    
    @staticmethod
    def print_provider_summary() -> None:
        """
        Print a summary of all providers and their model counts
        """
        providers = ["openai", "anthropic", "google", "cohere", "meta", "mistral"]
        
        content_lines = []
        total_models = 0
        
        for provider_name in providers:
            models = OpenRouterModel.get_by_provider(provider_name)
            count = len(models)
            total_models += count
            content_lines.append(f"{provider_name.title():<12}: {count} models")
        
        content_lines.append(f"\n{'Total':<12}: {total_models} models")
        
        OpenRouterPrinter.print_section("\n".join(content_lines), "Provider Summary", "📊")
    
    @staticmethod
    def print_model_comparison(models: List[OpenRouterModel], metric: str = "max_total_tokens") -> None:
        """
        Print a comparison of models by a specific metric
        
        Args:
            models: List of models to compare
            metric: Metric to compare ('max_input_tokens', 'max_output_tokens', 'max_total_tokens')
        """
        if not models:
            OpenRouterPrinter.print_section("No models to compare", "Model Comparison", "📊")
            return
        
        # Sort models by the specified metric
        if metric == "max_input_tokens":
            sorted_models = sorted(models, key=lambda m: m.get_max_input_tokens(), reverse=True)
            metric_name = "Max Input Tokens"
        elif metric == "max_output_tokens":
            sorted_models = sorted(models, key=lambda m: m.get_max_output_tokens(), reverse=True)
            metric_name = "Max Output Tokens"
        else:  # max_total_tokens
            sorted_models = sorted(models, key=lambda m: m.get_max_total_tokens(), reverse=True)
            metric_name = "Max Total Tokens"
        
        # Create comparison table
        header = f"{'Rank':<4} {'Model':<35} {metric_name:<15} {'Provider':<12}"
        separator = "-" * 80
        
        model_lines = []
        for i, model in enumerate(sorted_models, 1):
            if metric == "max_input_tokens":
                value = f"{model.get_max_input_tokens():,}"
            elif metric == "max_output_tokens":
                value = f"{model.get_max_output_tokens():,}"
            else:
                value = f"{model.get_max_total_tokens():,}"
            
            model_lines.append(f"{i:<4} {model.value:<35} {value:<15} {model.get_provider():<12}")
        
        content = f"{header}\n{separator}\n" + "\n".join(model_lines)
        OpenRouterPrinter.print_section(content, f"Model Comparison by {metric_name}", "📊")
    
    @staticmethod
    def print_section(content: str, title: str, icon: str = "📋") -> None:
        """
        Print a generic section with title and content
        
        Args:
            content: Content to display
            title: Section title
            icon: Optional icon for the section
        """
        print("=" * 80)
        print(f"{icon} {title}")
        print("=" * 80)
        if content.strip():  # Only print content if it's not empty
            print(content)
        print("=" * 80)
        print()
    
    @staticmethod
    def print_config_info(client) -> None:
        """Print OpenRouter client configuration information."""
        config_info = f"""Provider: {client.provider.value}
Base URL: {client.config.base_url}
Default Model: {client.config.model}
Temperature: {client.config.temperature}
Max Tokens: {client.config.max_tokens}
Timeout: {client.config.timeout}
HTTP Referer: {client.config.additional_params.get('http_referer', 'N/A')}
X-Title: {client.config.additional_params.get('x_title', 'N/A')}"""
        OpenRouterPrinter.print_section(config_info, "OpenRouter Configuration", "🔧")
