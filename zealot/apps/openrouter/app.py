"""
OpenRouter LLM App
LLM application with provider and model selection using minimal LLMClient
"""

import sys
import os

# Add the specific directories to Python path
llm_path = os.path.join(os.path.dirname(__file__), '..', '..', 'common', 'clients', 'llm')
printer_path = os.path.join(os.path.dirname(__file__), '..', '..', 'utils', 'printer', 'llm')
app_path = os.path.join(os.path.dirname(__file__), '..', '..', 'common', 'app', 'llm')
catalog_path = os.path.join(os.path.dirname(__file__), '..', '..', 'common', 'catalog', 'llm')
sys.path.insert(0, llm_path)
sys.path.insert(0, printer_path)
sys.path.insert(0, app_path)
sys.path.insert(0, catalog_path)

# Direct imports
from client import LLMClient
from factory import LLMClientFactory
from native import NativeLLMPrinter
from llm import LLMApp
from catalog import LLMModelCatalog


class OpenRouterApp(LLMApp):
    """OpenRouter LLM App with provider and model selection using minimal LLMClient"""
    
    def __init__(self, api_key: str = "${OPENROUTER_API_KEY}"):
        """Initialize the OpenRouter LLM app."""
        super().__init__("OpenRouter LLM App")
        self.api_key = api_key
        self.current_provider = None
        self.current_model = None
        self.client = None
    
    def set_provider_and_model(self, provider: str, model: str, **kwargs):
        """Set the provider and model for the client."""
        self.current_provider = provider
        self.current_model = model
        
        # Create client with selected provider and model
        self.client = LLMClientFactory.create(
            provider=provider,
            api_key=self.api_key,
            model=model,
            **kwargs
        )
        
        print(f"✅ Client configured: Provider: {provider} | Model: {model}")
    
    def run(self, message: str, provider: str = None, model: str = None, **kwargs):
        """
        Run the app with provider and model selection.
        
        Args:
            message (str): The prompt to send to the LLM
            provider (str): The provider to use (e.g., 'openai', 'anthropic', 'cohere')
            model (str): The model to use (e.g., 'gpt-4', 'claude-3-sonnet-20240229')
            **kwargs: Additional parameters for the client
        """
        if provider and model:
            self.set_provider_and_model(provider, model, **kwargs)
        elif self.client is None:
            # Default to OpenRouter with a common model
            self.set_provider_and_model("openrouter", "openai/gpt-3.5-turbo", **kwargs)
        
        formatted_message = f"Hello! {message}"
        
        try:
            # Get API key and validate
            api_key = self.client.get_api_key()
            print(f"✅ API Key validated for {self.current_provider}")
            
            # Print client info
            print(f"Provider: {self.current_provider} | Model: {self.current_model}")
            print(f"🔧 LLM CLIENT: {self.current_provider} | Model: {self.current_model}")
            
        except Exception as e:
            NativeLLMPrinter.print_error(f"Failed to initialize client: {e}", "CONFIGURATION ERROR")
            return
        
        NativeLLMPrinter.print_prompt(formatted_message)
        
        print("=" * 80)
        print(f"🔑 API KEY VALIDATION | Provider: {self.current_provider} | Model: {self.current_model}")
        print("=" * 80)
        
        try:
            print(f"✅ API key found and validated | Provider: {self.current_provider} | Model: {self.current_model}")
            
            print("\n" + "=" * 80)
            print("🚀 LLM EXECUTION")
            print("=" * 80)
            
            NativeLLMPrinter.print_processing(f"Calling {self.current_provider} API | Provider: {self.current_provider} | Model: {self.current_model}")
            
            # Generate response using inherited method
            response = self.generate(formatted_message)
            
            NativeLLMPrinter.print_success("Response received!", self.client, self.current_model)
            NativeLLMPrinter.print_response(response)
            
        except Exception as e:
            error_message = str(e)
            if "Environment variable" in error_message:
                NativeLLMPrinter.print_error(error_message, "API_KEY_ERROR")
            else:
                NativeLLMPrinter.print_error(f"API Error: {error_message}", "API_ERROR", self.client, self.current_model)
    
    def list_models(self):
        """List available OpenRouter models with token limits using catalog"""
        models = LLMModelCatalog.get_all_models()
        print("\n📋 Available OpenRouter Models (from catalog):")
        print("=" * 80)
        
        # Group by provider for better display
        providers = {}
        for model in models:
            provider = model.get_provider()
            if provider not in providers:
                providers[provider] = []
            providers[provider].append(model)
        
        for provider, provider_models in providers.items():
            print(f"\n🔹 {provider.title()} Models:")
            for model in provider_models:
                info = model.get_token_info()
                print(f"  • {info['model']} | Max Input: {info['max_input_tokens_formatted']} | Max Output: {info['max_output_tokens_formatted']}")
        
        return [model.value for model in models]
    
    def list_models_by_provider(self, provider: str = None):
        """List models grouped by provider or for a specific provider"""
        if provider:
            models = LLMModelCatalog.get_by_provider(provider)
            print(f"\n📋 {provider.title()} Models:")
            print("=" * 80)
            for model in models:
                info = model.get_token_info()
                print(f"• {info['model']} | Max Input: {info['max_input_tokens_formatted']} | Max Output: {info['max_output_tokens_formatted']}")
            return [model.value for model in models]
        else:
            providers = ["openai", "anthropic", "google", "cohere", "meta", "mistral"]
            print("\n📋 All Models by Provider:")
            print("=" * 80)
            for prov in providers:
                models = LLMModelCatalog.get_by_provider(prov)
                if models:
                    print(f"\n{prov.title()}:")
                    for model in models:
                        info = model.get_token_info()
                        print(f"  • {info['model']} | Max Input: {info['max_input_tokens_formatted']} | Max Output: {info['max_output_tokens_formatted']}")
            return [model.value for model in LLMModelCatalog.get_all_models()]
    
    def show_model_info(self, model_name: str):
        """Show detailed information for a specific model"""
        try:
            model = LLMModelCatalog.from_string(model_name)
            info = model.get_token_info()
            print(f"\n📊 Model Information:")
            print("=" * 80)
            print(f"Provider: {info['provider']}")
            print(f"Model: {info['model']}")
            print(f"Max Input Tokens: {info['max_input_tokens_formatted']}")
            print(f"Max Output Tokens: {info['max_output_tokens_formatted']}")
            print(f"Max Total Tokens: {info['max_total_tokens_formatted']}")
        except ValueError as e:
            print(f"❌ Error: {e}")
    
    def validate_request(self, model_name: str, input_tokens: int, output_tokens: int):
        """Validate token usage for a specific model using catalog"""
        try:
            model = LLMModelCatalog.from_string(model_name)
            is_valid, error = model.validate_tokens(input_tokens, output_tokens)
            if is_valid:
                print(f"✅ Token validation passed for {model_name}")
                print(f"   Input: {input_tokens:,} | Output: {output_tokens:,} | Total: {input_tokens + output_tokens:,}")
            else:
                print(f"❌ Token validation failed for {model_name}: {error}")
        except ValueError as e:
            print(f"❌ Error: {e}")
    
    def demonstrate_catalog_features(self):
        """Demonstrate various catalog features"""
        print("\n🚀 Catalog Features Demonstration")
        print("=" * 80)
        
        # 1. Get all models
        all_models = LLMModelCatalog.get_all_models()
        print(f"📊 Total models in catalog: {len(all_models)}")
        
        # 2. Find models by capability (high token limits)
        high_token_models = [model for model in all_models if model.get_max_input_tokens() > 100000]
        print(f"🔍 Models with >100K input tokens: {len(high_token_models)}")
        for model in high_token_models:
            info = model.get_token_info()
            print(f"  • {info['model']} ({info['max_input_tokens_formatted']} input)")
        
        # 3. Compare models by provider
        print(f"\n📈 Models per provider:")
        providers = {}
        for model in all_models:
            provider = model.get_provider()
            providers[provider] = providers.get(provider, 0) + 1
        
        for provider, count in sorted(providers.items()):
            print(f"  • {provider.title()}: {count} models")
        
        # 4. Find best model for specific use case
        print(f"\n🎯 Best models for different use cases:")
        
        # High output tokens
        high_output = max(all_models, key=lambda m: m.get_max_output_tokens())
        print(f"  • Highest output tokens: {high_output.value} ({high_output.get_max_output_tokens():,})")
        
        # High input tokens
        high_input = max(all_models, key=lambda m: m.get_max_input_tokens())
        print(f"  • Highest input tokens: {high_input.value} ({high_input.get_max_input_tokens():,})")
        
        # 5. Model search by name pattern
        print(f"\n🔍 Model search examples:")
        gpt_models = [model for model in all_models if "gpt" in model.value.lower()]
        print(f"  • GPT models: {[m.value for m in gpt_models]}")
        
        claude_models = [model for model in all_models if "claude" in model.value.lower()]
        print(f"  • Claude models: {[m.value for m in claude_models]}")


def interactive_loop():
    """Interactive loop for choosing provider, model, and prompt"""
    print("🤖 Interactive OpenRouter LLM App")
    print("=" * 50)
    print("Choose provider → model → prompt → repeat until 'exit'")
    print("=" * 50)
    
    # Initialize the OpenRouter LLM app
    app = OpenRouterApp()
    
    # Get all providers and models from catalog
    all_models = LLMModelCatalog.get_all_models()
    providers = list(set(model.get_provider() for model in all_models))
    providers.sort()
    
    while True:
        try:
            print(f"\n📋 Available Providers:")
            for i, provider in enumerate(providers, 1):
                model_count = len(LLMModelCatalog.get_by_provider(provider))
                print(f"  {i}. {provider.title()} ({model_count} models)")
            print(f"  {len(providers) + 1}. List all models")
            print(f"  {len(providers) + 2}. Exit")
            
            # Get provider choice
            choice = input(f"\n🔹 Choose provider (1-{len(providers) + 2}): ").strip()
            
            if choice == str(len(providers) + 2) or choice.lower() in ['exit', 'quit', 'q']:
                print("👋 Goodbye!")
                break
            elif choice == str(len(providers) + 1):
                # List all models
                print(f"\n📊 All Models in Catalog:")
                for provider in providers:
                    models = LLMModelCatalog.get_by_provider(provider)
                    if models:
                        print(f"\n🔹 {provider.title()}:")
                        for model in models:
                            info = model.get_token_info()
                            print(f"  • {info['model']} | Max Input: {info['max_input_tokens_formatted']} | Max Output: {info['max_output_tokens_formatted']}")
                continue
            
            try:
                provider_index = int(choice) - 1
                if 0 <= provider_index < len(providers):
                    selected_provider = providers[provider_index]
                else:
                    print("❌ Invalid choice. Please try again.")
                    continue
            except ValueError:
                print("❌ Please enter a valid number.")
                continue
            
            # Get models for selected provider
            provider_models = LLMModelCatalog.get_by_provider(selected_provider)
            print(f"\n📋 {selected_provider.title()} Models:")
            for i, model in enumerate(provider_models, 1):
                info = model.get_token_info()
                print(f"  {i}. {info['model']} | Max Input: {info['max_input_tokens_formatted']} | Max Output: {info['max_output_tokens_formatted']}")
            
            # Get model choice
            model_choice = input(f"\n🔹 Choose model (1-{len(provider_models)}): ").strip()
            
            try:
                model_index = int(model_choice) - 1
                if 0 <= model_index < len(provider_models):
                    selected_model = provider_models[model_index].value
                else:
                    print("❌ Invalid choice. Please try again.")
                    continue
            except ValueError:
                print("❌ Please enter a valid number.")
                continue
            
            # Get prompt
            print(f"\n💬 Enter your prompt (or 'back' to choose different model):")
            prompt = input("🔹 Prompt: ").strip()
            
            if prompt.lower() in ['back', 'b']:
                continue
            elif not prompt:
                print("❌ Please enter a prompt.")
                continue
            
            # Show model info
            model_info = LLMModelCatalog.from_string(selected_model).get_token_info()
            print(f"\n📊 Selected: {model_info['provider'].title()} - {model_info['model']}")
            print(f"   Max Input: {model_info['max_input_tokens_formatted']} | Max Output: {model_info['max_output_tokens_formatted']}")
            
            # Run the LLM
            print(f"\n🚀 Generating response...")
            print("=" * 50)
            app.run(prompt, provider="openrouter", model=selected_model)
            print("=" * 50)
            
        except KeyboardInterrupt:
            print("\n\n👋 Goodbye!")
            break
        except Exception as e:
            print(f"❌ Error: {e}")
            print("Please try again.")


def main():
    """Main function with choice between interactive and demo modes"""
    print("🤖 OpenRouter LLM App")
    print("=" * 30)
    print("1. Interactive Mode (choose provider → model → prompt)")
    print("2. Demo Mode (show examples)")
    
    choice = input("\n🔹 Choose mode (1-2): ").strip()
    
    if choice == "1":
        interactive_loop()
    elif choice == "2":
        demo_mode()
    else:
        print("❌ Invalid choice. Starting interactive mode...")
        interactive_loop()


def demo_mode():
    """Demo mode showing catalog features"""
    print("🚀 Demo Mode - Catalog Features")
    print("=" * 50)
    
    # Initialize the OpenRouter LLM app
    app = OpenRouterApp()
    
    # Get models from catalog
    all_models = LLMModelCatalog.get_all_models()
    
    # Example 1: Use with OpenAI via OpenRouter (from catalog)
    print("🚀 Example 1: OpenAI GPT-4 via OpenRouter")
    gpt4o_model = next((model for model in all_models if "gpt-4o" in model.value), None)
    if gpt4o_model:
        app.run("Tell me about machine learning", provider="openrouter", model=gpt4o_model.value)
    else:
        print("❌ GPT-4o model not found in catalog")
    
    print("\n" + "="*100 + "\n")
    
    # Example 2: Use with Cohere via OpenRouter (from catalog)
    print("🚀 Example 2: Cohere Command via OpenRouter")
    cohere_model = next((model for model in all_models if "command-a" in model.value), None)
    if cohere_model:
        app.run("Explain quantum computing", provider="openrouter", model=cohere_model.value)
    else:
        print("❌ Cohere Command model not found in catalog")
    
    print("\n" + "="*100 + "\n")
    
    # Example 3: List available models by provider (using catalog)
    print("🚀 Example 3: List Models by Provider")
    providers = ["openai", "anthropic", "cohere", "google", "meta", "mistral"]
    for provider in providers:
        models = LLMModelCatalog.get_by_provider(provider)
        if models:
            print(f"\n📋 {provider.title()} Models:")
            for model in models:
                info = model.get_token_info()
                print(f"  • {info['model']} | Max Input: {info['max_input_tokens_formatted']} | Max Output: {info['max_output_tokens_formatted']}")
    
    print("\n" + "="*100 + "\n")
    
    # Example 4: Model validation using catalog
    print("🚀 Example 4: Model Validation")
    test_model = LLMModelCatalog.from_string("openai/gpt-4o")
    is_valid, error = test_model.validate_tokens(1000, 500)
    if is_valid:
        print(f"✅ Token validation passed for {test_model.value}")
    else:
        print(f"❌ Token validation failed: {error}")
    
    print("\n" + "="*100 + "\n")
    
    # Example 5: Model information display
    print("🚀 Example 5: Model Information")
    for model in all_models[:3]:  # Show first 3 models
        info = model.get_token_info()
        print(f"\n📊 {info['provider'].title()} - {info['model']}")
        print(f"   Max Input: {info['max_input_tokens_formatted']}")
        print(f"   Max Output: {info['max_output_tokens_formatted']}")
        print(f"   Max Total: {info['max_total_tokens_formatted']}")
    
    print("\n" + "="*100 + "\n")
    
    # Example 6: Catalog features demonstration
    print("🚀 Example 6: Catalog Features")
    app.demonstrate_catalog_features()


if __name__ == "__main__":
    main()