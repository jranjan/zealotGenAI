"""
OpenRouter LLM App Runner
Separates app logic from runner logic
"""

import sys
from pathlib import Path
import os

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from zealot.utils.llm_client import LLMProvider
from zealot.utils.llm_client.clients.openrouter import OpenRouterModel
from zealot.utils.printer import OpenRouterPrinter, LLMPrinter
from zealot.apps.llm.openrouter.app import OpenRouterApp


def main():
    """Main function demonstrating OpenRouter app functionality"""
    # Initialize the OpenRouter app
    app = OpenRouterApp(LLMProvider.OPENROUTER, "OpenRouter Demo App")
    
    # 1. List all models supported by OpenRouter
    app.list_models()
    all_models = OpenRouterModel.get_all_models()
    
    # 2. Show OpenRouter configuration
    client = app._get_client()
    OpenRouterPrinter.print_config_info(client)
    
    # 3. Ask user to select a model by number
    # Ask user to pick a model
    while True:
        try:
            print("Please select a model by entering its number (1-{}):".format(len(all_models)))
            choice = input("Enter model number: ").strip()
            
            if not choice:
                print("❌ Please enter a number.")
                continue
                
            model_index = int(choice) - 1
            
            if 0 <= model_index < len(all_models):
                selected_model = all_models[model_index]
                model_name = selected_model.value
                print(f"✅ Selected: {model_name}")
                break
            else:
                print(f"❌ Please enter a number between 1 and {len(all_models)}")
                
        except ValueError:
            print("❌ Please enter a valid number.")
        except (KeyboardInterrupt, EOFError):
            print("\n❌ Selection cancelled.")
            return

    # Show information about the selected model
    app.show_model_info(model_name)
    
    print("\n" + "=" * 80)
    print("🔧 CONFIGURATION SETUP")
    print("=" * 80)
    
    # 4. Run the app for the specific model and print output
    # Use configuration from openrouter.json instead of hardcoded parameters
    client = app._get_client()
    config = client.config

    # Use the configuration parameters from openrouter.json
    # Flatten additional_params into the main parameters
    provider_params = {
        "temperature": config.temperature,
        "max_tokens": config.max_tokens,
        **(config.additional_params or {})
    }
    print(f"🔧 Using OpenRouter configuration: {provider_params}")
    
    print("\n" + "=" * 80)
    print("📝 PROMPT DEFINITION")
    print("=" * 80)
    
    # 5. Define LLM prompt
    prompt = "Explain the concept of artificial intelligence in simple terms and provide a practical example."
    print(f"🤖 {prompt}")
    
    print("\n" + "=" * 80)
    print("🚀 LLM EXECUTION")
    print("=" * 80)
    
    # 6. Invoke LLM with user-friendly output
    try:
        # Input section
        LLMPrinter.print_prompt(prompt)
        
        # Model information section
        LLMPrinter.print_llm_client(client, selected_model=model_name)
        
        # API key validation
        print("\n" + "=" * 80)
        print("🔑 API KEY VALIDATION")
        print("=" * 80)
        
        if not os.getenv('OPENROUTER_API_KEY'):
            OpenRouterPrinter.print_error("OPENROUTER_API_KEY environment variable is not set!\nTo run this demo, set your API key:\nexport OPENROUTER_API_KEY='your-api-key-here'")
            return
        
        print("✅ API key found and validated")
        
        # Processing section
        LLMPrinter.print_processing("Calling OpenRouter API...")
        
        # Generate response
        response = app.run(prompt, model_name, provider_params=provider_params)
        
        # Success section
        LLMPrinter.print_success("Response received!")
        
        # Output section
        LLMPrinter.print_response(response)
        
        print("\n" + "=" * 80)
        print("✅ EXECUTION COMPLETE")
        print("=" * 80)
        print("✅ OpenRouter app execution completed successfully!")
    except ImportError as e:
        OpenRouterPrinter.print_error(f"Package Error: {e}", "MISSING PACKAGE")
    except ValueError as e:
        OpenRouterPrinter.print_error(f"Configuration Error: {e}", "CONFIGURATION ERROR")
    except ConnectionError as e:
        OpenRouterPrinter.print_error(f"Connection Error: {e}", "CONNECTION ERROR")
    except Exception as e:
        OpenRouterPrinter.print_error(f"Unexpected Error: {e}", "UNEXPECTED ERROR")


if __name__ == "__main__":
    main()
