"""
Hello LLM App Runner
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
from zealot.utils.printer import LLMPrinter
from zealot.apps.llm.hello.app import LLMHelloApp


def main():
    """Main function demonstrating Hello LLM app functionality"""
    # Initialize the Hello LLM app
    app = LLMHelloApp(LLMProvider.COHERE, "Hello LLM App")
    
    print("=" * 80)
    print("🔧 CONFIGURATION SETUP")
    print("=" * 80)
    
    # Show configuration information
    try:
        client = app._get_client()
        # Get the default model from the client configuration
        provider_info = client.get_provider_info()
        LLMPrinter.print_llm_client(client, provider_info['model'])
    except Exception as e:
        LLMPrinter.print_error(f"Failed to initialize client or get provider info: {e}", "CONFIGURATION ERROR")
        return
    
    print("=" * 80)
    print("📝 PROMPT DEFINITION")
    print("=" * 80)
    
    # Define the prompt
    prompt = "How are you doing today?"
    LLMPrinter.print_prompt(prompt)
    
    print("=" * 80)
    print(f"🔑 API KEY VALIDATION | Provider: {provider_info['display_name']} | Model: {provider_info['model']}")
    print("=" * 80)
    
    # Check for API key
    api_key_env = f"{provider_info['provider'].upper()}_API_KEY"
    if not os.getenv(api_key_env):
        LLMPrinter.print_error(f"{api_key_env} environment variable is not set!\nTo run this demo, set your API key:\nexport {api_key_env}='your-api-key-here'")
        return
    
    print(f"✅ API key found and validated | Provider: {provider_info['display_name']} | Model: {provider_info['model']}")
    
    print("\n" + "=" * 80)
    print("🚀 LLM EXECUTION")
    print("=" * 80)
    
    # Execute the LLM
    try:
        # Processing section
        LLMPrinter.print_processing(f"Calling {provider_info['display_name']} API...")
        
        # Generate response
        response = app.run(prompt)
        
        # Success section
        LLMPrinter.print_success("Response received!", client, provider_info['model'])
        
        # Output section
        LLMPrinter.print_response(response)
        
    except ImportError as e:
        LLMPrinter.print_error(f"Package Error: {e}", "MISSING PACKAGE", client, provider_info['model'])
    except ValueError as e:
        LLMPrinter.print_error(f"Configuration Error: {e}", "CONFIGURATION ERROR", client, provider_info['model'])
    except ConnectionError as e:
        LLMPrinter.print_error(f"Connection Error: {e}", "CONNECTION ERROR", client, provider_info['model'])
    except Exception as e:
        LLMPrinter.print_error(f"Unexpected Error: {e}", "UNEXPECTED ERROR", client, provider_info['model'])


if __name__ == "__main__":
    main()
