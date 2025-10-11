"""
LLM Cohere App
Simple LLM application using the new minimal client
"""

import sys
import os

# Add the specific directories to Python path
llm_path = os.path.join(os.path.dirname(__file__), '..', '..', 'common', 'clients', 'llm')
printer_path = os.path.join(os.path.dirname(__file__), '..', '..', 'utils', 'printer', 'llm')
app_path = os.path.join(os.path.dirname(__file__), '..', '..', 'common', 'app', 'llm')
sys.path.insert(0, llm_path)
sys.path.insert(0, printer_path)
sys.path.insert(0, app_path)

# Direct imports to avoid broken common package
from client import LLMClient
from factory import LLMClientFactory
from native import NativeLLMPrinter
from llm import LLMApp


class LLMCohereApp(LLMApp):
    """
    Simple LLM Cohere App using the new minimal LLM client.
    
    This app demonstrates the new minimal LLM client with Cohere provider.
    Inherits from LLMApp for proper inheritance structure.
    """
    
    def __init__(self, api_key: str = "${COHERE_API_KEY}"):
        """Initialize the Cohere LLM app."""
        super().__init__("LLM Cohere App")
        self.api_key = api_key
        self.client = LLMClientFactory.create(
            provider="cohere",
            api_key=api_key,
            model="command-a-03-2025",
            temperature=0.7
        )
    
    def run(self, message: str = "Tell me about India!"):
        """
        Run the Cohere LLM app.
        
        Args:
            message (str): The prompt to send to the LLM
        """
        formatted_message = f"Hello! {message}"
        
        try:
            # Get API key and validate
            api_key = self.client.get_api_key()
            print(f"✅ API Key validated for Cohere")
            
            # Print client info
            print(f"Provider: {self.client.provider} | Model: {self.client.model}")
            # Note: NativeLLMPrinter.print_llm_client expects different parameters
            # For now, just print basic info
            print(f"🔧 LLM CLIENT: {self.client.provider} | Model: {self.client.model}")
            
        except Exception as e:
            NativeLLMPrinter.print_error(f"Failed to initialize client: {e}", "CONFIGURATION ERROR")
            return
        
        NativeLLMPrinter.print_prompt(formatted_message)
        
        print("=" * 80)
        print(f"🔑 API KEY VALIDATION | Provider: {self.client.provider} | Model: {self.client.model}")
        print("=" * 80)
        
        try:
            print(f"✅ API key found and validated | Provider: {self.client.provider} | Model: {self.client.model}")
            
            print("\n" + "=" * 80)
            print("🚀 LLM EXECUTION")
            print("=" * 80)
            
            NativeLLMPrinter.print_processing(f"Calling {self.client.provider} API | Provider: {self.client.provider} | Model: {self.client.model}")
            
            # Generate response using inherited method
            response = self.generate(formatted_message)
            
            NativeLLMPrinter.print_success("Response received!", self.client, self.client.model)
            NativeLLMPrinter.print_response(response)
            
        except Exception as e:
            error_message = str(e)
            if "Environment variable" in error_message:
                NativeLLMPrinter.print_error(error_message, "API_KEY_ERROR")
            else:
                NativeLLMPrinter.print_error(f"API Error: {error_message}", "API_ERROR", self.client, self.client.model)


def main():
    """Main function demonstrating Cohere LLM app functionality"""
    # Initialize the Cohere LLM app
    app = LLMCohereApp()
    
    # Run the app
    app.run()


if __name__ == "__main__":
    main()