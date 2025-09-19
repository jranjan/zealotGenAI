"""
LLM Printer
Comprehensive printing utilities for LLM applications
"""

from typing import Dict, Any


class LLMPrinter:
    """
    Comprehensive printer for LLM applications
    
    Provides static methods for printing prompts, responses, and LLM client information
    in a consistent, user-friendly format.
    """
    
    @staticmethod
    def print_prompt(prompt: str, title: str = "PROMPT") -> None:
        """
        Print a formatted prompt
        
        Args:
            prompt: The prompt text to display
            title: Optional title for the prompt section
        """
        print("=" * 80)
        print(f"📝 {title}")
        print("=" * 80)
        print(f"🤖 {prompt}")
        print()
    
    @staticmethod
    def print_response(response: str, title: str = "RESPONSE") -> None:
        """
        Print a formatted response
        
        Args:
            response: The response text to display
            title: Optional title for the response section
        """
        print("=" * 80)
        print(f"📤 {title}")
        print("=" * 80)
        print(f"{response}")
        print("=" * 80)
        print()
    
    @staticmethod
    def print_llm_client(client, selected_model: str, title: str = "LLM CLIENT INFO") -> None:
        """
        Print LLM client information
        
        Args:
            client: LLM client instance
            selected_model: Specific model name to highlight
            title: Optional title for the client info section
        """
        if client is None:
            print("=" * 80)
            print(f"❌ {title}")
            print("=" * 80)
            print("No LLM client provided")
            print("=" * 60)
            print()
            return
        
        print("=" * 80)
        print(f"🔧 {title}")
        print("=" * 80)
        
        try:
            # Get basic provider information
            provider_info = client.get_provider_info()
            print(f"Provider: {provider_info['display_name']}")
            
            # Show selected model prominently
            print(f"Selected Model: {selected_model}")
            print(f"Default Model: {provider_info['model']}")
            
            print(f"Temperature: {provider_info['temperature']}")
            print(f"Max Tokens: {provider_info['max_tokens']}")
            print(f"Base URL: {provider_info['base_url']}")
            
            # Try to get additional model attributes
            token_limits = client.get_token_limits()
            if token_limits:
                print(f"Max Input Tokens: {token_limits.get('max_input_tokens', 'N/A')}")
                print(f"Max Output Tokens: {token_limits.get('max_output_tokens', 'N/A')}")
                print(f"Max Total Tokens: {token_limits.get('max_total_tokens', 'N/A')}")
            
        except Exception as e:
            print(f"Error getting client info: {e}")
        
        print("=" * 80)
        print()
    
    @staticmethod
    def print_processing(message: str = "Processing...") -> None:
        """
        Print processing status
        
        Args:
            message: Processing message to display
        """
        print("🔄 PROCESSING...")
        print(f"   {message}")
    
    @staticmethod
    def print_success(message: str, client=None, model: str = None, title: str = "SUCCESS") -> None:
        """
        Print success message with client and model information
        
        Args:
            message: Success message to display
            client: LLM client instance (optional)
            model: Model name used (optional)
            title: Optional title for the success section
        """
        print("=" * 80)
        print(f"✅ {title}")
        print("=" * 80)
        
        # Add client and model information to the message if provided
        if client and model:
            try:
                provider_info = client.get_provider_info()
                print(f"{message} | Provider: {provider_info['display_name']} | Model: {model}")
            except Exception:
                # Fallback if client info is not available
                print(f"{message} | Model: {model}")
        elif model:
            print(f"{message} | Model: {model}")
        else:
            print(f"{message}")
        
        print("=" * 80)
        print()

    @staticmethod
    def print_error(error: str, title: str = "ERROR", client=None, model: str = None) -> None:
        """
        Print error message with client and model information
        
        Args:
            error: Error message to display
            title: Optional title for the error section
            client: LLM client instance (optional)
            model: Model name used (optional)
        """
        print("=" * 80)
        print(f"❌ {title}")
        print("=" * 80)
        
        # Add client and model information to the error message if provided
        if client and model:
            try:
                provider_info = client.get_provider_info()
                print(f"Error: {error} | Provider: {provider_info['display_name']} | Model: {model}")
            except Exception:
                # Fallback if client info is not available
                print(f"Error: {error} | Model: {model}")
        elif model:
            print(f"Error: {error} | Model: {model}")
        else:
            print(f"Error: {error}")
        
        print("=" * 80)
        print()
    
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
        print(content)
        print("=" * 80)
        print()
    
