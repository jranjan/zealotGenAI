"""
LLM App Class
"""

import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from zealot.utils.llm_client import create_llm_client, LLMProvider
from .base import App


class LLMApp(App):
    """
    Base class for LLM applications.
    
    Provides LLM functionality with lazy client initialization.
    """
    
    def __init__(self, provider: LLMProvider, name: str):
        """Initialize with LLM provider"""
        super().__init__(name)
        self.provider = provider
        self.client = None
    
    def _get_client(self):
        """Get or create the LLM client (lazy initialization)"""
        if self.client is None:
            self.client = create_llm_client(self.provider)
        return self.client
    
    def generate(self, message: str) -> str:
        """Generate response from LLM"""
        client = self._get_client()
        return client.generate_text(message)
    
    def run(self, message: str, show_processing: bool = True) -> str:
        """
        Run LLM generation with user-friendly formatted output
        
        Args:
            message: The input message to send to the LLM
            show_processing: Whether to show processing status messages
            
        Returns:
            The LLM response string, or None if error occurred
        """
        try:
            # Input section
            print("=" * 60)
            print("📝 INPUT")
            print("=" * 60)
            print(f"🤖 {message}")
            print()
            
            # Processing section
            if show_processing:
                print("🔄 PROCESSING...")
                print("   Calling LLM service...")
            
            response = self.generate(message)
            
            if show_processing:
                print("   ✅ Response received!")
                print()
            
            # Output section
            print("=" * 60)
            print("📤 OUTPUT")
            print("=" * 60)
            print(f"🤖 Response: {response}")
            print("=" * 60)
            print()
            
            return response
        except ImportError as e:
            error_msg = str(e)
            if "package not installed" in error_msg.lower():
                package_name = error_msg.split("Run: pip install ")[-1] if "Run: pip install" in error_msg else "the required package"
                print("=" * 60)
                print("❌ MISSING PACKAGE")
                print("=" * 60)
                print(f"Package: {package_name} is not installed")
                print(f"💡 Solution: Run 'pip install {package_name}' to install it")
                print("=" * 60)
                print()
            else:
                print("=" * 60)
                print("❌ IMPORT ERROR")
                print("=" * 60)
                print(f"Error: {error_msg}")
                print("=" * 60)
                print()
            return None
        except ValueError as e:
            print("=" * 60)
            print("❌ CONFIGURATION ERROR")
            print("=" * 60)
            print(f"Error: {e}")
            print("💡 Solution: Check your API key and configuration settings")
            print("=" * 60)
            print()
            return None
        except ConnectionError as e:
            print("=" * 60)
            print("❌ CONNECTION ERROR")
            print("=" * 60)
            print("Unable to connect to the LLM service")
            print(f"Details: {e}")
            print("💡 Solution: Check your internet connection and API endpoint")
            print("=" * 60)
            print()
            return None
        except Exception as e:
            print("=" * 60)
            print("❌ UNEXPECTED ERROR")
            print("=" * 60)
            print(f"Error: {e}")
            print("💡 Solution: Check the logs for more details or contact support")
            print("=" * 60)
            print()
            return None
