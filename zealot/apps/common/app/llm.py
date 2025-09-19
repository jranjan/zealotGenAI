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
    
    def run(self, message: str) -> str:
        """
        Run LLM generation (core functionality only)
        
        Args:
            message: The input message to send to the LLM
            
        Returns:
            The LLM response string, or None if error occurred
        """
        try:
            response = self.generate(message)
            return response
        except ImportError as e:
            error_msg = str(e)
            if "package not installed" in error_msg.lower():
                package_name = error_msg.split("Run: pip install ")[-1] if "Run: pip install" in error_msg else "the required package"
                raise ImportError(f"Package: {package_name} is not installed\n💡 Solution: Run 'pip install {package_name}' to install it")
            else:
                raise ImportError(error_msg)
        except ValueError as e:
            raise ValueError(f"Error: {e}\n💡 Solution: Check your API key and configuration settings")
        except ConnectionError as e:
            raise ConnectionError(f"Unable to connect to the LLM service\nDetails: {e}\n💡 Solution: Check your internet connection and API endpoint")
        except Exception as e:
            raise Exception(f"Error: {e}\n💡 Solution: Check the logs for more details or contact support")
