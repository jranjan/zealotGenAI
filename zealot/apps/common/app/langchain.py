"""
Langchain App Class
"""

import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from .base import App


class LangchainApp(App):
    """
    Base class for Langchain applications.
    
    Provides foundation for Langchain-based LLM applications.
    """
    
    def __init__(self, name: str):
        """Initialize Langchain app"""
        super().__init__(name)
        # Langchain-specific initialization would go here
    
    def run(self, *args, **kwargs):
        """Run the Langchain app - to be implemented by subclasses"""
        raise NotImplementedError("Subclasses must implement run method")
