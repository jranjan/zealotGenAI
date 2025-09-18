"""
Base App Class
"""

from abc import ABC, abstractmethod


class App(ABC):
    """
    Base class for all applications.
    
    Provides a common interface for all app types in the framework.
    """
    
    def __init__(self, name: str):
        """Initialize the app with a name"""
        self.name = name
    
    @abstractmethod
    def run(self, *args, **kwargs):
        """Run the application - must be implemented by subclasses"""
        pass
    
    def __str__(self) -> str:
        return self.name
