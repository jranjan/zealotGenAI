"""
Base App Class

This module provides the foundational App class that serves as the base
for all applications in the framework. It defines the common interface
and structure that all application types must follow.
"""

from abc import ABC, abstractmethod


class App(ABC):
    """
    Base class for all applications.
    
    This abstract base class provides a common interface and structure
    for all application types in the framework. It ensures that all
    applications have a consistent interface while allowing for
    specialized implementations.
    
    Features:
        - Abstract base class requiring implementation of core methods
        - Common initialization pattern with application naming
        - String representation for easy identification
        - Extensible design for various application types
        
    Attributes:
        name (str): The name of the application for identification
    """
    
    def __init__(self, name: str):
        """
        Initialize the application with a name.
        
        Args:
            name (str): The name of the application for identification
        """
        self.name = name
    
    @abstractmethod
    def run(self, *args, **kwargs):
        """
        Run the application - must be implemented by subclasses.
        
        This abstract method defines the core execution interface that
        all applications must implement. The specific implementation
        will vary based on the application type and requirements.
        
        Args:
            *args: Variable length argument list for application-specific parameters
            **kwargs: Arbitrary keyword arguments for application-specific options
            
        Returns:
            Application-specific return type (varies by implementation)
        """
        pass
    
    def __str__(self) -> str:
        """
        String representation of the application.
        
        Returns:
            str: The name of the application
        """
        return self.name
