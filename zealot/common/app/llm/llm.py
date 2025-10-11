"""
LLM App Class

This module provides the base LLMApp class for building Large Language Model
applications. It handles client initialization, text generation, and error
management for various LLM providers.
"""

from typing import Optional


class LLMApp:
    """
    Base class for LLM applications.
    
    This class provides a foundation for building applications that interact
    with Large Language Models. It handles client initialization, text generation,
    and comprehensive error management across different LLM providers.
    
    Features:
        - Lazy client initialization for better performance
        - Support for multiple LLM providers (OpenAI, Anthropic, Cohere, etc.)
        - Comprehensive error handling with user-friendly messages
        - Extensible design for custom LLM applications
        
    Attributes:
        client: The initialized LLM client (created by derived classes)
        name (str): The name of the application
    """
    
    def __init__(self, name: str):
        """
        Initialize the LLM application.
        
        Args:
            name (str): The name of the application for identification
        """
        self.name = name
        self.client = None
    
    def generate(self, message: str) -> str:
        """
        Generate response from the LLM.
        
        This method sends a message to the LLM and returns the generated response.
        It uses the client initialized by the derived class to perform the actual text generation.
        
        Args:
            message (str): The input message to send to the LLM
            
        Returns:
            str: The generated response from the LLM
            
        Raises:
            ValueError: When client is not initialized
        """
        if self.client is None:
            raise ValueError("Client not initialized. Derived class must initialize self.client")
        return self.client.generate(message)
    
    def run(self, message: str) -> str:
        """
        Run LLM generation with comprehensive error handling.
        
        This method provides the core functionality for running LLM generation
        with built-in error handling and user-friendly error messages. It
        catches various types of exceptions and provides helpful solutions.
        
        Args:
            message (str): The input message to send to the LLM
            
        Returns:
            str: The LLM response string
            
        Raises:
            ValueError: When there are configuration issues
            ConnectionError: When unable to connect to the LLM service
            Exception: For other unexpected errors
        """
        try:
            response = self.generate(message)
            return response
        except ValueError as e:
            raise ValueError(f"Error: {e}\n💡 Solution: Check your API key and configuration settings")
        except ConnectionError as e:
            raise ConnectionError(f"Unable to connect to the LLM service\nDetails: {e}\n💡 Solution: Check your internet connection and API endpoint")
        except Exception as e:
            raise Exception(f"Error: {e}\n💡 Solution: Check the logs for more details or contact support")