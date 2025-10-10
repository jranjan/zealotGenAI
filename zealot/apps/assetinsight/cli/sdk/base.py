"""
Base CLI Class - Common functionality for all CLI operations
"""

import sys
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional, Dict, Any


class BaseCLI(ABC):
    """Abstract base class for all CLI operations"""
    
    def __init__(self):
        """Initialize the CLI"""
        self.processor = None  # Will be set by subclasses
    
    @abstractmethod
    def run_operation(self, *args, **kwargs) -> None:
        """
        Run the main operation. Must be implemented by subclasses.
        
        Args:
            *args: Positional arguments
            **kwargs: Keyword arguments
        """
        pass
    
    def validate_input(self, *args, **kwargs) -> bool:
        """
        Validate input parameters. Can be overridden by subclasses.
        
        Args:
            *args: Positional arguments to validate
            **kwargs: Keyword arguments to validate
            
        Returns:
            True if valid, False otherwise
        """
        return True
    
    def display_results(self, results: Dict[str, Any]) -> None:
        """
        Display operation results. Can be overridden by subclasses.
        
        Args:
            results: Results dictionary to display
        """
        if results.get('success', True):
            print("✅ Operation completed successfully!")
        else:
            print(f"❌ Operation failed: {results.get('error', 'Unknown error')}")
    
    def display_error(self, error: str) -> None:
        """
        Display error message and exit.
        
        Args:
            error: Error message to display
        """
        print(f"❌ Error: {error}")
        sys.exit(1)
    
    def display_usage(self, usage: str, example: str = None) -> None:
        """
        Display usage information and exit.
        
        Args:
            usage: Usage string
            example: Optional example string
        """
        print(f"Usage: {usage}")
        if example:
            print(f"Example: {example}")
        sys.exit(1)
    
    def validate_file_exists(self, file_path: str, description: str = "file") -> bool:
        """
        Validate that a file exists.
        
        Args:
            file_path: Path to file
            description: Description of the file for error messages
            
        Returns:
            True if file exists, False otherwise
        """
        if not file_path:
            self.display_error(f"{description.capitalize()} path is required")
            return False
        
        if not Path(file_path).exists():
            self.display_error(f"{description.capitalize()} does not exist: {file_path}")
            return False
        
        return True
    
    def validate_directory_exists(self, dir_path: str, description: str = "directory") -> bool:
        """
        Validate that a directory exists and is a directory.
        
        Args:
            dir_path: Path to directory
            description: Description of the directory for error messages
            
        Returns:
            True if directory exists and is valid, False otherwise
        """
        if not dir_path:
            self.display_error(f"{description.capitalize()} path is required")
            return False
        
        path = Path(dir_path)
        if not path.exists():
            self.display_error(f"{description.capitalize()} does not exist: {dir_path}")
            return False
        
        if not path.is_dir():
            self.display_error(f"{description.capitalize()} path is not a directory: {dir_path}")
            return False
        
        return True
    
    def create_output_directory(self, output_path: str) -> bool:
        """
        Create output directory if it doesn't exist.
        
        Args:
            output_path: Path to output directory
            
        Returns:
            True if successful, False otherwise
        """
        try:
            Path(output_path).mkdir(parents=True, exist_ok=True)
            return True
        except Exception as e:
            self.display_error(f"Failed to create output directory {output_path}: {str(e)}")
            return False
    
    def format_file_size(self, size_bytes: int) -> str:
        """
        Format file size in human-readable format.
        
        Args:
            size_bytes: Size in bytes
            
        Returns:
            Formatted size string
        """
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.1f} PB"
    
    def format_number(self, number: int) -> str:
        """
        Format number with commas for thousands.
        
        Args:
            number: Number to format
            
        Returns:
            Formatted number string
        """
        return f"{number:,}"
    
    def print_header(self, title: str, char: str = "=", width: int = 50) -> None:
        """
        Print a formatted header.
        
        Args:
            title: Header title
            char: Character to use for the line
            width: Width of the line
        """
        print(f"\n{title}")
        print(char * width)
    
    def print_section(self, title: str, char: str = "-", width: int = 30) -> None:
        """
        Print a formatted section header.
        
        Args:
            title: Section title
            char: Character to use for the line
            width: Width of the line
        """
        print(f"\n{title}")
        print(char * width)
    
    def print_table_row(self, *columns, widths: list = None) -> None:
        """
        Print a table row with specified column widths.
        
        Args:
            *columns: Column values
            widths: List of column widths
        """
        if widths:
            formatted_columns = []
            for i, column in enumerate(columns):
                width = widths[i] if i < len(widths) else 20
                formatted_columns.append(f"{str(column):<{width}}")
            print("  " + "  ".join(formatted_columns))
        else:
            print("  " + "  ".join(str(col) for col in columns))
