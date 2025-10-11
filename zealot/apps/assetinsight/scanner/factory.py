"""
Scanner Factory - Factory pattern for creating scanner instances
"""

from enum import Enum
from typing import Dict, Any, Optional
from .base import Scanner
from .scanner import SourceScanner
from .supersonicscanner import SupersonicSourceScanner


class ScannerType(Enum):
    """Enumeration of available scanner types"""
    SOURCE = "source"
    SUPERSONIC_SOURCE_SCANNER = "supersonic_source_scanner"


class ScannerFactory:
    """Factory class for creating scanner instances"""
    
    @staticmethod
    def create_scanner(scanner_type: ScannerType, **kwargs) -> Scanner:
        """
        Create a scanner instance based on the specified type.
        
        Args:
            scanner_type: Type of scanner to create
            **kwargs: Additional arguments for scanner initialization
            
        Returns:
            Scanner instance
            
        Raises:
            ValueError: If scanner_type is not supported
        """
        if scanner_type == ScannerType.SOURCE:
            return SourceScanner()
        elif scanner_type == ScannerType.SUPERSONIC_SOURCE_SCANNER:
            max_workers = kwargs.get('max_workers', None)
            chunk_size = kwargs.get('chunk_size', 50)
            return SupersonicSourceScanner(max_workers=max_workers, chunk_size=chunk_size)
        else:
            raise ValueError(f"Unsupported scanner type: {scanner_type}")
    
    @staticmethod
    def get_available_types() -> Dict[str, str]:
        """
        Get available scanner types and their descriptions.
        
        Returns:
            Dictionary mapping type names to descriptions
        """
        return {
            ScannerType.SOURCE.value: "Standard single-threaded source scanner",
            ScannerType.SUPERSONIC_SOURCE_SCANNER.value: "Supersonic-high-performance multiprocessing source scanner"
        }
    
    @staticmethod
    def get_scanner_info(scanner_type: ScannerType) -> Dict[str, Any]:
        """
        Get detailed information about a specific scanner type.
        
        Args:
            scanner_type: Type of scanner to get info for
            
        Returns:
            Dictionary with scanner information
        """
        if scanner_type == ScannerType.SOURCE:
            return {
                'name': 'SourceScanner',
                'description': 'Standard single-threaded source data scanner',
                'performance': 'Single-threaded, suitable for small to medium datasets',
                'max_workers': 1,
                'chunk_size': 'N/A'
            }
        elif scanner_type == ScannerType.SUPERSONIC_SOURCE_SCANNER:
            return {
                'name': 'SupersonicSourceScanner',
                'description': 'Supersonic-high-performance multiprocessing scanner',
                'performance': 'Multiprocessing, suitable for large datasets with many files',
                'max_workers': 'Auto-detected (CPU count)',
                'chunk_size': 'Configurable (default: 50)'
            }
        else:
            raise ValueError(f"Unknown scanner type: {scanner_type}")
