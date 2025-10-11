"""
System Directory constants for the AssetInsight application.

This module defines system directory-related constants used throughout the AssetInsight system.
"""

from enum import Enum


class SystemDirectory(Enum):
    """
    Enumeration of system directory constants.
    
    Contains system-wide directory constants used across the application.
    """

    STAGE_FOLDER = "__stage"
    
    def __str__(self) -> str:
        """
        Return the string representation of the system directory constant.
        
        Returns:
            The constant value as a string
        """
        return self.value
    
    @classmethod
    def get_stage_folder(cls) -> str:
        """
        Get the stage folder name.
        
        Returns:
            The stage folder name
        """
        return cls.STAGE_FOLDER.value
