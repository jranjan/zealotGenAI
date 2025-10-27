"""
Dashboard Tabs Package
Contains all tab classes for the workflow pipeline
"""

from .base import BaseTab
from .analysis import OwnershipAnalyserTab
from .inventory import InventoryTab

__all__ = ['BaseTab', 'OwnershipAnalyserTab', 'InventoryTab']
