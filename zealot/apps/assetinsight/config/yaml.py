"""
YAML Configuration Loader for Asset Insight

This module provides a configuration loader that reads YAML files
and provides easy access to configuration values.
"""

import yaml
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass


@dataclass
class AssetConfigItem:
    """Represents a single asset configuration item."""
    name: str
    source_id: str
    result_id: str
    asset_fields: List[str] = None
    cloud_fields: List[str] = None


class AssetConfig:
    """
    Configuration loader for Asset Insight YAML files.
    
    This class loads YAML configuration files and provides easy access
    to configuration values with type safety and validation.
    """
    
    def __init__(self, config_path: Union[str, Path]):
        """
        Initialize the configuration loader.
        
        Args:
            config_path: Path to the YAML configuration file
        """
        self.config_path = Path(config_path)
        self._config_data: Optional[Dict[str, Any]] = None
        self._assets: Optional[List[AssetConfigItem]] = None
        
        if not self.config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
        
        self._load_config()
    
    def _load_config(self) -> None:
        """Load the YAML configuration file."""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                self._config_data = yaml.safe_load(f)
            
            # Parse assets if they exist
            if self._config_data and 'assets' in self._config_data:
                self._assets = [
                    AssetConfigItem(
                        name=asset.get('name', ''),
                        source_id=asset.get('source_id', ''),
                        result_id=asset.get('result_id', ''),
                        asset_fields=asset.get('asset_fields', []),
                        cloud_fields=asset.get('cloud_fields', [])
                    )
                    for asset in self._config_data['assets']
                ]
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML file: {e}")
        except Exception as e:
            raise RuntimeError(f"Error loading configuration: {e}")
    
    def get_config_value(self, key: str, default: Any = None) -> Any:
        """
        Get a configuration value by key.
        
        Args:
            key: Configuration key (supports dot notation for nested keys)
            default: Default value if key not found
            
        Returns:
            Configuration value or default
        """
        if not self._config_data:
            return default
        
        keys = key.split('.')
        current = self._config_data
        
        for k in keys:
            if isinstance(current, dict) and k in current:
                current = current[k]
            else:
                return default
        
        return current
    
    def get_assets(self) -> List[AssetConfigItem]:
        """
        Get all asset configurations.
        
        Returns:
            List of AssetConfigItem objects
        """
        return self._assets or []
    
    def get_asset_by_name(self, name: str) -> Optional[AssetConfigItem]:
        """
        Get asset configuration by name.
        
        Args:
            name: Asset name to search for
            
        Returns:
            AssetConfigItem if found, None otherwise
        """
        if not self._assets:
            return None
        
        for asset in self._assets:
            if asset.name == name:
                return asset
        
        return None
    
    def get_asset_by_source_id(self, source_id: str) -> Optional[AssetConfigItem]:
        """
        Get asset configuration by source_id.
        
        Args:
            source_id: Source ID to search for
            
        Returns:
            AssetConfigItem if found, None otherwise
        """
        if not self._assets:
            return None
        
        for asset in self._assets:
            if asset.source_id == source_id:
                return asset
        
        return None
    
    def get_asset_by_result_id(self, result_id: str) -> Optional[AssetConfigItem]:
        """
        Get asset configuration by result_id.
        
        Args:
            result_id: Result ID to search for
            
        Returns:
            AssetConfigItem if found, None otherwise
        """
        if not self._assets:
            return None
        
        for asset in self._assets:
            if asset.result_id == result_id:
                return asset
        
        return None
    
    def get_source_paths(self) -> List[str]:
        """
        Get all source paths from asset configurations.
        
        Returns:
            List of source paths
        """
        if not self._assets:
            return []
        
        return [asset.source_id for asset in self._assets]
    
    def get_result_paths(self) -> List[str]:
        """
        Get all result paths from asset configurations.
        
        Returns:
            List of result paths
        """
        if not self._assets:
            return []
        
        return [asset.result_id for asset in self._assets]
    
    def get_asset_names(self) -> List[str]:
        """
        Get all asset names.
        
        Returns:
            List of asset names
        """
        if not self._assets:
            return []
        
        return [asset.name for asset in self._assets]
    
    def reload(self) -> None:
        """Reload the configuration from file."""
        self._load_config()
    
    def is_valid(self) -> bool:
        """
        Check if the configuration is valid.
        
        Returns:
            True if configuration is valid, False otherwise
        """
        return self._config_data is not None and self._assets is not None
    
    def get_asset_fields(self, asset_name: str) -> List[str]:
        """
        Get asset fields for a specific asset.
        
        Args:
            asset_name: Name of the asset
            
        Returns:
            List of asset field names
        """
        asset = self.get_asset_by_name(asset_name)
        return asset.asset_fields if asset else []
    
    def get_cloud_fields(self, asset_name: str) -> List[str]:
        """
        Get cloud fields for a specific asset.
        
        Args:
            asset_name: Name of the asset
            
        Returns:
            List of cloud field names
        """
        asset = self.get_asset_by_name(asset_name)
        return asset.cloud_fields if asset else []
    
    def get_summary(self) -> Dict[str, Any]:
        """
        Get a summary of the configuration.
        
        Returns:
            Dictionary with configuration summary
        """
        if not self._assets:
            return {
                'total_assets': 0,
                'asset_names': [],
                'source_paths': [],
                'result_paths': []
            }
        
        return {
            'total_assets': len(self._assets),
            'asset_names': [asset.name for asset in self._assets],
            'source_paths': [asset.source_id for asset in self._assets],
            'result_paths': [asset.result_id for asset in self._assets]
        }