"""
Asset Field Configuration Loader
"""

import yaml
from pathlib import Path
from typing import Dict, List, Optional, Union


class AssetFieldConfig:
    """Minimal configuration loader for asset field definitions."""
    
    def __init__(self, config_path: Union[str, Path]):
        self.config_path = Path(config_path)
        self._config_data: Optional[Dict] = None
        
        if not self.config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
        
        self._load_config()
    
    def _load_config(self) -> None:
        """Load the YAML configuration file."""
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                self._config_data = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML file: {e}")
    
    def get_asset_fields(self, asset_name: str) -> List[str]:
        """Get asset fields for a specific asset."""
        if not self._config_data or 'assets' not in self._config_data:
            return []
        
        for asset in self._config_data['assets']:
            if asset.get('name') == asset_name:
                return asset.get('asset_fields', [])
        
        return []
    
    def get_asset_names(self) -> List[str]:
        """Get all asset names."""
        if not self._config_data or 'assets' not in self._config_data:
            return []
        
        return [asset.get('name', '') for asset in self._config_data['assets']]