"""
SchemaGuide class for database schema management.
"""

from typing import Dict, List, Any
from pathlib import Path
import yaml


class SchemaGuide:
    """Bare minimum schema management class."""
    
    def __init__(self, reader_instance=None):
        self.reader = reader_instance
    
    def get_assets_table_schema(self, yaml_path: str = None) -> Dict[str, Any]:
        """Get table schema for assets table by reading assets.yaml directly."""
        try:
            # Use provided path or default to config/asset.yaml
            if yaml_path is None:
                yaml_path = self._get_default_config_path()
            
            yaml_file = Path(yaml_path)
            if not yaml_file.exists():
                return {}
            
            with open(yaml_file, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f)
            
            if not data or 'assets' not in data or 'common' not in data['assets']:
                return {}
            
            # Create schema with columns from common section within assets
            schema = {
                'table_name': 'assets',
                'columns': []
            }
            
            for i, (column_name, data_type) in enumerate(data['assets']['common'].items(), 1):
                schema['columns'].append({
                    'column_name': column_name,
                    'data_type': data_type,
                    'is_nullable': 'YES',
                    'column_default': None,
                    'ordinal_position': i
                })
            
            return schema
        except Exception:
            return {}
    
    # Removed get_asset_names() - not needed since we only have common fields
    
    def _get_default_config_path(self) -> str:
        """Get the default path to asset.yaml configuration file."""
        # Look for config/asset.yaml relative to this file
        config_path = Path(__file__).parent.parent.parent / "config" / "asset.yaml"
        return str(config_path)
