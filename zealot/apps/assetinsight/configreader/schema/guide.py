"""
SchemaGuide class for database schema management.
"""

from typing import Dict, List, Any
from pathlib import Path
import yaml
from common.asset_class import AssetClass


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
            
            if not data or 'common_fields' not in data:
                return {}
            
            # Create schema with columns from common_fields section
            schema = {
                'table_name': 'assets',
                'columns': []
            }
            
            for i, (column_name, data_type) in enumerate(data['common_fields'].items(), 1):
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
    
    def get_all_table_schemas(self, yaml_path: str = None) -> Dict[str, Dict[str, Any]]:
        """Get schemas for all tables defined in the YAML file and AssetClass enum."""
        try:
            # Use provided path or default to config/asset.yaml
            if yaml_path is None:
                yaml_path = self._get_default_config_path()
            
            yaml_file = Path(yaml_path)
            if not yaml_file.exists():
                # If YAML doesn't exist, use AssetClass enum to generate schemas
                return self._generate_schemas_from_asset_class()
            
            with open(yaml_file, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f)
            
            if not data or 'common_fields' not in data:
                # If no common_fields, use AssetClass enum to generate schemas
                return self._generate_schemas_from_asset_class()
            
            schemas = {}
            
            # Get common fields
            common_fields = data['common_fields']
            
            # Get table names from YAML or AssetClass
            if 'tables' in data and data['tables']:
                table_names = list(data['tables'].keys())
            else:
                # Use AssetClass to get table names
                table_names = AssetClass.get_all_table_names()
            
            # Create schema for each table
            for table_name in table_names:
                schema = {
                    'table_name': table_name,
                    'columns': []
                }
                
                # Add common fields to each table
                for i, (column_name, data_type) in enumerate(common_fields.items(), 1):
                    schema['columns'].append({
                        'column_name': column_name,
                        'data_type': data_type,
                        'is_nullable': 'YES',
                        'column_default': None,
                        'ordinal_position': i
                    })
                
                schemas[table_name] = schema
            
            return schemas
        except Exception:
            # Fallback to AssetClass enum
            return self._generate_schemas_from_asset_class()
    
    def _generate_schemas_from_asset_class(self) -> Dict[str, Dict[str, Any]]:
        """Generate schemas from AssetClass when YAML is not available."""
        try:
            schemas = {}
            
            # Get all unique table names from AssetClass
            table_names = AssetClass.get_all_table_names()
            
            # Default common fields (fallback)
            default_fields = {
                'id': 'VARCHAR',
                'name': 'VARCHAR',
                'identifier': 'VARCHAR',
                'createdDate': 'VARCHAR',
                'lastModifiedDate': 'VARCHAR',
                'assetClass': 'VARCHAR',
                'startDate': 'VARCHAR',
                'endDate': 'VARCHAR',
                'lastSeenDate': 'VARCHAR',
                'status': 'VARCHAR',
                'accountId': 'VARCHAR',
                'deleted': 'VARCHAR',
                'parent_cloud': 'VARCHAR',
                'parent_cloud_id': 'VARCHAR',
                'parent_cloud_owner_email': 'VARCHAR',
                'cloud': 'VARCHAR',
                'cloud_id': 'VARCHAR',
                'cloud_owner_email': 'VARCHAR',
                'team': 'VARCHAR',
                'team_id': 'VARCHAR',
                'team_owner_email': 'VARCHAR',
                'properties': 'JSON',
                'tags': 'JSON',
                'raw_data': 'JSON'
            }
            
            # Create schema for each table
            for table_name in table_names:
                schema = {
                    'table_name': table_name,
                    'columns': []
                }
                
                # Add common fields to each table
                for i, (column_name, data_type) in enumerate(default_fields.items(), 1):
                    schema['columns'].append({
                        'column_name': column_name,
                        'data_type': data_type,
                        'is_nullable': 'YES',
                        'column_default': None,
                        'ordinal_position': i
                    })
                
                schemas[table_name] = schema
            
            return schemas
        except Exception:
            return {}
    
    def _get_default_config_path(self) -> str:
        """Get the default path to asset.yaml configuration file."""
        # Look for config/asset.yaml relative to this file
        config_path = Path(__file__).parent.parent.parent / "config" / "asset.yaml"
        return str(config_path)
