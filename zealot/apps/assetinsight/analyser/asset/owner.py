"""
Owner Asset Analyser - Concrete implementation for owner-specific asset analysis.

This analyser focuses on ownership, team assignments, and resource attribution
aspects of asset data using DuckDB for efficient querying.
"""

from typing import Any, Dict, List
from .asset import AssetAnalyser
from common.asset_class import AssetClass


class OwnerAnalyser(AssetAnalyser):
    """
    Concrete implementation of AssetAnalyser for owner-specific analysis.
    
    This analyser focuses on ownership, team assignments, and resource attribution
    aspects of asset data.
    """
    
    def __init__(self):
        """Initialize the owner analyser."""
        super().__init__("owner")
    
    def _create_union_query(self, base_query: str) -> str:
        """Create a UNION query across all asset tables"""
        table_names = self._get_existing_database_tables()
        
        print(f"🔍 Creating UNION query across tables: {table_names}")
        
        # Create UNION query for each table
        union_parts = []
        for table_name in table_names:
            # Replace 'FROM assets' with 'FROM {table_name}' in the query
            table_query = base_query.replace('FROM assets', f'FROM {table_name}')
            union_parts.append(f"({table_query})")
        
        union_query = " UNION ALL ".join(union_parts)
        print(f"🔍 Generated UNION query: {union_query[:200]}...")  # Show first 200 chars
        
        return union_query
    
    def analyse(self, source_directory: str, result_directory: str) -> Dict[str, Any]:
        """
        Analyze assets for ownership analysis.
        
        Args:
            source_directory: Path to source directory
            result_directory: Path to result directory
            
        Returns:
            Analysis results dictionary
        """
        try:
            self.create_reader(source_directory)
            summary = self.get_ownership_summary()
            return {
                'success': True,
                'summary': summary,
                'analyser_type': self.analyser_type
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'analyser_type': self.analyser_type
            }
        finally:
            self.close_reader()
    
    def get_asset_fields(self) -> List[str]:
        """
        Get the list of asset fields specific to ownership analysis.
        
        Returns:
            List of asset field names to extract
        """
        return [
            'id', 'name', 'assetClass', 'status', 'organization',
            'parent_cloud', 'cloud', 'team'
        ]
    
    def get_cloud_fields(self) -> List[str]:
        """
        Get the list of cloud fields specific to ownership analysis.
        
        Returns:
            List of cloud field names to extract
        """
        return ['parent_cloud', 'cloud', 'team']
    
    def process_asset_specific_data(self, asset: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process asset-specific data for ownership analysis.
        
        Args:
            asset: Asset data dictionary (already flattened with ownership fields)
            
        Returns:
            Processed asset data (no additional processing needed)
        """
        # Ownership fields are already flattened by FlattenerHelper
        # No additional processing needed
        return asset
    
    def get_ownership_summary(self) -> Dict[str, Any]:
        """
        Get ownership summary statistics using DuckDB SQL queries.
        
        Returns:
            Dictionary containing ownership summary data
            
        Raises:
            ValueError: If reader is not initialized
        """
        if not self.reader:
            raise ValueError("Reader not initialized. Call create_reader() first.")
        
        try:
            # First, let's check what columns are available in the first available table
            table_names = self._get_existing_database_tables()
            if not table_names:
                return {"error": "No asset tables found"}
            
            # Use the first table to get column info
            first_table = table_names[0]
            columns_result = self.reader.execute_query(f"PRAGMA table_info({first_table})")
            available_columns = [col['name'] for col in columns_result] if columns_result else []
            
            # Get total assets using UNION across all tables
            total_assets_query = self._create_union_query("SELECT COUNT(*) as total FROM assets")
            total_assets_result = self.reader.execute_query(total_assets_query)
            total_assets = total_assets_result[0]['total'] if total_assets_result else 0
            
            # Check for flattened attribution fields
            parent_cloud_field = None
            cloud_field = None
            team_field = None
            
            for col in available_columns:
                if 'parentCloud.name' in col or 'parent_cloud' in col:
                    parent_cloud_field = col
                elif 'cloud.name' in col or 'cloud' in col:
                    cloud_field = col
                elif 'team.name' in col or 'team' in col:
                    team_field = col
            
            # Get total parent clouds (treating unknown/empty as "Zombie")
            if parent_cloud_field:
                try:
                    total_parent_clouds_result = self.reader.execute_query(f"""
                        SELECT COUNT(DISTINCT COALESCE(NULLIF("{parent_cloud_field}", ''), 'Zombie')) as total 
                        FROM assets
                    """)
                    total_parent_clouds = total_parent_clouds_result[0]['total'] if total_parent_clouds_result else 0
                except Exception as e:
                    # If query fails, don't update the metric
                    total_parent_clouds = 0
            else:
                total_parent_clouds = 0
            
            # Get total clouds (treating unknown/empty as "Zombie")
            if cloud_field:
                try:
                    total_clouds_result = self.reader.execute_query(f"""
                        SELECT COUNT(DISTINCT COALESCE(NULLIF("{cloud_field}", ''), 'Zombie')) as total 
                        FROM assets
                    """)
                    total_clouds = total_clouds_result[0]['total'] if total_clouds_result else 0
                except Exception as e:
                    # If query fails, don't update the metric
                    total_clouds = 0
            else:
                total_clouds = 0
            
            # Get total teams (treating unknown/empty as "Zombie")
            if team_field:
                try:
                    total_teams_result = self.reader.execute_query(f"""
                        SELECT COUNT(DISTINCT COALESCE(NULLIF("{team_field}", ''), 'Zombie')) as total 
                        FROM assets
                    """)
                    total_teams = total_teams_result[0]['total'] if total_teams_result else 0
                except Exception as e:
                    # If query fails, don't update the metric
                    total_teams = 0
            else:
                total_teams = 0
            
            # Get total assets unowned
            unowned_conditions = []
            if parent_cloud_field:
                unowned_conditions.append(f'("{parent_cloud_field}" IS NULL OR "{parent_cloud_field}" = \'\')')
            if cloud_field:
                unowned_conditions.append(f'("{cloud_field}" IS NULL OR "{cloud_field}" = \'\')')
            if team_field:
                unowned_conditions.append(f'("{team_field}" IS NULL OR "{team_field}" = \'\')')
            
            if unowned_conditions:
                try:
                    unowned_query = f"""
                        SELECT COUNT(*) as total 
                        FROM assets 
                        WHERE {' AND '.join(unowned_conditions)}
                    """
                    total_assets_unowned_result = self.reader.execute_query(unowned_query)
                    total_assets_unowned = total_assets_unowned_result[0]['total'] if total_assets_unowned_result else 0
                except Exception as e:
                    # If query fails, don't update the metric
                    total_assets_unowned = 0
            else:
                # If no ownership fields are detected, assume all assets are owned
                total_assets_unowned = 0
            
            return {
                'total_parent_clouds': total_parent_clouds,
                'total_clouds': total_clouds,
                'total_assets': total_assets,
                'total_assets_unowned': total_assets_unowned,
                'total_teams': total_teams,
                'debug_info': {
                    'available_columns': available_columns,
                    'parent_cloud_field': parent_cloud_field,
                    'cloud_field': cloud_field,
                    'team_field': team_field
                }
            }
            
        except Exception as e:
            raise ValueError(f"Failed to get ownership summary: {str(e)}")
    
    def get_ownership_summary_for_table(self, table_name: str) -> Dict[str, Any]:
        """
        Get ownership summary statistics for a specific table.
        
        Args:
            table_name: Name of the table to query
            
        Returns:
            Dictionary containing ownership summary data
            
        Raises:
            ValueError: If reader is not initialized
        """
        if not self.reader:
            raise ValueError("Reader not initialized. Call create_reader() first.")
        
        try:
            # Get column info from the specified table
            columns_result = self.reader.execute_query(f"PRAGMA table_info({table_name})")
            available_columns = [col['name'] for col in columns_result] if columns_result else []
            
            # Get total assets from this table
            total_assets_result = self.reader.execute_query(f"SELECT COUNT(*) as total FROM {table_name}")
            total_assets = total_assets_result[0]['total'] if total_assets_result else 0
            
            # Check for flattened attribution fields
            parent_cloud_field = None
            cloud_field = None
            team_field = None
            
            for col in available_columns:
                if 'parentCloud.name' in col or 'parent_cloud' in col:
                    parent_cloud_field = col
                elif 'cloud.name' in col or 'cloud' in col:
                    cloud_field = col
                elif 'team.name' in col or 'team' in col:
                    team_field = col
            
            # Get totals for each field
            total_parent_clouds = 0
            total_clouds = 0
            total_teams = 0
            total_assets_unowned = 0
            
            if parent_cloud_field:
                try:
                    result = self.reader.execute_query(f"""
                        SELECT COUNT(DISTINCT COALESCE(NULLIF("{parent_cloud_field}", ''), 'Zombie')) as total 
                        FROM {table_name}
                    """)
                    total_parent_clouds = result[0]['total'] if result else 0
                except Exception:
                    total_parent_clouds = 0
            
            if cloud_field:
                try:
                    result = self.reader.execute_query(f"""
                        SELECT COUNT(DISTINCT COALESCE(NULLIF("{cloud_field}", ''), 'Zombie')) as total 
                        FROM {table_name}
                    """)
                    total_clouds = result[0]['total'] if result else 0
                except Exception:
                    total_clouds = 0
            
            if team_field:
                try:
                    result = self.reader.execute_query(f"""
                        SELECT COUNT(DISTINCT COALESCE(NULLIF("{team_field}", ''), 'Zombie')) as total 
                        FROM {table_name}
                    """)
                    total_teams = result[0]['total'] if result else 0
                except Exception:
                    total_teams = 0
            
            # Get total assets unowned
            unowned_conditions = []
            if parent_cloud_field:
                unowned_conditions.append(f'("{parent_cloud_field}" IS NULL OR "{parent_cloud_field}" = \'\')')
            if cloud_field:
                unowned_conditions.append(f'("{cloud_field}" IS NULL OR "{cloud_field}" = \'\')')
            if team_field:
                unowned_conditions.append(f'("{team_field}" IS NULL OR "{team_field}" = \'\')')
            
            if unowned_conditions:
                try:
                    unowned_query = f"""
                        SELECT COUNT(*) as total 
                        FROM {table_name} 
                        WHERE {' AND '.join(unowned_conditions)}
                    """
                    result = self.reader.execute_query(unowned_query)
                    total_assets_unowned = result[0]['total'] if result else 0
                except Exception:
                    total_assets_unowned = 0
            
            return {
                'total_parent_clouds': total_parent_clouds,
                'total_clouds': total_clouds,
                'total_assets': total_assets,
                'total_assets_unowned': total_assets_unowned,
                'total_teams': total_teams,
                'table_name': table_name,
                'debug_info': {
                    'available_columns': available_columns,
                    'parent_cloud_field': parent_cloud_field,
                    'cloud_field': cloud_field,
                    'team_field': team_field
                }
            }
            
        except Exception as e:
            raise ValueError(f"Failed to get ownership summary for table {table_name}: {str(e)}")
    
    def _get_existing_database_tables(self) -> List[str]:
        """Get list of tables that actually exist in the database"""
        try:
            tables_result = self.reader.execute_query("SHOW TABLES")
            table_names = [table['name'] if isinstance(table, dict) else table[0] for table in tables_result] if tables_result else []
            return table_names
        except Exception as e:
            print(f"❌ Error getting database tables: {e}")
            return []
    
    def _get_table_and_columns(self, table_name: str = None) -> tuple[List[str], str, List[str]]:
        """
        Get table names, first table, and available columns.
        
        Args:
            table_name: Optional specific table to use
            
        Returns:
            Tuple of (list of table names, first_table, list of column names)
        """
        if table_name:
            table_names = [table_name]
            first_table = table_name
        else:
            table_names = self._get_existing_database_tables()
            if not table_names:
                return [], "", []
            first_table = table_names[0]
        
        columns_result = self.reader.execute_query(f"PRAGMA table_info({first_table})")
        available_columns = [col['name'] for col in columns_result] if columns_result else []
        
        return table_names, first_table, available_columns
    
    def _find_field(self, available_columns: List[str], search_terms: List[str]) -> str:
        """
        Find a field in available columns by searching for terms.
        
        Args:
            available_columns: List of column names
            search_terms: List of terms to search for
            
        Returns:
            Found field name or None
        """
        for col in available_columns:
            if any(term in col.lower() for term in search_terms):
                return col
        return None
    
    def _is_json_field(self, table_name: str, field_name: str) -> bool:
        """
        Check if a field contains JSON data.
        
        Args:
            table_name: Table to query (None for UNION)
            field_name: Field to check
            
        Returns:
            True if field contains JSON data
        """
        try:
            if table_name:
                sample_query = f"SELECT \"{field_name}\" FROM {table_name} LIMIT 1"
            else:
                sample_query = self._create_union_query(f"SELECT \"{field_name}\" FROM assets LIMIT 1")
            
            sample_result = self.reader.execute_query(sample_query)
            if sample_result and sample_result[0].get(field_name):
                sample_value = sample_result[0].get(field_name, '')
                return isinstance(sample_value, str) and (sample_value.startswith('{') or sample_value.startswith('['))
            return False
        except Exception:
            return False
    
    def _build_distribution_query(self, table_name: str, field_name: str, output_name: str, 
                                    is_json: bool = False, json_path: str = None) -> str:
        """
        Build a distribution query for a single field.
        
        Args:
            table_name: Specific table name or None for UNION across all tables
            field_name: Name of the field to query
            output_name: Name of the output column
            is_json: Whether the field contains JSON data
            json_path: JSON path to extract (e.g., '$.mbu')
            
        Returns:
            SQL query string
        """
        # Determine if we need JSON extraction
        if is_json and json_path:
            value_expr = f"JSON_EXTRACT_STRING(\"{field_name}\", '{json_path}')"
            output_expr = f"COALESCE(NULLIF({value_expr}, ''), 'Unknown {output_name}')"
        else:
            value_expr = f"\"{field_name}\""
            output_expr = f"COALESCE(NULLIF({value_expr}, ''), 'Zombie')"
        
        base_query = f"""
            SELECT 
                {output_expr} as {output_name},
                COUNT(*) as total_assets,
                SUM(CASE 
                    WHEN ({value_expr} IS NULL OR {value_expr} = '') 
                    THEN 1 ELSE 0 
                END) as unowned_assets
            FROM {{table}}
            GROUP BY {output_expr}
            ORDER BY total_assets DESC
        """
        
        if table_name:
            return base_query.replace("{table}", table_name)
        else:
            return self._create_union_query(base_query.replace("{table}", "assets"))
    
    def _build_multi_field_distribution_query(self, table_name: str, fields_config: List[Dict]) -> str:
        """
        Build a distribution query for multiple fields.
        
        Args:
            table_name: Specific table name or None for UNION across all tables
            fields_config: List of dicts with 'field', 'output', 'is_json', 'json_path'
            
        Returns:
            SQL query string
        """
        def get_field_expr(field_config):
            if field_config.get('is_json') and field_config.get('json_path'):
                return f"JSON_EXTRACT_STRING(\"{field_config['field']}\", '{field_config['json_path']}')"
            else:
                return f"\"{field_config['field']}\""
        
        def get_output_expr(field_config):
            value_expr = get_field_expr(field_config)
            output_name = field_config['output']
            if field_config.get('is_json'):
                return f"COALESCE(NULLIF({value_expr}, ''), 'Unknown {output_name}')"
            else:
                return f"COALESCE(NULLIF({value_expr}, ''), 'Zombie')"
        
        # Build SELECT clause
        select_parts = [f"{get_output_expr(fc)} as {fc['output']}" for fc in fields_config]
        select_parts.extend([
            "COUNT(*) as total_assets",
            f"SUM(CASE WHEN ({get_field_expr(fields_config[0])} IS NULL OR {get_field_expr(fields_config[0])} = '') THEN 1 ELSE 0 END) as unowned_assets"
        ])
        
        # Build GROUP BY clause
        group_by_parts = [get_output_expr(fc) for fc in fields_config]
        
        base_query = f"""
            SELECT {', '.join(select_parts)}
            FROM {{table}}
            GROUP BY {', '.join(group_by_parts)}
            ORDER BY total_assets DESC
        """
        
        if table_name:
            return base_query.replace("{table}", table_name)
        else:
            return self._create_union_query(base_query.replace("{table}", "assets"))
    
    def get_parent_cloud_distribution(self, table_name: str = None) -> List[Dict[str, Any]]:
        """
        Get ownership distribution by parent cloud using DuckDB SQL query.
        
        Args:
            table_name: Optional specific table to query. If None, queries all tables.
        
        Returns:
            List of dictionaries containing parent_cloud, total_assets, and unowned_assets
            
        Raises:
            ValueError: If reader is not initialized
        """
        if not self.reader:
            raise ValueError("Reader not initialized. Call create_reader() first.")
        
        try:
            _, _, available_columns = self._get_table_and_columns(table_name)
            parent_cloud_field = self._find_field(available_columns, ['parentcloud.name', 'parent_cloud'])
            
            if not parent_cloud_field:
                return []
            
            distribution_query = self._build_distribution_query(table_name, parent_cloud_field, 'parent_cloud')
            return self.reader.execute_query(distribution_query)
            
        except Exception as e:
            print(f"⚠️ Parent cloud distribution query failed: {e}")
            return []
    
    def get_cloud_distribution(self, table_name: str = None) -> List[Dict[str, Any]]:
        """
        Get ownership distribution by cloud using DuckDB SQL query.
        
        Args:
            table_name: Optional specific table to query. If None, queries all tables.
        
        Returns:
            List of dictionaries containing cloud, total_assets, and unowned_assets
            
        Raises:
            ValueError: If reader is not initialized
        """
        if not self.reader:
            raise ValueError("Reader not initialized. Call create_reader() first.")
        
        try:
            _, _, available_columns = self._get_table_and_columns(table_name)
            cloud_field = self._find_field(available_columns, ['cloud.name', 'cloud'])
            
            if not cloud_field:
                return []
            
            distribution_query = self._build_distribution_query(table_name, cloud_field, 'cloud')
            return self.reader.execute_query(distribution_query)
            
        except Exception as e:
            return []
    
    def get_team_distribution(self, table_name: str = None) -> List[Dict[str, Any]]:
        """
        Get ownership distribution by team using DuckDB SQL query.
        
        Args:
            table_name: Optional specific table to query. If None, queries all tables.
        
        Returns:
            List of dictionaries containing team, total_assets, and unowned_assets
            
        Raises:
            ValueError: If reader is not initialized
        """
        if not self.reader:
            raise ValueError("Reader not initialized. Call create_reader() first.")
        
        try:
            _, _, available_columns = self._get_table_and_columns(table_name)
            team_field = self._find_field(available_columns, ['team.name', 'team'])
            
            if not team_field:
                return []
            
            distribution_query = self._build_distribution_query(table_name, team_field, 'team')
            return self.reader.execute_query(distribution_query)
            
        except Exception as e:
            return []
    
    def get_mbu_distribution(self, table_name: str = None) -> List[Dict[str, Any]]:
        """
        Get ownership distribution by MBU (Management Business Unit) using DuckDB SQL query.
        
        Args:
            table_name: Optional specific table to query. If None, queries all tables.
        
        Returns:
            List of dictionaries containing mbu, total_assets, and unowned_assets
            
        Raises:
            ValueError: If reader is not initialized
        """
        if not self.reader:
            raise ValueError("Reader not initialized. Call create_reader() first.")
        
        try:
            _, _, available_columns = self._get_table_and_columns(table_name)
            
            # Find MBU field
            mbu_field = self._find_field(available_columns, ['properties_mbu', 'mbu', 'properties.mbu'])
            if not mbu_field:
                # Fallback: look for any properties field
                mbu_field = self._find_field(available_columns, ['properties'])
            if not mbu_field:
                return []
            
            # Check if JSON field
            is_json = self._is_json_field(table_name, mbu_field)
            
            # Build and execute query
            distribution_query = self._build_distribution_query(
                table_name, mbu_field, 'mbu',
                is_json=is_json, json_path='$.mbu'
            )
            return self.reader.execute_query(distribution_query)
            
        except Exception as e:
            print(f"⚠️ MBU distribution query failed: {e}")
            return []
    
    def get_bu_distribution(self, table_name: str = None) -> List[Dict[str, Any]]:
        """
        Get ownership distribution by BU (Business Unit) and MBU (Management Business Unit) using DuckDB SQL query.
        
        Args:
            table_name: Optional specific table to query. If None, queries all tables.
        
        Returns:
            List of dictionaries containing bu, mbu, total_assets, and unowned_assets
            
        Raises:
            ValueError: If reader is not initialized
        """
        if not self.reader:
            raise ValueError("Reader not initialized. Call create_reader() first.")
        
        try:
            _, _, available_columns = self._get_table_and_columns(table_name)
            
            # Find BU and MBU fields
            bu_field = self._find_field(available_columns, ['properties_bu', 'bu', 'properties.bu', 'business_unit'])
            if not bu_field:
                bu_field = self._find_field(available_columns, ['properties'])
            if not bu_field:
                return []
            
            mbu_field = self._find_field(available_columns, ['properties_mbu', 'mbu', 'properties.mbu'])
            if not mbu_field:
                mbu_field = bu_field  # Use same field for MBU
            
            # Check if JSON fields
            bu_is_json = self._is_json_field(table_name, bu_field)
            mbu_is_json = self._is_json_field(table_name, mbu_field)
            
            # Build fields config
            fields_config = [
                {'field': bu_field, 'output': 'bu', 'is_json': bu_is_json, 'json_path': '$.bu'},
                {'field': mbu_field, 'output': 'mbu', 'is_json': mbu_is_json, 'json_path': '$.mbu'}
            ]
            
            # Build and execute query
            distribution_query = self._build_multi_field_distribution_query(table_name, fields_config)
            return self.reader.execute_query(distribution_query)
            
        except Exception as e:
            return []
