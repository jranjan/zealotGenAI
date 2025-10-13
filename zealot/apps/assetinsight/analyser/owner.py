"""
Owner Asset Analyser - Concrete implementation for owner-specific asset analysis.

This analyser focuses on ownership, team assignments, and resource attribution
aspects of asset data using DuckDB for efficient querying.
"""

from typing import Any, Dict, List
from .asset import AssetAnalyser


class OwnerAnalyser(AssetAnalyser):
    """
    Concrete implementation of AssetAnalyser for owner-specific analysis.
    
    This analyser focuses on ownership, team assignments, and resource attribution
    aspects of asset data.
    """
    
    def __init__(self):
        """Initialize the owner analyser."""
        super().__init__("owner")
    
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
            # First, let's check what columns are available in the assets table
            columns_result = self.reader.execute_query("PRAGMA table_info(assets)")
            available_columns = [col['name'] for col in columns_result] if columns_result else []
            
            # Get total assets
            total_assets_result = self.reader.execute_query("SELECT COUNT(*) as total FROM assets")
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
                total_assets_unowned = total_assets
            
            return {
                'total_parent_cloud': total_parent_clouds,
                'total_cloud': total_clouds,
                'total_asset': total_assets,
                'total_assets_unowned': total_assets_unowned,
                'total_team': total_teams,
                'debug_info': {
                    'available_columns': available_columns,
                    'parent_cloud_field': parent_cloud_field,
                    'cloud_field': cloud_field,
                    'team_field': team_field
                }
            }
            
        except Exception as e:
            raise ValueError(f"Failed to get ownership summary: {str(e)}")
    
    def get_parent_cloud_distribution(self) -> List[Dict[str, Any]]:
        """
        Get ownership distribution by parent cloud using DuckDB SQL query.
        
        Returns:
            List of dictionaries containing parent_cloud, total_assets, and unowned_assets
            
        Raises:
            ValueError: If reader is not initialized
        """
        if not self.reader:
            raise ValueError("Reader not initialized. Call create_reader() first.")
        
        try:
            # First, let's check what columns are available in the assets table
            columns_result = self.reader.execute_query("PRAGMA table_info(assets)")
            available_columns = [col['name'] for col in columns_result] if columns_result else []
            
            # Find the parent cloud field
            parent_cloud_field = None
            for col in available_columns:
                if 'parentCloud.name' in col or 'parent_cloud' in col:
                    parent_cloud_field = col
                    break
            
            if not parent_cloud_field:
                return []
            
            distribution_query = f"""
                SELECT 
                    COALESCE(NULLIF("{parent_cloud_field}", ''), 'Zombie') as parent_cloud,
                    COUNT(*) as total_assets,
                    SUM(CASE 
                        WHEN ("{parent_cloud_field}" IS NULL OR "{parent_cloud_field}" = '') 
                        THEN 1 ELSE 0 
                    END) as unowned_assets
                FROM assets 
                GROUP BY COALESCE(NULLIF("{parent_cloud_field}", ''), 'Zombie')
                ORDER BY total_assets DESC
            """
            
            try:
                return self.reader.execute_query(distribution_query)
            except Exception as e:
                # If query fails, return empty list to avoid updating metrics
                return []
            
        except Exception as e:
            raise ValueError(f"Failed to get parent cloud distribution: {str(e)}")
    
    def get_cloud_distribution(self) -> List[Dict[str, Any]]:
        """
        Get ownership distribution by cloud using DuckDB SQL query.
        
        Returns:
            List of dictionaries containing cloud, total_assets, and unowned_assets
            
        Raises:
            ValueError: If reader is not initialized
        """
        if not self.reader:
            raise ValueError("Reader not initialized. Call create_reader() first.")
        
        try:
            # First, let's check what columns are available in the assets table
            columns_result = self.reader.execute_query("PRAGMA table_info(assets)")
            available_columns = [col['name'] for col in columns_result] if columns_result else []
            
            # Find the cloud field
            cloud_field = None
            for col in available_columns:
                if 'cloud.name' in col or 'cloud' in col:
                    cloud_field = col
                    break
            
            if not cloud_field:
                return []
            
            distribution_query = f"""
                SELECT 
                    COALESCE(NULLIF("{cloud_field}", ''), 'Zombie') as cloud,
                    COUNT(*) as total_assets,
                    SUM(CASE 
                        WHEN ("{cloud_field}" IS NULL OR "{cloud_field}" = '') 
                        THEN 1 ELSE 0 
                    END) as unowned_assets
                FROM assets 
                GROUP BY COALESCE(NULLIF("{cloud_field}", ''), 'Zombie')
                ORDER BY total_assets DESC
            """
            
            try:
                return self.reader.execute_query(distribution_query)
            except Exception as e:
                # If query fails, return empty list to avoid updating metrics
                return []
            
        except Exception as e:
            raise ValueError(f"Failed to get cloud distribution: {str(e)}")
    
    def get_team_distribution(self) -> List[Dict[str, Any]]:
        """
        Get ownership distribution by team using DuckDB SQL query.
        
        Returns:
            List of dictionaries containing team, total_assets, and unowned_assets
            
        Raises:
            ValueError: If reader is not initialized
        """
        if not self.reader:
            raise ValueError("Reader not initialized. Call create_reader() first.")
        
        try:
            # First, let's check what columns are available in the assets table
            columns_result = self.reader.execute_query("PRAGMA table_info(assets)")
            available_columns = [col['name'] for col in columns_result] if columns_result else []
            
            # Find the team field
            team_field = None
            for col in available_columns:
                if 'team.name' in col or 'team' in col:
                    team_field = col
                    break
            
            if not team_field:
                return []
            
            distribution_query = f"""
                SELECT 
                    COALESCE(NULLIF("{team_field}", ''), 'Zombie') as team,
                    COUNT(*) as total_assets,
                    SUM(CASE 
                        WHEN ("{team_field}" IS NULL OR "{team_field}" = '') 
                        THEN 1 ELSE 0 
                    END) as unowned_assets
                FROM assets 
                GROUP BY COALESCE(NULLIF("{team_field}", ''), 'Zombie')
                ORDER BY total_assets DESC
            """
            
            try:
                return self.reader.execute_query(distribution_query)
            except Exception as e:
                # If query fails, return empty list to avoid updating metrics
                return []
            
        except Exception as e:
            raise ValueError(f"Failed to get team distribution: {str(e)}")
    
    def get_mbu_distribution(self) -> List[Dict[str, Any]]:
        """
        Get ownership distribution by MBU (Management Business Unit) using DuckDB SQL query.
        
        Returns:
            List of dictionaries containing mbu, total_assets, and unowned_assets
            
        Raises:
            ValueError: If reader is not initialized
        """
        if not self.reader:
            raise ValueError("Reader not initialized. Call create_reader() first.")
        
        try:
            # First, let's check what columns are available in the assets table
            columns_result = self.reader.execute_query("PRAGMA table_info(assets)")
            available_columns = [col['name'] for col in columns_result] if columns_result else []
            
            # Debug: Print available columns to help diagnose the issue
            print(f"🔍 Available columns in assets table: {available_columns}")
            
            # Find the MBU field - look for various possible field names
            mbu_field = None
            for col in available_columns:
                if any(term in col.lower() for term in ['properties_mbu', 'mbu', 'properties.mbu']):
                    mbu_field = col
                    print(f"✅ Found MBU field: {mbu_field}")
                    break
            
            if not mbu_field:
                print("❌ No MBU field found. Available columns:", available_columns)
                # Try to find any field that might contain MBU data
                potential_mbu_fields = [col for col in available_columns if 'properties' in col.lower()]
                if potential_mbu_fields:
                    print(f"🔍 Potential MBU fields (properties-related): {potential_mbu_fields}")
                    # Use the first properties field as a fallback
                    mbu_field = potential_mbu_fields[0]
                    print(f"🔄 Using fallback field: {mbu_field}")
                else:
                    return []
            
            # Check if the field is JSON or direct string
            # Try to determine field type by checking if it contains JSON data
            sample_query = f"SELECT \"{mbu_field}\" FROM assets LIMIT 1"
            try:
                sample_result = self.reader.execute_query(sample_query)
                if sample_result and sample_result[0][mbu_field]:
                    sample_value = sample_result[0][mbu_field]
                    is_json = isinstance(sample_value, str) and (sample_value.startswith('{') or sample_value.startswith('['))
                    print(f"🔍 Field '{mbu_field}' appears to be {'JSON' if is_json else 'direct string'}")
                else:
                    is_json = False
            except:
                is_json = False
            
            if is_json:
                # Handle JSON field
                distribution_query = f"""
                    SELECT 
                        COALESCE(NULLIF(JSON_EXTRACT_STRING("{mbu_field}", '$.mbu'), ''), 'Zombie') as mbu,
                        COUNT(*) as total_assets,
                        SUM(CASE 
                            WHEN (JSON_EXTRACT_STRING("{mbu_field}", '$.mbu') IS NULL OR JSON_EXTRACT_STRING("{mbu_field}", '$.mbu') = '') 
                            THEN 1 ELSE 0 
                        END) as unowned_assets
                    FROM assets 
                    GROUP BY COALESCE(NULLIF(JSON_EXTRACT_STRING("{mbu_field}", '$.mbu'), ''), 'Zombie')
                    ORDER BY total_assets DESC
                """
            else:
                # Handle direct string field
                distribution_query = f"""
                    SELECT 
                        COALESCE(NULLIF("{mbu_field}", ''), 'Zombie') as mbu,
                        COUNT(*) as total_assets,
                        SUM(CASE 
                            WHEN ("{mbu_field}" IS NULL OR "{mbu_field}" = '') 
                            THEN 1 ELSE 0 
                        END) as unowned_assets
                    FROM assets 
                    GROUP BY COALESCE(NULLIF("{mbu_field}", ''), 'Zombie')
                    ORDER BY total_assets DESC
                """
            
            try:
                return self.reader.execute_query(distribution_query)
            except Exception as e:
                # If query fails, return empty list to avoid updating metrics
                return []
            
        except Exception as e:
            raise ValueError(f"Failed to get MBU distribution: {str(e)}")
