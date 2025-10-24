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
    
    def _get_table_name(self, asset_class: str = None) -> str:
        """Get table name for asset class"""
        if asset_class:
            for ac in AssetClass:
                if ac.class_name == asset_class:
                    return ac.table_name
        return None
    
    def _query_single_table(self, query: str, table_name: str) -> List[Dict[str, Any]]:
        """Execute query on single table"""
        try:
            return self.reader.execute_query(query.replace('FROM {table}', f'FROM {table_name}'))
        except Exception as e:
            print(f"⚠️ Error querying {table_name}: {e}")
            return []
    
    def _query_all_tables(self, query: str) -> List[Dict[str, Any]]:
        """Execute query on all tables and combine results"""
        all_results = []
        for table_name in AssetClass.get_all_table_names():
            table_query = query.replace('FROM {table}', f'FROM {table_name}')
            try:
                result = self.reader.execute_query(table_query)
                all_results.extend(result)
            except Exception as e:
                print(f"⚠️ Error querying {table_name}: {e}")
                continue
        return all_results
    
    def _combine_results(self, results: List[Dict[str, Any]], group_key: str) -> List[Dict[str, Any]]:
        """Combine results by grouping key"""
        combined = {}
        for result in results:
            key = result[group_key]
            if key not in combined:
                combined[key] = {group_key: key, 'total_assets': 0, 'unowned_assets': 0}
            combined[key]['total_assets'] += result['total_assets']
            combined[key]['unowned_assets'] += result['unowned_assets']
        
        result_list = list(combined.values())
        result_list.sort(key=lambda x: x['total_assets'], reverse=True)
        return result_list
    
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
    
    def get_ownership_summary(self, asset_class: str = None) -> Dict[str, Any]:
        """Get ownership summary statistics"""
        if not self.reader:
            raise ValueError("Reader not initialized. Call create_reader() first.")
        
        try:
            table_names = AssetClass.get_all_table_names()
            if not table_names:
                return {"error": "No asset tables found"}
            
            # Get column info
            first_table = table_names[0]
            columns_result = self.reader.execute_query(f"PRAGMA table_info({first_table})")
            available_columns = [col['name'] for col in columns_result] if columns_result else []
            
            # Find ownership fields
            parent_cloud_field = next((col for col in available_columns if 'parentCloud.name' in col or 'parent_cloud' in col), None)
            cloud_field = next((col for col in available_columns if 'cloud.name' in col or 'cloud' in col), None)
            team_field = next((col for col in available_columns if 'team.name' in col or 'team' in col), None)
            
            # Get total assets
            if asset_class:
                table_name = self._get_table_name(asset_class)
                total_assets = self._query_single_table("SELECT COUNT(*) as total FROM {table}", table_name)[0]['total'] if table_name else 0
            else:
                total_assets = sum(self._query_single_table("SELECT COUNT(*) as total FROM {table}", tn)[0]['total'] for tn in table_names)
            
            # Get counts for each field
            total_parent_clouds = self._get_distinct_count(parent_cloud_field, asset_class) if parent_cloud_field else 0
            total_clouds = self._get_distinct_count(cloud_field, asset_class) if cloud_field else 0
            total_teams = self._get_distinct_count(team_field, asset_class) if team_field else 0
            
            # Get unowned assets
            unowned_conditions = [f'("{field}" IS NULL OR "{field}" = \'\')' for field in [parent_cloud_field, cloud_field, team_field] if field]
            total_assets_unowned = self._get_unowned_count(unowned_conditions, asset_class) if unowned_conditions else 0
            
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
    
    def _get_distinct_count(self, field: str, asset_class: str = None) -> int:
        """Get distinct count for a field"""
        query = f'SELECT COUNT(DISTINCT COALESCE(NULLIF("{field}", \'\'), \'Zombie\')) as total FROM {{table}}'
        
        if asset_class:
            table_name = self._get_table_name(asset_class)
            if table_name:
                result = self._query_single_table(query, table_name)
                return result[0]['total'] if result else 0
        else:
            results = self._query_all_tables(query)
            return sum(r['total'] for r in results)
        return 0
    
    def _get_unowned_count(self, conditions: List[str], asset_class: str = None) -> int:
        """Get count of unowned assets"""
        query = f"SELECT COUNT(*) as total FROM {{table}} WHERE {' AND '.join(conditions)}"
        
        if asset_class:
            table_name = self._get_table_name(asset_class)
            if table_name:
                result = self._query_single_table(query, table_name)
                return result[0]['total'] if result else 0
        else:
            results = self._query_all_tables(query)
            return sum(r['total'] for r in results)
        return 0
    
    def get_parent_cloud_distribution(self, asset_class: str = None) -> List[Dict[str, Any]]:
        """Get ownership distribution by parent cloud"""
        # Try multiple field patterns for parent cloud
        field_patterns = [
            'parentCloud.name', 'parent_cloud', 'parentCloud', 'parent_cloud_name',
            'parentCloudName', 'parentCloudName.name', 'cloud.parentCloud',
            'attribution', 'properties', 'ownership'
        ]
        
        field = self._find_field(field_patterns)
        
        if not field:
            print("⚠️ No parent cloud field found. Available fields will be shown in console.")
            # Show available fields for debugging
            self._debug_available_fields(asset_class)
            return []
        
        print(f"🔍 Using parent cloud field: {field}")
        return self._get_distribution(field, 'parent_cloud', asset_class)
    
    def get_cloud_distribution(self, asset_class: str = None) -> List[Dict[str, Any]]:
        """Get ownership distribution by cloud"""
        # Try multiple field patterns for cloud
        field_patterns = [
            'cloud.name', 'cloud', 'cloud_name', 'cloud_provider',
            'provider', 'aws', 'azure', 'gcp', 'google_cloud',
            'parent_cloud', 'parentCloud', 'parentCloud.name',
            'attribution', 'properties', 'ownership'
        ]
        
        field = self._find_field(field_patterns)
        
        if not field:
            print("⚠️ No cloud field found. Available fields will be shown in console.")
            # Show available fields for debugging
            self._debug_available_fields(asset_class)
            return []
        
        print(f"🔍 Using cloud field: {field}")
        return self._get_distribution(field, 'cloud', asset_class)
    
    def get_team_distribution(self, asset_class: str = None) -> List[Dict[str, Any]]:
        """Get ownership distribution by team"""
        # Try multiple field patterns for team
        field_patterns = [
            'team.name', 'team', 'team_name', 'teamName', 'teamName.name',
            'owner', 'owner.name', 'owner_name', 'assigned_team',
            'attribution', 'properties', 'ownership'
        ]
        
        field = self._find_field(field_patterns)
        
        if not field:
            print("⚠️ No team field found. Available fields will be shown in console.")
            # Show available fields for debugging
            self._debug_available_fields(asset_class)
            return []
        
        print(f"🔍 Using team field: {field}")
        return self._get_distribution(field, 'team', asset_class)
    
    def _find_field(self, field_patterns: List[str]) -> str:
        """Find field matching any of the patterns"""
        if not self.reader:
            return None
        
        table_names = AssetClass.get_all_table_names()
        if not table_names:
            return None
        
        columns_result = self.reader.execute_query(f"PRAGMA table_info({table_names[0]})")
        available_columns = [col['name'] for col in columns_result] if columns_result else []
        
        for pattern in field_patterns:
            for col in available_columns:
                if pattern in col:
                    return col
        return None
    
    def _get_distribution(self, field: str, group_key: str, asset_class: str = None) -> List[Dict[str, Any]]:
        """Get distribution for a field"""
        if not field:
            return []
        
        # Check if field contains JSON data and adjust query accordingly
        is_json = self._is_json_field(field, asset_class)
        
        if is_json and group_key == 'mbu':
            # Handle JSON field for MBU
            field_expr = f"JSON_EXTRACT_STRING(\"{field}\", '$.mbu')"
        elif is_json and group_key == 'bu':
            # Handle JSON field for BU
            field_expr = f"JSON_EXTRACT_STRING(\"{field}\", '$.bu')"
        else:
            # Handle direct string field
            field_expr = f"\"{field}\""
        
        query = f"""
            SELECT 
                COALESCE(NULLIF({field_expr}, ''), 'Zombie') as {group_key},
                COUNT(*) as total_assets,
                SUM(CASE 
                    WHEN ({field_expr} IS NULL OR {field_expr} = '') 
                    THEN 1 ELSE 0 
                END) as unowned_assets
            FROM {{table}} 
            GROUP BY COALESCE(NULLIF({field_expr}, ''), 'Zombie')
        """
        
        print(f"🔍 Executing {group_key} distribution query with field: {field} (JSON: {is_json})")
        
        if asset_class:
            table_name = self._get_table_name(asset_class)
            if table_name:
                return self._query_single_table(query, table_name)
        else:
            results = self._query_all_tables(query)
            return self._combine_results(results, group_key)
        return []
    
    
    def get_mbu_distribution(self, asset_class: str = None) -> List[Dict[str, Any]]:
        """Get ownership distribution by MBU"""
        # Try multiple field patterns for MBU
        field_patterns = [
            'properties_mbu', 'mbu', 'properties.mbu', 'properties_mbu_mbu',
            'properties_bu_mbu', 'mbu_name', 'management_business_unit',
            'properties', 'attribution', 'ownership'
        ]
        
        field = self._find_field(field_patterns)
        
        if not field:
            print("⚠️ No MBU field found. Available fields will be shown in console.")
            # Show available fields for debugging
            self._debug_available_fields(asset_class)
            return []
        
        print(f"🔍 Using MBU field: {field}")
        return self._get_distribution(field, 'mbu', asset_class)
    
    def get_bu_distribution(self, asset_class: str = None) -> List[Dict[str, Any]]:
        """Get ownership distribution by BU and MBU"""
        bu_field = self._find_field(['properties_bu', 'bu', 'properties.bu', 'business_unit'])
        mbu_field = self._find_field(['properties_mbu', 'mbu', 'properties.mbu'])
        
        if not bu_field:
            bu_field = self._find_field(['properties'])
        if not mbu_field:
            mbu_field = bu_field  # Use BU field as fallback
            
        return self._get_bu_mbu_distribution(bu_field, mbu_field, asset_class) if bu_field else []
    
    def _get_bu_mbu_distribution(self, bu_field: str, mbu_field: str, asset_class: str = None) -> List[Dict[str, Any]]:
        """Get BU/MBU distribution with field expressions"""
        # Check if fields are JSON
        bu_expr = f"JSON_EXTRACT_STRING(\"{bu_field}\", '$.bu')" if self._is_json_field(bu_field, asset_class) else f"\"{bu_field}\""
        mbu_expr = f"JSON_EXTRACT_STRING(\"{mbu_field}\", '$.mbu')" if self._is_json_field(mbu_field, asset_class) else f"\"{mbu_field}\""
        
        query = f"""
            SELECT 
                COALESCE(NULLIF({bu_expr}, ''), 'Unknown BU') as bu,
                COALESCE(NULLIF({mbu_expr}, ''), 'Unknown MBU') as mbu,
                COUNT(*) as total_assets,
                SUM(CASE 
                    WHEN ({bu_expr} IS NULL OR {bu_expr} = '') 
                    THEN 1 ELSE 0 
                END) as unowned_assets
            FROM {{table}} 
            GROUP BY 
                COALESCE(NULLIF({bu_expr}, ''), 'Unknown BU'),
                COALESCE(NULLIF({mbu_expr}, ''), 'Unknown MBU')
        """
        
        if asset_class:
            table_name = self._get_table_name(asset_class)
            if table_name:
                return self._query_single_table(query, table_name)
        else:
            results = self._query_all_tables(query)
            # Combine by bu and mbu
            combined = {}
            for result in results:
                key = (result['bu'], result['mbu'])
                if key not in combined:
                    combined[key] = {'bu': result['bu'], 'mbu': result['mbu'], 'total_assets': 0, 'unowned_assets': 0}
                combined[key]['total_assets'] += result['total_assets']
                combined[key]['unowned_assets'] += result['unowned_assets']
            
            result_list = list(combined.values())
            result_list.sort(key=lambda x: x['total_assets'], reverse=True)
            return result_list
        return []
    
    def _is_json_field(self, field: str, asset_class: str = None) -> bool:
        """Check if field contains JSON data"""
        if not field:
            return False
        
        try:
            if asset_class:
                table_name = self._get_table_name(asset_class)
                if not table_name:
                    return False
                sample_query = f"SELECT \"{field}\" FROM {table_name} LIMIT 1"
            else:
                table_names = AssetClass.get_all_table_names()
                if not table_names:
                    return False
                sample_query = f"SELECT \"{field}\" FROM {table_names[0]} LIMIT 1"
            
            result = self.reader.execute_query(sample_query)
            if result and result[0][field]:
                value = result[0][field]
                return isinstance(value, str) and (value.startswith('{') or value.startswith('['))
        except:
            pass
        return False
    
    def _debug_available_fields(self, asset_class: str = None):
        """Debug method to show available fields in the database"""
        try:
            if asset_class:
                table_name = self._get_table_name(asset_class)
                if not table_name:
                    print(f"❌ Table not found for asset class: {asset_class}")
                    return
                tables_to_check = [table_name]
            else:
                tables_to_check = AssetClass.get_all_table_names()
            
            print(f"🔍 Checking tables: {tables_to_check}")
            
            for table_name in tables_to_check:
                try:
                    columns_result = self.reader.execute_query(f"PRAGMA table_info({table_name})")
                    available_columns = [col['name'] for col in columns_result] if columns_result else []
                    print(f"📋 Available columns in {table_name}: {available_columns}")
                    
                    # Look for any field that might contain ownership data
                    ownership_candidates = [col for col in available_columns if any(term in col.lower() for term in [
                        'mbu', 'bu', 'cloud', 'team', 'owner', 'parent', 'properties', 'attribution', 'ownership'
                    ])]
                    if ownership_candidates:
                        print(f"🎯 Potential ownership fields in {table_name}: {ownership_candidates}")
                        
                        # Show sample data for potential fields
                        for candidate in ownership_candidates[:5]:  # Check first 5 candidates
                            try:
                                sample_query = f"SELECT \"{candidate}\" FROM {table_name} LIMIT 1"
                                sample_result = self.reader.execute_query(sample_query)
                                if sample_result and sample_result[0][candidate]:
                                    sample_value = str(sample_result[0][candidate])[:100]
                                    print(f"📄 Sample data for {candidate}: {sample_value}...")
                            except Exception as e:
                                print(f"⚠️ Error sampling {candidate}: {e}")
                except Exception as e:
                    print(f"⚠️ Error checking table {table_name}: {e}")
                    
        except Exception as e:
            print(f"❌ Error in debug_available_fields: {e}")
