"""
Flattener Helper - Utility functions for data flattening operations
"""

from typing import Dict, Any, Optional, List


class FlattenerHelper:
    """Helper class for data flattening operations"""
    
    @staticmethod
    def extract_ownership_info(asset_attributions: List[Dict[str, Any]]) -> Dict[str, Optional[str]]:
        """
        Extract ownership information from assetAttributions array.
        
        Looks for the attribution with "resource_owner" in the roles array and extracts:
        - parent_cloud, parent_cloud_id, parent_cloud_owner_email
        - cloud, cloud_id, cloud_owner_email  
        - team, team_id, team_owner_email
        
        Args:
            asset_attributions: List of attribution dictionaries
            
        Returns:
            Dictionary containing ownership information with None values if not found
        """
        # Initialize result with None values
        result = {
            'parent_cloud': None,
            'parent_cloud_id': None,
            'parent_cloud_owner_email': None,
            'cloud': None,
            'cloud_id': None,
            'cloud_owner_email': None,
            'team': None,
            'team_id': None,
            'team_owner_email': None
        }
        
        # Find the attribution with "resource_owner" in roles array
        resource_attribution = None
        for attribution in asset_attributions:
            roles = attribution.get('roles', [])
            if isinstance(roles, list) and 'resource_owner' in roles:
                resource_attribution = attribution
                break
        
        if not resource_attribution:
            return result
        
        # Extract parent cloud information
        parent_cloud = resource_attribution.get('parentCloud')
        if parent_cloud and isinstance(parent_cloud, dict):
            result['parent_cloud'] = parent_cloud.get('name') or None
            result['parent_cloud_id'] = parent_cloud.get('identifier') or None
            
            # Extract parent cloud owner email
            parent_cloud_lead = parent_cloud.get('lead')
            if parent_cloud_lead and isinstance(parent_cloud_lead, dict):
                result['parent_cloud_owner_email'] = parent_cloud_lead.get('emailId') or None
            else:
                result['parent_cloud_owner_email'] = None
        else:
            result['parent_cloud'] = None
            result['parent_cloud_id'] = None
            result['parent_cloud_owner_email'] = None
        
        # Extract cloud information
        cloud = resource_attribution.get('cloud')
        if cloud and isinstance(cloud, dict):
            result['cloud'] = cloud.get('name') or None
            result['cloud_id'] = cloud.get('identifier') or None
            
            # Extract cloud owner email
            cloud_lead = cloud.get('lead')
            if cloud_lead and isinstance(cloud_lead, dict):
                result['cloud_owner_email'] = cloud_lead.get('emailId') or None
            else:
                result['cloud_owner_email'] = None
        else:
            result['cloud'] = None
            result['cloud_id'] = None
            result['cloud_owner_email'] = None
        
        # Extract team information
        team = resource_attribution.get('team')
        if team and isinstance(team, dict):
            result['team'] = team.get('name') or None
            result['team_id'] = team.get('identifier') or None
            
            # Extract team owner email
            team_lead = team.get('lead')
            if team_lead and isinstance(team_lead, dict):
                result['team_owner_email'] = team_lead.get('emailId') or None
            else:
                result['team_owner_email'] = None
        else:
            result['team'] = None
            result['team_id'] = None
            result['team_owner_email'] = None
        
        return result
    
    @staticmethod
    def extract_all_ownership_info(asset_attributions: List[Dict[str, Any]]) -> List[Dict[str, Optional[str]]]:
        """
        Extract ownership information from all attributions in the array.
        
        Args:
            asset_attributions: List of attribution dictionaries
            
        Returns:
            List of dictionaries containing ownership information for each attribution
        """
        results = []
        
        for attribution in asset_attributions:
            result = {
                'role': attribution.get('role'),
                'parent_cloud': None,
                'parent_cloud_id': None,
                'parent_cloud_owner_email': None,
                'cloud': None,
                'cloud_id': None,
                'cloud_owner_email': None,
                'team': None,
                'team_id': None,
                'team_owner_email': None
            }
            
            # Extract parent cloud information
            parent_cloud = attribution.get('parentCloud')
            if parent_cloud and isinstance(parent_cloud, dict):
                result['parent_cloud'] = parent_cloud.get('name') or None
                result['parent_cloud_id'] = parent_cloud.get('identifier') or None
                
                # Extract parent cloud owner email
                parent_cloud_lead = parent_cloud.get('lead')
                if parent_cloud_lead and isinstance(parent_cloud_lead, dict):
                    result['parent_cloud_owner_email'] = parent_cloud_lead.get('emailId') or None
                else:
                    result['parent_cloud_owner_email'] = None
            else:
                result['parent_cloud'] = None
                result['parent_cloud_id'] = None
                result['parent_cloud_owner_email'] = None
            
            # Extract cloud information
            cloud = attribution.get('cloud')
            if cloud and isinstance(cloud, dict):
                result['cloud'] = cloud.get('name') or None
                result['cloud_id'] = cloud.get('identifier') or None
                
                # Extract cloud owner email
                cloud_lead = cloud.get('lead')
                if cloud_lead and isinstance(cloud_lead, dict):
                    result['cloud_owner_email'] = cloud_lead.get('emailId') or None
                else:
                    result['cloud_owner_email'] = None
            else:
                result['cloud'] = None
                result['cloud_id'] = None
                result['cloud_owner_email'] = None
            
            # Extract team information
            team = attribution.get('team')
            if team and isinstance(team, dict):
                result['team'] = team.get('name') or None
                result['team_id'] = team.get('identifier') or None
                
                # Extract team owner email
                team_lead = team.get('lead')
                if team_lead and isinstance(team_lead, dict):
                    result['team_owner_email'] = team_lead.get('emailId') or None
                else:
                    result['team_owner_email'] = None
            else:
                result['team'] = None
                result['team_id'] = None
                result['team_owner_email'] = None
            
            results.append(result)
        
        return results
    
    @staticmethod
    def extract_properties(properties: Dict[str, Any]) -> Dict[str, Optional[str]]:
        """
        Extract properties and format them with 'properties_' prefix.
        
        Args:
            properties: Dictionary containing properties
            
        Returns:
            Dictionary with properties prefixed with 'properties_'
        """
        result = {}
        
        if not properties or not isinstance(properties, dict):
            return result
        
        for key, value in properties.items():
            # Convert value to string and handle None/empty values
            if value is None or value == "":
                result[f"properties_{key}"] = None
            else:
                result[f"properties_{key}"] = str(value)
        
        return result
    
    @staticmethod
    def extract_properties_from_asset(asset: Dict[str, Any]) -> Dict[str, Optional[str]]:
        """
        Extract properties from an asset dictionary.
        
        Args:
            asset: Asset dictionary containing properties
            
        Returns:
            Dictionary with properties prefixed with 'properties_'
        """
        if not asset or not isinstance(asset, dict):
            return {}
        
        properties = asset.get('properties')
        if not properties or not isinstance(properties, dict):
            return {}
        
        return FlattenerHelper.extract_properties(properties)
    
    @staticmethod
    def extract_tags(tags: Dict[str, Any]) -> Dict[str, Optional[str]]:
        """
        Extract tags and format them with 'tags_' prefix.
        
        Args:
            tags: Dictionary containing tags
            
        Returns:
            Dictionary with tags prefixed with 'tags_'
        """
        result = {}
        
        if not tags or not isinstance(tags, dict):
            return result
        
        for key, value in tags.items():
            # Convert value to string and handle None/empty values
            if value is None or value == "":
                result[f"tags_{key}"] = None
            else:
                result[f"tags_{key}"] = str(value)
        
        return result
    
    @staticmethod
    def extract_tags_from_asset(asset: Dict[str, Any]) -> Dict[str, Optional[str]]:
        """
        Extract tags from an asset dictionary.
        
        Args:
            asset: Asset dictionary containing tags
            
        Returns:
            Dictionary with tags prefixed with 'tags_'
        """
        if not asset or not isinstance(asset, dict):
            return {}
        
        tags = asset.get('tags')
        if not tags or not isinstance(tags, dict):
            return {}
        
        return FlattenerHelper.extract_tags(tags)
    
    @staticmethod
    def flatten_asset(asset: Dict[str, Any]) -> Dict[str, Any]:
        """
        Flatten an asset by retaining top-level keys and processing only specific nested structures.
        
        This method:
        - Retains all top-level keys as-is (including other dicts/lists like assetUsers, assetCriticalities)
        - Processes ONLY 'properties' using extract_properties_from_asset()
        - Processes ONLY 'tags' using extract_tags_from_asset()
        - Processes ONLY 'assetAttributions' using extract_ownership_info()
        - Leaves all other nested structures (dicts/lists) unchanged
        
        Args:
            asset: Asset dictionary to flatten
            
        Returns:
            Flattened asset dictionary with only specific nested structures processed
        """
        if not asset or not isinstance(asset, dict):
            return {}
        
        # Start with only the basic top-level fields (no nested structures)
        flattened = {}
        
        # Keep only simple top-level fields (strings, numbers, booleans, null)
        for key, value in asset.items():
            if key in ['assetAttributions', 'properties', 'tags', 'location']:
                # Skip these - we'll process them separately or exclude them
                continue
            elif isinstance(value, (str, int, float, bool)) or value is None:
                # Keep simple fields
                flattened[key] = value
            # Skip all other nested structures (dicts, lists, etc.)
        
        # Process ONLY assetAttributions for ownership information
        attributions = asset.get('assetAttributions')
        if attributions and isinstance(attributions, list):
            ownership_info = FlattenerHelper.extract_ownership_info(attributions)
            flattened.update(ownership_info)
        
        # Process ONLY properties
        properties_info = FlattenerHelper.extract_properties_from_asset(asset)
        flattened.update(properties_info)
        
        # Process ONLY tags
        tags_info = FlattenerHelper.extract_tags_from_asset(asset)
        flattened.update(tags_info)
        
        return flattened
    
    @staticmethod
    def flatten_assets(assets: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Flatten a list of assets.
        
        Args:
            assets: List of asset dictionaries to flatten
            
        Returns:
            List of flattened asset dictionaries
        """
        if not assets or not isinstance(assets, list):
            return []
        
        return [FlattenerHelper.flatten_asset(asset) for asset in assets]
