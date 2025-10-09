"""
Asset Data Loader - Specialized JSON loader for asset management data.

This module provides a specialized implementation of JSONLoader specifically
designed for processing asset data and generating organized output files
by parentCloud with team and asset aggregations.
"""

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from collections import defaultdict
from zealot.utils.loader.json import JSONLoader


class AssetDataLoader(JSONLoader):
    """
    Specialized JSON loader for asset management data.
    
    This class extends JSONLoader to provide asset-specific functionality
    for processing asset data and generating organized output files.
    
    Key Features:
    - Memory-efficient file-by-file processing (no bulk loading)
    - Zombie cloud handling for assets with missing parent cloud info
    - Progress tracking with optional callbacks
    - Support for any asset class (server, database, network, etc.)
    - Real-time processing statistics
    """
    
    def __init__(self, json_folder: Union[str, Path] = None):
        """
        Initialize the Asset Data loader.
        
        Args:
            json_folder: Path to the folder containing asset JSON files.
        """
        if json_folder is None:
            # Default to current directory
            json_folder = Path.cwd()
        
        super().__init__(json_folder, recursive=False)
    
    def process_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process asset data and add computed fields.
        
        Args:
            data: Raw JSON asset data
            
        Returns:
            Processed asset data with additional computed fields
        """
        processed = data.copy()
        
        # Add computed fields
        processed['_is_valid'] = self._validate_asset_data(data)
        processed['_parent_cloud_id'] = self._extract_parent_cloud_id(data)
        processed['_parent_cloud_name'] = self._extract_parent_cloud_name(data)
        processed['_team_count'] = self._count_teams(data)
        processed['_has_attributions'] = len(data.get('assetAttributions', [])) > 0
        
        return processed
    
    def _validate_asset_data(self, data: Dict[str, Any]) -> bool:
        """
        Validate asset data structure.
        
        Args:
            data: Asset data to validate
            
        Returns:
            True if asset data is valid, False otherwise
        """
        required_fields = ['id', 'name', 'assetClass', 'organization', 'status']
        return all(field in data for field in required_fields)
    
    def _extract_parent_cloud_id(self, data: Dict[str, Any]) -> Optional[str]:
        """Extract parent cloud ID from asset attributions."""
        attributions = data.get('assetAttributions', [])
        for attribution in attributions:
            parent_cloud = attribution.get('parentCloud')
            if parent_cloud:
                return parent_cloud.get('externalId')
        return None
    
    def _extract_parent_cloud_name(self, data: Dict[str, Any]) -> Optional[str]:
        """Extract parent cloud name from asset attributions."""
        attributions = data.get('assetAttributions', [])
        for attribution in attributions:
            parent_cloud = attribution.get('parentCloud')
            if parent_cloud:
                return parent_cloud.get('name')
        return None
    
    def _count_teams(self, data: Dict[str, Any]) -> int:
        """Count unique teams in asset attributions."""
        teams = set()
        attributions = data.get('assetAttributions', [])
        for attribution in attributions:
            team = attribution.get('team')
            if team:
                teams.add(team.get('externalId'))
        return len(teams)
    
    def get_assets_by_parent_cloud(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Group all assets by parent cloud.
        Assets with missing parent cloud are grouped under "Zombie" cloud.
        Processes files one by one for memory efficiency.
        
        Returns:
            Dictionary mapping parent cloud ID to list of assets
        """
        parent_cloud_assets = defaultdict(list)
        ZOMBIE_CLOUD_ID = "zombie_cloud"
        
        # Process files one by one instead of loading all into memory
        for filename in self.list_files():
            asset_data = self.get_file_data(filename)
            if asset_data:
                processed_data = self.process_data(asset_data)
                if processed_data and processed_data.get('_is_valid', False):
                    parent_cloud_id = processed_data.get('_parent_cloud_id')
                    
                    # If no parent cloud, assign to Zombie cloud
                    if not parent_cloud_id:
                        parent_cloud_id = ZOMBIE_CLOUD_ID
                        # Update the asset data to reflect Zombie cloud assignment
                        processed_data['_parent_cloud_id'] = ZOMBIE_CLOUD_ID
                        processed_data['_parent_cloud_name'] = "Zombie Cloud"
                    
                    parent_cloud_assets[parent_cloud_id].append(processed_data)
        
        return dict(parent_cloud_assets)
    
    def get_teams_by_parent_cloud(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Group all teams by parent cloud.
        Assets with missing parent cloud are grouped under "Zombie" cloud.
        Processes files one by one for memory efficiency.
        
        Returns:
            Dictionary mapping parent cloud ID to list of unique teams
        """
        parent_cloud_teams = defaultdict(set)
        ZOMBIE_CLOUD_ID = "zombie_cloud"
        
        # Process files one by one instead of loading all into memory
        for filename in self.list_files():
            asset_data = self.get_file_data(filename)
            if asset_data:
                attributions = asset_data.get('assetAttributions', [])
                
                # Check if asset has any attributions with parent cloud
                has_parent_cloud = any(attribution.get('parentCloud') for attribution in attributions)
                
                for attribution in attributions:
                    parent_cloud = attribution.get('parentCloud')
                    team = attribution.get('team')
                    
                    if team:
                        # Determine parent cloud ID
                        if parent_cloud:
                            parent_cloud_id = parent_cloud.get('externalId')
                        else:
                            # If no parent cloud in this attribution, use Zombie cloud
                            parent_cloud_id = ZOMBIE_CLOUD_ID
                        
                        team_data = {
                            'externalId': team.get('externalId'),
                            'name': team.get('name'),
                            'type': team.get('type'),
                            'status': team.get('status'),
                            'lead': team.get('lead', {}),
                            'cloud': {
                                'externalId': parent_cloud_id,
                                'name': parent_cloud.get('name', 'Zombie Cloud') if parent_cloud else 'Zombie Cloud',
                                'type': parent_cloud.get('type', 'zombie_cloud') if parent_cloud else 'zombie_cloud'
                            }
                        }
                        parent_cloud_teams[parent_cloud_id].add(json.dumps(team_data, sort_keys=True))
                
                # If asset has no attributions at all, create a default team for Zombie cloud
                if not attributions:
                    default_team_data = {
                        'externalId': 'zombie_team',
                        'name': 'Zombie Team',
                        'type': 'zombie_team',
                        'status': 'unknown',
                        'lead': {},
                        'cloud': {
                            'externalId': ZOMBIE_CLOUD_ID,
                            'name': 'Zombie Cloud',
                            'type': 'zombie_cloud'
                        }
                    }
                    parent_cloud_teams[ZOMBIE_CLOUD_ID].add(json.dumps(default_team_data, sort_keys=True))
        
        # Convert sets back to lists of dictionaries
        result = {}
        for parent_cloud_id, team_set in parent_cloud_teams.items():
            result[parent_cloud_id] = [json.loads(team_json) for team_json in team_set]
        
        return result
    
    def get_team_assets_mapping(self) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
        """
        Get mapping of parent cloud -> team -> assets.
        Assets with missing parent cloud are grouped under "Zombie" cloud.
        Processes files one by one for memory efficiency.
        
        Returns:
            Nested dictionary: parent_cloud_id -> team_id -> list of assets
        """
        team_assets_mapping = defaultdict(lambda: defaultdict(list))
        ZOMBIE_CLOUD_ID = "zombie_cloud"
        ZOMBIE_TEAM_ID = "zombie_team"
        
        # Process files one by one instead of loading all into memory
        for filename in self.list_files():
            asset_data = self.get_file_data(filename)
            if asset_data:
                attributions = asset_data.get('assetAttributions', [])
                
                # Create simplified asset data
                simplified_asset = {
                    'id': asset_data.get('id'),
                    'name': asset_data.get('name'),
                    'identifier': asset_data.get('identifier'),
                    'assetClass': asset_data.get('assetClass'),
                    'status': asset_data.get('status'),
                    'organization': asset_data.get('organization'),
                    'properties': asset_data.get('properties', {}),
                    'lastSeenDate': asset_data.get('lastSeenDate'),
                    'createdDate': asset_data.get('createdDate')
                }
                
                if attributions:
                    # Process each attribution
                    for attribution in attributions:
                        parent_cloud = attribution.get('parentCloud')
                        team = attribution.get('team')
                        
                        # Determine parent cloud and team IDs
                        if parent_cloud and team:
                            parent_cloud_id = parent_cloud.get('externalId')
                            team_id = team.get('externalId')
                        elif team:
                            # Team exists but no parent cloud - assign to Zombie cloud
                            parent_cloud_id = ZOMBIE_CLOUD_ID
                            team_id = team.get('externalId')
                        else:
                            # No team - assign to Zombie cloud and team
                            parent_cloud_id = ZOMBIE_CLOUD_ID
                            team_id = ZOMBIE_TEAM_ID
                        
                        team_assets_mapping[parent_cloud_id][team_id].append(simplified_asset)
                else:
                    # No attributions at all - assign to Zombie cloud and team
                    team_assets_mapping[ZOMBIE_CLOUD_ID][ZOMBIE_TEAM_ID].append(simplified_asset)
        
        return dict(team_assets_mapping)
    
    def generate_parent_cloud_summary(self) -> List[Dict[str, Any]]:
        """
        Generate the required output format for each parent cloud.
        Assets with missing parent cloud are grouped under "Zombie" cloud.
        
        Returns:
            List of parent cloud summaries in the required format
        """
        parent_cloud_assets = self.get_assets_by_parent_cloud()
        parent_cloud_teams = self.get_teams_by_parent_cloud()
        team_assets_mapping = self.get_team_assets_mapping()
        
        result = []
        
        for parent_cloud_id in parent_cloud_assets.keys():
            # Get parent cloud details
            if parent_cloud_id == "zombie_cloud":
                parent_cloud_name = "Zombie Cloud"
                cloud_type = "zombie_cloud"
            else:
                # Get parent cloud details from first asset
                parent_cloud_name = None
                cloud_type = "parent_cloud"
                for asset in parent_cloud_assets[parent_cloud_id]:
                    attributions = asset.get('assetAttributions', [])
                    for attribution in attributions:
                        parent_cloud = attribution.get('parentCloud')
                        if parent_cloud and parent_cloud.get('externalId') == parent_cloud_id:
                            parent_cloud_name = parent_cloud.get('name')
                            cloud_type = parent_cloud.get('type', 'parent_cloud')
                            break
                    if parent_cloud_name:
                        break
                
                # Fallback if no name found
                if not parent_cloud_name:
                    parent_cloud_name = f"Unknown Cloud ({parent_cloud_id})"
            
            # Get teams for this parent cloud
            teams = parent_cloud_teams.get(parent_cloud_id, [])
            team_assets = team_assets_mapping.get(parent_cloud_id, {})
            
            # Build team assets list
            team_assets_list = []
            for team in teams:
                team_id = team.get('externalId')
                assets = team_assets.get(team_id, [])
                
                team_assets_list.append({
                    'team_name': team.get('name'),
                    'team_id': team_id,
                    'team_assets': assets,
                    'asset_count': len(assets)
                })
            
            # Create parent cloud summary
            parent_cloud_summary = {
                'cloud': {
                    'externalId': parent_cloud_id,
                    'name': parent_cloud_name,
                    'type': cloud_type
                },
                'cloud_assets': parent_cloud_assets[parent_cloud_id],
                'total_teams': len(teams),
                'total_assets': len(parent_cloud_assets[parent_cloud_id]),
                'team_assets': team_assets_list
            }
            
            result.append(parent_cloud_summary)
        
        return result
    
    def save_parent_cloud_files(self, output_dir: Union[str, Path] = "output") -> List[Path]:
        """
        Save parent cloud summaries to individual JSON files.
        
        Args:
            output_dir: Directory to save the output files
            
        Returns:
            List of created file paths
        """
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        parent_cloud_summaries = self.generate_parent_cloud_summary()
        created_files = []
        
        for summary in parent_cloud_summaries:
            cloud_name = summary['cloud']['name']
            cloud_id = summary['cloud']['externalId']
            
            # Create safe filename
            safe_name = "".join(c for c in cloud_name if c.isalnum() or c in (' ', '-', '_')).rstrip()
            safe_name = safe_name.replace(' ', '_')
            filename = f"{safe_name}_{cloud_id}.json"
            
            file_path = output_path / filename
            
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(summary, f, indent=2, ensure_ascii=False)
            
            created_files.append(file_path)
            print(f"Created file: {file_path}")
        
        return created_files
    
    def get_asset_statistics(self) -> Dict[str, Any]:
        """
        Get overall statistics about the loaded assets.
        Includes Zombie cloud statistics for assets with missing parent cloud.
        Processes files one by one for memory efficiency.
        
        Returns:
            Dictionary with asset statistics
        """
        total_assets = 0
        valid_assets = 0
        
        # Process files one by one to count them
        for filename in self.list_files():
            total_assets += 1
            asset_data = self.get_file_data(filename)
            if asset_data:
                processed_data = self.process_data(asset_data)
                if processed_data and processed_data.get('_is_valid', False):
                    valid_assets += 1
        
        parent_clouds = self.get_assets_by_parent_cloud()
        teams = self.get_teams_by_parent_cloud()
        
        # Calculate Zombie cloud statistics
        zombie_assets = parent_clouds.get('zombie_cloud', [])
        zombie_teams = teams.get('zombie_cloud', [])
        
        return {
            'total_files_loaded': total_assets,
            'valid_assets': valid_assets,
            'invalid_assets': total_assets - valid_assets,
            'parent_clouds_count': len(parent_clouds),
            'total_teams': sum(len(team_list) for team_list in teams.values()),
            'assets_by_parent_cloud': {cloud_id: len(assets) for cloud_id, assets in parent_clouds.items()},
            'zombie_cloud_stats': {
                'zombie_assets_count': len(zombie_assets),
                'zombie_teams_count': len(zombie_teams),
                'zombie_assets_percentage': round((len(zombie_assets) / valid_assets * 100), 2) if valid_assets > 0 else 0
            }
        }
    
    def process_directory_with_progress(self, callback=None) -> Dict[str, Any]:
        """
        Process directory files one by one with optional progress callback.
        
        Args:
            callback: Optional callback function called for each file processed.
                     Signature: callback(filename, total_files, current_file, asset_data)
        
        Returns:
            Dictionary with processing results and statistics
        """
        files = self.list_files()
        total_files = len(files)
        processed_count = 0
        valid_count = 0
        parent_cloud_assets = defaultdict(list)
        parent_cloud_teams = defaultdict(set)
        team_assets_mapping = defaultdict(lambda: defaultdict(list))
        
        ZOMBIE_CLOUD_ID = "zombie_cloud"
        ZOMBIE_TEAM_ID = "zombie_team"
        
        for filename in files:
            processed_count += 1
            asset_data = self.get_file_data(filename)
            
            if asset_data:
                processed_data = self.process_data(asset_data)
                is_valid = processed_data and processed_data.get('_is_valid', False)
                
                if is_valid:
                    valid_count += 1
                    
                    # Process for parent cloud grouping
                    parent_cloud_id = processed_data.get('_parent_cloud_id')
                    if not parent_cloud_id:
                        parent_cloud_id = ZOMBIE_CLOUD_ID
                        processed_data['_parent_cloud_id'] = ZOMBIE_CLOUD_ID
                        processed_data['_parent_cloud_name'] = "Zombie Cloud"
                    
                    parent_cloud_assets[parent_cloud_id].append(processed_data)
                    
                    # Process for team grouping
                    attributions = asset_data.get('assetAttributions', [])
                    if attributions:
                        for attribution in attributions:
                            parent_cloud = attribution.get('parentCloud')
                            team = attribution.get('team')
                            
                            if team:
                                if parent_cloud:
                                    pc_id = parent_cloud.get('externalId')
                                else:
                                    pc_id = ZOMBIE_CLOUD_ID
                                
                                team_data = {
                                    'externalId': team.get('externalId'),
                                    'name': team.get('name'),
                                    'type': team.get('type'),
                                    'status': team.get('status'),
                                    'lead': team.get('lead', {}),
                                    'cloud': {
                                        'externalId': pc_id,
                                        'name': parent_cloud.get('name', 'Zombie Cloud') if parent_cloud else 'Zombie Cloud',
                                        'type': parent_cloud.get('type', 'zombie_cloud') if parent_cloud else 'zombie_cloud'
                                    }
                                }
                                parent_cloud_teams[pc_id].add(json.dumps(team_data, sort_keys=True))
                    else:
                        # No attributions - create zombie team
                        default_team_data = {
                            'externalId': 'zombie_team',
                            'name': 'Zombie Team',
                            'type': 'zombie_team',
                            'status': 'unknown',
                            'lead': {},
                            'cloud': {
                                'externalId': ZOMBIE_CLOUD_ID,
                                'name': 'Zombie Cloud',
                                'type': 'zombie_cloud'
                            }
                        }
                        parent_cloud_teams[ZOMBIE_CLOUD_ID].add(json.dumps(default_team_data, sort_keys=True))
                    
                    # Process for team assets mapping
                    simplified_asset = {
                        'id': asset_data.get('id'),
                        'name': asset_data.get('name'),
                        'identifier': asset_data.get('identifier'),
                        'assetClass': asset_data.get('assetClass'),
                        'status': asset_data.get('status'),
                        'organization': asset_data.get('organization'),
                        'properties': asset_data.get('properties', {}),
                        'lastSeenDate': asset_data.get('lastSeenDate'),
                        'createdDate': asset_data.get('createdDate')
                    }
                    
                    if attributions:
                        for attribution in attributions:
                            parent_cloud = attribution.get('parentCloud')
                            team = attribution.get('team')
                            
                            if parent_cloud and team:
                                pc_id = parent_cloud.get('externalId')
                                team_id = team.get('externalId')
                            elif team:
                                pc_id = ZOMBIE_CLOUD_ID
                                team_id = team.get('externalId')
                            else:
                                pc_id = ZOMBIE_CLOUD_ID
                                team_id = ZOMBIE_TEAM_ID
                            
                            team_assets_mapping[pc_id][team_id].append(simplified_asset)
                    else:
                        team_assets_mapping[ZOMBIE_CLOUD_ID][ZOMBIE_TEAM_ID].append(simplified_asset)
            
            # Call progress callback if provided
            if callback:
                callback(filename, total_files, processed_count, asset_data)
        
        # Convert team sets to lists
        teams_result = {}
        for pc_id, team_set in parent_cloud_teams.items():
            teams_result[pc_id] = [json.loads(team_json) for team_json in team_set]
        
        return {
            'total_files_processed': processed_count,
            'valid_assets': valid_count,
            'invalid_assets': processed_count - valid_count,
            'parent_cloud_assets': dict(parent_cloud_assets),
            'parent_cloud_teams': teams_result,
            'team_assets_mapping': dict(team_assets_mapping)
        }


# Example usage
if __name__ == "__main__":
    # Example usage of AssetDataLoader
    try:
        loader = AssetDataLoader("path/to/asset/files")
        
        # Get statistics
        stats = loader.get_asset_statistics()
        print(f"Asset Statistics: {stats}")
        
        # Generate parent cloud summaries
        summaries = loader.generate_parent_cloud_summary()
        print(f"Generated {len(summaries)} parent cloud summaries")
        
        # Save to files
        output_files = loader.save_parent_cloud_files("output")
        print(f"Created {len(output_files)} output files")
        
    except Exception as e:
        print(f"Error: {e}")
