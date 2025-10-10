"""
Asset Owner Analyser - Handles asset ownership analysis
"""

from typing import Dict, Any, List
from pathlib import Path
import json
import os
from datetime import datetime
from collections import defaultdict
from .asset import AssetAnalyser


class AssetOwnerAnalyser(AssetAnalyser):
    """Handles asset ownership analysis operations"""
    
    def __init__(self):
        super().__init__()
    
    def analyse_with_config(self, config, source_directory: str, result_directory: str) -> Dict[str, Any]:
        """
        Analyze assets using configuration.
        
        Args:
            config: Configuration object
            source_directory: Path to source directory
            result_directory: Path to result directory
            
        Returns:
            Analysis results dictionary
        """
        try:
            # Create timestamped result directory
            timestamp = datetime.now().strftime("%Y_%m_%d_%H%M%S")
            result_path = Path(result_directory) / f"analysis_{timestamp}"
            result_path.mkdir(parents=True, exist_ok=True)
            
            # Process files
            results = self._process_assets(config, source_directory, str(result_path))
            
            return results
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'total_clouds': 0,
                'total_assets': 0,
                'clouds': {}
            }
    
    def _process_assets(self, config, source_directory: str, result_directory: str) -> Dict[str, Any]:
        """Process assets and generate results"""
        source_path = Path(source_directory)
        json_files = list(source_path.glob("*.json"))
        
        if not json_files:
            return {
                'success': True,
                'total_clouds': 0,
                'total_assets': 0,
                'clouds': {}
            }
        
        # Group assets by parent cloud
        cloud_groups = defaultdict(lambda: {'assets': [], 'teams': set()})
        total_assets = 0
        
        for json_file in json_files:
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                assets = data if isinstance(data, list) else [data]
                
                for asset in assets:
                    total_assets += 1
                    
                    # Get parent cloud
                    parent_cloud = self._get_parent_cloud(asset)
                    cloud_groups[parent_cloud]['assets'].append(asset)
                    
                    # Extract team information
                    team_name = self._get_team_name(asset)
                    if team_name:
                        cloud_groups[parent_cloud]['teams'].add(team_name)
                        
            except Exception as e:
                print(f"Error processing {json_file}: {e}")
                continue
        
        # Generate results
        results = {
            'success': True,
            'total_clouds': len(cloud_groups),
            'total_assets': total_assets,
            'clouds': {}
        }
        
        for parent_cloud, group_data in cloud_groups.items():
            cloud_result = self._create_cloud_result(parent_cloud, group_data, config)
            results['clouds'][parent_cloud] = cloud_result
            
            # Write individual cloud result file
            output_file = result_directory / f"{parent_cloud.replace(' ', '_')}.json"
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(cloud_result, f, indent=2, ensure_ascii=False)
        
        return results
    
    def _get_parent_cloud(self, asset: Dict[str, Any]) -> str:
        """Extract parent cloud name from asset"""
        try:
            attributions = asset.get('assetAttributions', [])
            if attributions and len(attributions) > 0:
                return attributions[0].get('parentCloud', {}).get('name', 'Unknown')
            return 'Zombie Cloud'
        except:
            return 'Zombie Cloud'
    
    def _get_team_name(self, asset: Dict[str, Any]) -> str:
        """Extract team name from asset"""
        try:
            attributions = asset.get('assetAttributions', [])
            if attributions and len(attributions) > 0:
                return attributions[0].get('team', {}).get('name', 'Unknown')
            return 'Unknown'
        except:
            return 'Unknown'
    
    def _create_cloud_result(self, parent_cloud: str, group_data: Dict, config) -> Dict[str, Any]:
        """Create result structure for a cloud"""
        assets = group_data['assets']
        teams = list(group_data['teams'])
        
        # Group assets by team
        team_groups = defaultdict(list)
        for asset in assets:
            team_name = self._get_team_name(asset)
            team_groups[team_name].append(asset)
        
        # Create team results
        team_results = []
        for team_name, team_assets in team_groups.items():
            team_result = {
                'team_name': team_name,
                'total_team_assets': len(team_assets),
                'assets': self._extract_asset_fields(team_assets, config)
            }
            team_results.append(team_result)
        
        return {
            'cloud': parent_cloud,
            'total_cloud_team': len(teams),
            'total_cloud_assets': len(assets),
            'team': team_results
        }
    
    def _extract_asset_fields(self, assets: List[Dict], config) -> List[Dict]:
        """Extract specified fields from assets"""
        asset_fields = getattr(config, 'asset_fields', [])
        cloud_fields = getattr(config, 'cloud_fields', [])
        
        extracted_assets = []
        for asset in assets:
            extracted_asset = {}
            
            # Extract basic asset fields
            for field in asset_fields:
                extracted_asset[field] = asset.get(field, None)
            
            # Extract cloud-related fields
            attributions = asset.get('assetAttributions', [])
            if attributions and len(attributions) > 0:
                attribution = attributions[0]
                for field in cloud_fields:
                    if field == 'parent_cloud':
                        extracted_asset[field] = attribution.get('parentCloud', {}).get('name', 'Unknown')
                    elif field == 'cloud':
                        extracted_asset[field] = attribution.get('cloud', {}).get('name', 'Unknown')
                    elif field == 'team':
                        extracted_asset[field] = attribution.get('team', {}).get('name', 'Unknown')
                    else:
                        extracted_asset[field] = attribution.get(field, 'Unknown')
            else:
                for field in cloud_fields:
                    extracted_asset[field] = 'Unknown'
            
            extracted_assets.append(extracted_asset)
        
        return extracted_assets
