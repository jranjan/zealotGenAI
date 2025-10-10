"""
Asset Analyser - Processes asset data based on YAML configuration.

This module provides an analyser that reads YAML configuration files and processes
each asset class to create organized results in the specified result directory.
"""

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from abc import ABC, abstractmethod


@dataclass
class AssetClassResult:
    """Represents the result for a specific asset class."""
    asset_class: str
    source_path: str
    result_path: str
    total_assets: int
    parent_clouds: List[Dict[str, Any]]
    processing_stats: Dict[str, Any]


class AssetAnalyser(ABC):
    """
    Abstract base class for asset analysers that process asset data from directories.
    
    This class processes asset data from source directories and creates
    organized results in the specified result directory. The caller
    provides all parameters directly.
    """
    
    def __init__(self):
        """
        Initialize the asset analyser.
        """
        self.results: List[AssetClassResult] = []
    
    @abstractmethod
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
        pass
    
    def _create_simplified_asset(self, asset: Dict[str, Any], asset_fields: List[str], cloud_fields: List[str] = None) -> Dict[str, Any]:
        """
        Create a simplified asset with only the specified fields.
        
        Args:
            asset: Original asset data
            asset_fields: List of asset field names to include
            cloud_fields: List of cloud field names to include
            
        Returns:
            Simplified asset with only specified fields
        """
        simplified = {}
        
        # Add asset fields
        for field in asset_fields:
            if field in asset:
                simplified[field] = asset[field]
            elif field in asset.get('properties', {}):
                simplified[field] = asset['properties'][field]
            else:
                simplified[field] = None
        
        # Add cloud fields from assetAttributions (only names)
        if cloud_fields:
            if 'assetAttributions' in asset and asset.get('assetAttributions'):
                # Asset has attributions - extract from them
                for attr in asset.get('assetAttributions', []):
                    if attr and isinstance(attr, dict):
                        if 'parentCloud' in cloud_fields and attr.get('parentCloud'):
                            parent_cloud = attr['parentCloud']
                            simplified['parentCloud'] = parent_cloud.get('name') if isinstance(parent_cloud, dict) else None
                        if 'cloud' in cloud_fields and attr.get('cloud'):
                            cloud = attr['cloud']
                            simplified['cloud'] = cloud.get('name') if isinstance(cloud, dict) else None
                        if 'team' in cloud_fields and attr.get('team'):
                            team = attr['team']
                            simplified['team'] = team.get('name') if isinstance(team, dict) else None
                        break  # Use first valid attribution
            else:
                # Asset has no attributions - set to TBD for Zombie Cloud
                if 'parentCloud' in cloud_fields:
                    simplified['parentCloud'] = 'Zombie Cloud'
                if 'cloud' in cloud_fields:
                    simplified['cloud'] = 'TBD'
                if 'team' in cloud_fields:
                    simplified['team'] = 'TBD'
        
        return simplified

    def process_assets(self, assets: List[Dict[str, Any]], asset_fields: List[str] = None, cloud_fields: List[str] = None) -> List[Dict[str, Any]]:
        """
        Process assets and group by parent cloud.
        
        Args:
            assets: List of asset data dictionaries
            
        Returns:
            List of parent cloud summaries
        """
        clouds = defaultdict(lambda: {'assets': [], 'teams': set()})
        
        for asset in assets:
            try:
                asset_processed = False
                
                # Create simplified asset if fields are specified
                simplified_asset = asset
                if asset_fields or cloud_fields:
                    simplified_asset = self._create_simplified_asset(asset, asset_fields or [], cloud_fields)
                
                # Check if asset has assetAttributions (original format)
                if 'assetAttributions' in asset and asset.get('assetAttributions'):
                    for attr in asset.get('assetAttributions', []):
                        # Skip null attributions
                        if not attr or not isinstance(attr, dict):
                            continue
                            
                        pc = attr.get('parentCloud') or {}
                        team = attr.get('team') or {}
                        
                        # Ensure pc and team are dictionaries
                        if not isinstance(pc, dict) or not isinstance(team, dict):
                            continue
                        
                        if pc.get('externalId') and team.get('externalId'):
                            cloud_id = pc['externalId']
                            team_name = team.get('name') or 'unknown'
                            clouds[cloud_id]['assets'].append(simplified_asset)
                            clouds[cloud_id]['teams'].add(team_name)
                            asset_processed = True
                
                # If asset wasn't processed through attributions, put in Zombie
                if not asset_processed:
                    cloud_id = 'zombie_cloud'
                    team_name = 'TBD'
                    
                    clouds[cloud_id]['assets'].append(simplified_asset)
                    clouds[cloud_id]['teams'].add(team_name)
                    asset_processed = True
                    
            except Exception as e:
                print(f"⚠️  Error processing asset {asset.get('id', 'unknown')}: {e}")
                import traceback
                traceback.print_exc()
                # Put failed assets in Zombie
                simplified_asset = asset
                if asset_fields or cloud_fields:
                    simplified_asset = self._create_simplified_asset(asset, asset_fields or [], cloud_fields)
                clouds['zombie_cloud']['assets'].append(simplified_asset)
                clouds['zombie_cloud']['teams'].add('TBD')
                continue
        
        # Create cloud name mapping for efficient lookup
        cloud_name_map = {'zombie_cloud': 'Zombie Cloud'}
        
        # Build cloud name mapping from original assets
        for original_asset in assets:
            if 'assetAttributions' in original_asset:
                for attr in original_asset.get('assetAttributions', []):
                    if attr and isinstance(attr, dict):
                        parent_cloud = attr.get('parentCloud') or {}
                        if isinstance(parent_cloud, dict) and parent_cloud.get('externalId'):
                            cloud_id = parent_cloud.get('externalId')
                            if cloud_id not in cloud_name_map:
                                cloud_name_map[cloud_id] = parent_cloud.get('name', 'Unknown')
            else:
                # For direct format, use organization as cloud name
                org = original_asset.get('organization')
                if org and 'direct_org' not in cloud_name_map:
                    cloud_name_map['direct_org'] = org
        
        # Generate output
        result = []
        for cloud_id, data in clouds.items():
            # Get cloud name from mapping
            cloud_name = cloud_name_map.get(cloud_id, 'Unknown')
            
            # Group assets by team and collect cloud fields
            team_assets = defaultdict(lambda: {'assets': [], 'cloud_info': {}})
            
            # Create asset ID to original asset mapping for efficient lookup
            asset_id_map = {asset.get('id'): asset for asset in assets if asset.get('id')}
            
            for simplified_asset in data['assets']:
                try:
                    team_name = "TBD"
                    cloud_info = {}
                    
                    # Find the corresponding original asset using the mapping
                    original_asset = asset_id_map.get(simplified_asset.get('id'))
                    
                    if original_asset and 'assetAttributions' in original_asset:
                        for attr in original_asset.get('assetAttributions', []):
                            if attr and isinstance(attr, dict):
                                pc = attr.get('parentCloud') or {}
                                team = attr.get('team') or {}
                                cloud = attr.get('cloud') or {}
                                
                                if isinstance(pc, dict) and pc.get('externalId') == cloud_id:
                                    team_name = team.get('name', 'Unknown Team') if isinstance(team, dict) else 'Unknown Team'
                                    
                                    # Extract cloud fields if specified
                                    if cloud_fields:
                                        if 'parentCloud' in cloud_fields and pc:
                                            cloud_info['parentCloud'] = pc
                                        if 'cloud' in cloud_fields and cloud:
                                            cloud_info['cloud'] = cloud
                                        if 'team' in cloud_fields and team:
                                            cloud_info['team'] = team
                                    break
                    
                    # If no team found in attributions, keep as TBD for Zombie Cloud
                    # team_name is already set to "TBD" by default
                    
                    team_assets[team_name]['assets'].append(simplified_asset)
                    if cloud_info:
                        team_assets[team_name]['cloud_info'] = cloud_info
                        
                except Exception as e:
                    print(f"⚠️  Error processing asset for team grouping {simplified_asset.get('id', 'unknown')}: {e}")
                    team_assets['TBD']['assets'].append(simplified_asset)
            
            # Convert team_assets to hierarchical structure
            teams = []
            for team_name, team_data in team_assets.items():
                team_structure = {
                    'team_name': team_name,  # Add team name to the structure
                    'total_team_assets': len(team_data['assets']),
                    'assets': team_data['assets']
                }
                
                # Add cloud fields if available
                if team_data['cloud_info']:
                    team_structure.update(team_data['cloud_info'])
                
                teams.append(team_structure)
            
            result.append({
                'cloud': cloud_name,
                'total_cloud_team': len(team_assets),
                'total_cloud_assets': len(data['assets']),
                'team': teams
            })
        
        return result
    
    def load_assets_from_directory(self, source_path: Union[str, Path]) -> List[Dict[str, Any]]:
        """
        Load all asset JSON files from a directory.
        
        Args:
            source_path: Path to directory containing asset JSON files
            
        Returns:
            List of asset data dictionaries
        """
        source_dir = Path(source_path)
        if not source_dir.exists():
            raise FileNotFoundError(f"Source directory not found: {source_path}")
        
        if not source_dir.is_dir():
            raise ValueError(f"Path is not a directory: {source_path}")
        
        assets = []
        json_files = list(source_dir.glob("*.json"))
        
        if not json_files:
            print(f"⚠️  No JSON files found in directory: {source_path}")
            return assets
        
        print(f"📁 Found {len(json_files)} JSON files in {source_path}")
        
        loaded_count = 0
        error_count = 0
        
        for json_file in json_files:
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    
                    # Handle both single asset and array of assets
                    if isinstance(data, list):
                        # Array of assets
                        for asset_data in data:
                            if self._is_valid_asset(asset_data):
                                assets.append(asset_data)
                                loaded_count += 1
                            else:
                                error_count += 1
                    elif isinstance(data, dict):
                        # Single asset
                        if self._is_valid_asset(data):
                            assets.append(data)
                            loaded_count += 1
                        else:
                            error_count += 1
                    else:
                        print(f"⚠️  Skipping invalid file format: {json_file.name}")
                        error_count += 1
                        
            except json.JSONDecodeError as e:
                print(f"❌ JSON decode error in {json_file.name}: {e}")
                error_count += 1
            except Exception as e:
                print(f"❌ Error loading {json_file.name}: {e}")
                error_count += 1
        
        print(f"📊 Loaded {loaded_count} valid assets, {error_count} errors")
        return assets
    
    def _is_valid_asset(self, asset_data: Dict[str, Any]) -> bool:
        """
        Validate basic asset data structure.
        
        Args:
            asset_data: Asset data to validate
            
        Returns:
            True if asset data is valid, False otherwise
        """
        if not isinstance(asset_data, dict):
            return False
        
        # Check for basic required fields (allow null values)
        required_fields = ['id', 'assetClass', 'organization']
        return all(field in asset_data for field in required_fields)
    
    
    def analyse_asset_class(self, asset_class_name: str, source_path: Union[str, Path], result_path: Union[str, Path], asset_fields: List[str] = None, cloud_fields: List[str] = None) -> AssetClassResult:
        """
        Analyse a specific asset class with direct parameters.
        
        Args:
            asset_class_name: Name of the asset class
            source_path: Path to directory containing asset JSON files
            result_path: Path to directory where results should be saved
            
        Returns:
            AssetClassResult with processing results
        """
        # Clear any previous results to ensure fresh processing
        self.results = []
        
        print(f"🔄 Analysing {asset_class_name} from {source_path}")
        print(f"📁 Result directory: {result_path}")
        
        # Load assets from source directory
        assets = self.load_assets_from_directory(source_path)
        
        if not assets:
            print("⚠️  No valid assets found in directory")
            return AssetClassResult(
                asset_class=asset_class_name,
                source_path=str(source_path),
                result_path=str(result_path),
                total_assets=0,
                parent_clouds=[],
                processing_stats={
                    'total_assets_processed': 0,
                    'total_teams_found': 0,
                    'parent_clouds_count': 0,
                    'files_created': 0,
                    'source_files_loaded': 0
                }
            )
        
        # Process assets
        try:
            parent_clouds = self.process_assets(assets, asset_fields, cloud_fields)
        except Exception as e:
            print(f"❌ Error in process_assets: {e}")
            import traceback
            traceback.print_exc()
            raise
        
        # Create timestamped result directory
        timestamp = datetime.now().strftime("%Y_%m_%d_%H%M%S")
        result_dir = Path(result_path) / timestamp
        result_dir.mkdir(parents=True, exist_ok=True)
        
        # Save results for each parent cloud
        created_files = []
        for cloud_data in parent_clouds:
            cloud_name = cloud_data['cloud']
            
            # Create safe filename with asset class as prefix
            safe_name = "".join(c for c in cloud_name if c.isalnum() or c in (' ', '-', '_')).rstrip()
            safe_name = safe_name.replace(' ', '_')
            filename = f"{asset_class_name}_{safe_name}.json"
            
            file_path = result_dir / filename
            
            # Wrap cloud data in the new structure
            total_cloud_assets = cloud_data['total_cloud_assets']
            total_teams = cloud_data['total_cloud_team']
            
            output_data = {
                'total_cloud': 1,
                'total_assets': total_cloud_assets,
                'cloud': {
                    'cloud': cloud_name,
                    'total_cloud_team': total_teams,
                    'total_cloud_assets': total_cloud_assets,
                    'team': cloud_data['team']
                }
            }
            
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=2, ensure_ascii=False)
            
            created_files.append(file_path)
            print(f"  📄 Created: {file_path}")
        
        # Calculate processing statistics
        # Use the actual number of assets loaded, not the sum of cloud assets (which can double-count)
        total_assets = len(assets)
        total_teams = sum(cloud['total_cloud_team'] for cloud in parent_clouds)
        
        processing_stats = {
            'total_assets_processed': total_assets,
            'total_teams_found': total_teams,
            'parent_clouds_count': len(parent_clouds),
            'files_created': len(created_files),
            'source_files_loaded': len(assets)
        }
        
        print(f"✅ Analysis complete: {total_assets} assets, {len(parent_clouds)} parent clouds")
        
        return AssetClassResult(
            asset_class=asset_class_name,
            source_path=str(source_path),
            result_path=str(result_path),
            total_assets=total_assets,
            parent_clouds=parent_clouds,
            processing_stats=processing_stats
        )


