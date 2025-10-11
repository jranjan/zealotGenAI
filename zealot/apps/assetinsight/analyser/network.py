"""
Network Asset Analyser - Concrete implementation for network-specific asset analysis.

This analyser focuses on network topology, connectivity, and infrastructure
aspects of asset data.
"""

from typing import Any, Dict, List
from .asset import AssetAnalyser


class NetworkAnalyser(AssetAnalyser):
    """
    Concrete implementation of AssetAnalyser for network-specific analysis.
    
    This analyser focuses on network topology, connectivity, and infrastructure
    aspects of asset data.
    """
    
    def __init__(self):
        """Initialize the network analyser."""
        super().__init__("network")
    
    def analyse_with_config(self, config, source_directory: str, result_directory: str) -> Dict[str, Any]:
        """
        Analyze assets using configuration for network analysis.
        
        Args:
            config: Configuration object
            source_directory: Path to source directory
            result_directory: Path to result directory
            
        Returns:
            Analysis results dictionary
        """
        try:
            self.create_reader(source_directory)
            summary = self.get_network_summary()
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
        Get the list of asset fields specific to network analysis.
        
        Returns:
            List of asset field names to extract
        """
        return [
            'id', 'name', 'identifier', 'assetClass', 'organization',
            'status', 'properties.hostname', 'properties.fqdn',
            'properties.private_ip_address', 'properties.public_ip_address',
            'properties.mac_address', 'properties.domain',
            'properties.substrate', 'properties.chassis_type'
        ]
    
    def get_cloud_fields(self) -> List[str]:
        """
        Get the list of cloud fields specific to network analysis.
        
        Returns:
            List of cloud field names to extract
        """
        return ['parent_cloud', 'cloud', 'team']
    
    def process_asset_specific_data(self, asset: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process asset-specific data for network analysis.
        
        Args:
            asset: Asset data dictionary
            
        Returns:
            Processed asset data with network-specific fields
        """
        processed_asset = asset.copy()
        
        # Add network-specific processing
        processed_asset['network_type'] = self._determine_network_type(asset)
        processed_asset['connectivity_status'] = self._assess_connectivity_status(asset)
        processed_asset['network_segment'] = self._determine_network_segment(asset)
        
        return processed_asset
    
    def get_network_summary(self) -> Dict[str, Any]:
        """
        Get network summary statistics using DuckDB SQL queries.
        
        Returns:
            Dictionary containing network summary data
            
        Raises:
            ValueError: If reader is not initialized
        """
        if not self.reader:
            raise ValueError("Reader not initialized. Call create_reader() first.")
        
        try:
            # Get total assets
            total_assets_result = self.reader.execute_query("SELECT COUNT(*) as total FROM assets")
            total_assets = total_assets_result[0]['total'] if total_assets_result else 0
            
            # Get assets with network connectivity
            network_assets_result = self.reader.execute_query("""
                SELECT COUNT(*) as total 
                FROM assets 
                WHERE (properties_private_ip_address IS NOT NULL AND properties_private_ip_address != '')
                OR (properties_public_ip_address IS NOT NULL AND properties_public_ip_address != '')
            """)
            network_assets = network_assets_result[0]['total'] if network_assets_result else 0
            
            # Get assets with hostnames
            hostname_assets_result = self.reader.execute_query("""
                SELECT COUNT(*) as total 
                FROM assets 
                WHERE properties_hostname IS NOT NULL AND properties_hostname != ''
            """)
            hostname_assets = hostname_assets_result[0]['total'] if hostname_assets_result else 0
            
            # Get assets with MAC addresses
            mac_assets_result = self.reader.execute_query("""
                SELECT COUNT(*) as total 
                FROM assets 
                WHERE properties_mac_address IS NOT NULL AND properties_mac_address != ''
            """)
            mac_assets = mac_assets_result[0]['total'] if mac_assets_result else 0
            
            return {
                'total_assets': total_assets,
                'network_assets': network_assets,
                'hostname_assets': hostname_assets,
                'mac_assets': mac_assets,
                'network_coverage': (network_assets / total_assets * 100) if total_assets > 0 else 0
            }
            
        except Exception as e:
            raise ValueError(f"Failed to get network summary: {str(e)}")
    
    def _determine_network_type(self, asset: Dict[str, Any]) -> str:
        """
        Determine network type for an asset.
        
        Args:
            asset: Asset data dictionary
            
        Returns:
            Network type ('Public', 'Private', 'Hybrid', 'Unknown')
        """
        properties = asset.get('properties', {})
        
        has_public_ip = bool(properties.get('public_ip_address'))
        has_private_ip = bool(properties.get('private_ip_address'))
        
        if has_public_ip and has_private_ip:
            return 'Hybrid'
        elif has_public_ip:
            return 'Public'
        elif has_private_ip:
            return 'Private'
        else:
            return 'Unknown'
    
    def _assess_connectivity_status(self, asset: Dict[str, Any]) -> str:
        """
        Assess connectivity status for an asset.
        
        Args:
            asset: Asset data dictionary
            
        Returns:
            Connectivity status ('Connected', 'Partially Connected', 'Disconnected')
        """
        properties = asset.get('properties', {})
        
        has_ip = bool(properties.get('private_ip_address') or properties.get('public_ip_address'))
        has_hostname = bool(properties.get('hostname'))
        has_mac = bool(properties.get('mac_address'))
        
        connection_indicators = sum([has_ip, has_hostname, has_mac])
        
        if connection_indicators >= 2:
            return 'Connected'
        elif connection_indicators == 1:
            return 'Partially Connected'
        else:
            return 'Disconnected'
    
    def _determine_network_segment(self, asset: Dict[str, Any]) -> str:
        """
        Determine network segment for an asset.
        
        Args:
            asset: Asset data dictionary
            
        Returns:
            Network segment ('DMZ', 'Internal', 'External', 'Unknown')
        """
        properties = asset.get('properties', {})
        
        # Check for public IP (likely external/DMZ)
        if properties.get('public_ip_address'):
            return 'DMZ'
        
        # Check for private IP (likely internal)
        if properties.get('private_ip_address'):
            return 'Internal'
        
        # Check for hostname patterns
        hostname = properties.get('hostname', '').lower()
        if 'dmz' in hostname or 'external' in hostname:
            return 'DMZ'
        elif 'internal' in hostname or 'lan' in hostname:
            return 'Internal'
        
        return 'Unknown'