"""
Security Asset Analyser - Concrete implementation for security-specific asset analysis.

This analyser focuses on security posture, compliance, and vulnerability
aspects of asset data.
"""

from typing import Any, Dict, List
from .asset import AssetAnalyser


class SecurityAnalyser(AssetAnalyser):
    """
    Concrete implementation of AssetAnalyser for security-specific analysis.
    
    This analyser focuses on security posture, compliance, and vulnerability
    aspects of asset data.
    """
    
    def __init__(self):
        """Initialize the security analyser."""
        super().__init__("security")
    
    def analyse(self, source_directory: str, result_directory: str) -> Dict[str, Any]:
        """
        Analyze assets for security analysis.
        
        Args:
            source_directory: Path to source directory
            result_directory: Path to result directory
            
        Returns:
            Analysis results dictionary
        """
        try:
            self.create_reader(source_directory)
            summary = self.get_security_summary()
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
        Get the list of asset fields specific to security analysis.
        
        Returns:
            List of asset field names to extract
        """
        return [
            'id', 'name', 'identifier', 'assetClass', 'organization',
            'status', 'properties.os_version', 'properties.platform',
            'properties.public_ip_address', 'properties.private_ip_address',
            'properties.hostname', 'properties.fqdn', 'properties.domain',
            'properties.mac_address', 'properties.serial_number'
        ]
    
    def get_cloud_fields(self) -> List[str]:
        """
        Get the list of cloud fields specific to security analysis.
        
        Returns:
            List of cloud field names to extract
        """
        return ['parent_cloud', 'cloud', 'team']
    
    def process_asset_specific_data(self, asset: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process asset-specific data for security analysis.
        
        Args:
            asset: Asset data dictionary
            
        Returns:
            Processed asset data with security-specific fields
        """
        processed_asset = asset.copy()
        
        # Add security-specific processing
        processed_asset['security_risk_score'] = self._calculate_security_risk_score(asset)
        processed_asset['exposure_level'] = self._assess_exposure_level(asset)
        processed_asset['compliance_status'] = self._check_compliance_status(asset)
        
        return processed_asset
    
    def get_security_summary(self) -> Dict[str, Any]:
        """
        Get security summary statistics using DuckDB SQL queries.
        
        Returns:
            Dictionary containing security summary data
            
        Raises:
            ValueError: If reader is not initialized
        """
        if not self.reader:
            raise ValueError("Reader not initialized. Call create_reader() first.")
        
        try:
            # Get total assets
            total_assets_result = self.reader.execute_query("SELECT COUNT(*) as total FROM assets")
            total_assets = total_assets_result[0]['total'] if total_assets_result else 0
            
            # Get assets with public IPs (potential exposure)
            public_ip_result = self.reader.execute_query("""
                SELECT COUNT(*) as total 
                FROM assets 
                WHERE properties_public_ip_address IS NOT NULL 
                AND properties_public_ip_address != ''
            """)
            assets_with_public_ip = public_ip_result[0]['total'] if public_ip_result else 0
            
            # Get assets with missing security info
            missing_security_result = self.reader.execute_query("""
                SELECT COUNT(*) as total 
                FROM assets 
                WHERE (properties_os_version IS NULL OR properties_os_version = '')
                AND (properties_platform IS NULL OR properties_platform = '')
            """)
            assets_missing_security = missing_security_result[0]['total'] if missing_security_result else 0
            
            return {
                'total_assets': total_assets,
                'assets_with_public_ip': assets_with_public_ip,
                'assets_missing_security': assets_missing_security,
                'exposure_percentage': (assets_with_public_ip / total_assets * 100) if total_assets > 0 else 0
            }
            
        except Exception as e:
            raise ValueError(f"Failed to get security summary: {str(e)}")
    
    def _calculate_security_risk_score(self, asset: Dict[str, Any]) -> int:
        """
        Calculate security risk score for an asset.
        
        Args:
            asset: Asset data dictionary
            
        Returns:
            Security risk score (0-100)
        """
        score = 0
        
        # Check for public IP (high risk)
        if asset.get('properties', {}).get('public_ip_address'):
            score += 40
        
        # Check for missing OS version (medium risk)
        if not asset.get('properties', {}).get('os_version'):
            score += 20
        
        # Check for missing platform info (low risk)
        if not asset.get('properties', {}).get('platform'):
            score += 10
        
        # Check for missing hostname (low risk)
        if not asset.get('properties', {}).get('hostname'):
            score += 10
        
        # Check for missing MAC address (low risk)
        if not asset.get('properties', {}).get('mac_address'):
            score += 10
        
        # Check for missing serial number (low risk)
        if not asset.get('properties', {}).get('serial_number'):
            score += 10
        
        return min(score, 100)
    
    def _assess_exposure_level(self, asset: Dict[str, Any]) -> str:
        """
        Assess exposure level for an asset.
        
        Args:
            asset: Asset data dictionary
            
        Returns:
            Exposure level ('High', 'Medium', 'Low')
        """
        properties = asset.get('properties', {})
        
        # High exposure: has public IP
        if properties.get('public_ip_address'):
            return 'High'
        
        # Medium exposure: has hostname but no public IP
        if properties.get('hostname') and not properties.get('public_ip_address'):
            return 'Medium'
        
        # Low exposure: internal only
        return 'Low'
    
    def _check_compliance_status(self, asset: Dict[str, Any]) -> str:
        """
        Check compliance status for an asset.
        
        Args:
            asset: Asset data dictionary
            
        Returns:
            Compliance status ('Compliant', 'Partial', 'Non-compliant')
        """
        properties = asset.get('properties', {})
        required_fields = ['os_version', 'platform', 'hostname']
        
        present_fields = sum(1 for field in required_fields if properties.get(field))
        
        if present_fields == len(required_fields):
            return 'Compliant'
        elif present_fields > 0:
            return 'Partial'
        else:
            return 'Non-compliant'