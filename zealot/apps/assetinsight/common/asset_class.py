"""
Asset Class enumeration for the AssetInsight application.

This module defines the supported asset types that can be analyzed
and processed by the AssetInsight system.
"""

from enum import Enum


class AssetClass(Enum):
    """
    Enumeration of supported asset classes in the AssetInsight system.
    
    Each asset class represents a different type of infrastructure component
    that can be discovered, analyzed, and managed.
    """
    
    # Scan Entity
    SERVER = ("server", "Server", "servers")
    NETWORK_DEVICE = ("network_device", "Network Device", "network_device")
    RDS = ("aws_rds_instance", "RDS", "rds")
    AWS_EC2 = ("aws_ec2_instance", "AWS EC2", "ec2")
    
    # AWS Services
    AWS_ACCOUNT = ("aws_account", "AWS Account", "aws_accounts")
    AWS_EKS_CLUSTER = ("aws_eks_cluster", "AWS EKS", "containers")
    AWS_SQS = ("aws_sqs", "AWS SQS", "aws_services")
    
    # Application Services
    SERVICE = ("service", "Service", "services")
    
    def __str__(self) -> str:
        """
        Return the string representation of the asset class.
        
        Returns:
            The asset class name as a string
        """
        return self.value[0]
    
    @property
    def class_name(self) -> str:
        """
        Get the class name (first element of the tuple).
        
        Returns:
            The class name as a string
        """
        return self.value[0]
    
    @property
    def user_name(self) -> str:
        """
        Get the user-friendly name (second element of the tuple).
        
        Returns:
            The user-friendly display name
        """
        return self.value[1]
    
    @property
    def table_name(self) -> str:
        """
        Get the database table name (third element of the tuple).
        
        Returns:
            The database table name
        """
        return self.value[2]
    
    @classmethod
    def from_string(cls, value: str) -> 'AssetClass':
        """
        Create an AssetClass from a string value.
        
        Args:
            value: String representation of the asset class
            
        Returns:
            AssetClass enum instance
            
        Raises:
            ValueError: If the value is not a valid asset class
        """
        for asset_class in cls:
            if asset_class.value[0] == value:
                return asset_class
        raise ValueError(f"Invalid asset class: {value}")
    
    @classmethod
    def get_all_values(cls) -> list[str]:
        """
        Get all asset class values as a list of strings.
        
        Returns:
            List of all asset class names
        """
        return [asset_class.value[0] for asset_class in cls]
    
    @classmethod
    def get_all_user_names(cls) -> list[str]:
        """
        Get all user-friendly names as a list of strings.
        
        Returns:
            List of all user-friendly display names
        """
        return [asset_class.value[1] for asset_class in cls]
    
    @classmethod
    def get_class_user_mapping(cls) -> dict[str, str]:
        """
        Get a mapping of class names to user names.
        
        Returns:
            Dictionary mapping class names to user names
        """
        return {asset_class.value[0]: asset_class.value[1] for asset_class in cls}
    
    @property
    def display_name(self) -> str:
        """
        Get a human-readable display name for the asset class.
        
        Returns:
            The user-friendly display name
        """
        return self.value[1]
    
    @classmethod
    def get_all_table_names(cls) -> list[str]:
        """
        Get all table names from the enum.
        
        Returns:
            List of all table names
        """
        return list(set(asset_class.table_name for asset_class in cls))
    
    @classmethod
    def get_asset_classes_for_table(cls, table_name: str) -> list[str]:
        """
        Get all asset class names that map to a specific table.
        
        Args:
            table_name: The table name to search for
            
        Returns:
            List of asset class names that map to the table
        """
        return [asset_class.class_name for asset_class in cls if asset_class.table_name == table_name]
