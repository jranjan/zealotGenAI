"""
Transformer CLI - Command line interface for data transformation
"""

import sys
from pathlib import Path

# Add the parent directory to Python path for imports
current_dir = Path(__file__).parent.parent
sys.path.insert(0, str(current_dir))

from transformer import TransformerFactory, TransformerType


class TransformerCLI:
    """CLI class for data transformation operations"""
    
    def __init__(self):
        self.transformer = None
    
    def run_transformation(self, source_folder: str, target_folder: str, transformer_type: str = "supersonic_flattener", **kwargs) -> None:
        """
        Run data transformation.
        
        Args:
            source_folder: Path to source directory containing JSON files
            target_folder: Path to target directory for transformed files
            transformer_type: Type of transformer to use ("flattener", "sonic_flattener", or "supersonic_flattener")
            **kwargs: Additional arguments for transformer initialization
        """
        try:
            # Create transformer using factory pattern
            transformer_type_map = {
                "flattener": TransformerType.FLATTENER,
                "sonic_flattener": TransformerType.SONIC_FLATTENER,
                "supersonic_flattener": TransformerType.SUPERSONIC_FLATTENER
            }
            
            if transformer_type not in transformer_type_map:
                print(f"❌ Error: Unknown transformer type: {transformer_type}")
                print(f"Available types: {', '.join(transformer_type_map.keys())}")
                sys.exit(1)
            
            # Use factory to create transformer
            self.transformer = TransformerFactory.create_transformer(
                transformer_type_map[transformer_type], 
                **kwargs
            )
            
            print(f"🔧 Transforming data from: {source_folder}")
            print(f"📁 Output directory: {target_folder}")
            print(f"🔧 Using transformer: {type(self.transformer).__name__}")
            print()
            
            # Note: No automatic cleanup - timestamped directories keep things organized
            
            # Transform directory
            result = self.transformer.transform_directory(source_folder, target_folder)
            
            if not result['success'] and result.get('error'):
                print(f"❌ Error: {result['error']}")
                sys.exit(1)
            
            self._display_results(result)
            
            # Analyze keys if requested
            if kwargs.get('analyze_keys', False):
                self._analyze_keys(source_folder, target_folder)
            
        except Exception as e:
            print(f"❌ Error: {str(e)}")
            sys.exit(1)
    
    def _display_results(self, result: dict) -> None:
        """
        Display transformation results.
        
        Args:
            result: Transformation results dictionary
        """
        print("📊 Transformation Results:")
        print(f"  Total files: {result['total_files']}")
        print(f"  Successful: {result['successful']}")
        print(f"  Failed: {result['failed']}")
        
        if result['failed'] > 0:
            print("\n❌ Failed files:")
            for file_info in result['files']:
                if file_info['status'] != "Success":
                    print(f"  - {Path(file_info['input']).name}: {file_info['status']}")
    
    def _analyze_keys(self, source_folder: str, target_folder: str) -> None:
        """
        Analyze flattened keys.
        
        Args:
            source_folder: Path to source directory
            target_folder: Path to target directory
        """
        print("\n🔍 Analyzing flattened keys...")
        
        # Analyze key mapping
        print("\n📋 Key Transformation Analysis:")
        key_mapping = self.transformer.analyze_key_mapping(source_folder, target_folder)
        
        if key_mapping:
            print(f"  Analyzed {key_mapping.get('files_analyzed', 0)} files with {key_mapping.get('total_mappings', 0)} key transformations")
            print("\n  Original → Flattened Key Mappings:")
            
            for original_key, flattened_keys in key_mapping.get('mappings', {}).items():
                for flattened_key in flattened_keys:
                    transformation = self._describe_transformation(original_key, flattened_key)
                    print(f"    {original_key} → {flattened_key} ({transformation})")
        
        # Analyze key categories
        key_categories = self.transformer.analyze_flattened_keys(target_folder)
        
        if key_categories:
            print("\n📋 Flattened Keys by Category:")
            
            print("\n🔑 Core Keys:")
            for key in key_categories.get('core', []):
                print(f"  • {key}")
            
            print("\n📝 Properties:")
            for key in key_categories.get('properties', []):
                print(f"  • {key}")
            
            print("\n🏢 Asset Attribution:")
            for key in key_categories.get('asset_attribution', []):
                print(f"  • {key}")
            
            total_keys = sum(len(keys) for keys in key_categories.values())
            print(f"\n📊 Total flattened keys: {total_keys}")
        else:
            print("❌ Could not analyze keys - no output files found")
    
    def _describe_transformation(self, original_key: str, flattened_key: str) -> str:
        """
        Describe the type of transformation applied to a key.
        
        Args:
            original_key: Original key name
            flattened_key: Flattened key name
            
        Returns:
            Description of the transformation
        """
        if original_key == flattened_key:
            return "No change"
        elif "." in flattened_key and "." not in original_key:
            return "Nested → Dot notation"
        elif "[" in flattened_key and "[" not in original_key:
            return "Array → Indexed notation"
        elif len(flattened_key) > len(original_key):
            return "Expanded path"
        else:
            return "Modified"
    
    def validate_input(self, source_folder: str, target_folder: str) -> bool:
        """
        Validate input parameters.
        
        Args:
            source_folder: Path to source directory
            target_folder: Path to target directory
            
        Returns:
            True if valid, False otherwise
        """
        if not source_folder:
            print("❌ Error: Source directory path is required")
            return False
        
        if not target_folder:
            print("❌ Error: Target directory path is required")
            return False
        
        if not Path(source_folder).exists():
            print(f"❌ Error: Source directory does not exist: {source_folder}")
            return False
        
        if not Path(source_folder).is_dir():
            print(f"❌ Error: Source path is not a directory: {source_folder}")
            return False
        
        return True
