"""
SonicFlattener - Multithreaded data flattening operations
"""

import json
import os
from pathlib import Path
from typing import Dict, List, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from .flattener import Flattener


class SonicFlattener(Flattener):
    """Multithreaded data flattening operations for high-performance processing"""
    
    def __init__(self, max_workers: int = None):
        super().__init__()
        self.max_workers = max_workers or min(32, (os.cpu_count() or 1) + 4)
    
    def transform_directory(self, source_dir: str, target_dir: str) -> Dict[str, Any]:
        """
        Transform all JSON files in source directory using multithreading.
        
        Args:
            source_dir: Source directory containing JSON files
            target_dir: Target directory for transformed files
            
        Returns:
            Dictionary with transformation results
        """
        start_time = time.time()
        
        # Use base class setup logic
        setup_result = self.setup_directories(source_dir, target_dir)
        if not setup_result['success']:
            return setup_result
        
        # Extract setup results
        source_path = setup_result['source_path']
        target_path = setup_result['target_path']
        json_files = setup_result['json_files']
        total_files = setup_result['total_files']
        
        print(f"🚀 SonicFlattener: Processing {total_files} files with {self.max_workers} threads")
        
        # Process files in parallel
        results = []
        successful = 0
        failed = 0
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all files for processing
            future_to_file = {
                executor.submit(self._process_single_file, str(file_path), target_dir): file_path 
                for file_path in json_files
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_file):
                file_path = future_to_file[future]
                try:
                    result = future.result()
                    results.append(result)
                    
                    if result['success']:
                        successful += 1
                        print(f"✅ Processed: {file_path.name}")
                    else:
                        failed += 1
                        print(f"❌ Failed: {file_path.name} - {result.get('error', 'Unknown error')}")
                        
                except Exception as e:
                    failed += 1
                    error_result = {
                        'success': False,
                        'input': str(file_path),
                        'output': None,
                        'error': str(e),
                        'source_assets': 0,
                        'normalised_assets': 0,
                        'missing_attribution': 0,
                        'missing_properties': 0
                    }
                    results.append(error_result)
                    print(f"❌ Exception: {file_path.name} - {str(e)}")
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        print(f"🎯 SonicFlattener: Completed {total_files} files in {processing_time:.2f}s ({total_files / processing_time:.1f} files/sec)")
        
        return self.create_result_dict(
            success=True,
            total_files=total_files,
            successful=successful,
            failed=failed,
            files=results,
            processing_time=processing_time
        )
    
    def transform_file(self, source_file: str, target_file: str) -> bool:
        """
        Transform a single JSON file.
        
        Args:
            source_file: Path to source file
            target_file: Path to target file
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            Path(target_file).parent.mkdir(parents=True, exist_ok=True)
            result = self._process_single_file(source_file, str(Path(target_file).parent))
            return result['success']
        except Exception as e:
            print(f"Error transforming file {source_file}: {e}")
            return False
    
    def _process_single_file(self, file_path: str, target_dir: str) -> Dict[str, Any]:
        """
        Process a single JSON file and create flattened version.
        
        Args:
            file_path: Path to source JSON file
            target_dir: Target directory for output
            
        Returns:
            Dictionary with processing results
        """
        try:
            source_path = Path(file_path)
            target_path = Path(target_dir)
            
            # Create flattened filename
            flattened_filename = f"{source_path.stem}_flattened.json"
            output_path = target_path / flattened_filename
            
            # Load and normalize JSON data using base class method
            assets = self._load_and_normalize_json(file_path)
            
            # Flatten assets with analysis using base class method
            flattened_assets, missing_attribution, missing_properties, missing_name, missing_parent_cloud = self._flatten_assets_with_analysis(assets)
            
            # Save flattened data
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(flattened_assets, f, indent=2, ensure_ascii=False)
            
            return self.create_file_result(
                success=True,
                file_path=str(source_path),
                input=str(source_path),
                output=str(output_path),
                source_assets=len(assets),
                normalised_assets=len(flattened_assets),
                missing_attribution=missing_attribution,
                missing_properties=missing_properties,
                missing_name=missing_name,
                missing_parent_cloud=missing_parent_cloud
            )
            
        except Exception as e:
            return self.create_file_result(
                success=False,
                file_path=file_path,
                error=str(e),
                input=file_path,
                output=None,
                source_assets=0,
                normalised_assets=0,
                missing_attribution=0,
                missing_properties=0
            )
    
    
    def get_performance_info(self) -> Dict[str, Any]:
        """Get performance information about the flattener"""
        return {
            'max_workers': self.max_workers,
            'cpu_count': os.cpu_count(),
            'flattener_type': 'SonicFlattener (Multithreaded)'
        }
    
