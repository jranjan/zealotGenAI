"""
SonicFlattener - High-performance multiprocessing data flattening using FlattenerHelper

This module provides a high-performance multiprocessing flattener that leverages
FlattenerHelper for all flattening operations while using multiprocessing Pool
for maximum performance with large file counts.
"""

import os
from pathlib import Path
from typing import Dict, List, Any
from multiprocessing import Pool, cpu_count
import time
import warnings
import logging
from .basic import BasicFlattener

# Suppress Streamlit warnings in multiprocessing workers
warnings.filterwarnings("ignore", message=".*missing ScriptRunContext.*")
logging.getLogger("streamlit.runtime.scriptrunner_utils.script_run_context").setLevel(logging.ERROR)


class SonicFlattener(BasicFlattener):
    """
    High-performance multiprocessing flattener that uses FlattenerHelper for all operations.
    
    This class extends the base Flattener to provide maximum performance using
    multiprocessing Pool while maintaining full compatibility with FlattenerHelper patterns.
    """
    
    def __init__(self, max_workers: int = None, chunk_size: int = 100):
        """
        Initialize the SonicFlattener.
        
        Args:
            max_workers: Maximum number of worker processes (default: min(cpu_count, 16))
            chunk_size: Number of files to process per chunk (default: 100)
        """
        super().__init__()
        self.max_workers = max_workers or min(cpu_count(), 16)
        self.chunk_size = chunk_size
    
    def transform_directory(self, source_folder: str, target_folder: str) -> Dict[str, Any]:
        """
        Transform all JSON files in a source directory using multiprocessing.
        
        Args:
            source_folder: Path to source directory containing JSON files
            target_folder: Path to target directory for flattened files
            
        Returns:
            Dictionary containing transformation results and statistics
        """
        start_time = time.time()
        
        # Use base class setup logic
        setup_result = self.setup_directories(source_folder, target_folder)
        if not setup_result['success']:
            return setup_result
        
        source_path = setup_result['source_path']
        target_path = setup_result['target_path']
        json_files = setup_result['json_files']
        total_files = setup_result['total_files']
        
        if total_files == 0:
            return {
                'success': False,
                'error': 'No JSON files found in source directory',
                'total_files': 0,
                'successful': 0,
                'failed': 0,
                'files': []
            }
        
        print(f"🚀 SonicFlattener: Processing {total_files} files with {self.max_workers} processes")
        
        # Split files into chunks for processing
        file_chunks = [
            [str(f) for f in json_files[i:i + self.chunk_size]]
            for i in range(0, len(json_files), self.chunk_size)
        ]
        
        all_results = []
        successful = 0
        failed = 0
        
        # Process chunks in parallel using multiprocessing Pool
        with Pool(processes=self.max_workers) as pool:
            # Submit all chunks with target directory
            chunk_args = [(chunk, str(target_path)) for chunk in file_chunks]
            chunk_results = pool.starmap(_process_file_chunk, chunk_args)
            
            # Collect results
            for chunk_result in chunk_results:
                all_results.extend(chunk_result['results'])
                successful += chunk_result['successful']
                failed += chunk_result['failed']
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        print(f"🎯 SonicFlattener: Completed {total_files} files in {processing_time:.2f}s ({total_files / processing_time:.1f} files/sec)")
        
        return self.create_result_dict(
            success=True,
            total_files=total_files,
            successful=successful,
            failed=failed,
            files=all_results,
            processing_time=processing_time
        )
    
    def get_performance_info(self) -> Dict[str, Any]:
        """
        Get performance information about the SonicFlattener.
        
        Returns:
            Dictionary containing performance information
        """
        return {
            'flattener_type': 'SonicFlattener (Multiprocessing)',
            'uses_flattener_helper': True,
            'optimized': True,
            'max_workers': self.max_workers,
            'chunk_size': self.chunk_size,
            'cpu_count': cpu_count()
        }
    


def _process_file_chunk(file_paths: List[str], target_dir: str) -> Dict[str, Any]:
    """
    Process a chunk of files in a single process using FlattenerHelper.
    
    Args:
        file_paths: List of file paths to process
        target_dir: Target directory for output files
        
    Returns:
        Dictionary with chunk processing results
    """
    # Suppress Streamlit warnings in worker process
    import warnings
    import logging
    import json
    from pathlib import Path
    from ..utils.flattener_helper import FlattenerHelper
    
    warnings.filterwarnings("ignore", message=".*missing ScriptRunContext.*")
    logging.getLogger("streamlit.runtime.scriptrunner_utils.script_run_context").setLevel(logging.ERROR)
    
    chunk_results = []
    successful = 0
    failed = 0
    
    for file_path in file_paths:
        try:
            source_path = Path(file_path)
            target_path = Path(target_dir)
            
            # Create flattened filename
            flattened_filename = f"{source_path.stem}_flattened.json"
            output_path = target_path / flattened_filename
            
            # Load and normalize JSON data
            with open(source_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            assets = data if isinstance(data, list) else [data]
            
            # Flatten each asset using FlattenerHelper
            flattened_assets = []
            missing_attribution = 0
            missing_properties = 0
            missing_name = 0
            missing_parent_cloud = 0
            
            for asset in assets:
                if not isinstance(asset, dict):
                    continue
                
                # Check for missing attribution
                if 'assetAttributions' not in asset or not asset['assetAttributions']:
                    missing_attribution += 1
                
                # Check for missing properties
                if 'properties' not in asset or not asset['properties']:
                    missing_properties += 1
                
                # Check for missing name
                name_value = asset.get('name')
                if not name_value or str(name_value).strip() in ['', 'None', 'null']:
                    missing_name += 1
                
                # Check for missing parent cloud
                has_parent_cloud = False
                if 'assetAttributions' in asset and asset['assetAttributions']:
                    for attr in asset['assetAttributions']:
                        if attr and isinstance(attr, dict) and 'parentCloud' in attr and attr['parentCloud']:
                            has_parent_cloud = True
                            break
                
                if not has_parent_cloud:
                    missing_parent_cloud += 1
                
                # Flatten the asset using FlattenerHelper
                flattened_asset = FlattenerHelper.flatten_asset(asset)
                flattened_assets.append(flattened_asset)
            
            # Save flattened data
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(flattened_assets, f, indent=2, ensure_ascii=False)
            
            result = {
                'success': True,
                'file_path': str(source_path),
                'input': str(source_path),
                'output': str(output_path),
                'source_assets': len(assets),
                'normalised_assets': len(flattened_assets),
                'missing_attribution': missing_attribution,
                'missing_properties': missing_properties,
                'missing_name': missing_name,
                'missing_parent_cloud': missing_parent_cloud
            }
            
            chunk_results.append(result)
            successful += 1
            
        except Exception as e:
            result = {
                'success': False,
                'file_path': file_path,
                'error': str(e),
                'input': file_path,
                'output': None,
                'source_assets': 0,
                'normalised_assets': 0,
                'missing_attribution': 0,
                'missing_properties': 0,
                'missing_name': 0,
                'missing_parent_cloud': 0
            }
            
            chunk_results.append(result)
            failed += 1
    
    return {
        'results': chunk_results,
        'successful': successful,
        'failed': failed
    }
