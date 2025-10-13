"""
SupersonicFlattener - High-performance multiprocessing data flattening operations
Uses multiprocessing to bypass Python's GIL for maximum performance with large file counts
"""

import json
import os
from pathlib import Path
from typing import Dict, List, Any, Optional
from multiprocessing import Pool, cpu_count
import time
import warnings
import logging
from .flattener import Flattener
from .utils.flattener_helper import FlattenerHelper

# Suppress Streamlit warnings in multiprocessing workers
warnings.filterwarnings("ignore", message=".*missing ScriptRunContext.*")
logging.getLogger("streamlit.runtime.scriptrunner_utils.script_run_context").setLevel(logging.ERROR)


class SupersonicFlattener(Flattener):
    """Supersonic-high-performance multiprocessing data flattening operations"""
    
    def __init__(self, max_workers: int = None, chunk_size: int = 100):
        super().__init__()
        self.max_workers = max_workers or min(cpu_count(), 16)  # Limit to prevent memory issues
        self.chunk_size = chunk_size  # Process files in chunks to manage memory
        self._temp_dir = None
    
    def transform_directory(self, source_dir: str, target_dir: str) -> Dict[str, Any]:
        """
        Transform all JSON files in source directory using multiprocessing.
        
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
        
        print(f"🚀 SupersonicFlattener: Processing {len(json_files)} files with {self.max_workers} processes")
        
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
            chunk_args = [(chunk, target_dir) for chunk in file_chunks]
            chunk_results = pool.starmap(_process_file_chunk, chunk_args)
            
            # Collect results
            for chunk_result in chunk_results:
                all_results.extend(chunk_result['results'])
                successful += chunk_result['successful']
                failed += chunk_result['failed']
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        print(f"🎯 SupersonicFlattener: Completed {total_files} files in {processing_time:.2f}s ({total_files / processing_time:.1f} files/sec)")
        
        return self.create_result_dict(
            success=True,
            total_files=total_files,
            successful=successful,
            failed=failed,
            files=all_results,
            processing_time=processing_time
        )
    
    
    def transform_file(self, source_file: str, target_file: str) -> bool:
        """
        Transform a single JSON file.
        
        Args:
            source_file: Path to source JSON file
            target_file: Path to target JSON file
            
        Returns:
            True if successful, False otherwise
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
            'chunk_size': self.chunk_size,
            'cpu_count': cpu_count(),
            'flattener_type': 'SupersonicFlattener (Multiprocessing)'
        }
    


def _create_file_result(success: bool, file_path: str, error: Optional[str] = None, **kwargs) -> Dict[str, Any]:
    """Helper function to create file result (for multiprocessing compatibility)."""
    result = {
        'success': success,
        'file': file_path,
        'error': error
    }
    result.update(kwargs)
    return result

def _process_single_file(file_path: str, target_dir: str) -> Dict[str, Any]:
    """
    Process a single JSON file and create flattened version.
    This function runs in a separate process.
    
    Args:
        file_path: Path to source JSON file
        target_dir: Target directory for output
        
    Returns:
        Dictionary with processing results
    """
    # Suppress Streamlit warnings in worker process
    import warnings
    import logging
    warnings.filterwarnings("ignore", message=".*missing ScriptRunContext.*")
    logging.getLogger("streamlit.runtime.scriptrunner_utils.script_run_context").setLevel(logging.ERROR)
    
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
            
            # Flatten the asset using FlattenerHelper
            flattened_asset = FlattenerHelper.flatten_asset(asset)
            flattened_assets.append(flattened_asset)
        
        # Save flattened data
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(flattened_assets, f, indent=2, ensure_ascii=False)
        
        return _create_file_result(
            success=True,
            file_path=str(source_path),
            input=str(source_path),
            output=str(output_path),
            source_assets=len(assets),
            normalised_assets=len(flattened_assets),
            missing_attribution=missing_attribution,
            missing_properties=missing_properties,
            missing_name=missing_name
        )
        
    except Exception as e:
        return _create_file_result(
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




def _process_file_chunk(file_paths: List[str], target_dir: str) -> Dict[str, Any]:
    """
    Process a chunk of files in a single process.
    
    Args:
        file_paths: List of file paths to process
        target_dir: Target directory for output files
        
    Returns:
        Dictionary with chunk processing results
    """
    # Suppress Streamlit warnings in worker process
    import warnings
    import logging
    warnings.filterwarnings("ignore", message=".*missing ScriptRunContext.*")
    logging.getLogger("streamlit.runtime.scriptrunner_utils.script_run_context").setLevel(logging.ERROR)
    
    chunk_results = []
    successful = 0
    failed = 0
    
    for file_path in file_paths:
        result = _process_single_file(file_path, target_dir)
        chunk_results.append(result)
        
        if result['success']:
            successful += 1
        else:
            failed += 1
    
    return {
        'results': chunk_results,
        'successful': successful,
        'failed': failed
    }
