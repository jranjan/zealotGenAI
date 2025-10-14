"""
SonicFlattener - Multithreaded data flattening operations using FlattenerHelper

This module provides a high-performance multithreaded flattener that leverages
FlattenerHelper for all flattening operations while using ThreadPoolExecutor
for parallel file processing.
"""

import os
from pathlib import Path
from typing import Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from .flattener import Flattener


class SonicFlattener(Flattener):
    """
    Multithreaded data flattener that uses FlattenerHelper for all operations.
    
    This class extends the base Flattener to provide high-performance parallel
    processing using ThreadPoolExecutor while maintaining full compatibility
    with FlattenerHelper patterns.
    """
    
    def __init__(self, max_workers: int = None):
        """
        Initialize the SonicFlattener.
        
        Args:
            max_workers: Maximum number of worker threads (default: min(32, cpu_count + 4))
        """
        super().__init__()
        self.max_workers = max_workers or min(32, (os.cpu_count() or 1) + 4)
    
    def transform_directory(self, source_folder: str, target_folder: str) -> Dict[str, Any]:
        """
        Transform all JSON files in a source directory using multithreading.
        
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
        
        print(f"🚀 SonicFlattener: Processing {total_files} files with {self.max_workers} threads")
        
        # Process files in parallel
        results = []
        successful = 0
        failed = 0
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all files for processing
            future_to_file = {
                executor.submit(self._process_single_file, str(file_path), str(target_path)): file_path 
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
                    error_result = self.create_file_result(
                        success=False,
                        file_path=str(file_path),
                        error=str(e),
                        input=str(file_path),
                        output=None,
                        source_assets=0,
                        normalised_assets=0,
                        missing_attribution=0,
                        missing_properties=0
                    )
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
    
    def get_performance_info(self) -> Dict[str, Any]:
        """
        Get performance information about the SonicFlattener.
        
        Returns:
            Dictionary containing performance information
        """
        return {
            'flattener_type': 'SonicFlattener (Multithreaded)',
            'uses_flattener_helper': True,
            'optimized': True,
            'max_workers': self.max_workers,
            'cpu_count': os.cpu_count()
        }
    
