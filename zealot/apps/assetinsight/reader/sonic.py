"""
SonicReader - High-performance multiprocessing source data reading operations
"""

import json
import os
import multiprocessing
from pathlib import Path
from typing import Dict, Any, List, Set
from concurrent.futures import ProcessPoolExecutor, as_completed
import warnings
import logging
from .base import Reader

# Suppress Streamlit warnings in multiprocessing workers
warnings.filterwarnings("ignore", message=".*missing ScriptRunContext.*")
logging.getLogger("streamlit.runtime.scriptrunner_utils.script_run_context").setLevel(logging.ERROR)


class SonicReader(Reader):
    """
    Supersonic-high-performance multiprocessing source data scanner.
    
    Uses multiprocessing to scan large directories with many JSON files
    in parallel, bypassing Python's GIL limitations.
    """
    
    def __init__(self, max_workers: int = None, chunk_size: int = 50):
        """
        Initialize SupersonicScanner.
        
        Args:
            max_workers: Maximum number of worker processes (default: CPU count)
            chunk_size: Number of files to process per chunk (default: 50)
        """
        super().__init__()
        self.max_workers = max_workers or min(multiprocessing.cpu_count(), 16)
        self.chunk_size = chunk_size
    
    def scan_directory(self, directory_path: str) -> Dict[str, Any]:
        """
        Scan a directory using multiprocessing and return inspection results.
        
        Args:
            directory_path: Path to the directory to scan
            
        Returns:
            Dictionary with scanning results
        """
        return self.scan_source_directory(directory_path)
    
    def get_directory_info(self, directory_path: str) -> Dict[str, Any]:
        """
        Get basic directory information without full scanning.
        
        Args:
            directory_path: Path to the directory
            
        Returns:
            Dictionary with basic directory information
        """
        validation = self.validate_directory(directory_path)
        if not validation['valid']:
            return validation
        
        file_count = self.count_files(directory_path, "*.json")
        json_files = list(Path(directory_path).glob("*.json"))
        return {
            'valid': True,
            'total_files': file_count,
            'json_files': [f.name for f in json_files[:5]]
        }
    
    def scan_source_directory(self, directory_path: str) -> Dict[str, Any]:
        """
        Scan source directory using multiprocessing for high performance.
        
        Args:
            directory_path: Path to source directory
            
        Returns:
            Dictionary with analysis results
        """
        validation = self.validate_directory(directory_path)
        if not validation['valid']:
            return {
                'success': False,
                'error': validation['error'],
                'total_files': 0,
                'estimated_assets': 0,
                'source_folder': directory_path,
                'file_details': [],
                'asset_classes': []
            }
        
        try:
            source_path = Path(directory_path)
            json_files = list(source_path.glob("*.json"))
            
            if not json_files:
                return {
                    'success': True,
                    'total_files': 0,
                    'estimated_assets': 0,
                    'source_folder': directory_path,
                    'file_details': [],
                    'asset_classes': []
                }
            
            print(f"🚀 SupersonicScanner: Processing {len(json_files)} files with {self.max_workers} workers")
            
            # Split files into chunks for processing
            file_chunks = [
                [str(f) for f in json_files[i:i + self.chunk_size]]
                for i in range(0, len(json_files), self.chunk_size)
            ]
            
            all_results = []
            total_assets = 0
            all_asset_classes = set()
            
            # Process chunks in parallel using ProcessPoolExecutor
            with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
                # Submit all chunks
                future_to_chunk = {
                    executor.submit(self._process_file_chunk, chunk): i 
                    for i, chunk in enumerate(file_chunks)
                }
                
                # Collect results as they complete
                for future in as_completed(future_to_chunk):
                    try:
                        chunk_result = future.result()
                        all_results.extend(chunk_result['results'])
                        total_assets += chunk_result['total_assets']
                        all_asset_classes.update(chunk_result['asset_classes'])
                        
                        chunk_idx = future_to_chunk[future]
                        print(f"✅ Completed chunk {chunk_idx + 1}/{len(file_chunks)}")
                        
                    except Exception as e:
                        print(f"❌ Error processing chunk: {e}")
                        continue
            
            # Convert results to file details format
            file_details = []
            for result in all_results:
                file_details.append({
                    'File': result['file'],
                    'Size (MB)': round(result['size_mb'], 2),
                    'Assets': result['assets'],
                    'Asset Classes': result['asset_classes_display']
                })
            
            print(f"🎯 SupersonicScanner: Processed {len(json_files)} files, {total_assets} assets")
            
            return {
                'success': True,
                'total_files': len(json_files),
                'estimated_assets': total_assets,
                'source_folder': directory_path,
                'file_details': file_details,
                'asset_classes': sorted(list(all_asset_classes))
            }
            
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'total_files': 0,
                'estimated_assets': 0,
                'source_folder': directory_path,
                'file_details': [],
                'asset_classes': []
            }
    
    def _process_single_file(self, file_path: str) -> Dict[str, Any]:
        """
        Process a single JSON file to extract asset information.
        This function runs in a separate process.
        
        Args:
            file_path: Path to the JSON file to process
            
        Returns:
            Dictionary with file processing results
        """
        # Suppress Streamlit warnings in worker process
        import warnings
        import logging
        warnings.filterwarnings("ignore", message=".*missing ScriptRunContext.*")
        logging.getLogger("streamlit.runtime.scriptrunner_utils.script_run_context").setLevel(logging.ERROR)
        
        try:
            file_path_obj = Path(file_path)
            file_size = file_path_obj.stat().st_size
            file_size_mb = file_size / (1024 * 1024)
            
            # Load and analyze JSON file
            with open(file_path_obj, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if isinstance(data, list):
                asset_count = len(data)
                # Extract asset classes from each asset
                file_asset_classes = set()
                for asset in data:
                    if isinstance(asset, dict) and 'assetClass' in asset:
                        asset_class = asset['assetClass']
                        if asset_class:
                            file_asset_classes.add(asset_class)
            else:
                asset_count = 1
                # Single asset object
                if isinstance(data, dict) and 'assetClass' in data:
                    asset_class = data['assetClass']
                    if asset_class:
                        file_asset_classes = {asset_class}
                    else:
                        file_asset_classes = set()
                else:
                    file_asset_classes = set()
            
            # Format asset classes for display
            asset_classes_display = ', '.join(sorted(file_asset_classes)) if file_asset_classes else 'Unknown'
            
            return {
                'success': True,
                'file': file_path_obj.name,
                'size_mb': file_size_mb,
                'assets': asset_count,
                'asset_classes': list(file_asset_classes),
                'asset_classes_display': asset_classes_display
            }
            
        except Exception as e:
            return {
                'success': False,
                'file': Path(file_path).name,
                'size_mb': 0,
                'assets': 0,
                'asset_classes': [],
                'asset_classes_display': 'Error',
                'error': str(e)
            }
    
    def _process_file_chunk(self, file_paths: List[str]) -> Dict[str, Any]:
        """
        Process a chunk of files in a single process.
        
        Args:
            file_paths: List of file paths to process
            
        Returns:
            Dictionary with chunk processing results
        """
        # Suppress Streamlit warnings in worker process
        import warnings
        import logging
        warnings.filterwarnings("ignore", message=".*missing ScriptRunContext.*")
        logging.getLogger("streamlit.runtime.scriptrunner_utils.script_run_context").setLevel(logging.ERROR)
        
        chunk_results = []
        total_assets = 0
        all_asset_classes = set()
        
        for file_path in file_paths:
            result = self._process_single_file(file_path)
            chunk_results.append(result)
            
            if result['success']:
                total_assets += result['assets']
                all_asset_classes.update(result['asset_classes'])
        
        return {
            'results': chunk_results,
            'total_assets': total_assets,
            'asset_classes': list(all_asset_classes)
        }
    
    def get_performance_info(self) -> Dict[str, Any]:
        """
        Get performance information about the scanner.
        
        Returns:
            Dictionary with performance metrics
        """
        return {
            'max_workers': self.max_workers,
            'chunk_size': self.chunk_size,
            'cpu_count': multiprocessing.cpu_count(),
            'scanner_type': 'SupersonicScanner (Multiprocessing)'
        }
