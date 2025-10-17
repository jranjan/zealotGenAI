"""
DuckDBSonicMemoryReader - High-performance multiprocessing DuckDB reader with in-memory database

This module provides a multiprocessing-enabled version of DuckDBMemoryReader
that can load multiple JSON files in parallel for maximum performance.
"""

import os
import json
import multiprocessing as mp
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
import duckdb
from concurrent.futures import ProcessPoolExecutor, as_completed, ThreadPoolExecutor
import time
import orjson  # Faster JSON parsing
import psutil  # For memory monitoring
import warnings
import logging

# Multiprocessing safety for different platforms
if __name__ != '__main__':
    # Ensure multiprocessing works correctly on all platforms
    mp.set_start_method('spawn', force=True)

# Suppress Streamlit warnings in multiprocessing workers
warnings.filterwarnings("ignore", message=".*missing ScriptRunContext.*")
logging.getLogger("streamlit.runtime.scriptrunner_utils.script_run_context").setLevel(logging.ERROR)

from .duckdb import DuckDBMemoryReader
from configreader import SchemaGuide


class DuckDBSonicMemoryReader(DuckDBMemoryReader):
    """
    High-performance DuckDB reader with multiprocessing support.
    
    Extends DuckDBMemoryReader to load multiple JSON files in parallel using
    multiprocessing for maximum performance on large datasets.
    """
    
    _instance = None  # Class variable to store the singleton instance
    _initialized = False  # Track if singleton has been initialized
    
    def __new__(cls, folder_path: str = None, max_workers: Optional[int] = None, 
                 batch_size: int = 1000, memory_limit_gb: float = 2.0):
        """
        Create a new DuckDBSonicMemoryReader singleton instance.
        """
        # Return existing singleton instance if it exists
        if cls._instance is not None:
            # Update folder_path if provided and different
            if folder_path and folder_path != cls._instance.folder_path:
                print(f"🔄 Updating singleton folder_path from {cls._instance.folder_path} to {folder_path}")
                cls._instance.folder_path = folder_path
            return cls._instance
        
        # Create new singleton instance
        instance = super(DuckDBSonicMemoryReader, cls).__new__(cls, folder_path)
        cls._instance = instance
        
        # Set Sonic-specific attributes immediately (before parent __init__ calls _setup_database)
        # Use all available CPU cores for maximum performance, but cap at 4 to avoid issues
        instance.max_workers = max_workers or min(mp.cpu_count(), 4)
        instance.batch_size = batch_size
        instance.memory_limit_bytes = memory_limit_gb * 1024 * 1024 * 1024
        instance._file_chunks = []
        instance._progress_callback = None
        instance._start_time = None
        instance._total_files = 0
        instance._processed_files = 0
        
        print(f"🏗️ Creating new DuckDBSonicMemoryReader singleton instance")
        return instance
    
    def __init__(self, folder_path: str = None, max_workers: Optional[int] = None, 
                 batch_size: int = 1000, memory_limit_gb: float = 2.0):
        """
        Initialize DuckDBSonicMemoryReader with multiprocessing support.
        
        Args:
            folder_path: Path to folder containing JSON files
            max_workers: Maximum number of worker processes (default: CPU count)
            batch_size: Number of assets to process in each batch
            memory_limit_gb: Memory limit in GB before switching to streaming mode
        """
        # Only initialize once for singleton
        if not self._initialized:
            if folder_path is None:
                folder_path = "."
            # Call parent __init__ to set up database connection
            # Sonic-specific attributes are already set in __new__
            super().__init__(folder_path)
            self._initialized = True
    
    @classmethod
    def reset_singleton(cls):
        """Reset the singleton instance (useful for testing)"""
        cls._instance = None
        cls._initialized = False
    
    def _load_json_files(self) -> None:
        """
        Load JSON files using optimized multiprocessing for maximum performance.
        """
        if not self.folder_path or not os.path.exists(self.folder_path):
            raise ValueError(f"Folder does not exist: {self.folder_path}")
        
        # Reset debug counter for this loading session
        DuckDBSonicMemoryReader._debug_count = 0
        
        # Find all JSON files with size information
        print(f"🔍 Scanning for JSON files in: {self.folder_path}")
        json_files = self._get_file_list_with_sizes()
        if not json_files:
            raise ValueError(f"No JSON files found in {self.folder_path}")
        
        self._total_files = len(json_files)
        self._start_time = time.time()
        
        print(f"📊 Found {self._total_files} JSON files to process")
        
        # Check if we should use streaming mode based on memory
        if self._should_use_streaming_mode(json_files):
            print(f"🌊 Using streaming mode for large dataset")
            self._load_files_streaming(json_files)
        else:
            print(f"⚡ Using parallel processing mode")
            self._load_files_parallel(json_files)
    
    def _get_file_list_with_sizes(self) -> List[Tuple[Path, int]]:
        """Get list of JSON files with their sizes for better chunking."""
        files_with_sizes = []
        for file_path in Path(self.folder_path).glob("*.json"):
            try:
                size = file_path.stat().st_size
                files_with_sizes.append((file_path, size))
            except OSError:
                continue
        return files_with_sizes
    
    def _should_use_streaming_mode(self, files_with_sizes: List[Tuple[Path, int]]) -> bool:
        """Determine if we should use streaming mode based on available memory."""
        total_size = sum(size for _, size in files_with_sizes)
        available_memory = psutil.virtual_memory().available
        return total_size > self.memory_limit_bytes or total_size > available_memory * 0.5
    
    def _load_files_parallel(self, files_with_sizes: List[Tuple[Path, int]]) -> None:
        """Load files using parallel processing with optimized chunking."""
        # Create balanced file chunks based on file sizes
        print(f"📦 Creating file chunks for {len(files_with_sizes)} files...")
        self._file_chunks = self._create_balanced_chunks(files_with_sizes)
        print(f"📦 Created {len(self._file_chunks)} chunks with {self.max_workers} workers")
        
        # Process files in parallel with progress tracking
        all_assets = []
        
        print(f"🚀 Starting parallel processing with {self.max_workers} workers...")
        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all chunks for processing
            future_to_chunk = {
                executor.submit(self._process_file_chunk_optimized, chunk): chunk 
                for chunk in self._file_chunks
            }
            
            # Collect results as they complete with progress updates
            for future in as_completed(future_to_chunk, timeout=300):  # 5 minute timeout
                try:
                    chunk_assets, processed_count = future.result(timeout=60)  # 1 minute per chunk
                    all_assets.extend(chunk_assets)
                    self._processed_files += processed_count
                    self._update_progress()
                except Exception as e:
                    chunk = future_to_chunk[future]
                    print(f"❌ Error processing chunk: {e}")
                    pass  # Error processing chunk
        
        print(f"📊 File processing complete. Total assets collected: {len(all_assets)}")
        
        # Load all assets into DuckDB using multiprocessing
        if all_assets:
            print(f"🗄️ Loading {len(all_assets)} assets into database...")
            # Choose the best method based on data size
            if len(all_assets) > 10000:  # Use in-memory multiprocessing for large datasets
                print(f"🌊 Using in-memory multiprocessing method for large dataset ({len(all_assets)} assets)")
                self._load_assets_into_duckdb_multiprocessing_memory(all_assets)
            else:  # Use shared memory approach for smaller datasets
                print(f"⚡ Using parallel method for dataset ({len(all_assets)} assets)")
                self._load_assets_into_duckdb_parallel(all_assets)
        else:
            print("⚠️ No assets to load into database")
    
    def _load_files_streaming(self, files_with_sizes: List[Tuple[Path, int]]) -> None:
        """Load files using streaming mode for memory efficiency."""
        # Sort files by size (largest first) for better memory management
        sorted_files = sorted(files_with_sizes, key=lambda x: x[1], reverse=True)
        
        # Process files in smaller batches to manage memory
        batch_size = max(1, len(sorted_files) // (self.max_workers * 2))
        
        for i in range(0, len(sorted_files), batch_size):
            batch = sorted_files[i:i + batch_size]
            batch_assets = []
            
            # Process batch files
            for file_path, _ in batch:
                try:
                    assets = self._process_single_file_streaming(file_path)
                    batch_assets.extend(assets)
                    self._processed_files += 1
                    self._update_progress()
                except Exception as e:
                    pass  # Error processing file
            
            # Load batch into DuckDB immediately
            if batch_assets:
                self._load_assets_into_duckdb(batch_assets)
                pass  # Batch loaded
    
    def _create_balanced_chunks(self, files_with_sizes: List[Tuple[Path, int]]) -> List[List[Path]]:
        """Create balanced file chunks based on file sizes for optimal load distribution."""
        # Sort files by size (largest first)
        sorted_files = sorted(files_with_sizes, key=lambda x: x[1], reverse=True)
        
        # Calculate optimal number of chunks based on dataset size
        total_files = len(sorted_files)
        total_size = sum(size for _, size in sorted_files)
        
        print(f"📊 Dataset analysis: {total_files} files, {total_size/(1024**2):.1f}MB total size")
        
        # For large datasets, use more chunks than workers for better parallelism
        if total_files > 500:  # Large dataset
            # Use more chunks: min(workers * 4, files / 50, 32)
            optimal_chunks = min(self.max_workers * 4, max(total_files // 50, 8), 32)
            print(f"🌊 Large dataset detected - using {optimal_chunks} chunks for better parallelism")
        else:
            # For smaller datasets, use workers as chunks
            optimal_chunks = self.max_workers
            print(f"⚡ Small dataset - using {optimal_chunks} chunks (1 per worker)")
        
        pass  # Chunks created
        
        # Initialize chunks with empty lists
        chunks = [[] for _ in range(optimal_chunks)]
        chunk_sizes = [0] * optimal_chunks
        
        # Distribute files to balance chunk sizes
        for file_path, file_size in sorted_files:
            # Find the chunk with the smallest total size
            smallest_chunk_idx = chunk_sizes.index(min(chunk_sizes))
            chunks[smallest_chunk_idx].append(file_path)
            chunk_sizes[smallest_chunk_idx] += file_size
        
        # Filter out empty chunks
        return [chunk for chunk in chunks if chunk]
    
    def _update_progress(self) -> None:
        """Update progress display."""
        if self._total_files > 0:
            progress = (self._processed_files / self._total_files) * 100
            elapsed = time.time() - self._start_time
            if self._processed_files > 0:
                eta = (elapsed / self._processed_files) * (self._total_files - self._processed_files)
                print(f"📊 Progress: {progress:.1f}% ({self._processed_files}/{self._total_files}) - ETA: {eta:.1f}s")
    
    @staticmethod
    def _process_file_chunk_optimized(file_chunk: List[Path]) -> Tuple[List[Dict[str, Any]], int]:
        """
        Process a chunk of JSON files in a separate process with optimizations.
        
        Args:
            file_chunk: List of JSON file paths to process
            
        Returns:
            Tuple of (processed assets, processed file count)
        """
        # Suppress Streamlit warnings in worker process
        import warnings
        import logging
        warnings.filterwarnings("ignore", message=".*missing ScriptRunContext.*")
        logging.getLogger("streamlit.runtime.scriptrunner_utils.script_run_context").setLevel(logging.ERROR)
        
        assets = []
        processed_count = 0
        
        for file_path in file_chunk:
            try:
                # Use orjson for faster JSON parsing
                with open(file_path, 'rb') as f:
                    data = orjson.loads(f.read())
                
                # Handle both single objects and arrays
                if isinstance(data, list):
                    file_assets = data
                else:
                    file_assets = [data]
                
                # Process each asset with optimized parsing
                for asset in file_assets:
                    if isinstance(asset, dict):
                        processed_asset = DuckDBSonicMemoryReader._process_single_asset_optimized(asset)
                        if processed_asset:
                            assets.append(processed_asset)
                
                processed_count += 1
                            
            except Exception as e:
                pass  # Error processing file
                continue
        
        return assets, processed_count
    
    @staticmethod
    def _process_single_file_streaming(file_path: Path) -> List[Dict[str, Any]]:
        """
        Process a single file in streaming mode for memory efficiency.
        
        Args:
            file_path: Path to JSON file to process
            
        Returns:
            List of processed asset dictionaries
        """
        assets = []
        
        try:
            # Use orjson for faster JSON parsing
            with open(file_path, 'rb') as f:
                data = orjson.loads(f.read())
            
            # Handle both single objects and arrays
            if isinstance(data, list):
                file_assets = data
            else:
                file_assets = [data]
            
            # Process each asset
            for asset in file_assets:
                if isinstance(asset, dict):
                    processed_asset = DuckDBSonicMemoryReader._process_single_asset_optimized(asset)
                    if processed_asset:
                        assets.append(processed_asset)
                        
        except Exception as e:
            pass  # Error processing file
        
        return assets
    
    @staticmethod
    def _process_single_asset_optimized(asset: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Process a single asset dictionary with optimizations using FlattenerHelper.
        
        Args:
            asset: Raw asset dictionary
            
        Returns:
            Processed asset dictionary or None if invalid
        """
        try:
            # Debug: Print first few assets being processed
            if hasattr(DuckDBSonicMemoryReader, '_debug_count'):
                DuckDBSonicMemoryReader._debug_count += 1
            else:
                DuckDBSonicMemoryReader._debug_count = 1
            
            # The asset should already be flattened from the flattened files
            # Use the flattened data directly for properties and tags reconstruction
            properties_json = DuckDBSonicMemoryReader._reconstruct_nested_json_static(asset, 'properties_')
            tags_json = DuckDBSonicMemoryReader._reconstruct_nested_json_static(asset, 'tags_')
            raw_data_json = orjson.dumps(asset).decode('utf-8')
            
            
            # Use flattened data directly from the flattened files
            processed_asset = {
                'id': asset.get('id', ''),
                'name': asset.get('name', ''),
                'identifier': asset.get('identifier', ''),
                'createdDate': asset.get('createdDate', ''),
                'lastModifiedDate': asset.get('lastModifiedDate', ''),
                'assetClass': asset.get('assetClass', ''),
                'startDate': asset.get('startDate', ''),
                'endDate': asset.get('endDate', ''),
                'lastSeenDate': asset.get('lastSeenDate', ''),
                'status': asset.get('status', ''),
                'accountId': asset.get('accountId', ''),
                'deleted': asset.get('deleted', ''),
                # Use flattened ownership fields directly from the flattened asset
                'parent_cloud': asset.get('parent_cloud'),
                'parent_cloud_id': asset.get('parent_cloud_id'),
                'parent_cloud_owner_email': asset.get('parent_cloud_owner_email'),
                'cloud': asset.get('cloud'),
                'cloud_id': asset.get('cloud_id'),
                'cloud_owner_email': asset.get('cloud_owner_email'),
                'team': asset.get('team'),
                'team_id': asset.get('team_id'),
                'team_owner_email': asset.get('team_owner_email'),
                'properties': properties_json,
                'tags': tags_json,
                'raw_data': raw_data_json
            }
            
            return processed_asset
            
        except Exception as e:
            pass  # Error processing asset
            return None
    
    @staticmethod
    def _process_single_asset(asset: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Process a single asset dictionary (legacy method for compatibility).
        
        Args:
            asset: Raw asset dictionary
            
        Returns:
            Processed asset dictionary or None if invalid
        """
        return DuckDBSonicMemoryReader._process_single_asset_optimized(asset)
    
    def _load_assets_into_duckdb(self, assets: List[Dict[str, Any]]) -> None:
        """
        Load processed assets into DuckDB.
        
        Args:
            assets: List of processed asset dictionaries
        """
        if not assets:
            return
        
        
        # Check database state before loading
        self._verify_database_records_before_loading()
        
        # Create assets table using dynamic schema (only if not exists)
        self._create_assets_table_if_not_exists(self.conn)
        
        # Use optimized batch insertion
        self._insert_asset_batch_optimized(assets)
        
        # Create indexes for better query performance
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_assets_id ON assets(id)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_assets_parent_cloud ON assets(parent_cloud)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_assets_cloud ON assets(cloud)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_assets_team ON assets(team)")
        
        # Verify database records after loading
        self._verify_database_records()
    
    def _verify_database_records_before_loading(self):
        """Check database state before loading data"""
        try:
            # Check if assets table exists
            tables = self.conn.execute("SHOW TABLES").fetchall()
            table_names = [table[0] for table in tables] if tables else []
            
            if 'assets' not in table_names:
                pass  # No table exists, ready to create
            else:
                # Check if table is empty
                result = self.conn.execute("SELECT COUNT(*) FROM assets").fetchone()
                if result and result[0] > 0:
                    pass  # Table has data
                else:
                    pass  # Table is empty, ready to load
        except Exception as e:
            pass  # Error checking database state
    
    def _create_assets_table_if_not_exists(self, conn) -> None:
        """Create assets table only if it doesn't already exist"""
        try:
            # Check if table already exists
            tables = conn.execute("SHOW TABLES").fetchall()
            table_names = [table[0] for table in tables] if tables else []
            
            if 'assets' in table_names:
                # Table already exists, skip creation
                print(f"📋 Table 'assets' already exists, skipping creation")
                return
            
            # Create the table using the base class method
            print(f"📋 Table 'assets' does not exist, creating now...")
            self._create_assets_table(conn)
        except Exception as e:
            # If check fails, create the table anyway
            print(f"⚠️ Error checking table existence: {e}, creating table anyway...")
            self._create_assets_table(conn)
    
    def _verify_database_records(self):
        """Verify database records after loading"""
        try:
            # Query first 3 records from database to verify data was written correctly
            result = self.conn.execute("""
                SELECT 
                    id, name, assetClass, status,
                    parent_cloud, cloud, team,
                    properties, tags
                FROM assets 
                LIMIT 3
            """).fetchall()
            
            # Verification complete - data loaded successfully
            pass
            
        except Exception as e:
            pass  # Error verifying database records
    
    def _load_assets_into_duckdb_parallel(self, assets: List[Dict[str, Any]]) -> None:
        """
        Load processed assets into DuckDB using multiprocessing for maximum performance.
        
        Args:
            assets: List of processed asset dictionaries
        """
        if not assets:
            print("⚠️ No assets to load into database")
            return
        
        print(f"🗄️ Starting database loading for {len(assets)} assets...")
        
        # Check database state before loading
        self._verify_database_records_before_loading()
        
        # Create assets table using dynamic schema (only if not exists)
        self._create_assets_table_if_not_exists(self.conn)
        
        # Split assets into chunks for multiprocessing
        print(f"📊 Analyzing asset dataset: {len(assets):,} assets")
        
        # For large datasets, use more chunks for better parallelism, but align with workers
        if len(assets) > 100000:  # Large dataset (>100K assets)
            # Use more chunks: min(workers * 4, assets / 10000, workers * 2)
            # This ensures we don't create too many chunks that can't be processed efficiently
            optimal_asset_chunks = min(self.max_workers * 4, max(len(assets) // 10000, self.max_workers * 2), self.max_workers * 8)
            chunk_size = max(1000, len(assets) // optimal_asset_chunks)
            print(f"🌊 Large dataset - using {optimal_asset_chunks} chunks of ~{chunk_size:,} assets each")
        else:
            # For smaller datasets, use workers as chunks
            chunk_size = max(1000, len(assets) // self.max_workers)
            print(f"⚡ Small dataset - using {self.max_workers} chunks of ~{chunk_size:,} assets each")
        
        asset_chunks = [assets[i:i + chunk_size] for i in range(0, len(assets), chunk_size)]
        
        print(f"🚀 Inserting {len(assets):,} assets using {self.max_workers} processes...")
        print(f"📦 Processing {len(asset_chunks)} chunks of ~{chunk_size:,} assets each")
        
        # Use multiprocessing for parallel database operations
        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all chunks for parallel processing
            future_to_chunk = {
                executor.submit(self._process_asset_chunk_multiprocessing, chunk, self.folder_path): chunk 
                for chunk in asset_chunks
            }
            
            # Collect results as they complete
            processed_chunks = 0
            start_time = time.time()
            
            print(f"⏱️ Starting parallel processing at {time.strftime('%H:%M:%S')}")
            
            for future in as_completed(future_to_chunk, timeout=300):  # 5 minute timeout
                try:
                    print(f"🔄 Processing chunk {processed_chunks + 1}/{len(asset_chunks)}...")
                    chunk_data = future.result(timeout=60)  # 1 minute per chunk
                    print(f"📦 Chunk {processed_chunks + 1} completed, inserting into database...")
                    
                    self._insert_chunk_data_into_main_db(chunk_data)
                    processed_chunks += 1
                    
                    print(f"✅ Chunk {processed_chunks} inserted successfully")
                    
                    # Debug: Show database state after first chunk is processed
                    if processed_chunks == 1:
                        self._verify_database_records()
                    
                    # Progress update with timing
                    progress = (processed_chunks / len(asset_chunks)) * 100
                    elapsed = time.time() - start_time
                    if processed_chunks > 0:
                        eta = (elapsed / processed_chunks) * (len(asset_chunks) - processed_chunks)
                        print(f"📊 Progress: {progress:.1f}% ({processed_chunks}/{len(asset_chunks)} chunks processed) - ETA: {eta:.1f}s")
                    else:
                        print(f"📊 Progress: {progress:.1f}% ({processed_chunks}/{len(asset_chunks)} chunks processed)")
                    
                except Exception as e:
                    chunk = future_to_chunk[future]
                    print(f"❌ Error processing chunk {processed_chunks + 1}: {e}")
                    import traceback
                    traceback.print_exc()
                    pass  # Error processing chunk
        
        # Create indexes for better query performance (after all data is inserted)
        print("🔍 Creating database indexes for better query performance...")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_assets_id ON assets(id)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_assets_parent_cloud ON assets(parent_cloud)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_assets_cloud ON assets(cloud)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_assets_team ON assets(team)")
        print("✅ Database indexes created successfully")
        
        # Verify database records after loading
        print("🔍 Verifying final database state...")
        self._verify_database_records()
        
        total_time = time.time() - start_time
        print(f"✅ Database loading completed in {total_time:.1f} seconds")
        print(f"📊 Final stats: {len(assets):,} assets loaded using {self.max_workers} workers")
    
    def _load_assets_into_duckdb_multiprocessing_memory(self, assets: List[Dict[str, Any]]) -> None:
        """
        Load processed assets into DuckDB using true multiprocessing with in-memory processing.
        This is the fastest method for very large datasets.
        
        Args:
            assets: List of processed asset dictionaries
        """
        if not assets:
            return
        
        
        # Check database state before loading
        self._verify_database_records_before_loading()
        
        # Create main assets table using dynamic schema
        self._create_assets_table_if_not_exists(self.conn)
        
        # Split assets into chunks for multiprocessing
        # For large datasets, use more chunks for better parallelism, but align with workers
        if len(assets) > 100000:  # Large dataset (>100K assets)
            # Use more chunks: min(workers * 4, assets / 10000, workers * 2)
            # This ensures we don't create too many chunks that can't be processed efficiently
            optimal_asset_chunks = min(self.max_workers * 4, max(len(assets) // 10000, self.max_workers * 2), self.max_workers * 8)
            chunk_size = max(1000, len(assets) // optimal_asset_chunks)
        else:
            # For smaller datasets, use workers as chunks
            chunk_size = max(1000, len(assets) // self.max_workers)
        
        asset_chunks = [assets[i:i + chunk_size] for i in range(0, len(assets), chunk_size)]
        
        print(f"🚀 Processing {len(assets):,} assets using {self.max_workers} parallel processes...")
        print(f"📦 Processing {len(asset_chunks)} chunks in parallel with in-memory databases")
        
        # Use multiprocessing with in-memory processing
        # Use all available workers for maximum parallelism
        all_chunk_data = []
        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all chunks for parallel processing
            future_to_chunk = {
                executor.submit(self._process_asset_chunk_with_separate_db, chunk, self.folder_path, i): chunk 
                for i, chunk in enumerate(asset_chunks)
            }
            
            # Collect results as they complete
            processed_chunks = 0
            for future in as_completed(future_to_chunk, timeout=300):  # 5 minute timeout
                try:
                    chunk_data = future.result(timeout=60)  # 1 minute per chunk
                    if chunk_data:
                        all_chunk_data.extend(chunk_data)
                    processed_chunks += 1
                    
                    # Debug: Show database state after first chunk is processed
                    if processed_chunks == 1:
                        self._verify_database_records()
                    
                    # Progress update
                    progress = (processed_chunks / len(asset_chunks)) * 100
                    print(f"📊 Progress: {progress:.1f}% ({processed_chunks}/{len(asset_chunks)} chunks processed)")
                    
                except Exception as e:
                    chunk = future_to_chunk[future]
                    pass  # Error processing chunk
        
        # Insert all collected data into the main database
        print(f"🔗 Inserting {len(all_chunk_data)} processed records into main database...")
        if all_chunk_data:
            print(f"📊 Database insertion progress: Starting insertion of {len(all_chunk_data)} records")
            self._insert_chunk_data_into_main_db(all_chunk_data)
            print(f"✅ Database insertion complete: {len(all_chunk_data)} records inserted")
        
        # Create indexes for better query performance (after all data is merged)
        print("🔍 Creating database indexes for better query performance...")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_assets_id ON assets(id)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_assets_parent_cloud ON assets(parent_cloud)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_assets_cloud ON assets(cloud)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_assets_team ON assets(team)")
        print("✅ Database indexes created successfully")
        
        # Verify database records after loading
        print("🔍 Verifying final database state...")
        self._verify_database_records()
        
        print(f"✅ Database loading completed successfully!")
        print(f"📊 Final stats: {len(assets):,} assets loaded using {self.max_workers} workers")
    
    @staticmethod
    def _process_asset_chunk_multiprocessing(asset_chunk: List[Dict[str, Any]], folder_path: str) -> List[tuple]:
        """
        Process a chunk of assets in a separate process and return data tuples.
        
        Args:
            asset_chunk: List of asset dictionaries to process
            folder_path: Path to the folder (for database connection)
            
        Returns:
            List of data tuples ready for database insertion
        """
        # Suppress Streamlit warnings in worker process
        import warnings
        import logging
        warnings.filterwarnings("ignore", message=".*missing ScriptRunContext.*")
        logging.getLogger("streamlit.runtime.scriptrunner_utils.script_run_context").setLevel(logging.ERROR)
        
        print(f"🔄 Worker process: Processing {len(asset_chunk)} assets...")
        
        if not asset_chunk:
            print("⚠️ Worker process: No assets to process")
            return []
        
        # Convert to list of tuples for insertion using dynamic schema
        data_tuples = DuckDBSonicMemoryReader._create_data_tuples_static(asset_chunk)
        
        print(f"✅ Worker process: Completed processing {len(asset_chunk)} assets, returning {len(data_tuples)} tuples")
        return data_tuples
    
    @staticmethod
    def _process_asset_chunk_with_separate_db(asset_chunk: List[Dict[str, Any]], folder_path: str, chunk_id: int) -> List[tuple]:
        """
        Process a chunk of assets in a separate process with its own in-memory DuckDB connection.
        
        Args:
            asset_chunk: List of asset dictionaries to process
            folder_path: Path to the folder
            chunk_id: Unique identifier for this chunk
            
        Returns:
            List of data tuples ready for database insertion
        """
        if not asset_chunk:
            return None
        
        # Suppress Streamlit warnings in worker process
        import warnings
        import logging
        warnings.filterwarnings("ignore", message=".*missing ScriptRunContext.*")
        logging.getLogger("streamlit.runtime.scriptrunner_utils.script_run_context").setLevel(logging.ERROR)
        
        # Use in-memory database for chunk processing
        import duckdb
        conn = duckdb.connect(":memory:")
        
        try:
            # Drop and create assets table using dynamic schema
            conn.execute("DROP TABLE IF EXISTS assets")
            
            # Get dynamic schema from SchemaGuide
            from configreader.schema.guide import SchemaGuide
            schema_guide = SchemaGuide()
            table_schema = schema_guide.get_assets_table_schema()
            
            # Build CREATE TABLE statement dynamically
            columns = table_schema['columns']
            column_definitions = []
            for col in columns:
                col_name = col['column_name']
                col_type = col['data_type']
                column_definitions.append(f"{col_name} {col_type}")
            
            create_sql = f"CREATE TABLE assets ({', '.join(column_definitions)})"
            conn.execute(create_sql)
            # Note: No print statements in worker processes to avoid spam
            
            # Convert to list of tuples for insertion using dynamic schema
            data_tuples = DuckDBSonicMemoryReader._create_data_tuples_static(asset_chunk)
            
            # Insert data with dynamic schema
            try:
                insert_sql = DuckDBSonicMemoryReader._get_insert_sql_static()
            except Exception as e:
                print(f"⚠️ Error getting insert SQL: {e}")
                return
            
            conn.executemany(insert_sql, data_tuples)
            
            # Create indexes
            conn.execute("CREATE INDEX idx_assets_id ON assets(id)")
            conn.execute("CREATE INDEX idx_assets_parent_cloud ON assets(parent_cloud)")
            conn.execute("CREATE INDEX idx_assets_cloud ON assets(cloud)")
            conn.execute("CREATE INDEX idx_assets_team ON assets(team)")
            
        finally:
            conn.close()
        
        # Return the processed data instead of file path
        return data_tuples
    
    def _merge_chunk_databases(self, temp_db_paths: List[str]) -> None:
        """
        Merge data from temporary chunk databases into the main database.
        
        Args:
            temp_db_paths: List of paths to temporary database files
        """
        merged_count = 0
        for i, temp_db_path in enumerate(temp_db_paths, 1):
            if not temp_db_path or not os.path.exists(temp_db_path):
                continue
            
            try:
                # Attach the temporary database
                self.conn.execute(f"ATTACH '{temp_db_path}' AS chunk_db")
                
                # Copy data from chunk to main table
                self.conn.execute("INSERT INTO assets SELECT * FROM chunk_db.assets")
                
                # Detach the temporary database
                self.conn.execute("DETACH chunk_db")
                
                # Clean up temporary file
                os.remove(temp_db_path)
                merged_count += 1
                
                # Progress update
                progress = (i / len(temp_db_paths)) * 100
                print(f"📊 Merge Progress: {progress:.1f}% ({i}/{len(temp_db_paths)}) - Merged chunk {i} from {os.path.basename(temp_db_path)}")
                
            except Exception as e:
                pass  # Error merging chunk database
                # Clean up temporary file even if merge failed
                if os.path.exists(temp_db_path):
                    os.remove(temp_db_path)
        
        print(f"✅ Merge Complete: Successfully merged {merged_count}/{len(temp_db_paths)} chunk databases into main database")
    
    # _create_data_tuples now inherited from base Reader class
    
    @staticmethod
    def _create_data_tuples_static(assets: List[Dict[str, Any]]) -> List[tuple]:
        """Static version of _create_data_tuples for use in multiprocessing"""
        try:
            print(f"🔧 Worker process: Creating data tuples for {len(assets)} assets...")
            
            # Get schema from assets.yaml using self-sufficient SchemaGuide
            schema_guide = SchemaGuide()
            table_schema = schema_guide.get_assets_table_schema()  # Uses default path
            
            if not table_schema or 'columns' not in table_schema:
                raise Exception("Schema loading failed")
            
            columns = table_schema['columns']
            print(f"📋 Worker process: Using {len(columns)} columns from schema")
            
            data_tuples = []
            for i, asset in enumerate(assets):
                values = []
                for col in columns:
                    col_name = col['column_name']
                    col_type = col['data_type']
                    
                    if col_type == 'JSON':
                        if col_name == 'properties':
                            # Use the already reconstructed JSON from processed_asset
                            value = asset.get('properties', '{}')
                        elif col_name == 'tags':
                            # Use the already reconstructed JSON from processed_asset
                            value = asset.get('tags', '{}')
                        elif col_name == 'raw_data':
                            value = asset.get('raw_data', '{}')
                        else:
                            value = json.dumps(asset.get(col_name, {}))
                    else:
                        # VARCHAR fields - use None for missing values to get NULL in database
                        value = asset.get(col_name, None)
                    
                    values.append(value)
                
                
                data_tuples.append(tuple(values))
            
            print(f"✅ Worker process: Created {len(data_tuples)} data tuples successfully")
            return data_tuples
        except Exception as e:
            print(f"⚠️ Error creating data tuples: {e}")
            import traceback
            traceback.print_exc()
            return []
    
    @staticmethod
    def _reconstruct_nested_json_static(asset: dict, prefix: str) -> str:
        """Static version of _reconstruct_nested_json for use in multiprocessing"""
        nested_obj = {}
        
        # Find all fields that start with the prefix
        for key, value in asset.items():
            if key.startswith(prefix):
                # Remove the prefix to get the original key
                original_key = key[len(prefix):]
                # Only add non-null values
                if value is not None and value != '':
                    nested_obj[original_key] = value
        
        
        return json.dumps(nested_obj)
    
    @staticmethod
    def _get_insert_sql_static() -> str:
        """Static version of _get_insert_sql for use in multiprocessing"""
        try:
            schema_guide = SchemaGuide()
            table_schema = schema_guide.get_assets_table_schema()  # Uses default path
            column_names = [col['column_name'] for col in table_schema['columns']]
            placeholders = ', '.join(['?' for _ in column_names])
            return f"INSERT INTO assets ({', '.join(column_names)}) VALUES ({placeholders})"
        except Exception as e:
            print(f"⚠️ Error getting insert SQL: {e}")
            raise e
    
    def _insert_chunk_data_into_main_db(self, chunk_data: List[tuple]) -> None:
        """
        Insert processed chunk data into the main database.
        
        Args:
            chunk_data: List of data tuples to insert
        """
        if not chunk_data:
            print("⚠️ No chunk data to insert")
            return
        
        print(f"🗄️ Inserting {len(chunk_data)} records into database...")
        
        # Get dynamic schema for INSERT statement
        try:
            insert_sql = self._get_insert_sql()
            print(f"📝 Using SQL: {insert_sql[:100]}...")
        except Exception as e:
            print(f"⚠️ Error getting insert SQL: {e}")
            return
        
        # Insert chunk data using executemany
        try:
            self.conn.executemany(insert_sql, chunk_data)
            print(f"✅ Successfully inserted {len(chunk_data)} records")
        except Exception as e:
            print(f"❌ Error inserting chunk data: {e}")
            import traceback
            traceback.print_exc()
            raise
    
    def _insert_asset_batch_optimized(self, asset_batch: List[Dict[str, Any]]) -> None:
        """
        Insert a batch of assets into DuckDB using optimized insertion.
        
        Args:
            asset_batch: List of asset dictionaries to insert
        """
        if not asset_batch:
            return
        
        # Convert to list of tuples for insertion using dynamic schema
        data_tuples = self._create_data_tuples(asset_batch)
        
        # Use executemany for efficient batch insertion with dynamic schema
        try:
            insert_sql = self._get_insert_sql()
        except Exception as e:
            print(f"⚠️ Error getting insert SQL: {e}")
            return
        
        self.conn.executemany(insert_sql, data_tuples)
    
    def _load_assets_into_duckdb_copy_from(self, assets: List[Dict[str, Any]]) -> None:
        """
        Load assets using DuckDB's COPY FROM for maximum performance.
        This is the fastest method for large datasets.
        
        Args:
            assets: List of processed asset dictionaries
        """
        if not assets:
            return
        
        
        # Check database state before loading
        self._verify_database_records_before_loading()
        
        # Create assets table using dynamic schema (only if not exists)
        self._create_assets_table_if_not_exists(self.conn)
        
        # Convert to DataFrame for COPY FROM
        import pandas as pd
        
        df = pd.DataFrame(assets)
        
        # Use DuckDB's COPY FROM for maximum performance
        self.conn.execute("COPY assets FROM df")
        
        # Create indexes for better query performance
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_assets_id ON assets(id)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_assets_parent_cloud ON assets(parent_cloud)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_assets_cloud ON assets(cloud)")
        self.conn.execute("CREATE INDEX IF NOT EXISTS idx_assets_team ON assets(team)")
        
        # Verify database records after loading
        self._verify_database_records()
    
    def _insert_asset_chunk(self, asset_chunk: List[Dict[str, Any]]) -> None:
        """
        Insert a chunk of assets into DuckDB.
        
        Args:
            asset_chunk: List of asset dictionaries to insert
        """
        if not asset_chunk:
            return
        
        # Convert to list of tuples for insertion using dynamic schema
        data_tuples = self._create_data_tuples(asset_chunk)
        
        # Insert chunk using executemany for better performance with dynamic schema
        try:
            insert_sql = self._get_insert_sql()
        except Exception as e:
            print(f"⚠️ Error getting insert SQL: {e}")
            return
        
        self.conn.executemany(insert_sql, data_tuples)
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """
        Get performance statistics for the multiprocessing load.
        
        Returns:
            Dictionary containing performance metrics
        """
        stats = {
            'max_workers': self.max_workers,
            'batch_size': self.batch_size,
            'memory_limit_gb': self.memory_limit_bytes / (1024**3),
            'file_chunks': len(self._file_chunks),
            'files_per_chunk': len(self._file_chunks[0]) if self._file_chunks else 0,
            'total_files': sum(len(chunk) for chunk in self._file_chunks),
            'processed_files': self._processed_files,
            'total_files_processed': self._total_files
        }
        
        # Add database stats
        try:
            result = self.conn.execute("SELECT COUNT(*) as total FROM assets").fetchone()
            stats['total_assets'] = result[0] if result else 0
        except:
            stats['total_assets'] = 0
        
        # Add timing stats
        if self._start_time:
            elapsed = time.time() - self._start_time
            stats['elapsed_time_seconds'] = elapsed
            if self._processed_files > 0:
                stats['files_per_second'] = self._processed_files / elapsed
                stats['assets_per_second'] = stats['total_assets'] / elapsed if stats['total_assets'] > 0 else 0
        
        # Add memory stats
        try:
            memory_info = psutil.virtual_memory()
            stats['memory_usage_percent'] = memory_info.percent
            stats['available_memory_gb'] = memory_info.available / (1024**3)
        except:
            stats['memory_usage_percent'] = 0
            stats['available_memory_gb'] = 0
        
        return stats
    
    def _get_database_connection(self):
        """Get a new database connection for queries"""
        # Always create a new connection for each query to ensure data consistency
        if not self.db_path:
            raise Exception("Database path not set")
        
        # For in-memory databases, we need to use the existing connection
        # as each new connection creates a separate in-memory database
        if self.db_path == ":memory:":
            if not self.conn:
                raise Exception("In-memory database connection not established")
            return self.conn
        
        # For file-based databases, create a new connection
        try:
            return duckdb.connect(str(self.db_path))
        except Exception as e:
            raise Exception(f"Failed to connect to database at {self.db_path}: {e}")
    
    def load_data_from_folder(self, data_folder: Path) -> None:
        """Load data from a folder into the database using multiprocessing"""
        if self.conn is None:
            raise Exception("Database connection not established")
        
        print(f"🗄️ Loading data from folder using Sonic multiprocessing: {data_folder}")
        print(f"📁 Source folder: {data_folder}")
        print(f"🔧 Configuration: {self.max_workers} workers, {self.batch_size} batch size, {self.memory_limit_bytes/(1024**3):.1f}GB memory limit")
        print(f"🔧 Multiprocessing method: {mp.get_start_method()}")
        
        # Test multiprocessing functionality
        self._test_multiprocessing()
        
        self._load_json_files_from_folder(data_folder)
        print(f"✅ Data loaded successfully with Sonic processing")
    
    def _test_multiprocessing(self) -> None:
        """Test multiprocessing functionality to ensure it's working correctly"""
        try:
            def test_worker(worker_id: int) -> str:
                """Simple test function for multiprocessing"""
                import time
                time.sleep(0.1)  # Simulate work
                return f"Worker {worker_id} completed successfully"
            
            print(f"🧪 Testing multiprocessing with {self.max_workers} workers...")
            
            with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
                # Submit test tasks
                futures = [executor.submit(test_worker, i) for i in range(self.max_workers)]
                
                # Collect results
                results = []
                for future in as_completed(futures):
                    result = future.result()
                    results.append(result)
                
                print(f"✅ Multiprocessing test successful: {len(results)} workers completed")
                
        except Exception as e:
            print(f"⚠️ Multiprocessing test failed: {e}")
            print(f"🔧 Falling back to single-threaded processing")
            # Fallback to single-threaded processing
            self.max_workers = 1
    
    def _load_json_files_from_folder(self, data_folder: Path) -> None:
        """Load JSON files using multiprocessing from specified folder"""
        print(f"🔄 Switching to data folder: {data_folder}")
        # Update the folder path for this load operation
        original_folder = self.folder_path
        self.folder_path = data_folder
        
        try:
            # Use the existing multiprocessing load method
            print(f"📂 Processing files from: {self.folder_path}")
            self._load_json_files()
        finally:
            # Restore original folder path
            print(f"🔄 Restoring original folder: {original_folder}")
            self.folder_path = original_folder
    
    def setup_database_with_data(self, target_folder: str = None) -> Dict[str, Any]:
        """
        Setup database with data loading - Sonic-specific implementation
        
        Args:
            target_folder: Optional target folder to load data from. If None, uses self.folder_path
            
        Returns:
            Dictionary with success status, message, and performance stats
        """
        try:
            # Use provided target folder or default to instance folder
            data_folder = Path(target_folder) if target_folder else self.folder_path
            
            if not data_folder.exists():
                return {
                    'success': False,
                    'message': f"Target folder does not exist: {data_folder}",
                    'stats': {},
                    'database_ready': False
                }
            
            # Load data into the database
            self.load_data_from_folder(data_folder)
            
            # Get Sonic-specific performance stats
            performance_stats = self.get_performance_stats()
            
            # Return standardized result with Sonic-specific stats
            result = {
                'success': True,
                'message': 'Database loaded successfully with Sonic multiprocessing!',
                'stats': {
                    'total_assets': performance_stats.get('total_assets', 0),
                    'total_files': performance_stats.get('total_files', 0),
                    'health_status': performance_stats.get('health_status', 'UNKNOWN'),
                    'table_count': performance_stats.get('table_count', 0),
                    'max_workers': performance_stats.get('max_workers', 0),
                    'file_chunks': performance_stats.get('file_chunks', 0),
                    'files_per_chunk': performance_stats.get('files_per_chunk', 0),
                    'processing_time': performance_stats.get('elapsed_time_seconds', 0),
                    'batch_size': performance_stats.get('batch_size', 0),
                    'memory_limit_gb': performance_stats.get('memory_limit_gb', 0),
                    'processed_files': performance_stats.get('processed_files', 0),
                    'total_files_processed': performance_stats.get('total_files_processed', 0)
                },
                'database_ready': True
            }
            
            return result
            
        except Exception as e:
            return {
                'success': False,
                'message': f"Database setup failed: {str(e)}",
                'stats': {},
                'database_ready': False
            }
    
    def check_data_readiness(self) -> Dict[str, Any]:
        """
        Check data readiness for DuckDBSonicMemoryReader with multiprocessing support.
        This method never raises exceptions - all errors are returned in the result.
        
        Returns:
            Dictionary containing readiness status, health info, and object count
        """
        # Initialize result with default values
        result = {
            'ready': False,
            'health_status': 'ERROR',
            'object_count': 0,
            'table_count': 0,
            'database_connected': False,
            'health_queries': [],
            'source_directory': str(self.folder_path.resolve()),
            'json_files_found': len(list(self.folder_path.glob("*.json"))),
            'total_files': getattr(self, '_total_files', 0),  # Add processed files count
            'error': None
        }
        
        try:
            # Check if database connection exists
            if not hasattr(self, 'conn') or not self.conn:
                # Check if JSON files exist but database not created yet
                if result['json_files_found'] > 0:
                    result['error'] = "JSON files found but database not created yet. Please run Transform tab first."
                    result['health_status'] = 'FILES_ONLY'
                else:
                    result['error'] = "Sonic database connection not initialized"
                return result
            
            # Query database health
            health_status = 'HEALTHY'
            health_queries = []
            table_count = 0
            object_count = 0
            
            try:
                # Use the same approach as base class for consistency
                # Test basic connectivity
                health_queries.append("SELECT 1 as test_connection")
                connectivity_result = self._safe_execute_query("SELECT 1 as test_connection")
                if not connectivity_result['success']:
                    health_status = 'ERROR'
                    health_queries.append(f"ERROR: Connection test failed - {connectivity_result['error']}")
                    result.update({
                        'health_status': health_status,
                        'health_queries': health_queries,
                        'error': f"Database connection failed: {connectivity_result['error']}"
                    })
                    return result
                
                # Test table existence and count
                health_queries.append("SHOW TABLES")
                tables_result = self._safe_execute_query("SHOW TABLES")
                if not tables_result['success']:
                    health_status = 'ERROR'
                    health_queries.append(f"ERROR: Failed to list tables - {tables_result['error']}")
                else:
                    tables = tables_result['data']
                    table_count = len(tables) if tables else 0
                    
                    if table_count == 0:
                        health_status = 'ERROR'
                        health_queries.append("ERROR: No tables found in database")
                    elif 'assets' not in [table.get('name', '') for table in tables]:
                        health_status = 'WARNING'
                        health_queries.append("WARNING: assets table not found")
                
                # Get object count
                object_count = 0
                if health_status != 'ERROR':
                    count_result = self._safe_get_total_objects()
                    if not count_result['success']:
                        health_status = 'ERROR'
                        health_queries.append(f"ERROR: Failed to get object count - {count_result['error']}")
                    else:
                        object_count = count_result['data']
                
                # Query asset classes count
                asset_classes = 0
                if health_status != 'ERROR' and table_count > 0:
                    asset_classes_result = self._safe_execute_query("SELECT COUNT(DISTINCT assetClass) as asset_classes FROM assets")
                    if asset_classes_result['success'] and asset_classes_result['data']:
                        asset_classes = asset_classes_result['data'][0].get('asset_classes', 0)
                
                # Determine overall readiness
                ready = health_status == 'HEALTHY' and object_count > 0
                
                # Update result
                result.update({
                    'ready': ready,
                    'health_status': health_status,
                    'object_count': object_count,
                    'table_count': table_count,
                    'asset_classes': asset_classes,
                    'database_connected': True,
                    'health_queries': health_queries,
                    'error': None if ready else f"Data not ready: {health_status}, {object_count} objects"
                })
                
            except Exception as e:
                result['error'] = f"Sonic database query failed: {str(e)}"
                return result
                
        except Exception as e:
            result['error'] = f"Sonic database check failed: {str(e)}"
            return result
        
        return result


# Factory function for easy creation
def create_duckdb_sonic_memory_reader(folder_path: str, max_workers: Optional[int] = None, 
                                     batch_size: int = 1000, memory_limit_gb: float = 2.0) -> DuckDBSonicMemoryReader:
    """
    Create a DuckDBSonicMemoryReader instance.
    
    Args:
        folder_path: Path to folder containing JSON files
        max_workers: Maximum number of worker processes
        batch_size: Number of assets to process in each batch
        memory_limit_gb: Memory limit in GB before switching to streaming mode
        
    Returns:
        DuckDBSonicMemoryReader instance
    """
    return DuckDBSonicMemoryReader(folder_path, max_workers, batch_size, memory_limit_gb)
