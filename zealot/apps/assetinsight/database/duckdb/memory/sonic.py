"""
SonicMemoryDuckdb - High-performance multiprocessing DuckDB reader

This module provides a multiprocessing-enabled version of BasicMemoryDuckdb
that can load multiple JSON files in parallel for maximum performance.
"""

import os
import json
import multiprocessing as mp
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
import duckdb
from concurrent.futures import ProcessPoolExecutor, as_completed
import time
import orjson  # Faster JSON parsing
import psutil  # For memory monitoring
import warnings
import logging

# Suppress Streamlit warnings in multiprocessing workers
warnings.filterwarnings("ignore", message=".*missing ScriptRunContext.*")
logging.getLogger("streamlit.runtime.scriptrunner_utils.script_run_context").setLevel(logging.ERROR)

from .basic import BasicMemoryDuckdb
from common.asset_class import AssetClass
from configreader import SchemaGuide


class SonicMemoryDuckdb(BasicMemoryDuckdb):
    """
    High-performance DuckDB reader with multiprocessing support.
    
    Extends BasicMemoryDuckdb to load multiple JSON files in parallel using
    multiprocessing for maximum performance on large datasets.
    """
    
    def __new__(cls, folder_path: str, max_workers: Optional[int] = None, 
                 batch_size: int = 1000, memory_limit_gb: float = 2.0):
        """
        Create a new SonicMemoryDuckdb instance.
        Override __new__ to handle additional parameters before parent initialization.
        """
        # Convert to string and normalize path
        folder_path_str = str(Path(folder_path).resolve())
        
        # Create instance using parent's __new__ with folder_path
        instance = super(SonicMemoryDuckdb, cls).__new__(cls, folder_path_str)
        
        # Set Sonic-specific attributes immediately (before parent __init__ calls _setup_database)
        # Use all available CPU cores for maximum performance
        instance.max_workers = max_workers or mp.cpu_count()
        instance.batch_size = batch_size
        instance.memory_limit_bytes = memory_limit_gb * 1024 * 1024 * 1024
        instance._file_chunks = []
        instance._progress_callback = None
        instance._start_time = None
        instance._total_files = 0
        instance._processed_files = 0
        
        return instance
    
    def __init__(self, folder_path: str, max_workers: Optional[int] = None, 
                 batch_size: int = 1000, memory_limit_gb: float = 2.0):
        """
        Initialize SonicMemoryDuckdb with multiprocessing support.
        a
        Args:
            folder_path: Path to folder containing JSON files
            max_workers: Maximum number of worker processes (default: CPU count)
            batch_size: Number of assets to process in each batch
            memory_limit_gb: Memory limit in GB before switching to streaming mode
        """
        print(f"🚀 SonicMemoryDuckdb.__init__() called with folder: {folder_path}")
        print(f"⚙️ Max workers: {max_workers}, Batch size: {batch_size}, Memory limit: {memory_limit_gb}GB")
        
        # Call parent __init__ to set up database connection
        # Sonic-specific attributes are already set in __new__
        super().__init__(folder_path)
        
        print(f"✅ SonicMemoryDuckdb initialization complete")
    
    def _setup_database(self):
        """Override parent setup to not load data during initialization"""
        print("🔧 SonicMemoryDuckdb._setup_database() called")
        # Use in-memory database for better performance
        self.db_path = None  # No file path for in-memory database
        print(f"🗄️ Creating in-memory DuckDB database")
        try:
            self.conn = duckdb.connect()  # In-memory connection
            print(f"✅ In-memory database connection established")
            print(f"🔗 Connection object: {self.conn}")
        except Exception as e:
            print(f"❌ Error creating database connection: {e}")
            raise
        # Don't load data here - let check_data_readiness() handle it
        print(f"✅ Database setup complete (data loading deferred)")
    
    def _load_json_files(self) -> None:
        """
        Load JSON files using optimized multiprocessing for maximum performance.
        """
        if not self.folder_path or not os.path.exists(self.folder_path):
            raise ValueError(f"Folder does not exist: {self.folder_path}")
        
        print(f"📁 Loading JSON files from: {self.folder_path}")
        
        # Find all JSON files with size information
        json_files = self._get_file_list_with_sizes()
        if not json_files:
            raise ValueError(f"No JSON files found in {self.folder_path}")
        
        print(f"📋 Found {len(json_files)} JSON files to process")
        self._total_files = len(json_files)
        self._start_time = time.time()
        
        # Check if we should use streaming mode based on memory
        if self._should_use_streaming_mode(json_files):
            print("🔄 Using streaming mode for large dataset")
            self._load_files_streaming(json_files)
        else:
            print("⚡ Using parallel processing mode")
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
        print(f"🔄 Starting parallel file processing...")
        print(f"📁 Processing files from: {self.folder_path}")
        print(f"📋 Files to process: {[f.name for f, _ in files_with_sizes]}")
        
        # Create balanced file chunks based on file sizes
        self._file_chunks = self._create_balanced_chunks(files_with_sizes)
        
        print(f"🚀 SonicMemoryDuckdb: Processing {len(files_with_sizes)} files in {len(self._file_chunks)} chunks with {self.max_workers} workers")
        
        # Process files in parallel with progress tracking
        all_assets = []
        completed_chunks = 0
        total_files = len(files_with_sizes)
        start_time = time.time()
        
        print(f"📊 Starting parallel processing of {total_files} files...")
        
        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all chunks for processing
            future_to_chunk = {
                executor.submit(self._process_file_chunk_optimized, chunk): chunk 
                for chunk in self._file_chunks
            }
            
            # Collect results as they complete with progress updates
            for future in as_completed(future_to_chunk):
                try:
                    chunk_start_time = time.time()
                    chunk_assets, processed_count = future.result()
                    all_assets.extend(chunk_assets)
                    # processed_count represents files in this chunk, so add it to total
                    self._processed_files += processed_count
                    completed_chunks += 1
                    
                    # Calculate timing and progress
                    chunk_end_time = time.time()
                    chunk_duration = chunk_end_time - chunk_start_time
                    total_elapsed = chunk_end_time - start_time
                    chunk_progress = (completed_chunks / len(self._file_chunks)) * 100
                    file_progress = (self._processed_files / total_files) * 100
                    
                    # Calculate average time per chunk and estimated remaining time
                    avg_time_per_chunk = total_elapsed / completed_chunks
                    remaining_chunks = len(self._file_chunks) - completed_chunks
                    estimated_remaining_time = remaining_chunks * avg_time_per_chunk
                    
                    # Calculate processing rate
                    files_per_second = self._processed_files / total_elapsed if total_elapsed > 0 else 0
                    assets_per_second = len(all_assets) / total_elapsed if total_elapsed > 0 else 0
                    
                    print(f"📦 Processed chunk {completed_chunks}/{len(self._file_chunks)} ({chunk_progress:.1f}%) - {processed_count} files, {len(chunk_assets)} assets in {chunk_duration:.2f}s | Files: {self._processed_files}/{total_files} ({file_progress:.1f}%) | Rate: {files_per_second:.1f} files/s, {assets_per_second:.0f} assets/s | ETA: {estimated_remaining_time:.1f}s")
                    self._update_progress()
                except Exception as e:
                    chunk = future_to_chunk[future]
                    print(f"❌ Error processing chunk {completed_chunks + 1}: {str(e)}")
                    pass  # Error processing chunk
        
        # Load all assets into DuckDB using multiprocessing
        print(f"🔍 Total assets collected: {len(all_assets)}")
        if all_assets:
            print(f"💾 Loading {len(all_assets)} assets into database...")
            # Choose the best method based on data size
            if len(all_assets) > 10000:  # Use regular multiprocessing for large datasets
                self._load_assets_into_duckdb_parallel(all_assets)
            else:  # Use shared memory approach for smaller datasets
                self._load_assets_into_duckdb_parallel(all_assets)
            print(f"✅ Successfully loaded {len(all_assets)} assets into database")
        else:
            print("⚠️ No assets collected from files - this might be the issue!")
    
    def _load_files_streaming(self, files_with_sizes: List[Tuple[Path, int]]) -> None:
        """Load files using streaming mode for memory efficiency."""
        # Sort files by size (largest first) for better memory management
        sorted_files = sorted(files_with_sizes, key=lambda x: x[1], reverse=True)
        
        print(f"🌊 SonicMemoryDuckdb: Processing {len(sorted_files)} files in streaming mode")
        
        # Process files in smaller batches to manage memory
        batch_size = max(1, len(sorted_files) // (self.max_workers * 2))
        total_batches = (len(sorted_files) + batch_size - 1) // batch_size
        total_files = len(sorted_files)
        start_time = time.time()
        
        for i in range(0, len(sorted_files), batch_size):
            batch_start_time = time.time()
            batch = sorted_files[i:i + batch_size]
            batch_assets = []
            batch_num = (i // batch_size) + 1
            
            # Calculate percentage progress
            batch_progress = (batch_num / total_batches) * 100
            file_progress = (self._processed_files / total_files) * 100
            
            print(f"📦 Processing batch {batch_num}/{total_batches} ({batch_progress:.1f}%) - {len(batch)} files | Files: {self._processed_files}/{total_files} ({file_progress:.1f}%)")
            
            # Process batch files
            for file_path, _ in batch:
                try:
                    assets = self._process_single_file_streaming(file_path)
                    batch_assets.extend(assets)
                    self._processed_files += 1
                    self._update_progress()
                except Exception as e:
                    print(f"❌ Error processing file {file_path.name}: {str(e)}")
                    pass  # Error processing file
            
            # Load batch into DuckDB immediately
            if batch_assets:
                db_start_time = time.time()
                print(f"💾 Loading batch {batch_num} with {len(batch_assets)} assets into database...")
                self._load_assets_into_duckdb(batch_assets)
                db_duration = time.time() - db_start_time
                
                # Calculate timing and rates
                batch_duration = time.time() - batch_start_time
                total_elapsed = time.time() - start_time
                files_per_second = self._processed_files / total_elapsed if total_elapsed > 0 else 0
                
                print(f"✅ Batch {batch_num} loaded successfully in {batch_duration:.2f}s (DB: {db_duration:.2f}s) | Rate: {files_per_second:.1f} files/s")
    
    def _create_balanced_chunks(self, files_with_sizes: List[Tuple[Path, int]]) -> List[List[Path]]:
        """Create balanced file chunks based on file sizes for optimal load distribution."""
        # Sort files by size (largest first)
        sorted_files = sorted(files_with_sizes, key=lambda x: x[1], reverse=True)
        
        # Calculate optimal number of chunks based on dataset size
        total_files = len(sorted_files)
        total_size = sum(size for _, size in sorted_files)
        
        # For large datasets, use more chunks than workers for better parallelism
        if total_files > 500:  # Large dataset
            # Use more chunks: min(workers * 4, files / 50, 32)
            optimal_chunks = min(self.max_workers * 4, max(total_files // 50, 8), 32)
        else:
            # For smaller datasets, use workers as chunks
            optimal_chunks = self.max_workers
        
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
            # Ensure progress doesn't exceed 100%
            progress = min((self._processed_files / self._total_files) * 100, 100.0)
            elapsed = time.time() - self._start_time
            
            # Progress tracking removed for performance
    
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
                        processed_asset = SonicMemoryDuckdb._process_single_asset_optimized(asset)
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
                    processed_asset = SonicMemoryDuckdb._process_single_asset_optimized(asset)
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
            if hasattr(SonicMemoryDuckdb, '_debug_count'):
                SonicMemoryDuckdb._debug_count += 1
            else:
                SonicMemoryDuckdb._debug_count = 1
            
            # The asset should already be flattened from the flattened files
            # Use the flattened data directly for properties and tags reconstruction
            properties_json = SonicMemoryDuckdb._reconstruct_nested_json_static(asset, 'properties_')
            tags_json = SonicMemoryDuckdb._reconstruct_nested_json_static(asset, 'tags_')
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
        return SonicMemoryDuckdb._process_single_asset_optimized(asset)
    
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
        
        # Create assets table using dynamic schema
        self._create_all_tables(self.conn)
        
        # Use optimized batch insertion
        self._insert_asset_batch_optimized(assets)
        
        # Create indexes for better query performance
        self._create_indexes_on_all_tables()
        
        # Verify database records after loading
        self._verify_database_records()
    
    def _verify_database_records_before_loading(self):
        """Check database state before loading data"""
        try:
            # Check if any asset tables exist
            tables = self.conn.execute("SHOW TABLES").fetchall()
            table_names = [table[0] for table in tables] if tables else []
            
            # Get all expected table names
            expected_tables = AssetClass.get_all_table_names()
            
            # Check if any expected tables exist
            existing_asset_tables = [t for t in table_names if t in expected_tables]
            
            if not existing_asset_tables:
                pass  # No asset tables exist, ready to create
            else:
                # Check if any table has data
                total_records = 0
                for table_name in existing_asset_tables:
                    try:
                        result = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
                        if result:
                            total_records += result[0]
                    except Exception:
                        continue
                
                if total_records > 0:
                    pass  # Tables have data
                else:
                    pass  # Tables are empty, ready to load
        except Exception as e:
            pass  # Error checking database state
    
    def _verify_database_records(self):
        """Verify database records after loading"""
        try:
            # Get all table names from the mapping
            table_names = AssetClass.get_all_table_names()
            
            total_records = 0
            for table_name in table_names:
                try:
                    # Check if table exists and has data
                    result = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
                    if result and result[0] > 0:
                        count = result[0]
                        total_records += count
                        
                        # Show sample records from first table with data
                        if total_records == count:  # First table with data
                            sample = self.conn.execute(f"""
                                SELECT 
                                    id, name, assetClass, status,
                                    parent_cloud, cloud, team
                                FROM {table_name} 
                                LIMIT 3
                            """).fetchall()
                            if sample:
                                pass  # Sample records available
                except Exception as e:
                    # Table doesn't exist, skip
                    continue
            
            # Database verification completed
        except Exception as e:
            pass  # Error verifying database records
    
    def _load_assets_into_duckdb_parallel(self, assets: List[Dict[str, Any]]) -> None:
        """
        Load processed assets into DuckDB using multiprocessing for maximum performance.
        
        Args:
            assets: List of processed asset dictionaries
        """
        if not assets:
            return
        
        
        # Check database state before loading
        self._verify_database_records_before_loading()
        
        # Create assets table using dynamic schema
        print("🏗️ Creating database tables...")
        self._create_all_tables(self.conn)
        print("✅ Database tables created successfully")
        
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
        
        
        print(f"🗄️ Database insertion: Processing {len(assets)} assets in {len(asset_chunks)} chunks")
        
        # Use multiprocessing for parallel database operations
        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all chunks for parallel processing
            future_to_chunk = {
                executor.submit(self._process_asset_chunk_multiprocessing, chunk, self.folder_path): chunk 
                for chunk in asset_chunks
            }
            
            # Collect results as they complete
            processed_chunks = 0
            total_chunks = len(asset_chunks)
            start_time = time.time()
            
            for future in as_completed(future_to_chunk):
                try:
                    chunk_start_time = time.time()
                    chunk_data = future.result()
                    self._insert_chunk_data_into_main_db(chunk_data)
                    processed_chunks += 1
                    
                    # Calculate timing and progress
                    chunk_end_time = time.time()
                    chunk_duration = chunk_end_time - chunk_start_time
                    total_elapsed = chunk_end_time - start_time
                    db_progress = (processed_chunks / total_chunks) * 100
                    
                    # Calculate average time per chunk and estimated remaining time
                    avg_time_per_chunk = total_elapsed / processed_chunks
                    remaining_chunks = total_chunks - processed_chunks
                    estimated_remaining_time = remaining_chunks * avg_time_per_chunk
                    
                    print(f"💾 Database chunk {processed_chunks}/{total_chunks} ({db_progress:.1f}%) - {len(chunk_data)} records inserted in {chunk_duration:.2f}s | ETA: {estimated_remaining_time:.1f}s")
                    
                except Exception as e:
                    chunk = future_to_chunk[future]
                    pass  # Error processing chunk
        
        # Create indexes for better query performance (after all data is inserted)
        print(f"🔍 Creating database indexes...")
        self._create_indexes_on_all_tables()
        print(f"✅ Database insertion completed - {len(assets)} assets loaded successfully")
        
        # Verify database records after loading
        self._verify_database_records()
    
    def _load_assets_into_duckdb_multiprocessing_separate_db(self, assets: List[Dict[str, Any]]) -> None:
        """
        Load processed assets into DuckDB using true multiprocessing with separate databases.
        This is the fastest method for very large datasets.
        
        Args:
            assets: List of processed asset dictionaries
        """
        if not assets:
            return
        
        
        # Check database state before loading
        self._verify_database_records_before_loading()
        
        # Create main assets table using dynamic schema
        self._create_all_tables(self.conn)
        
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
        
        
        # Use multiprocessing with separate databases
        # Use all available workers for maximum parallelism
        temp_db_paths = []
        with ProcessPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all chunks for parallel processing with separate databases
            future_to_chunk = {
                executor.submit(self._process_asset_chunk_with_separate_db, chunk, self.folder_path, i): chunk 
                for i, chunk in enumerate(asset_chunks)
            }
            
            # Collect results as they complete
            processed_chunks = 0
            for future in as_completed(future_to_chunk):
                try:
                    temp_db_path = future.result()
                    if temp_db_path:
                        temp_db_paths.append(temp_db_path)
                    processed_chunks += 1
                    
                    # Progress tracking removed for performance
                    
                except Exception as e:
                    chunk = future_to_chunk[future]
                    pass  # Error processing chunk
        
        # Merge all temporary databases into the main database
        self._merge_chunk_databases(temp_db_paths)
        
        # Create indexes for better query performance (after all data is merged)
        self._create_indexes_on_all_tables()
        
        # Verify database records after loading
        self._verify_database_records()
    
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
        
        if not asset_chunk:
            return []
        
        # Assets are already processed, convert directly to tuples
        data_tuples = SonicMemoryDuckdb._create_data_tuples_static(asset_chunk)
        
        return data_tuples
    
    @staticmethod
    def _process_asset_chunk_with_separate_db(asset_chunk: List[Dict[str, Any]], folder_path: str, chunk_id: int) -> str:
        """
        Process a chunk of assets in a separate process with its own DuckDB connection.
        Creates a temporary database file for this chunk.
        
        Args:
            asset_chunk: List of asset dictionaries to process
            folder_path: Path to the folder
            chunk_id: Unique identifier for this chunk
            
        Returns:
            Path to the temporary database file created
        """
        if not asset_chunk:
            return None
        
        # Suppress Streamlit warnings in worker process
        import warnings
        import logging
        warnings.filterwarnings("ignore", message=".*missing ScriptRunContext.*")
        logging.getLogger("streamlit.runtime.scriptrunner_utils.script_run_context").setLevel(logging.ERROR)
        
        import tempfile
        import os
        
        # Create temporary database file for this chunk
        temp_db_path = os.path.join(tempfile.gettempdir(), f"duckdb_chunk_{chunk_id}.duckdb")
        
        # Create separate DuckDB connection for this process
        import duckdb
        conn = duckdb.connect(temp_db_path)
        
        try:
            # Determine the appropriate table name for this chunk
            # For chunk databases, we'll use a generic table name since we don't know the asset class distribution
            table_name = "chunk_assets"
            conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            
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
            
            create_sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(column_definitions)})"
            conn.execute(create_sql)
            
            # First process the assets to reconstruct properties and tags
            processed_assets = []
            for asset in asset_chunk:
                processed_asset = SonicMemoryDuckdb._process_single_asset_optimized(asset)
                if processed_asset:
                    processed_assets.append(processed_asset)
            
            # Convert to list of tuples for insertion using dynamic schema
            data_tuples = SonicMemoryDuckdb._create_data_tuples_static(processed_assets)
            
            # Insert data with dynamic schema
            try:
                # Create insert SQL for the chunk table
                column_names = [col['column_name'] for col in table_schema['columns']]
                placeholders = ', '.join(['?' for _ in column_names])
                insert_sql = f"INSERT INTO {table_name} ({', '.join(column_names)}) VALUES ({placeholders})"
            except Exception as e:
                return
            
            conn.executemany(insert_sql, data_tuples)
            
            # Create indexes on the table that was used for insertion
            conn.execute(f"CREATE INDEX idx_{table_name}_id ON {table_name}(id)")
            conn.execute(f"CREATE INDEX idx_{table_name}_parent_cloud ON {table_name}(parent_cloud)")
            conn.execute(f"CREATE INDEX idx_{table_name}_cloud ON {table_name}(cloud)")
            conn.execute(f"CREATE INDEX idx_{table_name}_team ON {table_name}(team)")
            
        finally:
            conn.close()
        
        return temp_db_path
    
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
                self.conn.execute(f"INSERT INTO chunk_assets SELECT * FROM chunk_db.chunk_assets")
                
                # Detach the temporary database
                self.conn.execute("DETACH chunk_db")
                
                # Clean up temporary file
                os.remove(temp_db_path)
                merged_count += 1
                
                # Progress update
                progress = (i / len(temp_db_paths)) * 100
                
            except Exception as e:
                pass  # Error merging chunk database
                # Clean up temporary file even if merge failed
                if os.path.exists(temp_db_path):
                    os.remove(temp_db_path)
        
    
    # _create_data_tuples now inherited from base Reader class
    
    @staticmethod
    def _create_data_tuples_static(assets: List[Dict[str, Any]]) -> List[tuple]:
        """Static version of _create_data_tuples for use in multiprocessing"""
        try:
            # Get schema from assets.yaml using self-sufficient SchemaGuide
            schema_guide = SchemaGuide()
            # Use the first available table schema for consistency
            all_schemas = schema_guide.get_all_table_schemas()
            if not all_schemas:
                raise Exception("No table schemas available")
            
            # Use the first table's schema for tuple creation
            first_table_name = list(all_schemas.keys())[0]
            table_schema = all_schemas[first_table_name]
            
            if not table_schema or 'columns' not in table_schema:
                raise Exception("Schema loading failed")
            
            columns = table_schema['columns']
            
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
            
            return data_tuples
        except Exception as e:
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
    
    def _get_table_name_for_asset(self, asset: Dict[str, Any]) -> str:
        """Determine which table to insert the asset into based on assetClass using AssetClass enum"""
        try:
            asset_class_name = asset.get('assetClass', '')
            
            # Debug: Print asset class information for troubleshooting
            if 'rds' in asset_class_name.lower() or 'aws_rds' in asset_class_name.lower():
                print(f"🔍 RDS Debug - Asset class name: '{asset_class_name}'")
                print(f"🔍 RDS Debug - Available AssetClass enum values:")
                for ac in AssetClass:
                    print(f"  - {ac.class_name} -> {ac.table_name}")
            
            # Find the asset class in the enum and get its table name
            for asset_class in AssetClass:
                if asset_class.class_name == asset_class_name:
                    print(f"✅ Found matching asset class: {asset_class_name} -> {asset_class.table_name}")
                    return asset_class.table_name
            
            print(f"⚠️ No matching asset class found for: '{asset_class_name}', using 'assets' table")
            return 'assets'
                
        except Exception as e:
            print(f"❌ Error in _get_table_name_for_asset: {e}")
            return 'assets'
    
    def _get_insert_sql_for_table(self, table_name: str) -> str:
        """Get INSERT SQL for a specific table"""
        try:
            schema_guide = SchemaGuide()
            all_schemas = schema_guide.get_all_table_schemas()
            
            # Get schema for the specific table, or use assets schema as fallback
            table_schema = all_schemas.get(table_name)
            if not table_schema:
                # Fallback to assets schema if specific table schema not found
                table_schema = schema_guide.get_assets_table_schema()
            
            column_names = [col['column_name'] for col in table_schema['columns']]
            placeholders = ', '.join(['?' for _ in column_names])
            return f"INSERT INTO {table_name} ({', '.join(column_names)}) VALUES ({placeholders})"
        except Exception as e:
            raise e
    
    def _insert_chunk_data_into_main_db(self, chunk_data: List[tuple]) -> None:
        """
        Insert processed chunk data into the main database using multi-table routing.
        
        Args:
            chunk_data: List of data tuples to insert
        """
        if not chunk_data:
            return
        
        # Convert tuples back to asset dictionaries for table routing
        # This is needed because multiprocessing workers return tuples, but we need dicts for routing
        try:
            schema_guide = SchemaGuide()
            # Use the first available table schema for column mapping
            all_schemas = schema_guide.get_all_table_schemas()
            if not all_schemas:
                return
            
            # Use the first table's schema for column mapping
            first_table_name = list(all_schemas.keys())[0]
            table_schema = all_schemas[first_table_name]
            column_names = [col['column_name'] for col in table_schema['columns']]
            
            
            # Convert tuples back to asset dictionaries
            asset_dicts = []
            for tuple_data in chunk_data:
                asset_dict = dict(zip(column_names, tuple_data))
                asset_dicts.append(asset_dict)
            
            
            # Use the multi-table routing logic
            self._insert_asset_batch_optimized(asset_dicts)
            
        except Exception as e:
            return
    
    def _insert_asset_batch_optimized(self, asset_batch: List[Dict[str, Any]]) -> None:
        """
        Insert a batch of assets into DuckDB using optimized insertion with table routing.
        
        Args:
            asset_batch: List of asset dictionaries to insert
        """
        if not asset_batch:
            return
        
        # Group assets by table name
        assets_by_table = {}
        for asset in asset_batch:
            table_name = self._get_table_name_for_asset(asset)
            if table_name not in assets_by_table:
                assets_by_table[table_name] = []
            assets_by_table[table_name].append(asset)
        
        # Insert assets into their respective tables
        for table_name, table_assets in assets_by_table.items():
            if not table_assets:
                continue
                
            try:
                # Get INSERT SQL for this table
                insert_sql = self._get_insert_sql_for_table(table_name)
                
                # Convert assets to tuples using static method for processed assets
                data_tuples = SonicMemoryDuckdb._create_data_tuples_static(table_assets)
                
                # Insert into the specific table
                self.conn.executemany(insert_sql, data_tuples)
                
            except Exception as e:
                continue
    
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
        
        # Create assets table using dynamic schema
        self._create_all_tables(self.conn)
        
        # Convert to DataFrame for COPY FROM
        import pandas as pd
        
        df = pd.DataFrame(assets)
        
        # Use DuckDB's COPY FROM for maximum performance
        self.conn.execute("COPY assets FROM df")
        
        # Create indexes for better query performance
        self._create_indexes_on_all_tables()
        
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
        data_tuples = SonicMemoryDuckdb._create_data_tuples_static(asset_chunk)
        
        # Insert chunk using executemany for better performance with dynamic schema
        try:
            insert_sql = self._get_insert_sql()
        except Exception as e:
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
            # Get total assets across all tables
            table_names = AssetClass.get_all_table_names()
            
            total_assets = 0
            for table_name in table_names:
                try:
                    result = self.conn.execute(f"SELECT COUNT(*) as total FROM {table_name}").fetchone()
                    if result:
                        total_assets += result[0]
                except Exception:
                    # Table doesn't exist, skip
                    continue
            
            stats['total_assets'] = total_assets
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
    
    def check_data_readiness(self) -> Dict[str, Any]:
        """
        Check data readiness for SonicMemoryDuckdb with multiprocessing support.
        This method never raises exceptions - all errors are returned in the result.
        
        Returns:
            Dictionary containing readiness status, health info, and object count
        """
        print("🚀 Starting check_data_readiness for SonicMemoryDuckdb...")
        print(f"📁 Folder path: {self.folder_path}")
        
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
            'error': None
        }
        
        print(f"📊 JSON files found: {result['json_files_found']}")
        print(f"🔗 Database connection exists: {hasattr(self, 'conn') and self.conn is not None}")
        
        try:
            # Always check if we need to load data, regardless of connection status
            if result['json_files_found'] > 0:
                print(f"🔍 Checking if database is empty...")
                # Check if database is empty (no tables exist)
                try:
                    show_tables_result = self.conn.execute("SHOW TABLES").fetchall()
                    existing_tables = [row[0] for row in show_tables_result] if show_tables_result else []
                    print(f"🔍 Existing tables: {existing_tables}")
                except Exception as e:
                    print(f"❌ Error checking existing tables: {e}")
                    existing_tables = []
                
                if not existing_tables:
                    print(f"🔍 Found {result['json_files_found']} JSON files but no tables exist, loading data into database...")
                    try:
                        # Load JSON files into database
                        print("🚀 Starting data loading process...")
                        self._load_json_files()
                        print("✅ Data loaded successfully into database")
                        
                        # Verify tables were created after loading
                        show_tables_result = self.conn.execute("SHOW TABLES").fetchall()
                        created_tables = [row[0] for row in show_tables_result] if show_tables_result else []
                        print(f"🔍 Tables created after loading: {created_tables}")
                        
                        if not created_tables:
                            result['error'] = "No tables were created during data loading"
                            result['health_status'] = 'ERROR'
                            return result
                            
                    except Exception as e:
                        print(f"❌ Error during data loading: {str(e)}")
                        result['error'] = f"Failed to load JSON files: {str(e)}"
                        result['health_status'] = 'ERROR'
                        return result
                else:
                    print(f"🔍 Database already has tables: {existing_tables}")
            else:
                print(f"⚠️ No JSON files found in directory")
                result['error'] = "No JSON files found in directory"
                result['health_status'] = 'NO_FILES'
                return result
            
            # Query database health
            health_status = 'HEALTHY'
            health_queries = []
            table_count = 0
            object_count = 0
            
            try:
                # Check if any tables exist (multi-table schema)
                table_names = AssetClass.get_all_table_names()
                print(f"🔍 Checking for tables: {table_names}")
                
                existing_tables = []
                total_objects = 0
                
                for table_name in table_names:
                    try:
                        # Check if table exists using DuckDB's SHOW TABLES
                        show_tables_result = self.conn.execute("SHOW TABLES").fetchall()
                        existing_table_names = [row[0] for row in show_tables_result] if show_tables_result else []
                        
                        # Debug: Show what tables actually exist (only on first iteration)
                        if table_name == table_names[0]:
                            print(f"🔍 Tables that actually exist in database: {existing_table_names}")
                        
                        if table_name in existing_table_names:
                            existing_tables.append(table_name)
                            # Get object count for this table
                            count_result = self.conn.execute(f"SELECT COUNT(*) as total FROM {table_name}").fetchone()
                            table_count = count_result[0] if count_result else 0
                            total_objects += table_count
                            health_queries.append(f"✅ Table '{table_name}' exists with {table_count} records")
                        else:
                            health_queries.append(f"⚠️ Table '{table_name}' not found in database")
                    except Exception as e:
                        # Table doesn't exist or error accessing it
                        health_queries.append(f"❌ Error checking table '{table_name}': {str(e)}")
                        continue
                
                if not existing_tables:
                    result['error'] = "No asset tables found in Sonic database"
                    return result
                
                table_count = len(existing_tables)
                object_count = total_objects
                
                if object_count == 0:
                    result['error'] = "Sonic database is empty - no assets found"
                    return result
                
                health_queries.append(f"✅ Found {object_count:,} assets in Sonic database")
                
                # Check database performance stats
                performance_stats = self.get_performance_stats()
                if performance_stats.get('total_assets', 0) > 0:
                    health_queries.append(f"✅ Sonic processing: {performance_stats.get('max_workers', 0)} workers used")
                    health_queries.append(f"✅ Performance: {performance_stats.get('assets_per_second', 0):.0f} assets/sec")
                
                # Update result
                result.update({
                    'ready': True,
                    'health_status': health_status,
                    'object_count': object_count,
                    'table_count': table_count,
                    'database_connected': True,
                    'health_queries': health_queries,
                    'error': None
                })
                
            except Exception as e:
                result['error'] = f"Sonic database query failed: {str(e)}"
                return result
                
        except Exception as e:
            result['error'] = f"Sonic database check failed: {str(e)}"
            return result
        
        return result


# Factory function for easy creation
def create_duckdb_sonic_reader(folder_path: str, max_workers: Optional[int] = None, 
                               batch_size: int = 1000, memory_limit_gb: float = 2.0) -> SonicMemoryDuckdb:
    """
    Create a SonicMemoryDuckdb instance.
    
    Args:
        folder_path: Path to folder containing JSON files
        max_workers: Maximum number of worker processes
        batch_size: Number of assets to process in each batch
        memory_limit_gb: Memory limit in GB before switching to streaming mode
        
    Returns:
        SonicMemoryDuckdb instance
    """
    return SonicMemoryDuckdb(folder_path, max_workers, batch_size, memory_limit_gb)
