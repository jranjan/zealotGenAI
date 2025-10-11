#!/usr/bin/env python3
"""
Test script for SonicDuckDBReader
"""

import sys
from pathlib import Path

# Add the current directory to Python path
current_dir = Path('zealot/apps/assetinsight')
sys.path.insert(0, str(current_dir))

from database.reader.factory import ReaderFactory
import time

def test_duckdb_sonic():
    """Test DuckDBSonicReader with multiprocessing"""
    
    # Test data directory
    test_folder = "/Users/jyoti.ranjan/Downloads/assets/servers"
    
    print("🚀 Testing DuckDBSonicReader with multiprocessing...")
    print(f"📁 Test folder: {test_folder}")
    
    # Test with different worker counts
    worker_counts = [1, 2, 4, 8]
    
    for workers in worker_counts:
        print(f"\n🔧 Testing with {workers} workers...")
        
        start_time = time.time()
        
        try:
            # Create sonic reader
            result = ReaderFactory.create_sonic_reader(test_folder, max_workers=workers)
            
            end_time = time.time()
            duration = end_time - start_time
            
            if result.get('ready', False):
                print(f"✅ Success! Loaded in {duration:.2f}s")
                print(f"   📊 Total assets: {result.get('total_assets', 0):,}")
                print(f"   🗃️ Tables: {result.get('table_count', 0)}")
                print(f"   ⚡ Workers used: {result.get('max_workers', 0)}")
                print(f"   📦 File chunks: {result.get('file_chunks', 0)}")
                print(f"   📄 Files per chunk: {result.get('files_per_chunk', 0)}")
                print(f"   📁 Total files: {result.get('total_files', 0)}")
            else:
                print(f"❌ Failed: {result.get('error', 'Unknown error')}")
                
        except Exception as e:
            print(f"❌ Exception: {e}")
    
    print("\n🎯 Performance comparison complete!")

if __name__ == "__main__":
    test_duckdb_sonic()
