"""
Minimal DuckDB Reader - Singleton Pattern
"""

import json
import duckdb
from pathlib import Path
from typing import Dict, Any, List
import tempfile
import os
from ..base import Reader


class BasicMemoryDuckdb(Reader):
    """Singleton DuckDB reader"""
    
    _instances = {}  # Class variable to store instances by folder_path
    _initialized = set()  # Track which folder_paths have been initialized
    
    def __new__(cls, folder_path: str):
        # Convert to string and normalize path
        folder_path_str = str(Path(folder_path).resolve())
        
        # Return existing instance if it exists
        if folder_path_str in cls._instances:
            return cls._instances[folder_path_str]
        
        # Create new instance
        instance = super(BasicMemoryDuckdb, cls).__new__(cls)
        cls._instances[folder_path_str] = instance
        return instance
    
    def __init__(self, folder_path: str):
        # Convert to string and normalize path
        folder_path_str = str(Path(folder_path).resolve())
        
        # Only initialize once per folder_path
        if folder_path_str not in self._initialized:
            super().__init__(folder_path)
            self.folder_path = Path(folder_path)
            self.db_path = None
            self.conn = None
            self._setup_database()
            self._initialized.add(folder_path_str)
    
    def _setup_database(self):
        """Setup DuckDB database and load JSON files"""
        # Use a persistent database file in the folder instead of temp file
        self.db_path = self.folder_path / "assets.db"
        print(f"🗄️ Creating database at: {self.db_path}")
        self.conn = duckdb.connect(str(self.db_path))
        print(f"✅ Database connection established")
        self._load_json_files()
        print(f"✅ Database setup complete")
    
    # Schema methods now inherited from base Reader class
    
    def _load_json_files(self):
        """Load JSON files into DuckDB"""
        json_files = list(self.folder_path.glob("*.json"))
        if not json_files:
            print("⚠️ No JSON files found in directory")
            return
        
        print(f"🚀 Loading {len(json_files)} JSON files into DuckDB...")
        
        # Drop existing table to ensure clean schema
        self.conn.execute("DROP TABLE IF EXISTS assets")
        
        # Create table using SchemaGuide
        self._create_assets_table(self.conn)
        
        # Load files with progress tracking
        total_assets = 0
        for i, file_path in enumerate(json_files, 1):
            file_assets = self._load_single_file(file_path)
            total_assets += file_assets
            
            # Progress update
            progress = (i / len(json_files)) * 100
            print(f"📊 Progress: {progress:.1f}% ({i}/{len(json_files)}) - Loaded {file_assets} assets from {file_path.name}")
        
        print(f"✅ Load Complete: {total_assets:,} total assets loaded from {len(json_files)} files")
    
    def _load_single_file(self, file_path: Path) -> int:
        """Load a single JSON file into DuckDB and return asset count"""
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        if not isinstance(data, list):
            return 0
        
        # Get schema for dynamic field extraction
        try:
            table_schema = self._get_schema()
        except Exception as e:
            print(f"⚠️ Could not load schema for data insertion: {e}")
            return 0
        
        # Get column names and types
        columns = table_schema['columns']
        column_names = [col['column_name'] for col in columns]
        
        asset_count = 0
        for asset in data:
            if not isinstance(asset, dict):
                continue
            
            # Extract values for all columns dynamically
            values = []
            for col in columns:
                col_name = col['column_name']
                col_type = col['data_type']
                
                if col_type == 'JSON':
                    if col_name == 'properties':
                        value = self._reconstruct_nested_json(asset, 'properties_')
                    elif col_name == 'tags':
                        value = self._reconstruct_nested_json(asset, 'tags_')
                    elif col_name == 'raw_data':
                        value = json.dumps(asset)
                    else:
                        value = json.dumps(asset.get(col_name, {}))
                else:
                    # VARCHAR fields - use None for missing values to get NULL in database
                    value = asset.get(col_name, None)
                
                values.append(value)
            
            # Build dynamic INSERT statement
            placeholders = ', '.join(['?' for _ in column_names])
            insert_sql = f"INSERT INTO assets ({', '.join(column_names)}) VALUES ({placeholders})"
            
            # Insert into database
            self.conn.execute(insert_sql, values)
            
            asset_count += 1
        
        return asset_count
    
    def execute_query(self, sql_query: str) -> List[Dict[str, Any]]:
        """Execute a SQL query and return results"""
        # Create new connection for this query to avoid connection issues
        if not self.db_path or not os.path.exists(self.db_path):
            return []
        
        conn = duckdb.connect(str(self.db_path))
        try:
            result = conn.execute(sql_query).fetchall()
            columns = [desc[0] for desc in conn.description]
            return [dict(zip(columns, row)) for row in result]
        finally:
            conn.close()
    
    def get_total_objects(self) -> int:
        """Get total number of assets in the database"""
        # Create new connection for this query to avoid connection issues
        if not self.db_path or not os.path.exists(self.db_path):
            return 0
        
        conn = duckdb.connect(str(self.db_path))
        try:
            result = conn.execute("SELECT COUNT(*) as count FROM assets").fetchone()
            return result[0] if result else 0
        finally:
            conn.close()
    
    def check_data_readiness(self) -> Dict[str, Any]:
        """
        Check data readiness by querying DuckDB database health and getting object count.
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
            'error': None
        }
        
        # Check if database connection exists
        if not hasattr(self, 'conn') or not self.conn:
            result['error'] = "Database connection not initialized"
            return result
        
        # Query database health
        health_status = 'HEALTHY'
        health_queries = []
        table_count = 0
        
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
        
        # Test table structure (only if we have tables)
        if health_status != 'ERROR' and table_count > 0:
            health_queries.append("DESCRIBE assets")
            schema_result = self._safe_execute_query("DESCRIBE assets")
            if not schema_result['success']:
                health_status = 'WARNING'
                health_queries.append(f"WARNING: Failed to describe assets table - {schema_result['error']}")
            else:
                schema = schema_result['data']
                # Get expected columns from schema
                try:
                    table_schema = self._get_schema()
                    expected_columns = [col['column_name'] for col in table_schema['columns']]
                except Exception:
                    # Fallback to basic columns if schema loading fails
                    expected_columns = ['id', 'name', 'assetClass']
                
                schema_columns = [col.get('column_name', '') for col in schema]
                missing_columns = [col for col in expected_columns if col not in schema_columns]
                if missing_columns:
                    health_status = 'WARNING'
                    health_queries.append(f"WARNING: Missing expected columns: {missing_columns}")
        
        # Query number of objects
        object_count = 0
        if health_status != 'ERROR':
            count_result = self._safe_get_total_objects()
            if not count_result['success']:
                health_status = 'ERROR'
                health_queries.append(f"ERROR: Failed to get object count - {count_result['error']}")
            else:
                object_count = count_result['data']
        
        # Determine overall readiness
        ready = health_status == 'HEALTHY' and object_count > 0
        
        # Update result with final values
        result.update({
            'ready': ready,
            'health_status': health_status,
            'object_count': object_count,
            'table_count': table_count,
            'database_connected': True,
            'health_queries': health_queries,
            'error': None if ready else f"Data not ready: {health_status}, {object_count} objects"
        })
        
        return result
    
    def _safe_execute_query(self, sql_query: str) -> Dict[str, Any]:
        """Safely execute a SQL query without raising exceptions."""
        try:
            data = self.execute_query(sql_query)
            return {'success': True, 'data': data, 'error': None}
        except Exception as e:
            return {'success': False, 'data': None, 'error': str(e)}
    
    def _safe_get_total_objects(self) -> Dict[str, Any]:
        """Safely get total objects count without raising exceptions."""
        try:
            count = self.get_total_objects()
            return {'success': True, 'data': count, 'error': None}
        except Exception as e:
            return {'success': False, 'data': 0, 'error': str(e)}
    
    # _reconstruct_nested_json now inherited from base Reader class
    
    def close(self):
        """Close the database and clean up resources"""
        if self.conn:
            self.conn.close()
        if self.db_path and os.path.exists(self.db_path):
            os.unlink(self.db_path)
    
    @classmethod
    def get_instance(cls, folder_path: str):
        """Get singleton instance for a specific folder path"""
        folder_path_str = str(Path(folder_path).resolve())
        if folder_path_str not in cls._instances:
            cls._instances[folder_path_str] = cls(folder_path)
        return cls._instances[folder_path_str]
    
    @classmethod
    def close_instance(cls, folder_path: str):
        """Close and remove instance for a specific folder path"""
        folder_path_str = str(Path(folder_path).resolve())
        if folder_path_str in cls._instances:
            instance = cls._instances[folder_path_str]
            instance.close()
            del cls._instances[folder_path_str]
            cls._initialized.discard(folder_path_str)
    
    @classmethod
    def close_all_instances(cls):
        """Close and remove all instances"""
        for instance in list(cls._instances.values()):
            instance.close()
        cls._instances.clear()
        cls._initialized.clear()
    
    @classmethod
    def get_all_instances(cls):
        """Get all active instances"""
        return dict(cls._instances)
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """
        Get performance statistics for the database reader.
        
        Returns:
            Dictionary containing performance statistics
        """
        # Create a new connection for this query to avoid connection issues
        if not self.db_path or not os.path.exists(self.db_path):
            print(f"⚠️ Database file not found: {self.db_path}")
            return {
                'status': 'not_connected',
                'total_files': 0,
                'total_assets': 0,
                'asset_classes': 0,
                'health_status': 'Unknown'
            }
        
        try:
            # Create new connection for this query
            print(f"🔌 Connecting to database: {self.db_path}")
            conn = duckdb.connect(str(self.db_path))
            
            try:
                # Get basic stats
                print("📊 Querying total assets...")
                result = conn.execute("SELECT COUNT(*) as total_assets FROM assets").fetchone()
                total_assets = result[0] if result else 0
                print(f"📊 Found {total_assets} total assets")
                
                # Get file count - BasicMemoryDuckdb doesn't track source_file, so estimate from folder
                json_files = list(self.folder_path.glob("*.json"))
                total_files = len(json_files)
                print(f"📁 Found {total_files} JSON files in folder")
                
                # Get asset classes
                print("📊 Querying asset classes...")
                result = conn.execute("SELECT COUNT(DISTINCT assetClass) as asset_classes FROM assets").fetchone()
                asset_classes = result[0] if result else 0
                print(f"📊 Found {asset_classes} asset classes")
                
                return {
                    'status': 'connected',
                    'total_files': total_files,
                    'total_assets': total_assets,
                    'asset_classes': asset_classes,
                    'health_status': 'Healthy' if total_assets > 0 else 'Empty'
                }
            finally:
                # Always close the connection
                conn.close()
                print("🔌 Connection closed")
                
        except Exception as e:
            print(f"❌ Error in get_performance_stats: {str(e)}")
            return {
                'status': 'error',
                'error': str(e),
                'total_files': 0,
                'total_assets': 0,
                'asset_classes': 0,
                'health_status': 'Error'
            }
