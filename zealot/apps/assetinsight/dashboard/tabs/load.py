"""
Load Tab - Handles database loading and creation
"""

import streamlit as st
from typing import Dict, Any
import sys
from pathlib import Path
import multiprocessing
import json

# Add the current directory to Python path
sys.path.append(str(Path(__file__).parent.parent.parent))

from dashboard.tabs.base import BaseTab
from utils.dataframe_utils import safe_dataframe
from database import DatabaseFactory, DatabaseType


class LoadTab(BaseTab):
    """Handles database loading and creation"""
    
    def __init__(self):
        super().__init__(
            tab_name="Load",
            description="Load and create database from normalized data"
        )
    
    def _render_content(self):
        """Render load database UI"""
        # Get the normalised data directory from session state
        normalised_data = st.session_state.get('normalised_data', {})
        target_folder = normalised_data.get('target_folder')
        
        # Directory input (pre-populate if available from Normalise tab)
        if target_folder:
            st.info(f"📁 **Source Directory**: `{target_folder}`")
        else:
            target_folder = st.text_input(
                "Waiting for normalised to be available...",
                placeholder="/path/to/normalised/data",
                key="load_data_path"
            )
        
        # Database Status Section - only show if database is already loaded
        if target_folder and st.session_state.get('database_ready', False):
            self._display_database_status(target_folder)
        
        # Load Database Section
        st.markdown("---")
        
        if st.button("Load Database", type="primary", use_container_width=True):
            if target_folder:
                st.session_state['run_load_database'] = True
                st.rerun()
            else:
                st.warning("⚠️ Please provide a data directory path")
        
        if st.session_state.get('run_load_database', False):
            st.session_state['run_load_database'] = False
            if target_folder:
                result = self._load_database(target_folder)
                st.session_state['load_result'] = result
            else:
                st.warning("⚠️ Please provide a data directory path")
        
        # Display load results
        if 'load_result' in st.session_state:
            self._display_load_results(st.session_state['load_result'])
        
        # Database Health Stats Section
        if st.session_state.get('database_ready', False) and 'database_stats' in st.session_state:
            self._display_database_health_metrics()
        
        # Table Metadata Section
        if st.session_state.get('database_ready', False):
            self._display_table_metadata()
        
        # File & Asset Explorer Section
        if st.session_state.get('database_ready', False):
            self._display_file_asset_selector()
    
    def _display_database_status(self, target_folder: str):
        """Display database status information"""
        try:
            st.markdown("**Database Status Check**")
            
            with st.spinner("Checking database status..."):
                # Use factory to create SonicMemoryDuckdb
                try:
                    reader = DatabaseFactory.create_reader(
                        DatabaseType.SONIC,
                        target_folder,
                        max_workers=multiprocessing.cpu_count(),
                        batch_size=2000,
                        memory_limit_gb=4.0
                    )
                    readiness_result = reader.check_data_readiness()
                    
                    # Display status information as metrics
                    col1, col2, col3, col4, col5 = st.columns(5)
                    
                    with col1:
                        if readiness_result.get('ready', False):
                            st.metric("🗄️ Database", "✅ Ready")
                        elif readiness_result.get('json_files_found', 0) > 0:
                            st.metric("🗄️ Database", "📄 Files Only")
                        else:
                            st.metric("🗄️ Database", "❌ Not Ready")
                    
                    with col2:
                        health_status = readiness_result.get('health_status', 'UNKNOWN')
                        st.metric("🏥 Health", health_status)
                    
                    with col3:
                        object_count = readiness_result.get('object_count', 0)
                        st.metric("📊 Assets", f"{object_count:,}")
                    
                    with col4:
                        table_count = readiness_result.get('table_count', 0)
                        st.metric("🗃️ Tables", str(table_count))
                    
                    with col5:
                        json_files_found = readiness_result.get('json_files_found', 0)
                        st.metric("📄 JSON Files", str(json_files_found))
                    
                    # Show warning if not ready
                    if not readiness_result.get('ready', False):
                        error_msg = readiness_result.get('error', 'Unknown error')
                        if readiness_result.get('health_status') == 'FILES_ONLY':
                            st.info("📄 **Status**: JSON files found but database not created yet.")
                            st.info("💡 **Next Step**: Click 'Load Database' to create the database for analysis.")
                        else:
                            st.warning(f"⚠️ Database not ready: {error_msg}")
                            st.info("💡 **Tip**: Make sure to complete the Normalise tab first to create normalized data.")
                    
                except Exception as e:
                    st.error(f"❌ Failed to check database status: {str(e)}")
                    st.info("💡 **Tip**: Make sure the directory contains normalized JSON files from the Normalise tab.")
            
        except Exception as e:
            st.error(f"❌ Failed to check database status: {str(e)}")
    
    def _load_database(self, target_folder: str) -> Dict[str, Any]:
        """Load database from normalized data"""
        try:
            with st.spinner("🗄️ Setting up database for high performance analytics..."):
                reader = DatabaseFactory.create_reader(
                    DatabaseType.SONIC,
                    target_folder,
                    max_workers=multiprocessing.cpu_count(),
                    batch_size=2000,
                    memory_limit_gb=4.0
                )
                readiness_result = reader.check_data_readiness()
                
                if readiness_result.get('ready', False):
                    # Performance stats are already included in the factory result
                    performance_stats = {
                        'max_workers': readiness_result.get('max_workers', 0),
                        'file_chunks': readiness_result.get('file_chunks', 0),
                        'files_per_chunk': readiness_result.get('files_per_chunk', 0),
                        'total_files': readiness_result.get('total_files', 0)
                    }
                    
                    # Database loaded successfully
                    result = {
                        'success': True,
                        'message': 'Database loaded successfully with SonicMemoryDuckdb!',
                        'stats': {
                            'total_assets': readiness_result.get('object_count', 0),
                            'total_files': readiness_result.get('json_files_found', 0),
                            'health_status': readiness_result.get('health_status', 'UNKNOWN'),
                            'table_count': readiness_result.get('table_count', 0),
                            'max_workers': performance_stats.get('max_workers', 0),
                            'file_chunks': performance_stats.get('file_chunks', 0),
                            'files_per_chunk': performance_stats.get('files_per_chunk', 0),
                            'processing_time': performance_stats.get('processing_time', 0)
                        },
                        'database_ready': True
                    }
                    
                    # Store in session state for other tabs
                    st.session_state['database_ready'] = True
                    st.session_state['database_stats'] = result['stats']
                    st.session_state['database_path'] = target_folder
                    
                    st.success("✅ Database loaded successfully!")
                    
                else:
                    result = {
                        'success': False,
                        'message': f"Database loading failed: {readiness_result.get('error', 'Unknown error')}",
                        'stats': {},
                        'database_ready': False
                    }
                    st.error(f"❌ {result['message']}")
                
                return result
                
        except Exception as e:
            result = {
                'success': False,
                'message': f"Database loading failed: {str(e)}",
                'stats': {},
                'database_ready': False
            }
            st.error(f"❌ {result['message']}")
            return result
        finally:
            # Factory handles reader cleanup automatically
            pass
    
    def _display_load_results(self, result: Dict[str, Any]):
        """Display load results"""
        if not result['success']:
            st.markdown("### ❌ Load Failed")
            st.error(result['message'])
    
    def _display_database_health_metrics(self):
        """Display database health stats using reader class methods"""
        st.markdown("---")
        st.markdown("### 📊 Database Health Metrics")
        
        # Get database path from session state
        target_folder = st.session_state.get('database_path')
        if not target_folder:
            st.warning("⚠️ No database path available. Please load the database first.")
            return
        
        try:
            reader = DatabaseFactory.create_reader(
                DatabaseType.SONIC,
                target_folder,
                max_workers=multiprocessing.cpu_count(),
                batch_size=2000,
                memory_limit_gb=4.0
            )
            result = reader.check_data_readiness()
            
            # Extract stats from factory result
            stats = {
                'status': 'connected' if result.get('ready', False) else 'error',
                'total_files': result.get('total_files', 0),
                'total_assets': result.get('object_count', 0),
                'asset_classes': result.get('asset_classes', 0),
                'health_status': result.get('health_status', 'Unknown')
            }
            
            if stats['status'] == 'connected' and stats.get('total_assets', 0) > 0:
                st.success("✅ Database is ready and healthy")
                
                # Display all metrics in a single row
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric(
                        "📁 Total Files", 
                        f"{stats.get('total_files', 0):,}",
                        help="Total number of JSON files processed"
                    )
                
                with col2:
                    st.metric(
                        "🏢 Total Assets", 
                        f"{stats.get('total_assets', 0):,}",
                        help="Total number of assets in the database"
                    )
                
                with col3:
                    st.metric(
                        "🏷️ Asset Classes", 
                        f"{stats.get('asset_classes', 1)} (Server)",
                        help="Number of asset classes in the database"
                    )
                
                with col4:
                    st.metric(
                        "🏥 Health Status", 
                        stats.get('health_status', 'Healthy'),
                        help="Database health status"
                    )
            else:
                st.warning("⚠️ Database not ready or empty")
                if 'error' in stats:
                    st.error(f"Error: {stats['error']}")
                else:
                    st.info("💡 Try clicking 'Load Database' again to refresh the database connection.")
                
        except Exception as e:
            st.error(f"❌ Error accessing database: {str(e)}")
            st.info("💡 Try clicking 'Load Database' again to refresh the database connection.")
    
    def _display_table_metadata(self):
        """Display table metadata with column information"""
        st.markdown("---")
        st.markdown("### 🔍 Table Metadata Explorer")
        
        # Get the database path from session state
        target_folder = st.session_state.get('database_path')
        if not target_folder:
            st.warning("⚠️ No database path available. Please load the database first.")
            return
        
        # Check if database is ready
        if not st.session_state.get('database_ready', False):
            st.warning("⚠️ Database not ready. Please click 'Load Database' first.")
            return
        
        try:
            # Use SchemaAnalyser for metadata queries
            from analyser import SchemaAnalyser
            
            schema_analyser = SchemaAnalyser()
            
            # Get list of tables using SchemaAnalyser
            tables = schema_analyser.get_table_list(target_folder)
            
            if not tables:
                st.warning("⚠️ No tables found in the database")
                st.info("💡 **Tip:** Make sure the database was loaded successfully. Try clicking 'Load Database' again.")
                return
            
            st.success(f"✅ Found {len(tables)} table(s) in the database")
            
            # Table selector
            selected_table = st.selectbox(
                "Select a table to explore:",
                tables,
                key="table_selector",
                help="Choose a table to view its column metadata"
            )
            
            if selected_table:
                # Get table metadata using SchemaAnalyser
                try:
                    metadata_result = schema_analyser.get_table_metadata(target_folder, selected_table)
                    
                    if metadata_result:
                        st.success(f"📋 Found {len(metadata_result)} columns in table '{selected_table}'")
                        
                        # Convert to DataFrame
                        import pandas as pd
                        df_metadata = pd.DataFrame(metadata_result)
                        
                        # Rename columns for better display
                        df_metadata = df_metadata.rename(columns={
                            'column_name': 'Column Name',
                            'data_type': 'Data Type',
                            'is_nullable': 'Nullable',
                            'ordinal_position': 'Position'
                        })
                        
                        # Remove Default Value column if it exists
                        if 'column_default' in df_metadata.columns:
                            df_metadata = df_metadata.drop('column_default', axis=1)
                        
                        # Display the metadata table
                        st.dataframe(
                            df_metadata,
                            use_container_width=True,
                            hide_index=True,
                            column_config={
                                "Column Name": st.column_config.TextColumn(
                                    "Column Name",
                                    help="Name of the column",
                                    width="large"
                                ),
                                "Data Type": st.column_config.TextColumn(
                                    "Data Type",
                                    help="Data type of the column",
                                    width="medium"
                                ),
                                "Nullable": st.column_config.TextColumn(
                                    "Nullable",
                                    help="Whether the column allows NULL values",
                                    width="small"
                                ),
                                "Position": st.column_config.NumberColumn(
                                    "Position",
                                    help="Position of the column in the table",
                                    width="small"
                                )
                            }
                        )
                        
                    else:
                        st.warning(f"⚠️ No metadata found for table '{selected_table}'")
                        
                except Exception as metadata_error:
                    st.error(f"❌ Error fetching table metadata: {str(metadata_error)}")
            
        except Exception as e:
            st.error(f"❌ Error accessing table metadata: {str(e)}")
            st.info("💡 **Troubleshooting:** Try reloading the database or check if the database file exists.")
    
    def _display_file_asset_selector(self):
        """Display file and asset selector with side-by-side content comparison"""
        st.markdown("---")
        st.markdown("### 🔍 File & Assets Explorer")
        
        # Get database path from session state
        target_folder = st.session_state.get('database_path')
        if not target_folder:
            st.warning("⚠️ No database path available. Please load the database first.")
            return
        
        # Check if database is ready
        if not st.session_state.get('database_ready', False):
            st.warning("⚠️ Database not ready. Please click 'Load Database' first.")
            return
        
        try:
            # Get the normalised data directory from session state (where the flattened files are)
            normalised_data = st.session_state.get('normalised_data', {})
            normalised_dir = normalised_data.get('target_folder', '')
            if not normalised_dir:
                st.warning("⚠️ No normalised data directory available. Please run the Normalise tab first.")
                return
            
            # Get list of JSON files from the normalised directory
            json_files = list(Path(normalised_dir).glob("*.json"))
            if not json_files:
                st.warning("⚠️ No flattened JSON files found in the normalised directory")
                return
            
            # File selector with unique key
            file_options = [f.name for f in json_files]
            selected_file = st.selectbox(
                "Choose flattened file:",
                file_options,
                key="load_tab_file_selector",
                help="Select a flattened JSON file to explore"
            )
            
            if selected_file:
                selected_file_path = Path(normalised_dir) / selected_file
                
                # Load the flattened file content
                try:
                    with open(selected_file_path, 'r', encoding='utf-8') as f:
                        flattened_data = json.load(f)
                    
                    if not isinstance(flattened_data, list):
                        st.warning("⚠️ Selected file does not contain a list of assets")
                        return
                    
                    # Asset selector
                    asset_options = []
                    for i, asset in enumerate(flattened_data):
                        asset_id = asset.get('id', f'asset_{i}')
                        asset_name = asset.get('name', 'Unknown')
                        asset_class = asset.get('assetClass', 'Unknown')
                        display_name = f"{asset_id} - {asset_name} ({asset_class})"
                        asset_options.append((display_name, i))
                    
                    if not asset_options:
                        st.warning("⚠️ No assets found in the selected file")
                        return
                    
                    selected_asset_display = st.selectbox(
                        "Choose asset:",
                        [option[0] for option in asset_options],
                        key="load_tab_asset_selector",
                        help="Select an asset to view its content"
                    )
                    
                    if selected_asset_display:
                        # Find the selected asset index
                        selected_asset_index = None
                        for display_name, index in asset_options:
                            if display_name == selected_asset_display:
                                selected_asset_index = index
                                break
                        
                        if selected_asset_index is not None:
                            selected_asset = flattened_data[selected_asset_index]
                            self._display_asset_comparison(selected_asset, target_folder)
                
                except Exception as file_error:
                    st.error(f"❌ Error loading file: {str(file_error)}")
        
        except Exception as e:
            st.error(f"❌ Error in file selector: {str(e)}")
    
    def _display_asset_comparison(self, asset: dict, target_folder: str):
        """Display side-by-side comparison of flattened file content and database content"""
        st.markdown("---")
        st.markdown("#### 📊 Asset Content Comparison")
        
        # Get asset ID for database query
        asset_id = asset.get('id', '')
        if not asset_id:
            st.warning("⚠️ Assets has no ID, cannot query database")
            return
        
        try:
            reader = DatabaseFactory.create_reader(DatabaseType.SONIC, target_folder)
            
            # Check if reader is ready
            readiness_result = reader.check_data_readiness()
            if readiness_result.get('ready', False):
                
                # Query for the specific asset
                db_query = f"SELECT * FROM assets WHERE id = '{asset_id}'"
                db_results = reader.execute_query(db_query)
                
                if db_results:
                    db_asset = db_results[0]
                    
                    # Create two columns for side-by-side display
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.markdown("#### 📄 Flattened File Content")
                        st.json(asset)
                    
                    with col2:
                        st.markdown("#### 🗄️ Database Content")
                        st.json(db_asset)
                    
                else:
                    st.warning(f"⚠️ Asset with ID '{asset_id}' not found in database")
            else:
                st.error("❌ Database not ready for querying")
        
        except Exception as e:
            st.error(f"❌ Error querying database: {str(e)}")
    
    def is_complete(self, workflow_state):
        """Check if database loading is complete"""
        return workflow_state.get('database_ready', False)
