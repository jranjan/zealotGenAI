"""
Load Tab - Handles database loading and creation
"""

import streamlit as st
from typing import Dict, Any
import sys
from pathlib import Path
import multiprocessing

# Add the current directory to Python path
sys.path.append(str(Path(__file__).parent.parent.parent))

from dashboard.tabs.base import BaseTab
from database.reader.duckdb_sonic import DuckDBSonicReader
from utils.dataframe_utils import safe_dataframe


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
            st.info(f"📁 **Source Directory**: `{target_folder}` (from Normalise tab)")
        else:
            target_folder = st.text_input(
                "Enter path to normalised data directory:",
                placeholder="/path/to/normalised/data",
                key="load_data_path"
            )
        
        # Database Status Section - only show if database is already loaded
        if target_folder and st.session_state.get('database_ready', False):
            self._display_database_status(target_folder)
        
        # Load Database Section
        st.markdown("---")
        st.markdown("### 🗄️ Database Loading")
        st.info("🚀 Using **DuckDBSonicReader** - High-performance multiprocessing database engine")
        
        col1, col2 = st.columns([0.3, 0.7])
        
        with col1:
            if st.button("Load Database", type="primary", use_container_width=True):
                if target_folder:
                    st.session_state['run_load_database'] = True
                    st.rerun()
                else:
                    st.warning("⚠️ Please provide a data directory path")
        
        with col2:
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
    
    def _display_database_status(self, target_folder: str):
        """Display database status information"""
        reader = None
        try:
            st.markdown("**Database Status Check**")
            
            with st.spinner("Checking database status..."):
                # Try to create a DuckDBSonicReader to check status
                try:
                    reader = DuckDBSonicReader(
                        target_folder,
                        max_workers=multiprocessing.cpu_count(),
                        batch_size=2000,
                        memory_limit_gb=4.0
                    )
                    
                    # Get readiness status
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
        finally:
            # Ensure reader is properly closed
            if reader is not None:
                try:
                    reader.close()
                except Exception as close_error:
                    # Don't raise the close error, just log it
                    print(f"Warning: Error closing reader: {close_error}")
    
    def _load_database(self, target_folder: str) -> Dict[str, Any]:
        """Load database from normalized data"""
        reader = None
        try:
            with st.spinner("🗄️ Setting up database..."):
                # Create DuckDBSonicReader to load database
                reader = DuckDBSonicReader(
                    target_folder,
                    max_workers=multiprocessing.cpu_count(),
                    batch_size=2000,
                    memory_limit_gb=4.0
                )
                
                # Get database statistics
                readiness_result = reader.check_data_readiness()
                
                if readiness_result.get('ready', False):
                    # Get performance stats from DuckDBSonicReader
                    performance_stats = reader.get_performance_stats()
                    
                    # Database loaded successfully
                    result = {
                        'success': True,
                        'message': 'Database loaded successfully with DuckDBSonicReader!',
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
            # Ensure reader is properly closed
            if reader is not None:
                try:
                    reader.close()
                except Exception as close_error:
                    # Don't raise the close error, just log it
                    print(f"Warning: Error closing reader: {close_error}")
    
    def _display_load_results(self, result: Dict[str, Any]):
        """Display load results"""
        if result['success']:
            st.markdown("### ✅ Load Results")
            
            # Display statistics
            stats = result.get('stats', {})
            if stats:
                # Create summary data
                summary_data = {
                    'Metric': ['Total Assets', 'Total Files', 'Health Status', 'Tables', 'Max Workers', 'File Chunks', 'Processing Time'],
                    'Value': [
                        f"{stats.get('total_assets', 0):,}",
                        f"{stats.get('total_files', 0):,}",
                        stats.get('health_status', 'Unknown'),
                        str(stats.get('table_count', 0)),
                        str(stats.get('max_workers', 0)),
                        str(stats.get('file_chunks', 0)),
                        f"{stats.get('processing_time', 0):.2f}s" if stats.get('processing_time', 0) > 0 else 'N/A'
                    ]
                }
                
                df_summary = safe_dataframe(summary_data)
                st.dataframe(
                    df_summary,
                    width='content',
                    hide_index=True,
                    column_config={
                        col: st.column_config.TextColumn(
                            col,
                            help=f"Shows {col.lower()} information"
                        )
                        for col in df_summary.columns
                    }
                )
            
            # Show next steps
            st.info("🎯 **Next Steps**: You can now proceed to the Ownership tab to analyze the loaded data.")
            
            # Show performance benefits
            if stats.get('max_workers', 0) > 1:
                st.success(f"🚀 **Performance**: Database loaded using {stats.get('max_workers', 0)} parallel workers for maximum speed!")
            
        else:
            st.markdown("### ❌ Load Failed")
            st.error(result['message'])
    
    def is_complete(self, workflow_state):
        """Check if database loading is complete"""
        return workflow_state.get('database_ready', False)
