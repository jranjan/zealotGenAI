"""
Scan Source Tab - Handles source data for scan
"""

import streamlit as st
import os
from pathlib import Path
import sys

# Add the current directory to Python path
current_dir = Path(__file__).parent.parent.parent.parent
sys.path.insert(0, str(current_dir))

from reader import ReaderFactory, ReaderType
from ..base import BaseTab


class SourceTab(BaseTab):
    """Source tab for scan"""
    
    def __init__(self):
        super().__init__(
            tab_name="Source",
            description="Configure and manage scan source data"
        )
        # Use factory to create SonicReader for maximum performance
        self.source_reader = ReaderFactory.create_reader(ReaderType.SONIC_READER)
    
    def _render_content(self):
        """Render scan source interface"""
        st.markdown("### Dataset Directory")
        
        # Dataset directory input
        dataset_path = st.text_input(
            "Enter dataset directory:",
            value="/Users/jyoti.ranjan/Downloads/infrascan/raw",
            help="Enter the path to the dataset directory",
            key="scan_dataset_path"
        )
        
        if dataset_path:
            st.success(f"📁 Dataset directory: {dataset_path}")
        
        # Extract Button
        st.markdown("---")
        if st.button("🔍 Extract", key="extract_scan_data", type='primary', use_container_width=True):
            self._extract_scan_data()
        
        # Results Section
        if st.session_state.get('scan_source_data'):
            st.markdown("---")
            st.markdown("### 📊 Scan Extraction Results")
            source_data = st.session_state.get('scan_source_data')
            st.json(source_data)
    
    def _extract_scan_data(self):
        """Extract scan data from the selected directory"""
        dataset_path = st.session_state.get('scan_dataset_path')
        
        if not dataset_path:
            st.error("❌ Please enter a dataset directory")
            return
        
        if not os.path.exists(dataset_path):
            st.error("❌ Directory does not exist")
            return
        
        try:
            import time
            start_time = time.time()
            
            with st.spinner("🔍 Extracting scan data..."):
                source_data = self.source_reader.scan_source_directory(dataset_path)
                
                # Store results in session state
                st.session_state.scan_source_data = source_data
                
                # Calculate and display processing time
                processing_time = time.time() - start_time
                st.success(f"✅ Extraction complete!")
                st.info(f"⏱️ **Processing time = {processing_time:.2f} sec**")
                
        except Exception as e:
            st.error(f"❌ Error extracting data: {str(e)}")

