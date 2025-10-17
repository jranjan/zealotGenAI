"""
Source Tab - Handles source data analysis and validation
"""

import streamlit as st
import json
import os
from pathlib import Path
import pandas as pd
import sys

# Add the current directory to Python path
current_dir = Path(__file__).parent.parent.parent
sys.path.insert(0, str(current_dir))

from reader import ReaderFactory, ReaderType
from common.system_data import SystemDirectory
from common.asset_class import AssetClass
from utils.dataframe_utils import safe_dataframe
from .base import BaseTab


class SourceTab(BaseTab):
    """Handles source data analysis and validation"""
    
    def __init__(self):
        super().__init__(
            tab_name="Source",
            description=""
        )
        # Use factory to create SonicReader for maximum performance with large directories
        self.source_reader = ReaderFactory.create_reader(ReaderType.SONIC_READER)
    
    def _get_friendly_asset_class_name(self, asset_class_name: str) -> str:
        """Get friendly name for asset class from AssetClass enum."""
        try:
            for asset_class in AssetClass:
                if asset_class.class_name == asset_class_name:
                    return asset_class.user_name
            return asset_class_name
        except Exception:
            return asset_class_name
    
    def _render_content(self):
        """Render source selection and analysis UI"""
        # Source Selection Section
        self._render_source_selection()
        st.markdown("---")
        
        # Analysis Button Section
        if st.button("🔍 Analyze Source", key="analyze_source", width='stretch', type='primary'):
            self._analyze_source()
        
        # Results Section
        if st.session_state.get('source_data'):
            st.markdown("---")
            self._render_results()
    
    def _render_source_selection(self):
        """Render source directory selection UI"""
        # Directory path input
        self.source_folder = st.text_input(
            "Enter dataset directory:",
            value="/Users/jyoti.ranjan/Downloads/assets/",
            help="Path to folder containing asset data",
            key="source_folder_input"
        )
        
        # Asset class selection
        self.asset_class = self._get_asset_class_selection()
        
        # Show validation for selected asset class
        self._show_asset_class_validation()
    
    def _get_asset_class_selection(self):
        """Get asset class selection from directory folders."""
        if not (self.source_folder and os.path.exists(self.source_folder)):
            st.info("👆 Please enter a valid directory path to see asset class options")
            return None
        
        try:
            # Get valid folder names (exclude system directories)
            system_dirs = [member.value for member in SystemDirectory]
            folder_names = [
                item for item in os.listdir(self.source_folder)
                if os.path.isdir(os.path.join(self.source_folder, item)) and item not in system_dirs
            ]
            
            if not folder_names:
                st.warning("⚠️ No subdirectories found in the selected path")
                return None
            
            # Create selectbox options with friendly names
            folder_names.sort()
            options = []
            folder_mapping = {}
            
            for folder_name in folder_names:
                friendly_name = self._get_friendly_asset_class_name(folder_name)
                display_name = f"{friendly_name} ({folder_name})" if friendly_name != folder_name else folder_name
                options.append(display_name)
                folder_mapping[display_name] = folder_name
            
            selected = st.selectbox(
                "Choose asset class:",
                options=options,
                help="Select the asset class folder to analyze",
                key="asset_class_select"
            )
            
            return folder_mapping[selected]
            
        except Exception as e:
            st.warning(f"⚠️ Error reading directory: {str(e)}")
            return None
    
    def _show_asset_class_validation(self):
        """Show validation status for selected asset class."""
        if not (self.source_folder and self.asset_class):
            return
        
        try:
            full_path = os.path.join(self.source_folder, self.asset_class)
            dir_info = self.source_reader.get_directory_info(full_path)
            
            if dir_info['valid']:
                st.success(f"✅ {dir_info['total_files']} JSON files found in {self.asset_class}")
            else:
                st.error(f"❌ {dir_info['error']}")
        except Exception as e:
            st.warning(f"⚠️ {str(e)}")
        
    
    def _analyze_source(self):
        """Analyze source directory and update workflow state"""
        if not self.source_folder:
            st.error("❌ Please enter a dataset directory")
            return
        
        if not self.asset_class:
            st.error("❌ Please select an asset class")
            return
        
        try:
            with st.spinner("🔍 Analyzing source data..."):
                full_path = os.path.join(self.source_folder, self.asset_class)
                source_data = self.source_reader.scan_source_directory(full_path)
                
                # Store results in session state
                st.session_state.source_data = source_data
                st.session_state.selected_asset_class = self.asset_class
                
                st.success(f"✅ Analysis complete for {self.asset_class}!")
                
        except Exception as e:
            st.error(f"❌ Error analyzing source: {str(e)}")
    
    def _render_results(self):
        """Render source analysis results"""
        source_data = st.session_state.get('source_data')
        selected_asset_class = st.session_state.get('selected_asset_class', 'Unknown')
        if not source_data:
            return
        
        st.markdown("### 📊 Summary")
        self._render_summary_metrics(source_data, selected_asset_class)
        st.markdown("---")
        self._render_file_details(source_data)
    
    def _render_summary_metrics(self, source_data, selected_asset_class):
        """Render summary metrics in a grid layout."""
        file_details = source_data.get('file_details', [])
        processed_files = len([f for f in file_details if f.get('Assets', 0) > 0])
        processed_assets = sum(f.get('Assets', 0) for f in file_details if f.get('Assets', 0) > 0)
        success_rate = (processed_files / source_data['total_files'] * 100) if source_data['total_files'] > 0 else 0
        
        # Row 1
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("📁 Total Files", source_data['total_files'])
        with col2:
            st.metric("🏢 Total Assets", f"{source_data['estimated_assets']:,}")
        with col3:
            st.metric("✅ Processed Files", processed_files)
        with col4:
            st.metric("❌ Errored Files", len(file_details) - processed_files)
        
        # Row 2
        col5, col6, col7, col8 = st.columns(4)
        with col5:
            st.metric("📊 Processed Assets", f"{processed_assets:,}")
        with col6:
            st.metric("📈 Success Rate", f"{success_rate:.1f}%")
        with col7:
            st.metric("🔍 Analysis Status", "Complete")
        with col8:
            st.metric("🏷️ Asset Classes", len(source_data.get('asset_classes', [])))
    
    def _render_file_details(self, source_data):
        """Render detailed file information table."""
        st.markdown("### 📋 File Details")
        
        if not source_data.get('file_details'):
            st.info("No file details available")
            return
        
        # Create table data
        table_data = [
            {
                'File Name': file_info['File'],
                'Asset Count': file_info['Assets'],
                'Asset Classes': file_info.get('Asset Classes', 'Unknown')
            }
            for file_info in source_data['file_details']
        ]
        
        df_files = safe_dataframe(table_data)
        st.dataframe(df_files, width='content')
