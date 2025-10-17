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

from scanner import ScannerFactory
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
        # Use factory to create SonicScanner for maximum performance with large directories
        self.source_scanner = ScannerFactory.create_sonic_scanner()
    
    def _get_friendly_asset_class_name(self, asset_class_name: str) -> str:
        """
        Get friendly name for asset class from AssetClass enum.
        
        Args:
            asset_class_name: Raw asset class name (e.g., 'aws_ec2_instance')
            
        Returns:
            Friendly name (e.g., 'AWS EC2 Instance') or original name if not found
        """
        try:
            # Try to find matching AssetClass by class_name
            for asset_class in AssetClass:
                if asset_class.class_name == asset_class_name:
                    return asset_class.user_name
            # If not found, return original name
            return asset_class_name
        except Exception:
            # If any error, return original name
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
        
        # Asset class selection (folder names from directory)
        self.asset_class = None
        if self.source_folder and os.path.exists(self.source_folder):
            try:
                # Get folder names from the directory (exclude system directories)
                folder_names = []
                # Get all system directory names from SystemDirectory enum
                system_dirs = [member.value for member in SystemDirectory]
                
                for item in os.listdir(self.source_folder):
                    item_path = os.path.join(self.source_folder, item)
                    if os.path.isdir(item_path) and item not in system_dirs:
                        folder_names.append(item)
                
                if folder_names:
                    # Sort folder names for better UX
                    folder_names.sort()
                    
                    # Create options with friendly names for display
                    options_with_friendly_names = []
                    for folder_name in folder_names:
                        friendly_name = self._get_friendly_asset_class_name(folder_name)
                        if friendly_name != folder_name:
                            options_with_friendly_names.append(f"{friendly_name} ({folder_name})")
                        else:
                            options_with_friendly_names.append(folder_name)
                    
                    # Create mapping for selection
                    folder_to_friendly = dict(zip(options_with_friendly_names, folder_names))
                    
                    selected_friendly = st.selectbox(
                        "Choose asset class:",
                        options=options_with_friendly_names,
                        help="Select the asset class folder to analyze",
                        key="asset_class_select"
                    )
                    
                    # Get the actual folder name from selection
                    self.asset_class = folder_to_friendly[selected_friendly]
                else:
                    st.warning("⚠️ No subdirectories found in the selected path")
            except Exception as e:
                st.warning(f"⚠️ Error reading directory: {str(e)}")
        else:
            st.info("👆 Please enter a valid directory path to see asset class options")
        
        # Show validation for the selected asset class
        if self.source_folder and self.asset_class:
            try:
                full_path = os.path.join(self.source_folder, self.asset_class)
                dir_info = self.source_scanner.get_directory_info(full_path)
                if dir_info['valid']:
                    st.success(f"✅ {dir_info['total_files']} JSON files found in {self.asset_class}")
                else:
                    st.error(f"❌ {dir_info['error']}")
            except Exception as e:
                st.warning(f"⚠️ {str(e)}")
        
    
    def _analyze_source(self):
        """Analyze source directory and update workflow state"""
        # Validate inputs
        if not self.source_folder:
            st.error("❌ Please enter a dataset directory")
            return
        
        if not self.asset_class:
            st.error("❌ Please select an asset class")
            return
        
        try:
            # Create a progress container for better feedback
            progress_container = st.container()
            with progress_container:
                st.info(f"🔍 Starting source analysis for {self.asset_class}...")
                progress_bar = st.progress(0)
                status_text = st.empty()
            
            with st.spinner("🔍 Analyzing source data..."):
                # Update status
                status_text.text("🔄 Scanning directory for JSON files...")
                progress_bar.progress(0.2)
                
                # Use the selected asset class folder
                full_path = os.path.join(self.source_folder, self.asset_class)
                
                # Use SourceAnalyser to analyze the directory
                source_data = self.source_scanner.scan_source_directory(full_path)
                
                # Update progress
                progress_bar.progress(0.8)
                status_text.text("📊 Processing file details and asset counts...")
                progress_bar.progress(0.9)
                
                # Store in session state for display
                st.session_state.source_data = source_data
                st.session_state.selected_asset_class = self.asset_class
                
                # Complete progress
                status_text.text("✅ Source analysis complete!")
                progress_bar.progress(1.0)
                
        except Exception as e:
            st.error(f"❌ Error analyzing source: {str(e)}")
    
    def _render_results(self):
        """Render source analysis results"""
        source_data = st.session_state.get('source_data')
        selected_asset_class = st.session_state.get('selected_asset_class', 'Unknown')
        if not source_data:
            return
        
        # Summary stats section
        st.markdown("### 📊 Summary")
        
        # Show selected asset class with friendly name
        friendly_asset_class = self._get_friendly_asset_class_name(selected_asset_class)
        
        asset_classes = source_data.get('asset_classes', [])
        # Format asset classes with friendly names
        asset_class_display = "Unknown"
        if asset_classes:
            friendly_classes = [self._get_friendly_asset_class_name(ac) for ac in asset_classes]
            asset_class_display = f"{len(asset_classes)} ({', '.join(friendly_classes)})"
        
        # Calculate processed and errored files
        file_details = source_data.get('file_details', [])
        processed_files = len([f for f in file_details if f.get('Assets', 0) > 0])
        errored_files = len([f for f in file_details if f.get('Assets', 0) == 0])
        
        # Calculate processed and errored assets
        processed_assets = sum(f.get('Assets', 0) for f in file_details if f.get('Assets', 0) > 0)
        errored_assets = source_data['estimated_assets'] - processed_assets
        
        # Display stats as metrics - Two rows (4 columns each)
        # Row 1
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                label="📁 Total Files",
                value=source_data['total_files']
            )
        
        with col2:
            st.metric(
                label="🏢 Total Assets",
                value=f"{source_data['estimated_assets']:,}"
            )
        
        with col3:
            st.metric(
                label="🏷️ Asset Classes",
                value=asset_class_display
            )
        
        with col4:
            st.metric(
                label="✅ Processed Files",
                value=processed_files
            )
        
        # Row 2
        col5, col6, col7, col8 = st.columns(4)
        
        with col5:
            st.metric(
                label="❌ Errored Files",
                value=errored_files
            )
        
        with col6:
            st.metric(
                label="📊 Processed Assets",
                value=f"{processed_assets:,}"
            )
        
        with col7:
            st.metric(
                label="📈 Success Rate",
                value=f"{(processed_files / source_data['total_files'] * 100):.1f}%" if source_data['total_files'] > 0 else "0%"
            )
        
        with col8:
            st.metric(
                label="🔍 Analysis Status",
                value="Complete"
            )
        
        st.markdown("---")
        
        # Detailed files section
        st.markdown("### 📋 File Details")
        
        if source_data['file_details']:
            # Create table with file name, asset count, and asset classes
            table_data = []
            for file_info in source_data['file_details']:
                table_data.append({
                    'File Name': file_info['File'],
                    'Asset Count': file_info['Assets'],
                    'Asset Classes': file_info.get('Asset Classes', 'Unknown')
                })
            
            df_files = safe_dataframe(table_data)
            
            st.dataframe(
                df_files, 
                width='content',
                column_config={
                    col: st.column_config.TextColumn(
                        col,
                        help=f"Shows {col.lower()} information"
                    )
                    for col in df_files.columns
                }
            )
        else:
            st.info("No file details available")
