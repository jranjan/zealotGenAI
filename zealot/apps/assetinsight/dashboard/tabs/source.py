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

from scanner import SourceScanner
from .base import BaseTab


class SourceTab(BaseTab):
    """Handles source data analysis and validation"""
    
    def __init__(self):
        super().__init__(
            tab_name="📁 Source",
            description=""
        )
        self.source_scanner = SourceScanner()
    
    def _check_prerequisites(self, workflow_state):
        """Source tab has no prerequisites"""
        return True
    
    def _render_content(self, workflow_state):
        """Render source selection and analysis UI"""
        self._render_source_selection()
        
        # Actions section below the input
        if self._render_action_button("🔍 Analyze Source"):
            self._analyze_source()
    
    def _render_source_selection(self):
        """Render source directory selection UI"""
        # Directory path input
        self.source_folder = st.text_input(
            "Enter directory path:",
            value="/path/to/source",
            help="Path to folder containing JSON files",
            key="source_folder_input"
        )
        
        # Show validation
        if self.source_folder and self.source_folder != "/path/to/source":
            try:
                dir_info = self.source_scanner.get_directory_info(self.source_folder)
                if dir_info['valid']:
                    st.success(f"✅ {dir_info['total_files']} JSON files found")
                else:
                    st.error(f"❌ {dir_info['error']}")
            except Exception as e:
                st.warning(f"⚠️ {str(e)}")
        
    
    def _analyze_source(self):
        """Analyze source directory and update workflow state"""
        try:
            with st.spinner("🔍 Analyzing source data..."):
                # Use SourceAnalyser to analyze the directory
                source_data = self.source_scanner.scan_source_directory(self.source_folder)
                
                # Update workflow state
                self._update_workflow_state({
                    'source_data': source_data,
                    'source_complete': True
                })
                
                self._render_success_message("Source analysis complete!")
                st.rerun()
                
        except Exception as e:
            self._render_error_message(f"Error analyzing source: {str(e)}")
    
    def _render_results(self, workflow_state):
        """Render source analysis results"""
        source_data = self._get_workflow_state_value('source_data')
        if not source_data:
            return
        
        st.markdown("---")
        st.markdown("### Source Analysis Results")
        
        # Summary table with all metrics
        st.markdown("### Summary")
        
        asset_classes = source_data.get('asset_classes', [])
        # Format asset classes as "count (class1, class2)"
        asset_class_display = "Unknown"
        if asset_classes:
            asset_class_display = f"{len(asset_classes)} ({', '.join(asset_classes)})"
        
        summary_data = {
            'Metric': ['Total Files', 'Total Assets', 'Asset Classes'],
            'Value': [
                source_data['total_files'],
                f"{source_data['estimated_assets']:,}",
                asset_class_display
            ]
        }
        
        df_summary = pd.DataFrame(summary_data)
        st.dataframe(df_summary, use_container_width=False, hide_index=True)
        
        # File details table - File Name, Asset Count, and Asset Classes
        if source_data['file_details']:
            st.markdown("### File Analysis")
            
            # Create table with file name, asset count, and asset classes
            table_data = []
            for file_info in source_data['file_details']:
                table_data.append({
                    'File Name': file_info['File'],
                    'Asset Count': file_info['Assets'],
                    'Asset Classes': file_info.get('Asset Classes', 'Unknown')
                })
            
            df_files = pd.DataFrame(table_data)
            st.dataframe(df_files, use_container_width=False)
        
        # Progress indicator
        self._render_progress_indicator(
            "Source",
            "Normaliser tab to flatten your data"
        )
    
    def is_complete(self, workflow_state):
        """Check if source stage is complete"""
        return self._get_workflow_state_value('source_complete', False)
