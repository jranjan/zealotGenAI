"""
Normaliser Tab - Handles data normalisation/flattening
"""

import streamlit as st
import os
import json
import multiprocessing
from pathlib import Path
import pandas as pd
import sys
from collections import defaultdict

# Add the current directory to Python path
current_dir = Path(__file__).parent.parent.parent
sys.path.insert(0, str(current_dir))

from transformer import TransformerFactory, TransformerType
from common.system_data import SystemDirectory
from utils.dataframe_utils import safe_dataframe
from .base import BaseTab


class NormaliserTab(BaseTab):
    """Handles data normalisation/flattening"""
    
    def __init__(self):
        super().__init__(
            tab_name="Normalise",
            description=""
        )
        # Use factory to create SupersonicFlattener for maximum performance (multiprocessing)
        self.transformer = TransformerFactory.create_transformer(
            TransformerType.SUPERSONIC_FLATTENER,
            max_workers=min(8, multiprocessing.cpu_count()),  # Optimized for multiprocessing
            chunk_size=20  # Smaller chunks for better memory management
        )
        # Initialize target folder (auto-generate by default)
        self.target_folder = None
    
    def _render_content(self):
        """Render normaliser UI"""
        # Source Info Section
        self._render_target_selection()
        st.markdown("---")
        
        # Check if source data is available
        source_data = st.session_state.get('source_data', {})
        source_folder = source_data.get('source_folder', '')
        
        # Transform Button Section
        if st.button("🔧 Normalise Data", key="normalise_data", width='stretch', type='primary'):
            if source_folder and Path(source_folder).exists():
                self._normalise_data()
            else:
                st.warning("⚠️ Please provide a valid source folder path")
        
        # Results Section
        if st.session_state.get('normalised_data'):
            st.markdown("---")
            self._render_results()
    
    def _render_target_selection(self):
        """Render target directory selection UI"""
        source_data = st.session_state.get('source_data', {})
        source_folder = source_data.get('source_folder', '')
        st.info(f"**Source:** `{source_folder if source_folder else 'Unknown'}`")
        
        # Check if selected folder exists
        if source_folder:
            if Path(source_folder).exists():
                st.success("✅ Selected folder exists")
            else:
                st.error("❌ Selected folder does not exist")
        else:
            st.warning("⚠️ No folder selected")
        
        # Show a placeholder path that will be generated dynamically
        project_root = Path(__file__).parent.parent.parent.parent.parent.parent.resolve()
        
        if source_folder and Path(source_folder).exists():
            source_path = Path(source_folder).resolve()
            # Extract asset class from source path
            asset_class = source_path.name
            # Show the new stage directory structure with timestamp
            placeholder_path = str(source_path.parent / SystemDirectory.get_stage_folder() / asset_class / f"{asset_class}_normalised_<timestamp>")
        else:
            placeholder_path = str(project_root / "output" / "flattened_<timestamp>")
        
        st.info(f"**Output will be created at:** `{placeholder_path}`")
        
        # Set target folder to None (auto-generate)
        self.target_folder = None

    def _normalise_data(self):
        """Normalise data and update session state"""
        source_data = st.session_state.get('source_data', {})
        source_folder = source_data.get('source_folder', '')
        
        # Generate fresh timestamp for this run
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y_%m_%d_%H%M%S")
        
        # Get the project root directory (independent of where script is run from)
        project_root = Path(__file__).parent.parent.parent.parent.parent.parent.resolve()
        
        # Use custom target folder if provided, otherwise generate timestamped path
        if self.target_folder and self.target_folder.strip():
            target_folder = self.target_folder.strip()
            # If relative path, make it relative to project root
            if not Path(target_folder).is_absolute():
                target_folder = str(project_root / target_folder)
        else:
            # Auto-generate path - create stage directory structure
            if source_folder and Path(source_folder).exists():
                source_path = Path(source_folder).resolve()
                # Extract asset class from source path
                asset_class = source_path.name
                # Create path: /Users/jyoti.ranjan/Downloads/assets/__stage/servers/servers_normalised_2024_10_10_194500
                # From: /Users/jyoti.ranjan/Downloads/assets/servers
                target_folder = str(source_path.parent / SystemDirectory.get_stage_folder() / asset_class / f"{asset_class}_normalised_{timestamp}")
            else:
                # Fallback to project root if no source folder
                target_folder = str(project_root / "output" / f"flattened_{timestamp}")
        
        # Update the target folder for display
        self.target_folder = target_folder
        
        # Prevent creating files in the current directory or assetinsight directory
        target_path = Path(target_folder).resolve()
        
        # Get the actual assetinsight directory (where this file is located)
        assetinsight_dir = Path(__file__).parent.parent.parent.resolve()
        
        # Check if target is the assetinsight directory or inside assetinsight
        if (target_path == assetinsight_dir or 
            target_path.is_relative_to(assetinsight_dir)):
            self._render_error_message("❌ Cannot create files in the assetinsight directory. Please specify a proper output directory.")
            return
        
        try:
            # Create a progress container for better feedback
            progress_container = st.container()
            with progress_container:
                st.info("🔧 Starting data normalisation...")
                progress_bar = st.progress(0)
                status_text = st.empty()
            
            with st.spinner("🔧 Normalising data..."):
                # Update status
                status_text.text("🔄 Processing files with optimized multiprocessing...")
                progress_bar.progress(0.1)
                
                result = self.transformer.transform_directory(source_folder, target_folder)
                
                # Update progress
                progress_bar.progress(0.9)
                status_text.text("✅ Normalisation complete!")
                progress_bar.progress(1.0)
                
                if 'error' in result:
                    self._render_error_message(f"Error: {result['error']}")
                else:
                    # Update progress
                    status_text.text("✅ Normalisation complete!")
                    progress_bar.progress(1.0)
                    
                    # Update session state with normalisation results only (no database)
                    st.session_state.update({
                        'normalised_data': {
                            'target_folder': target_folder,
                            'total_files': result['total_files'],
                            'successful': result['successful'],
                            'failed': result['failed'],
                            'files': result.get('files', []),
                            'database_ready': False  # Database will be created in Load tab
                        },
                        'normaliser_complete': True
                    })
                    
                    self._render_success_message("Data normalisation complete! Proceed to the Load tab to create the database.")
                    st.rerun()
                    
        except Exception as e:
            self._render_error_message(f"Error during normalisation: {str(e)}")
    
    def _render_results(self):
        """Render normalisation results"""
        normalised_data = st.session_state.get('normalised_data')
        if not normalised_data:
            return
        
        # Summary metrics section
        # Key metrics
        success_rate = (normalised_data['successful'] / normalised_data['total_files'] * 100) if normalised_data['total_files'] > 0 else 0
        
        # Calculate total assets from files (database not created in Transform tab)
        total_assets = sum(file_info.get('source_assets', 0) for file_info in normalised_data.get('files', []))
        
        metrics = {
            "📁 Files Processed": normalised_data['total_files'],
            "✅ Successful": normalised_data['successful'],
            "❌ Failed": normalised_data['failed'],
            "📊 Total Assets": f"{total_assets:,}",
            "📈 Success Rate": f"{success_rate:.1f}%"
        }
        self._render_metrics(metrics)
        
        st.markdown("---")
        
        # File details
        if normalised_data['files']:
            file_details = []
            for file_info in normalised_data['files']:
                file_details.append({
                    'Source File': Path(file_info['input']).name,
                    'Source Assets': file_info.get('source_assets', 0),
                    'Target File': Path(file_info['output']).name if file_info.get('output') else 'N/A',
                    'Normalised Assets': file_info.get('normalised_assets', 0),
                    'Missing Ownership': file_info.get('missing_attribution', 0),
                    'Missing Name': file_info.get('missing_name', 0),
                    'Missing Properties': file_info.get('missing_properties', 0)
                })
            
            self._render_dataframe(
                file_details,
                title="📋 File Processing Details"
            )
        
        st.markdown("---")
        
        # File and Asset Selector
        self._render_file_asset_selector()
    
    def _render_file_asset_selector(self):
        """Render file and asset selector with JSON content display"""
        normalised_data = st.session_state.get('normalised_data')
        if not normalised_data or not normalised_data.get('files'):
            return
        
        st.markdown("### 📁 Normalised Assets Explorer")
        
        # Get the output folder from the first processed file
        first_file = normalised_data['files'][0]
        output_folder = Path(first_file['output']).parent
        
        # Get source folder from session state
        source_data = st.session_state.get('source_data', {})
        source_folder = source_data.get('source_folder', '')
        
        # File selector
        output_files = list(Path(output_folder).glob("*.json"))
        if not output_files:
            st.warning("No flattened files found in output directory")
            return
        
        # Create file selection dropdown
        file_options = []
        file_mapping = {}
        
        for output_file in output_files:
            # Extract original filename from flattened filename
            if output_file.stem.startswith('flattened_'):
                original_name = output_file.stem.replace('flattened_', '')
            else:
                original_name = output_file.stem.replace('_flattened', '')
            
            display_name = f"{original_name} → {output_file.name}"
            file_options.append(display_name)
            file_mapping[display_name] = output_file.name
        
        selected_file_display = st.selectbox(
            "Select a flattened file to explore:",
            file_options,
            key="file_selector"
        )
        
        if selected_file_display:
            selected_file = file_mapping[selected_file_display]
            
            # Find corresponding source file
            source_file = None
            if selected_file.startswith('flattened_'):
                original_name = selected_file.replace('flattened_', '').replace('.json', '')
            else:
                original_name = selected_file.replace('_flattened.json', '')
            
            for f in Path(source_folder).glob("*.json"):
                if f.stem == original_name:
                    source_file = f
                    break
            
            if source_file:
                self._render_asset_selector(str(source_file), str(output_folder / selected_file))
            else:
                st.warning(f"Could not find corresponding source file for {selected_file}")
    
    def _render_asset_selector(self, source_file_path: str, flattened_file_path: str):
        """Render asset selector and JSON content display"""
        try:
            # Load source data
            with open(source_file_path, 'r', encoding='utf-8') as f:
                source_data = json.load(f)
            
            # Load flattened data
            with open(flattened_file_path, 'r', encoding='utf-8') as f:
                flattened_data = json.load(f)
            
            # Check if data is a list of assets
            if isinstance(source_data, list) and len(source_data) > 1:
                # Create asset selection dropdown (show asset ID and name)
                asset_options = []
                for i, asset in enumerate(source_data):
                    asset_id = asset.get('id', f'asset_{i+1}')
                    asset_name = asset.get('name', 'Unknown')
                    asset_class = asset.get('assetClass', 'Unknown')
                    display_name = f"{asset_id} - {asset_name} ({asset_class})"
                    asset_options.append(display_name)
                
                selected_asset_idx = st.selectbox(
                    "Choose an asset to view:",
                    range(len(source_data)),
                    format_func=lambda x: asset_options[x],
                    key="asset_selector"
                )
                
                if selected_asset_idx is not None:
                    self._render_asset_content(source_data[selected_asset_idx], flattened_data[selected_asset_idx])
            else:
                # Single asset or not a list
                asset_data = source_data[0] if isinstance(source_data, list) else source_data
                flattened_asset = flattened_data[0] if isinstance(flattened_data, list) else flattened_data
                self._render_asset_content(asset_data, flattened_asset)
                
        except Exception as e:
            st.error(f"Error loading file: {str(e)}")
    
    def _render_asset_content(self, source_asset: dict, flattened_asset: dict):
        """Render the complete JSON content of the selected asset"""
        st.markdown("---")
        
        # Create two columns for source and flattened data
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### 🔍 Original Asset")
            st.json(source_asset)
        
        with col2:
            st.markdown("#### 🔧 Flattened Asset")
            st.json(flattened_asset)