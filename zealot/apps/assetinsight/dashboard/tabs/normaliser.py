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
from .base import BaseTab


class NormaliserTab(BaseTab):
    """Handles data normalisation/flattening"""
    
    def __init__(self):
        super().__init__(
            tab_name="🔧 Normaliser",
            description=""
        )
        # Use factory to create SupersonicFlattener for maximum performance (multiprocessing)
        self.transformer = TransformerFactory.create_transformer(
            TransformerType.SUPERSONIC_FLATTENER,
            max_workers=min(8, multiprocessing.cpu_count()),  # Optimized for multiprocessing
            chunk_size=20  # Smaller chunks for better memory management
        )
    
    def _check_prerequisites(self, workflow_state):
        """Check if source stage is complete"""
        return self._get_workflow_state_value('source_complete', False)
    
    def _render_prerequisite_warning(self):
        """Render warning when source stage is not complete"""
        st.warning("⚠️ **Source stage not complete** - Please complete the Source tab first")
        st.markdown("Go to the **Source** tab to analyze your data directory")
    
    def _render_content(self, workflow_state):
        """Render normaliser UI"""
        self._render_target_selection(workflow_state)
        
        # Actions section below the input
        if self._render_action_button("🔧 Normalise Data"):
            self._normalise_data()
    
    def _render_target_selection(self, workflow_state):
        """Render target directory selection UI"""
        source_data = self._get_workflow_state_value('source_data', {})
        source_folder = source_data.get('source_folder', '')
        st.info(f"**Source:** `{source_folder if source_folder else 'Unknown'}`")
        
        # Show a placeholder path that will be generated dynamically
        project_root = Path(__file__).parent.parent.parent.parent.parent.parent.resolve()
        
        if source_folder and Path(source_folder).exists():
            source_path = Path(source_folder).resolve()
            placeholder_path = str(source_path.parent / f"{source_path.name}_flattened_<timestamp>")
        else:
            placeholder_path = str(project_root / "output" / "flattened_<timestamp>")
        
        st.info(f"**Output will be created at:** `{placeholder_path}`")
        
        # Target directory input (optional override)
        self.target_folder = st.text_input(
            "Override output directory path (optional):",
            value="",
            help="Leave empty to auto-generate timestamped directory, or specify custom path",
            key="target_folder_input"
        )
    
    
    def _render_flattened_keys_analysis(self):
        """Render file selector and key mapping analysis"""
        normalised_data = self._get_workflow_state_value('normalised_data')
        if not normalised_data or not normalised_data.get('files'):
            return
        
        # Get the output folder from the first processed file
        first_file = normalised_data['files'][0]
        output_folder = Path(first_file['output']).parent
        
        # Get source folder from workflow state
        source_data = self._get_workflow_state_value('source_data', {})
        source_folder = source_data.get('source_folder', '')
        
        st.markdown("### Key Mapping Analysis")
        
        # File selector
        output_files = list(Path(output_folder).glob("*.json"))
        if not output_files:
            st.warning("No flattened files found in output directory")
            return
        
        # Create file selection dropdown with original and flattened file names
        file_options = []
        file_mapping = {}
        
        for output_file in output_files:
            # Extract original filename from flattened filename
            # Handle both formats: "originalname_flattened.json" and "flattened_originalname.json"
            if output_file.stem.startswith('flattened_'):
                original_name = output_file.stem.replace('flattened_', '')
            else:
                original_name = output_file.stem.replace('_flattened', '')
            
            display_name = f"{original_name} → {output_file.name}"
            file_options.append(display_name)
            file_mapping[display_name] = output_file.name
        
        selected_file_display = st.selectbox(
            "Select a flattened file to analyze:",
            file_options,
            key="file_selector"
        )
        
        if selected_file_display:
            selected_file = file_mapping[selected_file_display]
        
        if selected_file:
            # Find corresponding source file
            source_file = None
            for f in Path(source_folder).glob("*.json"):
                if f.stem in selected_file:
                    source_file = f
                    break
            
            if source_file:
                self._render_asset_selector(str(source_file), str(output_folder / selected_file))
            else:
                st.warning(f"Could not find corresponding source file for {selected_file}")
    
    def _render_asset_selector(self, source_file_path: str, flattened_file_path: str):
        """Render asset selector and key mapping analysis"""
        try:
            # Load source data
            with open(source_file_path, 'r', encoding='utf-8') as f:
                source_data = json.load(f)
            
            # Load flattened data
            with open(flattened_file_path, 'r', encoding='utf-8') as f:
                flattened_data = json.load(f)
            
            # Check if data is a list of assets
            if isinstance(source_data, list) and len(source_data) > 1:
                st.markdown("#### Select Asset to Analyze")
                
                # Create asset selection dropdown
                asset_options = []
                for i, asset in enumerate(source_data):
                    asset_name = asset.get('name', f'Asset {i+1}')
                    asset_id = asset.get('id', f'asset_{i+1}')
                    asset_class = asset.get('assetClass', 'Unknown')
                    asset_options.append(f"{asset_name} ({asset_class}) - {asset_id}")
                
                selected_asset_idx = st.selectbox(
                    "Choose an asset:",
                    range(len(source_data)),
                    format_func=lambda x: asset_options[x],
                    key="asset_selector"
                )
                
                if selected_asset_idx is not None:
                    self._render_asset_key_mapping(source_file_path, flattened_file_path, selected_asset_idx)
            else:
                # Single asset or not a list
                asset_data = source_data[0] if isinstance(source_data, list) else source_data
                self._render_asset_key_mapping(source_file_path, flattened_file_path, 0)
                
        except Exception as e:
            st.error(f"Error loading file: {str(e)}")
    
    def _render_asset_key_mapping(self, source_file_path: str, flattened_file_path: str, asset_index: int = 0):
        """Render key mapping for a specific asset"""
        # Use the analyser to get all processing done
        result = self.transformer.get_asset_key_mappings(source_file_path, flattened_file_path, asset_index)
        
        if 'error' in result:
            st.error(f"Error analyzing asset: {result['error']}")
            return
        
        mappings = result['mappings']
        summary = result['summary']
        asset_info = result['asset_info']
        
        if mappings:
            asset_name = f"{asset_info['name']} ({asset_info['asset_class']}) - {asset_info['id']}"
            
            st.markdown("---")
            st.markdown(f"### 🔍 Key Mappings for {asset_name}")
            st.markdown("")
            
            # Add summary metrics as a two-column table
            summary_data = {
                'Metric': ['Total Transformations', 'Direct Values', 'Objects', 'Arrays', 'Missing Attribution', 'Missing Properties'],
                'Count': [summary['total_mappings'], summary['direct_values'], summary['objects'], summary['arrays'], 
                         mappings[0].get('is_missing_attribution', False) if mappings else False,
                         mappings[0].get('is_missing_properties', False) if mappings else False]
            }
            
            df_summary = pd.DataFrame(summary_data)
            st.dataframe(df_summary, use_container_width=False, hide_index=True)
            
            st.markdown("")
            
            # Create table data
            table_data = []
            for mapping in mappings:
                table_data.append({
                    'Original Key': mapping['original_key'],
                    'Flattened Key': mapping['flattened_key'],
                    'Value': mapping['value'],
                    'Type': mapping.get('type', 'value')
                })
            
            df = pd.DataFrame(table_data)
            
            # Configure column display
            st.dataframe(
                df, 
                use_container_width=True, 
                hide_index=True,
                column_config={
                    "Original Key": st.column_config.TextColumn(
                        "Original Key",
                        help="The original nested key from the source data",
                        width="large"
                    ),
                    "Flattened Key": st.column_config.TextColumn(
                        "Flattened Key", 
                        help="The flattened key using dot notation",
                        width="xlarge"
                    ),
                    "Value": st.column_config.TextColumn(
                        "Value",
                        help="The actual value stored in this key",
                        width="xlarge"
                    ),
                    "Type": st.column_config.TextColumn(
                        "Type",
                        help="Type of data: value, object, array, etc.",
                        width="medium"
                    )
                }
            )
        else:
            st.info("No key mappings found for this asset")
    
    def _normalise_data(self):
        """Normalise data and update workflow state"""
        source_data = self._get_workflow_state_value('source_data', {})
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
            # Auto-generate path - use source directory with _flattened_timestamp suffix
            if source_folder and Path(source_folder).exists():
                source_path = Path(source_folder).resolve()
                target_folder = str(source_path.parent / f"{source_path.name}_flattened_{timestamp}")
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
        
        # Note: No automatic cleanup - timestamped directories keep things organized
        
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
                    # Update workflow state
                    self._update_workflow_state({
                        'normalised_data': {
                            'target_folder': target_folder,
                            'total_files': result['total_files'],
                            'successful': result['successful'],
                            'failed': result['failed'],
                            'files': result.get('files', [])
                        },
                        'normaliser_complete': True
                    })
                    
                    self._render_success_message("Data normalisation complete!")
                    st.rerun()
                    
        except Exception as e:
            self._render_error_message(f"Error during normalisation: {str(e)}")
    
    def _render_results(self, workflow_state):
        """Render normalisation results"""
        normalised_data = self._get_workflow_state_value('normalised_data')
        if not normalised_data:
            return
        
        st.markdown("---")
        st.markdown("### 📊 Normalisation Results")
        
        # Key metrics
        success_rate = (normalised_data['successful'] / normalised_data['total_files'] * 100) if normalised_data['total_files'] > 0 else 0
        
        # Calculate total assets from all files
        total_assets = sum(file_info.get('source_assets', 0) for file_info in normalised_data.get('files', []))
        
        metrics = {
            "📁 Files Processed": normalised_data['total_files'],
            "✅ Successful": normalised_data['successful'],
            "❌ Failed": normalised_data['failed'],
            "📊 Total Assets": f"{total_assets:,}",
            "📈 Success Rate": f"{success_rate:.1f}%"
        }
        self._render_metrics(metrics)
        
        # File details
        if normalised_data['files']:
            file_details = []
            for file_info in normalised_data['files']:
                file_details.append({
                    'Source File': Path(file_info['input']).name,
                    'Source Assets': file_info.get('source_assets', 0),
                    'Target File': Path(file_info['output']).name if file_info.get('output') else 'N/A',
                    'Normalised Assets': file_info.get('normalised_assets', 0),
                    'Missing Attribution': file_info.get('missing_attribution', 0),
                    'Missing Properties': file_info.get('missing_properties', 0)
                })
            
            self._render_dataframe(
                file_details,
                title="📋 File Processing Details"
            )
        
        # Flattened Keys Analysis
        self._render_flattened_keys_analysis()
        
        # Progress indicator
        self._render_progress_indicator(
            "Normaliser",
            "Analyser tab to process your flattened data"
        )
    
    def is_complete(self, workflow_state):
        """Check if normaliser stage is complete"""
        return self._get_workflow_state_value('normaliser_complete', False)
