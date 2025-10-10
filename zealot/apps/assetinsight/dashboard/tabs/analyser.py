"""
Analyser Tab - Handles asset analysis using configuration
"""

import streamlit as st
import os
import sys
from pathlib import Path

# Add the current directory to Python path
current_dir = Path(__file__).parent.parent.parent
sys.path.insert(0, str(current_dir))

from analyser.owner import AssetOwnerAnalyser
from config.yaml import AssetConfig
from .base import BaseTab


class AnalyserTab(BaseTab):
    """Handles asset analysis using configuration"""
    
    def __init__(self):
        super().__init__(
            tab_name="🔍 Analyser",
            description="Analyze flattened data for insights using configuration"
        )
        self.config_path = "config/asset.yaml"  # Default configuration path
    
    def _check_prerequisites(self, workflow_state):
        """Check if normaliser stage is complete"""
        return self._get_workflow_state_value('normaliser_complete', False)
    
    def _render_prerequisite_warning(self):
        """Render warning when normaliser stage is not complete"""
        st.warning("⚠️ **Normaliser stage not complete** - Please complete the Normaliser tab first")
        st.markdown("Go to the **Normaliser** tab to flatten your data")
    
    def _render_content(self, workflow_state):
        """Render analyser UI"""
        col1, col2 = st.columns([3, 1])
        
        with col1:
            self._render_configuration_selection()
        
        with col2:
            if self._render_action_button("🔍 Run Analysis"):
                self._run_analysis()
    
    def _render_configuration_selection(self):
        """Render configuration selection UI"""
        st.markdown("**Configuration File**")
        
        # Configuration file selection
        config_paths = [
            "config/asset.yaml",
            "asset.yaml",
            "./config/asset.yaml"
        ]
        
        config_path = self._render_file_selection(
            label="Configuration File",
            common_paths=config_paths,
            session_key_prefix="config",
            help_text="Path to your asset.yaml configuration file",
            default_value="config/asset.yaml"
        )
        
        # Store config path for later use
        self.config_path = config_path
        
        # Show configuration preview
        self._render_configuration_preview(config_path)
    
    def _run_analysis(self):
        """Run analysis and update workflow state"""
        if not os.path.exists(self.config_path):
            self._render_error_message(f"Configuration file not found: {self.config_path}")
            return
        
        try:
            with st.spinner("🔍 Running analysis..."):
                # Load configuration
                config = AssetConfig(self.config_path)
                
                # Run analysis
                analyser = AssetOwnerAnalyser()
                results = []
                
                for asset_class in config.asset_classes:
                    result = analyser.analyse_asset_class(
                        asset_class=asset_class,
                        source_dir=asset_class.source_id,
                        result_dir=asset_class.result_id
                    )
                    results.append({
                        'name': asset_class.name,
                        'result': result
                    })
                
                # Update workflow state
                self._update_workflow_state({
                    'analysis_data': {
                        'config_path': self.config_path,
                        'results': results,
                        'config': config
                    },
                    'analyser_complete': True
                })
                
                self._render_success_message("Analysis complete!")
                st.rerun()
                
        except Exception as e:
            self._render_error_message(f"Error during analysis: {str(e)}")
    
    def _render_results(self, workflow_state):
        """Render analysis results"""
        analysis_data = self._get_workflow_state_value('analysis_data')
        if not analysis_data:
            return
        
        st.markdown("---")
        st.markdown("### 📊 Analysis Results")
        
        results = analysis_data.get('results', [])
        
        if not results:
            self._render_warning_message("No analysis results available")
            return
        
        # Calculate summary metrics
        total_clouds = 0
        total_assets = 0
        
        for result_data in results:
            result = result_data['result']
            if hasattr(result, 'parent_clouds') and result.parent_clouds:
                total_clouds += len(result.parent_clouds)
                for cloud in result.parent_clouds:
                    total_assets += cloud.get('total_cloud_assets', 0)
        
        # Summary metrics
        metrics = {
            "📊 Asset Classes": len(results),
            "🌐 Total Clouds": total_clouds,
            "📈 Total Assets": f"{total_assets:,}"
        }
        self._render_metrics(metrics)
        
        # Results by asset class
        st.markdown("### 📋 Results by Asset Class")
        for result_data in results:
            result = result_data['result']
            st.markdown(f"#### {result_data['name']}")
            
            if hasattr(result, 'parent_clouds') and result.parent_clouds:
                st.write(f"**Clouds:** {len(result.parent_clouds)}")
                st.write(f"**Assets:** {sum(cloud.get('total_cloud_assets', 0) for cloud in result.parent_clouds):,}")
            else:
                st.write("No data available")
        
        # Progress indicator
        self._render_progress_indicator(
            "Analyser",
            "Human Intelligence tab to visualize your results"
        )
    
    def is_complete(self, workflow_state):
        """Check if analyser stage is complete"""
        return self._get_workflow_state_value('analyser_complete', False)
