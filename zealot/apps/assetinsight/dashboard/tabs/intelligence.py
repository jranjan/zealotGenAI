"""
Intelligence Tab - Handles results visualization
"""

import streamlit as st
import pandas as pd
import plotly.express as px
from .base import BaseTab


class IntelligenceTab(BaseTab):
    """Handles results visualization and human intelligence"""
    
    def __init__(self):
        super().__init__(
            tab_name="🧠 Human Intelligence",
            description="Visualize and interpret analysis results"
        )
    
    def _check_prerequisites(self, workflow_state):
        """Check if analyser stage is complete"""
        return self._get_workflow_state_value('analyser_complete', False)
    
    def _render_prerequisite_warning(self):
        """Render warning when analyser stage is not complete"""
        st.warning("⚠️ **Analyser stage not complete** - Please complete the Analyser tab first")
        st.markdown("Go to the **Analyser** tab to process your data")
    
    def _render_content(self, workflow_state):
        """Render intelligence UI"""
        analysis_data = self._get_workflow_state_value('analysis_data', {})
        
        if not analysis_data:
            self._render_error_message("No analysis data available")
            return
        
        # Render visualization options
        self._render_visualization_options()
        
        # Render results
        self._render_analysis_visualizations(analysis_data)
    
    def _render_visualization_options(self):
        """Render visualization control options"""
        st.markdown("### 🎛️ Visualization Options")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            self.show_cloud_distribution = st.checkbox(
                "🌐 Cloud Distribution", 
                value=True,
                help="Show host distribution by parent cloud"
            )
        
        with col2:
            self.show_team_distribution = st.checkbox(
                "👥 Team Distribution", 
                value=True,
                help="Show host distribution by team"
            )
        
        with col3:
            self.show_detailed_info = st.checkbox(
                "📋 Detailed Info", 
                value=False,
                help="Show detailed tables and information"
            )
    
    def _render_analysis_visualizations(self, analysis_data):
        """Render analysis visualizations"""
        results = analysis_data.get('results', [])
        
        if not results:
            self._render_warning_message("No analysis results available")
            return
        
        # Overall summary
        self._render_overall_summary(results)
        
        # Individual asset class results
        for result_data in results:
            self._render_asset_class_results(result_data)
    
    def _render_overall_summary(self, results):
        """Render overall summary across all asset classes"""
        st.markdown("---")
        st.markdown("### 📊 Overall Summary")
        
        # Calculate totals
        total_clouds = 0
        total_assets = 0
        total_teams = 0
        
        for result_data in results:
            result = result_data['result']
            if hasattr(result, 'parent_clouds') and result.parent_clouds:
                total_clouds += len(result.parent_clouds)
                for cloud in result.parent_clouds:
                    total_assets += cloud.get('total_cloud_assets', 0)
                    if 'team' in cloud and cloud['team']:
                        total_teams += len(cloud['team'])
        
        metrics = {
            "📊 Asset Classes": len(results),
            "🌐 Total Clouds": total_clouds,
            "👥 Total Teams": total_teams,
            "📈 Total Assets": f"{total_assets:,}"
        }
        self._render_metrics(metrics)
    
    def _render_asset_class_results(self, result_data):
        """Render results for a specific asset class"""
        result = result_data['result']
        asset_class_name = result_data['name']
        
        st.markdown(f"---")
        st.markdown(f"### 📊 {asset_class_name} Analysis")
        
        if not hasattr(result, 'parent_clouds') or not result.parent_clouds:
            self._render_warning_message(f"No data available for {asset_class_name}")
            return
        
        # Create tabs for different views
        tab1, tab2 = st.tabs(["🌐 Cloud Distribution", "👥 Team Distribution"])
        
        with tab1:
            self._render_cloud_distribution(result, asset_class_name)
        
        with tab2:
            self._render_team_distribution(result, asset_class_name)
    
    def _render_cloud_distribution(self, result, asset_class_name):
        """Render cloud distribution visualization"""
        if not self.show_cloud_distribution:
            self._render_info_message("Cloud distribution visualization is disabled")
            return
        
        # Create cloud distribution chart
        cloud_data = []
        for cloud in result.parent_clouds:
            cloud_data.append({
                'Cloud': cloud.get('cloud', 'Unknown'),
                'Total Hosts': cloud.get('total_cloud_assets', 0)
            })
        
        if not cloud_data:
            self._render_warning_message("No cloud data available")
            return
        
        df_clouds = pd.DataFrame(cloud_data)
        df_clouds = df_clouds.sort_values('Total Hosts', ascending=False)
        
        # Create bar chart
        fig = px.bar(
            df_clouds,
            x='Cloud',
            y='Total Hosts',
            title=f'{asset_class_name} - Host Distribution by Parent Cloud',
            color='Total Hosts',
            color_continuous_scale='viridis'
        )
        fig.update_layout(
            xaxis_tickangle=-45,
            height=500,
            showlegend=False
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Cloud details table
        if self.show_detailed_info:
            self._render_dataframe(df_clouds, title="📋 Cloud Details")
    
    def _render_team_distribution(self, result, asset_class_name):
        """Render team distribution visualization"""
        if not self.show_team_distribution:
            self._render_info_message("Team distribution visualization is disabled")
            return
        
        # Collect all team data
        team_data = []
        for cloud in result.parent_clouds:
            if 'team' in cloud and cloud['team']:
                for team in cloud['team']:
                    team_data.append({
                        'Team Name': team.get('team_name', 'Unknown Team'),
                        'Hosts': team.get('total_team_assets', 0),
                        'Cloud': cloud.get('cloud', 'Unknown')
                    })
        
        if not team_data:
            self._render_warning_message("No team data available")
            return
        
        # Clean the data
        cleaned_team_data = []
        for team in team_data:
            cleaned_team = {
                'Team Name': str(team.get('Team Name', 'Unknown Team')).strip(),
                'Hosts': team.get('Hosts', 0),
                'Cloud': str(team.get('Cloud', 'Unknown')).strip()
            }
            if cleaned_team['Team Name'] and cleaned_team['Team Name'] != 'Unknown Team':
                cleaned_team_data.append(cleaned_team)
        
        if not cleaned_team_data:
            self._render_warning_message("No valid team data available")
            return
        
        df_teams = pd.DataFrame(cleaned_team_data)
        df_teams['Hosts'] = pd.to_numeric(df_teams['Hosts'], errors='coerce').fillna(0).astype(int)
        
        # Show summary stats
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Total Teams", len(cleaned_team_data))
        with col2:
            st.metric("Total Hosts", df_teams['Hosts'].sum())
        
        # Create bar chart
        fig = px.bar(
            df_teams,
            x='Team Name',
            y='Hosts',
            title=f'{asset_class_name} - Host Distribution by Team',
            color='Hosts',
            color_continuous_scale='viridis',
            orientation='v'
        )
        fig.update_layout(
            xaxis_tickangle=-45,
            height=500,
            showlegend=False,
            xaxis_title="Team Name",
            yaxis_title="Host Count"
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Team details table
        if self.show_detailed_info:
            self._render_dataframe(df_teams, title="📋 Team Details")
    
    def is_complete(self, workflow_state):
        """Check if intelligence stage is complete"""
        return self._get_workflow_state_value('analyser_complete', False)
