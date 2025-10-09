#!/usr/bin/env python3
"""
Simple Asset Insight Dashboard
A basic Streamlit app that uses AssetAnalyser and displays results
"""

import streamlit as st
import json
import os
from pathlib import Path
import sys
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# Add the current directory to Python path
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

from analyser.asset import AssetAnalyser
from config.yaml import AssetConfig

def create_cloud_distribution_chart(parent_clouds):
    """Create a bar chart showing host distribution per cloud."""
    if not parent_clouds:
        return None
    
    # Prepare data for chart
    cloud_data = []
    for cloud in parent_clouds:
        cloud_name = cloud.get('cloud', 'Unknown')
        total_assets = cloud.get('total_cloud_assets', 0)
        cloud_data.append({
            'Cloud': cloud_name,
            'Total Hosts': total_assets
        })
    
    # Create DataFrame
    df = pd.DataFrame(cloud_data)
    df = df.sort_values('Total Hosts', ascending=False)
    
    # Create bar chart
    fig = px.bar(
        df, 
        x='Cloud', 
        y='Total Hosts',
        title='Host Distribution by Parent Cloud',
        color='Total Hosts',
        color_continuous_scale='viridis'
    )
    
    fig.update_layout(
        xaxis_tickangle=-45,
        height=500,
        showlegend=False
    )
    
    return fig

def create_team_distribution_chart(parent_clouds, selected_cloud=None):
    """Create a bar chart showing host distribution per team."""
    if not parent_clouds:
        return None
    
    # Prepare data for chart
    team_data = []
    
    for cloud in parent_clouds:
        cloud_name = cloud.get('cloud', 'Unknown')
        
        # If a specific cloud is selected, only show that cloud's teams
        if selected_cloud and cloud_name != selected_cloud:
            continue
            
        if 'team' in cloud and cloud['team']:
            for team in cloud['team']:
                team_name = team.get('team_name', 'Unknown')
                team_assets = team.get('total_team_assets', 0)
                team_data.append({
                    'Cloud': cloud_name,
                    'Team': team_name,
                    'Hosts': team_assets
                })
    
    if not team_data:
        return None
    
    # Create DataFrame
    df = pd.DataFrame(team_data)
    df = df.sort_values('Hosts', ascending=False)
    
    # Create bar chart
    if selected_cloud:
        fig = px.bar(
            df, 
            x='Team', 
            y='Hosts',
            title=f'Host Distribution by Team in {selected_cloud}',
            color='Hosts',
            color_continuous_scale='plasma'
        )
    else:
        fig = px.bar(
            df, 
            x='Team', 
            y='Hosts',
            title='Host Distribution by Team (All Clouds)',
            color='Cloud',
            color_discrete_sequence=px.colors.qualitative.Set3
        )
    
    fig.update_layout(
        xaxis_tickangle=-45,
        height=500
    )
    
    return fig

def create_cloud_team_heatmap(parent_clouds):
    """Create a heatmap showing host distribution across clouds and teams."""
    if not parent_clouds:
        return None
    
    # Prepare data for heatmap
    heatmap_data = []
    
    for cloud in parent_clouds:
        cloud_name = cloud.get('cloud', 'Unknown')
        if 'team' in cloud and cloud['team']:
            for team in cloud['team']:
                team_name = team.get('team_name', 'Unknown')
                team_assets = team.get('total_team_assets', 0)
                heatmap_data.append({
                    'Cloud': cloud_name,
                    'Team': team_name,
                    'Hosts': team_assets
                })
    
    if not heatmap_data:
        return None
    
    # Create DataFrame
    df = pd.DataFrame(heatmap_data)
    
    # Handle duplicates by aggregating (sum hosts for same team-cloud combination)
    df_agg = df.groupby(['Team', 'Cloud'])['Hosts'].sum().reset_index()
    
    # Pivot for heatmap
    try:
        pivot_df = df_agg.pivot(index='Team', columns='Cloud', values='Hosts').fillna(0)
        
        # Create heatmap
        fig = px.imshow(
            pivot_df,
            title='Host Distribution Heatmap: Teams vs Clouds',
            color_continuous_scale='Blues',
            aspect='auto'
        )
        
        fig.update_layout(
            height=max(400, len(pivot_df) * 30),
            xaxis_title='Parent Cloud',
            yaxis_title='Team'
        )
        
        return fig
    except Exception as e:
        # If pivot still fails, create a simpler visualization
        st.warning(f"Could not create heatmap due to data structure: {str(e)}")
        return None

def main():
    st.set_page_config(
        page_title="Asset Insight - Simple Dashboard",
        page_icon="📊",
        layout="wide"
    )
    
    st.title("📊 Asset Insight Dashboard")
    st.markdown("---")
    
    # Configuration file input
    st.sidebar.header("Configuration")
    config_path = st.sidebar.text_input(
        "Configuration File Path", 
        value="asset.yaml",
        help="Path to your asset.yaml configuration file"
    )
    
    if st.sidebar.button("🔄 Run Analysis", type="primary"):
        if not os.path.exists(config_path):
            st.error(f"❌ Configuration file not found: {config_path}")
            return
        
        try:
            # Load configuration
            config = AssetConfig(config_path)
            st.sidebar.success("✅ Configuration loaded!")
            
            # Run analysis for each asset class
            with st.spinner("🔄 Running analysis..."):
                results = []
                
                for asset in config.get_assets():
                    st.write(f"📊 Analyzing {asset.name}...")
                    
                    # Create a fresh analyser for each asset class to ensure clean processing
                    fresh_analyser = AssetAnalyser()
                    
                    # Get field configurations
                    asset_fields = config.get_asset_fields(asset.name)
                    cloud_fields = config.get_cloud_fields(asset.name)
                    
                    # Run analysis
                    result = fresh_analyser.analyse_asset_class(
                        asset_class_name=asset.name,
                        source_path=asset.source_id,
                        result_path=asset.result_id,
                        asset_fields=asset_fields,
                        cloud_fields=cloud_fields
                    )
                    
                    results.append({
                        'name': asset.name,
                        'result': result
                    })
                    
                    st.success(f"✅ {asset.name} analysis complete!")
            
            # Store results in session state
            st.session_state.analysis_results = results
            st.session_state.config = config
            
            # Force a rerun to show the results
            st.rerun()
            
        except Exception as e:
            st.error(f"❌ Error during analysis: {str(e)}")
            return
            
    # Display results if available
    if 'analysis_results' in st.session_state and st.session_state.analysis_results:
        st.markdown("## 📈 Analysis Results")
        
        # Create tabs for different views
        tab1, tab2 = st.tabs(["🌐 Cloud Distribution", "👥 Team Distribution"])
        
        for result_data in st.session_state.analysis_results:
            result = result_data['result']
            
            with tab1:
                st.markdown(f"### 🌐 {result_data['name']} - Cloud Distribution")
                
                # Cloud distribution chart
                cloud_chart = create_cloud_distribution_chart(result.parent_clouds)
                if cloud_chart:
                    st.plotly_chart(cloud_chart, use_container_width=True)
                else:
                    st.write("No cloud data available for visualization")
                
                # Cloud details table
                if result.parent_clouds:
                    st.markdown("### 📋 Cloud Details")
                    cloud_data = []
                    for cloud in result.parent_clouds:
                        cloud_data.append({
                            'Cloud Name': cloud.get('cloud', 'Unknown'),
                            'Total Teams': cloud.get('total_cloud_team', 0),
                            'Total Hosts': cloud.get('total_cloud_assets', 0)
                        })
                    
                    df_clouds = pd.DataFrame(cloud_data)
                    st.dataframe(df_clouds, use_container_width=True)
            
            with tab2:
                st.markdown(f"### 👥 {result_data['name']} - Team Distribution")
                
                # Cloud selector for team view
                if result.parent_clouds:
                    cloud_names = [cloud.get('cloud', 'Unknown') for cloud in result.parent_clouds]
                    selected_cloud = st.selectbox(
                        "Select Parent Cloud to View Teams:",
                        ["All"] + cloud_names,
                        key=f"cloud_selector_{result_data['name']}"
                    )
                    
                    if selected_cloud == "All":
                        # Show teams from all clouds
                        team_data = []
                        for cloud in result.parent_clouds:
                            if 'team' in cloud and cloud['team']:
                                for team in cloud['team']:
                                    team_data.append({
                                        'Team Name': team.get('team_name', 'Unknown Team'),
                                        'Hosts': team.get('total_team_assets', 0),
                                        'Cloud': cloud.get('cloud', 'Unknown')
                                    })
                        
                        if team_data:
                            df_teams = pd.DataFrame(team_data)
                            
                            # Show summary stats at the top
                            col1, col2 = st.columns(2)
                            with col1:
                                st.metric("Total Teams", len(team_data))
                            with col2:
                                st.metric("Total Hosts", df_teams['Hosts'].sum())
                            
                            # Create vertical bar chart for all teams
                            fig = px.bar(
                                df_teams, 
                                x='Team Name', 
                                y='Hosts',
                                title=f"Host Distribution by Team (All Clouds)",
                                color='Cloud',
                                color_discrete_sequence=px.colors.qualitative.Set3,
                                orientation='v'
                            )
                            fig.update_layout(
                                xaxis_tickangle=-45,
                                height=500,
                                showlegend=True,
                                xaxis_title="Team Name",
                                yaxis_title="Host Count"
                            )
                            st.plotly_chart(fig, use_container_width=True)
                            
                            # Team details table
                            st.markdown("### 📋 Team Details")
                            st.dataframe(df_teams, use_container_width=True)
                        else:
                            st.write("No team data available")
                    else:
                        # Find the selected cloud data
                        selected_cloud_data = None
                        for cloud in result.parent_clouds:
                            if cloud.get('cloud', 'Unknown') == selected_cloud:
                                selected_cloud_data = cloud
                                break
                        
                        if selected_cloud_data and 'team' in selected_cloud_data and selected_cloud_data['team']:
                            # Debug: Show team structure
                            if st.checkbox("Show Detailed Info", key=f"debug_{result_data['name']}"):
                                st.json(selected_cloud_data['team'][0] if selected_cloud_data['team'] else {})
                            
                            # Create team bar chart for selected cloud
                            team_data = []
                            for team in selected_cloud_data['team']:
                                team_data.append({
                                    'Team Name': team.get('team_name', 'Unknown Team'),
                                    'Hosts': team.get('total_team_assets', 0)
                                })
                            
                            if team_data:
                                df_teams = pd.DataFrame(team_data)
                                
                                # Show summary stats at the top
                                col1, col2 = st.columns(2)
                                with col1:
                                    st.metric("Total Teams", len(team_data))
                                with col2:
                                    st.metric("Total Hosts", df_teams['Hosts'].sum())
                                
                                # Create vertical bar chart
                                fig = px.bar(
                                    df_teams, 
                                    x='Team Name', 
                                    y='Hosts',
                                    title=f"Host Distribution by Team in {selected_cloud}",
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
                                st.markdown("### 📋 Team Details")
                                st.dataframe(df_teams, use_container_width=True)
                            else:
                                st.write("No team data available for the selected cloud")
                        else:
                            st.write("No teams found in the selected cloud")
    
    else:
        st.info("👈 Please load a configuration file and run analysis to see results.")
        
        # Show configuration used if available
        if 'config' in st.session_state and st.session_state.config:
            st.markdown("### 📝 Configuration Used")
            
            # Read and display the actual configuration file
            config_path = st.session_state.config.config_path
            try:
                with open(config_path, 'r') as f:
                    config_content = f.read()
                st.code(config_content, language="yaml")
            except Exception as e:
                st.error(f"Could not read configuration file: {e}")
        else:
            # Show actual asset.yaml configuration from repository
            st.markdown("### 📝 Configuration File")
            try:
                # Try to read the asset.yaml file from the current directory
                asset_yaml_path = Path(__file__).parent / "asset.yaml"
                if asset_yaml_path.exists():
                    with open(asset_yaml_path, 'r') as f:
                        config_content = f.read()
                    st.code(config_content, language="yaml")
                else:
                    # Fallback to example if asset.yaml doesn't exist
                    st.markdown("### 📝 Example Configuration")
                    st.code("""
assets:
  - name: Server
    source_id: /path/to/your/servers
    result_id: /path/to/your/results
    asset_fields:
      - id
      - name
      - identifier
      - assetClass
    cloud_fields:
      - parentCloud
      - cloud
      - team
                    """, language="yaml")
            except Exception as e:
                st.error(f"Could not read configuration file: {e}")

if __name__ == "__main__":
    main()
