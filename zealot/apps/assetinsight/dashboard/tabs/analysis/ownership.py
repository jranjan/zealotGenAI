"""
Ownership Tab - Handles asset ownership analysis
"""

import streamlit as st
from typing import Dict, Any
from ..base import BaseTab
import sys
from pathlib import Path
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
sys.path.append(str(Path(__file__).parent.parent.parent.parent))
from analyser.asset.owner import OwnerAnalyser
from utils.dataframe_utils import safe_dataframe, clean_numeric_column, clean_string_column


class OwnershipAnalyserTab(BaseTab):
    """Handles asset ownership analysis"""
    
    def __init__(self):
        super().__init__(
            tab_name="Ownership",
            description=""
        )
        from analyser import AnalyserFactory, AnalyserType
        self.analyser = AnalyserFactory.create_analyser(AnalyserType.OWNER)
    
    def _render_content(self):
        """Render ownership analysis UI as standalone tab"""
        target_folder = self._get_target_folder()
        
        if target_folder:
            self._display_database_status(target_folder)
            st.markdown("---")
            self._render_analysis_tools(target_folder)
    
    def _get_target_folder(self):
        """Get target folder from session state or user input."""
        target_folder = st.session_state.get('database_path')
        
        if not target_folder:
            target_folder = st.text_input(
                "Enter path to normalised data directory:",
                placeholder="/path/to/normalised/data",
                key="ownership_data_path"
            )
        
        return target_folder
    
    def _render_analysis_tools(self, target_folder):
        """Render analysis tools with left-right layout."""
        # Create left and right columns (15% for buttons, 85% for results)
        left_col, right_col = st.columns([0.15, 0.85])
        
        # Left pane: Asset class selector and analysis buttons
        with left_col:
            # Asset class selector
            st.markdown("#### Asset Class Filter")
            from common.asset_class import AssetClass
            
            # Get available asset classes
            asset_classes = [ac.class_name for ac in AssetClass]
            
            selected_asset_class = st.selectbox(
                "Choose Asset Class:",
                options=asset_classes,
                index=0,
                key="ownership_asset_class_selector",
                help="Select a specific asset class to analyze"
            )
            
            # Store selected asset class in session state
            st.session_state['selected_asset_class'] = selected_asset_class
            
            st.markdown("---")
            
            # All analysis buttons in single column
            if st.button("Ownership Summary", type="primary", width='stretch'):
                st.session_state['run_ownership_summary'] = True
                st.rerun()
            
            if st.button("Parent Cloud Analysis", type="primary", width='stretch'):
                st.session_state['run_parent_cloud_analysis'] = True
                st.rerun()
            
            if st.button("Cloud Analysis", type="primary", width='stretch'):
                st.session_state['run_cloud_analysis'] = True
                st.rerun()
            
            if st.button("Team Analysis", type="primary", width='stretch'):
                st.session_state['run_team_analysis'] = True
                st.rerun()
            
            if st.button("MBU Analysis", type="primary", width='stretch'):
                st.session_state['run_mbu_analysis'] = True
                st.rerun()
            
            if st.button("BU Analysis", type="primary", width='stretch'):
                st.session_state['run_bu_analysis'] = True
                st.rerun()
        
        # Right pane: Analysis results in tabs
        with right_col:
            st.markdown("### Analysis Results")
            
            # Initialize analysis results in session state
            if 'analysis_results' not in st.session_state:
                st.session_state.analysis_results = {}
            
            # Handle specific analysis button clicks and store results
            if st.session_state.get('run_ownership_summary', False):
                st.session_state['run_ownership_summary'] = False
                if target_folder and st.session_state.get('database_ready', False):
                    selected_asset_class = st.session_state.get('selected_asset_class')
                    result = self._get_ownership_summary_result(target_folder, selected_asset_class)
                    st.session_state.analysis_results['ownership_summary'] = result
                elif not target_folder:
                    st.warning("⚠️ Please provide a data directory path")
                else:
                    st.warning("⚠️ Database not loaded. Please complete the Load tab first.")
            
            if st.session_state.get('run_parent_cloud_analysis', False):
                st.session_state['run_parent_cloud_analysis'] = False
                if target_folder and st.session_state.get('database_ready', False):
                    selected_asset_class = st.session_state.get('selected_asset_class')
                    result = self._get_parent_cloud_analysis_result(target_folder, selected_asset_class)
                    st.session_state.analysis_results['parent_cloud_analysis'] = result
                elif not target_folder:
                    st.warning("⚠️ Please provide a data directory path")
                else:
                    st.warning("⚠️ Database not loaded. Please complete the Load tab first.")
            
            if st.session_state.get('run_cloud_analysis', False):
                st.session_state['run_cloud_analysis'] = False
                if target_folder and st.session_state.get('database_ready', False):
                    selected_asset_class = st.session_state.get('selected_asset_class')
                    result = self._get_cloud_analysis_result(target_folder, selected_asset_class)
                    st.session_state.analysis_results['cloud_analysis'] = result
                elif not target_folder:
                    st.warning("⚠️ Please provide a data directory path")
                else:
                    st.warning("⚠️ Database not loaded. Please complete the Load tab first.")
            
            if st.session_state.get('run_team_analysis', False):
                st.session_state['run_team_analysis'] = False
                if target_folder and st.session_state.get('database_ready', False):
                    selected_asset_class = st.session_state.get('selected_asset_class')
                    result = self._get_team_analysis_result(target_folder, selected_asset_class)
                    st.session_state.analysis_results['team_analysis'] = result
                elif not target_folder:
                    st.warning("⚠️ Please provide a data directory path")
                else:
                    st.warning("⚠️ Database not loaded. Please complete the Load tab first.")
            
            if st.session_state.get('run_mbu_analysis', False):
                st.session_state['run_mbu_analysis'] = False
                if target_folder and st.session_state.get('database_ready', False):
                    selected_asset_class = st.session_state.get('selected_asset_class')
                    result = self._get_mbu_analysis_result(target_folder, selected_asset_class)
                    st.session_state.analysis_results['mbu_analysis'] = result
                elif not target_folder:
                    st.warning("⚠️ Please provide a data directory path")
                else:
                    st.warning("⚠️ Database not loaded. Please complete the Load tab first.")
            
            if st.session_state.get('run_bu_analysis', False):
                st.session_state['run_bu_analysis'] = False
                if target_folder and st.session_state.get('database_ready', False):
                    selected_asset_class = st.session_state.get('selected_asset_class')
                    result = self._get_bu_analysis_result(target_folder, selected_asset_class)
                    st.session_state.analysis_results['bu_analysis'] = result
                elif not target_folder:
                    st.warning("⚠️ Please provide a data directory path")
                else:
                    st.warning("⚠️ Database not loaded. Please complete the Load tab first.")
            
            # Create tabs for each analysis result
            if st.session_state.analysis_results:
                # Get available analysis results
                available_analyses = list(st.session_state.analysis_results.keys())
                
                # Create tab names
                tab_names = []
                for analysis in available_analyses:
                    if analysis == 'ownership_summary':
                        tab_names.append("Summary")
                    elif analysis == 'parent_cloud_analysis':
                        tab_names.append("Parent Cloud")
                    elif analysis == 'cloud_analysis':
                        tab_names.append("Cloud")
                    elif analysis == 'team_analysis':
                        tab_names.append("Team")
                    elif analysis == 'mbu_analysis':
                        tab_names.append("MBU")
                    elif analysis == 'bu_analysis':
                        tab_names.append("BU")
                
                # Create tabs
                if tab_names:
                    tabs = st.tabs(tab_names)
                    
                    for i, (analysis, tab) in enumerate(zip(available_analyses, tabs)):
                        with tab:
                            self._display_analysis_result(analysis, st.session_state.analysis_results[analysis])
            else:
                st.info("👆 Click any analysis button to see results here")
        
        
        # Details Section - Analysis Results
        if st.session_state.get('ownership_complete', False):
            st.markdown("---")
            # Show the analysis results
            summary = st.session_state.get('ownership_summary', {})
            self._display_ownership_summary_table(summary)
            
            # Show all ownership distribution charts only if analyser is initialized
            if st.session_state.get('ownership_analyser_initialized', False):
                self._display_all_distribution_charts()
    
    
    def _handle_analysis_click(self, target_folder):
        """Handle ownership analysis button click"""
        try:
            with st.spinner("🔍 Running ownership analysis..."):
                # Check if database is already ready from normalisation
                normalised_data = st.session_state.get('normalised_data', {})
                if normalised_data.get('database_ready', False):
                    st.info("🗄️ Using pre-created database from normalisation phase...")
                
                # Create reader with the target folder
                self.analyser.create_reader(target_folder)
                
                # Get ownership summary
                summary = self.analyser.get_ownership_summary()
                
                if summary['total_assets'] == 0:
                    st.warning("⚠️ No data found in the database. Please check the Transform tab.")
                else:
                    st.success("✅ Ownership analysis completed!")
                    # Store results in session state
                    st.session_state.update({
                        'ownership_summary': summary,
                        'ownership_complete': True,
                        'ownership_analyser_initialized': True
                    })
                    self._display_ownership_summary_table(summary)
                    
        except Exception as e:
            st.error(f"❌ Analysis failed: {str(e)}")
        # Note: Don't close reader here as we need it for charts
    
    def _display_database_status(self, target_folder: str):
        """Display database status information before analysis"""
        # Check if database is ready from Load tab
        if st.session_state.get('database_ready', False):
            st.success("✅ Database is ready for analysis!")
            
            # Display database stats if available
            if 'database_stats' in st.session_state:
                stats = st.session_state['database_stats']
                col1, col2, col3, col4 = st.columns(4)
                
                with col1:
                    st.metric("📊 Total Assets", f"{stats.get('total_assets', 0):,}")
                with col2:
                    st.metric("📄 Total Files", stats.get('total_files', 0))
                with col3:
                    st.metric("🏥 Health", stats.get('health_status', 'Unknown'))
                with col4:
                    st.metric("🗃️ Tables", stats.get('table_count', 0))
        else:
            st.warning("⚠️ Database not loaded. Please complete the Load tab first.")
            st.info("💡 **Next Step**: Go to the Load tab to create the database from normalized data.")
    
    def _display_ownership_summary_table(self, summary: Dict[str, Any]):
        """Display ownership summary as a table"""
        
        # Create summary data for the table using the 5 fields
        summary_data = [
            ["Total Parent Clouds", f"{summary['total_parent_clouds']:,}"],
            ["Total Clouds", f"{summary['total_clouds']:,}"],
            ["Total Assets", f"{summary['total_assets']:,}"],
            ["Total Assets Unowned", f"{summary['total_assets_unowned']:,}"],
            ["Total Teams", f"{summary['total_teams']:,}"]
        ]
        
         
        # Display the table
        df = pd.DataFrame(summary_data, columns=["Metric", "Value"])
        
        # Ensure all values are strings to avoid type conversion issues
        df['Value'] = df['Value'].astype(str)
        
        # Style the table
        st.dataframe(
            df,
            width='stretch',
            hide_index=True,
            column_config={
                "Metric": st.column_config.TextColumn("Metric", width="small"),
                "Value": st.column_config.TextColumn("Value", width="small")
            }
        )
        
        # Show additional insights
        if summary['total_assets_unowned'] > 0:
            st.warning(f"⚠️ {summary['total_assets_unowned']:,} assets are unowned and need to be assigned to teams.")
        
        if summary['total_assets'] > 0:
            st.info(f"📊 Analysis completed on {summary['total_assets']:,} total assets")
    
    def _display_all_distribution_charts(self):
        """Display all ownership distribution charts"""
        
        # Check if analyser has a reader initialized
        if not hasattr(self.analyser, 'reader') or not self.analyser.reader:
            st.warning("⚠️ No data available for distribution charts. Please run the analysis first.")
            return
        
        # Create tabs for different distribution views
        tab1, tab2, tab3 = st.tabs([
            "🏢 Parent Cloud", 
            "☁️ Cloud", 
            "👥 Team"
        ])
        
        with tab1:
            self._display_distribution_chart(
                "Parent Cloud", 
                self.analyser.get_parent_cloud_distribution(),
                "parent_cloud"
            )
        
        with tab2:
            self._display_distribution_chart(
                "Cloud", 
                self.analyser.get_cloud_distribution(),
                "cloud"
            )
        
        with tab3:
            self._display_distribution_chart(
                "Team", 
                self.analyser.get_team_distribution(),
                "team"
            )
    
    def _display_distribution_chart(self, title: str, distribution_data: list, field_name: str):
        """Display a single distribution chart"""
        
        try:
            if not distribution_data:
                st.warning("⚠️ No distribution data available")
                return
            
            # Create DataFrame
            df = pd.DataFrame(distribution_data)
            
            # Ensure numeric columns are properly typed
            df['total_assets'] = pd.to_numeric(df['total_assets'], errors='coerce').fillna(0).astype(int)
            df['unowned_assets'] = pd.to_numeric(df['unowned_assets'], errors='coerce').fillna(0).astype(int)
            
            # Create the bar chart
            fig = go.Figure()
            
            # Add total assets bar
            fig.add_trace(go.Bar(
                name='Total Assets',
                x=df[field_name],
                y=df['total_assets'],
                marker_color='lightblue',
                text=df['total_assets'],
                textposition='auto'
            ))
            
            # Add unowned assets bar
            fig.add_trace(go.Bar(
                name='Unowned Assets',
                x=df[field_name],
                y=df['unowned_assets'],
                marker_color='red',
                text=df['unowned_assets'],
                textposition='auto'
            ))
            
            # Update layout
            fig.update_layout(
                title=f"Assets Ownership Distribution by {title}",
                xaxis_title=title,
                yaxis_title="Number of Assets",
                barmode='group',
                height=400,
                showlegend=True,
                xaxis={'categoryorder': 'total descending'}
            )
            
            # Display the chart
            st.plotly_chart(fig, use_container_width=True)
            
            # Show summary statistics
            # Display metrics as a table (same style as Transform tab)
            total_unowned = df['unowned_assets'].sum()
            unowned_percentage = (total_unowned / df['total_assets'].sum()) * 100 if df['total_assets'].sum() > 0 else 0
            
            # Calculate owned percentage for consistency
            owned_percentage = ((df['total_assets'].sum() - total_unowned) / df['total_assets'].sum()) * 100 if df['total_assets'].sum() > 0 else 0
            
            summary_data = {
                'Metric': [f'Total {title}s', 'Total Unowned Assets', 'Ownership Coverage'],
                'Count': [len(df), f"{total_unowned:,}", f"{owned_percentage:.1f}%"]
            }
            
            df_summary = pd.DataFrame(summary_data)
            st.dataframe(
                df_summary, 
                width='content', 
                hide_index=True,
                column_config={
                    col: st.column_config.TextColumn(
                        col,
                        help=f"Shows {col.lower()} information",
                        width="small"
                    )
                    for col in df_summary.columns
                }
            )
            
        except Exception as e:
            st.error(f"❌ Error creating {title.lower()} distribution chart: {str(e)}")
    
    def is_complete(self, workflow_state):
        """Check if ownership analysis is complete"""
        return workflow_state.get('ownership_complete', False)
    
    def _ensure_reader_connection(self, target_folder: str):
        """Ensure reader connection is established and valid"""
        try:
            # Check if reader exists and is connected
            if not hasattr(self.analyser, 'reader') or self.analyser.reader is None:
                # The database is already loaded by SonicMemoryDuckdb in Load tab
                # We need to use MemoryDuckdb to query the existing database files
                from database.duckdb.memory.basic import BasicMemoryDuckdb
                
                # Create a new BasicMemoryDuckdb instance to query the existing database
                # This will connect to the database files created by the Load tab
                self.analyser.reader = BasicMemoryDuckdb.get_instance(target_folder)
                
                # Check if database is ready
                readiness_result = self.analyser.reader.check_data_readiness()
                if not readiness_result.get('ready', False):
                    raise Exception(f"Database not ready: {readiness_result.get('error', 'Unknown error')}")
                    
            return True
        except Exception as e:
            # If connection fails, try to create a new one
            try:
                from database.duckdb.memory.basic import BasicMemoryDuckdb
                
                self.analyser.reader = BasicMemoryDuckdb.get_instance(target_folder)
                
                # Check if database is ready
                readiness_result = self.analyser.reader.check_data_readiness()
                if not readiness_result.get('ready', False):
                    raise Exception(f"Database not ready: {readiness_result.get('error', 'Unknown error')}")
                    
                return True
            except Exception as e2:
                raise Exception(f"Failed to establish database connection: {str(e2)}")
    
    def _close_reader_connection(self):
        """Close reader connection if it exists"""
        try:
            if hasattr(self.analyser, 'close_reader'):
                self.analyser.close_reader()
        except Exception as e:
            # Ignore errors when closing connection
            pass
    
    def _get_ownership_summary_result(self, target_folder: str, asset_class: str = None):
        """Get ownership summary analysis result"""
        try:
            # Ensure reader connection is established
            self._ensure_reader_connection(target_folder)
            summary = self.analyser.get_ownership_summary(asset_class)
            if summary is None:
                return {
                    'type': 'ownership_summary',
                    'data': None,
                    'success': False,
                    'error': 'Failed to get ownership summary - no data returned'
                }
            return {
                'type': 'ownership_summary',
                'data': summary,
                'success': True,
                'error': None
            }
        except Exception as e:
            return {
                'type': 'ownership_summary',
                'data': None,
                'success': False,
                'error': str(e)
            }
    
    
    def _run_ownership_summary(self, target_folder: str):
        """Run ownership summary analysis"""
        try:
            st.info("📊 Running Ownership Summary Analysis...")
            
            # Get ownership summary from analyser
            summary = self.analyser.get_ownership_summary()
            
            if summary:
                # Display summary metrics
                # Display summary metrics as a table (same style as Transform tab)
                summary_data = {
                    'Metric': ['Total Parent Clouds', 'Total Clouds', 'Total Assets', 'Unowned Assets', 'Total Teams', 'Owned Assets'],
                    'Count': [
                        summary.get('total_parent_clouds', 0),
                        summary.get('total_clouds', 0),
                        summary.get('total_assets', 0),
                        summary.get('total_assets_unowned', 0),
                        summary.get('total_teams', 0),
                        summary.get('total_assets', 0) - summary.get('total_assets_unowned', 0)
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
                
                st.success("✅ Ownership summary analysis completed!")
            else:
                st.error("❌ Failed to get ownership summary")
                
        except Exception as e:
            st.error(f"❌ Error running ownership summary: {str(e)}")
    
    def _get_parent_cloud_analysis_result(self, target_folder: str, asset_class: str = None):
        """Get parent cloud analysis result"""
        try:
            # Ensure reader connection is established
            self._ensure_reader_connection(target_folder)
            distribution = self.analyser.get_parent_cloud_distribution(asset_class)
            if distribution is None:
                return {
                    'type': 'parent_cloud_analysis',
                    'data': None,
                    'success': False,
                    'error': 'Failed to get parent cloud distribution - no data returned'
                }
            return {
                'type': 'parent_cloud_analysis',
                'data': distribution,
                'success': len(distribution) > 0,
                'error': None if len(distribution) > 0 else 'No data available'
            }
        except Exception as e:
            return {
                'type': 'parent_cloud_analysis',
                'data': None,
                'success': False,
                'error': str(e)
            }
    
    def _run_parent_cloud_analysis(self, target_folder: str):
        """Run parent cloud analysis"""
        try:
            st.info("🏢 Running Parent Cloud Analysis...")
            
            # Get parent cloud distribution
            distribution = self.analyser.get_parent_cloud_distribution(target_folder)
            
            if distribution and not distribution.empty:
                # Create chart
                fig = px.bar(
                    distribution, 
                    x='parent_cloud', 
                    y=['total_assets', 'unowned_assets'],
                    barmode='group'
                )
                
                
                st.plotly_chart(fig, use_container_width=True)
                
                # Display table with left alignment
                st.dataframe(
                    distribution, 
                    width='stretch',
                    column_config={
                        col: st.column_config.TextColumn(
                            col,
                            help=f"Shows {col.lower()} information",
                            width="small"
                        )
                        for col in distribution.columns
                    }
                )
                st.success("✅ Parent cloud analysis completed!")
            else:
                st.warning("⚠️ No data available")
                
        except Exception as e:
            st.error(f"❌ Error running parent cloud analysis: {str(e)}")
    
    def _run_cloud_analysis(self, target_folder: str):
        """Run cloud analysis"""
        try:
            st.info("☁️ Running Cloud Analysis...")
            
            # Get cloud distribution
            distribution = self.analyser.get_cloud_distribution(target_folder)
            
            if distribution and not distribution.empty:
                # Create chart
                fig = px.bar(
                    distribution, 
                    x='cloud', 
                    y=['total_assets', 'unowned_assets'],
                    barmode='group'
                )
                
                
                st.plotly_chart(fig, use_container_width=True)
                
                # Display table with left alignment
                st.dataframe(
                    distribution, 
                    width='stretch',
                    column_config={
                        col: st.column_config.TextColumn(
                            col,
                            help=f"Shows {col.lower()} information",
                            width="small"
                        )
                        for col in distribution.columns
                    }
                )
                st.success("✅ Cloud analysis completed!")
            else:
                st.warning("⚠️ No data available")
                
        except Exception as e:
            st.error(f"❌ Error running cloud analysis: {str(e)}")
    
    def _run_team_analysis(self, target_folder: str):
        """Run team analysis"""
        try:
            st.info("👥 Running Team Analysis...")
            
            # Get team distribution
            distribution = self.analyser.get_team_distribution(target_folder)
            
            if distribution and not distribution.empty:
                # Create chart with dynamic legend labels
                # Prepare data for legend labels
                total_assets_sum = distribution['total_assets'].sum()
                unowned_assets_sum = distribution['unowned_assets'].sum()
                
                total_label = f"Total Assets ({total_assets_sum:,})" if total_assets_sum > 0 else "Total Assets (0, not shown)"
                unowned_label = f"Unowned Assets ({unowned_assets_sum:,})" if unowned_assets_sum > 0 else "Unowned Assets (0, not shown)"
                
                # Rename columns to match legend labels
                df_chart = distribution.copy()
                df_chart = df_chart.rename(columns={
                    'total_assets': total_label,
                    'unowned_assets': unowned_label
                })
                
                fig = px.bar(
                    df_chart, 
                    x='team', 
                    y=[total_label, unowned_label],
                    barmode='group'
                )
                
                
                st.plotly_chart(fig, use_container_width=True)
                
                # Clean numeric columns and ensure integer values
                if 'total_assets' in distribution.columns:
                    distribution['total_assets'] = clean_numeric_column(distribution['total_assets']).astype(int)
                if 'unowned_assets' in distribution.columns:
                    distribution['unowned_assets'] = clean_numeric_column(distribution['unowned_assets']).astype(int)
                
                # Rename columns for display
                display_df = distribution.rename(columns={
                    'total_assets': 'Total Assets',
                    'unowned_assets': 'Unowned Assets'
                })
                
                # Display table with left alignment
                st.dataframe(
                    display_df, 
                    width='stretch',
                    column_config={
                        col: st.column_config.TextColumn(
                            col,
                            help=f"Shows {col.lower()} information",
                            width="small"
                        )
                        for col in display_df.columns
                    }
                )
                st.success("✅ Team analysis completed!")
            else:
                st.warning("⚠️ No data available")
                
        except Exception as e:
            st.error(f"❌ Error running team analysis: {str(e)}")
    
    def _run_ownership_trends(self, target_folder: str):
        """Run ownership trends analysis"""
        try:
            st.info("📈 Running Ownership Trends Analysis...")
            
            # Placeholder for trends analysis
            st.write("**Ownership Trends Analysis**")
            st.write("This analysis would show ownership trends over time.")
            
            # Sample trend data
            import numpy as np
            
            dates = pd.date_range('2024-01-01', periods=12, freq='M')
            trend_data = pd.DataFrame({
                'Month': dates,
                'Owned Assets': np.random.randint(140000, 160000, 12),
                'Unowned Assets': np.random.randint(20000, 30000, 12)
            })
            
            fig = px.line(trend_data, x='Month', y=['Owned Assets', 'Unowned Assets'], 
                         title="Ownership Trends Over Time")
            st.plotly_chart(fig, use_container_width=True)
            
            st.success("✅ Ownership trends analysis completed!")
                
        except Exception as e:
            st.error(f"❌ Error running ownership trends: {str(e)}")
    
    def _run_deep_dive_analysis(self, target_folder: str):
        """Run deep dive analysis"""
        try:
            st.info("🔍 Running Deep Dive Analysis...")
            
            # Placeholder for deep dive analysis
            st.write("**Deep Dive Analysis**")
            st.write("This analysis would provide detailed insights into ownership patterns.")
            
            # Sample deep dive metrics as a table (same style as Transform tab)
            summary_data = {
                'Metric': ['Ownership Coverage', 'Top Owning Team', 'Unowned Assets'],
                'Count': ['85.3%', 'Team A', '24,959']
            }
            
            df_summary = pd.DataFrame(summary_data)
            st.dataframe(
                df_summary, 
                width='content', 
                hide_index=True,
                column_config={
                    col: st.column_config.TextColumn(
                        col,
                        help=f"Shows {col.lower()} information",
                        width="small"
                    )
                    for col in df_summary.columns
                }
            )
            
            # Sample detailed breakdown
            st.markdown("**Detailed Breakdown:**")
            breakdown_data = pd.DataFrame({
                'Category': ['Servers', 'Databases', 'Network', 'Storage'],
                'Total': [85000, 45000, 25000, 15193],
                'Owned': [75000, 40000, 20000, 12000],
                'Unowned': [10000, 5000, 5000, 3193]
            })
            st.dataframe(
                breakdown_data, 
                width='stretch',
                column_config={
                    col: st.column_config.TextColumn(
                        col,
                        help=f"Shows {col.lower()} information",
                        width="small"
                    )
                    for col in breakdown_data.columns
                }
            )
            
            st.success("✅ Deep dive analysis completed!")
                
        except Exception as e:
            st.error(f"❌ Error running deep dive analysis: {str(e)}")
    
    def _get_cloud_analysis_result(self, target_folder: str, asset_class: str = None):
        """Get cloud analysis result"""
        try:
            # Ensure reader connection is established
            self._ensure_reader_connection(target_folder)
            distribution = self.analyser.get_cloud_distribution(asset_class)
            if distribution is None or len(distribution) == 0:
                return {
                    'type': 'cloud_analysis',
                    'data': None,
                    'success': False,
                    'error': 'No cloud data found - cloud field may not exist in the database'
                }
            return {
                'type': 'cloud_analysis',
                'data': distribution,
                'success': True,
                'error': None
            }
        except Exception as e:
            return {
                'type': 'cloud_analysis',
                'data': None,
                'success': False,
                'error': str(e)
            }
    
    def _get_team_analysis_result(self, target_folder: str, asset_class: str = None):
        """Get team analysis result"""
        try:
            # Ensure reader connection is established
            self._ensure_reader_connection(target_folder)
            distribution = self.analyser.get_team_distribution(asset_class)
            if distribution is None or len(distribution) == 0:
                return {
                    'type': 'team_analysis',
                    'data': None,
                    'success': False,
                    'error': 'No team data found - team field may not exist in the database'
                }
            return {
                'type': 'team_analysis',
                'data': distribution,
                'success': True,
                'error': None
            }
        except Exception as e:
            return {
                'type': 'team_analysis',
                'data': None,
                'success': False,
                'error': str(e)
            }
    
    def _get_mbu_analysis_result(self, target_folder: str, asset_class: str = None):
        """Get MBU analysis result"""
        try:
            # Ensure reader connection is established
            self._ensure_reader_connection(target_folder)
            distribution = self.analyser.get_mbu_distribution(asset_class)
            if distribution is None or len(distribution) == 0:
                return {
                    'type': 'mbu_analysis',
                    'data': None,
                    'success': False,
                    'error': 'No data available'
                }
            return {
                'type': 'mbu_analysis',
                'data': distribution,
                'success': True,
                'error': None
            }
        except Exception as e:
            return {
                'type': 'mbu_analysis',
                'data': None,
                'success': False,
                'error': str(e)
            }
    
    def _get_bu_analysis_result(self, target_folder: str, asset_class: str = None):
        """Get BU analysis result"""
        try:
            # Ensure reader connection is established
            self._ensure_reader_connection(target_folder)
            distribution = self.analyser.get_bu_distribution(asset_class)
            if distribution is None or len(distribution) == 0:
                return {
                    'type': 'bu_analysis',
                    'data': None,
                    'success': False,
                    'error': 'No data available'
                }
            return {
                'type': 'bu_analysis',
                'data': distribution,
                'success': True,
                'error': None
            }
        except Exception as e:
            return {
                'type': 'bu_analysis',
                'data': None,
                'success': False,
                'error': str(e)
            }
    
    def _get_ownership_trends_result(self, target_folder: str):
        """Get ownership trends result"""
        try:
            # Generate sample trend data
            import numpy as np
            
            dates = pd.date_range('2024-01-01', periods=12, freq='M')
            trend_data = pd.DataFrame({
                'Month': dates,
                'Owned Assets': np.random.randint(140000, 160000, 12),
                'Unowned Assets': np.random.randint(20000, 30000, 12)
            })
            
            return {
                'type': 'ownership_trends',
                'data': trend_data,
                'success': True,
                'error': None
            }
        except Exception as e:
            return {
                'type': 'ownership_trends',
                'data': None,
                'success': False,
                'error': str(e)
            }
    
    def _get_deep_dive_analysis_result(self, target_folder: str):
        """Get deep dive analysis result"""
        try:
            # Generate sample deep dive data
            
            breakdown_data = pd.DataFrame({
                'Category': ['Servers', 'Databases', 'Network', 'Storage'],
                'Total': [85000, 45000, 25000, 15193],
                'Owned': [75000, 40000, 20000, 12000],
                'Unowned': [10000, 5000, 5000, 3193]
            })
            
            metrics = {
                'ownership_coverage': 85.3,
                'top_owning_team': 'Team A',
                'unowned_assets': 24959
            }
            
            return {
                'type': 'deep_dive_analysis',
                'data': {'breakdown': breakdown_data, 'metrics': metrics},
                'success': True,
                'error': None
            }
        except Exception as e:
            return {
                'type': 'deep_dive_analysis',
                'data': None,
                'success': False,
                'error': str(e)
            }
    
    def _display_analysis_result(self, analysis_type: str, result: dict):
        """Display analysis result in tab"""
        if not result['success']:
            st.error(f"❌ Analysis failed: {result['error']}")
            return
        
        data = result['data']
        
        if analysis_type == 'ownership_summary':
            if data:
                # Display summary metrics as a table (same style as Transform tab)
                total_assets = data.get('total_assets', 0)
                unowned_assets = data.get('total_assets_unowned', 0)
                owned_assets = total_assets - unowned_assets
                
                summary_data = {
                    'Metric': ['Total Parent Clouds', 'Total Clouds', 'Total Assets', 'Unowned Assets', 'Total Teams', 'Owned Assets'],
                    'Count': [
                        str(data.get('total_parent_clouds', 0)),
                        str(data.get('total_clouds', 0)),
                        str(total_assets),
                        str(unowned_assets),
                        str(data.get('total_teams', 0)),
                        str(owned_assets)
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
        
        elif analysis_type == 'parent_cloud_analysis':
            if data is not None and len(data) > 0:
                # Convert to DataFrame for plotting and ensure proper data types
                df = safe_dataframe(data)
                
                # Ensure numeric columns are properly typed as integers
                if 'total_assets' in df.columns:
                    df['total_assets'] = clean_numeric_column(df['total_assets']).astype(int)
                if 'unowned_assets' in df.columns:
                    df['unowned_assets'] = clean_numeric_column(df['unowned_assets']).astype(int)
                
                # Rename columns for user-friendly display
                df_display = df.copy()
                df_display = df_display.rename(columns={
                    'parent_cloud': 'Parent Cloud',
                    'total_assets': 'Total Assets',
                    'unowned_assets': 'Unowned Assets'
                })
                
                # Create chart with dynamic legend labels
                # Prepare data for legend labels
                total_assets_sum = df['total_assets'].sum()
                unowned_assets_sum = df['unowned_assets'].sum()
                owned_assets_sum = total_assets_sum - unowned_assets_sum
                ownership_percentage = (owned_assets_sum / total_assets_sum * 100) if total_assets_sum > 0 else 0
                
                total_label = f"Total Assets ({total_assets_sum:,})" if total_assets_sum > 0 else "Total Assets (0, not shown)"
                unowned_label = f"Unowned Assets ({unowned_assets_sum:,})" if unowned_assets_sum > 0 else "Unowned Assets (0, not shown)"
                
                # Rename columns to match legend labels
                df_chart = df.copy()
                df_chart = df_chart.rename(columns={
                    'total_assets': total_label,
                    'unowned_assets': unowned_label
                })
                
                fig = px.bar(
                    df_chart, 
                    x='parent_cloud', 
                    y=[total_label, unowned_label],
                    barmode='group',
                    labels={
                        'parent_cloud': 'Parent Cloud'
                    }
                )
                
                
                # Update layout for better readability
                fig.update_layout(
                    title=f"Assets Distribution by Parent Cloud<br><sub>Ownership: {ownership_percentage:.1f}% ({owned_assets_sum:,} owned out of {total_assets_sum:,} total)</sub>",
                    xaxis_title="Parent Cloud",
                    yaxis_title="Number of Assets",
                    height=500,
                    xaxis={'categoryorder': 'total descending'}
                )
                
                # Update hover template for better user experience
                fig.update_traces(
                    hovertemplate="Parent Cloud: %{x}<br>" +
                                 "Assets: %{y:,.0f}<br>" +
                                 "<extra></extra>"
                )
                st.plotly_chart(fig, use_container_width=True)
                
                # Add separator between chart and table
                st.markdown("---")
                
                # Display table with user-friendly headers and left alignment
                st.dataframe(
                    df_display, 
                    width='stretch',
                    column_config={
                        col: st.column_config.TextColumn(
                            col,
                            help=f"Shows {col.lower()} information",
                            width="small"
                        )
                        for col in df_display.columns
                    }
                )
            else:
                st.warning("⚠️ No data available")
        
        elif analysis_type == 'cloud_analysis':
            if data is not None and len(data) > 0:
                # Convert to DataFrame for plotting and ensure proper data types
                df = safe_dataframe(data)
                
                # Ensure numeric columns are properly typed as integers
                if 'total_assets' in df.columns:
                    df['total_assets'] = clean_numeric_column(df['total_assets']).astype(int)
                if 'unowned_assets' in df.columns:
                    df['unowned_assets'] = clean_numeric_column(df['unowned_assets']).astype(int)
                
                # Rename columns for user-friendly display
                df_display = df.copy()
                df_display = df_display.rename(columns={
                    'cloud': 'Cloud',
                    'total_assets': 'Total Assets',
                    'unowned_assets': 'Unowned Assets'
                })
                
                # Create chart with dynamic legend labels
                # Prepare data for legend labels
                total_assets_sum = df['total_assets'].sum()
                unowned_assets_sum = df['unowned_assets'].sum()
                owned_assets_sum = total_assets_sum - unowned_assets_sum
                ownership_percentage = (owned_assets_sum / total_assets_sum * 100) if total_assets_sum > 0 else 0
                
                total_label = f"Total Assets ({total_assets_sum:,})" if total_assets_sum > 0 else "Total Assets (0, not shown)"
                unowned_label = f"Unowned Assets ({unowned_assets_sum:,})" if unowned_assets_sum > 0 else "Unowned Assets (0, not shown)"
                
                # Rename columns to match legend labels
                df_chart = df.copy()
                df_chart = df_chart.rename(columns={
                    'total_assets': total_label,
                    'unowned_assets': unowned_label
                })
                
                fig = px.bar(
                    df_chart, 
                    x='cloud', 
                    y=[total_label, unowned_label],
                    barmode='group',
                    labels={
                        'cloud': 'Cloud'
                    }
                )
                
                
                # Update layout for better readability
                fig.update_layout(
                    title=f"Assets Distribution by Cloud<br><sub>Ownership: {ownership_percentage:.1f}% ({owned_assets_sum:,} owned out of {total_assets_sum:,} total)</sub>",
                    xaxis_title="Cloud",
                    yaxis_title="Number of Assets",
                    height=500,
                    xaxis={'categoryorder': 'total descending'}
                )
                
                # Update hover template for better user experience
                fig.update_traces(
                    hovertemplate="Cloud: %{x}<br>" +
                                 "Assets: %{y:,.0f}<br>" +
                                 "<extra></extra>"
                )
                st.plotly_chart(fig, use_container_width=True)
                
                # Add separator between chart and table
                st.markdown("---")
                
                # Display table with user-friendly headers and left alignment
                st.dataframe(
                    df_display, 
                    width='stretch',
                    column_config={
                        col: st.column_config.TextColumn(
                            col,
                            help=f"Shows {col.lower()} information",
                            width="small"
                        )
                        for col in df_display.columns
                    }
                )
            else:
                st.warning("⚠️ No data available")
        
        elif analysis_type == 'team_analysis':
            if data is not None and len(data) > 0:
                # Convert to DataFrame for plotting and ensure proper data types
                df = safe_dataframe(data)
                
                # Ensure numeric columns are properly typed as integers
                if 'total_assets' in df.columns:
                    df['total_assets'] = clean_numeric_column(df['total_assets']).astype(int)
                if 'unowned_assets' in df.columns:
                    df['unowned_assets'] = clean_numeric_column(df['unowned_assets']).astype(int)
                
                # Rename columns for user-friendly display
                df_display = df.copy()
                df_display = df_display.rename(columns={
                    'team': 'Team',
                    'total_assets': 'Total Assets',
                    'unowned_assets': 'Unowned Assets'
                })
                
                # Create chart with dynamic legend labels
                # Prepare data for legend labels
                total_assets_sum = df['total_assets'].sum()
                unowned_assets_sum = df['unowned_assets'].sum()
                
                total_label = f"Total Teams ({total_assets_sum:,})" if total_assets_sum > 0 else "Total Teams (0, not shown)"
                unowned_label = f"Unowned Teams ({unowned_assets_sum:,})" if unowned_assets_sum > 0 else "Unowned Teams (0, not shown)"
                
                fig = px.bar(
                    df, 
                    x='team', 
                    y=['total_assets', 'unowned_assets'],
                    barmode='group',
                    labels={
                        'team': 'Team',
                        'total_assets': total_label,
                        'unowned_assets': unowned_label
                    }
                )
                
                
                # Update hover template for better user experience
                fig.update_traces(
                    hovertemplate="Team: %{x}<br>" +
                                 "Assets: %{y:,.0f}<br>" +
                                 "<extra></extra>"
                )
                st.plotly_chart(fig, use_container_width=True)
                
                # Add separator between chart and table
                st.markdown("---")
                
                # Display table with user-friendly headers and left alignment
                st.dataframe(
                    df_display, 
                    width='stretch',
                    column_config={
                        col: st.column_config.TextColumn(
                            col,
                            help=f"Shows {col.lower()} information",
                            width="small"
                        )
                        for col in df_display.columns
                    }
                )
            else:
                st.warning("⚠️ No data available")
        
        elif analysis_type == 'mbu_analysis':
            if data is not None and len(data) > 0:
                # Convert to DataFrame for plotting and ensure proper data types
                df = safe_dataframe(data)
                
                # Ensure numeric columns are properly typed as integers
                if 'total_assets' in df.columns:
                    df['total_assets'] = clean_numeric_column(df['total_assets']).astype(int)
                if 'unowned_assets' in df.columns:
                    df['unowned_assets'] = clean_numeric_column(df['unowned_assets']).astype(int)
                
                # Rename columns for user-friendly display
                df_display = df.copy()
                df_display = df_display.rename(columns={
                    'mbu': 'MBU',
                    'total_assets': 'Total Assets',
                    'unowned_assets': 'Unowned Assets'
                })
                
                # Create chart for MBU only with dynamic legend labels
                # Prepare data for legend labels
                total_assets_sum = df['total_assets'].sum()
                unowned_assets_sum = df['unowned_assets'].sum()
                
                total_label = f"Total Assets ({total_assets_sum:,})" if total_assets_sum > 0 else "Total Assets (0, not shown)"
                unowned_label = f"Unowned Assets ({unowned_assets_sum:,})" if unowned_assets_sum > 0 else "Unowned Assets (0, not shown)"
                
                # Rename columns to match legend labels
                df_chart = df.copy()
                df_chart = df_chart.rename(columns={
                    'total_assets': total_label,
                    'unowned_assets': unowned_label
                })
                
                fig = px.bar(
                    df_chart, 
                    x='mbu', 
                    y=[total_label, unowned_label],
                    barmode='group',
                    labels={
                        'mbu': 'MBU'
                    }
                )
                
                
                # Update layout for better readability
                fig.update_layout(
                    title="Assets Distribution by Management Business Unit (MBU)",
                    xaxis_title="MBU",
                    yaxis_title="Number of Assets",
                    height=500,
                    xaxis={'categoryorder': 'total descending'}
                )
                
                # Update hover template for better user experience
                fig.update_traces(
                    hovertemplate="MBU: %{x}<br>" +
                                 "Assets: %{y:,.0f}<br>" +
                                 "<extra></extra>"
                )
                st.plotly_chart(fig, use_container_width=True)
                
                # Add separator between chart and table
                st.markdown("---")
                
                # Display table with user-friendly headers and left alignment
                st.dataframe(
                    df_display, 
                    width='stretch',
                    column_config={
                        col: st.column_config.TextColumn(
                            col,
                            help=f"Shows {col.lower()} information",
                            width="small"
                        )
                        for col in df_display.columns
                    }
                )
            else:
                st.warning("⚠️ No data available")
        
        elif analysis_type == 'bu_analysis':
            if data is not None and len(data) > 0:
                # Convert to DataFrame for plotting and ensure proper data types
                df = safe_dataframe(data)
                
                # Ensure numeric columns are properly typed as integers
                if 'total_assets' in df.columns:
                    df['total_assets'] = clean_numeric_column(df['total_assets']).astype(int)
                if 'unowned_assets' in df.columns:
                    df['unowned_assets'] = clean_numeric_column(df['unowned_assets']).astype(int)
                
                # Create a combined label for the chart (BU | MBU)
                df['bu_mbu'] = df['bu'].astype(str) + ' | ' + df['mbu'].astype(str)
                
                # Rename columns for user-friendly display
                df_display = df.copy()
                df_display = df_display.rename(columns={
                    'bu': 'BU',
                    'mbu': 'MBU',
                    'total_assets': 'Total Assets',
                    'unowned_assets': 'Unowned Assets'
                })
                
                # Create chart with combined BU|MBU labels and dynamic legend
                # Prepare data for legend labels
                total_assets_sum = df['total_assets'].sum()
                unowned_assets_sum = df['unowned_assets'].sum()
                
                total_label = f"Total Assets ({total_assets_sum:,})" if total_assets_sum > 0 else "Total Assets (0, not shown)"
                unowned_label = f"Unowned Assets ({unowned_assets_sum:,})" if unowned_assets_sum > 0 else "Unowned Assets (0, not shown)"
                
                # Rename columns to match legend labels
                df_chart = df.copy()
                df_chart = df_chart.rename(columns={
                    'total_assets': total_label,
                    'unowned_assets': unowned_label
                })
                
                fig = px.bar(
                    df_chart, 
                    x='bu_mbu', 
                    y=[total_label, unowned_label],
                    barmode='group',
                    labels={
                        'bu_mbu': 'BU | MBU'
                    }
                )
                
                
                # Update layout for better readability
                fig.update_layout(
                    title="Assets Distribution by Business Unit and Management Business Unit",
                    xaxis_title="BU | MBU",
                    yaxis_title="Number of Assets",
                    height=500,
                    xaxis={'categoryorder': 'total descending'}
                )
                
                # Update hover template for better user experience
                fig.update_traces(
                    hovertemplate="BU | MBU: %{x}<br>" +
                                 "Assets: %{y:,.0f}<br>" +
                                 "<extra></extra>"
                )
                st.plotly_chart(fig, use_container_width=True)
                
                # Add separator between chart and table
                st.markdown("---")
                
                # Display table with user-friendly headers and left alignment
                st.dataframe(
                    df_display, 
                    width='stretch',
                    column_config={
                        col: st.column_config.TextColumn(
                            col,
                            help=f"Shows {col.lower()} information",
                            width="small"
                        )
                        for col in df_display.columns
                    }
                )
            else:
                st.warning("⚠️ No data available")
        

