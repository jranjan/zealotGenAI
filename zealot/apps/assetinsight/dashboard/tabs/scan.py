"""
Scan Tab - Measures scan coverage and provides scanning insights
"""

import streamlit as st
import pandas as pd
import plotly.express as px
from typing import Dict, List, Any, Optional
import sys
from pathlib import Path
from datetime import datetime

# Add the current directory to Python path
sys.path.append(str(Path(__file__).parent.parent.parent))

from dashboard.tabs.base import BaseTab
from database.duckdb.factory import DatabaseFactory
from common.asset_class import AssetClass


class ScanTab(BaseTab):
    """Handles scan coverage measurement and analysis"""
    
    def __init__(self):
        super().__init__(
            tab_name="Scan",
            description="Measure scan coverage and analyze scanning patterns"
        )
        
        # Initialize scan metrics
        self.scan_metrics = {
            'total_assets': 0,
            'scanned_assets': 0,
            'coverage_percentage': 0.0,
            'last_scan_date': None,
            'scan_frequency': 'Unknown',
            'missing_assets': [],
            'scan_history': []
        }
    
    def _render_content(self):
        """Render scan coverage interface"""
        self._render_header()
        
        if not self._check_database_ready():
            return
        
        scan_data = self._get_scan_data()
        if not scan_data:
            st.error("❌ No scan data available. Please ensure your database is loaded with asset data.")
            return
        
        self._render_scan_sections(scan_data)
    
    def _render_header(self):
        """Render scan tab header."""
        st.markdown("""
        <div style='background-color: #1f77b4; color: white; padding: 10px 15px; border-radius: 5px; margin-bottom: 20px; text-align: left; font-weight: bold;'>
            
        </div>
        """, unsafe_allow_html=True)
    
    def _check_database_ready(self):
        """Check if database is ready."""
        if not st.session_state.get('database_ready', False):
            st.warning("⚠️ Database not ready. Please complete the Inventory workflow first.")
            return False
        return True
    
    def _render_scan_sections(self, scan_data):
        """Render all scan-related sections."""
        self._display_scan_metrics(scan_data)
        self._display_coverage_chart(scan_data)
        self._display_scan_history(scan_data)
        self._display_missing_assets(scan_data)
        self._display_scan_recommendations(scan_data)
    
    def _create_union_query(self, base_query: str) -> str:
        """Create a UNION query across all asset tables"""
        table_names = AssetClass.get_all_table_names()
        
        # Debug: Print number of tables being queried
        print(f"🔍 Scan Tab: Querying across {len(table_names)} tables: {', '.join(table_names)}")
        
        # Create UNION query for each table
        union_parts = []
        for table_name in table_names:
            # Replace 'FROM assets' with 'FROM {table_name}' in the query
            table_query = base_query.replace('FROM assets', f'FROM {table_name}')
            union_parts.append(f"({table_query})")
        
        return " UNION ALL ".join(union_parts)
    
    def _get_scan_data(self) -> Optional[Dict[str, Any]]:
        """Get scan data from the database"""
        try:
            # Get database instance
            db_instance = st.session_state.get('db_instance')
            if not db_instance:
                print("❌ Scan Tab: No database instance found in session state")
                return None
            
            print("🔍 Scan Tab: Database instance found, querying scan data...")
            
            # First, let's check what tables exist and what data is available
            try:
                tables_result = db_instance.conn.execute("SHOW TABLES").fetchall()
                table_names = [table[0] for table in tables_result] if tables_result else []
                print(f"📋 Available tables: {table_names}")
                
                # Check if we have any data in the first table
                if table_names:
                    first_table = table_names[0]
                    count_result = db_instance.conn.execute(f"SELECT COUNT(*) FROM {first_table}").fetchone()
                    total_records = count_result[0] if count_result else 0
                    print(f"📊 Total records in {first_table}: {total_records}")
                    
                    # Check what columns are available
                    columns_result = db_instance.conn.execute(f"PRAGMA table_info({first_table})").fetchall()
                    column_names = [col[1] for col in columns_result] if columns_result else []
                    print(f"📝 Available columns in {first_table}: {column_names}")
            except Exception as e:
                print(f"❌ Error checking database structure: {e}")
            
            # Query scan-related data
            scan_queries = {
                'total_assets': self._create_union_query("SELECT COUNT(*) as count FROM assets"),
                'scanned_assets': self._create_union_query("SELECT COUNT(*) as count FROM assets WHERE lastScanned IS NOT NULL"),
                'scan_coverage_by_team': self._create_union_query("""
                    SELECT team, 
                           COUNT(*) as total_assets,
                           COUNT(CASE WHEN lastScanned IS NOT NULL THEN 1 END) as scanned_assets,
                           ROUND(COUNT(CASE WHEN lastScanned IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) as coverage_pct
                    FROM assets 
                    GROUP BY team 
                    ORDER BY coverage_pct DESC
                """),
                'scan_coverage_by_cloud': """
                    SELECT cloud, 
                           COUNT(*) as total_assets,
                           COUNT(CASE WHEN lastScanned IS NOT NULL THEN 1 END) as scanned_assets,
                           ROUND(COUNT(CASE WHEN lastScanned IS NOT NULL THEN 1 END) * 100.0 / COUNT(*), 2) as coverage_pct
                    FROM assets 
                    GROUP BY cloud 
                    ORDER BY coverage_pct DESC
                """,
                'recent_scans': """
                    SELECT lastScanned, COUNT(*) as scan_count
                    FROM assets 
                    WHERE lastScanned IS NOT NULL 
                    GROUP BY lastScanned 
                    ORDER BY lastScanned DESC 
                    LIMIT 30
                """,
                'missing_scans': """
                    SELECT id, name, team, cloud, assetClass
                    FROM assets 
                    WHERE lastScanned IS NULL 
                    ORDER BY team, cloud
                    LIMIT 100
                """
            }
            
            results = {}
            for query_name, query in scan_queries.items():
                try:
                    print(f"🔍 Executing query: {query_name}")
                    result = db_instance.conn.execute(query).fetchall()
                    if result:
                        if query_name in ['total_assets', 'scanned_assets']:
                            # Ensure we get a single numeric value
                            value = result[0][0]
                            # Handle case where result might be a list
                            if isinstance(value, list):
                                value = value[0] if value else 0
                            results[query_name] = int(value) if value is not None else 0
                            print(f"✅ {query_name}: {results[query_name]}")
                        else:
                            results[query_name] = result
                            print(f"✅ {query_name}: {len(result)} records")
                    else:
                        results[query_name] = [] if query_name not in ['total_assets', 'scanned_assets'] else 0
                        print(f"⚠️ {query_name}: No results")
                except Exception as e:
                    print(f"❌ Error executing {query_name}: {e}")
                    st.error(f"Error executing {query_name}: {e}")
                    results[query_name] = [] if query_name not in ['total_assets', 'scanned_assets'] else 0
            
            # Debug: Show summary of results
            total_assets = results.get('total_assets', 0)
            scanned_assets = results.get('scanned_assets', 0)
            print(f"📊 Scan Data Summary: {total_assets} total assets, {scanned_assets} scanned assets")
            
            return results
            
        except Exception as e:
            st.error(f"Error getting scan data: {e}")
            return None
    
    def _display_scan_metrics(self, scan_data: Dict[str, Any]):
        """Display key scan metrics"""
        st.markdown("### 📊 Scan Coverage Overview")
        
        total_assets = scan_data.get('total_assets', 0) or 0
        scanned_assets = scan_data.get('scanned_assets', 0) or 0
        
        # Ensure values are numbers
        try:
            total_assets = int(total_assets) if total_assets is not None else 0
            scanned_assets = int(scanned_assets) if scanned_assets is not None else 0
        except (ValueError, TypeError):
            total_assets = 0
            scanned_assets = 0
        
        coverage_pct = (scanned_assets / total_assets * 100) if total_assets > 0 else 0
        
        # Debug: Show what data we received
        print(f"🔍 Scan metrics - Total assets: {total_assets}, Scanned assets: {scanned_assets}, Coverage: {coverage_pct:.1f}%")
        
        # Create metrics columns
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric(
                label="Total Assets",
                value=f"{total_assets:,}",
                help="Total number of assets in inventory"
            )
        
        with col2:
            st.metric(
                label="Scanned Assets",
                value=f"{scanned_assets:,}",
                help="Number of assets that have been scanned"
            )
        
        with col3:
            st.metric(
                label="Coverage",
                value=f"{coverage_pct:.1f}%",
                help="Percentage of assets that have been scanned"
            )
        
        with col4:
            missing_assets = total_assets - scanned_assets
            st.metric(
                label="Missing Scans",
                value=f"{missing_assets:,}",
                help="Number of assets that need to be scanned"
            )
        
        # Coverage progress bar
        st.markdown("#### Coverage Progress")
        st.progress(coverage_pct / 100)
        
        if coverage_pct < 50:
            st.warning("⚠️ Low scan coverage detected. Consider prioritizing scan activities.")
        elif coverage_pct < 80:
            st.info("ℹ️ Moderate scan coverage. Continue scanning to improve coverage.")
        else:
            st.success("✅ Good scan coverage! Maintain regular scanning schedule.")
    
    def _display_coverage_chart(self, scan_data: Dict[str, Any]):
        """Display scan coverage charts"""
        st.markdown("### 📈 Coverage Analysis")
        
        # Team coverage chart
        team_coverage = scan_data.get('scan_coverage_by_team', [])
        if team_coverage:
            st.markdown("#### Coverage by Team")
            team_df = pd.DataFrame(team_coverage, columns=['team', 'total_assets', 'scanned_assets', 'coverage_pct'])
            
            # Create horizontal bar chart
            fig = px.bar(
                team_df, 
                x='coverage_pct', 
                y='team',
                orientation='h',
                title="Scan Coverage by Team",
                labels={'coverage_pct': 'Coverage %', 'team': 'Team'},
                color='coverage_pct',
                color_continuous_scale='RdYlGn'
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
        
        # Cloud coverage chart
        cloud_coverage = scan_data.get('scan_coverage_by_cloud', [])
        if cloud_coverage:
            st.markdown("#### Coverage by Cloud Provider")
            cloud_df = pd.DataFrame(cloud_coverage, columns=['cloud', 'total_assets', 'scanned_assets', 'coverage_pct'])
            
            # Create pie chart
            fig = px.pie(
                cloud_df,
                values='total_assets',
                names='cloud',
                title="Asset Distribution by Cloud Provider",
                color_discrete_sequence=px.colors.qualitative.Set3
            )
            st.plotly_chart(fig, use_container_width=True)
    
    def _display_scan_history(self, scan_data: Dict[str, Any]):
        """Display scan history timeline"""
        st.markdown("### 📅 Scan History")
        
        recent_scans = scan_data.get('recent_scans', [])
        if recent_scans:
            scan_df = pd.DataFrame(recent_scans, columns=['lastScanned', 'scan_count'])
            scan_df['lastScanned'] = pd.to_datetime(scan_df['lastScanned'])
            scan_df = scan_df.sort_values('lastScanned')
            
            # Create timeline chart
            fig = px.line(
                scan_df,
                x='lastScanned',
                y='scan_count',
                title="Scan Activity Over Time",
                labels={'lastScanned': 'Scan Date', 'scan_count': 'Assets Scanned'}
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
            
            # Display recent scan summary
            st.markdown("#### Recent Scan Summary")
            st.dataframe(scan_df.tail(10), width='stretch')
        else:
            st.info("No recent scan data available.")
    
    def _display_missing_assets(self, scan_data: Dict[str, Any]):
        """Display assets that need to be scanned"""
        st.markdown("### 🚨 Assets Requiring Scans")
        
        missing_assets = scan_data.get('missing_scans', [])
        if missing_assets:
            missing_df = pd.DataFrame(missing_assets, columns=['id', 'name', 'team', 'cloud', 'assetClass'])
            
            # Summary by team
            team_summary = missing_df.groupby('team').size().reset_index(name='missing_count')
            st.markdown("#### Missing Scans by Team")
            st.dataframe(team_summary, width='stretch')
            
            # Detailed list
            st.markdown("#### Detailed Missing Assets List")
            st.dataframe(missing_df, width='stretch')
            
            # Download option
            csv = missing_df.to_csv(index=False)
            st.download_button(
                label="Download Missing Assets CSV",
                data=csv,
                file_name=f"missing_scans_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
        else:
            st.success("✅ All assets have been scanned!")
    
    def _display_scan_recommendations(self, scan_data: Dict[str, Any]):
        """Display scan recommendations"""
        st.markdown("### 💡 Scan Recommendations")
        
        # Ensure values are numeric
        total_assets = scan_data.get('total_assets', 0)
        scanned_assets = scan_data.get('scanned_assets', 0)
        
        # Convert to int if they're not already numeric
        try:
            total_assets = int(total_assets) if total_assets is not None else 0
            scanned_assets = int(scanned_assets) if scanned_assets is not None else 0
        except (ValueError, TypeError):
            total_assets = 0
            scanned_assets = 0
        
        coverage_pct = (scanned_assets / total_assets * 100) if total_assets > 0 else 0
        
        recommendations = []
        
        if coverage_pct < 30:
            recommendations.append("🔴 **Critical**: Very low scan coverage. Prioritize scanning all critical assets immediately.")
        elif coverage_pct < 60:
            recommendations.append("🟡 **High Priority**: Low scan coverage. Focus on high-value assets and critical teams.")
        elif coverage_pct < 80:
            recommendations.append("🟢 **Medium Priority**: Moderate coverage. Continue systematic scanning of remaining assets.")
        else:
            recommendations.append("✅ **Good**: High coverage. Focus on maintaining regular scan schedules.")
        
        # Team-specific recommendations
        team_coverage = scan_data.get('scan_coverage_by_team', [])
        if team_coverage:
            low_coverage_teams = [team for team, total, scanned, pct in team_coverage if pct < 50]
            if low_coverage_teams:
                recommendations.append(f"🎯 **Team Focus**: Prioritize scanning for teams with low coverage: {', '.join(low_coverage_teams)}")
        
        # Cloud-specific recommendations
        cloud_coverage = scan_data.get('scan_coverage_by_cloud', [])
        if cloud_coverage:
            low_coverage_clouds = [cloud for cloud, total, scanned, pct in cloud_coverage if pct < 50]
            if low_coverage_clouds:
                recommendations.append(f"☁️ **Cloud Focus**: Prioritize scanning for cloud providers with low coverage: {', '.join(low_coverage_clouds)}")
        
        # General recommendations
        recommendations.extend([
            "📅 **Schedule**: Establish regular scan schedules (weekly/monthly) for all assets",
            "🔄 **Automation**: Consider automated scanning tools for continuous coverage",
            "📊 **Monitoring**: Set up alerts for assets that haven't been scanned within expected timeframes",
            "🎯 **Prioritization**: Focus on critical assets and high-risk environments first"
        ])
        
        for i, rec in enumerate(recommendations, 1):
            st.markdown(f"{i}. {rec}")
    
    def get_scan_summary(self) -> Dict[str, Any]:
        """Get a summary of scan coverage for other components"""
        scan_data = self._get_scan_data()
        if not scan_data:
            return {}
        
        total_assets = scan_data.get('total_assets', 0)
        scanned_assets = scan_data.get('scanned_assets', 0)
        coverage_pct = (scanned_assets / total_assets * 100) if total_assets > 0 else 0
        
        return {
            'total_assets': total_assets,
            'scanned_assets': scanned_assets,
            'coverage_percentage': coverage_pct,
            'missing_assets': total_assets - scanned_assets,
            'status': 'good' if coverage_pct >= 80 else 'warning' if coverage_pct >= 50 else 'critical'
        }
