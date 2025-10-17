#!/usr/bin/env python3
"""
Asset Insight Streamlit Dashboard

A complete 3-phase dashboard for asset data processing:
1. Source - Select and analyze raw data directory
2. Transform - Transform/flatten data and create DuckDB database
3. Ownership - Analyze asset ownership with interactive charts

Usage:
  streamlit run app.py
"""

import streamlit as st
import sys
import warnings
import logging
from pathlib import Path

# Suppress Streamlit warnings about missing ScriptRunContext in multiprocessing
warnings.filterwarnings("ignore", message=".*missing ScriptRunContext.*")
logging.getLogger("streamlit.runtime.scriptrunner_utils.script_run_context").setLevel(logging.ERROR)

# Add the current directory to Python path
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

# Import dashboard components with error handling
try:
    from dashboard.tabs import SourceTab, NormaliserTab
    from dashboard.tabs.load import LoadTab
    from dashboard.tabs.analysis.ownership import OwnershipAnalyserTab
except ImportError as e:
    st.error(f"Failed to import dashboard components: {str(e)}")
    st.stop()

# Import database components
try:
    from database.reader.factory import ReaderFactory, ReaderType
except ImportError as e:
    st.error(f"Failed to import database components: {str(e)}")
    st.stop()


# Page configuration
st.set_page_config(
    page_title="Asset Insight Studio",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize database at app startup
@st.cache_resource
def initialize_database():
    """Initialize database connection and create tables at app startup"""
    try:
        # Create a default database instance with connection and tables
        # This will be reused throughout the app lifecycle
        # Use in-memory database for better performance
        temp_folder = Path.cwd() / "temp_memory_db"
        temp_folder.mkdir(exist_ok=True)
        
        # Get the singleton database instance through factory
        db_instance = ReaderFactory.create_reader(folder_path=str(temp_folder), reader_type=ReaderType.MEMORY_SONIC)
        
        # Create tables (schema only, no data loaded yet)
        table_result = db_instance.create_tables()
        if not table_result['success']:
            st.warning(f"Table creation warning: {table_result['message']}")
        
        return db_instance
    except Exception as e:
        st.error(f"Failed to initialize database: {str(e)}")
        return None

# Initialize database only when needed (in Load tab)
# Don't create database instance globally - let Load tab handle it
# Database will be created when Load tab is accessed

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .workflow-container {
        background-color: #f8f9fa;
        padding: 2rem;
        border-radius: 1rem;
        margin: 1rem 0;
    }
    .phase-indicator {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin: 2rem 0;
    }
    .phase-step {
        flex: 1;
        text-align: center;
        padding: 1rem;
        margin: 0 0.5rem;
        border-radius: 0.5rem;
        background-color: #e9ecef;
        border: 2px solid #dee2e6;
    }
    .phase-step.active {
        background-color: #d4edda;
        border-color: #28a745;
    }
    .phase-step.completed {
        background-color: #cce5ff;
        border-color: #007bff;
    }
</style>
""", unsafe_allow_html=True)

# Main header using LLM Studio style
st.markdown("""
<div style='text-align: center; margin: 30px 0; padding: 20px; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); border-radius: 15px; color: white;'>
    <h1 style='margin: 0; font-size: 3rem; font-weight: 700; display: flex; align-items: center; justify-content: center; gap: 15px;'>
        <span style='font-size: 3.5rem;'>🤖</span>
        <span>Asset Insight Studio</span>
    </h1>
</div>
""", unsafe_allow_html=True)

# Main content area with two main tabs
main_tab1, main_tab2 = st.tabs(["📦 Inventory", "🔍 Scan Coverage"])

# Inventory tab with sub-tabs
with main_tab1:
    st.markdown("### Asset Inventory Management")
    st.markdown("Manage and analyze your asset inventory through the complete data pipeline.")
    
    # Sub-tabs within Inventory
    inv_tab1, inv_tab2, inv_tab3, inv_tab4 = st.tabs(["Source", "Normalise", "Load", "Ownership"])
    
    with inv_tab1:
        try:
            source_tab = SourceTab()
            source_tab.render()
        except Exception as e:
            st.error(f"Error in Source tab: {str(e)}")
            st.exception(e)

    with inv_tab2:
        try:
            normaliser_tab = NormaliserTab()
            normaliser_tab.render()
        except Exception as e:
            st.error(f"Error in Normalise tab: {str(e)}")
            st.exception(e)

    with inv_tab3:
        try:
            load_tab = LoadTab()
            load_tab.render()
        except Exception as e:
            st.error(f"Error in Load tab: {str(e)}")
            st.exception(e)

    with inv_tab4:
        try:
            ownership_tab = OwnershipAnalyserTab()
            ownership_tab.render()
        except Exception as e:
            st.error(f"Error in Ownership tab: {str(e)}")
            st.exception(e)

# Scan Coverage tab
with main_tab2:
    st.markdown("### Scan Coverage Analysis")
    st.markdown("Analyze scan coverage, vulnerability assessment, and security posture across your asset inventory.")
    
    # Placeholder content for Scan Coverage tab
    st.info("🚧 Scan Coverage functionality coming soon!")
    
    # Add some placeholder content
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Total Scans", "1,234", "12%")
    
    with col2:
        st.metric("Coverage %", "87.5%", "5.2%")
    
    with col3:
        st.metric("Vulnerabilities", "45", "-8")
    
    st.markdown("---")
    
    # Placeholder for future scan coverage features
    st.markdown("#### Planned Features:")
    st.markdown("""
    - **Vulnerability Assessment**: Track security vulnerabilities across assets
    - **Scan Status Monitoring**: Real-time scan progress and completion status
    - **Coverage Mapping**: Visual representation of scan coverage by asset type
    - **Compliance Reporting**: Generate compliance reports for security standards
    - **Risk Analysis**: Identify high-risk assets and security gaps
    - **Scan Scheduling**: Manage automated scan schedules and triggers
    """)
    
    # Placeholder chart
    import plotly.express as px
    import pandas as pd
    
    # Sample data for demonstration
    scan_data = pd.DataFrame({
        'Asset Type': ['Servers', 'Workstations', 'Network Devices', 'Mobile Devices', 'IoT Devices'],
        'Total Assets': [450, 1200, 85, 300, 150],
        'Scanned': [420, 1100, 80, 250, 120],
        'Coverage %': [93.3, 91.7, 94.1, 83.3, 80.0]
    })
    
    fig = px.bar(
        scan_data, 
        x='Asset Type', 
        y='Coverage %',
        title="Scan Coverage by Asset Type",
        color='Coverage %',
        color_continuous_scale='RdYlGn'
    )
    fig.update_layout(height=400)
    st.plotly_chart(fig, use_container_width=True)

# Footer with copyright and branding (using StreamlitUI)
# Add the utils directory to the path for imports
utils_path = Path(__file__).parent.parent.parent.parent / "zealot" / "utils" / "streamlit"

# Only add to path if it exists
if utils_path.exists():
    sys.path.insert(0, str(utils_path.resolve()))
else:
    # Fallback: try relative path from current working directory
    fallback_path = Path("../../../zealot/utils/streamlit").resolve()
    if fallback_path.exists():
        sys.path.insert(0, str(fallback_path))
        utils_path = fallback_path

try:
    from copyright_footer import StreamlitUI
    
    # Check for logo file, use fallback if not found
    logo_path = Path(__file__).parent.parent.parent.parent / "assets" / "logo.jpg"
    if not logo_path.exists():
        logo_path = None
    
    StreamlitUI.render_footer(
        logo_path=str(logo_path) if logo_path else None,
        logo_width=100,
        copyright_text="All rights reserved",
        linkedin_url="https://www.linkedin.com/in/jyoti-ranjan-5083595/",
        author_name="Jyoti Ranjan",
        additional_text="This work reflects my personal AI learning journey and is shared for educational and knowledge-building purposes. While unauthorized reproduction, modification, or commercial use without prior written consent is strictly prohibited, I warmly welcome discussions, feedback, and collaborative learning exchanges.",
        logo_fallback="🚀"
    )
except ImportError as e:
    # Fallback to simple footer if import fails
    st.markdown("---")
    st.markdown("**🚀 © 2025 [Jyoti Ranjan](https://www.linkedin.com/in/jyoti-ranjan-5083595/). All rights reserved.**")
    # Log the import error for debugging
    st.error(f"Footer import failed: {str(e)}")
except Exception as e:
    # Handle any other footer errors
    st.markdown("---")
    st.markdown("**🚀 © 2025 [Jyoti Ranjan](https://www.linkedin.com/in/jyoti-ranjan-5083595/). All rights reserved.**")
    st.error(f"Footer rendering failed: {str(e)}")


# This is a Streamlit app - no main function needed
# Run with: streamlit run app.py