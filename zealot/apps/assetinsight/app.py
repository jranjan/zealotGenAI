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
from pathlib import Path

# Add the current directory to Python path
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

# Import dashboard components
from dashboard.tabs import SourceTab, NormaliserTab
from dashboard.tabs.analysis.ownership import OwnershipAnalyserTab


# Page configuration
st.set_page_config(
    page_title="Asset Insight Studio",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded"
)

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

# Main content area with tabs
tab1, tab2, tab3 = st.tabs(["Source", "Transform", "Ownership"])

with tab1:
    source_tab = SourceTab()
    source_tab.render()

with tab2:
    normaliser_tab = NormaliserTab()
    normaliser_tab.render()

with tab3:
    ownership_tab = OwnershipAnalyserTab()
    ownership_tab.render()

# Footer with copyright and branding (using StreamlitUI)
import sys
from pathlib import Path

# Add the utils directory to the path for imports
project_root = Path(__file__).parent.parent.parent.parent.parent.parent.resolve()
sys.path.append(str(project_root / "zealot" / "utils" / "streamlit"))

try:
    from copyright_footer import StreamlitUI
    
    StreamlitUI.render_footer(
        logo_path=str(project_root / "assets" / "logo.jpg"),
        logo_width=100,
        copyright_text="All rights reserved",
        linkedin_url="https://www.linkedin.com/in/jyoti-ranjan-5083595/",
        author_name="Jyoti Ranjan",
        additional_text="This work reflects my personal AI learning journey and is shared for educational and knowledge-building purposes. While unauthorized reproduction, modification, or commercial use without prior written consent is strictly prohibited, I warmly welcome discussions, feedback, and collaborative learning exchanges.",
        logo_fallback="🚀"
    )
except ImportError:
    # Fallback to simple footer if import fails
    st.markdown("---")
    st.markdown("**🚀 © 2025 [Jyoti Ranjan](https://www.linkedin.com/in/jyoti-ranjan-5083595/). All rights reserved.**")


# This is a Streamlit app - no main function needed
# Run with: streamlit run app.py