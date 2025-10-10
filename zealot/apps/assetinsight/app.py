#!/usr/bin/env python3
"""
Asset Insight Streamlit Dashboard

A comprehensive multi-phase dashboard for asset data processing:
1. Source - Analyze source data directory
2. Transform - Transform/flatten data structures
3. Analyze - Perform asset analysis with configuration
4. Intelligence - Advanced analysis and insights

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
from dashboard.tabs import SourceTab, NormaliserTab, AnalyserTab, IntelligenceTab


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

# Initialize session state
if 'workflow_state' not in st.session_state:
    st.session_state.workflow_state = {
        'source_complete': False,
        'transform_complete': False,
        'analyze_complete': False,
        'intelligence_complete': False,
        'current_phase': 'source'
    }

# Workflow progress indicator
st.markdown('<div class="workflow-container">', unsafe_allow_html=True)
st.markdown("### 📋 Workflow Progress")

phase_steps = [
    ("🔍 Source", "source", st.session_state.workflow_state.get('source_complete', False)),
    ("🔄 Transform", "transform", st.session_state.workflow_state.get('transform_complete', False)),
    ("📊 Analyze", "analyze", st.session_state.workflow_state.get('analyze_complete', False)),
    ("🧠 Intelligence", "intelligence", st.session_state.workflow_state.get('intelligence_complete', False))
]

current_phase = st.session_state.workflow_state.get('current_phase', 'source')

# Create phase indicators
cols = st.columns(4)
for i, (name, phase, completed) in enumerate(phase_steps):
    with cols[i]:
        if phase == current_phase:
            st.markdown(f'<div class="phase-step active"><strong>{name}</strong><br>Current</div>', unsafe_allow_html=True)
        elif completed:
            st.markdown(f'<div class="phase-step completed"><strong>{name}</strong><br>✅ Complete</div>', unsafe_allow_html=True)
        else:
            st.markdown(f'<div class="phase-step"><strong>{name}</strong><br>⏳ Pending</div>', unsafe_allow_html=True)

st.markdown('</div>', unsafe_allow_html=True)

# Main content area with tabs
tab1, tab2, tab3, tab4 = st.tabs(["🔍 Source", "🔄 Transform", "📊 Analyze", "🧠 Intelligence"])

with tab1:
    source_tab = SourceTab()
    source_tab.render(st.session_state.workflow_state)

with tab2:
    normaliser_tab = NormaliserTab()
    normaliser_tab.render(st.session_state.workflow_state)

with tab3:
    analyser_tab = AnalyserTab()
    analyser_tab.render(st.session_state.workflow_state)

with tab4:
    intelligence_tab = IntelligenceTab()
    intelligence_tab.render(st.session_state.workflow_state)

# Footer with copyright and branding (using LLM Studio footer function)
import sys
from pathlib import Path

# Add the utils directory to the path for imports
project_root = Path(__file__).parent.parent.parent.parent.parent.parent.resolve()
sys.path.append(str(project_root / "zealot" / "utils" / "streamlit"))

try:
    from copyright_footer import render_llm_studio_footer
    
    render_llm_studio_footer(
        logo_path=str(project_root / "assets" / "logo.jpg"),
        author_name="Jyoti Ranjan",
        linkedin_url="https://www.linkedin.com/in/jyoti-ranjan-5083595/",
        project_name="Asset Insight Studio"
    )
except ImportError:
    # Fallback to simple footer if import fails
    st.markdown("---")
    st.markdown("**© 2025 [Jyoti Ranjan](https://www.linkedin.com/in/jyoti-ranjan-5083595/). All rights reserved.**")


# This is a Streamlit app - no main function needed
# Run with: streamlit run app.py