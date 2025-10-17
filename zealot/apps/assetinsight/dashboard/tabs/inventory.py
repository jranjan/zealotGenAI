"""
Inventory Tab - Main tab containing all asset management operations
"""

import streamlit as st
from typing import Dict, Any
import sys
from pathlib import Path

# Add the current directory to Python path
sys.path.append(str(Path(__file__).parent.parent.parent))

from dashboard.tabs.base import BaseTab
from dashboard.tabs.source import SourceTab
from dashboard.tabs.normaliser import NormaliserTab
from dashboard.tabs.load import LoadTab
from dashboard.tabs.analysis.ownership import OwnershipAnalyserTab


class InventoryTab(BaseTab):
    """Main inventory tab containing all asset management operations"""
    
    def __init__(self):
        super().__init__(
            tab_name="Inventory",
            description="Complete asset inventory management workflow"
        )
        
        # Initialize sub-tabs
        self.source_tab = SourceTab()
        self.normaliser_tab = NormaliserTab()
        self.load_tab = LoadTab()
        self.ownership_tab = OwnershipAnalyserTab()
    
    def _render_content(self):
        """Render inventory management interface"""
        st.markdown("""
        <div style='background-color: #1f77b4; color: white; padding: 10px 15px; border-radius: 5px; margin-bottom: 20px; text-align: left; font-weight: bold;'>
            
        </div>
        """, unsafe_allow_html=True)
        
        # Create sub-tabs within the inventory tab
        tab1, tab2, tab3, tab4 = st.tabs(["📁 Source", "🔧 Normalise", "🗄️ Load", "👥 Ownership"])
        
        with tab1:
            st.markdown("### Source Data Management")
            self.source_tab._render_content()
        
        with tab2:
            st.markdown("### Data Normalization")
            self.normaliser_tab._render_content()
        
        with tab3:
            st.markdown("### Database Loading")
            self.load_tab._render_content()
        
        with tab4:
            st.markdown("### Ownership Analysis")
            self.ownership_tab._render_content()
    
    def get_workflow_status(self) -> Dict[str, Any]:
        """Get the current status of the inventory workflow"""
        return {
            'source_complete': st.session_state.get('source_complete', False),
            'normaliser_complete': st.session_state.get('normaliser_complete', False),
            'database_ready': st.session_state.get('database_ready', False),
            'ownership_complete': st.session_state.get('ownership_complete', False)
        }
