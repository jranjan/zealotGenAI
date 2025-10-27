"""
Scan Tab - Main composite tab for scan management
"""

import streamlit as st
from ..base import BaseTab
from .source import SourceTab
from .coverage import CoverageTab
from .load import LoadTab


class ScanTab(BaseTab):
    """Main scan tab containing all scan-related operations"""
    
    def __init__(self):
        super().__init__(
            tab_name="Scan",
            description="Measure scan coverage and analyze scanning patterns"
        )
        
        # Initialize sub-tabs
        self.source_tab = SourceTab()
        self.coverage_tab = CoverageTab()
        self.load_tab = LoadTab()
    
    def _render_content(self):
        """Render scan management interface"""
        # Create sub-tabs within the scan tab
        tab1, tab2, tab3 = st.tabs(["📁 Source", "📦 Load", "📋 Coverage"])
        
        with tab1:
            self.source_tab._render_content()
        
        with tab2:
            self.load_tab._render_content()
        
        with tab3:
            self.coverage_tab._render_content()
