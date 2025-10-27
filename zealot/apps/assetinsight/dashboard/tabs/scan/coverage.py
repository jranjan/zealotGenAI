"""
Scan Coverage Tab - Displays scan metrics and coverage analysis
"""

import streamlit as st
from ..base import BaseTab


class CoverageTab(BaseTab):
    """Coverage tab for scan"""
    
    def __init__(self):
        super().__init__(
            tab_name="Coverage",
            description="View scan coverage and subject analysis"
        )
    
    def _render_content(self):
        """Render scan coverage interface"""
        st.info("📊 Scan coverage analysis will be displayed here.")

