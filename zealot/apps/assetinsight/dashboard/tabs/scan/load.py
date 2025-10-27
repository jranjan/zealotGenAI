"""
Scan Load Tab - Handles loading scan data
"""

import streamlit as st
from ..base import BaseTab


class LoadTab(BaseTab):
    """Load tab for scan"""
    
    def __init__(self):
        super().__init__(
            tab_name="Load",
            description="Load scan data into the database"
        )
    
    def _render_content(self):
        """Render scan load interface"""
        st.info("📦 Scan data loading will be configured here.")
