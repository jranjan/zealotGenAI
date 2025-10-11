"""
Base Tab - Simple base class for all tabs
Contains common utility methods
"""

import streamlit as st
from abc import ABC, abstractmethod


class BaseTab(ABC):
    """Simple base class for all tabs"""
    
    def __init__(self, tab_name: str, description: str):
        self.tab_name = tab_name
        self.description = description
    
    def render(self):
        """Main render method - simple and clean"""
        if self.description:
            st.markdown(self.description)
        self._render_content()
    
    @abstractmethod
    def _render_content(self):
        """Render the main content of the tab - must be implemented by subclasses"""
        pass
    
    def _render_metrics(self, metrics):
        """Render metrics in columns"""
        if not metrics:
            return
        
        cols = st.columns(len(metrics))
        for i, (label, value) in enumerate(metrics.items()):
            with cols[i]:
                st.metric(label, value)
    
    def _render_dataframe(self, data, title=None):
        """Render a dataframe with optional title"""
        if data is None or len(data) == 0:
            return
        
        if title:
            st.markdown(f"### {title}")
        
        import pandas as pd
        from utils.dataframe_utils import safe_dataframe
        
        df = safe_dataframe(data)
        
        st.dataframe(
            df, 
            width='stretch',
            column_config={
                col: st.column_config.TextColumn(
                    col,
                    help=f"Shows {col.lower()} information"
                )
                for col in df.columns
            }
        )
    
    def _render_success_message(self, message, next_step=None):
        """Render success message with optional next step"""
        st.success(f"✅ {message}")
        if next_step:
            st.markdown(f"👉 **Next:** {next_step}")
    
    def _render_error_message(self, error):
        """Render error message"""
        st.error(f"❌ {error}")
    
    def _render_warning_message(self, warning):
        """Render warning message"""
        st.warning(f"⚠️ {warning}")
    
    def _render_info_message(self, info):
        """Render info message"""
        st.info(f"ℹ️ {info}")
    
    def _get_session_state_value(self, key, default=None):
        """Get value from session state with default"""
        return st.session_state.get(key, default)
    
    def _set_session_state_value(self, key, value):
        """Set value in session state"""
        st.session_state[key] = value
    
    def _update_workflow_state(self, updates):
        """Update workflow state with given updates"""
        if 'workflow_state' not in st.session_state:
            st.session_state.workflow_state = {}
        
        for key, value in updates.items():
            st.session_state.workflow_state[key] = value
    
    def _get_workflow_state_value(self, key, default=None):
        """Get value from workflow state with default"""
        workflow_state = st.session_state.get('workflow_state', {})
        return workflow_state.get(key, default)
    
    def _render_file_selection(self, 
                              label, 
                              common_paths, 
                              session_key_prefix,
                              help_text=None,
                              default_value="/path/to/input"):
        """Render file/folder selection UI"""
        st.markdown(f"**{label}**")
        
        option_key = f"{session_key_prefix}_option"
        custom_key = f"{session_key_prefix}_custom"
        
        option = st.selectbox(
            "Choose a common path or enter custom:",
            ["Custom path..."] + common_paths,
            key=option_key
        )
        
        if option == "Custom path...":
            path = st.text_input(
                "Enter path:",
                value=default_value,
                help=help_text,
                key=custom_key
            )
        else:
            path = option
            st.success(f"Selected: {path}")
        
        return path
    
    def _render_action_button(self, 
                            button_text, 
                            button_type="primary",
                            width='stretch',
                            key=None):
        """Render action button"""
        st.markdown("**Actions**")
        return st.button(
            button_text, 
            type=button_type, 
            width=width,
            key=key
        )
    
    def _render_progress_indicator(self, stage_name, next_stage=None):
        """Render progress indicator"""
        st.markdown("---")
        st.success(f"✅ **{stage_name} Stage Complete**")
        if next_stage:
            st.markdown(f"👉 **Next:** Go to {next_stage} tab")
    
