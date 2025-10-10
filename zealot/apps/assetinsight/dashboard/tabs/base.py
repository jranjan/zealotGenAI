"""
Base Tab - Abstract base class for all workflow tabs
Contains common functionality and interface
"""

import streamlit as st
import os
from abc import ABC, abstractmethod


class BaseTab(ABC):
    """Abstract base class for all workflow tabs"""
    
    def __init__(self, tab_name: str, description: str):
        self.tab_name = tab_name
        self.description = description
    
    def render(self, workflow_state):
        """Main render method - template pattern"""
        st.markdown(f"### {self.tab_name}")
        st.markdown(self.description)
        
        # Check prerequisites
        if not self._check_prerequisites(workflow_state):
            self._render_prerequisite_warning()
            return
        
        # Render tab content
        self._render_content(workflow_state)
        
        # Render results if stage is complete
        if self.is_complete(workflow_state):
            self._render_results(workflow_state)
    
    def _check_prerequisites(self, workflow_state):
        """Check if prerequisites are met for this tab"""
        return True  # Override in subclasses
    
    def _render_prerequisite_warning(self):
        """Render warning when prerequisites are not met"""
        st.warning("⚠️ **Prerequisites not met** - Please complete previous stages first")
    
    @abstractmethod
    def _render_content(self, workflow_state):
        """Render the main content of the tab - must be implemented by subclasses"""
        pass
    
    def _render_results(self, workflow_state):
        """Render results when stage is complete - can be overridden by subclasses"""
        st.success(f"✅ **{self.tab_name} Stage Complete**")
    
    @abstractmethod
    def is_complete(self, workflow_state):
        """Check if this stage is complete - must be implemented by subclasses"""
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
        df = pd.DataFrame(data)
        st.dataframe(df, use_container_width=True)
    
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
                            use_container_width=True,
                            key=None):
        """Render action button"""
        st.markdown("**Actions**")
        return st.button(
            button_text, 
            type=button_type, 
            use_container_width=use_container_width,
            key=key
        )
    
    def _render_progress_indicator(self, stage_name, next_stage=None):
        """Render progress indicator"""
        st.markdown("---")
        st.success(f"✅ **{stage_name} Stage Complete**")
        if next_stage:
            st.markdown(f"👉 **Next:** Go to {next_stage} tab")
    
    def _render_configuration_preview(self, config_path):
        """Render configuration file preview"""
        if not config_path or not os.path.exists(config_path):
            return
        
        st.info(f"✅ Configuration file found: `{config_path}`")
        
        with st.expander("📋 View Configuration Content"):
            try:
                with open(config_path, 'r') as f:
                    config_content = f.read()
                st.code(config_content, language="yaml")
            except Exception as e:
                st.error(f"Error reading configuration: {e}")
    
    def _render_file_validation(self, path, path_type="directory"):
        """Render file/directory validation info"""
        if not path:
            st.warning("⚠️ No path provided")
            return
        
        if not os.path.exists(path):
            st.warning(f"⚠️ {path_type.title()} does not exist")
            return
        
        if path_type == "directory" and not os.path.isdir(path):
            st.error(f"❌ Path exists but is not a directory")
            return
        
        if path_type == "file" and not os.path.isfile(path):
            st.error(f"❌ Path exists but is not a file")
            return
        
        st.success(f"✅ {path_type.title()} exists")
        
        if path_type == "directory":
            # Count JSON files
            json_files = list(Path(path).glob("*.json"))
            st.info(f"Found {len(json_files)} JSON files")
            
            if json_files:
                st.write("Sample files:")
                for file_path in json_files[:3]:
                    st.write(f"- {file_path.name}")
                if len(json_files) > 3:
                    st.write(f"... and {len(json_files) - 3} more files")
