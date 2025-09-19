"""
Example: How to use Streamlit UI utilities in your apps
"""

import streamlit as st
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from zealot.utils.streamlit_ui import (
    StreamlitUI, 
    render_llm_studio_footer, 
    render_app_header
)

def main():
    """Example app showing how to use the UI utilities"""
    
    # Method 1: Use convenience functions
    render_app_header(
        app_name="My Custom App",
        subtitle="A demonstration of reusable UI components"
    )
    
    st.write("This is the main content of your app...")
    
    # Method 2: Use the utility class directly for custom footer
    StreamlitUI.render_footer(
        logo_path="assets/logo.jpg",
        logo_width=80,
        copyright_text="All rights reserved",
        linkedin_url="https://www.linkedin.com/in/jyoti-ranjan-5083595/",
        author_name="Jyoti Ranjan",
        additional_text="This is a custom footer for my app.",
        logo_fallback="🎯"
    )
    
    # Method 3: Use predefined LLM Studio footer
    st.write("---")
    st.write("**Using predefined LLM Studio footer:**")
    render_llm_studio_footer()
    
    # Method 4: Show info boxes
    st.write("---")
    st.write("**Info boxes:**")
    
    StreamlitUI.render_info_box(
        "This is an info message",
        message_type="info",
        icon="ℹ️"
    )
    
    StreamlitUI.render_info_box(
        "This is a success message",
        message_type="success",
        icon="✅"
    )
    
    StreamlitUI.render_info_box(
        "This is a warning message",
        message_type="warning",
        icon="⚠️"
    )
    
    StreamlitUI.render_info_box(
        "This is an error message",
        message_type="error",
        icon="❌"
    )

if __name__ == "__main__":
    main()
