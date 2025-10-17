"""
Streamlit UI Utilities
Reusable components for Streamlit applications
"""

import streamlit as st
from pathlib import Path
from typing import Optional, Dict, Any


class StreamlitUI:
    """Utility class for common Streamlit UI components"""
    
    @staticmethod
    def render_footer(
        logo_path: Optional[str] = None,
        logo_width: int = 100,
        copyright_text: str = "",
        linkedin_url: str = "",
        author_name: str = "",
        additional_text: str = "",
        logo_fallback: str = "🚀"
    ) -> None:
        """
        Render a standardized footer with logo and copyright information
        
        Args:
            logo_path: Path to logo image file
            logo_width: Width of the logo in pixels
            copyright_text: Copyright text (e.g., "All rights reserved")
            linkedin_url: LinkedIn profile URL
            author_name: Author name (will be hyperlinked)
            additional_text: Additional text to display
            logo_fallback: Fallback emoji if logo fails to load
        """
        st.markdown("---")  # Separator line
        
        # Main footer container with centered content
        col1, col2, col3 = st.columns([1, 6, 1])
        
        with col1:
            st.write("")  # Empty column for spacing
        
        with col2:
            # Logo and copyright in the same row
            col_logo, col_text = st.columns([1, 4])
            
            with col_logo:
                # Display logo or fallback
                if logo_path and Path(logo_path).exists():
                    try:
                        # Convert absolute path to relative path for Streamlit
                        logo_path_str = str(logo_path)
                        if logo_path_str.startswith('/'):
                            # Try to find a relative path that works
                            cwd = Path.cwd()
                            try:
                                relative_path = Path(logo_path).relative_to(cwd)
                                st.image(str(relative_path), width=logo_width)
                            except ValueError:
                                # If we can't make it relative, try the absolute path
                                st.image(logo_path_str, width=logo_width)
                        else:
                            st.image(logo_path_str, width=logo_width)
                    except Exception as e:
                        st.write(logo_fallback)
                else:
                    st.write(logo_fallback)
            
            with col_text:
                # Copyright text with hyperlink
                if author_name and linkedin_url:
                    author_link = f"[{author_name}]({linkedin_url})"
                else:
                    author_link = author_name or "Author"
                
                footer_text = f"**© 2025 {author_link}. {copyright_text}**"
                if additional_text:
                    footer_text += f"  \n{additional_text}"
                
                st.markdown(footer_text)
        
        with col3:
            st.write("")  # Empty column for spacing
    
    @staticmethod
    def render_header(
        title: str,
        subtitle: str = "",
        icon: str = "🤖",
        gradient: bool = True
    ) -> None:
        """
        Render a standardized header with title and subtitle
        
        Args:
            title: Main title text
            subtitle: Subtitle text (optional)
            icon: Icon to display next to title
            gradient: Whether to use gradient background
        """
        if gradient:
            st.markdown(f"""
            <div style='text-align: center; margin: 30px 0; padding: 20px; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); border-radius: 15px; color: white;'>
                <h1 style='margin: 0; font-size: 3rem; font-weight: 700; display: flex; align-items: center; justify-content: center; gap: 15px;'>
                    <span style='font-size: 3.5rem;'>{icon}</span>
                    <span>{title}</span>
                </h1>
                <p style='margin: 0; font-size: 1.2rem; opacity: 0.9; font-weight: 300;'>{subtitle}</p>
            </div>
            """, unsafe_allow_html=True)
        else:
            st.title(f"{icon} {title}")
            if subtitle:
                st.markdown(f"*{subtitle}*")
    
    @staticmethod
    def render_centered_content(
        content: str,
        title: str = "",
        icon: str = "ℹ️"
    ) -> None:
        """
        Render centered content with optional title and icon
        
        Args:
            content: Content to display
            title: Optional title
            icon: Optional icon
        """
        col1, col2, col3 = st.columns([1, 6, 1])
        
        with col2:
            if title:
                st.markdown(f"### {icon} {title}")
            st.markdown(content)
    
    @staticmethod
    def render_info_box(
        message: str,
        message_type: str = "info",
        icon: str = "ℹ️"
    ) -> None:
        """
        Render an info box with different styles
        
        Args:
            message: Message to display
            message_type: Type of message (info, success, warning, error)
            icon: Icon to display
        """
        type_styles = {
            "info": "background-color: #e7f3ff; border-left: 4px solid #2196F3; color: #0d47a1;",
            "success": "background-color: #e8f5e8; border-left: 4px solid #4CAF50; color: #1b5e20;",
            "warning": "background-color: #fff3e0; border-left: 4px solid #FF9800; color: #e65100;",
            "error": "background-color: #ffebee; border-left: 4px solid #f44336; color: #c62828;"
        }
        
        style = type_styles.get(message_type, type_styles["info"])
        
        st.markdown(f"""
        <div style='padding: 16px; margin: 16px 0; border-radius: 4px; {style}'>
            <strong>{icon} {message_type.title()}:</strong> {message}
        </div>
        """, unsafe_allow_html=True)


# Convenience functions for common use cases
def render_llm_studio_footer(
    logo_path: str = None,
    author_name: str = "Jyoti Ranjan",
    linkedin_url: str = "https://www.linkedin.com/in/jyoti-ranjan-5083595/",
    project_name: str = "LLM Studio"
) -> None:
    """
    Render a footer specifically designed for LLM Studio apps
    
    Args:
        logo_path: Path to logo image (if None, will try to find assets/images/logo.jpg)
        author_name: Author name
        linkedin_url: LinkedIn profile URL
        project_name: Name of the project
    """
    # Auto-detect logo path if not provided
    if logo_path is None:
        possible_paths = [
            "assets/images/logo.jpg",
            "../assets/images/logo.jpg",
            "../../assets/images/logo.jpg",
            "../../../assets/images/logo.jpg",
            "../../../../assets/images/logo.jpg"
        ]
        for path in possible_paths:
            if Path(path).exists():
                logo_path = path
                break
        else:
            logo_path = None  # No logo found
    
    additional_text = (
        f"This work reflects my personal AI learning journey and is shared for educational and knowledge-building purposes. "
        f"While unauthorized reproduction, modification, or commercial use without prior written consent is strictly prohibited, "
        f"I warmly welcome discussions, feedback, and collaborative learning exchanges."
    )
    
    StreamlitUI.render_footer(
        logo_path=logo_path,
        logo_width=100,
        copyright_text="All rights reserved",
        linkedin_url=linkedin_url,
        author_name=author_name,
        additional_text=additional_text,
        logo_fallback="🚀"
    )


def render_app_header(
    app_name: str = "LLM Studio",
    subtitle: str = "Interactive interface for testing different LLM providers and models"
) -> None:
    """
    Render a header specifically designed for LLM apps
    
    Args:
        app_name: Name of the application
        subtitle: Subtitle text
    """
    StreamlitUI.render_header(
        title=app_name,
        subtitle=subtitle,
        icon="🤖",
        gradient=True
    )
