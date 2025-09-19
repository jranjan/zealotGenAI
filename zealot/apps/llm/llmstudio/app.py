"""
LLM Studio Streamlit App
Interactive UI for testing different LLM providers and models
"""

import os
import streamlit as st
import sys
from pathlib import Path
from typing import Dict, Any, Optional, Tuple
from dataclasses import dataclass

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from zealot.utils.llm_client import LLMProvider, create_llm_client
from zealot.utils.llm_client.clients.openrouter import OpenRouterModel
from zealot.apps.common.app.llm import LLMApp
from zealot.utils.streamlit.copyright_footer import render_llm_studio_footer, render_app_header


@dataclass
class AppConfig:
    """Configuration for the LLM Studio app"""
    page_title: str = "LLM Studio"
    page_icon: str = "🤖"
    layout: str = "wide"
    sidebar_state: str = "expanded"


@dataclass
class ModelConfig:
    """Configuration for model parameters"""
    provider: LLMProvider
    model: str
    temperature: float
    max_tokens: int
    top_k: int
    top_p: float
    frequency_penalty: float
    presence_penalty: float
    system_prompt: str
    api_key: str


class LLMStudioApp(LLMApp):
    """Main application class for LLM Studio"""
    
    def __init__(self):
        # Initialize with OpenRouter provider
        super().__init__(LLMProvider.OPENROUTER, "LLM Studio")
        self.setup_page_config()
        self.provider_options = self._get_provider_options()
        self.default_models = self._get_default_models()
    
    def setup_page_config(self):
        """Setup Streamlit page configuration"""
        st.set_page_config(
            page_title=AppConfig.page_title,
            page_icon=AppConfig.page_icon,
            layout=AppConfig.layout,
            initial_sidebar_state=AppConfig.sidebar_state
        )
    
    def _get_provider_options(self) -> Dict[str, LLMProvider]:
        """Get available provider options"""
        return {
            "Cohere": LLMProvider.COHERE,
            "OpenAI": LLMProvider.OPENAI,
            "Anthropic": LLMProvider.ANTHROPIC,
            "Google": LLMProvider.GOOGLE,
            "OpenRouter": LLMProvider.OPENROUTER
        }
    
    def _get_default_models(self) -> Dict[LLMProvider, str]:
        """Get default models for each provider"""
        return {
            LLMProvider.COHERE: "command",
            LLMProvider.OPENAI: "gpt-4o",
            LLMProvider.ANTHROPIC: "claude-3-5-sonnet-20241022",
            LLMProvider.GOOGLE: "gemini-pro"
        }
    
    def render_header(self):
        """Render the app header"""
        render_app_header(
            app_name="LLM Studio",
            subtitle="Interactive interface for testing different LLM providers and models"
        )
    
    def render_sidebar(self) -> ModelConfig:
        """Render the sidebar configuration"""
        with st.sidebar:
            st.header("⚙️ Configuration")
            
            # Always use OpenRouter
            selected_provider = LLMProvider.OPENROUTER
            selected_provider_name = "OpenRouter"
            
            # Show provider info
            st.info(f"🤖 Provider: {selected_provider_name}")
            
            # Model selection
            selected_model = self._render_model_selection(selected_provider)
            
            # Model parameters
            temperature, max_tokens, top_k, top_p, frequency_penalty, presence_penalty = self._render_model_parameters(selected_provider, selected_model)
            
            # Additional parameters
            system_prompt = self._render_additional_parameters()
            
            # Check API key from environment
            api_key = self._check_api_key()
            
            return ModelConfig(
                provider=selected_provider,
                model=selected_model,
                temperature=temperature,
                max_tokens=max_tokens,
                top_k=top_k,
                top_p=top_p,
                frequency_penalty=frequency_penalty,
                presence_penalty=presence_penalty,
                system_prompt=system_prompt,
                api_key=api_key
            )
    
    def _render_model_selection(self, provider: LLMProvider) -> str:
        """Render model selection for OpenRouter"""
        return self._render_openrouter_model_selection()
    
    def _render_openrouter_model_selection(self) -> str:
        """Render OpenRouter model selection"""
        try:
            all_models = OpenRouterModel.get_all_models()
            model_options = [model.value for model in all_models]
            return st.selectbox("Model", options=model_options, index=0)
        except Exception as e:
            st.error(f"Error loading OpenRouter models: {e}")
            return "openai/gpt-4o"
    
    def _render_model_parameters(self, provider: LLMProvider, model: str) -> Tuple[float, int, int, float, float, float]:
        """Render model parameter controls with OpenRouter model-specific limits"""
        st.subheader("🔧 Model Parameters")
        
        # Get OpenRouter model-specific limits and defaults
        try:
            model_enum = OpenRouterModel.from_string(model)
            token_info = model_enum.get_token_info()
            max_output_tokens = token_info['max_output_tokens']
            default_tokens = min(1000, max_output_tokens)
        except Exception:
            max_output_tokens = 100000
            default_tokens = 1000
        
        # Basic parameters - displayed vertically
        temperature = st.slider(
            "Temperature",
            min_value=0.0,
            max_value=2.0,
            value=0.7,
            step=0.1,
            help="Controls randomness in the response. Lower = more focused, Higher = more creative"
        )
        
        max_tokens = st.number_input(
            "Max Tokens",
            min_value=1,
            max_value=max_output_tokens,
            value=default_tokens,
            step=100,
            help=f"Maximum number of tokens to generate (Max: {max_output_tokens:,})"
        )
        
        # Advanced parameters
        st.subheader("🎛️ Advanced Parameters")
        
        # All parameters displayed vertically
        top_k = st.slider(
            "Top-K",
            min_value=1,
            max_value=100,
            value=40,
            step=1,
            help="Defines a fixed shortlist of the most likely tokens, preventing the model from wandering too far into low-probability options"
        )
        
        top_p = st.slider(
            "Top-P (Nucleus Sampling)",
            min_value=0.0,
            max_value=1.0,
            value=0.9,
            step=0.01,
            help="Creates a probability-based shortlist that adapts dynamically to context, ensuring the model considers just enough options to stay fluent while remaining flexible"
        )
        
        frequency_penalty = st.slider(
            "Frequency Penalty",
            min_value=-2.0,
            max_value=2.0,
            value=0.0,
            step=0.1,
            help="Helps avoid redundancy by discouraging the model from repeating the same words too often"
        )
        
        presence_penalty = st.slider(
            "Presence Penalty",
            min_value=-2.0,
            max_value=2.0,
            value=0.0,
            step=0.1,
            help="Pushes for novelty, encouraging the model to introduce words or ideas that haven't appeared yet in the text"
        )
        
        return temperature, max_tokens, top_k, top_p, frequency_penalty, presence_penalty
    
    def _render_additional_parameters(self) -> str:
        """Render additional parameter controls"""
        st.subheader("📝 Additional Parameters")
        
        return st.text_area(
            "System Prompt (Optional)",
            value="",
            height=100,
            help="Optional system prompt to set the context"
        )
    
    def _check_api_key(self) -> str:
        """Check for OpenRouter API key in environment variables"""
        api_key_env = "OPENROUTER_API_KEY"
        api_key = os.getenv(api_key_env)
        
        if api_key:
            st.success(f"✅ {api_key_env} found")
        else:
            st.error(f"❌ {api_key_env} not found in environment variables")
            st.info("Please set your OpenRouter API key:\n```bash\nexport OPENROUTER_API_KEY='your-api-key-here'\n```")
        
        return api_key or ""
    
    def render_main_content(self, config: ModelConfig):
        """Render the main content area"""
        col1, col2 = st.columns([2, 1])
        
        with col1:
            self._render_chat_interface(config)
        
        with col2:
            self._render_model_info(config)
            self._render_configuration_summary(config)
    
    def _render_chat_interface(self, config: ModelConfig):
        """Render the chat interface"""
        st.subheader("💬 Chat Interface")
        
        user_input = st.text_area(
            "Ask",
            value="",
            height=150,
            placeholder="Enter your question or prompt here...",
            help="Type your question or prompt for the LLM"
        )
        
        if st.button("🚀 Generate Response", type="primary", use_container_width=True):
            self._handle_generation(config, user_input)
    
    def _handle_generation(self, config: ModelConfig, user_input: str):
        """Handle response generation"""
        if not user_input.strip():
            st.error("Please enter a question or prompt")
        elif not config.api_key:
            st.error("Please set your OPENROUTER_API_KEY environment variable")
        else:
            self._generate_response(config, user_input)
    
    def _generate_response(self, config: ModelConfig, user_input: str):
        """Generate response from the selected LLM"""
        try:
            with st.spinner("🤖 Generating response..."):
                # Use inherited client from LLMApp
                client = self._get_client()
                
                # Prepare prompt
                full_prompt = self._prepare_prompt(user_input, config.system_prompt)
                
                # Generate response using the base class method with additional parameters
                response = client.generate_text(
                    full_prompt,
                    model=config.model,
                    temperature=config.temperature,
                    max_tokens=config.max_tokens,
                    top_k=config.top_k,
                    top_p=config.top_p,
                    frequency_penalty=config.frequency_penalty,
                    presence_penalty=config.presence_penalty
                )
            
            self._display_response(response, config)
            
        except Exception as e:
            self._display_error(e)
    
    def _prepare_prompt(self, user_input: str, system_prompt: str) -> str:
        """Prepare the full prompt with system message if provided"""
        if system_prompt:
            return f"System: {system_prompt}\n\nUser: {user_input}"
        return user_input
    
    def _display_response(self, response: str, config: ModelConfig):
        """Display the generated response"""
        st.subheader("📤 Response")
        st.write(response)
        st.success(f"✅ Response generated successfully using OpenRouter ({config.model})")
    
    def _display_error(self, error: Exception):
        """Display error information"""
        st.error(f"❌ Error generating response: {str(error)}")
        
        with st.expander("🔍 Error Details"):
            st.code(str(error))
    
    def _render_model_info(self, config: ModelConfig):
        """Render OpenRouter model information panel"""
        st.subheader("📊 Model Info")
        
        try:
            self._render_openrouter_model_info(config.model)
        except Exception as e:
            st.error(f"Error loading model info: {e}")
    
    def _render_openrouter_model_info(self, model: str):
        """Render OpenRouter model information"""
        model_enum = OpenRouterModel.from_string(model)
        info = model_enum.get_token_info()
        
        st.metric("Provider", info['provider'].title())
        st.metric("Model", info['model'])
        st.metric("Max Input Tokens", info['max_input_tokens_formatted'])
        st.metric("Max Output Tokens", info['max_output_tokens_formatted'])
        st.metric("Max Total Tokens", info['max_total_tokens_formatted'])
    
    
    def _render_configuration_summary(self, config: ModelConfig):
        """Render configuration summary"""
        st.subheader("⚙️ Current Settings")
        
        summary = {
            "Provider": "OpenRouter",
            "Model": config.model,
            "Temperature": config.temperature,
            "Max Tokens": config.max_tokens,
            "Top-K": config.top_k,
            "Top-P": config.top_p,
            "Frequency Penalty": config.frequency_penalty,
            "Presence Penalty": config.presence_penalty,
            "System Prompt": config.system_prompt[:50] + "..." if len(config.system_prompt) > 50 else config.system_prompt
        }
        
        st.json(summary)
    
    def render_footer(self):
        """Render the app footer with copyright notice and logo"""
        render_llm_studio_footer(
            logo_path=str(project_root / "assets" / "logo.jpg"),
            author_name="Jyoti Ranjan",
            linkedin_url="https://www.linkedin.com/in/jyoti-ranjan-5083595/",
            project_name="LLM Studio"
        )

    def run(self):
        """Run the main application"""
        self.render_header()
        config = self.render_sidebar()
        self.render_main_content(config)
        self.render_footer()

def main():
    """Main entry point"""
    app = LLMStudioApp()
    app.run()


if __name__ == "__main__":
    main()

