"""
LLM Studio Streamlit App
Interactive UI for testing different LLM providers and models using minimal LLMClient
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

# Add specific directories to Python path
llm_path = os.path.join(os.path.dirname(__file__), '..', '..', 'common', 'clients', 'llm')
catalog_path = os.path.join(os.path.dirname(__file__), '..', '..', 'common', 'catalog', 'llm')
app_path = os.path.join(os.path.dirname(__file__), '..', '..', 'common', 'app', 'llm')
streamlit_path = os.path.join(os.path.dirname(__file__), '..', '..', 'utils', 'streamlit')
prompt_path = os.path.join(os.path.dirname(__file__), '..', '..', 'utils', 'prompt')
sys.path.insert(0, llm_path)
sys.path.insert(0, catalog_path)
sys.path.insert(0, app_path)
sys.path.insert(0, streamlit_path)
sys.path.insert(0, prompt_path)

# Direct imports
from client import LLMClient, ProviderInfo
from factory import LLMClientFactory
from catalog import LLMModelCatalog
from llm import LLMApp
from copyright_footer import render_llm_studio_footer, render_app_header
from custom import SystemPrompt


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
    provider: str
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
        super().__init__("LLM Studio")
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
    
    def _get_provider_options(self) -> Dict[str, str]:
        """Get available provider options"""
        return {
            "OpenRouter": "openrouter"
        }
    
    def _get_default_models(self) -> Dict[str, str]:
        """Get default models for each provider"""
        return {
            "openrouter": "openai/gpt-4o"
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
            selected_provider = "openrouter"
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
            api_key = self._check_api_key(selected_provider)
            
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
    
    def _render_model_selection(self, provider: str) -> str:
        """Render model selection for OpenRouter"""
        return self._render_openrouter_model_selection()
    
    def _render_openrouter_model_selection(self) -> str:
        """Render OpenRouter model selection using catalog"""
        try:
            all_models = LLMModelCatalog.get_all_models()
            model_options = [model.value for model in all_models]
            default_model = self.default_models.get("openrouter", "openai/gpt-4o")
            
            # Find default model index
            try:
                default_index = model_options.index(default_model)
            except ValueError:
                default_index = 0
                
            return st.selectbox("Model", options=model_options, index=default_index)
        except Exception as e:
            st.error(f"Error loading OpenRouter models: {e}")
            return "openai/gpt-4o"
    
    
    def _render_model_parameters(self, provider: str, model: str) -> Tuple[float, int, int, float, float, float]:
        """Render model parameter controls with LLMModelCatalog limits"""
        st.subheader("🔧 Model Parameters")
        
        # Get model-specific limits and defaults from catalog
        try:
            model_enum = LLMModelCatalog.from_string(model)
            token_info = model_enum.get_token_info()
            max_output_tokens = token_info['max_output_tokens']
            max_input_tokens = token_info['max_input_tokens']
            default_tokens = min(1000, max_output_tokens)
            
            # Display model info
            st.info(f"📊 **{token_info['provider'].title()}** | Max Input: {token_info['max_input_tokens_formatted']} | Max Output: {token_info['max_output_tokens_formatted']}")
        except Exception as e:
            st.warning(f"⚠️ Could not load model info: {e}")
            max_output_tokens = 4000
            max_input_tokens = 128000
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
    
    
    def _validate_model_config(self, config: ModelConfig, user_input: str) -> dict:
        """Validate model configuration using LLMModelCatalog"""
        try:
            # Get model info from catalog
            model_enum = LLMModelCatalog.from_string(config.model)
            token_info = model_enum.get_token_info()
            
            # Estimate input tokens (rough: 1 token ≈ 4 characters)
            estimated_input_tokens = len(user_input) // 4
            if config.system_prompt:
                estimated_input_tokens += len(config.system_prompt) // 4
            
            # Validate token limits
            is_valid, error = model_enum.validate_tokens(estimated_input_tokens, config.max_tokens)
            
            if not is_valid:
                suggestion = None
                if "Input tokens" in error:
                    suggestion = f"Try reducing your prompt length or use a model with higher input limits (current: {token_info['max_input_tokens_formatted']})"
                elif "Output tokens" in error:
                    suggestion = f"Reduce max_tokens to {token_info['max_output_tokens']} or less"
                elif "Total tokens" in error:
                    suggestion = f"Reduce either your prompt length or max_tokens (total limit: {token_info['max_total_tokens_formatted']})"
                
                return {
                    'valid': False,
                    'error': error,
                    'suggestion': suggestion
                }
            
            return {'valid': True}
            
        except Exception as e:
            return {
                'valid': False,
                'error': f"Model validation failed: {str(e)}",
                'suggestion': "Try selecting a different model from the dropdown"
            }
    
    def _render_additional_parameters(self) -> str:
        """Render additional parameter controls"""
        st.subheader("📝 Additional Parameters")
        
        # System prompt template selection
        prompt_options = SystemPrompt.get_display_options()
        options = SystemPrompt.get_streamlit_selectbox_options()
        selected_prompt_key = st.selectbox(
            "System Prompt Template",
            options=options,
            index=0,
            help="Choose a predefined system prompt template or select 'Custom' for manual input"
        )
        
        # Get the selected prompt
        if selected_prompt_key and selected_prompt_key != "Custom":
            selected_prompt = prompt_options[selected_prompt_key]
            default_prompt = selected_prompt.prompt
            st.info(f"📋 **{selected_prompt.name}** ({selected_prompt.category})")
        else:
            default_prompt = ""
        
        # Allow custom system prompt input
        custom_prompt = st.text_area(
            "Custom System Prompt",
            value=default_prompt,
            height=100,
            help="Modify the selected template or enter a custom system prompt"
        )
        
        return custom_prompt
    
    def _check_api_key(self, provider: str) -> str:
        """Check for OpenRouter API key in environment variables or manual input"""
        api_key_env = "OPENROUTER_API_KEY"
        api_key = os.getenv(api_key_env)
        
        if api_key:
            st.success(f"✅ {api_key_env} found")
        else:
            st.error(f"❌ {api_key_env} not found in environment variables")
            st.info("Please set your OpenRouter API key:")
            st.code("export OPENROUTER_API_KEY='your-api-key-here'")
            
            # Allow manual input as fallback
            st.markdown("**Or enter your API key manually:**")
            manual_key = st.text_input(
                "OpenRouter API Key",
                type="password",
                help="Enter your OpenRouter API key directly"
            )
            if manual_key:
                api_key = manual_key
                st.success("✅ API key entered manually")
        
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
            # Validate model and parameters
            validation_result = self._validate_model_config(config, user_input)
            if validation_result['valid']:
                self._generate_response(config, user_input)
            else:
                st.error(f"❌ Configuration Error: {validation_result['error']}")
                if validation_result.get('suggestion'):
                    st.info(f"💡 Suggestion: {validation_result['suggestion']}")
    
    def _generate_response(self, config: ModelConfig, user_input: str):
        """Generate response from the selected LLM"""
        try:
            with st.spinner("🤖 Generating response..."):
                # Create client using the new minimal architecture
                client = LLMClientFactory.create(
                    provider=config.provider,
                    api_key=config.api_key,
                    model=config.model,
                    temperature=config.temperature,
                    max_tokens=config.max_tokens
                )
                
                # Prepare prompt
                full_prompt = self._prepare_prompt(user_input, config.system_prompt)
                
                # Generate response with additional parameters
                response = client.generate(
                    full_prompt,
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
        """Render OpenRouter model information using catalog"""
        model_enum = LLMModelCatalog.from_string(model)
        info = model_enum.get_token_info()
        
        # Basic model info
        st.metric("Provider", info['provider'].title())
        st.metric("Model", info['model'])
        
        # Token limits with better formatting
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Max Input", info['max_input_tokens_formatted'])
        with col2:
            st.metric("Max Output", info['max_output_tokens_formatted'])
        
        st.metric("Max Total", info['max_total_tokens_formatted'])
        
        # Model capabilities info
        with st.expander("🔍 Model Details"):
            st.write(f"**Full Model Name:** {info['model']}")
            st.write(f"**Provider:** {info['provider'].title()}")
            st.write(f"**Input Token Limit:** {info['max_input_tokens']:,}")
            st.write(f"**Output Token Limit:** {info['max_output_tokens']:,}")
            st.write(f"**Total Token Limit:** {info['max_total_tokens']:,}")
            
            # Add model-specific notes
            if "gpt-4o" in model.lower():
                st.info("🚀 **GPT-4o**: Latest OpenAI model with vision capabilities")
            elif "claude" in model.lower():
                st.info("🧠 **Claude**: Anthropic's advanced reasoning model")
            elif "gemini" in model.lower():
                st.info("💎 **Gemini**: Google's multimodal AI model")
            elif "command" in model.lower():
                st.info("⚡ **Command**: Cohere's instruction-following model")
            elif "llama" in model.lower():
                st.info("🦙 **Llama**: Meta's open-source language model")
            elif "mistral" in model.lower():
                st.info("🌪️ **Mistral**: High-performance open-source model")
    
    
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
            logo_path=None,  # Use auto-detection
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

