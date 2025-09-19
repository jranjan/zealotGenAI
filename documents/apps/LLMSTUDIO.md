# LLM Studio

Interactive Streamlit UI for testing different LLM providers and models.

## Features

- **Multi-Provider Support**: Test Cohere, OpenAI, Anthropic, Google, and OpenRouter
- **Model Selection**: Choose from available models for each provider
- **Parameter Tuning**: Adjust temperature and max tokens
- **System Prompts**: Add context with system prompts
- **Real-time Generation**: Get instant responses from LLMs
- **Model Information**: View token limits and capabilities

## Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Set API Keys

Set your API keys as environment variables:

```bash
# For Cohere
export COHERE_API_KEY="your-cohere-api-key"

# For OpenAI
export OPENAI_API_KEY="your-openai-api-key"

# For Anthropic
export ANTHROPIC_API_KEY="your-anthropic-api-key"

# For Google
export GOOGLE_API_KEY="your-google-api-key"

# For OpenRouter
export OPENROUTER_API_KEY="your-openrouter-api-key"
```

### 3. Run the App

```bash
python runapp.py
```

Or directly with Streamlit:

```bash
streamlit run app.py
```

## Usage

1. **Select Provider**: Choose from the dropdown in the sidebar
2. **Choose Model**: Select a model (OpenRouter shows all available models)
3. **Configure Parameters**:
   - **Temperature**: Controls randomness (0.0 = deterministic, 2.0 = very random)
   - **Max Tokens**: Maximum response length
4. **Add System Prompt**: Optional context for the AI
5. **Enter API Key**: Input your provider's API key
6. **Ask Question**: Type your prompt in the main text area
7. **Generate**: Click the "Generate Response" button

## Interface

### Sidebar
- **Configuration**: Provider and model selection
- **Model Parameters**: Temperature and max tokens sliders
- **Additional Parameters**: System prompt input
- **API Key**: Secure input for your API key

### Main Area
- **Chat Interface**: Large text area for your prompts
- **Response Display**: Shows the generated response
- **Model Info**: Displays token limits and capabilities
- **Current Settings**: JSON summary of your configuration

## Supported Providers

| Provider | Models | Notes |
|----------|--------|-------|
| **Cohere** | command | Fast, cost-effective |
| **OpenAI** | gpt-4o, gpt-4o-mini, gpt-3.5-turbo | High quality, versatile |
| **Anthropic** | claude-3-5-sonnet, claude-3-haiku | Excellent reasoning |
| **Google** | gemini-pro | Good for various tasks |
| **OpenRouter** | 10+ models | Access to multiple providers |

## Tips

- **Temperature**: Use 0.1-0.3 for factual responses, 0.7-1.0 for creative content
- **Max Tokens**: Start with 1000, adjust based on your needs
- **System Prompts**: Use to set context, tone, or specific instructions
- **Model Selection**: Try different models to find what works best for your use case

## Troubleshooting

- **API Key Issues**: Make sure your API key is correct and has sufficient credits
- **Model Not Found**: Some models may not be available in all regions
- **Rate Limits**: If you hit rate limits, wait a moment and try again
- **Connection Issues**: Check your internet connection and API endpoint availability
