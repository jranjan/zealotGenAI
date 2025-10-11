# OpenRouter LLM App Example

This example demonstrates how to use OpenRouter with the ZealotGenAI framework to access multiple LLM models through a single API with an interactive loop-based interface.

# Features

- **Interactive Loop Interface**: Choose provider → model → prompt in a continuous loop
- **Model Catalog Integration**: Uses LLMModelCatalog for comprehensive model management
- **Multiple Model Support**: Access GPT-4, Claude, Gemini, Llama, and more through OpenRouter
- **Unified Interface**: Same API for all models
- **Model Switching**: Switch between models at runtime using interactive selection
- **Token Validation**: Automatic validation against model limits
- **User-Friendly Output**: Beautiful formatted responses
- **Type Safety**: Enum-based model selection with comprehensive token limits
- **Clean Architecture**: Uses new minimal LLM client architecture

# File Structure

```
zealot/apps/openrouter/
├── app.py              # OpenRouterApp class with interactive loop
├── __init__.py         # Package initialization
├── __main__.py         # Module runner for python -m execution
└── requirements.txt    # Python dependencies

documents/apps/
└── OPENROUTER.md       # This documentation
```

- **`app.py`**: Contains the `OpenRouterApp` class with interactive loop functionality
- **`__init__.py`**: Package initialization
- **`__main__.py`**: Module runner that can be executed with `python -m zealot.apps.openrouter`
- **Clean Architecture**: Uses the new minimal LLM client architecture

# Setup

### 1. Get OpenRouter API Key

1. Visit [OpenRouter.ai](https://openrouter.ai)
2. Sign up for an account
3. Get your API key from the dashboard

### 2. Set Environment Variable

```bash
export OPENROUTER_API_KEY="your-api-key-here"
```

### 3. Install Dependencies

```bash
# Create virtual environment
./scripts/devtest/build_venv.sh openrouter

# Activate virtual environment
source alpha/openrouter/bin/activate

# Install dependencies
pip install -r zealot/apps/openrouter/requirements.txt
```

# Usage

### Interactive Mode (Recommended)

```bash
# Run the interactive OpenRouter app
python3 -m zealot.apps.openrouter

# Choose mode:
# 1. Interactive Mode (choose provider → model → prompt)
# 2. Demo Mode (show examples)
```

### Interactive Flow

1. **Choose Provider**: Select from available providers (OpenAI, Anthropic, Cohere, Google, Meta, Mistral)
2. **Choose Model**: Pick specific model for that provider with token limits
3. **Enter Prompt**: Type your question or request
4. **Get Response**: LLM generates response via OpenRouter
5. **Loop Back**: Choose different provider/model/prompt or type 'exit'

### Basic Usage (Programmatic)

```python
from zealot.apps.openrouter.app import OpenRouterApp

# Create app
app = OpenRouterApp()

# Use with specific model
app.run("What is the capital of France?", provider="openrouter", model="openai/gpt-4o")
app.run("Explain quantum computing", provider="openrouter", model="anthropic/claude-3.5-sonnet")
app.run("Write a Python function", provider="openrouter", model="cohere/command-a-03-2025")
```

### Available Models (via LLMModelCatalog)

- **OpenAI**: `openai/gpt-4o`, `openai/gpt-4o-mini`, `openai/gpt-3.5-turbo`
- **Anthropic**: `anthropic/claude-3.5-sonnet`, `anthropic/claude-3-haiku`
- **Google**: `google/gemini-pro`
- **Cohere**: `cohere/command-a-03-2025`, `cohere/command-r-plus`, `cohere/command-r`
- **Meta**: `meta-llama/llama-3.1-8b-instruct`
- **Mistral**: `mistralai/mistral-7b-instruct`

All models are managed through the `LLMModelCatalog` with comprehensive token limits and validation.


### Model Comparison Table

| Provider  | Model                              | Best For                       | Speed     | Cost     | Input Tokens | Output Tokens |
|:----------|:-----------------------------------|:-------------------------------|:----------|:---------|:-------------|:--------------|
| OpenAI    | `openai/gpt-4o`                    | General purpose, high quality  | Medium    | High     | 128,000      | 16,384        |
| OpenAI    | `openai/gpt-4o-mini`               | Fast responses, cost-effective | Fast      | Low      | 128,000      | 16,384        |
| Anthropic | `anthropic/claude-3.5-sonnet`      | Reasoning, analysis            | Medium    | High     | 200,000      | 8,192         |
| Anthropic | `anthropic/claude-3-haiku`         | Fast responses                 | Fast      | Medium   | 200,000      | 4,096         |
| Google    | `google/gemini-pro`                | Multimodal, creative           | Medium    | Medium   | 30,720       | 2,048         |
| Cohere    | `cohere/command`                   | Text generation, summarization | Fast      | Low      | 4,096        | 1,024         |
| Cohere    | `cohere/command-light`             | Fast text generation           | Very Fast | Very Low | 4,096        | 1,024         |
| Meta      | `meta-llama/llama-3.1-8b-instruct` | Coding, open source            | Fast      | Low      | 128,000      | 8,192         |
| Mistral   | `mistralai/mistral-7b-instruct`    | Fast, efficient                | Very Fast | Very Low | 32,768       | 8,192         |


### Using Token Limits in Client Classes

All LLM clients now support token validation:

```python
from zealot.utils.llm_client import create_llm_client, LLMProvider

# Create a client
client = create_llm_client(LLMProvider.OPENROUTER)

# Get token limits (if available)
limits = client.get_token_limits()
if limits:
    print(f"Max input: {limits['max_input_tokens']:,}")
    print(f"Max output: {limits['max_output_tokens']:,}")
    print(f"Max total: {limits['max_total_tokens']:,}")

# Validate before making requests
is_valid, error = client.validate_tokens(1000, 500)
if not is_valid:
    print(f"Request would fail: {error}")
else:
    response = client.generate_text("Your prompt here")
```

# Configuration Dataset Approach

The OpenRouter integration uses a **configuration dataset** approach that provides:

### 1. Centralized Configuration
All OpenRouter settings are stored in `zealot/config/llm/openrouter.json`:

```json
{
  "provider": "openrouter",
  "api_key": "${OPENROUTER_API_KEY}",
  "base_url": "https://openrouter.ai/api/v1/chat/completions",
  "model": "openai/gpt-4o",
  "temperature": 0.7,
  "max_tokens": 100,
  "timeout": 30,
  "additional_params": {
    "http_referer": "https://zealotgenai.com",
    "x_title": "ZealotGenAI"
  }
}
```

### 2. Configuration-Driven Parameters

The app automatically uses parameters from `openrouter.json` for all models:

- **Default Behavior**: Uses `temperature: 0.7`, `max_tokens: 100` from JSON config
- **Consistent Settings**: Same parameters applied to all models (OpenAI, Cohere, Anthropic, etc.)
- **Override Support**: Can still override specific parameters using `provider_params`
- **Single Source of Truth**: All configuration managed in one JSON file

### 3. Model Dataset Integration
The client automatically loads available models from the `OpenRouterModel` enum:

```python
# Get the available models from configuration
client = create_llm_client(LLMProvider.OPENROUTER)
models = client.get_available_models()
# Returns: ['openai/gpt-4o', 'cohere/command', 'anthropic/claude-3.5-sonnet', ...]

# Get model information
model_info = client.get_model_info('cohere/command')
# Returns: {'model': 'cohere/command', 'provider': 'cohere', 'max_input_tokens': 4096, ...}
```

### 4. Configuration-Based Model Selection
Models are selected using the configuration dataset:

```python
# Use default model from configuration
app.run("Hello world")

# Override model using configuration dataset
app.run("Hello world", model="cohere/command")

# Validate against configuration limits
is_valid, error = client.validate_tokens(1000, 500)
```

### 5. Benefits of Configuration Dataset
- **Centralized Management**: All models and settings in one place
- **Automatic Validation**: Token limits validated against configuration
- **Easy Switching**: Change models without code changes
- **Type Safety**: Enum-based model selection
- **Consistent API**: Same interface across all models

# Run the Example

### Interactive Mode

```bash
# Run the interactive OpenRouter app
python3 -m zealot.apps.openrouter

# Choose mode:
# 1. Interactive Mode (choose provider → model → prompt)
# 2. Demo Mode (show examples)
```

The interactive mode provides:
1. **Provider Selection**: Choose from available providers with model counts
2. **Model Selection**: Pick specific model for chosen provider with token limits
3. **Prompt Input**: Enter custom prompts with 'back' option
4. **Model Info**: Shows token limits before using
5. **Continuous Loop**: Keep using until you type 'exit'
6. **List All Models**: Option to see all models at once

### Demo Mode

The demo mode is built into the main app - just run the interactive mode and choose option 2.

### Using the App Class Directly

```python
# Import and use the app class directly
from zealot.apps.openrouter.app import OpenRouterApp

# Create and run the app
app = OpenRouterApp()

# List available models
app.list_models()

# Show model information
app.show_model_info("openai/gpt-4o")

# Run with a specific model
response = app.run("What is artificial intelligence?", provider="openrouter", model="openai/gpt-4o")

# Demonstrate catalog features
app.demonstrate_catalog_features()
```

# Benefits of OpenRouter

1. **Single API**: Access multiple LLM providers through one interface
2. **Cost Comparison**: Compare costs across different models
3. **Model Switching**: Easily switch between models for different use cases
4. **Unified Billing**: Single billing for all models
5. **Rate Limiting**: Built-in rate limiting and retry logic

# Pricing

OpenRouter provides transparent pricing for each model. Check their [pricing page](https://openrouter.ai/pricing) for current rates.

# Troubleshooting

- **API Key Error**: Make sure `OPENROUTER_API_KEY` environment variable is set
- **Model Not Found**: Check that the model name is correct
- **Rate Limiting**: OpenRouter has built-in rate limiting
- **Network Issues**: Check your internet connection
