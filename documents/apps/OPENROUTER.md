# OpenRouter LLM App Example

This example demonstrates how to use OpenRouter with the ZealotGenAI framework to access multiple LLM models through a single API.

# Features

- **Configuration Dataset**: Centralized model management through JSON configuration
- **Multiple Model Support**: Access GPT-4, Claude, Gemini, Llama, and more through OpenRouter
- **Unified Interface**: Same API for all models
- **Model Switching**: Switch between models at runtime using configuration
- **Token Validation**: Automatic validation against model limits
- **User-Friendly Output**: Beautiful formatted responses
- **Type Safety**: Enum-based model selection with comprehensive token limits
- **Clean Architecture**: Separated app logic and runner for better maintainability

# File Structure

```
zealot/apps/llm/openrouter/
├── app.py          # OpenRouterApp class (pure app logic)
├── runapp.py       # Interactive runner script with main() function
├── requirements.txt # Python dependencies
└── OPENROUTER.md   # This documentation
```

- **`app.py`**: Contains the `OpenRouterApp` class that can be imported and used by other modules
- **`runapp.py`**: Contains the interactive runner script that can be executed directly
- **Separation of Concerns**: App logic is separate from runner logic for better maintainability

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
./scripts/build_venv.sh openrouter

# Activate virtual environment
source alpha/openrouter/bin/activate

# Install dependencies
pip install -r zealot/apps/llm/openrouter/requirements.txt
```

# Usage

### Basic Usage

```python
from zealot.apps.llm.openrouter.app import OpenRouterApp
from zealot.utils.llm_client import LLMProvider

# Create app
app = OpenRouterApp(LLMProvider.OPENROUTER, "Zealot OpenRouter App")

# Use default model (GPT-4o)
app.run("What is the capital of France?")
```

### Switch Models

```python
# Using string model names
app.run("Explain quantum computing", model="anthropic/claude-3.5-sonnet")
app.run("Write a Python function", model="meta-llama/llama-3.1-8b-instruct")
app.run("Generate creative content", model="cohere/command")
app.run("Quick question", model="openai/gpt-4o-mini")

# Using enum for type safety (recommended)
from zealot.utils.llm_client.models import OpenRouterModel

app.run("Explain quantum computing", model=OpenRouterModel.CLAUDE_3_5_SONNET.value)
app.run("Write a Python function", model=OpenRouterModel.LLAMA_3_1_8B_INSTRUCT.value)
app.run("Generate creative content", model=OpenRouterModel.COHERE_COMMAND.value)
app.run("Quick question", model=OpenRouterModel.GPT_4O_MINI.value)
```

### Available Models

- **OpenAI**: `openai/gpt-4o`, `openai/gpt-4o-mini`, `openai/gpt-3.5-turbo`
- **Anthropic**: `anthropic/claude-3.5-sonnet`, `anthropic/claude-3-haiku`
- **Google**: `google/gemini-pro`
- **Cohere**: `cohere/command`, `cohere/command-light`, `cohere/command-nightly`
- **Meta**: `meta-llama/llama-3.1-8b-instruct`
- **Mistral**: `mistralai/mistral-7b-instruct`


### Model Comparison Table

| Provider  | Model                              | Best For                       | Speed     | Cost     | Input Tokens | Output Tokens |
|-----------|------------------------------------|--------------------------------|-----------|----------|--------------|---------------|
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
# Get available models from configuration
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

### Using the Runner Script

```bash
# Run the OpenRouter app using the interactive runner script
python3 zealot/apps/llm/openrouter/runapp.py
```

The runner script provides an interactive experience:
1. Lists all available OpenRouter models
2. Shows OpenRouter configuration
3. Displays a sample prompt
4. Allows you to select a model by number
5. Shows model information
6. Runs the app with the selected model

### Using the App Class Directly

```python
# Import and use the app class directly
from zealot.apps.llm.openrouter.app import OpenRouterApp
from zealot.utils.llm_client import LLMProvider

# Create and run the app
app = OpenRouterApp(LLMProvider.OPENROUTER, "OpenRouter Demo App")

# List available models
app.list_models()

# Show model information
app.show_model_info("openai/gpt-4o")

# Run with a specific model
response = app.run("What is artificial intelligence?", "openai/gpt-4o")

# Run with configuration from openrouter.json
# The app automatically uses parameters from zealot/config/llm/openrouter.json
response = app.run("Hello!", "cohere/command")

# Or override specific parameters if needed
custom_params = {
    "temperature": 0.5,
    "max_tokens": 200
}
response = app.run("Hello!", "openai/gpt-4o", provider_params=custom_params)
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
