# Hello LLM App

This is a simple LLM application example that demonstrates the basic usage of the ZealotGenAI framework with different LLM providers.

## Features

- **Simple Interface**: Easy-to-use LLM application with minimal setup
- **Multiple Providers**: Supports Cohere, OpenAI, Anthropic, and Google LLM providers
- **User-Friendly Output**: Beautiful formatted responses with clear input/output separation
- **Error Handling**: Comprehensive error handling with helpful messages
- **Extensible**: Easy to modify and extend for different use cases
- **Clean Architecture**: Separated app logic and runner for better maintainability

## File Structure

```
zealot/apps/llm/hello/
├── app.py          # LLMHelloApp class (pure app logic)
├── runapp.py       # Runner script with main() function
├── requirements.txt # Python dependencies
└── HELLO.md        # This documentation
```

- **`app.py`**: Contains the `LLMHelloApp` class that can be imported and used by other modules
- **`runapp.py`**: Contains the runner script that can be executed directly
- **Separation of Concerns**: App logic is separate from runner logic for better maintainability

## Setup

### 1. Get API Key

| Provider             | Website                                            | Steps                                |
|----------------------|----------------------------------------------------|--------------------------------------|
| **Cohere** (Default) | [Cohere.ai](https://cohere.ai)                     | Sign up → Get API key from dashboard |
| **OpenAI**           | [OpenAI Platform](https://platform.openai.com)     | Create account → Generate API key    |
| **Anthropic**        | [Anthropic Console](https://console.anthropic.com) | Sign up → Get API key                |
| **Google**           | [Google AI Studio](https://aistudio.google.com)    | Create project → Generate API key    |

### 2. Set Environment Variable

| Provider             | Environment Variable | Example                                    |
|----------------------|----------------------|--------------------------------------------|
| **Cohere** (Default) | `COHERE_API_KEY`     | `export COHERE_API_KEY="your-key-here"`    |
| **OpenAI**           | `OPENAI_API_KEY`     | `export OPENAI_API_KEY="your-key-here"`    |
| **Anthropic**        | `ANTHROPIC_API_KEY`  | `export ANTHROPIC_API_KEY="your-key-here"` |
| **Google**           | `GOOGLE_API_KEY`     | `export GOOGLE_API_KEY="your-key-here"`    |

### 3. Install Dependencies

```bash
# Create virtual environment
./scripts/devtest/build_venv.sh hello

# Activate virtual environment
source alpha/hello/bin/activate

# Install dependencies
pip install -r zealot/apps/llm/hello/requirements.txt
```

## Usage

### Basic Usage

```python
from zealot.apps.llm.hello.app import LLMHelloApp
from zealot.utils.llm_client import LLMProvider

# Create app with Cohere (default)
app = LLMHelloApp(LLMProvider.COHERE, "Hello LLM App")

# Use default message
app.run()

# Use custom message
app.run("What is the weather like today?")
```

### Switch Providers

```python
# Use different LLM providers
app_cohere = LLMHelloApp(LLMProvider.COHERE, "Cohere App")
app_openai = LLMHelloApp(LLMProvider.OPENAI, "OpenAI App")
app_anthropic = LLMHelloApp(LLMProvider.ANTHROPIC, "Anthropic App")
app_google = LLMHelloApp(LLMProvider.GOOGLE, "Google App")

# Run with different providers
app_cohere.run("Tell me a joke")
app_openai.run("Explain quantum computing")
app_anthropic.run("Write a poem")
app_google.run("What are the benefits of renewable energy?")
```

### Using the App Class Directly

```python
from zealot.apps.llm.hello.app import LLMHelloApp
from zealot.utils.llm_client import LLMProvider

# Create app instance
app = LLMHelloApp(LLMProvider.COHERE, "My Hello App")

# Run with custom message
response = app.run("How can I learn Python programming?")

# The app automatically adds "Hello!" prefix to messages
# So "How can I learn Python programming?" becomes
# "Hello! How can I learn Python programming?"
```

## Configuration

The Hello app uses the LLM client configuration system. Configuration files are located in `zealot/config/llm/`:

| Provider      | Config File      | Contains                          |
|---------------|------------------|-----------------------------------|
| **Cohere**    | `cohere.json`    | API keys, model names, parameters |
| **OpenAI**    | `openai.json`    | API keys, model names, parameters |
| **Anthropic** | `anthropic.json` | API keys, model names, parameters |
| **Google**    | `google.json`    | API keys, model names, parameters |

The app automatically loads the appropriate configuration based on the selected provider.

## Run the Example

### Using the Runner Script

```bash
# Run the hello app using the runner script
python3 zealot/apps/llm/hello/runapp.py
```

### Using the App Class Directly

```python
# Import and use the app class directly
from zealot.apps.llm.hello.app import LLMHelloApp
from zealot.utils.llm_client import LLMProvider

# Create and run the app
app = LLMHelloApp(LLMProvider.COHERE, "Hello LLM App")
app.run()
```

## Example Output

```
============================================================
📝 INPUT
============================================================
🤖 Hello! How are you doing today?

🔄 PROCESSING...
   Calling LLM service...
✅ Loaded config for cohere
   ✅ Response received!

============================================================
📤 OUTPUT
============================================================
🤖 Response: I'm doing well, thank you for asking! I'm here and ready to help you with any questions or tasks you might have. How are you doing today? Is there anything specific I can assist you with?

============================================================
```


## Troubleshooting

| Issue                 | Quick Fix                            |
|-----------------------|--------------------------------------|
| **API Key Error**     | Set the correct environment variable |
| **Package Not Found** | Install missing dependencies         |
| **Connection Issues** | Check internet connection            |
| **Model Not Found**   | Verify configuration files           |

## Author

Created as part of the ZealotGenAI framework for demonstrating LLM application development.
