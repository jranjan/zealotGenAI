# Cohere LLM App

This is a simple LLM application example that demonstrates the basic usage of the ZealotGenAI framework with the Cohere provider using the new minimal LLM client architecture.

## Features

- **Minimal Client Architecture**: Uses the new unified LLMClient for all providers
- **Cohere Integration**: Direct integration with Cohere API using command-a-03-2025 model
- **User-Friendly Output**: Beautiful formatted responses with clear input/output separation
- **Error Handling**: Comprehensive error handling with helpful messages
- **Extensible**: Easy to modify and extend for different use cases
- **Clean Architecture**: Inherits from LLMApp base class for consistency

## File Structure

```
zealot/apps/cohere/
├── app.py          # LLMCohereApp class (pure app logic)
├── __init__.py     # Package initialization
├── __main__.py     # Module runner for python -m execution
└── requirements.txt # Python dependencies

documents/apps/
└── COHERE.md       # This documentation
```

- **`app.py`**: Contains the `LLMCohereApp` class that inherits from `LLMApp`
- **`__main__.py`**: Module runner that can be executed with `python -m zealot.apps.cohere`
- **Clean Architecture**: Uses the new minimal LLM client architecture

## Setup

### 1. Get Cohere API Key

1. Visit [Cohere.ai](https://cohere.ai)
2. Sign up for an account
3. Get your API key from the dashboard

### 2. Set Environment Variable

```bash
export COHERE_API_KEY="your-cohere-api-key-here"
```

### 3. Install Dependencies

```bash
# Create virtual environment
./scripts/devtest/build_venv.sh cohere

# Activate virtual environment
source alpha/cohere/bin/activate

# Install dependencies
pip install -r zealot/apps/cohere/requirements.txt
```

## Usage

### Basic Usage

```python
from zealot.apps.cohere.app import LLMCohereApp

# Create app instance
app = LLMCohereApp()

# Run with custom message
app.run("What is the weather like today?")
```

### Using the App Class Directly

```python
from zealot.apps.cohere.app import LLMCohereApp

# Create app instance
app = LLMCohereApp()

# Run with custom message(s)
response = app.run("How can I learn Python programming?")

# The app uses Cohere's command-a-03-2025 model
# and provides beautiful formatted output
```

## Configuration

The Cohere app uses the new minimal LLM client architecture with built-in configuration:

- **Provider**: Cohere
- **Model**: command-a-03-2025
- **API Key**: From `COHERE_API_KEY` environment variable
- **Default Parameters**: Temperature, max tokens, etc. are configured in the client

## Run the Example

### Using Module Execution

```bash
# Run the Cohere app as a module
python3 -m zealot.apps.cohere
```

### Using the App Class Directly

```python
# Import and use the app class directly
from zealot.apps.cohere.app import LLMCohereApp

# Create and run the app
app = LLMCohereApp()
app.run("Hello! How are you today?")
```

## Example Output

```
============================================================
📝 INPUT
============================================================
🤖 Hello! How are you doing today?

🔄 PROCESSING...
   Calling cohere API | Provider: cohere | Model: command-a-03-2025
✅ Loaded config for cohere
   ✅ Response received!

============================================================
📤 OUTPUT
============================================================
🤖 Response: I'm doing well, thank you for asking! I'm here and ready to help you with any questions or tasks you might have. How are you doing today? Is there anything specific I can assist you with?

============================================================
```

## Troubleshooting

| Issue                 | Quick Fix                                   |
|:----------------------|:--------------------------------------------|
| **API Key Error**     | Set the COHERE_API_KEY environment variable |
| **Package Not Found** | Install missing dependencies                |
| **Connection Issues** | Check internet connection                   |
| **Model Not Found**   | Verify Cohere model availability            |

## Author

Created as part of the ZealotGenAI framework for demonstrating LLM application development with the new minimal client architecture.
