# 🤖 LLM Studio - Interactive LLM Testing Interface

# Overview

LLM Studio is a comprehensive Streamlit-based web application that provides an interactive interface for testing and experimenting with different Large Language Models (LLMs) through the OpenRouter platform. Built as part of the ZealotGenAI framework, it offers advanced parameter controls and real-time model interaction capabilities.

# 🚀 Features

### Core Functionality

- **Multi-Model Support**: Access to all OpenRouter-supported models
- **Interactive UI**: User-friendly Streamlit interface
- **Real-time Generation**: Live LLM response generation
- **Advanced Parameters**: Fine-tune model behavior with multiple knobs
- **Model Information**: Detailed token limits and capabilities display
- **Configuration Management**: Environment-based API key handling

### Advanced Parameter Controls
- **Temperature** (0.0-2.0): Controls response randomness and creativity
- **Max Tokens** (1-100,000): Limits response length with model-specific caps
- **Top-K** (1-100): Fixed shortlist of most likely tokens
- **Top-P** (0.0-1.0): Dynamic probability-based token selection
- **Frequency Penalty** (-2.0 to 2.0): Reduces word repetition
- **Presence Penalty** (-2.0 to 2.0): Encourages novel content


# 🏗️ Architecture

### Class Hierarchy
```
LLMApp (Base Class)
└── LLMStudioApp (Streamlit Implementation)
```

### Key Components

#### 1. **LLMStudioApp Class**
- **Inherits from**: `LLMApp` (zealot framework base class)
- **Provider**: OpenRouter (hardcoded)
- **Purpose**: Main application logic and UI rendering

#### 2. **ModelConfig Dataclass**
```python
@dataclass
class ModelConfig:
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
```

#### 3. **AppConfig Dataclass**
```python
@dataclass
class AppConfig:
    page_title: str = "LLM Studio"
    page_icon: str = "🤖"
    layout: str = "wide"
    sidebar_state: str = "expanded"
```

## 📁 File Structure

```
zealot/apps/llm/llmstudio/
├── app.py              # Main Streamlit application
├── runapp.py           # Application runner script
├── requirements.txt    # Python dependencies
└── __init__.py         # Package initialization
```

## 🛠️ Installation & Setup

### Prerequisites
- Python 3.8+
- Virtual environment (recommended)
- OpenRouter API key

### Installation Steps

1. **Navigate to the project directory**:
   ```bash
   cd /path/to/zealotGenAI/zealot/apps/llm/llmstudio
   ```

2. **Set up virtual environment**:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Set environment variable**:
   ```bash
   export OPENROUTER_API_KEY='your-api-key-here'
   ```

5. **Run the application**:
   ```bash
   python runapp.py
   ```

6. **Access the app**:
   - Open browser to `http://localhost:8501`

## 🎯 Usage Guide

### Basic Workflow

1. **Launch the App**: Run `python runapp.py`
2. **Select Model**: Choose from OpenRouter model dropdown
3. **Configure Parameters**: Adjust temperature, tokens, and advanced settings
4. **Enter Prompt**: Type your question or prompt
5. **Generate Response**: Click "🚀 Generate Response" button
6. **View Results**: Review the generated response and model information

# Parameter Tuning Guide

### Parameter Reference Table

| Parameter | Description |
|-----------|-------------|
| **Top-K** | Defines a fixed shortlist of the most likely tokens, preventing the model from wandering too far into low-probability options |
| **Top-P (Nucleus Sampling)** | Creates a probability-based shortlist that adapts dynamically to context, ensuring the model considers just enough options to stay fluent while remaining flexible |
| **Temperature** | Controls the randomness of selection, tuning whether the model plays it safe (low temperature) or experiments more freely (high temperature) |
| **Frequency Penalty** | Helps avoid redundancy by discouraging the model from repeating the same words too often |
| **Presence Penalty** | Pushes for novelty, encouraging the model to introduce words or ideas that haven't appeared yet in the text |

### Parameter Tuning Ranges

| Parameter | Range | Effect | Recommendation |
|-----------|-------|--------|----------------|
| **Temperature** | 0.0-0.3 | Very focused, deterministic responses | For precise, factual content |
| | 0.4-0.7 | Balanced creativity and coherence | **Recommended for most use cases** |
| | 0.8-1.2 | More creative and varied responses | For creative writing |
| | 1.3-2.0 | Highly creative, potentially less coherent | For experimental content |
| **Top-K** | 1-10 | Very focused, conservative responses | For deterministic outputs |
| | 20-40 | Balanced selection | **Recommended default** |
| | 50-100 | More diverse, potentially less coherent | For creative exploration |
| **Top-P (Nucleus Sampling)** | 0.1-0.3 | Very focused responses | For precise, narrow outputs |
| | 0.7-0.9 | Balanced creativity | **Recommended for most cases** |
| | 0.9-1.0 | Maximum diversity | For maximum creativity |
| **Penalty Parameters** | Negative values | Encourage repetition/avoid novelty | Rarely used |
| | 0.0 | No penalty (neutral) | **Default setting** |
| | Positive values | Reduce repetition/encourage novelty | For varied content |


### Parameter Combination Strategies

| Outcome | Potential Combination | Description | Example Use Case |
|---------|----------------------|-------------|------------------|
| **Conservative & Accurate** | Low Temperature (0.1–0.3), Small Top-K (≤20), No penalties | Model picks only from safest options. Predictable, focused, minimal risk of nonsense. | Step-by-step math solutions, legal contracts, medical instructions |
| **Balanced Assistant** | Medium Temperature (0.7–1.0), Top-P ~0.9, Mild penalties | Natural mix of predictability and creativity. Can adapt across domains without drifting. | General-purpose chatbot, customer support, Q&A with light variation |
| **Creative & Diverse** | High Temperature (1.2–1.8), Large Top-K (≥100), Presence Penalty (0.5–0.8) | Model explores unusual options, introduces new ideas, avoids repetition. | Storytelling, brainstorming, poetry, marketing slogans |
| **Structured but Flexible** | Medium Temperature (~0.7), Top-P 0.8, Small Frequency Penalty (0.2–0.4) | Ensures logical flow while reducing repeated phrases. | Technical documentation, product descriptions, long-form essays |
| **Exploratory / Idea Generator** | Temperature 1.0–1.5, Top-P ~0.95, Higher Presence Penalty (0.6–1.0) | Strong push for novelty — encourages out-of-the-box concepts. | Research ideation, design thinking sessions, new feature suggestions |
| **Highly Deterministic** | Temperature = 0, Top-K = 1 | Model always picks the top choice. Fully reproducible, zero randomness. | Unit test generation, deterministic query responses |
| **Conversational Naturalness** | Temperature 0.7–0.9, Top-P ~0.9, Mild Frequency Penalty (0.3–0.5) | Sounds like a human — avoids exact repetition but still flows smoothly. | Dialogue systems, teaching assistants, personal companions |
| **Safe & Guarded** | Low Temp (0.2–0.5), Top-P 0.7–0.8, No penalties | Model sticks to mainstream, cautious answers. | Compliance-focused chatbots, corporate communications |

### System Prompt Templates

Predefined AI behavior templates that set the tone, style, and expertise level for responses across professional, technical, friendly, and creative use cases.

The app includes predefined system prompt templates to guide the AI's behavior and response style. Choose from 15 different templates across 4 categories:

| Category | Template | Description | Best For |
|----------|----------|-------------|----------|
| **Professional / Formal** | General Professional | Formal, clear, concise responses avoiding slang | Business communications, official documents |
| | Business Advisor | Practical, actionable advice with examples | Business consulting, strategic planning |
| | Legal Expert | Precise, accurate, neutral legal guidance | Legal documents, compliance matters |
| | Medical Expert | Factual medical information with best practices | Healthcare advice, medical explanations |
| **Technical / Coding** | Software Engineer | Clear code examples with logical explanations | Programming help, code reviews |
| | Data Analyst | Data insights, trends, and recommendations | Data analysis, business intelligence |
| | JSON Structured Output | Responses in valid JSON format | API responses, structured data |
| | Step-by-Step Instructions | Detailed, sequential task guidance | Tutorials, procedures, how-to guides |
| **Friendly / Casual** | Friendly Assistant | Simple explanations with relatable examples | General help, casual conversations |
| | Educational Tutor | Patient explanations for learners | Teaching, educational content |
| | Concise Advisor | Short, clear, actionable answers | Quick help, brief responses |
| **Creative / Imaginative** | Storyteller | Imaginative narratives with vivid descriptions | Creative writing, storytelling |
| | Poet / Lyricist | Emotional poems and lyrics with rhythm | Poetry, songwriting, creative expression |
| | Brainstorming Partner | Multiple creative ideas and solutions | Ideation, problem-solving, innovation |
| | Persona-based Roleplay | Consistent character responses | Roleplay, character interactions |

**Usage Tips:**
- Select "Custom" to write your own system prompt
- Combine system prompts with appropriate parameter settings
- Professional prompts work well with lower temperature (0.3-0.7)
- Creative prompts benefit from higher temperature (0.8-1.2)

# 🔍 Error Handling

### Common Issues
1. **API Key Missing**: Set `OPENROUTER_API_KEY` environment variable
2. **Model Loading Error**: Check internet connection and API key validity
3. **Parameter Validation**: Ensure values are within allowed ranges
4. **Network Issues**: Check OpenRouter service status

### Error Messages
- **Configuration Error**: Invalid parameter values
- **Connection Error**: Network or API connectivity issues
- **Package Error**: Missing dependencies
- **Unexpected Error**: General error handling with detailed information

# 📝 License

© 2025 [Jyoti Ranjan](https://www.linkedin.com/in/jyoti-ranjan-5083595/). All rights reserved.

This work reflects my personal AI learning journey and is shared for educational and knowledge-building purposes. While unauthorized reproduction, modification, or commercial use without prior written consent is strictly prohibited, I warmly welcome discussions, feedback, and collaborative learning exchanges.

# 🆘 Support

### Getting Help

| Support Type | Contact Method | Description |
|--------------|----------------|-------------|
| **Documentation** | This guide and code comments | Comprehensive usage and parameter guides |
| **Issues** | Report bugs and feature requests | Technical problems and enhancement requests |
| **Discussions** | Join community discussions | General questions and knowledge sharing |
| **LinkedIn** | [Jyoti Ranjan](https://www.linkedin.com/in/jyoti-ranjan-5083595/) | Professional networking and collaboration |
| **Email** | jranjan@gmail.com | Direct contact for detailed inquiries |

### Troubleshooting

1. Check environment variables
2. Verify API key validity
3. Review error messages
4. Check network connectivity
5. Consult documentation

---

**Built with ❤️ using Streamlit and the ZealotGenAI framework**
