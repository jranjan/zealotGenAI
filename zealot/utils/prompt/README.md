# Prompt Utilities

This package provides prompt utilities for LLM applications, organized by implementation type.

## Structure

```
zealot/utils/prompt/
├── __init__.py              # Main package exports
├── custom/                  # Custom project-specific tools
│   ├── __init__.py
│   └── system_prompts.py    # Custom system prompt templates
└── README.md               # This file
```

## Components

### Custom Tools (`custom/`)

**Purpose:** Project-specific prompt templates and utilities

**Features:**
- `SystemPrompt` enum with 15 predefined system prompts
- Categories: Professional, Technical, Friendly, Creative
- Streamlit integration helpers
- Simple, direct API

**Usage:**
```python
from zealot.utils.prompt import SystemPrompt

# Direct access
SystemPrompt.SOFTWARE_ENGINEER.prompt

# Category-based access
SystemPrompt.get_by_category("Technical / Coding")

# Name-based access
SystemPrompt.get_by_name("Software Engineer").prompt
```

## Design Philosophy

### Custom vs Standard

- **Custom (`custom/`)**: Project-specific implementations, tailored to your needs
- **Future Standard**: Room for standard prompt utilities (LangChain, etc.)

### Why This Structure?

1. **Clear Separation**: Custom vs standard implementations
2. **Future-Ready**: Easy to add standard prompt libraries
3. **Maintainable**: Clear boundaries between components
4. **Scalable**: Can add more custom tools as needed

## Future Enhancements

- Add standard prompt template libraries (LangChain, etc.)
- Add more custom prompt building tools
- Add prompt validation utilities
- Add prompt testing frameworks