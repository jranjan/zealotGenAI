"""
System prompt templates for LLM applications
Simple enum-based system prompts for different use cases
"""

from enum import Enum
from typing import Dict, List


class SystemPrompt(Enum):
    """System prompt templates for different use cases"""
    
    # Professional / Formal
    GENERAL_PROFESSIONAL = "You are a professional assistant. Respond formally, clearly, and concisely, avoiding slang or casual language."
    BUSINESS_ADVISOR = "You are a business consultant. Provide practical, actionable advice with examples where relevant."
    LEGAL_EXPERT = "You are a legal expert. Provide precise, accurate, and neutral legal guidance."
    MEDICAL_EXPERT = "You are a medical consultant. Respond factually, using general best practices and clear explanations."
    
    # Technical / Coding
    SOFTWARE_ENGINEER = "You are an expert software engineer. Provide clear code examples and explain the logic behind solutions."
    DATA_ANALYST = "You are a data analyst. Provide insights from data, explain trends, and suggest actionable recommendations."
    JSON_STRUCTURED_OUTPUT = "Always provide responses in valid JSON format with fields: title, description, and examples."
    STEP_BY_STEP_INSTRUCTIONS = "Provide detailed, step-by-step instructions for completing tasks or solving problems."
    
    # Friendly / Casual
    FRIENDLY_ASSISTANT = "You are a friendly AI assistant. Explain concepts simply, with relatable examples."
    EDUCATIONAL_TUTOR = "You are a patient tutor. Break down complex topics into simple explanations for learners."
    CONCISE_ADVISOR = "Provide short, clear, and actionable answers, prioritizing clarity over verbosity."
    
    # Creative / Imaginative
    STORYTELLER = "You are a creative storyteller. Generate imaginative and engaging narratives with vivid descriptions."
    POET_LYRICIST = "Compose original poems or lyrics with emotion, rhythm, and creativity."
    BRAINSTORMING_PARTNER = "Generate multiple creative ideas or solutions for a given problem, emphasizing originality."
    PERSONA_BASED_ROLEPLAY = "Adopt a persona as instructed and respond consistently in character, maintaining tone and style."
    
    @property
    def name(self) -> str:
        """Get the display name of the prompt"""
        return self._name_.replace('_', ' ').title()
    
    @property
    def prompt(self) -> str:
        """Get the prompt text"""
        return self.value
    
    @property
    def category(self) -> str:
        """Get the category of the prompt"""
        # Map enum names to categories
        categories = {
            'GENERAL_PROFESSIONAL': 'Professional / Formal',
            'BUSINESS_ADVISOR': 'Professional / Formal',
            'LEGAL_EXPERT': 'Professional / Formal',
            'MEDICAL_EXPERT': 'Professional / Formal',
            'SOFTWARE_ENGINEER': 'Technical / Coding',
            'DATA_ANALYST': 'Technical / Coding',
            'JSON_STRUCTURED_OUTPUT': 'Technical / Coding',
            'STEP_BY_STEP_INSTRUCTIONS': 'Technical / Coding',
            'FRIENDLY_ASSISTANT': 'Friendly / Casual',
            'EDUCATIONAL_TUTOR': 'Friendly / Casual',
            'CONCISE_ADVISOR': 'Friendly / Casual',
            'STORYTELLER': 'Creative / Imaginative',
            'POET_LYRICIST': 'Creative / Imaginative',
            'BRAINSTORMING_PARTNER': 'Creative / Imaginative',
            'PERSONA_BASED_ROLEPLAY': 'Creative / Imaginative'
        }
        return categories.get(self._name_, 'General')
    
    @classmethod
    def get_by_category(cls, category: str) -> List['SystemPrompt']:
        """Get all prompts in a specific category"""
        return [prompt for prompt in cls if prompt.category == category]
    
    @classmethod
    def get_all_categories(cls) -> List[str]:
        """Get all available categories"""
        return list(set(prompt.category for prompt in cls))
    
    @classmethod
    def get_display_options(cls) -> Dict[str, 'SystemPrompt']:
        """Get options for Streamlit selectbox"""
        return {f"{prompt.name} ({prompt.category})": prompt for prompt in cls}
    
    @classmethod
    def get_by_name(cls, name: str) -> 'SystemPrompt':
        """Get a specific prompt by name"""
        for prompt in cls:
            if prompt.name == name:
                return prompt
        raise ValueError(f"No system prompt found with name: {name}")
    
    @classmethod
    def get_streamlit_selectbox_options(cls) -> List[str]:
        """Get options formatted for Streamlit selectbox with Custom option"""
        options = ["Custom"] + list(cls.get_display_options().keys())
        return options
    
    def __str__(self) -> str:
        return f"{self.name} ({self.category})"
    
    def __repr__(self) -> str:
        return f"SystemPrompt.{self._name_}"

