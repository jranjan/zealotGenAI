"""
LLM Hello App
Simple LLM application example
"""

from zealot.apps.common.app import LLMApp


class LLMHelloApp(LLMApp):
    """Simple LLM Hello App"""
    
    def run(self, message: str = "How are you doing today?"):
        """Run the app with a message"""
        # Add "Hello!" prefix to the message for this specific app
        formatted_message = f"Hello! {message}"
        return super().run(formatted_message)