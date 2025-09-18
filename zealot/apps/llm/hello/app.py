"""
LLM Hello App
Simple LLM application example
"""

import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from zealot.utils.llm_client import LLMProvider
from zealot.apps.common.app import LLMApp


class LLMHelloApp(LLMApp):
    """Simple LLM Hello App"""
    
    def run(self, message: str = "How are you doing today?"):
        """Run the app with a message"""
        # Add "Hello!" prefix to the message for this specific app
        formatted_message = f"Hello! {message}"
        return super().run(formatted_message)


def main():
    """Main function"""
    app = LLMHelloApp(LLMProvider.COHERE, "Hello LLM App")
    app.run()


if __name__ == "__main__":
    main()