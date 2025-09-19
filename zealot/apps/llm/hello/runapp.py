"""
Hello LLM App Runner
Separates app logic from runner logic
"""

import sys
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent.parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from zealot.utils.llm_client import LLMProvider
from zealot.apps.llm.hello.app import LLMHelloApp


def main():
    """Main function"""
    app = LLMHelloApp(LLMProvider.COHERE, "Hello LLM App")
    app.run()


if __name__ == "__main__":
    main()
