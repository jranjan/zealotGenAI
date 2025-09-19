"""
LLM Studio Streamlit App Runner
"""

import subprocess
import sys
from pathlib import Path

def main():
    """Run the Streamlit app"""
    app_path = Path(__file__).parent / "app.py"
    project_root = Path(__file__).parent.parent.parent.parent.parent
    
    try:
        # Change to project root directory for proper asset resolution
        import os
        os.chdir(project_root)
        
        # Run streamlit app
        subprocess.run([
            sys.executable, "-m", "streamlit", "run", 
            str(app_path),
            "--server.port", "8501",
            "--server.address", "0.0.0.0"
        ], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running Streamlit app: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\nStreamlit app stopped by user")
        sys.exit(0)

if __name__ == "__main__":
    main()
