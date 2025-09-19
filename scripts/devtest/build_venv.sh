#!/bin/bash

# =============================================================================
# Virtual Environment Build Script for ZealotGenAI Apps
# =============================================================================
#
# This script builds virtual environments for Python applications in the zealot project.
# It can auto-detect common app paths or use explicit paths.
#
# USAGE:
#   ./build_venv.sh <app_name> [app_path] [--force]
#   ./build_venv.sh --help             # Show help
#
# EXAMPLES:
#   ./build_venv.sh --help             # Show help
#   ./build_venv.sh langchain          # Build venv (auto-detects path)
#   ./build_venv.sh myapp zealot/apps/myapp  # Build venv with explicit path
#   ./build_venv.sh langchain --force  # Recreate venv if it exists
#
# REQUIREMENTS:
#   - Each app folder must contain a requirements.txt file
#   - Virtual environments are built in alpha/{app_name}/venv/
#   - Each app name must be unique in the repository
#
# AUTHOR: Jyoti Ranjan (https://www.linkedin.com/in/jyoti-ranjan-5083595/)
# =============================================================================

set -e  # Exit on any error

# =============================================================================
# CONFIGURATION
# =============================================================================

# Get the script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$(dirname "$SCRIPT_DIR")")"
ALPHA_DIR="$PROJECT_ROOT/alpha"

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

# Function to auto-detect common app paths
# Args: app_name - name of the app
# Returns: detected path if found, empty string if not found
auto_detect_path() {
    local app_name="$1"
    
    # Common patterns to check
    local patterns=(
        "zealot/apps/llm/$app_name"
        "zealot/apps/$app_name"
        "apps/llm/$app_name"
        "apps/$app_name"
        "$app_name"
    )
    
    for pattern in "${patterns[@]}"; do
        local full_path="$PROJECT_ROOT/$pattern"
        if [ -d "$full_path" ] && [ -f "$full_path/requirements.txt" ]; then
            echo "$pattern"
            return 0
        fi
    done
    
    return 1
}

# Function to ask for confirmation
# Args: message - the message to display
# Returns: 0 if user confirms (y/Y), 1 if user declines (n/N)
ask_confirmation() {
    local message="$1"
    while true; do
        read -p "$message (y/N): " -n 1 -r
        echo
        case $REPLY in
            [Yy] ) return 0;;
            [Nn] ) return 1;;
            ""   ) return 1;;  # Default to No for empty input
            *    ) echo "Please answer y or n.";;
        esac
    done
}

# Function to display usage information and exit
# Shows help text with examples
show_usage() {
    echo "Usage: $0 <app_name> [app_path] [--force]"
    echo "       $0 --help"
    echo ""
    echo "Options:"
    echo "  --help, -h, help    Show this help message"
    echo "  --force             Recreate virtual environment if it already exists"
    echo ""
    echo "Examples:"
    echo "  $0 --help                           # Show this help message"
    echo "  $0 langchain                         # Build venv (auto-detects path)"
    echo "  $0 myapp zealot/apps/myapp           # Build venv with explicit path"
    echo "  $0 langchain --force                 # Recreate venv if it exists"
    echo ""
    echo "Note: Each app folder must contain a requirements.txt file"
    echo "Virtual environments are built in alpha/{app_name}/"
    exit 0
}

# =============================================================================
# COMMAND LINE ARGUMENT PARSING
# =============================================================================

# Parse command line arguments
APP_NAME="$1"
APP_PATH="$2"
FORCE_RECREATE=false

# Check for help option
if [ "$APP_NAME" = "--help" ] || [ "$APP_NAME" = "-h" ] || [ "$APP_NAME" = "help" ]; then
    show_usage
fi

# Check for --force option in any position
for arg in "$@"; do
    if [ "$arg" = "--force" ]; then
        FORCE_RECREATE=true
        break
    fi
done

# Remove --force from arguments for processing
ARGS=()
for arg in "$@"; do
    if [ "$arg" != "--force" ]; then
        ARGS+=("$arg")
    fi
done

# Reassign arguments after removing --force
APP_NAME="${ARGS[0]}"
APP_PATH="${ARGS[1]}"

# Validate required app name
if [ -z "$APP_NAME" ]; then
    echo "❌ Error: app_name is required."
    echo "Use: $0 <app_name> [app_path] [--force]"
    echo "Use: $0 --help for more information"
    exit 1
fi

# Auto-detect path if not provided
if [ -z "$APP_PATH" ]; then
    echo "🔍 Auto-detecting path for app: $APP_NAME"
    if APP_PATH=$(auto_detect_path "$APP_NAME"); then
        echo "✅ Found app at: $APP_PATH"
    else
        echo "❌ Error: Could not auto-detect path for '$APP_NAME'"
        echo "Please provide explicit path: $0 $APP_NAME <app_path>"
        echo "Common patterns checked: zealot/apps/llm/$APP_NAME, zealot/apps/$APP_NAME, apps/llm/$APP_NAME, apps/$APP_NAME, $APP_NAME"
        exit 1
    fi
fi

echo "🚀 Creating virtual environment for app: $APP_NAME"

# Create alpha directory if it doesn't exist
mkdir -p "$ALPHA_DIR"

# =============================================================================
# VIRTUAL ENVIRONMENT BUILD FUNCTIONS
# =============================================================================

# Function to build virtual environment for an app
# Args: app_name - name of the app, app_path - path to the app folder, force - whether to force recreate
# Builds a virtual environment in alpha/{app_name}/
build_venv() {
    local app_name="$1"
    local app_path="$2"
    local force="$3"
    local venv_path="$ALPHA_DIR/$app_name"
    local requirements_path="$PROJECT_ROOT/$app_path/requirements.txt"
    
    echo "📦 Building virtual environment for $app_name app..."
    
    # Create app directory in alpha
    mkdir -p "$ALPHA_DIR/$app_name"
    
    # Check if virtual environment already exists
    if [ -d "$venv_path" ]; then
        if [ "$force" = true ]; then
            echo "🗑️  Removing existing virtual environment at: $venv_path"
            rm -rf "$venv_path"
        else
            echo "⚠️  Virtual environment already exists at: $venv_path"
            if ask_confirmation "Do you want to recreate it?"; then
                echo "🗑️  Removing existing virtual environment..."
                rm -rf "$venv_path"
            else
                echo "⏭️  Skipping virtual environment creation."
                return 0
            fi
        fi
    fi
    
    echo "🐍 Creating Python virtual environment..."
    python3 -m venv "$venv_path"
    
    # Activate virtual environment and install requirements
    echo "📦 Installing requirements from: $requirements_path"
    source "$venv_path/bin/activate"
    pip install --upgrade pip
    pip install -r "$requirements_path"
    deactivate
    
    echo "✅ Virtual environment built: $venv_path"
}

# Function to check if requirements.txt exists for an app
# Args: app_name - name of the app, app_path - path to the app folder
# Returns: 0 if requirements.txt exists, 1 if not found
check_requirements() {
    local app_name="$1"
    local app_path="$2"
    local requirements_path="$PROJECT_ROOT/$app_path/requirements.txt"
    
    if [ -f "$requirements_path" ]; then
        echo "✅ Using existing requirements.txt: $requirements_path"
        return 0
    else
        echo "⚠️  No requirements.txt found for $app_name app at: $requirements_path"
        echo "   Please create a requirements.txt file in the app folder first."
        return 1
    fi
}

# =============================================================================
# MAIN PROCESSING
# =============================================================================

echo ""
echo "🔧 Processing $APP_NAME app..."

# Check if requirements.txt exists before building venv
if check_requirements "$APP_NAME" "$APP_PATH"; then
    # Build virtual environment only if requirements.txt exists
    build_venv "$APP_NAME" "$APP_PATH" "$FORCE_RECREATE"
    SUCCESS=true
else
    echo "❌ Skipping $APP_NAME app - requirements.txt not found"
    SUCCESS=false
fi

# =============================================================================
# FINAL SUMMARY AND OUTPUT
# =============================================================================

echo ""
if [ "$SUCCESS" = true ]; then
    echo "🎉 Virtual environment built successfully!"
    echo ""
    echo "📁 Generated files:"
    echo "   - alpha/$APP_NAME/venv/"
    echo "   - $APP_PATH/requirements.txt (used for installation)"
    echo ""
    echo "🔧 To activate the virtual environment:"
    echo "   source alpha/$APP_NAME/bin/activate"
    echo ""
    echo "🔧 To deactivate:"
    echo "   deactivate"
else
    echo "⚠️  No virtual environment was built (requirements.txt not found)"
fi

echo ""
echo "💡 Usage examples:"
echo "   $0 langchain                         # Build venv (auto-detects path)"
echo "   $0 myapp zealot/apps/myapp           # Build venv with explicit path"
echo "   $0 langchain --force                 # Recreate venv if it exists"
echo ""
echo "📝 Note: Each app folder must have a requirements.txt file"
echo "   Virtual environments are built in alpha/{app_name}/"
