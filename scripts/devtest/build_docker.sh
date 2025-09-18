#!/bin/bash

# =============================================================================
# Universal Docker Build Script for ZealotGenAI Apps
# =============================================================================
#
# This script creates Dockerfiles for Python applications in the zealot project.
# It can auto-detect common app paths or use explicit paths.
#
# USAGE:
#   ./build_docker.sh <app_name> [app_path]
#   ./build_docker.sh --help             # Show help
#
# EXAMPLES:
#   ./build_docker.sh --help             # Show help
#   ./build_docker.sh langchain          # Build app (auto-detects path)
#   ./build_docker.sh myapp zealot/apps/myapp  # Build custom app with explicit path
#
# REQUIREMENTS:
#   - Each app folder must contain a requirements.txt file
#   - The script will skip apps without requirements.txt
#   - Dockerfiles are created in the Dockerfiles/ directory
#
# AUTHOR: Jyoti Ranjan (https://www.linkedin.com/in/jyoti-ranjan-5083595/)
# =============================================================================

set -e  # Exit on any error

# =============================================================================
# CONFIGURATION
# =============================================================================

# Get the script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
DOCKERFILES_DIR="$PROJECT_ROOT/Dockerfiles"

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
        "zealot/apps/$app_name"
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

# Function to display usage information and exit
# Shows help text with examples
show_usage() {
    echo "Usage: $0 <app_name> [app_path]"
    echo "       $0 --help"
    echo ""
    echo "Options:"
    echo "  --help, -h, help    Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 --help                           # Show this help message"
    echo "  $0 langchain                         # Build app (auto-detects path)"
    echo "  $0 myapp zealot/apps/myapp           # Build custom app with explicit path"
    echo ""
    echo "Note: Each app folder must contain a requirements.txt file"
    exit 0
}

# =============================================================================
# COMMAND LINE ARGUMENT PARSING
# =============================================================================

# Parse command line arguments
APP_NAME="$1"
APP_PATH="$2"

# Check for help option
if [ "$APP_NAME" = "--help" ] || [ "$APP_NAME" = "-h" ] || [ "$APP_NAME" = "help" ]; then
    show_usage
fi

# Validate required app name
if [ -z "$APP_NAME" ]; then
    echo "❌ Error: app_name is required."
    echo "Use: $0 <app_name> [app_path]"
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
        echo "Common patterns checked: zealot/apps/$APP_NAME, apps/$APP_NAME, $APP_NAME"
        exit 1
    fi
fi

echo "🚀 Building Docker configuration for app: $APP_NAME"

# Create Dockerfiles directory if it doesn't exist
mkdir -p "$DOCKERFILES_DIR"

# =============================================================================
# DOCKERFILE CREATION FUNCTIONS
# =============================================================================

# Function to create Dockerfile for an app
# Args: app_name - name of the app, app_path - path to the app folder
# Creates a Dockerfile in the Dockerfiles directory
create_dockerfile() {
    local app_name="$1"
    local app_path="$2"
    local dockerfile_path="$DOCKERFILES_DIR/Dockerfile.$app_name"
    
    echo "📦 Building Dockerfile for $app_name app..."
    
    # Generate Dockerfile content using heredoc
    cat > "$dockerfile_path" << EOF
# Use Python 3.11 slim image
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Set environment variables
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Install system dependencies
RUN apt-get update && apt-get install -y \\
    gcc \\
    g++ \\
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY $app_path/requirements.txt /app/requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire project
COPY . /app/

# Expose port (adjust as needed)
EXPOSE 8000

# Set the default command
CMD ["python", "-m", "$app_path"]
EOF

    echo "✅ Dockerfile created: $dockerfile_path"
}

# Function to check if requirements.txt exists for an app
# Args: app_name - name of the app, app_path - path to the app folder
# Returns: 0 if requirements.txt exists, 1 if not found
check_requirements() {
    local app_name="$1"
    local app_path="$2"
    local requirements_path="$PROJECT_ROOT/$app_path/requirements.txt"
    
    # Create directory if it doesn't exist
    mkdir -p "$(dirname "$requirements_path")"
    
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

# Check if requirements.txt exists before creating Dockerfile
if check_requirements "$APP_NAME" "$APP_PATH"; then
    # Create Dockerfile only if requirements.txt exists
    create_dockerfile "$APP_NAME" "$APP_PATH"
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
    echo "🎉 Docker configuration built successfully!"
    echo ""
    echo "📁 Generated files:"
    echo "   - Dockerfiles/Dockerfile.$APP_NAME"
    echo "   - $APP_PATH/requirements.txt"
    echo ""
    echo "🔧 To build and run the app:"
    echo "   docker build -f Dockerfiles/Dockerfile.$APP_NAME -t zealot-$APP_NAME ."
else
    echo "⚠️  No Docker configuration was built (requirements.txt not found)"
fi

echo ""
echo "💡 Usage examples:"
echo "   $0 langchain                         # Build app (auto-detects path)"
echo "   $0 myapp zealot/apps/myapp           # Build custom app with explicit path"
echo ""
echo "📝 Note: Each app folder must have a requirements.txt file"
echo "   The script will use existing requirements.txt files and skip apps without them"
