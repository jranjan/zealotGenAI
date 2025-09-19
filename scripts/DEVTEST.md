# Development & Testing Tools

This document provides an overview of all development and testing tools available in the `scripts/` directory.

# Overview

The scripts in this directory are designed to streamline development workflows, automate common tasks, and provide consistent environments for testing and deployment. These tools handle Docker containerization, virtual environment management, and other development utilities.

# Tool Summary

| Tool Name                   | Script Name               | Purpose                                                           |
|-----------------------------|---------------------------|-------------------------------------------------------------------|
| Docker Builder              | `devtest/build_docker.sh` | Generate Dockerfiles and build Docker containers for applications |
| Virtual Environment Builder | `devtest/build_venv.sh`   | Create and manage Python virtual environments for applications    |

---

# Docker Builder (`devtest/build_docker.sh`)

**Purpose**: Automatically generates Dockerfiles and builds Docker containers for applications in the project.

### Features
- Auto-detects application paths based on common patterns
- Uses existing `requirements.txt` files from app directories
- Supports explicit path specification
- Creates parent directories if they don't exist
- Provides helpful error messages and usage guidance

### Usage

| Usage | Command |
|-------|---------|
| Basic (auto-detect) | `./scripts/devtest/build_docker.sh <app_name>` |
| Examples | `./scripts/devtest/build_docker.sh langchain`<br>`./scripts/devtest/build_docker.sh openrouter`<br>`./scripts/devtest/build_docker.sh hello` |
| Explicit path | `./scripts/devtest/build_docker.sh myapp /path/to/my/app` |
| Help | `./scripts/devtest/build_docker.sh --help` |

### Requirements
- The target app directory must contain a `requirements.txt` file
- The script will create the `Dockerfiles/` directory if it doesn't exist
- Generated Dockerfiles will be saved to `Dockerfiles/<app_name>.Dockerfile`

### Auto-Detection Logic
The script automatically searches for app paths in this order:
1. `zealot/apps/llm/<app_name>/` (for LLM apps)
2. `zealot/apps/<app_name>/`
3. `apps/llm/<app_name>/`
4. `apps/<app_name>/`
5. Root directory `<app_name>/`

### Generated Dockerfile Structure
```dockerfile
FROM python:3.11-slim
WORKDIR /app
ENV PYTHONPATH=/app
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
EXPOSE 8000
CMD ["python", "app.py"]
```

---

# Virtual Environment Builder (`devtest/build_venv.sh`)

**Purpose**: Creates and manages Python virtual environments for applications under the `alpha/` directory.

### Features
- Auto-detects application paths based on common patterns
- Uses existing `requirements.txt` files from app directories
- Supports explicit path specification
- Creates virtual environments in `alpha/<app_name>/` directory
- Force recreation option with confirmation prompt
- Installs dependencies automatically

### Usage

| Usage | Command |
|-------|---------|
| Basic (auto-detect) | `./scripts/devtest/build_venv.sh <app_name>` |
| Examples | `./scripts/devtest/build_venv.sh langchain`<br>`./scripts/devtest/build_venv.sh openrouter`<br>`./scripts/devtest/build_venv.sh hello` |
| Explicit path | `./scripts/devtest/build_venv.sh myapp /path/to/my/app` |
| Force recreation | `./scripts/devtest/build_venv.sh --force <app_name>` |
| Help | `./scripts/devtest/build_venv.sh --help` |

### Requirements
- The target app directory must contain a `requirements.txt` file
- Python 3.x must be available in the system PATH
- The script will create the `alpha/` directory if it doesn't exist

### Auto-Detection Logic
The script automatically searches for app paths in this order:
1. `zealot/apps/llm/<app_name>/` (for LLM apps)
2. `zealot/apps/<app_name>/`
3. `apps/llm/<app_name>/`
4. `apps/<app_name>/`
5. Root directory `<app_name>/`

### Virtual Environment Structure
```
alpha/
├── <app_name>/
│   ├── bin/
│   ├── lib/
│   ├── include/
│   └── pyvenv.cfg
└── ...
```

### Interactive Confirmation
If a virtual environment already exists and `--force` is not used:
```
Virtual environment already exists at alpha/<app_name>
Do you want to recreate it? (y/N):
```

### Activation
After creation, activate the virtual environment:
```bash
source alpha/<app_name>/bin/activate
```

---

## Common Workflows

### Quick Setup
```bash
# 1. Create app structure
mkdir -p zealot/apps/my_app
cd zealot/apps/my_app
echo "requests==2.31.0" > requirements.txt

# 2. Create venv
./scripts/devtest/build_venv.sh my_app

# 3. Activate and test
source alpha/my_app/bin/activate
python -c "import requests; print('Success!')"

# 4. Create Docker
./scripts/devtest/build_docker.sh my_app
```

### Daily Development
```bash
# Start development
source alpha/my_app/bin/activate
python my_app.py

# Update dependencies
vim zealot/apps/my_app/requirements.txt
./scripts/devtest/build_venv.sh --force my_app

# Test in Docker
./scripts/devtest/build_docker.sh my_app
docker build -f Dockerfiles/my_app.Dockerfile -t my_app .
docker run -p 8000:8000 my_app
```

---

## Troubleshooting

### Common Issues

1. **"No such file or directory" error:**
   - Ensure the app directory exists
   - Check that `requirements.txt` is present
   - Verify the app name is correct

2. **"Package not found" error:**
   - Check that `requirements.txt` contains valid package names
   - Ensure internet connection for package installation

3. **Permission denied:**
   - Make scripts executable: `chmod +x scripts/devtest/*.sh`
   - Check directory permissions

4. **Python not found:**
   - Ensure Python 3.x is installed and in PATH
   - Try using `python3` instead of `python`

### Getting Help

- Use `--help` flag with any script
- Check script output for specific error messages
- Ensure all requirements are met before running scripts

---

## Notes

- All scripts are designed to be idempotent (safe to run multiple times)
- Scripts will create necessary directories automatically
- Virtual environments are isolated per application
- Docker containers are built from the project root directory
- Scripts follow Unix conventions and should work on macOS and Linux
