#!/bin/bash
#
# Development setup script for Linux/macOS (Bash)
# One-step bootstrap for email scraper development environment

set -e  # Exit on any error

PYTHON_CMD="${1:-python3}"
FORCE=false

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --force)
            FORCE=true
            shift
            ;;
        --python)
            PYTHON_CMD="$2"
            shift 2
            ;;
        -h|--help)
            echo "Usage: $0 [--force] [--python PYTHON_CMD]"
            echo "  --force       Remove existing virtual environment"
            echo "  --python CMD  Python command to use (default: python3)"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

echo -e "\n🔧 \033[36mEmail Scraper Development Setup (Linux/macOS)\033[0m"
echo -e "\033[36m===================================================\033[0m\n"

# Check if we're in the project root
if [[ ! -f "pyproject.toml" ]]; then
    echo -e "❌ \033[31mERROR: pyproject.toml not found. Please run this script from the project root.\033[0m"
    exit 1
fi

echo -e "📋 \033[33mChecking prerequisites...\033[0m"

# Check if Python exists
if ! command -v "$PYTHON_CMD" &> /dev/null; then
    echo -e "❌ \033[31mPython not found: $PYTHON_CMD\033[0m"
    echo "Please install Python 3.8+ or specify with --python"
    exit 1
fi

# Check Python version
PYTHON_VERSION=$($PYTHON_CMD --version 2>&1 | cut -d' ' -f2)
PYTHON_MAJOR=$(echo "$PYTHON_VERSION" | cut -d'.' -f1)
PYTHON_MINOR=$(echo "$PYTHON_VERSION" | cut -d'.' -f2)

if [[ $PYTHON_MAJOR -lt 3 || ($PYTHON_MAJOR -eq 3 && $PYTHON_MINOR -lt 8) ]]; then
    echo -e "❌ \033[31mPython 3.8+ required. Found: Python $PYTHON_VERSION\033[0m"
    exit 1
fi

echo -e "✅ \033[32mPython: $PYTHON_VERSION\033[0m"

# Check if virtual environment exists
if [[ -d "venv" ]]; then
    if [[ "$FORCE" == "true" ]]; then
        echo -e "🗑️  \033[33mRemoving existing virtual environment...\033[0m"
        rm -rf venv
    else
        echo -e "⚠️  \033[33mVirtual environment already exists.\033[0m"
        read -p "Continue with existing environment? (Y/n): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Nn]$ ]]; then
            echo "Use --force to recreate the environment"
            exit 0
        fi
    fi
fi

# Create virtual environment
if [[ ! -d "venv" ]]; then
    echo -e "🔨 \033[33mCreating virtual environment...\033[0m"
    $PYTHON_CMD -m venv venv
    echo -e "✅ \033[32mVirtual environment created\033[0m"
fi

# Activate virtual environment
echo -e "🔌 \033[33mActivating virtual environment...\033[0m"
source venv/bin/activate

# Verify activation
if [[ "$VIRTUAL_ENV" == "" ]]; then
    echo -e "❌ \033[31mVirtual environment activation failed\033[0m"
    exit 1
fi

echo -e "✅ \033[32mVirtual environment activated\033[0m"

# Upgrade pip
echo -e "⬆️  \033[33mUpgrading pip...\033[0m"
python -m pip install --upgrade pip || echo -e "⚠️  \033[33mPip upgrade failed, continuing...\033[0m"

# Install package in development mode
echo -e "📦 \033[33mInstalling package in development mode...\033[0m"
python -m pip install -e ".[dev]"
echo -e "✅ \033[32mPackage installed in development mode\033[0m"

# Install Playwright browsers
echo -e "🎭 \033[33mInstalling Playwright browsers...\033[0m"
if python -m playwright install > /dev/null 2>&1; then
    echo -e "✅ \033[32mPlaywright browsers installed\033[0m"
else
    echo -e "⚠️  \033[33mPlaywright install failed. You may need to install manually: python -m playwright install\033[0m"
fi

# Install pre-commit hooks (if pre-commit is available)
echo -e "🎣 \033[33mSetting up pre-commit hooks...\033[0m"
if python -m pre_commit install > /dev/null 2>&1; then
    echo -e "✅ \033[32mPre-commit hooks installed\033[0m"
else
    echo -e "⚠️  \033[33mPre-commit not available or failed to install hooks\033[0m"
fi

echo ""
echo -e "🎉 \033[32mDevelopment setup complete!\033[0m"
echo -e "\033[36m===================================================\033[0m"
echo ""
echo -e "\033[37mTo activate the environment in future sessions:\033[0m"
echo -e "  \033[36msource venv/bin/activate\033[0m"
echo ""
echo -e "\033[37mTo test the installation:\033[0m"
echo -e "  \033[36mpython -m scraper --help\033[0m"
echo ""

# Test the installation
echo -e "🧪 \033[33mTesting installation...\033[0m"
if python -m scraper --help > /dev/null 2>&1; then
    echo -e "✅ \033[32mInstallation test passed\033[0m"
else
    echo -e "⚠️  \033[33mInstallation test failed. Check the logs above.\033[0m"
fi

echo ""
echo -e "Happy coding! 🚀 \033[35m\033[0m"
