#!/usr/bin/env pwsh
#
# Development setup script for Windows (PowerShell)
# One-step bootstrap for email scraper development environment

param(
    [switch]$Force,
    [string]$PythonCmd = "python"
)

Write-Host "🔧 Email Scraper Development Setup (Windows)" -ForegroundColor Cyan
Write-Host "=================================================" -ForegroundColor Cyan
Write-Host ""

# Check if we're in the project root
if (!(Test-Path "pyproject.toml")) {
    Write-Error "❌ ERROR: pyproject.toml not found. Please run this script from the project root."
    exit 1
}

# Function to check if command exists
function Test-Command {
    param($Command)
    try {
        Get-Command $Command -ErrorAction Stop | Out-Null
        return $true
    }
    catch {
        return $false
    }
}

Write-Host "📋 Checking prerequisites..." -ForegroundColor Yellow

# Check Python
if (!(Test-Command $PythonCmd)) {
    Write-Error "❌ Python not found. Please install Python 3.8+ or specify path with -PythonCmd"
    exit 1
}

# Check Python version
$pythonVersion = & $PythonCmd --version 2>&1 | Out-String
if ($pythonVersion -match "Python (\d+)\.(\d+)") {
    $major = [int]$matches[1]
    $minor = [int]$matches[2]
    if ($major -lt 3 -or ($major -eq 3 -and $minor -lt 8)) {
        Write-Error "❌ Python 3.8+ required. Found: $pythonVersion"
        exit 1
    }
    Write-Host "✅ Python: $pythonVersion" -ForegroundColor Green
} else {
    Write-Error "❌ Unable to determine Python version"
    exit 1
}

# Check if virtual environment exists
$venvExists = Test-Path "venv" -PathType Container
if ($venvExists -and !$Force) {
    Write-Host "⚠️  Virtual environment already exists. Use -Force to recreate." -ForegroundColor Yellow
    $response = Read-Host "Continue with existing environment? (Y/n)"
    if ($response -eq "n" -or $response -eq "N") {
        exit 0
    }
} elseif ($venvExists -and $Force) {
    Write-Host "🗑️  Removing existing virtual environment..." -ForegroundColor Yellow
    Remove-Item -Path "venv" -Recurse -Force
}

# Create virtual environment
if (!$venvExists -or $Force) {
    Write-Host "🔨 Creating virtual environment..." -ForegroundColor Yellow
    & $PythonCmd -m venv venv
    if ($LASTEXITCODE -ne 0) {
        Write-Error "❌ Failed to create virtual environment"
        exit 1
    }
    Write-Host "✅ Virtual environment created" -ForegroundColor Green
}

# Activate virtual environment
Write-Host "🔌 Activating virtual environment..." -ForegroundColor Yellow
$activateScript = "venv\Scripts\Activate.ps1"
if (!(Test-Path $activateScript)) {
    Write-Error "❌ Activation script not found: $activateScript"
    exit 1
}

# Source the activation script
& $activateScript

# Verify activation
$virtualPython = "venv\Scripts\python.exe"
if (!(Test-Path $virtualPython)) {
    Write-Error "❌ Virtual environment activation failed"
    exit 1
}

Write-Host "✅ Virtual environment activated" -ForegroundColor Green

# Upgrade pip
Write-Host "⬆️  Upgrading pip..." -ForegroundColor Yellow
& $virtualPython -m pip install --upgrade pip
if ($LASTEXITCODE -ne 0) {
    Write-Warning "⚠️  Pip upgrade failed, continuing..."
}

# Install package in development mode
Write-Host "📦 Installing package in development mode..." -ForegroundColor Yellow
& $virtualPython -m pip install -e ".[dev]"
if ($LASTEXITCODE -ne 0) {
    Write-Error "❌ Failed to install package in development mode"
    exit 1
}
Write-Host "✅ Package installed in development mode" -ForegroundColor Green

# Install Playwright browsers
Write-Host "🎭 Installing Playwright browsers..." -ForegroundColor Yellow
& $virtualPython -m playwright install
if ($LASTEXITCODE -ne 0) {
    Write-Warning "⚠️  Playwright install failed. You may need to install manually: python -m playwright install"
} else {
    Write-Host "✅ Playwright browsers installed" -ForegroundColor Green
}

# Install pre-commit hooks (if pre-commit is available)
Write-Host "🎣 Setting up pre-commit hooks..." -ForegroundColor Yellow
try {
    & $virtualPython -m pre_commit install 2>&1 | Out-Null
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✅ Pre-commit hooks installed" -ForegroundColor Green
    } else {
        Write-Warning "⚠️  Pre-commit not available or failed to install hooks"
    }
} catch {
    Write-Warning "⚠️  Pre-commit not available"
}

Write-Host ""
Write-Host "🎉 Development setup complete!" -ForegroundColor Green
Write-Host "=================================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "To activate the environment in future sessions:" -ForegroundColor White
Write-Host "  .\venv\Scripts\Activate.ps1" -ForegroundColor Cyan
Write-Host ""
Write-Host "To test the installation:" -ForegroundColor White
Write-Host "  python -m scraper --help" -ForegroundColor Cyan
Write-Host ""

# Test the installation
Write-Host "🧪 Testing installation..." -ForegroundColor Yellow
& $virtualPython -m scraper --help > $null 2>&1
if ($LASTEXITCODE -eq 0) {
    Write-Host "✅ Installation test passed" -ForegroundColor Green
} else {
    Write-Warning "⚠️  Installation test failed. Check the logs above."
}

Write-Host ""
Write-Host "Happy coding! 🚀" -ForegroundColor Magenta
