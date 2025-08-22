# Development Setup Scripts

This directory contains cross-platform development setup scripts for the email scraper project.

## Quick Start

### Windows (PowerShell)
```powershell
# Run from project root
.\scripts\dev_setup.ps1
```

### Linux/macOS (Bash)
```bash
# Run from project root
./scripts/dev_setup.sh
```

## What These Scripts Do

Both scripts perform the same one-step bootstrap process:

1. **Environment Validation**: Check Python version (3.8+ required)
2. **Virtual Environment**: Create and activate a Python virtual environment
3. **Package Installation**: Install the scraper package in development mode with `pip install -e .[dev]`
4. **Browser Setup**: Install Playwright browsers for web scraping
5. **Development Tools**: Set up pre-commit hooks (if available)
6. **Smoke Test**: Verify installation by running `scraper --help`

## Options

### PowerShell Script (`dev_setup.ps1`)
```powershell
# Force recreation of virtual environment
.\scripts\dev_setup.ps1 -Force

# Use specific Python command
.\scripts\dev_setup.ps1 -PythonCmd "python3.11"
```

### Bash Script (`dev_setup.sh`)
```bash
# Force recreation of virtual environment
./scripts/dev_setup.sh --force

# Use specific Python command
./scripts/dev_setup.sh --python python3.11

# Show help
./scripts/dev_setup.sh --help
```

## Prerequisites

- **Python 3.8+**: Required for the email scraper
- **Git**: For cloning the repository
- **Internet Connection**: For downloading dependencies

### Windows Additional Requirements
- **PowerShell 5.0+**: Usually pre-installed on Windows 10+
- **Execution Policy**: Must allow local scripts (`RemoteSigned` or `Unrestricted`)

### Linux/macOS Additional Requirements
- **bash**: Standard shell (usually pre-installed)
- **Build tools**: May be needed for some Python packages
  - Ubuntu/Debian: `sudo apt install build-essential`
  - CentOS/RHEL: `sudo yum groupinstall "Development Tools"`
  - macOS: Install Xcode command line tools

## Troubleshooting

### Common Issues

**Python Version Issues**
```bash
# Check your Python version
python --version
python3 --version

# Use specific Python version
./scripts/dev_setup.sh --python python3.9
```

**Permission Issues (Linux/macOS)**
```bash
# Make script executable
chmod +x scripts/dev_setup.sh

# Run with explicit bash
bash scripts/dev_setup.sh
```

**PowerShell Execution Policy (Windows)**
```powershell
# Check current policy
Get-ExecutionPolicy

# Set policy for current user (if needed)
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

**Playwright Install Issues**
```bash
# Manual Playwright install
python -m playwright install

# Install system dependencies (Linux)
python -m playwright install-deps
```

### Virtual Environment Issues

If you encounter virtual environment issues:

1. **Remove and recreate**: Use `--force` or `-Force` flag
2. **Manual cleanup**: Delete the `venv` directory and run the script again
3. **Check Python installation**: Ensure Python and pip are working correctly

### Package Installation Issues

If `pip install -e .[dev]` fails:

1. **Update pip**: `python -m pip install --upgrade pip`
2. **Check pyproject.toml**: Ensure the file exists and is valid
3. **Install dependencies manually**: Check the `[dev]` extras in `pyproject.toml`

## Development Workflow

After running the setup script:

1. **Activate environment** (for future sessions):
   - Windows: `.\venv\Scripts\Activate.ps1`
   - Linux/macOS: `source venv/bin/activate`

2. **Verify installation**:
   ```bash
   python -m scraper --help
   ```

3. **Run tests**:
   ```bash
   python -m pytest
   ```

4. **Run linting**:
   ```bash
   python -m flake8 scraper/
   python -m black scraper/
   ```

## Integration with CI/CD

These scripts can be used in CI/CD pipelines:

### GitHub Actions Example
```yaml
- name: Set up development environment
  run: |
    chmod +x scripts/dev_setup.sh
    ./scripts/dev_setup.sh --python python3.9
  shell: bash
```

### Docker Example
```dockerfile
COPY scripts/dev_setup.sh /setup/
RUN chmod +x /setup/dev_setup.sh && /setup/dev_setup.sh
```

## Contributing

When modifying these scripts:

1. **Test on both platforms**: Windows PowerShell and Linux/macOS bash
2. **Update this README**: Document any new options or requirements
3. **Follow the contract**: Scripts should be one-step bootstrap solutions
4. **Handle errors gracefully**: Provide clear error messages and suggestions

## Support

If you encounter issues with these setup scripts:

1. **Check the logs**: Scripts provide detailed output
2. **Review prerequisites**: Ensure all requirements are met  
3. **Try manual setup**: Follow the steps manually to isolate issues
4. **Report bugs**: Include your OS, Python version, and error output
