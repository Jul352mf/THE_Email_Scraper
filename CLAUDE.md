# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

THE Email Scraper is a Python application that extracts contact emails from company websites. It supports static scraping, sitemap parsing, and dynamic content rendering via Playwright. Input is via Excel files with a "Company" column, output is structured email data.

## Key Commands

### Setup and Installation
```bash
# Create and activate virtual environment
python -m venv .venv
# Windows PowerShell:
. .\.venv\Scripts\Activate.ps1
# POSIX (bash/zsh):
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Install Playwright browsers (required for dynamic rendering)
playwright install
```

### Running the Application
```bash
# Basic usage
scraper input.xlsx output.xlsx

# With environment variables (POSIX)
MAX_WORKERS=8 scraper input.xlsx output.xlsx

# With environment variables (PowerShell)
$env:MAX_WORKERS=8; scraper input.xlsx output.xlsx

# CLI options
scraper input.xlsx output.xlsx --workers 8 --verbose --process-pdfs
```

### Testing
```bash
# Run tests using pytest
pytest tests/

# Run specific test
pytest tests/test_tokenbucket.py
```

## Code Architecture

### Core Components

**Entry Point**: `__main__.py` and `scraper/cli.py` - CLI interface and argument parsing
**Configuration**: `scraper/config.py` - Centralized config with environment variable support
**Orchestration**: `scraper/orchestrator.py` - Main workflow coordination and company processing
**HTTP Layer**: `scraper/http.py`, `scraper/https_client.py`, `scraper/http_utils.py` - HTTP requests with rate limiting via TokenBucket

### Processing Pipeline
1. **Domain Resolution**: `scraper/google_search.py` + `scraper/domain_scorer.py` - Find and score company domains
2. **Content Discovery**: `scraper/sitemap.py` + `scraper/crawler.py` - Parse sitemaps and crawl pages
3. **Dynamic Rendering**: `scraper/browser_service.py` - Playwright-based rendering for SPAs/login flows
4. **Email Extraction**: `scraper/email_extractor.py`, `scraper/hybrid_email_extractor.py` - Extract emails from HTML/PDFs

### Special Features
- **Canvas Integration**: `Canvas/` directory contains Canvas LMS scraping with Playwright authentication
- **Rate Limiting**: TokenBucket implementation in `http.py` prevents overwhelming servers
- **PDF Processing**: Optional PDF text extraction for email discovery
- **Caching**: `scraper/cache.py` for response caching

### Configuration
Environment variables are defined in `scraper/config.py`. Key settings:
- `GOOGLE_API_KEY`, `GOOGLE_CX_ID` - Required for Google Search API
- `MAX_WORKERS` - Concurrent processing threads (default: 4)
- `PROCESS_PDFS` - Enable PDF email extraction (default: False)
- `DOMAIN_SCORE_THRESHOLD` - Domain relevance threshold (default: 60)

Create a `.env` file in project root for local configuration.

### Development Notes
- All modules use structured logging via Python's `logging` module
- HTTP requests go through centralized `http_client` with stats tracking
- Browser automation uses multiprocessing for isolation
- Error handling is comprehensive with custom exception classes
- Uses pandas for Excel I/O operations

### Testing Framework
Uses pytest for testing. Current test coverage includes concurrency tests for TokenBucket rate limiting.