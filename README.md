# THE Email Scraper — Advanced Email Discovery Tool

## Purpose

Extract contact emails and related pages for companies using intelligent discovery strategies. Supports multiple input formats (Excel, CSV), batch processing, smart page prioritization, and dynamic content rendering via Playwright.

## ✨ Key Features

- **🚀 High-Performance Processing**: Concurrent domain processing with intelligent caching and connection pooling
- **�️ Smart Retry Logic**: Circuit breaker pattern with exponential backoff for resilient HTTP requests (NEW!)
- **📈 Performance Monitoring**: Real-time performance metrics integrated into CLI output (NEW!)
- **�📊 Multiple Input Formats**: Excel (.xlsx, .xls) and CSV (.csv) support with encoding detection
- **📁 Batch Processing**: Automated processing of all files in input directory
- **🧠 Smart Discovery**: AI-powered page prioritization (Contact > About > Team pages)
- **⚡ Intelligent Stopping**: Configurable stopping criteria for thorough email discovery
- **📧 Email Pattern Caching**: Learn and reuse email patterns per domain
- **🔍 Dynamic Content**: Playwright browser automation for SPAs and login flows
- **🔄 Google API Caching**: TTL-based caching eliminates redundant Google API calls
- **🔗 Connection Pooling**: Optimized HTTP sessions with retry logic and connection reuse
- **⚡ Request Batching**: Parallel domain probing and batch processing for maximum speed

## Prerequisites

- Python 3.10+ (project has been run with Python 3.13 in logs)
- A virtual environment is strongly recommended

## 🚀 Quick Start

1. Create a virtual environment and activate it

- POSIX (bash / zsh / macOS / Linux):

    ```bash
    python -m venv .venv
    source .venv/bin/activate
    ```

- PowerShell (Windows):

    ```powershell
    python -m venv .venv
    . .\.venv\Scripts\Activate.ps1
    ```

2. Install dependencies

    ```bash
    pip install -r requirements.txt
    ```

3. Install Playwright browsers (required for dynamic rendering)

    ```bash
    # inside the activated virtualenv
    playwright install
    ```

    If you see errors like "Executable doesn't exist...", re-run this step inside the activated virtualenv.

4. Configure environment (create a `.env` file in the project root)

Minimum recommended variables:

- `GOOGLE_API_KEY` and `GOOGLE_CX_ID` (for Google site search path)
- `MAX_WORKERS`, `PROCESS_PDFS`, `DOMAIN_SCORE_THRESHOLD`, etc. — see `scraper/config.py` for defaults.

5. Run the scraper

### Single File Processing
```bash
# Excel file
scraper companies.xlsx results.xlsx

# CSV file (with automatic encoding detection)
scraper companies.csv results.xlsx

# With performance options
scraper input.xlsx output.xlsx --workers 8 --verbose
```text

### Batch Processing (New!)
```bash
# Process all files in input/ directory
scraper --batch

# Custom directories and options
scraper --batch --input-dir data --output-dir results --log-dir logs --workers 8

# Verbose batch processing with detailed logs
scraper --batch --verbose
```text

### Environment Overrides
```bash
# POSIX (Linux/macOS)
MAX_WORKERS=8 scraper companies.xlsx results.xlsx

# PowerShell (Windows)
$env:MAX_WORKERS=8; scraper companies.xlsx results.xlsx
```

## 📁 File & Module Organization (Async Streaming Architecture)

Active runtime structure:

```
scraper/
    cli.py                  # Entry point / argument parsing, streaming writes
    config.py               # Configuration & env parsing
    async_orchestrator.py   # Streaming company -> domain pipeline
    async_google_search.py  # Parallel & streaming Google queries
    async_http_client.py    # HTTP layer (pooling, rate limit, breaker)
    async_components.py     # Circuit breaker, token bucket, session manager
    async_browser_service.py# Playwright non-blocking rendering
    domain_scorer.py        # Domain scoring heuristic
    email_extractor.py      # Regex extraction
    hybrid_email_extractor.py# Sync cache + fallback reuse
    smart_discovery.py      # Priority URL generation & filtering
```

Data dirs (created as needed):
```
input/   output/   logs/
```

Removed: legacy `archive/` modules (deprecated sync prototypes).

## 🧠 Smart Discovery Features

### Intelligent Page Prioritization
- **Contact pages** processed first (contact, contacts, contact-us)
- **About pages** prioritized (about, about-us, team, staff)
- **Career pages** included (careers, jobs, employment)
- **Generic pages** processed last (blog, news, products)

### Early Stopping (Configurable)
- **Smart stopping**: Configurable stopping criteria (default: disabled for thorough discovery)
- **Environment control**: `EARLY_STOP_ENABLED=false` for maximum email discovery
- **Threshold setting**: `EARLY_STOP_THRESHOLD` for custom stopping points
- **Performance vs. thoroughness**: Balance speed and completeness based on needs

### Email Pattern Caching
- Learns email patterns per domain
- Filters out personal emails (gmail, yahoo, hotmail)
- Prioritizes business emails from same domain
- Improves accuracy over time

## 🆕 Latest Improvements (2025 Async Wave)

### Async Streaming Orchestrator (TASK-045)
- Streams Google results and launches domain tasks immediately
- Row-level streaming to disk → constant memory footprint
- Per-domain concurrency semaphore prevents overload

### Smart Retry Logic & Circuit Breaker (Earlier TASK-039)
- **Circuit Breaker Pattern**: Automatically stops requesting from consistently failing domains
- **Exponential Backoff**: Intelligent retry delays with jitter to prevent thundering herd
- **Selective Retry Logic**: Doesn't retry client errors (4xx), focuses on server errors (5xx)
- **Thread-Safe**: Works correctly in multi-threaded environment
- **Performance Impact**: Reduces wasted time on unreachable domains by up to 90%

### Integrated Performance Monitoring (TASK-044)
- **Real-time Metrics**: Performance stats displayed in CLI output after each run
- **Cache Analytics**: Hit/miss ratios help optimize caching effectiveness
- **Request Timing**: Average request times and throughput monitoring
- **Worker Suggestions**: System recommends optimal worker count based on performance
- **Connection Stats**: Per-domain connection reuse tracking

### Performance Analysis & 10x Improvement Plan
- **Bottleneck Analysis**: Identified sequential Google API calls as major bottleneck
- **Async Roadmap**: Detailed plan for async/await migration for 5-10x speed gains
- **Parallel Processing**: Strategy for concurrent URL processing within companies
- **Browser Service Optimization**: Enhanced shutdown handling and backpressure management

## ⚡ Performance Features

### Concurrent Processing
- Streaming pipeline (no full prefetch barrier)
- Domain dedupe & tracking prevents redundant work
- **Batch domain probing**: Multiple domains processed simultaneously
- **3-phase pipeline**: Domain collection → Batch probing → Parallel processing
- **Smart worker allocation**: Adapts to available resources

### Response Caching
- **TTL-based caching**: Avoids duplicate requests (1 hour default)
- **LRU eviction**: Manages memory usage efficiently
- **Cache hit tracking**: Monitor performance improvements

### Connection Pooling
- **Domain-specific sessions**: Reuse connections per domain
- **HTTP/2 ready**: Optimized for modern web servers
- **Retry logic**: Handles temporary failures gracefully

## Updated Processing Flow (Streaming)

1. Input load → company list
2. Streaming Google query emission
3. Domain scoring on arrival; schedule domain task if new
4. Probe + base fetch + discovery pages (bounded per-domain)
5. Hybrid async fallback (render) if empty
6. Filter & stream rows to CSV (callback)
7. Final dedupe + Excel export

Playwright note

- Playwright is required for rendering/login flows (Canvas). After adding or updating Playwright, always run `playwright install` inside the activated virtualenv to download browser executables.

## 🔧 Configuration

### Basic `.env` Configuration
```text
# Required
GOOGLE_API_KEY=your_key
GOOGLE_CX_ID=your_cx

# Performance (recommended)
MAX_WORKERS=8
PROCESS_PDFS=False
DOMAIN_SCORE_THRESHOLD=60
```

### Advanced Smart Discovery Settings
```text
# Smart Discovery Features
ENABLE_SMART_DISCOVERY=True
EARLY_STOP_ENABLED=False           # Disabled for thorough discovery
EARLY_STOP_THRESHOLD=3
MAX_PRIORITY_PAGES=25              # Increased for better coverage  
FALLBACK_PAGES_LIMIT=50            # Deeper fallback crawling
ENABLE_EMAIL_PATTERN_CACHE=True

# Performance Tuning
MAX_FALLBACK_PAGES=50              # Updated limit
MIN_CRAWL_DELAY=0.5
MAX_CRAWL_DELAY=2.0
REQUEST_TIMEOUT=30
```

### All Available Settings
| Setting | Default | Description |
|---------|---------|-------------|
| `MAX_WORKERS` | 4 | Concurrent processing threads |
| `ENABLE_SMART_DISCOVERY` | True | Use intelligent page prioritization |
| `ENABLE_EARLY_STOPPING` | True | Stop when sufficient emails found |
| `EARLY_STOP_THRESHOLD` | 3 | Number of emails to trigger early stop |
| `MAX_PRIORITY_PAGES` | 8 | Maximum priority pages to process |
| `DOMAIN_SCORE_THRESHOLD` | 60 | Minimum domain relevance score (0-100) |
| `PROCESS_PDFS` | False | Extract emails from PDF files |
| `MAX_FALLBACK_PAGES` | 12 | Maximum pages to crawl if no priority pages |

## 🔍 Troubleshooting

### Common Issues
- **"Executable doesn't exist..."** → Run `playwright install` inside the activated virtualenv
- **Missing Google keys** → Set `GOOGLE_API_KEY` and `GOOGLE_CX_ID` in `.env` file
- **Import/circular import errors** → Known issue with `http.py` naming conflict
- **High memory usage** → Reduce `MAX_WORKERS` or enable early stopping
- **Slow processing** → Enable smart discovery and caching features

### Google API Issues
- **IP not authorized** → See `google_ip_automation.md` for IP management solutions
- **Rate limiting** → Reduce `MAX_WORKERS` or implement API key rotation
- **Quota exceeded** → Monitor usage in Google Cloud Console

### Performance Optimization
- **Start with**: `MAX_WORKERS=4`, `ENABLE_SMART_DISCOVERY=True`
- **For faster processing**: Increase workers gradually, enable caching
- **For accuracy**: Lower `DOMAIN_SCORE_THRESHOLD`, process more pages
- **For speed**: Higher `EARLY_STOP_THRESHOLD`, fewer priority pages

### Windows-Specific
- **Multiprocessing issues** → Reduce `MAX_WORKERS` to 2-4
- **PowerShell encoding** → Use UTF-8 encoding for CSV files
- **Path issues** → Use absolute paths for input/output directories

## 📊 Performance Monitoring

### Built-in Performance Tracking
```bash
# Enable verbose logging to see performance metrics
scraper --batch --verbose

# Performance metrics in logs:
# - Request timing statistics
# - Cache hit/miss ratios  
# - Connection reuse statistics
# - Processing throughput
# - Worker efficiency analysis
```

### Performance Benchmarks
- **Without optimization**: ~2-5 companies/minute
- **With smart discovery**: ~10-15 companies/minute  
- **With full optimization**: ~30-50 companies/minute (achieved!)
- **Current performance**: MORE emails found, FASTER processing

### Performance Roadmap  
- ✅ **Phase 1 Complete**: Concurrent processing, smart discovery, caching
- ✅ **Phase 2 Complete**: Google API caching, connection pooling, request batching
- ✅ **Phase 3 Complete**: Performance monitoring, email discovery optimization
- 🎯 **Current Status**: All major performance optimizations implemented!

## 👨‍💻 Developer Guide

### Development Workflow
- Branch from `main` for features (e.g., `feature/performance-improvements`)
- Run formatting and linting before committing
- Add unit tests with `pytest`
- For debugging: Set `LOGLEVEL=DEBUG` or use single-row test files

### Key Modules (Current)
* `scraper/cli.py` – CLI + streaming sink
* `scraper/async_orchestrator.py` – core pipeline
* `scraper/async_http_client.py` & `async_components.py` – networking primitives
* `scraper/async_google_search.py` – search batching/streaming
* `scraper/smart_discovery.py` – URL heuristics & email filtering aid
* `scraper/hybrid_email_extractor.py` – cached multi-strategy fallback
* `scraper/domain_scorer.py` – domain relevance scoring
* (Removed) legacy sync orchestrator & archive modules

### Testing
```bash
# Run unit tests
pytest tests/

# Test with small dataset
echo "Company\nMicrosoft\nApple" > test.csv
scraper test.csv results.xlsx --verbose

# Batch processing test
mkdir -p input && cp test.csv input/
scraper --batch --verbose
```

## 📚 Additional Resources

* `ARCHITECTURE.md` – Detailed async streaming architecture
* `IMPROVEMENTS_SUMMARY.md` – Historical performance improvements
* `google_ip_automation.md` – Google API IP management
* `CLAUDE.md` – Guidelines & notes

## 🆘 Support

For issues, improvements, or questions:
1. Check the troubleshooting section above
2. Review the performance optimization settings
3. Enable verbose logging for detailed diagnostics
4. Consult the additional documentation files
