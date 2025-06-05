# THE Email Scraper

This project is an email discovery tool that looks up company domains via Google search and extracts email addresses from the discovered pages. It is designed around several cooperating modules:

- **`cli.py`** – command line interface parsing Excel files and orchestrating workers
- **`google_search.py`** – wrapper around the Google Custom Search API
- **`crawler.py`** and **`sitemap.py`** – fetch website pages and parse sitemaps
- **`email_extractor.py`** and **`hybrid_email_extractor.py`** – extract and validate email addresses
- **`orchestrator.py`** – coordinates the overall scraping process

The code requires **Python 3.10 or higher**.

## Configuration

Configuration is read from environment variables (typically via a `.env` file).
Two variables are **mandatory**:

- `GOOGLE_API_KEY` – Google API key used for Custom Search
- `GOOGLE_CX_ID` – ID of the Google Custom Search Engine

Many optional settings can be tuned; the defaults are taken from `config.py`:

- `MAX_WORKERS` – number of concurrent threads (default `4`)
- `MAX_FALLBACK_PAGES` – maximum pages to crawl per domain (default `12`)
- `PROCESS_PDFS` – set to `true` to inspect PDF files (default `false`)
- `ALLOW_INSECURE_SSL` – allow invalid TLS certificates (default `false`)
- `GOOGLE_SAFE_INTERVAL` – delay between Google API calls in seconds (default `0.8`)
- `GOOGLE_MAX_RETRIES` – retry attempts for Google API failures (default `5`)
- `DOMAIN_SCORE_THRESHOLD` – scoring threshold for valid domains (default `60`)
- `MAX_REDIRECTS` – redirect limit for HTTP requests (default `5`)
- `MAX_URL_LENGTH` – maximum URL length allowed (default `2000`)
- `CONNECTION_TIMEOUT` / `READ_TIMEOUT` – HTTP timeouts in seconds (defaults `10`/`20`)
- `MIN_CRAWL_DELAY` / `MAX_CRAWL_DELAY` – throttling delays in seconds (defaults `0.5`/`2.0`)
- `PROXIES` – comma separated list of proxy servers
- `MAX_URLS_PER_SITEMAP` – limit of `<loc>` entries parsed from each sitemap
- `BLOCKED_DOMAINS` – comma separated list of domains to skip

## Example `.env`

```dotenv
GOOGLE_API_KEY=your-key
GOOGLE_CX_ID=your-cx-id
MAX_WORKERS=8
PROCESS_PDFS=true
```

## Running the scraper

Invoke the CLI with an input Excel sheet containing a `Company` column and the desired output file:

```bash
python cli.py companies.xlsx results.xlsx --workers 8 --process-pdfs
```

The program will create `results.xlsx` with discovered domains and emails while logging progress to a timestamped log file.
