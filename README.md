# THE Email Scraper — Quickstart & Developer Guide

Purpose

- Extract contact emails and related pages for a list of companies (Excel input). Supports static pages, sitemaps and dynamic pages using Playwright.

Prerequisites

- Python 3.10+ (project has been run with Python 3.13 in logs).
- A virtual environment is strongly recommended.

Canonical quickstart (cross-platform)

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

    ```bash
    # example usage
    scraper test_input.xlsx results.xlsx
    # or set environment overrides inline (POSIX)
    MAX_WORKERS=8 scraper test_input.xlsx results.xlsx
    # or (PowerShell)
    $env:MAX_WORKERS=8; scraper test_input.xlsx results.xlsx
    ```

Expected flow (high level)

- CLI reads Excel input -> Orchestrator scores & resolves domains -> static fetcher or Playwright BrowserService renders pages -> parsers extract emails (optionally from PDFs) -> results written to `results.xlsx`.

Playwright note

- Playwright is required for rendering/login flows (Canvas). After adding or updating Playwright, always run `playwright install` inside the activated virtualenv to download browser executables.

Minimal `.env` example

```text
GOOGLE_API_KEY=your_key
GOOGLE_CX_ID=your_cx
MAX_WORKERS=8
PROCESS_PDFS=False
```

Troubleshooting

- Error: "Executable doesn't exist..." — run `playwright install` inside the activated virtualenv.
- Missing Google keys — set `GOOGLE_API_KEY` and `GOOGLE_CX_ID` or use a non‑Google lookup path if available.
- Windows multiprocessing issues with high worker counts — reduce `MAX_WORKERS` in `.env` or via the CLI.

Developer workflow

- Branch from `main` for features (e.g., `feature/canvas-scraper`).
- Run formatting and linting (e.g., `black`, `flake8`) before committing.
- Add unit tests with `pytest`; Playwright browser install is required in CI if tests rely on it.
- For local debugging, set `LOGLEVEL=DEBUG` or run with a single-row `test_input.xlsx`.

Config reference

- Defaults and keys are in `scraper/config.py` (see the `Config` class). Important values: `MAX_WORKERS`, `PROCESS_PDFS`, `GOOGLE_API_KEY`, `GOOGLE_CX_ID`, `MAX_URLS_PER_SITEMAP`, timeouts and user agents.

CI hints

- Ensure `playwright install` runs as a CI step and cache the downloaded browsers between runs to save time.

Next steps

- I can add `README.dev.md` with extended developer notes or create `scripts/dev_setup.ps1` to automate venv creation, dependency installation and `playwright install`.
