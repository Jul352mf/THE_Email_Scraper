"""
Sample configuration for the audit system.
This shows how to configure an audit for different scenarios.
"""

# Configuration for auditing THE_Email_Scraper repository
EMAIL_SCRAPER_CONFIG = {
    "REPO_URL": "https://github.com/Jul352mf/THE_Email_Scraper",
    "BRANCH": "main",
    "INCLUDE_GLOBS": [
        "**/*.py", 
        "**/*.json", 
        "**/*.yaml", 
        "**/*.yml", 
        "**/*.md",
        "**/*.txt"
    ],
    "EXCLUDE_GLOBS": [
        "**/__pycache__/**",
        "**/.git/**", 
        "**/venv/**", 
        "**/.venv/**",
        "**/node_modules/**",
        "**/dist/**",
        "**/build/**"
    ],
    "RUNTIME_ACTIONS": {
        "attempt_build": True,
        "attempt_tests": True,
        "attempt_lints": True,
        "safe_run_sample": False
    },
    "OUTPUTS": {
        "review_markdown_path": "REVIEW/EMAIL-SCRAPER-REVIEW.md",
        "findings_json_path": "REVIEW/EMAIL-SCRAPER-FINDINGS.json",
        "codemap_markdown_path": "REVIEW/EMAIL-SCRAPER-CODEMAP.md"
    },
    "DATES_LOCALE": "dd.mm.yyyy",
    "STYLE": "direct, concise, no fluff; bullets preferred; severity-ranked; Swiss metric/notation."
}

# Configuration for a typical web application
WEB_APP_CONFIG = {
    "REPO_URL": "https://github.com/example/webapp",
    "BRANCH": "develop",
    "INCLUDE_GLOBS": [
        "**/*.py", 
        "**/*.js", 
        "**/*.ts", 
        "**/*.tsx",
        "**/*.html",
        "**/*.css",
        "**/*.sql",
        "**/Dockerfile",
        "**/*.yaml",
        "**/*.yml",
        "**/*.json"
    ],
    "EXCLUDE_GLOBS": [
        "**/node_modules/**",
        "**/dist/**",
        "**/build/**",
        "**/.next/**",
        "**/.git/**",
        "**/venv/**",
        "**/.venv/**",
        "**/coverage/**",
        "**/vendor/**",
        "**/tmp/**",
        "**/__pycache__/**",
        "**/*.pyc"
    ],
    "RUNTIME_ACTIONS": {
        "attempt_build": True,
        "attempt_tests": True,
        "attempt_lints": True,
        "safe_run_sample": False
    },
    "OUTPUTS": {
        "review_markdown_path": "REVIEW/WEBAPP-REVIEW.md",
        "findings_json_path": "REVIEW/WEBAPP-FINDINGS.json",
        "codemap_markdown_path": "REVIEW/WEBAPP-CODEMAP.md"
    },
    "DATES_LOCALE": "dd.mm.yyyy",
    "STYLE": "direct, concise, no fluff; bullets preferred; severity-ranked; Swiss metric/notation."
}

# Minimal configuration for quick audits
QUICK_AUDIT_CONFIG = {
    "REPO_URL": "",
    "BRANCH": "main",
    "INCLUDE_GLOBS": ["**/*.py"],
    "EXCLUDE_GLOBS": ["**/__pycache__/**", "**/.git/**"],
    "RUNTIME_ACTIONS": {
        "attempt_build": False,
        "attempt_tests": False,
        "attempt_lints": False,
        "safe_run_sample": False
    },
    "OUTPUTS": {
        "review_markdown_path": "quick-review.md",
        "findings_json_path": "quick-findings.json",
        "codemap_markdown_path": "quick-codemap.md"
    },
    "DATES_LOCALE": "yyyy-mm-dd",
    "STYLE": "brief technical summary"
}