# Code Map

Detailed breakdown of repository structure and dependencies.

## ./

### AUDIT_SYSTEM_README.md (Markdown)
- **LOC**: 205
- **Size**: 5956 bytes
- **Purpose**: Documentation

### EXAMPLE-CODEMAP.md (Markdown)
- **LOC**: 266
- **Size**: 7241 bytes
- **Purpose**: Documentation

### EXAMPLE-FINDINGS.json (JSON)
- **LOC**: 6458
- **Size**: 162748 bytes
- **Purpose**: Source module

### EXAMPLE-REVIEW.md (Markdown)
- **LOC**: 958
- **Size**: 43347 bytes
- **Purpose**: Documentation

### README.md (Markdown)
- **LOC**: 103
- **Size**: 3349 bytes
- **Purpose**: Documentation

### __main__.py (Python)
- **LOC**: 20
- **Size**: 434 bytes
- **Purpose**: CLI entry point

### audit_example.py (Python)
- **LOC**: 97
- **Size**: 2732 bytes
- **Purpose**: Source module
- **Top Symbols**: audit_current_repository(), audit_with_custom_config(), focused_security_audit()

### requirements.txt (Unknown)
- **LOC**: 11
- **Size**: 180 bytes
- **Purpose**: Source module

## Canvas/

### __init__.py (Python)
- **LOC**: 6
- **Size**: 120 bytes
- **Purpose**: Source module

### canvas_scraper.py (Python)
- **LOC**: 146
- **Size**: 5662 bytes
- **Purpose**: Source module
- **Top Symbols**: CanvasScraper, login(), scrape_course(), fetch_modules_page(), parse_module_links()

## REVIEW/

### BRANCH-FINDINGS.json (JSON)
- **LOC**: 4826
- **Size**: 121611 bytes
- **Purpose**: Source module

### BRANCH-REVIEW.md (Markdown)
- **LOC**: 865
- **Size**: 38582 bytes
- **Purpose**: Documentation

### CODEMAP.md (Markdown)
- **LOC**: 243
- **Size**: 6720 bytes
- **Purpose**: Documentation

### EMAIL-SCRAPER-CODEMAP.md (Markdown)
- **LOC**: 286
- **Size**: 7622 bytes
- **Purpose**: Documentation

### EMAIL-SCRAPER-FINDINGS.json (JSON)
- **LOC**: 6554
- **Size**: 163810 bytes
- **Purpose**: Source module

### EMAIL-SCRAPER-REVIEW.md (Markdown)
- **LOC**: 920
- **Size**: 41202 bytes
- **Purpose**: Documentation

## archive/

### __init__.py (Python)
- **LOC**: 7
- **Size**: 183 bytes
- **Purpose**: Source module

### crawler_old.py (Python)
- **LOC**: 244
- **Size**: 9254 bytes
- **Purpose**: Source module
- **Top Symbols**: CrawlerError, Crawler, set_domain_limit(), get_domain_limit(), reset_counters()

### file_updater.py (Python)
- **LOC**: 199
- **Size**: 6830 bytes
- **Purpose**: Source module
- **Top Symbols**: FileUpdaterError, FileUpdater, update_file(), update_in_place()

### http_old.py (Python)
- **LOC**: 381
- **Size**: 13395 bytes
- **Purpose**: Source module
- **Top Symbols**: ThreadSafeSession, get_session(), clear_visited(), HttpClient, safe_get()

### progress.py (Python)
- **LOC**: 321
- **Size**: 10180 bytes
- **Purpose**: Source module
- **Top Symbols**: ProgressTracker, update(), set_callback(), get_stats(), MultiProgressTracker

### proxy.py (Python)
- **LOC**: 293
- **Size**: 8822 bytes
- **Purpose**: Source module
- **Top Symbols**: ProxyError, Proxy, get_proxy_dict(), is_available(), mark_success()

### rate_limiter.py (Python)
- **LOC**: 139
- **Size**: 4424 bytes
- **Purpose**: Source module
- **Top Symbols**: RateLimiter, set_rate(), wait(), execute_with_rate_limit()

### sitemap_old.py (Python)
- **LOC**: 271
- **Size**: 11070 bytes
- **Purpose**: Source module
- **Top Symbols**: SitemapError, SitemapParser, discover_sitemaps(), parse_sitemap(), get_priority_urls()

### tests.py (Python)
- **LOC**: 367
- **Size**: 13411 bytes
- **Purpose**: Test suite
- **Top Symbols**: TestConfig, test_config_defaults(), test_config_validation(), TestHttp, test_validate_url()

### worker.py (Python)
- **LOC**: 406
- **Size**: 13213 bytes
- **Purpose**: Source module
- **Top Symbols**: TaskResult, WorkerPool, add_task(), add_tasks(), start()

## audit_system/

### __init__.py (Python)
- **LOC**: 9
- **Size**: 235 bytes
- **Purpose**: Source module

### __main__.py (Python)
- **LOC**: 8
- **Size**: 106 bytes
- **Purpose**: CLI entry point

### audit_engine.py (Python)
- **LOC**: 255
- **Size**: 9699 bytes
- **Purpose**: Source module
- **Top Symbols**: AuditEngine, run_full_audit()

### cli.py (Python)
- **LOC**: 150
- **Size**: 4088 bytes
- **Purpose**: CLI entry point
- **Top Symbols**: main()

### code_analysis.py (Python)
- **LOC**: 428
- **Size**: 17956 bytes
- **Purpose**: Source module
- **Top Symbols**: Severity, Confidence, Evidence, Finding, CodeAnalyzer

### config.py (Python)
- **LOC**: 81
- **Size**: 3167 bytes
- **Purpose**: Configuration
- **Top Symbols**: RuntimeActions, OutputPaths, AuditConfig, from_dict(), ensure_output_dirs()

### file_enumeration.py (Python)
- **LOC**: 279
- **Size**: 10396 bytes
- **Purpose**: Source module
- **Top Symbols**: FileInfo, FileEnumerator, enumerate_files()

### report_generator.py (Python)
- **LOC**: 505
- **Size**: 21575 bytes
- **Purpose**: Source module
- **Top Symbols**: ReportGenerator, generate_all_reports(), generate_main_review(), generate_findings_json(), generate_codemap()

### sample_configs.py (Python)
- **LOC**: 107
- **Size**: 2987 bytes
- **Purpose**: Configuration

## scraper/

### __init__.py (Python)
- **LOC**: 0
- **Size**: 0 bytes
- **Purpose**: Source module

### async_scraper.py (Python)
- **LOC**: 123
- **Size**: 4138 bytes
- **Purpose**: Source module
- **Top Symbols**: AsyncBrowserPool, AsyncEmailExtractor

### browser_service.py (Python)
- **LOC**: 132
- **Size**: 4347 bytes
- **Purpose**: Business logic service
- **Top Symbols**: get_browser_service(), BrowserService, run(), render(), shutdown()

### cache.py (Python)
- **LOC**: 545
- **Size**: 17275 bytes
- **Purpose**: Source module
- **Top Symbols**: CacheError, Cache, get(), set(), delete()

### cli.py (Python)
- **LOC**: 416
- **Size**: 13971 bytes
- **Purpose**: CLI entry point
- **Top Symbols**: CLIError, CLI, validate_environment(), validate_input_file(), validate_output_file()

### config.py (Python)
- **LOC**: 292
- **Size**: 10688 bytes
- **Purpose**: Configuration
- **Top Symbols**: ConfigurationError, Config, as_dict(), validate(), validate_or_raise()

### crawler.py (Python)
- **LOC**: 219
- **Size**: 7494 bytes
- **Purpose**: Source module
- **Top Symbols**: CrawlerError, Crawler, set_domain_limit(), get_domain_limit(), reset_counters()

### domain_scorer.py (Python)
- **LOC**: 206
- **Size**: 7093 bytes
- **Purpose**: CLI entry point
- **Top Symbols**: DomainScoringError, DomainScorer, clean_company_name(), score_domain(), find_best_domain()

### email_extractor.py (Python)
- **LOC**: 284
- **Size**: 10402 bytes
- **Purpose**: Source module
- **Top Symbols**: EmailValidationError, EmailExtractor, is_valid_email(), clean_email(), deobfuscate_emails()

### email_extractor_manus.py (Python)
- **LOC**: 588
- **Size**: 25693 bytes
- **Purpose**: Source module
- **Top Symbols**: EmailValidationError, EmailExtractor, is_valid_email(), clean_email(), extract_from_url()

### google_fallback.py (Python)
- **LOC**: 197
- **Size**: 6774 bytes
- **Purpose**: Source module
- **Top Symbols**: GoogleFallbackError, GoogleFallback, search_with_fallback_engine(), search_with_cache()

### google_search.py (Python)
- **LOC**: 155
- **Size**: 5532 bytes
- **Purpose**: Source module
- **Top Symbols**: GoogleApiError, RateLimitExceededError, GoogleSearchClient, search(), search_with_fallback()

### http.py (Python)
- **LOC**: 432
- **Size**: 15758 bytes
- **Purpose**: Source module
- **Top Symbols**: canonicalise(), validate_url(), TokenBucket, consume(), _SessionManager

### hybrid_email_extractor.py (Python)
- **LOC**: 145
- **Size**: 5189 bytes
- **Purpose**: Source module
- **Top Symbols**: HybridEmailExtractor, extract_from_url(), extract_from_response()

### orchestrator.py (Python)
- **LOC**: 209
- **Size**: 8795 bytes
- **Purpose**: Source module
- **Top Symbols**: OrchestratorError, Orchestrator, reset_stats(), process_company(), set_options()

### sitemap.py (Python)
- **LOC**: 232
- **Size**: 8628 bytes
- **Purpose**: Source module
- **Top Symbols**: join_url(), SitemapError, SitemapParser, discover_sitemaps(), parse_sitemap()

## test/

### canvas_parser_test.py (Python)
- **LOC**: 26
- **Size**: 820 bytes
- **Purpose**: Test suite
- **Top Symbols**: run_tests()

### proxy_sanity.py (Python)
- **LOC**: 867
- **Size**: 23262 bytes
- **Purpose**: Test suite
- **Top Symbols**: test_proxy()
