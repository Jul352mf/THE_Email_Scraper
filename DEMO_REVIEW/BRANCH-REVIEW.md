# THE_Email_Scraper@main — Code-First Audit (24.08.2025)

## 1. Executive Summary

- **Repository**: THE_Email_Scraper on branch `main`
- **Scale**: 42 files, 9,987 lines of code
- **Languages**: Python
- **Critical Issues**: 0 found
- **Major Issues**: 68 found
- **Primary Function**: CLI entry point

## 2. What it is / What it's good for

**Primary Use Cases:**
- CLI entry point via `__main__.py`
- CLI entry point via `audit_system/__main__.py`
- CLI entry point via `audit_system/cli.py`

**Technology Profile:**
- Python: 42 files

## 3. Architecture Overview

**Module Structure:**
```
Canvas/
  ├── __init__.py (Python)
  ├── canvas_scraper.py (Python)
archive/
  ├── __init__.py (Python)
  ├── crawler_old.py (Python)
  ├── file_updater.py (Python)
  └── ... (7 more)
audit_system/
  ├── __init__.py (Python)
  ├── __main__.py (Python)
  ├── audit_engine.py (Python)
  └── ... (6 more)
scraper/
  ├── __init__.py (Python)
  ├── async_scraper.py (Python)
  ├── browser_service.py (Python)
  └── ... (13 more)
test/
  ├── canvas_parser_test.py (Python)
  ├── proxy_sanity.py (Python)
```

**Tech Stack by Layer:**
| Layer | Technology |
|-------|------------|
| Runtime | Python |

## 4. System Design Choices — Strengths & Trade-offs

**Strengths:**
- Asynchronous processing capabilities
- Externalized configuration
- Test coverage implemented

**Trade-offs:**

## 5. Risks & Issues (Top 10)

### 1. Code injection via eval
**Severity**: Major | **Confidence**: High
**Evidence**: `audit_system/code_analysis.py:137`
```
(r'eval\s*\(', "Code injection via eval()"),
```
**Impact**: Potential command injection vulnerability
**Fix**: Use subprocess with lists; Validate and sanitize input

### 2. Code injection via eval
**Severity**: Major | **Confidence**: High
**Evidence**: `test_audit_system.py:89`
```
eval(code);
```
**Impact**: Potential command injection vulnerability
**Fix**: Use subprocess with lists; Validate and sanitize input

### 3. Hardcoded API key
**Severity**: Major | **Confidence**: High
**Evidence**: `archive/tests.py:55`
```
config.api_key = "te***"
```
**Impact**: Hardcoded credentials pose security risk
**Fix**: Use environment variables; Use secure credential management

### 4. Hardcoded API key
**Severity**: Major | **Confidence**: High
**Evidence**: `archive/tests.py:61`
```
config.api_key = "te***"
```
**Impact**: Hardcoded credentials pose security risk
**Fix**: Use environment variables; Use secure credential management

### 5. Hardcoded API key
**Severity**: Major | **Confidence**: High
**Evidence**: `test_audit_system.py:63`
```
API_KEY = "sk***"
```
**Impact**: Hardcoded credentials pose security risk
**Fix**: Use environment variables; Use secure credential management

### 6. Hardcoded API key
**Severity**: Major | **Confidence**: High
**Evidence**: `test_audit_system.py:93`
```
const API_KEY = "ab***";
```
**Impact**: Hardcoded credentials pose security risk
**Fix**: Use environment variables; Use secure credential management

### 7. Hardcoded password
**Severity**: Major | **Confidence**: High
**Evidence**: `test_audit_system.py:64`
```
PASSWORD = "ad***"
```
**Impact**: Hardcoded credentials pose security risk
**Fix**: Use environment variables; Use secure credential management

### 8. Network operations without error handling
**Severity**: Major | **Confidence**: High
**Evidence**: `Canvas/canvas_scraper.py:1`
```
File contains network calls but no try/catch
```
**Impact**: Unhandled network errors can crash the application
**Fix**: Add try/catch blocks; Implement retry logic

### 9. Network operations without error handling
**Severity**: Major | **Confidence**: High
**Evidence**: `audit_system/sample_configs.py:1`
```
File contains network calls but no try/catch
```
**Impact**: Unhandled network errors can crash the application
**Fix**: Add try/catch blocks; Implement retry logic

### 10. Network operations without error handling
**Severity**: Major | **Confidence**: High
**Evidence**: `test/canvas_parser_test.py:1`
```
File contains network calls but no try/catch
```
**Impact**: Unhandled network errors can crash the application
**Fix**: Add try/catch blocks; Implement retry logic

## 6. Security Review

### Credentials Vulnerabilities
- **Hardcoded API key** (Major)
  - Location: `archive/tests.py:55`
  - Fix: Use environment variables
- **Hardcoded API key** (Major)
  - Location: `archive/tests.py:61`
  - Fix: Use environment variables
- **Hardcoded API key** (Major)
  - Location: `test_audit_system.py:63`
  - Fix: Use environment variables
- **Hardcoded password** (Major)
  - Location: `test_audit_system.py:64`
  - Fix: Use environment variables
- **Hardcoded API key** (Major)
  - Location: `test_audit_system.py:93`
  - Fix: Use environment variables

### Secrets Vulnerabilities
- **Hardcoded API key** (Major)
  - Location: `archive/tests.py:55`
  - Fix: Use environment variables
- **Hardcoded API key** (Major)
  - Location: `archive/tests.py:61`
  - Fix: Use environment variables
- **Hardcoded API key** (Major)
  - Location: `test_audit_system.py:63`
  - Fix: Use environment variables
- **Hardcoded password** (Major)
  - Location: `test_audit_system.py:64`
  - Fix: Use environment variables
- **Hardcoded API key** (Major)
  - Location: `test_audit_system.py:93`
  - Fix: Use environment variables

### Injection Vulnerabilities
- **Code injection via eval** (Major)
  - Location: `audit_system/code_analysis.py:137`
  - Fix: Use subprocess with lists
- **Shell injection risk** (Major)
  - Location: `audit_system/file_enumeration.py:141`
  - Fix: Use subprocess with lists
- **Code injection via eval** (Major)
  - Location: `test_audit_system.py:89`
  - Fix: Use subprocess with lists

## 7. Reliability & Ops

### Observability
**Issues Found:**
- Missing logging in business logic in `archive/tests.py`
- Missing logging in business logic in `audit_system/config.py`
- Missing logging in business logic in `audit_system/file_enumeration.py`

### Error Handling
**Issues Found:**
- Network operations without error handling in `Canvas/canvas_scraper.py`
- Network operations without error handling in `audit_system/sample_configs.py`
- Network operations without error handling in `test/canvas_parser_test.py`

## 8. Performance

**Issues Found:**
- **Potential N+1 query pattern** (Major)
  - Location: `Canvas/canvas_scraper.py:52`
  - Impact: N+1 queries can cause performance degradation
  - Fix: Use bulk operations
- **Reading entire file into memory** (Minor)
  - Location: `Canvas/canvas_scraper.py:107`
  - Impact: Large file operations may cause memory issues
  - Fix: Use streaming/chunked reading
- **Reading entire file into memory** (Minor)
  - Location: `Canvas/canvas_scraper.py:111`
  - Impact: Large file operations may cause memory issues
  - Fix: Use streaming/chunked reading
- **Deep nested loops detected** (Minor)
  - Location: `archive/crawler_old.py:233`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `archive/file_updater.py:142`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `archive/file_updater.py:143`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Potential N+1 query pattern** (Major)
  - Location: `archive/http_old.py:55`
  - Impact: N+1 queries can cause performance degradation
  - Fix: Use bulk operations
- **Potential N+1 query pattern** (Major)
  - Location: `archive/http_old.py:94`
  - Impact: N+1 queries can cause performance degradation
  - Fix: Use bulk operations
- **Potential N+1 query pattern** (Major)
  - Location: `archive/proxy.py:281`
  - Impact: N+1 queries can cause performance degradation
  - Fix: Use bulk operations
- **Deep nested loops detected** (Minor)
  - Location: `archive/proxy.py:261`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Potential N+1 query pattern** (Major)
  - Location: `archive/rate_limiter.py:63`
  - Impact: N+1 queries can cause performance degradation
  - Fix: Use bulk operations
- **Potential N+1 query pattern** (Major)
  - Location: `archive/sitemap_old.py:173`
  - Impact: N+1 queries can cause performance degradation
  - Fix: Use bulk operations
- **Potential N+1 query pattern** (Major)
  - Location: `archive/sitemap_old.py:179`
  - Impact: N+1 queries can cause performance degradation
  - Fix: Use bulk operations
- **Potential N+1 query pattern** (Major)
  - Location: `archive/sitemap_old.py:196`
  - Impact: N+1 queries can cause performance degradation
  - Fix: Use bulk operations
- **Deep nested loops detected** (Minor)
  - Location: `archive/sitemap_old.py:115`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `archive/sitemap_old.py:179`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `archive/sitemap_old.py:191`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `archive/sitemap_old.py:196`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `archive/sitemap_old.py:230`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `archive/sitemap_old.py:242`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Potential N+1 query pattern** (Major)
  - Location: `archive/tests.py:118`
  - Impact: N+1 queries can cause performance degradation
  - Fix: Use bulk operations
- **Deep nested loops detected** (Minor)
  - Location: `archive/tests.py:270`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Potential N+1 query pattern** (Major)
  - Location: `archive/worker.py:155`
  - Impact: N+1 queries can cause performance degradation
  - Fix: Use bulk operations
- **Deep nested loops detected** (Minor)
  - Location: `archive/worker.py:155`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `archive/worker.py:181`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Potential N+1 query pattern** (Major)
  - Location: `audit_system/audit_engine.py:227`
  - Impact: N+1 queries can cause performance degradation
  - Fix: Use bulk operations
- **Potential N+1 query pattern** (Major)
  - Location: `audit_system/audit_engine.py:238`
  - Impact: N+1 queries can cause performance degradation
  - Fix: Use bulk operations
- **Potential N+1 query pattern** (Major)
  - Location: `audit_system/audit_engine.py:242`
  - Impact: N+1 queries can cause performance degradation
  - Fix: Use bulk operations
- **Deep nested loops detected** (Minor)
  - Location: `audit_system/audit_engine.py:176`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `audit_system/audit_engine.py:227`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `audit_system/audit_engine.py:233`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `audit_system/audit_engine.py:238`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `audit_system/audit_engine.py:242`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `audit_system/audit_engine.py:247`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Potential N+1 query pattern** (Major)
  - Location: `audit_system/code_analysis.py:212`
  - Impact: N+1 queries can cause performance degradation
  - Fix: Use bulk operations
- **Potential N+1 query pattern** (Major)
  - Location: `audit_system/code_analysis.py:219`
  - Impact: N+1 queries can cause performance degradation
  - Fix: Use bulk operations
- **Potential N+1 query pattern** (Major)
  - Location: `audit_system/code_analysis.py:409`
  - Impact: N+1 queries can cause performance degradation
  - Fix: Use bulk operations
- **Reading entire file into memory** (Minor)
  - Location: `audit_system/code_analysis.py:75`
  - Impact: Large file operations may cause memory issues
  - Fix: Use streaming/chunked reading
- **Deep nested loops detected** (Minor)
  - Location: `audit_system/code_analysis.py:119`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `audit_system/code_analysis.py:141`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `audit_system/code_analysis.py:142`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `audit_system/code_analysis.py:164`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `audit_system/code_analysis.py:165`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `audit_system/code_analysis.py:189`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `audit_system/code_analysis.py:190`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `audit_system/code_analysis.py:209`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `audit_system/code_analysis.py:231`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `audit_system/code_analysis.py:232`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `audit_system/code_analysis.py:249`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `audit_system/code_analysis.py:277`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `audit_system/code_analysis.py:348`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `audit_system/code_analysis.py:349`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `audit_system/code_analysis.py:369`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `audit_system/code_analysis.py:385`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `audit_system/code_analysis.py:393`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Reading entire file into memory** (Minor)
  - Location: `audit_system/file_enumeration.py:153`
  - Impact: Large file operations may cause memory issues
  - Fix: Use streaming/chunked reading
- **Reading entire file into memory** (Minor)
  - Location: `audit_system/file_enumeration.py:163`
  - Impact: Large file operations may cause memory issues
  - Fix: Use streaming/chunked reading
- **Deep nested loops detected** (Minor)
  - Location: `audit_system/file_enumeration.py:70`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `audit_system/file_enumeration.py:72`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `audit_system/file_enumeration.py:81`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `audit_system/file_enumeration.py:183`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `audit_system/file_enumeration.py:197`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `audit_system/file_enumeration.py:218`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `audit_system/file_enumeration.py:239`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Potential N+1 query pattern** (Major)
  - Location: `audit_system/report_generator.py:2`
  - Impact: N+1 queries can cause performance degradation
  - Fix: Use bulk operations
- **Potential N+1 query pattern** (Major)
  - Location: `audit_system/report_generator.py:64`
  - Impact: N+1 queries can cause performance degradation
  - Fix: Use bulk operations
- **Potential N+1 query pattern** (Major)
  - Location: `audit_system/report_generator.py:96`
  - Impact: N+1 queries can cause performance degradation
  - Fix: Use bulk operations
- **Potential N+1 query pattern** (Major)
  - Location: `audit_system/report_generator.py:109`
  - Impact: N+1 queries can cause performance degradation
  - Fix: Use bulk operations
- **Potential N+1 query pattern** (Major)
  - Location: `audit_system/report_generator.py:159`
  - Impact: N+1 queries can cause performance degradation
  - Fix: Use bulk operations
- **Potential N+1 query pattern** (Major)
  - Location: `audit_system/report_generator.py:160`
  - Impact: N+1 queries can cause performance degradation
  - Fix: Use bulk operations
- **Potential N+1 query pattern** (Major)
  - Location: `audit_system/report_generator.py:185`
  - Impact: N+1 queries can cause performance degradation
  - Fix: Use bulk operations
- **Potential N+1 query pattern** (Major)
  - Location: `audit_system/report_generator.py:267`
  - Impact: N+1 queries can cause performance degradation
  - Fix: Use bulk operations
- **Potential N+1 query pattern** (Major)
  - Location: `audit_system/report_generator.py:268`
  - Impact: N+1 queries can cause performance degradation
  - Fix: Use bulk operations
- **Potential N+1 query pattern** (Major)
  - Location: `audit_system/report_generator.py:283`
  - Impact: N+1 queries can cause performance degradation
  - Fix: Use bulk operations
- **Potential N+1 query pattern** (Major)
  - Location: `audit_system/report_generator.py:301`
  - Impact: N+1 queries can cause performance degradation
  - Fix: Use bulk operations
- **Potential N+1 query pattern** (Major)
  - Location: `audit_system/report_generator.py:309`
  - Impact: N+1 queries can cause performance degradation
  - Fix: Use bulk operations
- **Potential N+1 query pattern** (Major)
  - Location: `audit_system/report_generator.py:318`
  - Impact: N+1 queries can cause performance degradation
  - Fix: Use bulk operations
- **Potential N+1 query pattern** (Major)
  - Location: `audit_system/report_generator.py:337`
  - Impact: N+1 queries can cause performance degradation
  - Fix: Use bulk operations
- **Potential N+1 query pattern** (Major)
  - Location: `audit_system/report_generator.py:347`
  - Impact: N+1 queries can cause performance degradation
  - Fix: Use bulk operations
- **Potential N+1 query pattern** (Major)
  - Location: `audit_system/report_generator.py:368`
  - Impact: N+1 queries can cause performance degradation
  - Fix: Use bulk operations
- **Potential N+1 query pattern** (Major)
  - Location: `audit_system/report_generator.py:443`
  - Impact: N+1 queries can cause performance degradation
  - Fix: Use bulk operations
- **Potential N+1 query pattern** (Major)
  - Location: `audit_system/report_generator.py:444`
  - Impact: N+1 queries can cause performance degradation
  - Fix: Use bulk operations
- **Potential N+1 query pattern** (Major)
  - Location: `audit_system/report_generator.py:445`
  - Impact: N+1 queries can cause performance degradation
  - Fix: Use bulk operations
- **Potential N+1 query pattern** (Major)
  - Location: `audit_system/report_generator.py:449`
  - Impact: N+1 queries can cause performance degradation
  - Fix: Use bulk operations
- **Potential N+1 query pattern** (Major)
  - Location: `audit_system/report_generator.py:454`
  - Impact: N+1 queries can cause performance degradation
  - Fix: Use bulk operations
- **Potential N+1 query pattern** (Major)
  - Location: `audit_system/report_generator.py:459`
  - Impact: N+1 queries can cause performance degradation
  - Fix: Use bulk operations
- **Deep nested loops detected** (Minor)
  - Location: `audit_system/report_generator.py:135`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `audit_system/report_generator.py:140`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `audit_system/report_generator.py:185`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `audit_system/report_generator.py:190`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `audit_system/report_generator.py:194`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `audit_system/report_generator.py:201`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `audit_system/report_generator.py:217`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `audit_system/report_generator.py:220`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `audit_system/report_generator.py:233`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `audit_system/report_generator.py:253`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `audit_system/report_generator.py:268`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `audit_system/report_generator.py:283`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `audit_system/report_generator.py:309`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `audit_system/report_generator.py:310`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `audit_system/report_generator.py:316`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `audit_system/report_generator.py:318`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `audit_system/report_generator.py:337`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `audit_system/report_generator.py:347`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `audit_system/report_generator.py:368`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `audit_system/report_generator.py:384`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `audit_system/report_generator.py:388`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `audit_system/report_generator.py:414`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `audit_system/report_generator.py:426`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `audit_system/report_generator.py:430`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `audit_system/report_generator.py:449`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `audit_system/report_generator.py:454`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `audit_system/report_generator.py:459`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `audit_system/report_generator.py:488`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `scraper/cache.py:138`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `scraper/cache.py:503`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Potential N+1 query pattern** (Major)
  - Location: `scraper/cli.py:222`
  - Impact: N+1 queries can cause performance degradation
  - Fix: Use bulk operations
- **Potential N+1 query pattern** (Major)
  - Location: `scraper/crawler.py:155`
  - Impact: N+1 queries can cause performance degradation
  - Fix: Use bulk operations
- **Potential N+1 query pattern** (Major)
  - Location: `scraper/domain_scorer.py:151`
  - Impact: N+1 queries can cause performance degradation
  - Fix: Use bulk operations
- **Potential N+1 query pattern** (Major)
  - Location: `scraper/domain_scorer.py:175`
  - Impact: N+1 queries can cause performance degradation
  - Fix: Use bulk operations
- **Deep nested loops detected** (Minor)
  - Location: `scraper/domain_scorer.py:151`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Potential N+1 query pattern** (Major)
  - Location: `scraper/email_extractor_manus.py:481`
  - Impact: N+1 queries can cause performance degradation
  - Fix: Use bulk operations
- **Potential N+1 query pattern** (Major)
  - Location: `scraper/email_extractor_manus.py:483`
  - Impact: N+1 queries can cause performance degradation
  - Fix: Use bulk operations
- **Deep nested loops detected** (Minor)
  - Location: `scraper/email_extractor_manus.py:341`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `scraper/email_extractor_manus.py:451`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `scraper/email_extractor_manus.py:468`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `scraper/email_extractor_manus.py:483`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `scraper/email_extractor_manus.py:486`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `scraper/email_extractor_manus.py:528`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Deep nested loops detected** (Minor)
  - Location: `scraper/email_extractor_manus.py:573`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Potential N+1 query pattern** (Major)
  - Location: `scraper/google_fallback.py:88`
  - Impact: N+1 queries can cause performance degradation
  - Fix: Use bulk operations
- **Potential N+1 query pattern** (Major)
  - Location: `scraper/google_fallback.py:122`
  - Impact: N+1 queries can cause performance degradation
  - Fix: Use bulk operations
- **Potential N+1 query pattern** (Major)
  - Location: `scraper/google_fallback.py:160`
  - Impact: N+1 queries can cause performance degradation
  - Fix: Use bulk operations
- **Potential N+1 query pattern** (Major)
  - Location: `scraper/google_fallback.py:173`
  - Impact: N+1 queries can cause performance degradation
  - Fix: Use bulk operations
- **Potential N+1 query pattern** (Major)
  - Location: `scraper/http.py:123`
  - Impact: N+1 queries can cause performance degradation
  - Fix: Use bulk operations
- **Deep nested loops detected** (Minor)
  - Location: `scraper/http.py:213`
  - Impact: Deeply nested loops can impact performance
  - Fix: Consider algorithmic optimization
- **Potential N+1 query pattern** (Major)
  - Location: `scraper/hybrid_email_extractor.py:30`
  - Impact: N+1 queries can cause performance degradation
  - Fix: Use bulk operations
- **Potential N+1 query pattern** (Major)
  - Location: `scraper/sitemap.py:145`
  - Impact: N+1 queries can cause performance degradation
  - Fix: Use bulk operations
- **Potential N+1 query pattern** (Major)
  - Location: `scraper/sitemap.py:174`
  - Impact: N+1 queries can cause performance degradation
  - Fix: Use bulk operations
- **Reading entire file into memory** (Minor)
  - Location: `test/canvas_parser_test.py:13`
  - Impact: Large file operations may cause memory issues
  - Fix: Use streaming/chunked reading
- **Reading entire file into memory** (Minor)
  - Location: `test/canvas_parser_test.py:19`
  - Impact: Large file operations may cause memory issues
  - Fix: Use streaming/chunked reading
- **Potential N+1 query pattern** (Major)
  - Location: `test_audit_system.py:70`
  - Impact: N+1 queries can cause performance degradation
  - Fix: Use bulk operations
- **Potential N+1 query pattern** (Major)
  - Location: `test_audit_system.py:74`
  - Impact: N+1 queries can cause performance degradation
  - Fix: Use bulk operations
- **Potential N+1 query pattern** (Major)
  - Location: `test_audit_system.py:95`
  - Impact: N+1 queries can cause performance degradation
  - Fix: Use bulk operations
- **Potential N+1 query pattern** (Major)
  - Location: `test_audit_system.py:98`
  - Impact: N+1 queries can cause performance degradation
  - Fix: Use bulk operations
- **Potential N+1 query pattern** (Major)
  - Location: `test_audit_system.py:161`
  - Impact: N+1 queries can cause performance degradation
  - Fix: Use bulk operations
- **Reading entire file into memory** (Minor)
  - Location: `test_audit_system.py:175`
  - Impact: Large file operations may cause memory issues
  - Fix: Use streaming/chunked reading

## 9. Data & Schema

- No dedicated data model files detected

## 10. Tests & Quality Gates

**Test Coverage:**
- Test files: 4
- Source files: 38
- Test ratio: 9.5%

**Test files:**
- `archive/tests.py` (367 LOC)
- `test/canvas_parser_test.py` (26 LOC)
- `test/proxy_sanity.py` (867 LOC)
- `test_audit_system.py` (200 LOC)

## 11. Build/CI/CD

- No build configuration files detected

## 15. Backlog

### Fast Wins (≤2h)
- Reading entire file into memory (Minor)
- Reading entire file into memory (Minor)
- Network operations without error handling (Major)
- Line too long (Minor)
- Line too long (Minor)

### High-Leverage (≤1d)
- Potential N+1 query pattern (Major)
- Potential code duplication (Minor)
- Deep nested loops detected (Minor)
- Large function detected (Minor)
- Potential code duplication (Minor)

### Deep Work (>1d)

## 16. Assumptions & Unknowns

**Assumptions:**
- Static analysis only; no runtime testing performed
- Pattern-based detection; may have false positives/negatives
- Code review based on current branch state

**Unknowns:**
- Runtime behavior and performance characteristics
- Integration with external services
- Production deployment configuration

## 17. Coverage Ledger

| File | Lang | LOC | Last Commit | SHA12 | Top Symbols | Purpose |
|------|------|-----|-------------|-------|-------------|---------|
| Canvas/__init__.py | Python | 6 | 76688fb (22.08.2025) | 28c3d5619841 | - | Source module |
| Canvas/canvas_scraper.py | Python | 146 | 76688fb (22.08.2025) | 496fb1b8303a | CanvasScraper, login(), scrape_course() | Source module |
| __main__.py | Python | 20 | 76688fb (22.08.2025) | fbca784745ed | - | CLI entry point |
| archive/__init__.py | Python | 7 | 76688fb (22.08.2025) | ff461c79c9e4 | - | Source module |
| archive/crawler_old.py | Python | 244 | 76688fb (22.08.2025) | 09f9ffd28e5a | CrawlerError, Crawler, set_domain_limit() | Source module |
| archive/file_updater.py | Python | 199 | 76688fb (22.08.2025) | 90b7b644a764 | FileUpdaterError, FileUpdater, update_file() | Source module |
| archive/http_old.py | Python | 381 | 76688fb (22.08.2025) | 9b7db7f7c562 | ThreadSafeSession, get_session(), clear_visited() | Source module |
| archive/progress.py | Python | 321 | 76688fb (22.08.2025) | 3ccd5b4b0fd6 | ProgressTracker, update(), set_callback() | Source module |
| archive/proxy.py | Python | 293 | 76688fb (22.08.2025) | bd52c4b0926e | ProxyError, Proxy, get_proxy_dict() | Source module |
| archive/rate_limiter.py | Python | 139 | 76688fb (22.08.2025) | c86579764947 | RateLimiter, set_rate(), wait() | Source module |
| archive/sitemap_old.py | Python | 271 | 76688fb (22.08.2025) | 38ef0946da17 | SitemapError, SitemapParser, discover_sitemaps() | Source module |
| archive/tests.py | Python | 367 | 76688fb (22.08.2025) | 66b85ae91c49 | TestConfig, test_config_defaults(), test_config_validation() | Test suite |
| archive/worker.py | Python | 406 | 76688fb (22.08.2025) | 48444d4346ad | TaskResult, WorkerPool, add_task() | Source module |
| audit_example.py | Python | 97 | 14917df (24.08.2025) | 404ef49eff1a | audit_current_repository(), audit_with_custom_config(), focused_security_audit() | Source module |
| audit_system/__init__.py | Python | 9 | 14917df (24.08.2025) | 4c9201e86def | - | Source module |
| audit_system/__main__.py | Python | 8 | 14917df (24.08.2025) | 5e1a15ec388d | - | CLI entry point |
| audit_system/audit_engine.py | Python | 255 | 14917df (24.08.2025) | d56376f0c064 | AuditEngine, run_full_audit() | Source module |
| audit_system/cli.py | Python | 150 | 14917df (24.08.2025) | 184fa0e8e1de | main() | CLI entry point |
| audit_system/code_analysis.py | Python | 428 | 14917df (24.08.2025) | 29bf0c54257a | Severity, Confidence, Evidence | Source module |
| audit_system/config.py | Python | 81 | 14917df (24.08.2025) | 3b635081ae46 | RuntimeActions, OutputPaths, AuditConfig | Configuration |
| audit_system/file_enumeration.py | Python | 279 | 14917df (24.08.2025) | 3fcd0926e24b | FileInfo, FileEnumerator, enumerate_files() | Source module |
| audit_system/report_generator.py | Python | 505 | 14917df (24.08.2025) | d7a0eeffefb5 | ReportGenerator, generate_all_reports(), generate_main_review() | Source module |
| audit_system/sample_configs.py | Python | 107 | 14917df (24.08.2025) | 95f05723185a | - | Configuration |
| scraper/__init__.py | Python | 0 | 76688fb (22.08.2025) | e3b0c44298fc | - | Source module |
| scraper/async_scraper.py | Python | 123 | 76688fb (22.08.2025) | 904e8ffff23f | AsyncBrowserPool, AsyncEmailExtractor | Source module |
| scraper/browser_service.py | Python | 132 | 76688fb (22.08.2025) | 7eeb460caf2d | get_browser_service(), BrowserService, run() | Business logic service |
| scraper/cache.py | Python | 545 | 76688fb (22.08.2025) | 359334657417 | CacheError, Cache, get() | Source module |
| scraper/cli.py | Python | 416 | 76688fb (22.08.2025) | efd2aa876ebe | CLIError, CLI, validate_environment() | CLI entry point |
| scraper/config.py | Python | 292 | 76688fb (22.08.2025) | 9d52638f3b0e | ConfigurationError, Config, as_dict() | Configuration |
| scraper/crawler.py | Python | 219 | 76688fb (22.08.2025) | e0e7596dc170 | CrawlerError, Crawler, set_domain_limit() | Source module |
| scraper/domain_scorer.py | Python | 206 | 76688fb (22.08.2025) | e9bcf03d399c | DomainScoringError, DomainScorer, clean_company_name() | CLI entry point |
| scraper/email_extractor.py | Python | 284 | 76688fb (22.08.2025) | 2c08a48b2d72 | EmailValidationError, EmailExtractor, is_valid_email() | Source module |
| scraper/email_extractor_manus.py | Python | 588 | 76688fb (22.08.2025) | 035e82b1ea23 | EmailValidationError, EmailExtractor, is_valid_email() | Source module |
| scraper/google_fallback.py | Python | 197 | 76688fb (22.08.2025) | ab997f74d12e | GoogleFallbackError, GoogleFallback, search_with_fallback_engine() | Source module |
| scraper/google_search.py | Python | 155 | 76688fb (22.08.2025) | f364fa384782 | GoogleApiError, RateLimitExceededError, GoogleSearchClient | Source module |
| scraper/http.py | Python | 432 | 76688fb (22.08.2025) | 4c11fc58550e | canonicalise(), validate_url(), TokenBucket | Source module |
| scraper/hybrid_email_extractor.py | Python | 145 | 76688fb (22.08.2025) | 4c03a8fed0c8 | HybridEmailExtractor, extract_from_url(), extract_from_response() | Source module |
| scraper/orchestrator.py | Python | 209 | 76688fb (22.08.2025) | dfc059c59bb4 | OrchestratorError, Orchestrator, reset_stats() | Source module |
| scraper/sitemap.py | Python | 232 | 76688fb (22.08.2025) | 6d55fdc87fa7 | join_url(), SitemapError, SitemapParser | Source module |
| test/canvas_parser_test.py | Python | 26 | 76688fb (22.08.2025) | 7abf56abbbfa | run_tests() | Test suite |
| test/proxy_sanity.py | Python | 867 | 76688fb (22.08.2025) | ba61272c7142 | test_proxy() | Test suite |
| test_audit_system.py | Python | 200 | 14917df (24.08.2025) | 5f8a70cfdfe6 | test_audit_system(), create_test_files(), get_user() | Test suite |

**Coverage**: 42 files analyzed (100% of in-scope files)

## 18. Appendix: Code Map

See separate CODEMAP.md for detailed module breakdown.
