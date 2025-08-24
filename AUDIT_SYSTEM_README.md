# Comprehensive Code-First Audit System

A rigorous auditing tool that produces detailed reviews by examining codebases line-by-line and generating evidence-backed findings.

## Features

- **Code-First Analysis**: Reviews code directly, not documentation
- **Multi-Language Support**: Python, JavaScript, TypeScript, Java, Go, Rust, C#, SQL
- **Security Focus**: Detects SQL injection, XSS, hardcoded secrets, command injection
- **Performance Analysis**: Identifies N+1 queries, memory issues, algorithmic problems
- **Architecture Review**: Evaluates design patterns, error handling, maintainability
- **Automated Reports**: Generates structured markdown and JSON reports
- **Build Integration**: Attempts to build, test, and lint projects
- **Evidence-Based**: Every finding includes file location and code snippets

## Quick Start

### Command Line Usage

```bash
# Audit current directory
python -m audit_system

# Audit specific repository
python -m audit_system --repo-path /path/to/repo

# Custom file patterns
python -m audit_system --include "**/*.py" "**/*.js" --exclude "**/test/**"

# Skip build/test attempts for faster analysis
python -m audit_system --no-build --no-tests --no-lints

# Custom output directory
python -m audit_system --output-dir MY_REVIEW
```

### Programmatic Usage

```python
from audit_system.config import AuditConfig
from audit_system.audit_engine import AuditEngine

# Create configuration
config = AuditConfig()
config.branch = "main"
config.include_globs = ["**/*.py", "**/*.js"]

# Run audit
engine = AuditEngine(config, "/path/to/repo")
success = engine.run_full_audit()
```

## Configuration

The audit system supports flexible configuration through the `AuditConfig` class:

```python
config = AuditConfig(
    repo_url="https://github.com/user/repo",
    branch="develop",
    include_globs=["**/*.py", "**/*.js", "**/*.ts"],
    exclude_globs=["**/node_modules/**", "**/__pycache__/**"],
    runtime_actions=RuntimeActions(
        attempt_build=True,
        attempt_tests=True,
        attempt_lints=True
    ),
    outputs=OutputPaths(
        review_markdown_path="REVIEW/MAIN-REVIEW.md",
        findings_json_path="REVIEW/FINDINGS.json",
        codemap_markdown_path="REVIEW/CODEMAP.md"
    )
)
```

## Output Files

### 1. Main Review (BRANCH-REVIEW.md)

Comprehensive markdown report containing:
- Executive summary with key metrics
- Architecture overview and tech stack
- Top 10 risks and issues with evidence
- Security, performance, and reliability analysis
- Build/CI/CD and testing assessment
- Prioritized backlog with effort estimates
- Coverage ledger showing all analyzed files

### 2. Findings JSON (BRANCH-FINDINGS.json)

Machine-readable findings for integration:
```json
{
  "id": "SEC-001",
  "title": "Potential SQL injection",
  "severity": "Critical",
  "confidence": "High",
  "evidence": {
    "path": "api/users.py",
    "line": 42,
    "snippet": "query = f\"SELECT * FROM users WHERE id={user_id}\""
  },
  "impact": "SQL injection vulnerability",
  "remediation": ["Use parameterized queries"],
  "effort": "S",
  "tags": ["security", "sql", "injection"]
}
```

### 3. Code Map (CODEMAP.md)

Detailed structure breakdown:
- Module organization
- File purposes and symbols
- Dependencies and relationships
- LOC and size metrics

## Analysis Capabilities

### Security Analysis
- **SQL Injection**: Pattern detection in queries
- **XSS**: Unsafe DOM manipulation
- **Command Injection**: Shell execution risks
- **Hardcoded Secrets**: API keys, passwords, tokens
- **Crypto Issues**: Weak algorithms, key management

### Performance Analysis
- **N+1 Queries**: Database query patterns
- **Memory Issues**: Large file operations
- **Algorithmic Complexity**: Nested loops, inefficient patterns
- **I/O Bottlenecks**: File and network operations

### Architecture Review
- **Design Patterns**: Coupling, cohesion, layering
- **Error Handling**: Try/catch coverage
- **Function Size**: Maintainability metrics
- **Code Duplication**: Repeated patterns

### Reliability & Ops
- **Logging**: Observability coverage
- **Timeouts**: Network resilience
- **Health Checks**: Operational readiness
- **Graceful Shutdown**: Resource cleanup

## Severity Levels

- **Blocker**: Production-down, critical vulnerabilities
- **Critical**: Security risks, data loss potential  
- **Major**: Performance issues, reliability concerns
- **Minor**: Maintainability, style issues

## Effort Estimates

- **S** (Small): ≤2 hours
- **M** (Medium): ≤1 day
- **L** (Large): >1 day

## Language Support

| Language | Features |
|----------|----------|
| Python | Full analysis, symbol extraction, security patterns |
| JavaScript/TypeScript | XSS detection, function analysis, async patterns |
| Java | Class/method extraction, security patterns |
| Go | Function/type analysis, concurrency patterns |
| SQL | Injection detection, query analysis |
| YAML/JSON | Configuration analysis |
| Dockerfile | Container security, best practices |

## Integration

The audit system can be integrated into CI/CD pipelines:

```yaml
# GitHub Actions example
- name: Code Audit
  run: |
    python -m audit_system --no-build --no-tests
    # Parse BRANCH-FINDINGS.json for CI decisions
```

## Examples

See `audit_example.py` for complete usage examples including:
- Default repository auditing
- Custom configuration
- Security-focused analysis
- Integration patterns

## Requirements

- Python 3.8+
- Git (for commit analysis)
- Optional: Build tools for the target repository

## Architecture

The audit system consists of several key components:

1. **FileEnumerator**: Discovers and filters files using glob patterns
2. **CodeAnalyzer**: Performs static analysis and pattern detection
3. **ReportGenerator**: Creates structured output in multiple formats
4. **AuditEngine**: Orchestrates the entire audit process

Each component is designed to be extensible and configurable for different analysis needs.