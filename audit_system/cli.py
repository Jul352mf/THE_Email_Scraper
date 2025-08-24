#!/usr/bin/env python3
"""
Command-line interface for the audit system.
"""

import argparse
import os
import sys
from pathlib import Path

from .config import AuditConfig, RuntimeActions, OutputPaths
from .audit_engine import AuditEngine


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Comprehensive Code-First Audit System",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Audit current directory
  python -m audit_system

  # Audit specific repository path
  python -m audit_system --repo-path /path/to/repo

  # Custom configuration
  python -m audit_system --branch develop --include "**/*.py" "**/*.js"

  # Skip build/test attempts
  python -m audit_system --no-build --no-tests
        """)
    
    parser.add_argument(
        "--repo-path",
        default=".",
        help="Path to repository to audit (default: current directory)"
    )
    
    parser.add_argument(
        "--branch",
        default="main",
        help="Branch name for audit (default: main)"
    )
    
    parser.add_argument(
        "--include",
        nargs="+",
        default=[
            "**/*.py", "**/*.ts", "**/*.tsx", "**/*.js", "**/*.go", "**/*.rs",
            "**/*.java", "**/*.cs", "**/*.sql", "**/*.yaml", "**/*.yml",
            "**/*.json", "**/*.Dockerfile", "**/Dockerfile", "**/*.sh"
        ],
        help="File patterns to include (default: common source files)"
    )
    
    parser.add_argument(
        "--exclude",
        nargs="+", 
        default=[
            "**/node_modules/**", "**/dist/**", "**/build/**", "**/.next/**",
            "**/.git/**", "**/venv/**", "**/.venv/**", "**/coverage/**",
            "**/vendor/**", "**/tmp/**", "**/__pycache__/**", "**/*.pyc"
        ],
        help="File patterns to exclude (default: common build/cache dirs)"
    )
    
    parser.add_argument(
        "--output-dir",
        default="REVIEW",
        help="Output directory for reports (default: REVIEW)"
    )
    
    parser.add_argument(
        "--no-build",
        action="store_true",
        help="Skip build attempts"
    )
    
    parser.add_argument(
        "--no-tests",
        action="store_true", 
        help="Skip test attempts"
    )
    
    parser.add_argument(
        "--no-lints",
        action="store_true",
        help="Skip lint attempts"
    )
    
    parser.add_argument(
        "--dates-locale",
        default="dd.mm.yyyy",
        choices=["dd.mm.yyyy", "yyyy-mm-dd"],
        help="Date format for reports (default: dd.mm.yyyy)"
    )
    
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Verbose output"
    )
    
    args = parser.parse_args()
    
    # Validate repository path
    repo_path = os.path.abspath(args.repo_path)
    if not os.path.exists(repo_path):
        print(f"❌ Repository path does not exist: {repo_path}")
        return 1
    
    if not os.path.isdir(repo_path):
        print(f"❌ Repository path is not a directory: {repo_path}")
        return 1
    
    # Create configuration
    runtime_actions = RuntimeActions(
        attempt_build=not args.no_build,
        attempt_tests=not args.no_tests,
        attempt_lints=not args.no_lints,
        safe_run_sample=False
    )
    
    outputs = OutputPaths(
        review_markdown_path=os.path.join(args.output_dir, "BRANCH-REVIEW.md"),
        findings_json_path=os.path.join(args.output_dir, "BRANCH-FINDINGS.json"), 
        codemap_markdown_path=os.path.join(args.output_dir, "CODEMAP.md")
    )
    
    config = AuditConfig(
        repo_url=f"file://{repo_path}",
        branch=args.branch,
        include_globs=args.include,
        exclude_globs=args.exclude,
        runtime_actions=runtime_actions,
        outputs=outputs,
        dates_locale=args.dates_locale
    )
    
    # Create audit engine and run
    engine = AuditEngine(config, repo_path)
    success = engine.run_full_audit()
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())