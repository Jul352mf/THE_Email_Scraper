#!/usr/bin/env python3
"""
Example script showing how to use the audit system programmatically.
"""

import os
from audit_system.config import AuditConfig
from audit_system.audit_engine import AuditEngine


def audit_current_repository():
    """Example: Audit the current repository with default settings."""
    
    # Create default configuration
    config = AuditConfig()
    config.branch = "main"  # Set current branch
    
    # Set custom output paths
    config.outputs.review_markdown_path = "EXAMPLE-REVIEW.md"
    config.outputs.findings_json_path = "EXAMPLE-FINDINGS.json"
    config.outputs.codemap_markdown_path = "EXAMPLE-CODEMAP.md"
    
    # Get current directory
    repo_path = os.getcwd()
    
    # Create and run audit
    engine = AuditEngine(config, repo_path)
    success = engine.run_full_audit()
    
    if success:
        print("✅ Audit completed successfully!")
        print(f"📋 Check {config.outputs.review_markdown_path} for the main report")
    else:
        print("❌ Audit failed!")
        
    return success


def audit_with_custom_config():
    """Example: Audit with custom configuration."""
    from audit_system.sample_configs import EMAIL_SCRAPER_CONFIG
    from audit_system.config import AuditConfig
    
    # Load configuration
    config = AuditConfig.from_dict(EMAIL_SCRAPER_CONFIG)
    
    # Override some settings
    config.runtime_actions.attempt_tests = False  # Skip tests for faster audit
    
    # Run audit
    engine = AuditEngine(config, ".")
    return engine.run_full_audit()


def focused_security_audit():
    """Example: Focus on security issues only."""
    
    config = AuditConfig()
    
    # Focus on security-relevant file types
    config.include_globs = [
        "**/*.py",
        "**/*.js", 
        "**/*.ts",
        "**/*.sql",
        "**/*.yaml",
        "**/*.yml"
    ]
    
    # Set custom output
    config.outputs.review_markdown_path = "SECURITY-REVIEW.md"
    config.outputs.findings_json_path = "SECURITY-FINDINGS.json"
    
    # Skip build/test to focus on static analysis
    config.runtime_actions.attempt_build = False
    config.runtime_actions.attempt_tests = False
    config.runtime_actions.attempt_lints = False
    
    # Run audit
    engine = AuditEngine(config, ".")
    return engine.run_full_audit()


if __name__ == "__main__":
    print("🔍 Running example audit...")
    
    # Try different audit approaches
    print("\n1. Default audit of current repository:")
    audit_current_repository()
    
    print("\n2. Custom configuration audit:")
    audit_with_custom_config()
    
    print("\n3. Security-focused audit:")
    focused_security_audit()
    
    print("\n✅ All example audits completed!")