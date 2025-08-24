#!/usr/bin/env python3
"""
Test script for the audit system.
"""

import os
import json
import tempfile
from pathlib import Path

def test_audit_system():
    """Test the audit system with a sample codebase."""
    print("🧪 Testing audit system...")
    
    # Create temporary test repository
    with tempfile.TemporaryDirectory() as temp_dir:
        test_repo = Path(temp_dir)
        
        # Create test files with various issues
        create_test_files(test_repo)
        
        # Run audit
        from audit_system.config import AuditConfig
        from audit_system.audit_engine import AuditEngine
        
        config = AuditConfig()
        config.outputs.review_markdown_path = str(test_repo / "TEST-REVIEW.md")
        config.outputs.findings_json_path = str(test_repo / "TEST-FINDINGS.json")
        config.outputs.codemap_markdown_path = str(test_repo / "TEST-CODEMAP.md")
        config.runtime_actions.attempt_build = False
        config.runtime_actions.attempt_tests = False
        config.runtime_actions.attempt_lints = False
        
        engine = AuditEngine(config, str(test_repo))
        success = engine.run_full_audit()
        
        if not success:
            print("❌ Audit failed")
            return False
        
        # Validate outputs
        return validate_outputs(test_repo)


def create_test_files(repo_path: Path):
    """Create test files with various code issues."""
    
    # Python file with security issues
    (repo_path / "vulnerable.py").write_text("""
import os
import subprocess

# SQL injection vulnerability
def get_user(user_id):
    query = f"SELECT * FROM users WHERE id = {user_id}"
    return execute_query(query)

# Command injection vulnerability  
def run_command(user_input):
    os.system(f"ls {user_input}")
    
# Hardcoded secrets
API_KEY = "sk-1234567890abcdef"
PASSWORD = "admin123"

# XSS vulnerability (if this were JS)
def render_user(name):
    return f"<div>Hello {name}</div>"

# Performance issue - N+1 pattern
def get_all_users():
    user_ids = get_user_ids()
    users = []
    for user_id in user_ids:
        user = get_user_details(user_id)  # N+1 query
        users.append(user)
    return users
""")

    # JavaScript file with issues
    (repo_path / "frontend.js").write_text("""
// XSS vulnerability
function displayMessage(message) {
    document.getElementById('content').innerHTML = message;
}

// Eval usage
function executeCode(code) {
    eval(code);
}

// Hardcoded API key
const API_KEY = "abc123def456";

// Performance issue
function inefficientSearch(items, target) {
    for (let i = 0; i < items.length; i++) {
        for (let j = 0; j < items.length; j++) {
            if (items[i] === target) {
                return i;
            }
        }
    }
    return -1;
}
""")

    # Configuration file
    (repo_path / "config.yaml").write_text("""
database:
  host: localhost
  password: "hardcoded_password"
  
api:
  secret_key: "very-secret-key-123"
""")

    # Long function example
    (repo_path / "long_function.py").write_text("""
def very_long_function():
    # This function is intentionally long to trigger size warnings
""" + "\n    pass  # Line {}\n".join([""] + [str(i) for i in range(1, 60)]))

    # Test file
    (repo_path / "test_example.py").write_text("""
import unittest

class TestExample(unittest.TestCase):
    def test_something(self):
        self.assertTrue(True)
""")


def validate_outputs(repo_path: Path):
    """Validate that the audit outputs are correct."""
    
    # Check files exist
    review_file = repo_path / "TEST-REVIEW.md"
    findings_file = repo_path / "TEST-FINDINGS.json"
    codemap_file = repo_path / "TEST-CODEMAP.md"
    
    for file_path in [review_file, findings_file, codemap_file]:
        if not file_path.exists():
            print(f"❌ Missing output file: {file_path}")
            return False
    
    # Validate JSON structure
    try:
        with open(findings_file) as f:
            findings = json.load(f)
        
        if not isinstance(findings, list):
            print("❌ Findings JSON should be a list")
            return False
        
        if len(findings) == 0:
            print("❌ No findings detected in test code")
            return False
        
        # Check that we found some security issues
        security_findings = [f for f in findings if 'security' in f.get('tags', [])]
        if len(security_findings) == 0:
            print("❌ No security findings detected in vulnerable test code")
            return False
        
        print(f"✅ Found {len(findings)} total findings")
        print(f"✅ Found {len(security_findings)} security findings")
        
    except json.JSONDecodeError as e:
        print(f"❌ Invalid JSON in findings file: {e}")
        return False
    
    # Validate markdown structure
    with open(review_file) as f:
        review_content = f.read()
    
    required_sections = [
        "Executive Summary",
        "Architecture Overview", 
        "Risks & Issues",
        "Security Review",
        "Coverage Ledger"
    ]
    
    for section in required_sections:
        if section not in review_content:
            print(f"❌ Missing section in review: {section}")
            return False
    
    print("✅ All output files validated successfully")
    return True


if __name__ == "__main__":
    success = test_audit_system()
    if success:
        print("🎉 All tests passed!")
    else:
        print("💥 Tests failed!")
        exit(1)