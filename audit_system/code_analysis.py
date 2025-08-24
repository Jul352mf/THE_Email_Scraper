"""
Static analysis engine for security, performance, and architecture review.
"""

import re
import os
import ast
import json
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from enum import Enum


class Severity(Enum):
    BLOCKER = "Blocker"
    CRITICAL = "Critical"
    MAJOR = "Major"
    MINOR = "Minor"


class Confidence(Enum):
    HIGH = "High"
    MEDIUM = "Med"
    LOW = "Low"


@dataclass
class Evidence:
    """Evidence for a finding."""
    path: str
    line: int
    snippet: str


@dataclass
class Finding:
    """A code review finding."""
    id: str
    title: str
    severity: Severity
    confidence: Confidence
    evidence: Evidence
    impact: str
    remediation: List[str]
    effort: str  # S/M/L
    tags: List[str]
    introduced_in_commit: Optional[str] = None
    related_tests: List[str] = None


class CodeAnalyzer:
    """Main code analysis engine."""
    
    def __init__(self, repo_path: str):
        self.repo_path = repo_path
        self.findings: List[Finding] = []
        self.finding_counter = 0
        
    def analyze_files(self, file_infos) -> List[Finding]:
        """Analyze all files and return findings."""
        self.findings = []
        self.finding_counter = 0
        
        for file_info in file_infos:
            self._analyze_file(file_info)
            
        return self.findings
    
    def _analyze_file(self, file_info):
        """Analyze a single file."""
        full_path = os.path.join(self.repo_path, file_info.path)
        
        try:
            with open(full_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
                lines = content.split('\n')
                
            # Security analysis
            self._analyze_security(file_info.path, content, lines, file_info.language)
            
            # Performance analysis
            self._analyze_performance(file_info.path, content, lines, file_info.language)
            
            # Architecture analysis
            self._analyze_architecture(file_info.path, content, lines, file_info.language)
            
            # Reliability analysis
            self._analyze_reliability(file_info.path, content, lines, file_info.language)
            
            # Maintainability analysis
            self._analyze_maintainability(file_info.path, content, lines, file_info.language)
            
        except Exception as e:
            # Record analysis failure as a finding
            self._add_finding(
                "ANAL-001",
                f"Failed to analyze file: {str(e)}",
                Severity.MINOR,
                Confidence.HIGH,
                Evidence(file_info.path, 1, "Failed to read file"),
                "File cannot be properly analyzed",
                ["Check file encoding and permissions"],
                "S",
                ["analysis", "error"]
            )
    
    def _analyze_security(self, file_path: str, content: str, lines: List[str], language: str):
        """Analyze security vulnerabilities."""
        
        # SQL Injection patterns
        sql_patterns = [
            (r'execute\s*\(\s*f?"[^"]*\{[^}]+\}[^"]*"', "SQL injection via f-string"),
            (r'execute\s*\(\s*"[^"]*"\s*%', "SQL injection via % formatting"),
            (r'execute\s*\(\s*"[^"]*"\s*\+', "SQL injection via string concatenation"),
            (r'query\s*\(\s*f?"[^"]*\{[^}]+\}[^"]*"', "SQL injection in query"),
        ]
        
        for i, line in enumerate(lines, 1):
            for pattern, desc in sql_patterns:
                if re.search(pattern, line, re.IGNORECASE):
                    self._add_finding(
                        f"SEC-{self._next_id():03d}",
                        desc,
                        Severity.CRITICAL,
                        Confidence.HIGH,
                        Evidence(file_path, i, line.strip()),
                        "Potential SQL injection vulnerability",
                        ["Use parameterized queries", "Validate and sanitize input"],
                        "S",
                        ["security", "sql", "injection"]
                    )
        
        # XSS patterns
        xss_patterns = [
            (r'innerHTML\s*=.*\+', "Potential XSS via innerHTML"),
            (r'document\.write\s*\(', "Potential XSS via document.write"),
            (r'eval\s*\(', "Code injection via eval()"),
        ]
        
        if language in ['JavaScript', 'TypeScript']:
            for i, line in enumerate(lines, 1):
                for pattern, desc in xss_patterns:
                    if re.search(pattern, line, re.IGNORECASE):
                        self._add_finding(
                            f"SEC-{self._next_id():03d}",
                            desc,
                            Severity.MAJOR,
                            Confidence.MEDIUM,
                            Evidence(file_path, i, line.strip()),
                            "Potential XSS vulnerability",
                            ["Use safe DOM manipulation", "Sanitize user input"],
                            "S",
                            ["security", "xss", "javascript"]
                        )
        
        # Hardcoded secrets
        secret_patterns = [
            (r'password\s*=\s*["\'][^"\']+["\']', "Hardcoded password"),
            (r'api_key\s*=\s*["\'][^"\']+["\']', "Hardcoded API key"),
            (r'secret\s*=\s*["\'][^"\']+["\']', "Hardcoded secret"),
            (r'token\s*=\s*["\'][^"\']{20,}["\']', "Hardcoded token"),
        ]
        
        for i, line in enumerate(lines, 1):
            for pattern, desc in secret_patterns:
                if re.search(pattern, line, re.IGNORECASE):
                    # Redact the actual secret
                    redacted_line = re.sub(r'(["\'][^"\']{2})[^"\']*(["\'])', r'\1***\2', line)
                    self._add_finding(
                        f"SEC-{self._next_id():03d}",
                        desc,
                        Severity.MAJOR,
                        Confidence.HIGH,
                        Evidence(file_path, i, redacted_line.strip()),
                        "Hardcoded credentials pose security risk",
                        ["Use environment variables", "Use secure credential management"],
                        "S",
                        ["security", "credentials", "secrets"]
                    )
        
        # Command injection
        if language == 'Python':
            cmd_patterns = [
                (r'os\.system\s*\(.*\+', "Command injection via os.system"),
                (r'subprocess\.(call|run)\s*\([^)]*shell\s*=\s*True', "Shell injection risk"),
                (r'eval\s*\(', "Code injection via eval"),
            ]
            
            for i, line in enumerate(lines, 1):
                for pattern, desc in cmd_patterns:
                    if re.search(pattern, line, re.IGNORECASE):
                        self._add_finding(
                            f"SEC-{self._next_id():03d}",
                            desc,
                            Severity.MAJOR,
                            Confidence.HIGH,
                            Evidence(file_path, i, line.strip()),
                            "Potential command injection vulnerability",
                            ["Use subprocess with lists", "Validate and sanitize input"],
                            "S",
                            ["security", "injection", "command"]
                        )
    
    def _analyze_performance(self, file_path: str, content: str, lines: List[str], language: str):
        """Analyze performance issues."""
        
        # N+1 query patterns
        if language == 'Python':
            for i, line in enumerate(lines, 1):
                if 'for' in line.lower() and (i + 1 < len(lines)):
                    next_line = lines[i]  # i+1 but 0-indexed
                    if any(keyword in next_line.lower() for keyword in ['query', 'select', 'find', 'get']):
                        self._add_finding(
                            f"PERF-{self._next_id():03d}",
                            "Potential N+1 query pattern",
                            Severity.MAJOR,
                            Confidence.MEDIUM,
                            Evidence(file_path, i, f"{line.strip()}\n{next_line.strip()}"),
                            "N+1 queries can cause performance degradation",
                            ["Use bulk operations", "Implement query optimization"],
                            "M",
                            ["performance", "database", "n+1"]
                        )
        
        # Large file operations
        large_file_patterns = [
            (r'\.read\(\)(?!\s*\()', "Reading entire file into memory"),
            (r'\.readlines\(\)', "Reading all lines into memory"),
        ]
        
        for i, line in enumerate(lines, 1):
            for pattern, desc in large_file_patterns:
                if re.search(pattern, line):
                    self._add_finding(
                        f"PERF-{self._next_id():03d}",
                        desc,
                        Severity.MINOR,
                        Confidence.MEDIUM,
                        Evidence(file_path, i, line.strip()),
                        "Large file operations may cause memory issues",
                        ["Use streaming/chunked reading", "Implement pagination"],
                        "S",
                        ["performance", "memory", "io"]
                    )
        
        # Inefficient loops
        if language == 'Python':
            nested_loop_depth = 0
            for i, line in enumerate(lines, 1):
                stripped = line.strip()
                if stripped.startswith('for ') or stripped.startswith('while '):
                    nested_loop_depth += 1
                    if nested_loop_depth >= 3:
                        self._add_finding(
                            f"PERF-{self._next_id():03d}",
                            "Deep nested loops detected",
                            Severity.MINOR,
                            Confidence.MEDIUM,
                            Evidence(file_path, i, line.strip()),
                            "Deeply nested loops can impact performance",
                            ["Consider algorithmic optimization", "Use more efficient data structures"],
                            "M",
                            ["performance", "algorithms", "complexity"]
                        )
                elif not line.startswith(' ') and not line.startswith('\t'):
                    nested_loop_depth = 0
    
    def _analyze_architecture(self, file_path: str, content: str, lines: List[str], language: str):
        """Analyze architecture and design patterns."""
        
        # Large functions/methods
        if language == 'Python':
            in_function = False
            function_lines = 0
            function_start_line = 0
            
            for i, line in enumerate(lines, 1):
                stripped = line.strip()
                if stripped.startswith('def '):
                    if in_function and function_lines > 50:
                        self._add_finding(
                            f"ARCH-{self._next_id():03d}",
                            "Large function detected",
                            Severity.MINOR,
                            Confidence.HIGH,
                            Evidence(file_path, function_start_line, f"Function has {function_lines} lines"),
                            "Large functions are harder to maintain and test",
                            ["Break into smaller functions", "Extract common logic"],
                            "M",
                            ["architecture", "maintainability", "functions"]
                        )
                    
                    in_function = True
                    function_lines = 0
                    function_start_line = i
                elif in_function:
                    if stripped and not stripped.startswith('#'):
                        function_lines += 1
                    elif not line.startswith(' ') and not line.startswith('\t') and stripped:
                        in_function = False
        
        # Missing error handling
        if language == 'Python':
            has_try_catch = 'try:' in content
            has_network_calls = any(keyword in content.lower() for keyword in ['requests.', 'urllib', 'http', 'aiohttp'])
            
            if has_network_calls and not has_try_catch:
                self._add_finding(
                    f"ARCH-{self._next_id():03d}",
                    "Network operations without error handling",
                    Severity.MAJOR,
                    Confidence.HIGH,
                    Evidence(file_path, 1, "File contains network calls but no try/catch"),
                    "Unhandled network errors can crash the application",
                    ["Add try/catch blocks", "Implement retry logic"],
                    "S",
                    ["architecture", "reliability", "error-handling"]
                )
    
    def _analyze_reliability(self, file_path: str, content: str, lines: List[str], language: str):
        """Analyze reliability and operational concerns."""
        
        # Missing logging
        has_logging = any(keyword in content.lower() for keyword in ['logger', 'log.', 'logging', 'print'])
        has_business_logic = len(lines) > 20  # Simple heuristic
        
        if has_business_logic and not has_logging:
            self._add_finding(
                f"REL-{self._next_id():03d}",
                "Missing logging in business logic",
                Severity.MINOR,
                Confidence.MEDIUM,
                Evidence(file_path, 1, "No logging statements found"),
                "Lack of logging makes debugging difficult",
                ["Add structured logging", "Log important operations and errors"],
                "S",
                ["reliability", "observability", "logging"]
            )
        
        # TODO/FIXME comments
        todo_patterns = [
            (r'#\s*TODO\s*:?\s*(.*)', "TODO"),
            (r'#\s*FIXME\s*:?\s*(.*)', "FIXME"),
            (r'#\s*BUG\s*:?\s*(.*)', "BUG"),
            (r'#\s*NOCOMMIT\s*:?\s*(.*)', "NOCOMMIT"),
        ]
        
        for i, line in enumerate(lines, 1):
            for pattern, tag in todo_patterns:
                match = re.search(pattern, line, re.IGNORECASE)
                if match:
                    comment = match.group(1) if match.group(1) else "No description"
                    self._add_finding(
                        f"TODO-{self._next_id():03d}",
                        f"{tag}: {comment}",
                        Severity.MINOR,
                        Confidence.HIGH,
                        Evidence(file_path, i, line.strip()),
                        "Open work item that should be addressed",
                        ["Complete the work item", "Create tracking issue"],
                        "S" if tag == "FIXME" else "M",
                        ["maintainability", "todo", tag.lower()]
                    )
    
    def _analyze_maintainability(self, file_path: str, content: str, lines: List[str], language: str):
        """Analyze maintainability issues."""
        
        # Long lines
        for i, line in enumerate(lines, 1):
            if len(line) > 120:
                self._add_finding(
                    f"MAINT-{self._next_id():03d}",
                    "Line too long",
                    Severity.MINOR,
                    Confidence.HIGH,
                    Evidence(file_path, i, f"{line[:50]}... ({len(line)} chars)"),
                    "Long lines reduce readability",
                    ["Break long lines", "Use consistent formatting"],
                    "S",
                    ["maintainability", "formatting", "readability"]
                )
        
        # Code duplication (simple check)
        line_counts = {}
        for i, line in enumerate(lines, 1):
            stripped = line.strip()
            if len(stripped) > 10 and not stripped.startswith('#'):
                if stripped in line_counts:
                    line_counts[stripped].append(i)
                else:
                    line_counts[stripped] = [i]
        
        for line_content, line_numbers in line_counts.items():
            if len(line_numbers) >= 3:  # Same line appears 3+ times
                self._add_finding(
                    f"MAINT-{self._next_id():03d}",
                    "Potential code duplication",
                    Severity.MINOR,
                    Confidence.MEDIUM,
                    Evidence(file_path, line_numbers[0], f"{line_content} (appears {len(line_numbers)} times)"),
                    "Code duplication increases maintenance burden",
                    ["Extract to function/variable", "Create reusable utilities"],
                    "M",
                    ["maintainability", "duplication", "refactoring"]
                )
                break  # Only report once per file
    
    def _add_finding(self, id_: str, title: str, severity: Severity, confidence: Confidence, 
                     evidence: Evidence, impact: str, remediation: List[str], effort: str, tags: List[str]):
        """Add a finding to the results."""
        finding = Finding(
            id=id_,
            title=title,
            severity=severity,
            confidence=confidence,
            evidence=evidence,
            impact=impact,
            remediation=remediation,
            effort=effort,
            tags=tags,
            related_tests=[]
        )
        self.findings.append(finding)
    
    def _next_id(self) -> int:
        """Get next finding ID number."""
        self.finding_counter += 1
        return self.finding_counter