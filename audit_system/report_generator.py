"""
Report generation module for audit system.
Generates markdown reports, JSON findings, and code maps.
"""

import json
import os
from typing import List, Dict, Any
from datetime import datetime

from .config import AuditConfig
from .file_enumeration import FileInfo
from .code_analysis import Finding, Severity


class ReportGenerator:
    """Generates audit reports in various formats."""
    
    def __init__(self, config: AuditConfig, repo_path: str):
        self.config = config
        self.repo_path = repo_path
        self.repo_name = os.path.basename(repo_path)
        
    def generate_all_reports(self, file_infos: List[FileInfo], findings: List[Finding]):
        """Generate all audit reports."""
        # Ensure output directories exist
        self.config.ensure_output_dirs()
        
        # Generate individual reports
        self.generate_main_review(file_infos, findings)
        self.generate_findings_json(findings)
        self.generate_codemap(file_infos)
    
    def generate_main_review(self, file_infos: List[FileInfo], findings: List[Finding]):
        """Generate the main review markdown report."""
        content = []
        
        # Title and date
        date_str = self.config.format_date()
        content.append(f"# {self.repo_name}@{self.config.branch} — Code-First Audit ({date_str})")
        content.append("")
        
        # Executive Summary
        content.extend(self._generate_executive_summary(file_infos, findings))
        
        # What it is / What it's good for
        content.extend(self._generate_purpose_section(file_infos))
        
        # Architecture Overview
        content.extend(self._generate_architecture_overview(file_infos))
        
        # System Design Choices
        content.extend(self._generate_system_design_section(file_infos, findings))
        
        # Top 10 Risks & Issues
        content.extend(self._generate_risks_section(findings))
        
        # Security Review
        content.extend(self._generate_security_section(findings))
        
        # Reliability & Ops
        content.extend(self._generate_reliability_section(findings))
        
        # Performance
        content.extend(self._generate_performance_section(findings))
        
        # Data & Schema
        content.extend(self._generate_data_section(file_infos, findings))
        
        # Tests & Quality Gates
        content.extend(self._generate_testing_section(file_infos, findings))
        
        # Build/CI/CD
        content.extend(self._generate_build_section(file_infos))
        
        # Backlog
        content.extend(self._generate_backlog_section(findings))
        
        # Assumptions & Unknowns
        content.extend(self._generate_assumptions_section())
        
        # Coverage Ledger
        content.extend(self._generate_coverage_ledger(file_infos))
        
        # Appendix: Code Map
        content.extend(self._generate_appendix_codemap(file_infos))
        
        # Write to file
        with open(self.config.outputs.review_markdown_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(content))
    
    def generate_findings_json(self, findings: List[Finding]):
        """Generate machine-readable findings JSON."""
        findings_data = []
        
        for finding in findings:
            finding_dict = {
                "id": finding.id,
                "title": finding.title,
                "severity": finding.severity.value,
                "confidence": finding.confidence.value,
                "evidence": {
                    "path": finding.evidence.path,
                    "line": finding.evidence.line,
                    "snippet": finding.evidence.snippet
                },
                "impact": finding.impact,
                "remediation": finding.remediation,
                "effort": finding.effort,
                "tags": finding.tags,
                "introduced_in_commit": finding.introduced_in_commit,
                "related_tests": finding.related_tests or []
            }
            findings_data.append(finding_dict)
        
        with open(self.config.outputs.findings_json_path, 'w', encoding='utf-8') as f:
            json.dump(findings_data, f, indent=2, ensure_ascii=False)
    
    def generate_codemap(self, file_infos: List[FileInfo]):
        """Generate code structure map."""
        content = []
        content.append("# Code Map")
        content.append("")
        content.append("Detailed breakdown of repository structure and dependencies.")
        content.append("")
        
        # Group files by directory
        dirs = {}
        for file_info in file_infos:
            dir_path = os.path.dirname(file_info.path) or "."
            if dir_path not in dirs:
                dirs[dir_path] = []
            dirs[dir_path].append(file_info)
        
        for dir_path in sorted(dirs.keys()):
            content.append(f"## {dir_path}/")
            content.append("")
            
            files = sorted(dirs[dir_path], key=lambda x: x.path)
            for file_info in files:
                content.append(f"### {os.path.basename(file_info.path)} ({file_info.language})")
                content.append(f"- **LOC**: {file_info.loc}")
                content.append(f"- **Size**: {file_info.size_bytes} bytes")
                content.append(f"- **Purpose**: {file_info.purpose}")
                if file_info.top_symbols:
                    content.append(f"- **Top Symbols**: {', '.join(file_info.top_symbols)}")
                content.append("")
        
        with open(self.config.outputs.codemap_markdown_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(content))
    
    def _generate_executive_summary(self, file_infos: List[FileInfo], findings: List[Finding]) -> List[str]:
        """Generate executive summary section."""
        content = ["## 1. Executive Summary"]
        content.append("")
        
        # Calculate stats
        total_loc = sum(f.loc for f in file_infos)
        languages = set(f.language for f in file_infos)
        critical_findings = [f for f in findings if f.severity == Severity.CRITICAL]
        major_findings = [f for f in findings if f.severity == Severity.MAJOR]
        
        content.append(f"- **Repository**: {self.repo_name} on branch `{self.config.branch}`")
        content.append(f"- **Scale**: {len(file_infos)} files, {total_loc:,} lines of code")
        content.append(f"- **Languages**: {', '.join(sorted(languages))}")
        content.append(f"- **Critical Issues**: {len(critical_findings)} found")
        content.append(f"- **Major Issues**: {len(major_findings)} found")
        
        # Determine primary purpose from file analysis
        main_files = [f for f in file_infos if 'main' in f.path.lower() or 'cli' in f.path.lower()]
        if main_files:
            content.append(f"- **Primary Function**: {main_files[0].purpose}")
        
        content.append("")
        return content
    
    def _generate_purpose_section(self, file_infos: List[FileInfo]) -> List[str]:
        """Generate what it is/what it's good for section."""
        content = ["## 2. What it is / What it's good for"]
        content.append("")
        
        # Analyze file patterns to determine purpose
        file_purposes = [f.purpose for f in file_infos]
        purpose_counts = {}
        for purpose in file_purposes:
            purpose_counts[purpose] = purpose_counts.get(purpose, 0) + 1
        
        # Main entry points
        entry_points = [f for f in file_infos if any(keyword in f.path.lower() 
                       for keyword in ['main', 'cli', '__main__', 'app', 'server'])]
        
        if entry_points:
            content.append("**Primary Use Cases:**")
            for ep in entry_points[:3]:  # Top 3 entry points
                content.append(f"- {ep.purpose} via `{ep.path}`")
            content.append("")
        
        # Technology stack
        languages = list(set(f.language for f in file_infos))
        content.append("**Technology Profile:**")
        for lang in sorted(languages):
            lang_files = [f for f in file_infos if f.language == lang]
            content.append(f"- {lang}: {len(lang_files)} files")
        content.append("")
        
        return content
    
    def _generate_architecture_overview(self, file_infos: List[FileInfo]) -> List[str]:
        """Generate architecture overview section."""
        content = ["## 3. Architecture Overview"]
        content.append("")
        
        # Simple ASCII diagram
        dirs = set(os.path.dirname(f.path) for f in file_infos if os.path.dirname(f.path))
        content.append("**Module Structure:**")
        content.append("```")
        for dir_name in sorted(dirs):
            files_in_dir = [f for f in file_infos if os.path.dirname(f.path) == dir_name]
            content.append(f"{dir_name}/")
            for file_info in files_in_dir[:3]:  # Show max 3 files per dir
                content.append(f"  ├── {os.path.basename(file_info.path)} ({file_info.language})")
            if len(files_in_dir) > 3:
                content.append(f"  └── ... ({len(files_in_dir)-3} more)")
        content.append("```")
        content.append("")
        
        # Tech stack table
        content.append("**Tech Stack by Layer:**")
        content.append("| Layer | Technology |")
        content.append("|-------|------------|")
        
        languages = set(f.language for f in file_infos)
        for lang in sorted(languages):
            if lang == 'Python':
                content.append("| Runtime | Python |")
            elif lang in ['JavaScript', 'TypeScript']:
                content.append("| Frontend | JavaScript/TypeScript |")
            elif lang == 'Dockerfile':
                content.append("| Container | Docker |")
            elif lang in ['YAML', 'JSON']:
                content.append("| Config | YAML/JSON |")
        
        content.append("")
        return content
    
    def _generate_system_design_section(self, file_infos: List[FileInfo], findings: List[Finding]) -> List[str]:
        """Generate system design choices section."""
        content = ["## 4. System Design Choices — Strengths & Trade-offs"]
        content.append("")
        
        # Analyze patterns from files
        has_async = any('async' in f.path or 'async' in ' '.join(f.top_symbols) 
                       for f in file_infos)
        has_config = any('config' in f.path.lower() for f in file_infos)
        has_tests = any('test' in f.path.lower() for f in file_infos)
        
        content.append("**Strengths:**")
        if has_async:
            content.append("- Asynchronous processing capabilities")
        if has_config:
            content.append("- Externalized configuration")
        if has_tests:
            content.append("- Test coverage implemented")
        
        content.append("")
        content.append("**Trade-offs:**")
        arch_findings = [f for f in findings if 'arch' in f.tags]
        for finding in arch_findings[:3]:  # Top 3 architectural issues
            content.append(f"- {finding.impact} ({finding.evidence.path}:{finding.evidence.line})")
        
        content.append("")
        return content
    
    def _generate_risks_section(self, findings: List[Finding]) -> List[str]:
        """Generate risks and issues section."""
        content = ["## 5. Risks & Issues (Top 10)"]
        content.append("")
        
        # Sort by severity
        severity_order = {Severity.BLOCKER: 0, Severity.CRITICAL: 1, Severity.MAJOR: 2, Severity.MINOR: 3}
        sorted_findings = sorted(findings, key=lambda f: (severity_order[f.severity], f.title))
        
        for i, finding in enumerate(sorted_findings[:10], 1):
            content.append(f"### {i}. {finding.title}")
            content.append(f"**Severity**: {finding.severity.value} | **Confidence**: {finding.confidence.value}")
            content.append(f"**Evidence**: `{finding.evidence.path}:{finding.evidence.line}`")
            content.append(f"```")
            content.append(finding.evidence.snippet)
            content.append(f"```")
            content.append(f"**Impact**: {finding.impact}")
            content.append(f"**Fix**: {'; '.join(finding.remediation)}")
            content.append("")
        
        return content
    
    def _generate_security_section(self, findings: List[Finding]) -> List[str]:
        """Generate security review section."""
        content = ["## 6. Security Review"]
        content.append("")
        
        sec_findings = [f for f in findings if 'security' in f.tags]
        if not sec_findings:
            content.append("- No significant security issues detected")
            content.append("")
            return content
        
        # Group by security type
        sec_types = {}
        for finding in sec_findings:
            for tag in finding.tags:
                if tag in ['injection', 'xss', 'credentials', 'secrets']:
                    if tag not in sec_types:
                        sec_types[tag] = []
                    sec_types[tag].append(finding)
        
        for sec_type, type_findings in sec_types.items():
            content.append(f"### {sec_type.title()} Vulnerabilities")
            for finding in type_findings:
                content.append(f"- **{finding.title}** ({finding.severity.value})")
                content.append(f"  - Location: `{finding.evidence.path}:{finding.evidence.line}`")
                content.append(f"  - Fix: {finding.remediation[0] if finding.remediation else 'Review needed'}")
            content.append("")
        
        return content
    
    def _generate_reliability_section(self, findings: List[Finding]) -> List[str]:
        """Generate reliability and ops section."""
        content = ["## 7. Reliability & Ops"]
        content.append("")
        
        rel_findings = [f for f in findings if 'reliability' in f.tags or 'observability' in f.tags]
        
        content.append("### Observability")
        logging_issues = [f for f in rel_findings if 'logging' in f.tags]
        if logging_issues:
            content.append("**Issues Found:**")
            for finding in logging_issues[:3]:
                content.append(f"- {finding.title} in `{finding.evidence.path}`")
        else:
            content.append("- No major logging issues detected")
        
        content.append("")
        content.append("### Error Handling")
        error_issues = [f for f in rel_findings if 'error-handling' in f.tags]
        if error_issues:
            content.append("**Issues Found:**")
            for finding in error_issues[:3]:
                content.append(f"- {finding.title} in `{finding.evidence.path}`")
        else:
            content.append("- Error handling patterns present")
        
        content.append("")
        return content
    
    def _generate_performance_section(self, findings: List[Finding]) -> List[str]:
        """Generate performance section."""
        content = ["## 8. Performance"]
        content.append("")
        
        perf_findings = [f for f in findings if 'performance' in f.tags]
        
        if not perf_findings:
            content.append("- No significant performance issues detected")
            content.append("")
            return content
        
        content.append("**Issues Found:**")
        for finding in perf_findings:
            content.append(f"- **{finding.title}** ({finding.severity.value})")
            content.append(f"  - Location: `{finding.evidence.path}:{finding.evidence.line}`")
            content.append(f"  - Impact: {finding.impact}")
            content.append(f"  - Fix: {finding.remediation[0] if finding.remediation else 'Review needed'}")
        
        content.append("")
        return content
    
    def _generate_data_section(self, file_infos: List[FileInfo], findings: List[Finding]) -> List[str]:
        """Generate data and schema section."""
        content = ["## 9. Data & Schema"]
        content.append("")
        
        # Look for data-related files
        data_files = [f for f in file_infos if any(keyword in f.path.lower() 
                     for keyword in ['model', 'schema', 'db', 'sql', 'migration'])]
        
        if data_files:
            content.append("**Data-related files:**")
            for f in data_files:
                content.append(f"- `{f.path}` ({f.purpose})")
        else:
            content.append("- No dedicated data model files detected")
        
        content.append("")
        return content
    
    def _generate_testing_section(self, file_infos: List[FileInfo], findings: List[Finding]) -> List[str]:
        """Generate tests and quality gates section."""
        content = ["## 10. Tests & Quality Gates"]
        content.append("")
        
        test_files = [f for f in file_infos if 'test' in f.path.lower()]
        total_files = len(file_infos)
        
        content.append(f"**Test Coverage:**")
        content.append(f"- Test files: {len(test_files)}")
        content.append(f"- Source files: {total_files - len(test_files)}")
        if total_files > 0:
            test_ratio = len(test_files) / total_files * 100
            content.append(f"- Test ratio: {test_ratio:.1f}%")
        
        if test_files:
            content.append("")
            content.append("**Test files:**")
            for f in test_files:
                content.append(f"- `{f.path}` ({f.loc} LOC)")
        
        content.append("")
        return content
    
    def _generate_build_section(self, file_infos: List[FileInfo]) -> List[str]:
        """Generate build/CI/CD section."""
        content = ["## 11. Build/CI/CD"]
        content.append("")
        
        build_files = [f for f in file_infos if any(name in f.path.lower() 
                      for name in ['dockerfile', 'requirements.txt', 'package.json', 'makefile'])]
        
        if build_files:
            content.append("**Build-related files:**")
            for f in build_files:
                content.append(f"- `{f.path}` ({f.purpose})")
        else:
            content.append("- No build configuration files detected")
        
        content.append("")
        return content
    
    def _generate_backlog_section(self, findings: List[Finding]) -> List[str]:
        """Generate backlog and prioritization section."""
        content = ["## 15. Backlog"]
        content.append("")
        
        # Categorize by effort
        fast_wins = [f for f in findings if f.effort == 'S']
        high_leverage = [f for f in findings if f.effort == 'M']
        deep_work = [f for f in findings if f.effort == 'L']
        
        content.append("### Fast Wins (≤2h)")
        for finding in fast_wins[:5]:
            content.append(f"- {finding.title} ({finding.severity.value})")
        
        content.append("")
        content.append("### High-Leverage (≤1d)")
        for finding in high_leverage[:5]:
            content.append(f"- {finding.title} ({finding.severity.value})")
        
        content.append("")
        content.append("### Deep Work (>1d)")
        for finding in deep_work[:3]:
            content.append(f"- {finding.title} ({finding.severity.value})")
        
        content.append("")
        return content
    
    def _generate_assumptions_section(self) -> List[str]:
        """Generate assumptions and unknowns section."""
        content = ["## 16. Assumptions & Unknowns"]
        content.append("")
        content.append("**Assumptions:**")
        content.append("- Static analysis only; no runtime testing performed")
        content.append("- Pattern-based detection; may have false positives/negatives")
        content.append("- Code review based on current branch state")
        content.append("")
        content.append("**Unknowns:**")
        content.append("- Runtime behavior and performance characteristics")
        content.append("- Integration with external services")
        content.append("- Production deployment configuration")
        content.append("")
        return content
    
    def _generate_coverage_ledger(self, file_infos: List[FileInfo]) -> List[str]:
        """Generate coverage ledger section."""
        content = ["## 17. Coverage Ledger"]
        content.append("")
        content.append("| File | Lang | LOC | Last Commit | SHA12 | Top Symbols | Purpose |")
        content.append("|------|------|-----|-------------|-------|-------------|---------|")
        
        for f in file_infos:
            symbols_str = ', '.join(f.top_symbols[:3]) if f.top_symbols else '-'
            commit_str = f"{f.last_commit_hash} ({f.last_commit_date})"
            content.append(f"| {f.path} | {f.language} | {f.loc} | {commit_str} | {f.sha12_checksum} | {symbols_str} | {f.purpose} |")
        
        coverage_pct = 100.0  # We analyze all enumerated files
        content.append("")
        content.append(f"**Coverage**: {len(file_infos)} files analyzed ({coverage_pct:.0f}% of in-scope files)")
        content.append("")
        return content
    
    def _generate_appendix_codemap(self, file_infos: List[FileInfo]) -> List[str]:
        """Generate appendix code map section."""
        content = ["## 18. Appendix: Code Map"]
        content.append("")
        content.append("See separate CODEMAP.md for detailed module breakdown.")
        content.append("")
        return content