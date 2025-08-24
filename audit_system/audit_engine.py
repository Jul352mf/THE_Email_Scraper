"""
Main audit engine that orchestrates the entire audit process.
"""

import os
import sys
import subprocess
from typing import List, Optional
import traceback

from .config import AuditConfig
from .file_enumeration import FileEnumerator, FileInfo
from .code_analysis import CodeAnalyzer, Finding
from .report_generator import ReportGenerator


class AuditEngine:
    """Main audit orchestrator."""
    
    def __init__(self, config: AuditConfig, repo_path: str):
        self.config = config
        self.repo_path = repo_path
        self.file_enumerator = FileEnumerator(repo_path, config.include_globs, config.exclude_globs)
        self.code_analyzer = CodeAnalyzer(repo_path)
        self.report_generator = ReportGenerator(config, repo_path)
        
    def run_full_audit(self) -> bool:
        """Run the complete audit process."""
        try:
            print(f"🔍 Starting audit of {self.repo_path}")
            
            # 1. Enumerate files
            print("📁 Enumerating files...")
            file_infos = self.file_enumerator.enumerate_files()
            print(f"   Found {len(file_infos)} files to analyze")
            
            # 2. Analyze code
            print("🔬 Analyzing code...")
            findings = self.code_analyzer.analyze_files(file_infos)
            print(f"   Found {len(findings)} issues")
            
            # 3. Runtime actions (if enabled)
            if self.config.runtime_actions.attempt_build:
                print("🔨 Attempting build...")
                self._attempt_build()
            
            if self.config.runtime_actions.attempt_tests:
                print("🧪 Attempting tests...")
                self._attempt_tests()
            
            if self.config.runtime_actions.attempt_lints:
                print("🧹 Attempting lints...")
                self._attempt_lints()
            
            # 4. Generate reports
            print("📊 Generating reports...")
            self.report_generator.generate_all_reports(file_infos, findings)
            
            print("✅ Audit completed successfully")
            self._print_summary(file_infos, findings)
            
            return True
            
        except Exception as e:
            print(f"❌ Audit failed: {str(e)}")
            traceback.print_exc()
            return False
    
    def _attempt_build(self) -> Optional[str]:
        """Attempt to build the project."""
        build_commands = [
            # Python
            ["python", "-m", "py_compile"] + [f.path for f in self.file_enumerator.enumerate_files() 
                                             if f.path.endswith('.py')][:5],  # Test first 5 files
            ["python", "-m", "compileall", "."],
            # Node.js
            ["npm", "run", "build"],
            ["yarn", "build"],
            # Go
            ["go", "build", "./..."],
            # Rust
            ["cargo", "build"],
            # Java
            ["mvn", "compile"],
            ["gradle", "build"],
        ]
        
        for cmd in build_commands:
            if self._command_exists(cmd[0]):
                try:
                    result = subprocess.run(
                        cmd, 
                        cwd=self.repo_path, 
                        capture_output=True, 
                        text=True, 
                        timeout=60
                    )
                    if result.returncode == 0:
                        print(f"   ✅ Build succeeded with: {' '.join(cmd)}")
                        return "SUCCESS"
                    else:
                        print(f"   ⚠️  Build failed with: {' '.join(cmd)}")
                        if result.stderr:
                            print(f"      Error: {result.stderr[:200]}...")
                except subprocess.TimeoutExpired:
                    print(f"   ⏰ Build timeout with: {' '.join(cmd)}")
                except Exception as e:
                    print(f"   ❌ Build error with {' '.join(cmd)}: {str(e)}")
        
        return "NO_BUILD_FOUND"
    
    def _attempt_tests(self) -> Optional[str]:
        """Attempt to run tests."""
        test_commands = [
            # Python
            ["python", "-m", "pytest"],
            ["python", "-m", "unittest", "discover"],
            ["python", "-m", "doctest"],
            # Node.js
            ["npm", "test"],
            ["yarn", "test"],
            # Go
            ["go", "test", "./..."],
            # Rust
            ["cargo", "test"],
            # Java
            ["mvn", "test"],
            ["gradle", "test"],
        ]
        
        for cmd in test_commands:
            if self._command_exists(cmd[0]):
                try:
                    result = subprocess.run(
                        cmd, 
                        cwd=self.repo_path, 
                        capture_output=True, 
                        text=True, 
                        timeout=120
                    )
                    if result.returncode == 0:
                        print(f"   ✅ Tests passed with: {' '.join(cmd)}")
                        return "PASSED"
                    else:
                        print(f"   ❌ Tests failed with: {' '.join(cmd)}")
                        if result.stdout:
                            print(f"      Output: {result.stdout[-200:]}")
                except subprocess.TimeoutExpired:
                    print(f"   ⏰ Test timeout with: {' '.join(cmd)}")
                except Exception as e:
                    print(f"   ❌ Test error with {' '.join(cmd)}: {str(e)}")
        
        return "NO_TESTS_FOUND"
    
    def _attempt_lints(self) -> Optional[str]:
        """Attempt to run linters."""
        lint_commands = [
            # Python
            ["flake8", "."],
            ["pylint", "--errors-only", "."],
            ["python", "-m", "py_compile"] + [f.path for f in self.file_enumerator.enumerate_files() 
                                             if f.path.endswith('.py')][:3],
            # JavaScript/TypeScript
            ["eslint", "."],
            ["tslint", "."],
            # Go
            ["golangci-lint", "run"],
            ["go", "vet", "./..."],
            # Rust
            ["cargo", "clippy"],
            # General
            ["shellcheck", "*.sh"],
        ]
        
        success_count = 0
        for cmd in lint_commands:
            if self._command_exists(cmd[0]):
                try:
                    result = subprocess.run(
                        cmd, 
                        cwd=self.repo_path, 
                        capture_output=True, 
                        text=True, 
                        timeout=60
                    )
                    if result.returncode == 0:
                        print(f"   ✅ Lint passed with: {' '.join(cmd)}")
                        success_count += 1
                    else:
                        print(f"   ⚠️  Lint issues with: {' '.join(cmd)}")
                        if result.stdout:
                            print(f"      Issues: {result.stdout[:200]}...")
                except subprocess.TimeoutExpired:
                    print(f"   ⏰ Lint timeout with: {' '.join(cmd)}")
                except Exception as e:
                    print(f"   ❌ Lint error with {' '.join(cmd)}: {str(e)}")
        
        return "SUCCESS" if success_count > 0 else "NO_LINTERS_FOUND"
    
    def _command_exists(self, command: str) -> bool:
        """Check if a command exists in the system PATH."""
        try:
            subprocess.run([command, "--help"], 
                         capture_output=True, 
                         timeout=5,
                         cwd=self.repo_path)
            return True
        except (subprocess.TimeoutExpired, FileNotFoundError, subprocess.SubprocessError):
            return False
    
    def _print_summary(self, file_infos: List[FileInfo], findings: List[Finding]):
        """Print audit summary."""
        print("\n" + "="*60)
        print("AUDIT SUMMARY")
        print("="*60)
        
        # File statistics
        total_loc = sum(f.loc for f in file_infos)
        languages = set(f.language for f in file_infos)
        print(f"📁 Files analyzed: {len(file_infos)}")
        print(f"📊 Total LOC: {total_loc:,}")
        print(f"🔤 Languages: {', '.join(sorted(languages))}")
        
        # Findings by severity
        from .code_analysis import Severity
        severity_counts = {}
        for severity in Severity:
            count = len([f for f in findings if f.severity == severity])
            if count > 0:
                severity_counts[severity.value] = count
        
        print(f"\n🔍 Issues found: {len(findings)}")
        for severity, count in severity_counts.items():
            print(f"   {severity}: {count}")
        
        # Top issue categories
        all_tags = []
        for finding in findings:
            all_tags.extend(finding.tags)
        
        tag_counts = {}
        for tag in all_tags:
            tag_counts[tag] = tag_counts.get(tag, 0) + 1
        
        if tag_counts:
            print(f"\n🏷️  Top issue categories:")
            for tag, count in sorted(tag_counts.items(), key=lambda x: x[1], reverse=True)[:5]:
                print(f"   {tag}: {count}")
        
        # Output files
        print(f"\n📋 Reports generated:")
        print(f"   Main review: {self.config.outputs.review_markdown_path}")
        print(f"   Findings JSON: {self.config.outputs.findings_json_path}")
        print(f"   Code map: {self.config.outputs.codemap_markdown_path}")
        print("="*60)