"""
File enumeration and filtering utilities.
Handles glob patterns and file discovery within repositories.
"""

import os
import glob
import hashlib
from pathlib import Path
from typing import List, Set, Dict, Optional
from dataclasses import dataclass
from datetime import datetime
import subprocess


@dataclass
class FileInfo:
    """Information about a source file."""
    path: str
    language: str
    loc: int
    last_commit_hash: str
    last_commit_date: str
    sha12_checksum: str
    top_symbols: List[str]
    purpose: str
    size_bytes: int


class FileEnumerator:
    """Enumerates and filters files based on glob patterns."""
    
    LANGUAGE_EXTENSIONS = {
        '.py': 'Python',
        '.js': 'JavaScript', 
        '.ts': 'TypeScript',
        '.tsx': 'TypeScript JSX',
        '.java': 'Java',
        '.go': 'Go',
        '.rs': 'Rust',
        '.cs': 'C#',
        '.sql': 'SQL',
        '.yaml': 'YAML',
        '.yml': 'YAML',
        '.json': 'JSON',
        '.sh': 'Shell',
        '.md': 'Markdown',
        '.dockerfile': 'Dockerfile',
        '': 'Dockerfile'  # For files named just "Dockerfile"
    }
    
    def __init__(self, repo_path: str, include_globs: List[str], exclude_globs: List[str]):
        self.repo_path = repo_path
        self.include_globs = include_globs
        self.exclude_globs = exclude_globs
        
    def enumerate_files(self) -> List[FileInfo]:
        """Enumerate all files matching include/exclude patterns."""
        all_files = set()
        
        # Get all files matching include patterns
        for pattern in self.include_globs:
            matches = glob.glob(os.path.join(self.repo_path, pattern), recursive=True)
            for match in matches:
                if os.path.isfile(match):
                    all_files.add(os.path.relpath(match, self.repo_path))
        
        # Filter out files matching exclude patterns
        filtered_files = set()
        for file_path in all_files:
            excluded = False
            for pattern in self.exclude_globs:
                if glob.fnmatch.fnmatch(file_path, pattern.replace('**/', '')):
                    excluded = True
                    break
            if not excluded:
                filtered_files.add(file_path)
        
        # Create FileInfo objects
        file_infos = []
        for file_path in sorted(filtered_files):
            full_path = os.path.join(self.repo_path, file_path)
            if os.path.exists(full_path):
                file_info = self._create_file_info(file_path, full_path)
                file_infos.append(file_info)
                
        return file_infos
    
    def _create_file_info(self, rel_path: str, full_path: str) -> FileInfo:
        """Create FileInfo object for a single file."""
        # Determine language
        ext = Path(full_path).suffix.lower()
        if ext == '' and Path(full_path).name.lower() == 'dockerfile':
            language = 'Dockerfile'
        else:
            language = self.LANGUAGE_EXTENSIONS.get(ext, 'Unknown')
        
        # Count lines
        loc = self._count_lines(full_path)
        
        # Get git info
        commit_hash, commit_date = self._get_git_info(rel_path)
        
        # Calculate checksum
        checksum = self._calculate_checksum(full_path)
        
        # Extract top-level symbols (simplified)
        top_symbols = self._extract_symbols(full_path, language)
        
        # Determine purpose (simplified)
        purpose = self._determine_purpose(rel_path, language)
        
        # Get file size
        size_bytes = os.path.getsize(full_path) if os.path.exists(full_path) else 0
        
        return FileInfo(
            path=rel_path,
            language=language,
            loc=loc,
            last_commit_hash=commit_hash,
            last_commit_date=commit_date,
            sha12_checksum=checksum[:12],
            top_symbols=top_symbols,
            purpose=purpose,
            size_bytes=size_bytes
        )
    
    def _count_lines(self, file_path: str) -> int:
        """Count lines of code in a file."""
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                return sum(1 for line in f)
        except Exception:
            return 0
    
    def _get_git_info(self, file_path: str) -> tuple[str, str]:
        """Get git commit info for a file."""
        try:
            # Get last commit hash and date
            cmd = f'cd "{self.repo_path}" && git --no-pager log -1 --format="%H|%ad" --date=format:"%d.%m.%Y" -- "{file_path}"'
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            if result.returncode == 0 and result.stdout.strip():
                parts = result.stdout.strip().split('|')
                return parts[0][:7], parts[1] if len(parts) > 1 else "unknown"
        except Exception:
            pass
        return "unknown", "unknown"
    
    def _calculate_checksum(self, file_path: str) -> str:
        """Calculate SHA256 checksum of file contents."""
        try:
            with open(file_path, 'rb') as f:
                content = f.read()
                return hashlib.sha256(content).hexdigest()
        except Exception:
            return "unknown"
    
    def _extract_symbols(self, file_path: str, language: str) -> List[str]:
        """Extract top-level symbols (functions, classes) from file."""
        symbols = []
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
                
            if language == 'Python':
                symbols = self._extract_python_symbols(content)
            elif language in ['JavaScript', 'TypeScript']:
                symbols = self._extract_js_symbols(content)
            elif language == 'Java':
                symbols = self._extract_java_symbols(content)
            elif language == 'Go':
                symbols = self._extract_go_symbols(content)
                
        except Exception:
            pass
        
        return symbols[:5]  # Limit to top 5 symbols
    
    def _extract_python_symbols(self, content: str) -> List[str]:
        """Extract Python classes and functions."""
        symbols = []
        lines = content.split('\n')
        for line in lines:
            stripped = line.strip()
            if stripped.startswith('def ') and not stripped.startswith('def _'):
                func_name = stripped.split('(')[0].replace('def ', '')
                symbols.append(f"{func_name}()")
            elif stripped.startswith('class '):
                class_name = stripped.split('(')[0].replace('class ', '').replace(':', '')
                symbols.append(class_name)
        return symbols
    
    def _extract_js_symbols(self, content: str) -> List[str]:
        """Extract JavaScript/TypeScript functions and classes."""
        symbols = []
        lines = content.split('\n')
        for line in lines:
            stripped = line.strip()
            if 'function ' in stripped:
                try:
                    func_name = stripped.split('function ')[1].split('(')[0].strip()
                    if func_name:
                        symbols.append(f"{func_name}()")
                except:
                    pass
            elif stripped.startswith('class '):
                try:
                    class_name = stripped.split('class ')[1].split(' ')[0].split('{')[0].strip()
                    symbols.append(class_name)
                except:
                    pass
        return symbols
    
    def _extract_java_symbols(self, content: str) -> List[str]:
        """Extract Java classes and methods."""
        symbols = []
        lines = content.split('\n')
        for line in lines:
            stripped = line.strip()
            if 'class ' in stripped and not stripped.startswith('//'):
                try:
                    class_name = stripped.split('class ')[1].split(' ')[0].split('{')[0].strip()
                    symbols.append(class_name)
                except:
                    pass
            elif 'public ' in stripped and '(' in stripped and not stripped.startswith('//'):
                try:
                    method_part = stripped.split('(')[0].strip()
                    method_name = method_part.split(' ')[-1]
                    symbols.append(f"{method_name}()")
                except:
                    pass
        return symbols
    
    def _extract_go_symbols(self, content: str) -> List[str]:
        """Extract Go functions and types."""
        symbols = []
        lines = content.split('\n')
        for line in lines:
            stripped = line.strip()
            if stripped.startswith('func '):
                try:
                    func_name = stripped.split('func ')[1].split('(')[0].strip()
                    symbols.append(f"{func_name}()")
                except:
                    pass
            elif stripped.startswith('type ') and 'struct' in stripped:
                try:
                    type_name = stripped.split('type ')[1].split(' ')[0].strip()
                    symbols.append(type_name)
                except:
                    pass
        return symbols
    
    def _determine_purpose(self, file_path: str, language: str) -> str:
        """Determine the purpose of a file based on its path and content."""
        path_lower = file_path.lower()
        
        # Common patterns
        if 'test' in path_lower:
            return "Test suite"
        elif 'config' in path_lower or file_path.endswith('.config.js') or file_path.endswith('.config.ts'):
            return "Configuration"
        elif path_lower.endswith('.md'):
            return "Documentation"
        elif 'docker' in path_lower:
            return "Container configuration"
        elif 'cli' in path_lower or 'main' in path_lower:
            return "CLI entry point"
        elif 'api' in path_lower or 'server' in path_lower:
            return "API service"
        elif 'util' in path_lower or 'helper' in path_lower:
            return "Utility functions"
        elif 'model' in path_lower or 'schema' in path_lower:
            return "Data models"
        elif 'service' in path_lower:
            return "Business logic service"
        else:
            return "Source module"