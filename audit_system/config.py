"""
Configuration system for the code audit tool.
Handles repository configuration, patterns, and output settings.
"""

import os
from dataclasses import dataclass, field
from typing import List, Dict, Any
from datetime import datetime


@dataclass
class RuntimeActions:
    """Runtime actions configuration."""
    attempt_build: bool = True
    attempt_tests: bool = True
    attempt_lints: bool = True
    safe_run_sample: bool = False


@dataclass
class OutputPaths:
    """Output file paths configuration."""
    review_markdown_path: str = "REVIEW/BRANCH-REVIEW.md"
    findings_json_path: str = "REVIEW/BRANCH-FINDINGS.json"
    codemap_markdown_path: str = "REVIEW/CODEMAP.md"


@dataclass
class AuditConfig:
    """Main configuration for the audit system."""
    repo_url: str = ""
    branch: str = "main"
    include_globs: List[str] = field(default_factory=lambda: [
        "**/*.py", "**/*.ts", "**/*.tsx", "**/*.js", "**/*.go", "**/*.rs",
        "**/*.java", "**/*.cs", "**/*.sql", "**/*.yaml", "**/*.yml",
        "**/*.json", "**/*.Dockerfile", "**/Dockerfile", "**/*.sh"
    ])
    exclude_globs: List[str] = field(default_factory=lambda: [
        "**/node_modules/**", "**/dist/**", "**/build/**", "**/.next/**",
        "**/.git/**", "**/venv/**", "**/.venv/**", "**/coverage/**",
        "**/vendor/**", "**/tmp/**", "**/__pycache__/**", "**/*.pyc"
    ])
    runtime_actions: RuntimeActions = field(default_factory=RuntimeActions)
    outputs: OutputPaths = field(default_factory=OutputPaths)
    dates_locale: str = "dd.mm.yyyy"
    style: str = "direct, concise, no fluff; bullets preferred; severity-ranked; Swiss metric/notation."
    
    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> 'AuditConfig':
        """Create config from dictionary."""
        runtime_actions = RuntimeActions(**config_dict.get('RUNTIME_ACTIONS', {}))
        outputs = OutputPaths(**config_dict.get('OUTPUTS', {}))
        
        return cls(
            repo_url=config_dict.get('REPO_URL', ''),
            branch=config_dict.get('BRANCH', 'main'),
            include_globs=config_dict.get('INCLUDE_GLOBS', cls().include_globs),
            exclude_globs=config_dict.get('EXCLUDE_GLOBS', cls().exclude_globs),
            runtime_actions=runtime_actions,
            outputs=outputs,
            dates_locale=config_dict.get('DATES_LOCALE', 'dd.mm.yyyy'),
            style=config_dict.get('STYLE', cls().style)
        )
    
    def ensure_output_dirs(self):
        """Ensure output directories exist."""
        for path in [self.outputs.review_markdown_path, self.outputs.findings_json_path, self.outputs.codemap_markdown_path]:
            dirname = os.path.dirname(path)
            if dirname:  # Only create if there's actually a directory component
                os.makedirs(dirname, exist_ok=True)
    
    def format_date(self, dt: datetime = None) -> str:
        """Format date according to locale setting."""
        if dt is None:
            dt = datetime.now()
        
        if self.dates_locale == "dd.mm.yyyy":
            return dt.strftime("%d.%m.%Y")
        else:
            return dt.strftime("%Y-%m-%d")