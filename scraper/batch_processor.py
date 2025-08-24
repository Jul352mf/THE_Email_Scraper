"""Minimal placeholder for (currently disabled) batch processing.

The legacy synchronous batch implementation depended on the removed
`orchestrator` module. Until an async batch pipeline is implemented this
module is intentionally minimal to avoid dead/buggy code and linter noise.

Public surface kept so existing imports (`from scraper.batch_processor import
batch_processor`) don't break. Any attempt to call processing methods will
raise a clear NotImplementedError explaining the status.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import List

import pandas as pd

log = logging.getLogger(__name__)


class BatchProcessor:
    """Disabled batch processor placeholder.

    Only input discovery and basic file reading are retained for future reuse.
    """

    SUPPORTED_FORMATS = {".xlsx", ".xls", ".csv"}

    def __init__(self, input_dir: str = "input") -> None:
        self.input_dir = Path(input_dir)
        self.input_dir.mkdir(exist_ok=True)

    # ---- Utility helpers (kept for future async implementation) ----
    def find_input_files(self) -> List[Path]:  # pragma: no cover - trivial
        files = [
            p for p in self.input_dir.iterdir()
            if p.is_file() and p.suffix.lower() in self.SUPPORTED_FORMATS
        ]
        files.sort(key=lambda f: f.stat().st_mtime, reverse=True)
        log.info("Found %d input files", len(files))
        return files

    def read_input_file(self, file_path: Path) -> pd.DataFrame:
        if file_path.suffix.lower() in {".xlsx", ".xls"}:
            df = pd.read_excel(file_path)
        else:
            df = pd.read_csv(file_path)
        if "Company" not in df.columns:
            # Attempt to normalize a variant
            for col in df.columns:
                if "company" in col.lower():
                    df = df.rename(columns={col: "Company"})
                    break
        return df

    # ---- Disabled API ----
    def process_single_file(self, *_, **__):  # pragma: no cover - disabled
        raise NotImplementedError(
            "Batch processing disabled: async implementation pending"
        )

    def process_all_files(self, *_, **__):  # pragma: no cover - disabled
        raise NotImplementedError(
            "Batch processing disabled: async implementation pending"
        )


batch_processor = BatchProcessor()
