# Deprecated / Archived Code

This repository previously contained legacy synchronous and experimental modules. They are not used by the current async-only pipeline (CLI -> AsyncOrchestrator).

## Folders

- `archive/` (kept temporarily):
  - `crawler_old.py`, `http_old.py`, `sitemap_old.py`: Legacy synchronous crawling implementations.
  - `rate_limiter.py`, `proxy.py`: Old networking helpers, superseded by async HTTP client + built-in rate controls.
  - `worker.py`, `progress.py`, `tests.py`: Legacy multiprocessing / progress logic replaced by `progress_tracker` and async orchestration.
  - `file_updater.py`: One-off maintenance script.

## Planned Removal

These files will be removed in a future cleanup pass unless a concrete migration need arises:

| File | Action |
| ---- | ------ |
| crawler_old.py | Remove |
| http_old.py | Remove |
| sitemap_old.py | Remove |
| rate_limiter.py | Remove |
| proxy.py | Remove |
| progress.py | Remove |
| worker.py | Remove |
| tests.py | Remove (not part of new test suite) |
| file_updater.py | Remove or move to tools/ if still needed |

## Rationale

1. Async architecture supersedes thread / process pool design.
2. Reduced maintenance surface & security risk.
3. Avoid confusion for new contributors.

## Next Steps

- After confirming no downstream references, delete the files listed.
- Update README to link to this document if historical context is needed.
- Add minimal unit tests covering snippet harvesting and sitemap-first flow.

If you still rely on anything here, copy it out before the removal PR.
