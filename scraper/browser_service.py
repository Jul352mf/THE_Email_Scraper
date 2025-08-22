"""BrowserService (refactored)

Thread-based, lazy, optional Playwright integration with fast fallback.

Goals:
* Avoid importing Playwright at module import time (graceful if missing).
* Eliminate multiprocessing.Manager usage (Windows pickling overhead / hangs).
* Provide the same external helper ``get_browser_service()`` returning a
    singleton with ``render(url, timeout=None) -> str`` and ``shutdown()``.
* If Playwright isn't installed or startup fails, service enters a disabled
    mode where ``render`` returns "" quickly (caller can detect empty HTML).
"""

from __future__ import annotations

import logging
import threading
import asyncio
from queue import Queue, Empty
from typing import Optional

log = logging.getLogger(__name__)

_browser_service = None  # singleton placeholder
_LOCK = threading.Lock()


class BrowserService:
    """Thread-backed renderer using Playwright (chromium) if available.

    Not a ``multiprocessing.Process`` anymore; simpler, more portable.
    The heavy lifting (Chromium) still runs out-of-process via Playwright.
    """

    def __init__(
        self,
        render_timeout: float = 30.0,
        idle_timeout: float = 5.0,
        ignore_https_errors: bool = True,
        headless: bool = True,
        max_queue: int = 100,
    ) -> None:
        self.render_timeout = render_timeout
        self.idle_timeout = idle_timeout
        self.ignore_https_errors = ignore_https_errors
        self.headless = headless
        self._requests: "Queue[tuple[str, str, float, float, Queue]]" = Queue(
            max_queue
        )
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._ready = threading.Event()
        self._disabled = False
        self._disable_reason = ""

    # ------------------------------ lifecycle ------------------------------
    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(
            target=self._worker,
            name="BrowserServiceWorker",
            daemon=True,
        )
        self._thread.start()
    # Wait a short time for readiness (Playwright init) but don't block
    # forever.
        self._ready.wait(timeout=5.0)
        if not self._ready.is_set():
            log.warning(
                "BrowserService not ready after 5s; best-effort mode"
            )

    def shutdown(self) -> None:
        self._stop.set()
        if self._thread and self._thread.is_alive():
            try:
                self._requests.put_nowait(("__STOP__", "", 0, 0, Queue()))
            except Exception:
                pass
            self._thread.join(timeout=5)
        self._thread = None
        log.info("BrowserService shutdown complete")

    # ------------------------------ public API -----------------------------
    def render(self, url: str, timeout: Optional[float] = None) -> str:
        if self._disabled:
            return ""
        if not self._thread or not self._thread.is_alive():
            self.start()
        if self._disabled:
            return ""
        total_timeout = (
            timeout
            if timeout is not None
            else (self.render_timeout + self.idle_timeout)
        )
        per_nav = int(self.render_timeout * 1000)
        per_idle = int(self.idle_timeout * 1000)
        resp_q: Queue = Queue(1)
        try:
            self._requests.put(
                (url, url, per_nav, per_idle, resp_q), timeout=0.1
            )
        except Exception:
            log.warning(
                "BrowserService queue full/closed; returning empty for %s", url
            )
            return ""
        try:
            return resp_q.get(timeout=total_timeout)
        except Empty:
            log.warning("BrowserService render timeout for %s", url)
            return ""

    async def render_async(
        self, url: str, timeout: Optional[float] = None
    ) -> str:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self.render, url, timeout)

    def is_disabled(self) -> bool:
        return self._disabled

    def disable_reason(self) -> str:
        return self._disable_reason

    # ------------------------------ worker loop ----------------------------
    def _worker(self) -> None:
        try:
            try:
                from playwright.sync_api import (
                    sync_playwright,
                    TimeoutError as PWTimeout,
                )  # type: ignore
            except ImportError as e:  # Playwright not installed
                self._disabled = True
                self._disable_reason = f"playwright import failed: {e}"
                log.info("BrowserService disabled (%s)", self._disable_reason)
                self._ready.set()
                return

            pw = sync_playwright().start()
            browser = pw.chromium.launch(headless=self.headless)
            context = browser.new_context(
                ignore_https_errors=self.ignore_https_errors
            )
            log.info("BrowserService started (thread mode)")
            self._ready.set()

            while not self._stop.is_set():
                try:
                    key, url, nav_ms, idle_ms, resp_q = self._requests.get(
                        timeout=0.5
                    )
                except Empty:
                    continue
                if key == "__STOP__":
                    break
                page = context.new_page()
                html = ""
                try:
                    try:
                        page.goto(
                            url, wait_until="networkidle", timeout=nav_ms
                        )
                    except PWTimeout:
                        log.debug("nav timeout after %dms for %s", nav_ms, url)
                        try:
                            page.goto(
                                url,
                                wait_until="domcontentloaded",
                                timeout=max(1000, nav_ms // 2),
                            )
                        except Exception:
                            pass
                    else:
                        try:
                            page.wait_for_load_state(
                                "networkidle", timeout=idle_ms
                            )
                        except PWTimeout:
                            pass
                    try:
                        html = page.content() or ""
                    except Exception as e:
                        log.debug("page.content() failed for %s: %s", url, e)
                        html = ""
                    if not html.strip():
                        try:
                            html = page.evaluate(
                                "() => document.documentElement.outerHTML"
                            ) or ""
                        except Exception:
                            pass
                    if not html.strip():
                        log.info(
                            "Empty HTML for %s (timeout/nav issues)",
                            url,
                        )
                except Exception as e:
                    log.debug("Render error for %s: %s", url, e)
                finally:
                    try:
                        resp_q.put_nowait(html)
                    except Exception:
                        pass
                    page.close()
        except Exception as e:
            self._disabled = True
            self._disable_reason = f"startup failure: {e}"
            log.warning(
                "BrowserService disabled due to error: %s", e, exc_info=True
            )
            self._ready.set()
        finally:
            try:
                if 'context' in locals():
                    context.close()
                if 'browser' in locals():
                    browser.close()
                if 'pw' in locals():
                    pw.stop()
            except Exception:
                pass


def get_browser_service() -> 'BrowserService':
    global _browser_service
    with _LOCK:
        if _browser_service is None:
            _browser_service = BrowserService()
    return _browser_service
