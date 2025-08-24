"""Async Browser Service

Non-blocking (asyncio) Playwright integration used by the new async
pipeline. This lives alongside the existing thread based synchronous
`browser_service.BrowserService` so we don't disrupt legacy code.  The
two services are intentionally decoupled – callers pick one style.

Design goals:
 - Lazy import / startup of Playwright (avoid cost if never used)
 - Single Chromium context reused for all pages (performance)
 - Per-call page lifecycle (avoid memory leaks)
 - Cooperative cancellation / timeouts using asyncio
 - Graceful disable/fallback if Playwright missing or startup fails

Public API:
 - `get_async_browser_service()` -> singleton `AsyncBrowserService`
 - `await service.render(url: str, timeout: float | None = None) -> str`
 - `await service.shutdown()` to close resources explicitly (optional
    – atexit handler tries best‑effort cleanup)

If the service is disabled (Playwright not installed / startup error)
`render` returns an empty string quickly so upstream logic can fallback.
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Optional

log = logging.getLogger(__name__)

_async_browser_service: 'AsyncBrowserService | None' = None
_SERVICE_LOCK = asyncio.Lock()


class AsyncBrowserService:
    """Async Playwright wrapper.

    We use `async_playwright` directly (no executor) so calls do not
    block the event loop except during awaited I/O.
    """

    def __init__(
        self,
        *,
        nav_timeout: float = 30.0,
        idle_timeout: float = 5.0,
        headless: bool = True,
        ignore_https_errors: bool = True,
        max_concurrent: int = 8,
    ) -> None:
        self.nav_timeout = nav_timeout
        self.idle_timeout = idle_timeout
        self.headless = headless
        self.ignore_https_errors = ignore_https_errors
        self._playwright = None  # type: ignore
        self._browser = None
        self._context = None
        self._started = False
        self._disabled = False
        self._disable_reason = ""
        self._sem = asyncio.Semaphore(max_concurrent)
        self._start_lock = asyncio.Lock()

    # ------------------------------------------------------------------
    async def start(self) -> None:
        if self._started or self._disabled:
            return
        async with self._start_lock:
            if self._started or self._disabled:
                return
            try:
                from playwright.async_api import (
                    async_playwright,  # type: ignore
                )
            except Exception as e:  # ImportError or other
                self._disabled = True
                self._disable_reason = (
                    f"playwright import failed: {e}"
                )
                log.info(
                    "AsyncBrowserService disabled (%s)",
                    self._disable_reason,
                )
                return
            try:
                self._playwright = await async_playwright().start()
                self._browser = await self._playwright.chromium.launch(
                    headless=self.headless
                )
                self._context = await self._browser.new_context(
                    ignore_https_errors=self.ignore_https_errors
                )
                self._started = True
                log.info(
                    "AsyncBrowserService started (headless=%s, concurrent=%d)",
                    self.headless,
                    self._sem._value,  # type: ignore[attr-defined]
                )
            except Exception as e:  # pragma: no cover - startup failures rare
                self._disabled = True
                self._disable_reason = f"startup failure: {e}"
                log.warning(
                    "AsyncBrowserService disabled: %s",
                    e,
                    exc_info=True,
                )

    # ------------------------------------------------------------------
    async def render(self, url: str, timeout: Optional[float] = None) -> str:
        """Render a URL and return HTML (empty string on failure/disabled)."""
        if self._disabled:
            return ""
        if not self._started:
            await self.start()
        if self._disabled or not self._context:
            return ""

        nav_timeout_ms = int((timeout or self.nav_timeout) * 1000)
        idle_timeout_ms = int(self.idle_timeout * 1000)

        async with self._sem:
            page = await self._context.new_page()
            html = ""
            try:
                from playwright.async_api import (
                    TimeoutError as PWTimeout,  # type: ignore
                )
                try:
                    await page.goto(
                        url,
                        wait_until="networkidle",
                        timeout=nav_timeout_ms,
                    )
                except PWTimeout:
                    log.debug(
                        "nav timeout after %dms for %s",
                        nav_timeout_ms,
                        url,
                    )
                    # Try weaker load state
                    try:
                        await page.goto(
                            url,
                            wait_until="domcontentloaded",
                            timeout=max(1000, nav_timeout_ms // 2),
                        )
                    except Exception:
                        pass
                else:
                    # Try extra idle wait
                    try:
                        await page.wait_for_load_state(
                            "networkidle", timeout=idle_timeout_ms
                        )
                    except PWTimeout:
                        pass
                try:
                    html = await page.content()
                except Exception as e:  # pragma: no cover
                    log.debug("page.content() failed for %s: %s", url, e)
                    html = ""
                if not html.strip():
                    try:
                        html = await page.evaluate(
                            "() => document.documentElement.outerHTML"
                        )
                    except Exception:
                        pass
                if not html.strip():
                    log.info("Empty HTML for %s", url)
            except Exception as e:
                log.debug("Render error for %s: %s", url, e)
            finally:
                try:
                    await page.close()
                except Exception:
                    pass
            return html or ""

    # ------------------------------------------------------------------
    async def shutdown(self) -> None:
        if self._disabled:
            return
        # Close context / browser / playwright in reverse order
        try:
            if self._context:
                await self._context.close()
        except Exception:
            pass
        try:
            if self._browser:
                await self._browser.close()
        except Exception:
            pass
        try:
            if self._playwright:
                await self._playwright.stop()
        except Exception:
            pass
        self._started = False
        log.info("AsyncBrowserService shutdown complete")

    # ------------------------------------------------------------------
    def is_disabled(self) -> bool:
        return self._disabled

    def disable_reason(self) -> str:
        return self._disable_reason


async def get_async_browser_service() -> AsyncBrowserService:
    global _async_browser_service
    if _async_browser_service is not None:
        return _async_browser_service
    async with _SERVICE_LOCK:
        if _async_browser_service is None:
            _async_browser_service = AsyncBrowserService()
    return _async_browser_service


@asynccontextmanager
async def browser_session():
    """Context manager yielding a started AsyncBrowserService."""
    svc = await get_async_browser_service()
    await svc.start()
    try:
        yield svc
    finally:
        # Caller can keep global instance alive; we *don't* auto shutdown
        # here to allow reuse. Explicit shutdown() if desired.
        pass


__all__ = [
    "AsyncBrowserService",
    "get_async_browser_service",
    "browser_session",
]
