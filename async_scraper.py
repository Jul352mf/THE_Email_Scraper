import asyncio
import logging
import uuid
import base64
import codecs
import re
from typing import Set, Optional
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, Page, TimeoutError as PWTimeout
import aiohttp

from scraper.email_extractor import EmailExtractor, EmailValidationError
from scraper.http import http_client

log = logging.getLogger(__name__)

class AsyncBrowserPool:
    def __init__(self, concurrency: int = 4, render_timeout: float = 60.0, idle_timeout: float = 15.0):
        self._sem = asyncio.Semaphore(concurrency)
        self._playwright = None
        self._browser = None
        self.render_timeout = render_timeout
        self.idle_timeout = idle_timeout

    async def start(self):
        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(headless=True)
        log.info("AsyncBrowserPool started with concurrency=%d", self._sem._value)

    async def stop(self):
        await self._browser.close()
        await self._playwright.stop()
        log.info("AsyncBrowserPool stopped")

    async def render(self, url: str) -> str:
        async with self._sem:
            page: Page = await self._browser.new_page()
            try:
                await page.goto(url, wait_until="networkidle", timeout=int(self.render_timeout * 1000))
                await page.wait_for_load_state("networkidle", timeout=int(self.idle_timeout * 1000))
                content = await page.content()
                return content
            except PWTimeout:
                log.warning("JS render timeout for %s", url)
                return ""
            except Exception:
                log.error("JS render error for %s", url, exc_info=True)
                return ""
            finally:
                await page.close()

class AsyncEmailExtractor:
    def __init__(self, browser_pool, use_js_fallback: bool = True):
        self.browser_pool    = browser_pool
        self.use_js          = use_js_fallback
        self._seen           = set()
        # ← instantiate once:
        self.static_extractor = EmailExtractor()

    async def extract_from_url(self, url: str) -> Set[str]:
        if url in self._seen:
            return set()
        self._seen.add(url)

        # 1) fetch via your blocking client, in a thread
        text = await asyncio.to_thread(
            http_client.safe_get,
            url,
            retry_count=2,
            timeout=(10, 60)
        )
        if not text or 'html' not in text.headers.get('Content-Type', ''):
            return set()

        html = text.text

        # 2) static pass
        hits = self.static_extractor.extract_from_text(html, url)
        log.info("Static pass found %d on %s", len(hits), url)
        if hits or not self.use_js:
            return hits

        # 3) JS fallback
        rendered = await self.browser_pool.render(url)
        # feed it back through the *same* static pipelines:
        hits = self.static_extractor.extract_from_text(rendered, url)
        if not hits:
            hits = self.static_extractor.extract_from_html(rendered, url)
        return hits

async def main(urls, out_path: str):
    logging.basicConfig(level=logging.INFO)
    browser_pool = AsyncBrowserPool(concurrency=4)
    await browser_pool.start()

    extractor = AsyncEmailExtractor(browser_pool)
    sem = asyncio.Semaphore(16)

    async def bound_extract(u):
        async with sem:
            hits = await extractor.extract_from_url(u)
            return u, hits

    tasks = [asyncio.create_task(bound_extract(u)) for u in urls]
    results = await asyncio.gather(*tasks)

    await browser_pool.stop()

    # write to file
    import csv
    with open(out_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['url', 'emails'])
        for url, emails in results:
            writer.writerow([url, ';'.join(emails)])

if __name__ == '__main__':
    import sys
    urls = []
    with open(sys.argv[1]) as f:
        for line in f:
            urls.append(line.strip())
    asyncio.run(main(urls, sys.argv[2]))
