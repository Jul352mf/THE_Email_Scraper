"""
Robust sitemap discovery and parsing (rev‑4)
===========================================

Efficiency upgrade
------------------
* The **GET** performed in `discover_sitemaps()` is now **cached** so
  `get_priority_urls()` never has to fetch the same sitemap again. No API
  change – `discover_sitemaps()` still yields just the URL, but the body is
  stored in `_sitemap_cache` keyed by canonical URL.

What changed
~~~~~~~~~~~~
* `self._sitemap_cache: dict[str, bytes]` added.
* `discover_sitemaps()` saves the body in the cache.
* `get_priority_urls()` checks the cache before deciding whether to call
  `safe_get()`.
* Cache cleared together with `_processed_sitemaps` via `clear_cache()`.
"""

from __future__ import annotations

import gzip
import logging
import time
from typing import Generator, List, Tuple, Set, Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from scraper.config import config
from scraper.http import http_client, canonicalise, validate_url

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def join_url(base: str, path: str) -> str:
    if path.startswith(("http://", "https://")):
        return path
    if not base.startswith(("http://", "https://")):
        base = f"https://{base}"
    return urljoin(base, path.lstrip("/"))


class SitemapError(Exception):
    pass


class SitemapParser:
    max_sitemap_size = 50 * 1024 * 1024  # 50 MB
    max_urls_per_sitemap = 10_000

    def __init__(self) -> None:
        self._processed_sitemaps: Set[str] = set()
        self._sitemap_cache: dict[str, bytes] = {}

    # ------------------------------------------------------------------
    # Discovery (HEAD + GET → cache)
    # ------------------------------------------------------------------

    def discover_sitemaps(self, domain: str) -> Generator[str, None, None]:
        if not domain:
            return

        naked = domain.removeprefix("www.")
        parts = naked.split(".")
        # if already has a sub-domain (3+ labels), don’t manufacture “www.”
        if len(parts) > 2:
            hosts = {naked}
        else:
            hosts = {naked, f"www.{naked}"}
            
        found = False
        start = time.time()

        for host in hosts:
            for fname in config.sitemap_filenames:
                url = f"https://{host}/{fname}"
                canon = canonicalise(url)
                if canon in self._processed_sitemaps or not validate_url(url):
                    continue

                # Cheap HEAD probe; does NOT mark as visited
                head = http_client.safe_get(url, method="HEAD", retry_count=2)
                if head is None:
                    continue
                ctype = head.headers.get("Content-Type", "").lower()
                if not any(t in ctype for t in ("xml", "gzip", "text")):
                    continue

                # Full GET (once) – body cached for later use
                body_resp = http_client.safe_get(url, retry_count=2)
                if not body_resp or not body_resp.ok:
                    continue
                size = len(body_resp.content)
                if size == 0 or size > self.max_sitemap_size:
                    continue

                self._processed_sitemaps.add(canon)
                self._sitemap_cache[canon] = body_resp.content  # ← cache body
                found = True
                yield url
                return  # stop after the first successful sitemap

        # robots.txt fallback
        if not found:
            robots_url = f"https://{naked}/robots.txt"
            rob = http_client.safe_get(robots_url, retry_count=2)
            if rob:
                for line in rob.text.splitlines():
                    if line.lower().startswith("sitemap:"):
                        sm_url = join_url(naked, line.split(":", 1)[1].strip())
                        if validate_url(sm_url):
                            canon = canonicalise(sm_url)
                        if not validate_url(sm_url):
                            continue
                        # only yield sitemaps on our target domain
                        from urllib.parse import urlparse
                        host = urlparse(sm_url).netloc.lower()
                        # strip any leading “www.”
                        host = host.removeprefix("www.")
                        if host != naked:
                            continue
                        yield sm_url


        log.debug("Sitemap discovery for %s took %.2f s", domain, time.time() - start)

    # ------------------------------------------------------------------
    # Parsing
    # ------------------------------------------------------------------

    def parse_sitemap(self, xml: bytes, remaining: Optional[int] = None) -> Generator[str, None, None]:
        if not xml or len(xml) > self.max_sitemap_size:
            return
        if xml.startswith(b"\x1f\x8b"):
            try:
                xml = gzip.decompress(xml)
            except Exception as exc:
                raise SitemapError(f"gzip decode failed: {exc}") from exc

        soup = BeautifulSoup(xml, "xml")

        idx = soup.find("sitemapindex")
        if idx:
            for sm in idx.find_all("sitemap"):
                loc = sm.find("loc")
                if not loc:
                    continue
                url = loc.get_text(strip=True)
                if not validate_url(url):
                    continue
                canon = canonicalise(url)
                if canon in self._processed_sitemaps:
                    continue
                self._processed_sitemaps.add(canon)
                resp = http_client.safe_get(url, retry_count=2)
                if resp and resp.ok:
                    yield from self.parse_sitemap(resp.content, remaining)
            return

        count = 0
        for loc in soup.find_all("loc"):
            url = loc.get_text(strip=True)
            if not validate_url(url):
                continue
            yield url
            count += 1
            if remaining is not None and count >= remaining:
                break

    # ------------------------------------------------------------------
    # High‑level helper
    # ------------------------------------------------------------------

    def get_priority_urls(self, domain: str) -> Tuple[List[str], bool]:
        start = time.time()
        priority: list[str] = []
        used = False
        dedup: set[str] = set()

        for sm_url in self.discover_sitemaps(domain):
            used = True
            canon = canonicalise(sm_url)
            content = self._sitemap_cache.get(canon)
            if content is None:
                resp = http_client.safe_get(sm_url, retry_count=2)
                if not resp or not resp.ok:
                    continue
                content = resp.content
            try:
                for u in self.parse_sitemap(content):
                    if any(p in u.lower() for p in config.priority_parts):
                        if u not in dedup:
                            priority.append(u)
                            dedup.add(u)
                    if len(priority) >= config.max_fallback_pages:
                        break
            except SitemapError as err:
                log.warning("Error parsing %s – %s", sm_url, err)
            if len(priority) >= config.max_fallback_pages:
                break

        log.debug("Priority URL extraction for %s finished in %.2f s – %d URLs",
                  domain, time.time() - start, len(priority))
        return priority, used

    # ------------------------------------------------------------------

    def clear_cache(self) -> None:
        self._processed_sitemaps.clear()
        self._sitemap_cache.clear()


sitemap_parser = SitemapParser()
