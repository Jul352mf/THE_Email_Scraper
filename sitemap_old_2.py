"""
Robust sitemap discovery and parsing (rev‑4, patched)
===========================================

Efficiency upgrade
------------------
* The **GET** performed in `discover_sitemaps()` is now **cached** so
  `get_priority_urls()` never has to fetch the same sitemap again.
* Logging of discovery duration only when no standard sitemap is found.
* Robots.txt fallback now deduplicates by canonical URL and respects domain.
* `parse_sitemap()` wraps gzip and XML parsing in exception guards to avoid
  bubbling errors from malformed sitemaps.

What changed
~~~~~~~~~~~~
* Early `return` on first valid sitemap remains.
* Canonicalization and duplicate-check in robots fallback.
* Conditional debug logging of discovery duration.
* Exception safety in `parse_sitemap()`.
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


def join_url(base: str, path: str) -> str:
    if path.startswith(("http://", "https://")):
        return path
    if not base.startswith(("http://", "https://")):
        base = f"https://{base}"
    return urljoin(base, path.lstrip("/"))


class SitemapError(Exception):
    pass


class SitemapParser:
    max_sitemap_size = 50 * 1024 * 1024  # 50 MB
    max_urls_per_sitemap = 10_000

    def __init__(self) -> None:
        self._processed_sitemaps: Set[str] = set()
        self._sitemap_cache: dict[str, bytes] = {}

    def discover_sitemaps(self, domain: str) -> Generator[str, None, None]:
        if not domain:
            log.warning("No domain provided to discover_sitemaps()")
            return

        naked = domain.removeprefix("www.")
        parts = naked.split(".")
        hosts = {naked} if len(parts) > 2 else {naked, f"www.{naked}"}

        found = False
        start = time.time()

        # Try standard sitemap filenames
        for host in hosts:
            for fname in config.sitemap_filenames:
                url = f"https://{host}/{fname}"
                canon = canonicalise(url)
                if canon in self._processed_sitemaps or not validate_url(url):
                    continue

                head = http_client.safe_get(url, method="HEAD", retry_count=2)
                if not head or 'xml' not in head.headers.get("Content-Type", "").lower():
                    continue

                body_resp = http_client.safe_get(url, retry_count=2)
                if not body_resp:
                    continue
                size = len(body_resp.content)
                if size == 0 or size > self.max_sitemap_size:
                    continue

                # Cache and yield first valid sitemap
                self._processed_sitemaps.add(canon)
                self._sitemap_cache[canon] = body_resp.content
                found = True
                elapsed = time.time() - start
                log.info("Found sitemap via standard filenames: %s (%.2fs)", url, elapsed)
                yield url
                return  # stop after first successful sitemap

        # robots.txt fallback if no standard sitemap found
        if not found:
            robots_url = f"https://{naked}/robots.txt"
            rob = http_client.safe_get(robots_url, retry_count=2)
            if rob:
                for line in rob.text.splitlines():
                    if not line.lower().startswith("sitemap:"):
                        continue
                    raw = line.split(":", 1)[1].strip()
                    sm_url = join_url(naked, raw)
                    if not validate_url(sm_url):
                        continue
                    canon = canonicalise(sm_url)
                    if canon in self._processed_sitemaps:
                        continue
                    # ensure sitemap is on same domain
                    from urllib.parse import urlparse
                    host = urlparse(sm_url).netloc.removeprefix("www.").lower()
                    if host != naked:
                        continue

                    self._processed_sitemaps.add(canon)
                    elapsed = time.time() - start
                    log.info("Found sitemap via robots.txt: %s (%.2fs)", sm_url, elapsed)
                    yield sm_url

        # Always log total discovery time if nothing was yielded
        if not found:
            total = time.time() - start
            log.debug("No sitemap found for %s after %.2fs", domain, total)

    def parse_sitemap(
        self,
        xml: bytes,
        remaining: Optional[int] = None,
    ) -> Generator[str, None, None]:
        # guard against empty or oversized sitemaps
        if not xml or len(xml) > self.max_sitemap_size:
            return

        # handle gzip-compressed sitemaps safely
        if xml.startswith(b"\x1f\x8b"):
            try:
                xml = gzip.decompress(xml)
            except Exception as exc:
                raise SitemapError(f"gzip decode failed: {exc}") from exc

        # parse XML safely
        try:
            soup = BeautifulSoup(xml, "xml")
        except Exception as exc:
            raise SitemapError(f"XML parse failed: {exc}") from exc

        # nested sitemapindex handling
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
                if not resp:
                    continue
                yield from self.parse_sitemap(resp.content, remaining)
            return

        # flat <url><loc> lists
        count = 0
        for loc in soup.find_all("loc"):
            url = loc.get_text(strip=True)
            if not validate_url(url):
                continue
            yield url
            count += 1
            if remaining is not None and count >= remaining:
                break

    def get_priority_urls(self, domain: str) -> Tuple[List[str], bool]:
        start = time.time()
        priority: List[str] = []
        used = False
        dedup: Set[str] = set()

        for sm_url in self.discover_sitemaps(domain):
            used = True
            canon = canonicalise(sm_url)
            content = self._sitemap_cache.get(canon)
            if content is None:
                resp = http_client.safe_get(sm_url, retry_count=2)
                if not resp:
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

        log.debug(
            "Priority URL extraction for %s finished in %.2f s – %d URLs",
            domain,
            time.time() - start,
            len(priority),
        )
        return priority, used

    def clear_cache(self) -> None:
        self._processed_sitemaps.clear()
        self._sitemap_cache.clear()


sitemap_parser = SitemapParser()
