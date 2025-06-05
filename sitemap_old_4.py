"""
Robust sitemap discovery and parsing (rev‑7, parallel nested sitemap support with strict max_urls_per_sitemap enforcement)
===========================================

This version:
* Parallelizes fetching of nested sitemaps in `parse_sitemap` using a thread pool.
* Honors `config.max_urls_per_sitemap` as a per-sitemap and overall URL limit when parsing.
* Flags discovery properly for both standard and robots.txt sitemaps.
"""
import gzip
import logging
import time
from typing import Generator, List, Tuple, Set, Optional
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed

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

    def __init__(self) -> None:
        self._processed_sitemaps: Set[str] = set()
        self._sitemap_cache: dict[str, bytes] = {}

    def discover_sitemaps(self, domain: str) -> Generator[str, None, None]:
        naked = domain.removeprefix("www.")
        parts = naked.split('.')
        hosts = {naked} if len(parts) > 2 else {naked, f"www.{naked}"}
        found = False
        start = time.time()

        # standard sitemap filenames
        for host in hosts:
            for fname in config.sitemap_filenames:
                url = f"https://{host}/{fname}"
                canon = canonicalise(url)
                if canon in self._processed_sitemaps or not validate_url(url):
                    continue
                head = http_client.safe_get(url, method="HEAD", retry_count=2)
                if not head or 'xml' not in head.headers.get('Content-Type','').lower():
                    continue
                resp = http_client.safe_get(url, retry_count=2)
                if not resp:
                    continue
                size = len(resp.content)
                if size == 0 or size > self.max_sitemap_size:
                    continue
                # mark found and cache
                found = True
                self._processed_sitemaps.add(canon)
                self._sitemap_cache[canon] = resp.content
                elapsed = time.time() - start
                log.info("Found sitemap via standard filenames: %s (%.2fs)", url, elapsed)
                yield url
                return

        # robots.txt fallback if nothing found by standard filenames
        if not found:
            robots_url = f"https://{naked}/robots.txt"
            rob = http_client.safe_get(robots_url, retry_count=2)
            if rob:
                for line in rob.text.splitlines():
                    if not line.lower().startswith('sitemap:'):
                        continue
                    raw = line.split(':',1)[1].strip()
                    sm_url = join_url(naked, raw)
                    canon = canonicalise(sm_url)
                    if canon in self._processed_sitemaps or not validate_url(sm_url):
                        continue
                    host = urlparse(sm_url).netloc.removeprefix('www.').lower()
                    if host != naked:
                        continue
                    # mark found and cache
                    found = True
                    self._processed_sitemaps.add(canon)
                    elapsed = time.time() - start
                    log.info("Found sitemap via robots.txt: %s (%.2fs)", sm_url, elapsed)
                    yield sm_url

        # log if nothing at all
        if not found:
            total = time.time() - start
            log.debug("No sitemap found for %s after %.2fs", domain, total)

    def parse_sitemap(
        self,
        xml: bytes,
        remaining: Optional[int] = None,
    ) -> Generator[str, None, None]:
        # default remaining to per-sitemap limit
        if remaining is None:
            remaining = config.max_urls_per_sitemap
        # guard against empty or oversized sitemaps
        if not xml or len(xml) > self.max_sitemap_size:
            return

        # decompress if gzip
        if xml.startswith(b"\x1f\x8b"):
            try:
                xml = gzip.decompress(xml)
            except Exception as exc:
                raise SitemapError(f"gzip decode failed: {exc}") from exc

        # parse XML
        try:
            soup = BeautifulSoup(xml, 'xml')
        except Exception as exc:
            raise SitemapError(f"XML parse failed: {exc}") from exc

        count = 0
        # nested sitemapindex
        idx = soup.find('sitemapindex')
        if idx:
            nested_urls = [sm.find('loc').get_text(strip=True)
                           for sm in idx.find_all('sitemap')
                           if sm.find('loc') and validate_url(sm.find('loc').get_text(strip=True))]
            if not nested_urls:
                return
            threads = min(len(nested_urls), 4)
            log.debug("Parallel-fetching %d nested sitemaps via %d threads", len(nested_urls), threads)
            with ThreadPoolExecutor(max_workers=threads) as pool:
                futures = {pool.submit(http_client.safe_get, url, retry_count=2): url for url in nested_urls}
                for fut in as_completed(futures):
                    url = futures[fut]
                    try:
                        resp = fut.result()
                        if not resp or 'xml' not in resp.headers.get('Content-Type','').lower():
                            continue
                        # cascade remaining
                        for u in self.parse_sitemap(resp.content, remaining - count):
                            yield u
                            count += 1
                            if count >= remaining:
                                return
                    except Exception as err:
                        log.warning("Error parsing nested sitemap %s – %s", url, err)
            return

        # flat <loc> lists with limit
        for loc in soup.find_all('loc'):
            u = loc.get_text(strip=True)
            if not validate_url(u):
                continue
            yield u
            count += 1
            if count >= remaining:
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
                self._sitemap_cache[canon] = content

            try:
                for u in self.parse_sitemap(content, config.max_urls_per_sitemap):
                    if len(priority) >= config.max_fallback_pages:
                        break
                    if any(p in u.lower() for p in config.priority_parts) and u not in dedup:
                        priority.append(u)
                        dedup.add(u)
            except SitemapError as err:
                log.warning("Error parsing %s – %s", sm_url, err)

            if len(priority) >= config.max_fallback_pages:
                break

        elapsed = time.time() - start
        log.debug("Priority URL extraction for %s finished in %.2f s – %d URLs", domain, elapsed, len(priority))
        return priority, used

    def clear_cache(self) -> None:
        self._processed_sitemaps.clear()
        self._sitemap_cache.clear()

sitemap_parser = SitemapParser()