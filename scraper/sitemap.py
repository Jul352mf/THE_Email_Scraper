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


def _looks_like_xml(content: bytes) -> bool:
    """
    Heuristic check: does the content start with XML declaration or contain key sitemap tags?
    """
    head = content.lstrip()[:200].lower()
    return bool(
        head.startswith(b"<?xml") or
        b"<urlset" in head or
        b"<sitemapindex" in head
    )


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
                if not head:
                    continue

                resp = http_client.safe_get(url, retry_count=2)
                if not resp:
                    continue
                if len(resp.content) == 0 or len(resp.content) > self.max_sitemap_size:
                    continue
                if not _looks_like_xml(resp.content):
                    continue

                found = True
                self._processed_sitemaps.add(canon)
                self._sitemap_cache[canon] = resp.content
                elapsed = time.time() - start
                log.info("Found sitemap via standard filenames: %s (%.2fs)", url, elapsed)
                yield url
                return

        # robots.txt fallback
        if not found:
            robots_url = f"https://{naked}/robots.txt"
            rob = http_client.safe_get(robots_url, retry_count=2)
            if rob:
                for line in rob.text.splitlines():
                    if not line.lower().startswith('sitemap:'):
                        continue
                    raw = line.split(':', 1)[1].strip()
                    sm_url = join_url(naked, raw)
                    canon = canonicalise(sm_url)
                    if canon in self._processed_sitemaps or not validate_url(sm_url):
                        continue
                    host_part = urlparse(sm_url).netloc.removeprefix('www.').lower()
                    if host_part != naked:
                        continue

                    resp = http_client.safe_get(sm_url, retry_count=2)
                    if not resp or len(resp.content) == 0 or len(resp.content) > self.max_sitemap_size:
                        continue
                    if not _looks_like_xml(resp.content):
                        continue

                    found = True
                    self._processed_sitemaps.add(canon)
                    self._sitemap_cache[canon] = resp.content
                    elapsed = time.time() - start
                    log.info("Found sitemap via robots.txt: %s (%.2fs)", sm_url, elapsed)
                    yield sm_url

        if not found:
            total = time.time() - start
            log.debug("No sitemap found for %s after %.2fs", domain, total)

    def parse_sitemap(
        self,
        xml: bytes,
        remaining: Optional[int] = None,
    ) -> Generator[str, None, None]:
        if remaining is None:
            remaining = config.max_urls_per_sitemap
        if not xml or len(xml) > self.max_sitemap_size:
            return

        if xml.startswith(b"\x1f\x8b"):
            try:
                xml = gzip.decompress(xml)
            except Exception as exc:
                raise SitemapError(f"gzip decode failed: {exc}") from exc

        if not _looks_like_xml(xml):
            raise SitemapError("Content does not appear to be valid XML sitemap")

        try:
            soup = BeautifulSoup(xml, 'xml')
        except Exception as exc:
            raise SitemapError(f"XML parse failed: {exc}") from exc

        count = 0
        idx = soup.find('sitemapindex')
        if idx:
            nested_urls = []
            seen = set()
            for sm in idx.find_all('sitemap'):
                loc = sm.find('loc')
                if loc:
                    url = loc.get_text(strip=True)
                    if validate_url(url) and url not in seen:
                        seen.add(url)
                        nested_urls.append(url)
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
                        if not resp or not _looks_like_xml(resp.content):
                            continue
                        for u in self.parse_sitemap(resp.content, remaining - count):
                            yield u
                            count += 1
                            if count >= remaining:
                                return
                    except Exception as err:
                        log.warning("Error parsing nested sitemap %s – %s", url, err)
            return

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

        # collect all sitemap URLs first
        sitemap_urls = list(self.discover_sitemaps(domain))
        used = bool(sitemap_urls)

        def _process_sitemap(sm_url: str) -> List[str]:
            urls: List[str] = []
            canon = canonicalise(sm_url)
            content = self._sitemap_cache.get(canon)
            if content is None:
                resp = http_client.safe_get(sm_url, retry_count=2)
                if not resp:
                    return urls
                content = resp.content
                self._sitemap_cache[canon] = content

            try:
                for u in self.parse_sitemap(content, config.max_urls_per_sitemap):
                    if any(p in u.lower() for p in config.priority_parts) and u not in dedup:
                        urls.append(u)
            except SitemapError as err:
                log.warning("Error parsing %s – %s", sm_url, err)
            return urls

        # parallel-fetch and parse
        with ThreadPoolExecutor(max_workers=min(len(sitemap_urls), 4 or 1)) as pool:
            futures = {pool.submit(_process_sitemap, url): url for url in sitemap_urls}
            for fut in as_completed(futures):
                for u in fut.result():
                    if len(priority) >= config.max_fallback_pages:
                        break
                    priority.append(u)
                    dedup.add(u)
                if len(priority) >= config.max_fallback_pages:
                    break

        elapsed = time.time() - start
        log.debug("Priority URL extraction for %s finished in %.2f s – %d URLs", domain, elapsed, len(priority))
        return priority, used

    def get_all_urls(self, domain: str) -> Tuple[List[str], bool]:
        """Return all URLs found in the domain's sitemaps."""
        all_urls: List[str] = []
        dedup: Set[str] = set()

        sitemap_urls = list(self.discover_sitemaps(domain))
        used = bool(sitemap_urls)
        for sm_url in sitemap_urls:
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
                    if u not in dedup:
                        all_urls.append(u)
                        dedup.add(u)
            except SitemapError as err:
                log.warning("Error parsing %s – %s", sm_url, err)

        return all_urls, used

    def clear_cache(self) -> None:
        self._processed_sitemaps.clear()
        self._sitemap_cache.clear()

sitemap_parser = SitemapParser()
