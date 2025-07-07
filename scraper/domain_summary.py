import logging
from collections import deque
from typing import List, Dict, Any, Tuple
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from scraper.http import http_client, canonicalise, validate_url
from scraper.sitemap import sitemap_parser
from scraper.browser_service import get_browser_service

log = logging.getLogger(__name__)

def _fetch_html(url: str) -> str:
    """Fetch page HTML with static request and JS fallback."""
    resp = http_client.safe_get(url, retry_count=2)
    if resp and 'html' in resp.headers.get('Content-Type', '').lower():
        return resp.text
    # fallback to JS rendering
    service = get_browser_service()
    try:
        return service.render(url)
    except Exception as exc:
        log.warning("JS render failed for %s: %s", url, exc)
        return ""

def _extract_info(html: str, url: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    title = soup.title.get_text(strip=True) if soup.title else ""
    meta_desc = ""
    meta_keys = ""
    for m in soup.find_all("meta"):
        name = (m.get("name") or "").lower()
        prop = (m.get("property") or "").lower()
        if name == "description" or prop == "og:description":
            meta_desc = m.get("content", "")
        elif name == "keywords":
            meta_keys = m.get("content", "")
    text = soup.get_text(" ", strip=True)
    text = text[:1000]
    return {
        "url": url,
        "title": title,
        "meta_description": meta_desc,
        "meta_keywords": meta_keys,
        "text": text,
    }

def _links_from(html: str, base: str, domain: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    links: List[str] = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href.lower().startswith("mailto:"):
            continue
        full = urljoin(base, href)
        if not validate_url(full):
            continue
        if domain not in canonicalise(full):
            continue
        links.append(full)
    return links

def summarize_domain(domain: str, max_pages: int = 100) -> Dict[str, Any]:
    """Return summary info for a domain."""
    pages: List[Dict[str, Any]] = []
    seen = set()

    # gather URLs from sitemap first
    urls, used_sitemap = sitemap_parser.get_all_urls(domain)
    if not urls:
        urls = [f"https://{domain}"]
    q = deque(urls[:max_pages])

    while q and len(pages) < max_pages:
        url = q.popleft()
        canon = canonicalise(url)
        if canon in seen:
            continue
        seen.add(canon)
        html = _fetch_html(url)
        if not html:
            continue
        info = _extract_info(html, url)
        pages.append(info)
        for link in _links_from(html, url, domain):
            if len(pages) + len(q) >= max_pages:
                break
            canon_link = canonicalise(link)
            if canon_link not in seen:
                q.append(link)
    return {
        "domain": domain,
        "page_count": len(pages),
        "used_sitemap": used_sitemap,
        "pages": pages,
    }
