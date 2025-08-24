"""Hybrid Email Extraction Utilities.

Provides a layered extraction strategy:
 1. Static HTML & text based extraction (fast)
 2. Obfuscation decoding (Cloudflare, JS char codes, ROT13, Base64)
 3. Optional JS rendering fallback via synchronous BrowserService
 4. Lightweight domain‑scoped discovery of common contact pages

Additionally exposes ``extract_emails_hybrid`` used by the async
orchestrator to perform a compact pass over a domain (base page + a few
discovery paths + optional JS) while caching results per domain to avoid
duplicate work.
"""

from __future__ import annotations

import base64
import codecs
import html as html_mod
import logging
import re
from functools import lru_cache
from typing import Optional, Set

from bs4 import BeautifulSoup
from requests import Response

from scraper.browser_service import get_browser_service
from scraper.email_extractor import EmailExtractor, EmailValidationError
from scraper.http_client import http_client

log = logging.getLogger(__name__)


class HybridEmailExtractor:
    def __init__(self, use_js_fallback: bool = True, cache_max: int = 5000):
        self.static_extractor = EmailExtractor()
        self.use_js = use_js_fallback
        self._seen_urls: Set[str] = set()
        # insertion-ordered dict used as simple LRU (pop oldest when full)
        self._domain_cache: dict[str, Set[str]] = {}
        self._cache_max = cache_max

    # ----------------------------- helpers -----------------------------
    def _decode_cfemail(self, cf: str) -> str:
        key = int(cf[:2], 16)
        return "".join(
            chr(int(cf[i:i+2], 16) ^ key) for i in range(2, len(cf), 2)
        )

    @lru_cache(maxsize=256)
    def _render_and_extract(self, url: str) -> Set[str]:
        log.info("JS fallback for %s", url)
        html = get_browser_service().render(url)
        return self._static_pass(html, url)

    # --------------------------- core passes ---------------------------
    def _static_pass(
        self, html_text: str, url: Optional[str] = None
    ) -> Set[str]:
        hits: Set[str] = set()
        soup = BeautifulSoup(html_text, "html.parser")

        # Cloudflare obfuscation
        for tag in soup.find_all(attrs={"data-cfemail": True}):
            try:
                raw = self._decode_cfemail(tag["data-cfemail"])
                cleaned = self.static_extractor.clean_email(raw)
                hits.add(cleaned)
            except EmailValidationError:
                pass
        if hits:
            log.debug("CF hits: %d on %s", len(hits), url)
            return hits

        # Aggregate candidate text
        text = soup.get_text(" ")
        text = html_mod.unescape(text)

        # JS char codes
        for js_seq in re.findall(r"fromCharCode\(([^)]+)\)", html_text):
            nums = [int(n) for n in js_seq.split(",") if n.strip().isdigit()]
            if nums:
                text += " " + "".join(chr(n) for n in nums)

        # ROT13 blocks
        for block in re.findall(r"[A-Za-z]{30,}", html_text):
            try:
                decoded = codecs.decode(block, "rot_13")
                text += " " + decoded
            except Exception:
                pass

        # Base64 blocks
        for b64 in re.findall(r"'([A-Za-z0-9+/=]{40,})'", html_text):
            try:
                decoded = base64.b64decode(b64).decode("utf-8", "ignore")
                text += " " + decoded
            except Exception:
                pass

        # Single text extraction pass
        hits.update(self.static_extractor.extract_from_text(text, url))
        if hits:
            log.debug("Static text hits: %d on %s", len(hits), url)
            return hits

        # HTML structural extraction (mailto / attributes)
        hits.update(self.static_extractor.extract_from_html(html_text, url))
        if hits:
            log.debug("HTML fallback hits: %d on %s", len(hits), url)
        return hits

    # ------------------------------ public -----------------------------
    def extract_from_url(
        self, url: str, *, use_js_fallback: Optional[bool] = None
    ) -> Set[str]:
        if url in self._seen_urls:
            log.debug("Skip duplicate %s", url)
            return set()
        self._seen_urls.add(url)

        resp = http_client.safe_get(url, retry_count=2, timeout=(10, 60))
        if not resp or "html" not in resp.headers.get("Content-Type", ""):
            return set()

        hits = self._static_pass(resp.text, url)
        log.info("Static pass found %d on %s", len(hits), url)
        if hits or not (
            self.use_js if use_js_fallback is None else use_js_fallback
        ):
            return hits

        try:
            return self._render_and_extract(url)
        except Exception:
            log.exception("JS fallback threw for %s", url)
            return set()

    def extract_from_response(
        self,
        response: Response,
        *,
        use_js_fallback: Optional[bool] = None,
    ) -> Set[str]:
        mode = self.use_js if use_js_fallback is None else use_js_fallback
        if not response or "html" not in response.headers.get(
            "Content-Type", ""
        ):
            return set()

        html_text = response.text
        hits = self._static_pass(html_text, response.url)
        if hits or not mode:
            log.info(
                "Static pass found %d emails in %s",
                len(hits),
                response.url,
            )
            return hits
        try:
            return self._render_and_extract(response.url)
        except Exception as e:
            log.warning(
                "JS fallback extraction failed for %s: %s",
                response.url,
                e,
            )
            return set()

    # ----------------- hybrid (async orchestrator hook) -----------------
    def extract_emails_hybrid(
        self,
        company: str,  # unused currently; reserved for future heuristics
        domain: str,
        base_url: str,
        *,
        max_discovery: int = 5,
        use_js: bool | None = None,
    ) -> Set[str]:
        if domain in self._domain_cache:
            return set(self._domain_cache[domain])

        emails: Set[str] = set()

        # Base page
        try:
            resp = http_client.safe_get(
                base_url, retry_count=1, timeout=(10, 30)
            )
            if resp and "html" in resp.headers.get("Content-Type", ""):
                emails.update(self._static_pass(resp.text, base_url))
        except Exception:
            pass

        # Discovery pages (early stop after enough findings)
        if not emails:
            try:
                from scraper.config import (
                    HIGH_PRIORITY_PARTS,
                    MEDIUM_PRIORITY_PARTS,
                )

                paths = list(HIGH_PRIORITY_PARTS) + list(
                    MEDIUM_PRIORITY_PARTS[:5]
                )
            except Exception:
                paths = [
                    "contact",
                    "about",
                    "impressum",
                    "legal",
                    "team",
                ]
            base = base_url.rstrip("/")
            for p in paths[:max_discovery]:
                url = f"{base}/{p}"
                try:
                    r = http_client.safe_get(
                        url, retry_count=0, timeout=(5, 20)
                    )
                    if r and "html" in r.headers.get("Content-Type", ""):
                        hits = self._static_pass(r.text, url)
                        emails.update(hits)
                        if len(emails) >= 3:  # heuristic early stop
                            break
                except Exception:
                    continue

        # Optional JS fallback
        if not emails and (use_js if use_js is not None else self.use_js):
            try:
                emails.update(self._render_and_extract(base_url))
            except Exception:
                pass

        if emails:
            # LRU eviction
            if len(self._domain_cache) >= self._cache_max:
                try:
                    oldest = next(iter(self._domain_cache))
                    self._domain_cache.pop(oldest, None)
                except StopIteration:
                    pass
            self._domain_cache[domain] = set(emails)
        return emails


# Singleton
hybrid_email_extractor = HybridEmailExtractor()

__all__ = [
    "HybridEmailExtractor",
    "hybrid_email_extractor",
]
