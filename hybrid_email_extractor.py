import logging
from functools import lru_cache
from bs4 import BeautifulSoup
import re, base64, codecs, html
from typing import Optional, Set
from requests import Response

from scraper.http import http_client
from scraper.browser_service import get_browser_service
from scraper.email_extractor import EmailExtractor, EmailValidationError

log = logging.getLogger(__name__)

class HybridEmailExtractor:
    def __init__(self, use_js_fallback: bool = True):
        self.static_extractor = EmailExtractor()
        self.use_js = use_js_fallback
        # to avoid duplicate extraction
        self._seen_urls: Set[str] = set()

    def _decode_cfemail(self, cf: str) -> str:
        key = int(cf[:2], 16)
        return ''.join(
            chr(int(cf[i:i+2], 16) ^ key)
            for i in range(2, len(cf), 2)
        )

    @lru_cache(maxsize=256)
    def _render_and_extract(self, url: str) -> Set[str]:
        log.info("JS fallback for %s", url)
        html = get_browser_service().render(url)
        return self._static_pass(html, url)

    def _static_pass(self, html_text: str, url: Optional[str]=None) -> Set[str]:
        # Unified static extraction pipeline that calls extract_from_text only once
        hits: Set[str] = set()
        soup = BeautifulSoup(html_text, 'html.parser')

        # 1) Cloudflare obfuscation
        cf = soup.find_all(attrs={'data-cfemail': True})
        for tag in cf:
            try:
                raw = self._decode_cfemail(tag['data-cfemail'])
                cleaned = self.static_extractor.clean_email(raw)
                hits.add(cleaned)
            except EmailValidationError:
                pass
        if hits:
            log.debug("CF hits: %d on %s", len(hits), url)
            return hits

        # 2) Gather all candidate text fragments
        text = soup.get_text(' ')
        text = html.unescape(text)

        # JS char codes
        for js in re.findall(r'fromCharCode\(([^)]+)\)', html_text):
            nums = [int(n) for n in js.split(',') if n.strip().isdigit()]
            text += ' ' + ''.join(chr(n) for n in nums)
            
            # log.debug("Found JS char codes %r on %s", js, url)

        # ROT13 blocks
        for block in re.findall(r"[A-Za-z]{30,}", html_text):
            try:
                text += ' ' + codecs.decode(block, 'rot_13')
                # log.debug("Found ROT13 blocks %r on %s", block, url)
            except Exception:
                pass

        # Base64 blocks
        for b64 in re.findall(r"'([A-Za-z0-9+/=]{40,})'", html_text):
            try:
                text += ' ' + base64.b64decode(b64).decode('utf-8', 'ignore')
                # log.debug("Found Base64 blocks %r on %s", b64, url)
            except Exception:
                pass

        # 3) Single text extraction pass
        hits.update(self.static_extractor.extract_from_text(text, url))
        if hits:
            log.debug("Static text hits: %d on %s", len(hits), url)
            return hits

        # 4) Fallback to HTML extraction (includes mailto)
        hits.update(self.static_extractor.extract_from_html(html_text, url))
        log.debug("HTML fallback hits: %d on %s", len(hits), url)
        return hits

    def extract_from_url(self, url: str, *, use_js_fallback: Optional[bool]=None) -> Set[str]:
        if url in self._seen_urls:
            log.debug("Skip duplicate %s", url)
            return set()
        self._seen_urls.add(url)

        resp = http_client.safe_get(url, retry_count=2, timeout=(10,60))
        if not resp or 'html' not in resp.headers.get('Content-Type', ''):
            return set()

        hits = self._static_pass(resp.text, url)
        log.info("Static pass found %d on %s", len(hits), url)
        if hits or not (self.use_js if use_js_fallback is None else use_js_fallback):
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
        use_js_fallback: Optional[bool] = None
    ) -> Set[str]:
        """
        Extract emails from an already-fetched Response, reusing the same logic
        as extract_from_url without issuing another HTTP request.
        """
        # decide whether to do JS fallback
        mode = self.use_js if use_js_fallback is None else use_js_fallback

        # verify we have HTML
        content_type = response.headers.get("Content-Type", "")
        if not response or "html" not in content_type:
            return set()

        html_text = response.text

        # Static pass
        hits = self._static_pass(html_text, response.url)
        if hits or not mode:
            log.info("Static pass found %d emails in %s", len(hits), response.url)
            return hits

        # JS fallback
        try:
            return self._render_and_extract(response.url)
        except Exception as e:
            log.warning("JS fallback extraction failed for %s: %s", response.url, e)
            return set()

# Singleton
hybrid_email_extractor = HybridEmailExtractor()
