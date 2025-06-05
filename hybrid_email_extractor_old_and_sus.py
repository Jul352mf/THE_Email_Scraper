import logging
from functools import lru_cache
from bs4 import BeautifulSoup
import re, base64, codecs, html
from typing import Optional, Set
from requests import Response

from scraper.http import http_client
from scraper.browser_service import browser_service
from scraper.email_extractor import EmailExtractor, EmailValidationError

log = logging.getLogger(__name__)

class HybridEmailExtractor:
    def __init__(self, use_js_fallback: bool = True):
        self.static_extractor = EmailExtractor()
        self.use_js = use_js_fallback

    def _decode_cfemail(self, cf: str) -> str:
        """
        Decode Cloudflare's email obfuscation (data-cfemail).
        """
        # First byte is the XOR key
        key = int(cf[:2], 16)
        # Subsequent bytes are the obfuscated chars
        decoded = ''.join(
            chr(int(cf[i:i+2], 16) ^ key)
            for i in range(2, len(cf), 2)
        )
        return decoded

    @lru_cache(maxsize=256)
    def _render_and_extract(self, url: str) -> Set[str]:
        """Render via headless browser and re-run static pipeline."""
        html = browser_service.render(url)
        log.info("JS fallback for %s", url)
        return self._static_pass(html, url)

    def _static_pass(self, html_text: str, url: Optional[str] = None) -> Set[str]:
        """
        Run static decoders + regex/mailto logic.
        """
        hits: Set[str] = set()
        soup = BeautifulSoup(html_text, "html.parser")

        # Cloudflare data-cfemail
        cf_tags = soup.find_all("a", attrs={"data-cfemail": True})
        log.debug("STATIC: found %d CF-protected anchors in %s", len(cf_tags), url)
        for tag in cf_tags:
            blob = tag.get("data-cfemail", "")
            try:
                raw = self._decode_cfemail(blob)
                cleaned = self.static_extractor.clean_email(raw)
                hits.add(cleaned)
            except EmailValidationError:
                continue
            except Exception:
                continue

        # If we found any via CF, bail out early
        if hits:
            log.debug("STATIC: skipping other passes, %d CF hits", len(hits))
            return hits

        # HTML entities
        ent = html.unescape(soup.get_text(" "))
        hits.update(self.static_extractor.extract_from_text(ent, url))

        if hits:
            return hits

        # JS char codes
        for js in re.findall(r'fromCharCode\(([^)]+)\)', html_text):
            nums = [int(n) for n in js.split(",") if n.strip().isdigit()]
            decoded = "".join(chr(n) for n in nums)
            hits.update(self.static_extractor.extract_from_text(decoded, url))
            if hits:
                return hits

        # ROT13
        for block in re.findall(r"[A-Za-z]{30,}", html_text):
            try:
                rot = codecs.decode(block, "rot_13")
                hits.update(self.static_extractor.extract_from_text(rot, url))
                if hits:
                    return hits
            except Exception:
                pass

        # Base64
        for b64 in re.findall(r"'([A-Za-z0-9+/=]{40,})'", html_text):
            try:
                decoded = base64.b64decode(b64).decode("utf-8", errors="ignore")
                hits.update(self.static_extractor.extract_from_text(decoded, url))
                if hits:
                    return hits
            except Exception:
                pass

        # Regex + mailto fallback
        hits.update(self.static_extractor.extract_from_html(html_text, url))
        return hits

    def extract_from_url(
        self,
        url: str,
        *,
        use_js_fallback: Optional[bool] = None
    ) -> Set[str]:
        mode = self.use_js if use_js_fallback is None else use_js_fallback

        resp = http_client.safe_get(url, retry_count=2, timeout=(10,60))
        if not resp or "html" not in resp.headers.get("Content-Type", ""):
            return set()

        html_text = resp.text

        # Static pass
        hits = self._static_pass(html_text, url)
        if hits or not mode:
            log.info("Static pass found %d emails in %s", len(hits), url)
            return hits

        # JS fallback
        try:
            return self._render_and_extract(url)
        except Exception:
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

# Singleton instance
hybrid_email_extractor = HybridEmailExtractor()
