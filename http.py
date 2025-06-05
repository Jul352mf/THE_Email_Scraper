"""
Enhanced HTTP client module (rev-3)
==================================

Change in this revision
-----------------------
* **HEAD requests no longer interact with the redirect-loop set**:
  * We *do not* check `visited` before sending a HEAD.
  * We *do not* add the canonical URL to `visited` after a HEAD.

Everything else remains as in rev-2 (TLS verification by default, canonical URL
loop detection, shared per-thread session, debug dump, etc.).
"""

from __future__ import annotations

import logging
import os
import re
from threading import local
import threading
import time
from typing import Any, Callable, Dict, Optional
from urllib.parse import urlparse, urlunparse, urljoin
import random

import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import ConnectTimeout, SSLError
from urllib3.util.retry import Retry

from scraper.config import config
import urllib3
from urllib3.exceptions import InsecureRequestWarning
from collections import Counter

log = logging.getLogger(__name__)

# suppress urllib3 warnings once at module top
urllib3.disable_warnings(InsecureRequestWarning)
logging.getLogger("urllib3.connectionpool").setLevel(logging.ERROR)

_thread_local = local()
_domain_buckets: dict[str, TokenBucket] = {}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def canonicalise(url: str) -> str:
    p = urlparse(url)
    host = p.netloc.lower().removeprefix("www.")
    path = p.path.rstrip("/") or "/"
    return urlunparse((p.scheme.lower(), host, path, "", "", ""))

def validate_url(url: str) -> bool:
    if not url or len(url) > config.max_url_length:
        return False
    try:
        p = urlparse(url)
        if p.scheme not in {"http", "https"} or not p.netloc:
            return False
        if re.search(r"^(file|data|javascript):", url, re.I):
            return False
        return True
    except Exception:
        return False

def _get_bucket_for(domain: str) -> TokenBucket:
    rate = 1.0 / config.min_crawl_delay
    cap = config.max_crawl_delay / config.min_crawl_delay
    bucket = _domain_buckets.get(domain)
    if not bucket:
        bucket = TokenBucket(rate_per_sec=rate, capacity=cap)
        _domain_buckets[domain] = bucket
    return bucket


class TokenBucket:
    def __init__(self, rate_per_sec: float, capacity: float):
        self.rate = rate_per_sec
        self.capacity = capacity
        self._lock = threading.Lock()
        self._tokens = capacity
        self._last = time.time()

    def consume(self, tokens: float = 1.0):
        with self._lock:
            now = time.time()
            # add tokens since last check
            delta = (now - self._last) * self.rate
            self._tokens = min(self.capacity, self._tokens + delta)
            self._last = now

            if self._tokens >= tokens:
                self._tokens -= tokens
                return  # go ahead immediately

            # not enough tokens—compute sleep
            needed = tokens - self._tokens
            wait = needed / self.rate
            time.sleep(wait)
            # after sleeping, “spend” the tokens
            self._tokens = 0


# ---------------------------------------------------------------------------
# Thread-local session manager
# ---------------------------------------------------------------------------

class _SessionManager:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._template: Optional[requests.Session] = None

    def _build_template(self) -> requests.Session:
        s = requests.Session()
        s.headers.update({"User-Agent": config.user_agents[0]})
        retry = Retry(
            total=2,
            backoff_factor=0.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET", "HEAD"),
        )
        adapter = HTTPAdapter(max_retries=retry)
        s.mount("http://", adapter)
        s.mount("https://", adapter)
        s.max_redirects = max(1, config.max_redirects)
        return s

    def session(self, domain: str) -> requests.Session:
        # Each thread has a dict of domain->Session
        sess_map = getattr(_thread_local, "sessions", None)
        if sess_map is None:
            sess_map = {}
            _thread_local.sessions = sess_map

        if domain not in sess_map:
            with self._lock:
                if self._template is None:
                    self._template = self._build_template()
                s = requests.Session()
                s.headers = self._template.headers.copy()
                s.adapters = self._template.adapters
                s.cookies = requests.cookies.RequestsCookieJar()
            sess_map[domain] = s

        sess = sess_map[domain]
        sess.verify = not config.insecure_ssl
        return sess

    def visited(self) -> set[str]:
        v = getattr(_thread_local, "visited", None)
        if v is None:
            v = set()
            _thread_local.visited = v
        return v

    def prune(self, keep: int = 1000) -> None:
        v = self.visited()
        if len(v) > keep:
            v.clear()

_session_mgr = _SessionManager()

# ---------------------------------------------------------------------------
# HTTP client
# ---------------------------------------------------------------------------

class HttpClient:
    DEBUG = os.getenv("DEBUG_MODE", "0").lower() in {"1", "true", "yes"}
    DEBUG_DIR = os.getenv("DEBUG_DIR", "debug_output")

    def __init__(self) -> None:
        self.stats = Counter()
        if self.DEBUG and not os.path.exists(self.DEBUG_DIR):
            try:
                os.makedirs(self.DEBUG_DIR, exist_ok=True)
            except Exception as exc:
                log.warning("Failed to create debug dir %s: %s", self.DEBUG_DIR, exc)
                self.DEBUG = False

    def safe_get(
        self,
        url: str,
        method: str = "GET",
        timeout: Optional[tuple[float, float]] = None,
        headers: Optional[Dict[str, str]] = None,
        retry_count: int = 1,
        retry_delay: float = 1.0,
        callback: Optional[Callable[[requests.Response], Any]] = None,
    ) -> Optional[requests.Response]:
        if not validate_url(url):
            log.warning("Skipping invalid URL: %s", url)
            self.stats["skipped_urls"] += 1
            return None

        #––– BLOCKED‐PATTERN CHECK –––
        blocked = {
            p.strip().lower()
            for p in os.getenv("BLOCKED_DOMAINS", "").split(",")
            if p.strip()
        }
        parsed = urlparse(url)
        host = parsed.netloc.lower()
        path = parsed.path.lower()
        for pat in blocked:
            if not pat.startswith(".") and host.endswith(pat):
                log.debug("Blocked domain %s (matched %s)", host, pat)
                self.stats["skipped_urls"] += 1
                return None
        for pat in blocked:
            if pat.startswith(".") and path.endswith(pat):
                log.debug("Blocked extension %s on %s", pat, url)
                self.stats["skipped_urls"] += 1
                return None

        head_mode = method.upper() == "HEAD"
        canon = canonicalise(url)

        # only throttle actual GETs, not HEADs
        if not head_mode:
            domain = normalise_domain(parsed.netloc)
            bucket = _get_bucket_for(domain)
            bucket.consume()
            
        # Rotate User-Agent per request
        hdrs = headers.copy() if headers else {}
        
        if config.proxies:
            proxy = random.choice(config.proxies)
            proxy_url = f"http://{proxy}"
            proxies = {"http": proxy_url, "https": proxy_url}
        else:
            proxies = None
        
        if not head_mode and hasattr(config, "user_agents"):
            hdrs["User-Agent"] = random.choice(config.user_agents)
        else:
            hdrs.setdefault("User-Agent", random.choice(config.user_agents))


        # track domain
        domain = normalise_domain(parsed.netloc)

        if not getattr(_thread_local, "visited", None):
            _thread_local.visited = set()

        # loop guard
        if not head_mode and canon in _thread_local.visited:
            log.warning("Redirect loop detected – already visited %s", url)
            self.stats["skipped_urls"] += 1
            return None

        if timeout is None:
            timeout = config.request_timeout

        sess = _session_mgr.session(domain)
        request_fn = sess.head if head_mode else sess.get
        hdrs = headers.copy() if headers else {}
        self.stats["total_requests"] += 1
        
        
        response: Optional[requests.Response] = None
        for attempt in range(retry_count):
            try:
                response = request_fn(
                    url,
                    allow_redirects=True,
                    timeout=timeout,
                    headers=(headers or hdrs),
                    proxies=proxies,
                )

                # 1) No response at all → trigger fallback
                if response is None:
                    raise requests.exceptions.ConnectionError(f"No response for {url}")

                status = response.status_code
                log.info("HTTP %s %s → %s", method, url, status)

                # 2) Too many requests → backoff & retry the *same* URL
                if status == 429:
                    backoff = retry_delay * (2 ** attempt)
                    log.warning(
                        "429 Too Many Requests for %s; backing off %.1fs (attempt %d/%d)",
                        url, backoff, attempt + 1, retry_count
                    )
                    time.sleep(backoff)
                    continue

                # 3) ANY non-2xx status → treat as error and fall into except
                if not (200 <= status < 300):
                    raise requests.exceptions.HTTPError(f"Bad status {status} for {url}")

                # SUCCESS! real 2xx response
                break

            except (ConnectTimeout, requests.exceptions.ConnectionError, 
                    SSLError, requests.exceptions.RetryError, 
                    requests.exceptions.HTTPError) as err:
                log.debug("Network/HTTP error (attempt %d/%d) for %s: %s",
                          attempt + 1, retry_count, url, err)

                # SSL fallback if allowed
                if isinstance(err, SSLError) and attempt + 1 >= retry_count and config.insecure_ssl:
                    try:
                        log.info("SSL failed, retrying with verify=False: %s", url)
                        response = request_fn(
                            url, allow_redirects=True, timeout=timeout,
                            headers=(headers or hdrs), verify=False, proxies=proxies
                        )
                        if response and response.ok:
                            break
                    except Exception as e2:
                        log.warning("SSL-off retry failed for %s: %s", url, e2)

                # www-prefix fallback
                parsed = urlparse(url)
                host = parsed.netloc
                if not host.startswith("www."):
                    fallback = urlunparse(parsed._replace(netloc="www." + host))
                    log.info("Retrying with www-prefix: %s", fallback)
                    try:
                        response = request_fn(
                            fallback, allow_redirects=True, timeout=timeout,
                            headers=(headers or hdrs), verify=not config.insecure_ssl,
                            proxies=proxies
                        )
                        if response and response.ok:
                            url = fallback
                            break
                    except Exception as e2:
                        log.warning("www-prefix retry failed for %s: %s", fallback, e2)

                # http-scheme fallback
                if parsed.scheme == "https":
                    http_url = urlunparse(parsed._replace(scheme="http"))
                    log.info("Retrying with HTTP: %s", http_url)
                    try:
                        response = request_fn(
                            http_url, allow_redirects=True, timeout=timeout,
                            headers=(headers or hdrs), proxies=proxies
                        )
                        if response and response.ok:
                            url = http_url
                            break
                    except Exception as e3:
                        log.warning("HTTP fallback failed for %s: %s", http_url, e3)

                # if still no good response, and we have more attempts, wait
                if attempt < retry_count - 1:
                    time.sleep(retry_delay * (2 ** attempt))

            except requests.RequestException as err:
                log.debug("Generic request exception (attempt %d/%d) for %s: %s",
                          attempt + 1, retry_count, url, err)
                if attempt < retry_count - 1:
                    time.sleep(retry_delay * (2 ** attempt))

        # record final status
        status = response.status_code if response else "no-response"
        self.stats[f"status_{status}"] += 1

        # give up if still no OK response
        if not response or not response.ok:
            return None

        if response.ok:
            if not hasattr(_thread_local, "visited_subdomains"):
                _thread_local.visited_subdomains = set()
            _thread_local.visited_subdomains.add(response.url)

        if self.DEBUG and not head_mode: # and response.status_code == 200
            log.info("DEBUG-DUMP %s → %d", url, response.status_code)
            self._dump_debug(url, response)

        # mark canon as fetched
        if not head_mode:
            _thread_local.visited.add(canon)

        if callback and not head_mode:
            try:
                callback(response)
            except Exception as exc:
                log.error("Callback raised for %s: %s", url, exc)

        _session_mgr.prune()
        return response

    def _dump_debug(self, url: str, resp: requests.Response) -> None:
        try:
            p = urlparse(url)
            host = p.netloc or "_"
            path = p.path.strip("/").replace("/", "_") or "index"
            fname = f"{host}_{path}.html"
            with open(os.path.join(self.DEBUG_DIR, fname), "wb") as fp:
                fp.write(resp.content)
            log.debug("Saved debug dump for %s → %s", url, fname)
        except Exception as exc:
            log.warning("Failed to save debug dump for %s: %s", url, exc)

def normalise_domain(url: str) -> str:
    """
    Normalize a domain by removing www prefix and converting to lowercase.
    """
    try:
        host = urlparse(url).netloc if url.startswith(("http://", "https://")) else url
        return host.lower().removeprefix("www.")
    except Exception as e:
        log.warning("Domain normalization error for %s: %s", url, e)
        return url.lower()

def join_url(base: str, path: str) -> str:
    """
    Join a base URL and a path, handling relative paths correctly.
    """
    if path.startswith(("http://", "https://")):
        return path

    if not base.startswith(("http://", "https://")):
        base = f"https://{base}"

    joined_url = urljoin(base, path.lstrip("/"))
    if not validate_url(joined_url):
        log.warning("Invalid joined URL: %s + %s", base, path)
        return ""
    return joined_url

# single, shared instance
http_client = HttpClient()