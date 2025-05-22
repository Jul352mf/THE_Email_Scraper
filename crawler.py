"""
Site & sitemap crawling utilities.
"""

SITEMAPS = ("sitemap.xml","sitemap_index.xml","sitemap-index.xml","sitemap1.xml")

# Shared global page counts
global_page_count: dict[str,int] = defaultdict(int)
global_page_lock = threading.Lock()

# ─── HTTP SESSION ───────────────────────────────────
UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
      " (KHTML, like Gecko) Chrome/124.0 Safari/537.36")
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": UA}); SESSION.timeout = (10,20)
if INSECURE_SSL:
    SESSION.verify = False
    import urllib3; urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ─── SMART SAFE_GET ────────────────────────────────
def safe_get(url: str, method: str = "GET"):
    try:
        fn = SESSION.head if method=="HEAD" else SESSION.get
        r = fn(url, allow_redirects=True)
    except requests.RequestException:
        return None
    final = r.url
    if final and final != url:
        thread_local = threading.local()
        if hasattr(thread_local, 'visited'):
            thread_local.visited.discard(url)
        try:
            r = SESSION.get(final)
        except requests.RequestException:
            return None
    return r if r and r.ok else None

# ─── SITEMAP DISCOVERY & PARSING ───────────────────
def discover_sitemaps(domain: str):
    any_found = False
    for name in SITEMAPS:
        url = f"https://{domain}/{name}"
        log.debug("Checking sitemap candidate: %s", url)
        head = safe_get(url, "HEAD")
        status = head.status_code if head else 'ERR'
        log.debug(" HEAD %s → %s", url, status)
        if head:
            body = safe_get(url)
            size = len(body.content) if body and body.ok else 'FAIL'
            log.debug(" GET %s → %s bytes", url, size)
            if body and body.ok and body.content.startswith(b"<?xml"):
                any_found = True
                yield url
    if not any_found:
        rt = safe_get(f"https://{domain}/robots.txt")
        if rt:
            for line in rt.text.splitlines():
                if line.lower().startswith("sitemap:"):
                    yield line.split(":",1)[1].strip()


def parse_sitemap(xml: bytes):
    soup = BeautifulSoup(xml, "xml")
    for loc in soup.find_all("loc"):
        u = loc.get_text(strip=True)
        if u:
            yield u


# ─── CRAWL SMALL SITES WITH SHARED QUOTA ────────────
def crawl_small(domain: str, limit: int = MAX_FALLBACK_PAGES) -> set[str]:
    start = time.time()
    avg_page = 2.0
    max_time = min(60, limit * avg_page)
    q = deque([f"https://{domain}"])
    seen = set(); found = set()
    while q:
        if time.time() - start > max_time and len(seen) >= limit//2:
            log.warning("Timeout on %s after %ds", domain, max_time)
            break
        url = q.popleft()
        with global_page_lock:
            if global_page_count[domain] >= limit:
                break
            global_page_count[domain] += 1
        if url in seen:
            continue
        seen.add(url)
        found |= emails_from_url(url)
        r = safe_get(url)
        if not r: continue
        soup = BeautifulSoup(r.text, "html.parser")
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if href.startswith("mailto:"):
                found.add(clean_email(href[7:]))
            elif href.startswith("/") or domain in href:
                full = urljoin(url, href)
                if full.startswith("http"):
                    q.append(full)
    return found