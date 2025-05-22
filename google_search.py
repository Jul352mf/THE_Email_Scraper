"""
Google Custom Search functions.
Cut & paste _respect_rate(), _do_google_search() and google_search() here
and change imports to use `from .config import settings`.
"""
# TODO: paste code


# ThreadPool for Google calls
google_executor = ThreadPoolExecutor(max_workers=1)


# ─── RATE LIMITER ─────────────────────────────────
def _respect_rate():
    global _last_google_ts
    with _google_lock:
        wait = GOOGLE_SAFE_INTERVAL - (time.time() - _last_google_ts)
        if wait > 0: time.sleep(wait)
        _last_google_ts = time.time()


# ─── GOOGLE SEARCH (serialized) ─────────────────────
def _do_google_search(query: str):
    from googleapiclient.discovery import build
    from googleapiclient.errors import HttpError
    service = build("customsearch","v1",developerKey=API_KEY,cache_discovery=False)
    for attempt in range(GOOGLE_MAX_RETRIES):
        try:
            _respect_rate()
            return service.cse().list(q=query,cx=CX_ID,num=10).execute().get("items",[])
        except HttpError as e:
            if getattr(e.resp,"status",None) in {403,429}:
                back = 2**attempt * 2
                log.warning("Google quota %s – sleeping %ds", e.resp.status, back)
                time.sleep(back)
                continue
            raise
    log.error("Google search failed for %s", query)
    return []


def google_search(query: str):
    return google_executor.submit(_do_google_search, query).result()
