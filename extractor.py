"""
Email extraction routines and regex.
"""
# TODO: paste EMAIL_RE, clean_email(), emails_from_url(), etc.

EMAIL_RE = re.compile(r"(?i)(?<![A-Z0-9._%+-])[A-Z0-9._%+-]+@(?:[A-Z0-9-]+\.)+[A-Z]{2,10}(?![A-Z0-9._%+-])")

# ─── EMAIL EXTRACTION ─────────────────────────────
def clean_email(e: str) -> str:
    local = e.split("?",1)[0]
    user, host = local.split("@",1)
    try:
        host = idna.decode(host)
    except Exception:
        pass
    return f"{user}@{host}".lower()


def emails_from_url(url: str) -> set[str]:
    if not PROCESS_PDFS and url.lower().endswith(".pdf"):
        log.debug("Skipping PDF %s", url)
        return set()
    r = safe_get(url)
    if r and "html" in r.headers.get("Content-Type",""):
        hits = {clean_email(m.group(0)) for m in EMAIL_RE.finditer(r.text)}
        log.debug(" %2d emails on %s", len(hits), url)
        return hits
    return set()
