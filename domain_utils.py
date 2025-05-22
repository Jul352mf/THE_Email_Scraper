"""
Domain helpers: normalisation & fuzzy scoring.
"""
# TODO: paste normalise_domain(), score_domain(), regexes, etc.


# ─── HELPERS ───────────────────────────────────────
def normalise_domain(u: str) -> str:
    host = urlparse(u).netloc if u.startswith("http") else u
    return host.lower().removeprefix("www.")


def score_domain(company: str, url: str) -> int:
    host = normalise_domain(url)
    base = re.sub(r"[^a-z0-9]", "", company.lower())
    penalty = 25 if any(b in host for b in {"linkedin","facebook","instagram","twitter"}) else 0
    ext = tldextract.extract(host)
    s = max(fuzz.partial_ratio(base, ext.domain or ""), fuzz.partial_ratio(base, ext.subdomain or ""))
    return max(0, s-penalty)