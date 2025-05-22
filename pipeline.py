"""
High-level orchestration of a single company and
bulk processing from Excel to Excel.
"""
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
import pandas as pd
from . import google_search, crawler, extractor, domain_utils, config

from . import google_search, domain_utils, extractor, crawler, config
log = logging.getLogger(__name__)

# ─── WORKER ───────────────────────────────────────
def process_company(company: str):
    stats = Counter()
    rows: list[dict[str,str]] = []
    stats["leads"] += 1
    log.info("▶ %s", company)

    hits = google_search(company)
    if not hits:
        stats["no_google"] += 1
        return stats, rows

    score, link = max(
        ((score_domain(company, h["link"]), h["link"]) for h in hits),
        key=lambda t: t[0]
    )
    if score < 60:
        stats["domain_unclear"] += 1
        return stats, rows

    domain = normalise_domain(link)
    stats["domain"] += 1
    log.info("✓ domain %s", domain)

    emails = emails_from_url(f"https://{domain}")
    used_sitemap = False
    for sm in discover_sitemaps(domain):
        used_sitemap = True
        r = safe_get(sm)
        if r:
            for page in parse_sitemap(r.content):
                if any(p in page.lower() for p in PRIORITY_PARTS):
                    emails |= emails_from_url(page)
    if used_sitemap:
        stats["sitemap"] += 1

    if not emails:
        log.debug("fallback crawl %s", domain)
        emails |= crawl_small(domain)

    if emails:
        stats["with_email"] += 1
        rows = [{"Company": company, "Domain": domain, "Email": e} for e in emails]
    else:
        stats["without_email"] += 1

    return stats, rows


def scrape_companies(src: str, dst: str):
    df = pd.read_excel(src)
    if "Company" not in df.columns:
        raise ValueError("Input needs 'Company' column")

    companies = [c for c in df["Company"].astype(str) if c.strip()]
    start_time = time.time()
    global_stats = Counter()
    all_rows: list[dict[str,str]] = []

    executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
    futures = {executor.submit(process_company, c): c for c in companies}
    try:
        for fut in as_completed(futures):
            stats, rows = fut.result()
            global_stats.update(stats)
            all_rows.extend(rows)
    except KeyboardInterrupt:
        log.warning("Interrupted by user; shutting down threads")
        executor.shutdown(wait=False)
        sys.exit(1)
    finally:
        executor.shutdown(wait=True)

    df_out = pd.DataFrame(all_rows, columns=["Company","Domain","Email"]).drop_duplicates()
    df_out.to_excel(dst, index=False)

    elapsed = time.time() - start_time
    log.info(
        "\n+--------------------------------------------------+\n"
        "| RUN SUMMARY                                      |\n"
        "+--------------------------------------------------+\n"
        f"| Leads           : {global_stats['leads']:>3}\n"
        f"| Domain found    : {global_stats['domain']:>3}\n"
        f"| No Google hits  : {global_stats['no_google']:>3}\n"
        f"| Domain unclear  : {global_stats['domain_unclear']:>3}\n"
        f"| Sitemap used    : {global_stats['sitemap']:>3}\n"
        f"| With e-mail     : {global_stats['with_email']:>3}\n"
        f"| Without e-mail  : {global_stats['without_email']:>3}\n"
        f"| Unique e-mails  : {df_out['Email'].nunique():>3}\n"
        f"| Runtime         : {elapsed:6.1f} s\n"
        "+--------------------------------------------------+"
    )
    log.info("Saved %d rows -> %s", len(df_out), dst)
    log.info("Verbose log -> %s", Path(LOGFILE).resolve())


__all__ = ["scrape_companies"]
