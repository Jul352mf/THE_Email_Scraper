import threading
from scraper.crawler import Crawler
from scraper.config import config


def test_crawler_default_workers():
    original_workers = config.max_workers
    config.max_workers = 3
    crawler = Crawler()

    started = []
    orig_thread = threading.Thread

    def tracking_thread(*a, **kw):
        t = orig_thread(*a, **kw)
        started.append(t)
        return t

    threading.Thread = tracking_thread  # type: ignore
    try:
        # Call with num_workers None so default applies; keep limit tiny
        crawler.crawl_small("example.com", limit=1, num_workers=None)
    finally:
        threading.Thread = orig_thread  # restore

    worker_threads = [
        t for t in started if t.name.startswith("CrawlerThread-")
    ]
    assert len(worker_threads) == 3, worker_threads
    config.max_workers = original_workers

