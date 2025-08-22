import threading
from scraper.crawler import Crawler
from scraper.config import config


def test_crawler_default_workers(monkeypatch):
    # Ensure config.max_workers is a known value for test
    original = config.max_workers
    config.max_workers = 3
    crawler = Crawler()

    started = []
    orig_thread = threading.Thread

    def tracking_thread(*a, **kw):
        t = orig_thread(*a, **kw)
        started.append(t)
        return t

    monkeypatch.setattr(threading, "Thread", tracking_thread)

    # Call with num_workers None so default applies; keep limit tiny
    crawler.crawl_small("example.com", limit=1, num_workers=None)

    # Confirm number of worker threads equals config.max_workers
    # Filter only names matching our pattern
    worker_threads = [t for t in started
                      if t.name.startswith("CrawlerThread-")]
    assert len(worker_threads) == 3, worker_threads

    config.max_workers = original
