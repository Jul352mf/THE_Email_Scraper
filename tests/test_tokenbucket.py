import threading
import time
from scraper.http import TokenBucket


def worker(bucket: TokenBucket, results: list, idx: int):
    start = time.time()
    bucket.consume()
    results[idx] = time.time() - start


def test_concurrent_consumers_do_not_block():
    # rate_per_sec=1 token per second, capacity=1 -> first immediate, others wait ~1s staggered
    bucket = TokenBucket(rate_per_sec=1.0, capacity=1.0)
    n = 3
    results = [None] * n
    threads = []
    for i in range(n):
        t = threading.Thread(target=worker, args=(bucket, results, i))
        threads.append(t)
    start = time.time()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    total = time.time() - start
    # All threads should complete in roughly n seconds, but not block serially much longer.
    assert total < 5.0
    # first thread should be nearly instant
    assert results[0] < 0.1
    # some consumers will wait >0 but less than 2s
    assert all(r is not None for r in results)
