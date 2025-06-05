# scraper/browser_service.py

import logging
import traceback
import threading
from queue import Queue, Empty
from playwright.sync_api import sync_playwright

log = logging.getLogger(__name__)

class BrowserService(threading.Thread):
    def __init__(self):
        super().__init__(daemon=True, name="BrowserService")
        self._requests: Queue[tuple[str, Queue[str]]] = Queue()
        self._stop = False

    def run(self):
        # start Playwright
        playwright = sync_playwright().start()
        browser = playwright.chromium.launch(headless=True)
        log.info("BrowserService: starting Playwright browser on thread %s", threading.current_thread().name)
        try:
            while not self._stop:
                try:
                    url, resp_q = self._requests.get(timeout=0.5)
                except Empty:
                    continue

                # sentinel to shut down
                if url is None:
                    break

                try:
                    page = browser.new_page()
                    page.goto(url, wait_until="networkidle", timeout=30000)
                    html = page.content()
                    page.close()
                    resp_q.put(html)
                except Exception as page_err:
                    # catch everything from Playwright
                    log.error("Playwright render error for %s: %s\n%s", url, page_err, traceback.format_exc())
                    try:
                        resp_q.put("")   # empty so extractor moves on
                    except Exception:
                        pass
        except Exception as thread_err:
            # this should never bring the thread down
            log.critical("BrowserService thread crash: %s\n%s", thread_err, traceback.format_exc())
        finally:
            try:
                browser.close()
            except Exception:
                pass
            playwright.stop()

    def render(self, url: str, timeout: float = 30.0) -> str:
        """Called from any thread: returns fully rendered HTML."""
        resp_q: Queue[str] = Queue(maxsize=1)
        self._requests.put((url, resp_q))
        return resp_q.get(timeout=timeout)

    def shutdown(self):
        self._stop = True
        # send sentinel to unblock queue
        self._requests.put((None, Queue()))
        
browser_service = BrowserService()