# scraper/browser_service.py

import logging
import traceback
import multiprocessing as mp
from queue import Queue, Empty
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

log = logging.getLogger(__name__)

class BrowserService(mp.Process):
    def __init__(self, render_timeout: float = 30.0):
        super().__init__(daemon=True, name="BrowserService")
        self._requests: Queue[tuple[str, Queue[str]]] = Queue()
        self._stop = False
        # set your page‐load timeout (seconds)
        self.render_timeout = render_timeout  

    def run(self):  # now runs in its own process
        playwright = sync_playwright().start()
        browser = playwright.chromium.launch(
            headless=True,
            ignore_https_errors=True    # <— allow invalid certs
            )
        
        log.info("BrowserService: starting Playwright browser in process %s", self.name)
        try:
            while not self._stop:
                try:
                    url, resp_q_pickled = self._requests.get(timeout=0.5)
                    resp_q = mp.loads(resp_q_pickled)
                except Empty:
                    continue

                # sentinel to shut down
                if url is None:
                    break

                try:
                    page = browser.new_page()
                    page.goto(
                        url, wait_until="domcontentloaded", timeout=int(self.render_timeout * 1000)
                        )
                    html = page.content()
                    resp_q.put(html)
                except PlaywrightTimeout:
                    log.warning("Playwright render timed out for %s", url)
                    resp_q.put("")  # treat as empty render
                except Exception as page_err:
                    log.error(
                        "Playwright render error for %s: %s\n%s",
                        url,
                        page_err,
                        traceback.format_exc()
                    )
                    resp_q.put("")   # always unblock caller
                finally:
                    try:
                        page.close()
                    except Exception:
                        pass

        except Exception as thread_err:
            log.critical("BrowserService thread crash: %s\n%s", thread_err, traceback.format_exc())
        finally:
            try:
                browser.close()
            except Exception:
                pass
            playwright.stop()

    def render(self, url: str, timeout: float = 30.0) -> str:
        """Called from any thread: returns fully rendered HTML."""
        resp_q = mp.Queue(1)
        # pickle the queue reference so it survives across the process boundary
        self._requests.put((url, mp.dumps(resp_q)))
        try:
            return resp_q.get(timeout=timeout)
        except Empty:
            log.warning("BrowserService.render timeout waiting for response for %s", url)
            return ""  # safe fallback if something went wrong

    def shutdown(self):
        self._stop.set()
        # sending None will also wake the loop
        self._requests.put((None, b""))

# in your startup code:
browser_service = BrowserService(render_timeout=45.0)
browser_service.start()