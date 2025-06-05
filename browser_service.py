import uuid, logging
from multiprocessing import Process, Event, Queue, Manager, TimeoutError
from queue import Empty
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

log = logging.getLogger(__name__)


# these four must exist before _ensure_comm() ever runs
_manager = None
_requests = None
_responses = None
_browser_service = None

def _ensure_comm():
    global _manager, _requests, _responses
    if _manager is None:
        from multiprocessing import Manager, Queue
        _manager   = Manager()
        _requests  = Queue()
        _responses = _manager.dict()

def get_browser_service():
    global _browser_service
    _ensure_comm()
    if _browser_service is None:
        _browser_service = BrowserService(
            render_timeout=30.0,
            idle_timeout=5.0,
            ignore_https_errors=True
        )
        _browser_service.start()
    return _browser_service


def _render_page(page, url: str, nav_timeout: int, idle_timeout: int) -> str:
    """
    Navigate to `url` waiting up to nav_timeout ms for initial networkidle.
    Then wait up to idle_timeout ms for a final networkidle.
    Return whatever HTML is available.
    """
    try:
        page.goto(
            url,
            wait_until="networkidle",
            timeout=nav_timeout
        )
    except PWTimeout:
        log.warning("networkidle nav timed out after %dms for %s", nav_timeout, url)
    else:
        try:
            page.wait_for_load_state("networkidle", timeout=idle_timeout)
        except PWTimeout:
            log.debug("extra idle wait of %dms expired for %s", idle_timeout, url)

    return page.content()

class BrowserService(Process):
    def __init__(self, render_timeout=60., idle_timeout=15., ignore_https_errors=True):
        _ensure_comm()
        super().__init__(daemon=True, name="BrowserService")
        # Only primitives here
        self.render_timeout     = render_timeout
        self.idle_timeout       = idle_timeout
        self.ignore_https_errors = ignore_https_errors
        # grab the shared, picklable objects
        self._requests  = _requests
        self._responses = _responses
        self._stop_event = Event()

    def run(self):
        playwright = sync_playwright().start()
        browser = playwright.chromium.launch(headless=True)
        context = browser.new_context(ignore_https_errors=self.ignore_https_errors)
        log.info("BrowserService started")

        try:
            while not self._stop_event.is_set():
                try:
                    request_id, url = self._requests.get(timeout=0.5)
                except Empty:
                    continue
                if url is None:  # shutdown sentinel
                    break

                resp_q = self._responses.get(request_id)
                if not resp_q:
                    continue

                page = browser.new_page()
                try:
                    html = _render_page(
                        page,
                        url,
                        nav_timeout=int(self.render_timeout * 1_000),
                        idle_timeout=int(self.idle_timeout * 1_000)
                    )
                    resp_q.put(html)
                except PWTimeout:
                    log.warning("Render timeout for %s", url)
                    resp_q.put("")
                except Exception as e:
                    log.error("Render error for %s: %s", url, e, exc_info=True)
                    resp_q.put("")
                finally:
                    page.close()
        finally:
            context.close()
            browser.close()
            playwright.stop()
            log.info("BrowserService shutdown complete")

    def render(self, url, timeout=None):
        if timeout is None:
            timeout = self.render_timeout + self.idle_timeout

        req_id = str(uuid.uuid4())
        resp_q = _manager.Queue(1)
        self._responses[req_id] = resp_q
        self._requests.put((req_id, url))

        try:
            return resp_q.get(timeout=timeout)
        except (Empty, TimeoutError):
            log.warning("Render() timeout for %s", url)
            return ""
        finally:
            self._responses.pop(req_id, None)

    def shutdown(self):
        self._stop_event.set()
        self._requests.put((None, None))
