import random
from scraper.http import HttpClient, _session_mgr


class DummyResp:
    def __init__(self, url: str):
        self.status_code = 200
        self.ok = True
        self.url = url
        self.content = b""


def test_user_agent_rotates_and_override_preserved():
    client = HttpClient()
    sess = _session_mgr.session("example.com")
    captured = []

    # Deterministic randomness for reproducibility
    random.seed(1234)

    orig_get = sess.get

    def fake_get(url, allow_redirects=True, timeout=None, headers=None, **kw):
        captured.append(headers.get("User-Agent"))
        return DummyResp(url)

    sess.get = fake_get
    try:
        # Distinct paths -> distinct canonical URLs (bypass loop guard)
        for i in range(10):
            client.safe_get(f"https://example.com/ua{i}")
        unique = len(set(captured))
        # Expect rotation (at least 2 distinct values)
        assert unique > 1, (
            f"Expected rotating UAs, got only {unique}: {captured}"
        )

        # Explicit override preserves provided UA (no injection)
        client.safe_get(
            "https://example.com/override",
            headers={"User-Agent": "StaticUA/1.0"},
        )
        assert captured[-1] == "StaticUA/1.0"
    finally:
        sess.get = orig_get
