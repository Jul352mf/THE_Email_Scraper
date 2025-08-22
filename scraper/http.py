"""Compatibility shim for legacy imports.

Tests and older code expect symbols in `scraper.http` but they were
moved to `scraper.http_client`. This lightweight re-export keeps the
public surface stable without duplication.
"""
from .http_client import (
    TokenBucket,
    HttpClient,
    _session_mgr,
    normalise_domain,
    join_url,
    http_client,
)

__all__ = [
    "TokenBucket",
    "HttpClient",
    "_session_mgr",
    "normalise_domain",
    "join_url",
    "http_client",
]
