"""Async support components (circuit breaker, token bucket, session manager).

Split out from `async_http_client` to keep that file focused on request logic.
"""
from __future__ import annotations

import asyncio
import logging
import random
import time
from collections import Counter
from typing import Any, Dict

from aiohttp import ClientSession, ClientTimeout, TCPConnector

from scraper.config import config

log = logging.getLogger(__name__)


class AsyncCircuitBreaker:
    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self._states: Dict[str, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()

    async def can_request(self, domain: str) -> bool:
        async with self._lock:
            state = self._get_state(domain)
            now = time.time()
            if state['failure_count'] < self.failure_threshold:
                return True
            if (now - state['last_failure_time']) > self.recovery_timeout:
                state['state'] = 'HALF_OPEN'
                log.info("Circuit breaker HALF_OPEN for %s", domain)
                return True
            return False

    async def record_success(self, domain: str) -> None:
        async with self._lock:
            state = self._get_state(domain)
            if state['state'] == 'HALF_OPEN':
                state['state'] = 'CLOSED'
                state['failure_count'] = 0
            elif state['failure_count'] > 0:
                state['failure_count'] -= 1

    async def record_failure(self, domain: str, error_type: str) -> None:
        async with self._lock:
            state = self._get_state(domain)
            state['failure_count'] += 1
            state['last_failure_time'] = time.time()
            state['error_types'][error_type] += 1
            if state['failure_count'] >= self.failure_threshold:
                state['state'] = 'OPEN'
                log.warning(
                    "Circuit breaker OPEN for %s (%d)",
                    domain,
                    state['failure_count'],
                )

    def _get_state(self, domain: str) -> Dict[str, Any]:
        if domain not in self._states:
            self._states[domain] = {
                'state': 'CLOSED',
                'failure_count': 0,
                'last_failure_time': 0.0,
                'error_types': Counter(),
            }
        return self._states[domain]

    async def get_stats(self) -> Dict[str, Any]:
        async with self._lock:
            open_c = sum(
                1 for s in self._states.values() if s['state'] == 'OPEN'
            )
            half = sum(
                1 for s in self._states.values() if s['state'] == 'HALF_OPEN'
            )
            return {
                'domains_tracked': len(self._states),
                'open_circuits': open_c,
                'half_open_circuits': half,
                'total_failures': sum(
                    s['failure_count'] for s in self._states.values()
                ),
                'domain_details': {
                    d: {
                        'state': s['state'],
                        'failure_count': s['failure_count'],
                        'error_types': dict(s['error_types']),
                    }
                    for d, s in self._states.items()
                },
            }


class AsyncTokenBucket:
    def __init__(self, capacity: float, refill_rate: float):
        self.capacity = capacity
        self.refill_rate = refill_rate
        self.tokens = capacity
        self.last_refill = time.time()
        self._lock = asyncio.Lock()
        self.stats = {
            'requests_allowed': 0,
            'requests_delayed': 0,
            'total_wait_time': 0.0,
        }

    async def consume(self, tokens: float = 1.0) -> None:
        async with self._lock:
            await self._refill()
            if self.tokens >= tokens:
                self.tokens -= tokens
                self.stats['requests_allowed'] += 1
                return
            needed = tokens - self.tokens
            wait = needed / self.refill_rate
            self.stats['requests_delayed'] += 1
            self.stats['total_wait_time'] += wait
        await asyncio.sleep(wait)
        async with self._lock:
            await self._refill()
            self.tokens = max(0, self.tokens - tokens)
            self.stats['requests_allowed'] += 1

    async def _refill(self) -> None:
        now = time.time()
        elapsed = now - self.last_refill
        self.tokens = min(
            self.capacity, self.tokens + elapsed * self.refill_rate
        )
        self.last_refill = now


class AsyncSessionManager:
    def __init__(self, max_sessions: int = 100):
        self.max_sessions = max_sessions
        self._sessions: Dict[str, ClientSession] = {}
        self._stats: Dict[str, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()

    async def get_session(self, domain: str) -> ClientSession:
        async with self._lock:
            existing = self._sessions.get(domain)
            if existing and not existing.closed:
                st = self._stats[domain]
                st['last_access'] = time.time()
                st['request_count'] += 1
                return existing
            if existing and existing.closed:
                del self._sessions[domain]
                self._stats.pop(domain, None)
            connector = TCPConnector(
                limit=30,
                limit_per_host=10,
                ttl_dns_cache=300,
                use_dns_cache=True,
                ssl=not config.insecure_ssl,
                keepalive_timeout=30,
            )
            timeout = ClientTimeout(
                total=config.request_timeout[0] + config.request_timeout[1],
                connect=config.request_timeout[0],
                sock_read=config.request_timeout[1],
            )
            session = ClientSession(
                connector=connector,
                timeout=timeout,
                headers={'User-Agent': self._get_user_agent()},
                auto_decompress=True,
            )
            self._sessions[domain] = session
            self._stats[domain] = {
                'created': time.time(),
                'last_access': time.time(),
                'request_count': 0,
            }
            return session

    def _get_user_agent(self) -> str:
        if hasattr(config, 'user_agents') and config.user_agents:
            return random.choice(config.user_agents)
        return 'THE_Email_Scraper/1.0 Async'

    async def close_all(self) -> None:
        async with self._lock:
            for s in list(self._sessions.values()):
                if not s.closed:
                    await s.close()
            self._sessions.clear()
            self._stats.clear()

    async def cleanup_stale_sessions(self, max_age: float = 3600) -> None:
        now = time.time()
        stale = []
        async with self._lock:
            for d, st in self._stats.items():
                if (now - st['last_access']) > max_age:
                    stale.append(d)
            for d in stale:
                sess = self._sessions.get(d)
                if sess and not sess.closed:
                    await sess.close()
                self._sessions.pop(d, None)
                self._stats.pop(d, None)

    async def get_stats(self) -> Dict[str, Any]:
        async with self._lock:
            total_requests = sum(
                st['request_count'] for st in self._stats.values()
            )
            return {
                'active_sessions': len(self._sessions),
                'total_requests': total_requests,
                'max_sessions': self.max_sessions,
                'session_details': dict(self._stats),
            }
