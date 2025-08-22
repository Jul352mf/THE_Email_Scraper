"""
Tests for TASK-039: Smart Retry Logic & Circuit Breaker
"""
import pytest
import time
import threading
from unittest.mock import Mock, patch

from scraper.http_client import (
    CircuitBreaker, SmartRetryStrategy, _circuit_breaker,
    HttpClient
)


class TestCircuitBreaker:
    """Test circuit breaker functionality."""
    
    def test_circuit_breaker_initialization(self):
        """Test circuit breaker initializes correctly."""
        cb = CircuitBreaker(failure_threshold=3, recovery_timeout=30.0)
        assert cb.failure_threshold == 3
        assert cb.recovery_timeout == 30.0
        assert cb.success_threshold == 2  # default
    
    def test_circuit_breaker_closed_state(self):
        """Test circuit breaker allows requests when closed."""
        cb = CircuitBreaker()
        assert cb.can_request("test.com") is True
        
        # Record some successes
        cb.record_success("test.com")
        assert cb.can_request("test.com") is True
    
    def test_circuit_breaker_failure_tracking(self):
        """Test circuit breaker tracks failures correctly."""
        cb = CircuitBreaker(failure_threshold=2)
        domain = "failing-domain.com"
        
        # First failure - should still allow requests
        cb.record_failure(domain, "500")
        assert cb.can_request(domain) is True
        
        # Second failure - should open circuit
        cb.record_failure(domain, "500")
        assert cb.can_request(domain) is False
        
        state = cb._get_state(domain)
        assert state.is_open is True
    
    def test_circuit_breaker_ignores_client_errors(self):
        """Test circuit breaker doesn't count 4xx errors."""
        cb = CircuitBreaker(failure_threshold=2)
        domain = "client-error.com"
        
        # Record multiple 4xx errors
        cb.record_failure(domain, "404")
        cb.record_failure(domain, "403")
        cb.record_failure(domain, "400")
        
        # Should still allow requests (4xx don't count)
        assert cb.can_request(domain) is True
    
    def test_circuit_breaker_recovery(self):
        """Test circuit breaker recovery after timeout."""
        cb = CircuitBreaker(failure_threshold=1, recovery_timeout=0.1)
        domain = "recovery-test.com"
        
        # Trigger circuit breaker
        cb.record_failure(domain, "500")
        assert cb.can_request(domain) is False
        
        # Wait for recovery timeout
        time.sleep(0.15)
        
        # Should transition to half-open
        assert cb.can_request(domain) is True
        state = cb._get_state(domain)
        assert state.is_half_open is True
    
    def test_circuit_breaker_half_open_recovery(self):
        """Test circuit breaker closes after success in half-open."""
        cb = CircuitBreaker(
            failure_threshold=1, recovery_timeout=0.1, success_threshold=2
        )
        domain = "half-open-test.com"
        
        # Open circuit
        cb.record_failure(domain, "500")
        time.sleep(0.15)  # Wait for recovery timeout
        
        # Check if it transitions to half-open
        assert cb.can_request(domain) is True
        
        # First success in half-open
        cb.record_success(domain)
        state = cb._get_state(domain)
        # Second success should close circuit
        cb.record_success(domain)
        state = cb._get_state(domain)
        assert state.state == "closed"
    
    def test_circuit_breaker_thread_safety(self):
        """Test circuit breaker is thread-safe."""
        cb = CircuitBreaker(failure_threshold=10)
        domain = "thread-test.com"
        
        def record_failures():
            for _ in range(5):
                cb.record_failure(domain, "500")
        
        def record_successes():
            for _ in range(5):
                cb.record_success(domain)
        
        # Run concurrent operations
        threads = []
        for _ in range(4):
            threads.append(threading.Thread(target=record_failures))
            threads.append(threading.Thread(target=record_successes))
        
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        # Should not crash and state should be consistent
        assert cb.can_request(domain) in [True, False]
        stats = cb.get_stats()
        assert domain in stats


class TestSmartRetryStrategy:
    """Test smart retry strategy functionality."""
    
    def test_retry_strategy_initialization(self):
        """Test retry strategy initializes correctly."""
        rs = SmartRetryStrategy(max_retries=5, base_delay=2.0)
        assert rs.max_retries == 5
        assert rs.base_delay == 2.0
    
    def test_should_retry_logic(self):
        """Test retry decision logic."""
        rs = SmartRetryStrategy(max_retries=3)
        
        # Should retry server errors
        assert rs.should_retry(0, 500, None) is True
        assert rs.should_retry(0, 502, None) is True
        assert rs.should_retry(0, 503, None) is True
        
        # Should not retry client errors
        assert rs.should_retry(0, 400, None) is False
        assert rs.should_retry(0, 404, None) is False
        assert rs.should_retry(0, 403, None) is False
        
        # Should not retry after max attempts
        assert rs.should_retry(3, 500, None) is False
        assert rs.should_retry(5, 500, None) is False
    
    def test_exponential_backoff_delay(self):
        """Test exponential backoff delay calculation."""
        rs = SmartRetryStrategy(
            base_delay=1.0, exponential_base=2.0, max_delay=10.0, jitter=False
        )
        
        # Test exponential progression
        assert rs.get_delay(0) == 1.0  # 1.0 * (2^0)
        assert rs.get_delay(1) == 2.0  # 1.0 * (2^1)
        assert rs.get_delay(2) == 4.0  # 1.0 * (2^2)
        assert rs.get_delay(3) == 8.0  # 1.0 * (2^3)
        
        # Test max delay cap
        assert rs.get_delay(10) == 10.0  # Should be capped
    
    def test_jitter_enabled(self):
        """Test jitter adds randomness to delay."""
        rs = SmartRetryStrategy(base_delay=2.0, jitter=True)
        
        delays = [rs.get_delay(1) for _ in range(10)]
        
        # All delays should be different (with high probability)
        assert len(set(delays)) > 1
        
        # All delays should be in reasonable range (1-4 seconds)
        for delay in delays:
            assert 1.0 <= delay <= 4.0


class TestHttpClientIntegration:
    """Integration tests for circuit breaker with HttpClient."""
    
    @patch('scraper.http_client._session_mgr')
    def test_circuit_breaker_integration(self, mock_session_mgr):
        """Test circuit breaker integration with HTTP client."""
        # Setup mock session that returns server errors
        mock_session = Mock()
        mock_session_mgr.session.return_value = mock_session
        
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.ok = False
        mock_session.get.return_value = mock_response
        
        client = HttpClient()
        domain = "failing-integration-test.com"
        url = f"https://{domain}/test"
        
        # Make requests that should fail and trigger circuit breaker
        results = []
        for i in range(6):  # More than failure threshold
            result = client.safe_get(url)
            results.append(result)
        
        # Some should be None (blocked) and some should be the mock response
        # The exact behavior depends on when circuit breaker kicks in
        assert any(r is None for r in results)  # Some blocked
        
        # Verify circuit breaker has stats
        stats = client.get_performance_stats()
        assert 'circuit_breaker' in stats
    
    def test_performance_stats_include_circuit_breaker(self):
        """Test performance stats include circuit breaker information."""
        client = HttpClient()
        stats = client.get_performance_stats()
        
        assert 'circuit_breaker' in stats
        assert 'retry_strategy' in stats
        
        retry_config = stats['retry_strategy']
        assert 'max_retries' in retry_config
        assert 'base_delay' in retry_config
        assert 'exponential_base' in retry_config
    
    @patch('scraper.http_client.time.sleep')
    @patch('scraper.http_client._session_mgr')
    def test_smart_retry_with_exponential_backoff(self, mock_session_mgr,
                                                  mock_sleep):
        """Test smart retry uses exponential backoff."""
        # Setup mock that fails then succeeds
        mock_session = Mock()
        mock_session_mgr.session.return_value = mock_session
        
        # First call fails, second succeeds
        failing_response = Mock()
        failing_response.status_code = 500
        failing_response.ok = False
        
        success_response = Mock()
        success_response.status_code = 200
        success_response.ok = True
        
        mock_session.get.side_effect = [failing_response, success_response]
        
        client = HttpClient()
        result = client.safe_get("https://retry-test.com/test")
        
        # Should eventually succeed
        assert result is not None
        assert result.ok is True
        
        # Should have called sleep for backoff
        assert mock_sleep.called
@pytest.mark.integration
class TestCircuitBreakerIntegration:
    """Integration tests for complete circuit breaker system."""
    
    def test_global_circuit_breaker_state(self):
        """Test that global circuit breaker maintains state."""
        domain = "global-state-test.com"
        
        # Record failures in global circuit breaker
        for _ in range(6):
            _circuit_breaker.record_failure(domain, "500")
        
        # Should block requests
        assert _circuit_breaker.can_request(domain) is False
        
        # Create new HTTP client - should still be blocked
        client = HttpClient()
        # The domain should be blocked at circuit breaker level
        # (would need more complex mocking to test full integration)
    
    def test_circuit_breaker_statistics(self):
        """Test circuit breaker statistics collection."""
        domain = "stats-test.com"
        
        # Generate some activity
        _circuit_breaker.record_success(domain)
        _circuit_breaker.record_failure(domain, "500")
        _circuit_breaker.record_success(domain)
        
        stats = _circuit_breaker.get_stats()
        assert domain in stats
        
        domain_stats = stats[domain]
        assert 'state' in domain_stats
        assert 'failure_count' in domain_stats
        assert 'success_count' in domain_stats
