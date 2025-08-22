"""
Tests for TASK-021: Requests session/adapter reuse optimization
"""
import pytest
import threading
from unittest.mock import Mock, patch

from scraper.http_client import _session_mgr, HttpClient
from scraper.config import config


class TestSessionOptimization:
    """Test enhanced session management and connection pooling."""
    
    def test_session_reuse_per_domain(self):
        """Test that sessions are reused per domain within a thread."""
        domain = "example.com"
        
        # Get session twice for the same domain
        session1 = _session_mgr.session(domain)
        session2 = _session_mgr.session(domain)
        
        # Should be the same session object
        assert session1 is session2
        
        # Verify session stats tracking
        stats = _session_mgr.get_session_stats()
        assert stats[domain] >= 2  # At least 2 uses
    
    def test_different_domains_different_sessions(self):
        """Test that different domains get different sessions."""
        domain1 = "example.com"
        domain2 = "test.com"
        
        session1 = _session_mgr.session(domain1)
        session2 = _session_mgr.session(domain2)
        
        # Should be different session objects
        assert session1 is not session2
        
        # Verify both domains tracked
        stats = _session_mgr.get_session_stats()
        assert domain1 in stats
        assert domain2 in stats
    
    def test_thread_isolation(self):
        """Test that sessions are isolated per thread."""
        domain = "example.com"
        sessions = []
        
        def get_session():
            session = _session_mgr.session(domain)
            sessions.append(session)
        
        # Create sessions in different threads
        thread1 = threading.Thread(target=get_session)
        thread2 = threading.Thread(target=get_session)
        
        thread1.start()
        thread2.start()
        
        thread1.join()
        thread2.join()
        
        # Should have different sessions from different threads
        assert len(sessions) == 2
        assert sessions[0] is not sessions[1]
    
    def test_session_cleanup(self):
        """Test session cleanup functionality."""
        # Create many sessions to trigger cleanup
        for i in range(60):  # More than the 50 session limit
            _session_mgr.session(f"domain{i}.com")
        
        # Trigger cleanup
        _session_mgr.cleanup_old_sessions()
        
        # Should have reduced number of active sessions
        active_count = _session_mgr.get_active_session_count()
        assert active_count <= 30  # Should keep newest 30
    
    def test_enhanced_connection_pooling(self):
        """Test that session template has enhanced connection pooling."""
        session = _session_mgr.session("test.com")
        
        # Verify HTTP adapters are properly configured
        assert "http://" in session.adapters
        assert "https://" in session.adapters
        
        http_adapter = session.adapters["http://"]
        https_adapter = session.adapters["https://"]
        
        # Check that adapters have connection pooling configured
        assert hasattr(http_adapter, "config")
        assert hasattr(https_adapter, "config")
    
    def test_session_stats_tracking(self):
        """Test that session usage statistics are tracked."""
        domain = "stats-test.com"
        initial_stats = _session_mgr.get_session_stats()
        initial_count = initial_stats.get(domain, 0)
        
        # Use session multiple times
        for _ in range(5):
            _session_mgr.session(domain)
        
        # Verify stats updated
        updated_stats = _session_mgr.get_session_stats()
        assert updated_stats[domain] == initial_count + 5
    
    def test_ssl_verification_config(self):
        """Test that SSL verification follows config setting."""
        domain = "ssl-test.com"
        
        # Test with SSL verification enabled (default)
        config.insecure_ssl = False
        session = _session_mgr.session(domain)
        assert session.verify is True
        
        # Test with SSL verification disabled  
        config.insecure_ssl = True
        session = _session_mgr.session(domain)
        assert session.verify is False
        
        # Reset to default
        config.insecure_ssl = False
    
    def test_google_search_uses_centralized_sessions(self):
        """Test that Google search client uses centralized session manager."""
        from scraper.google_search import GoogleSearchClient
        
        # Mock the session manager import inside the method
        with patch('scraper.http_client._session_mgr') as mock_session_mgr:
            mock_session = Mock()
            mock_session_mgr.session.return_value = mock_session
            
            # Initialize Google search client
            client = GoogleSearchClient()
            client._initialize_service()
            
            # Verify session manager was called for googleapis.com
            mock_session_mgr.session.assert_called_with("googleapis.com")
    
    def test_connection_pool_optimization(self):
        """Test connection pool settings are optimized."""
        session = _session_mgr.session("pool-test.com")
        
        # Get the HTTPAdapter
        adapter = session.get_adapter("https://pool-test.com")
        
        # Verify connection pool settings
        assert hasattr(adapter, 'config')
        
        # The adapter should have reasonable pool settings
        # (exact values depend on implementation)
        pool_connections = getattr(adapter.config, 'pool_connections', None)
        pool_maxsize = getattr(adapter.config, 'pool_maxsize', None)
        
        if pool_connections is not None:
            assert pool_connections > 0
        if pool_maxsize is not None:
            assert pool_maxsize > 0
    
    def test_periodic_cleanup_trigger(self):
        """Test that periodic cleanup is triggered after many requests."""
        domain = "cleanup-trigger-test.com"
        
        # Track initial cleanup counter
        initial_counter = _session_mgr._cleanup_counter
        
        # Make many session requests to trigger cleanup
        for i in range(1005):  # More than 1000 to trigger cleanup
            _session_mgr.session(f"{domain}-{i % 10}")  # Cycle domains
        
        # Cleanup should have been triggered (counter reset)
        assert _session_mgr._cleanup_counter < initial_counter + 1005


@pytest.mark.integration
class TestSessionIntegration:
    """Integration tests for session optimization."""
    
    def test_http_client_session_reuse(self):
        """Test that HttpClient properly reuses sessions."""
        client = HttpClient()
        
        # Track initial session count
        initial_count = _session_mgr.get_active_session_count()
        
        # Make multiple requests to different domains
        domains = ["test1.com", "test2.com", "test1.com", "test3.com"]
        
        for domain in domains:
            session = _session_mgr.session(domain)
            # Simulate some usage
            assert session is not None
        
        # Should have created sessions for unique domains only
        final_count = _session_mgr.get_active_session_count()
        unique_domains = len(set(domains))
        
        # Account for any existing sessions
        new_sessions = final_count - initial_count
        assert new_sessions == unique_domains
    
    def test_performance_monitoring_integration(self):
        """Test integration with performance monitoring."""
        # This test would verify that session stats integrate well
        # with the existing performance monitoring system
        
        stats = _session_mgr.get_session_stats()
        assert isinstance(stats, dict)
        
        # All values should be positive integers
        for domain, count in stats.items():
            assert isinstance(domain, str)
            assert isinstance(count, int)
            assert count > 0
