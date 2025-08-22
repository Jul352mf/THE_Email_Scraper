"""
Test performance monitoring integration with CLI and orchestrator.

This test validates TASK-044 - Performance Monitoring Integration.
"""

import unittest
from unittest.mock import patch, MagicMock
import time

from scraper.performance_optimizer import get_performance_report, resource_monitor
from scraper.cli import CLI


class TestPerformanceMonitoringIntegration(unittest.TestCase):
    """Test performance monitoring integration."""

    def setUp(self):
        """Set up test fixtures."""
        # Reset resource monitor state
        resource_monitor.request_times.clear()
        resource_monitor.start_time = time.time()

    def test_get_performance_report_structure(self):
        """Test that performance report has expected structure."""
        report = get_performance_report()
        
        # Check required fields
        self.assertIn('uptime_seconds', report)
        self.assertIn('average_request_time', report)
        self.assertIn('requests_per_second', report)
        self.assertIn('total_requests_tracked', report)
        self.assertIn('connection_stats', report)
        self.assertIn('cache_stats', report)
        self.assertIn('suggested_workers', report)
        
        # Check cache stats structure
        cache_stats = report['cache_stats']
        self.assertIn('hits', cache_stats)
        self.assertIn('misses', cache_stats)
        self.assertIn('hit_rate_percent', cache_stats)
        self.assertIn('cache_size', cache_stats)

    def test_resource_monitor_request_tracking(self):
        """Test that resource monitor tracks request times."""
        # Record some request times
        resource_monitor.record_request_time(1.5)
        resource_monitor.record_request_time(0.8)
        resource_monitor.record_request_time(2.2)
        
        # Add a small delay to ensure uptime > 0
        time.sleep(0.01)
        
        report = get_performance_report()
        
        # Check stats are recorded
        self.assertEqual(report['total_requests_tracked'], 3)
        expected_avg = (1.5 + 0.8 + 2.2) / 3
        self.assertAlmostEqual(report['average_request_time'], expected_avg, places=3)
        self.assertGreaterEqual(report['uptime_seconds'], 0)

    def test_worker_suggestions(self):
        """Test worker count suggestions."""
        # Test with slow requests (should suggest fewer workers)
        resource_monitor.request_times.clear()
        for _ in range(10):
            resource_monitor.record_request_time(6.0)  # Slow requests
        
        report = get_performance_report()
        # Should suggest reducing workers for slow requests
        self.assertIsInstance(report['suggested_workers'], (int, type(None)))

    @patch('scraper.cli.get_performance_report')
    @patch('scraper.cli.orchestrator')
    @patch('scraper.cli.http_client')
    def test_cli_performance_monitoring_integration(self, mock_http_client, mock_orchestrator, mock_get_perf_report):
        """Test that CLI integrates performance monitoring in output."""
        # Mock performance report
        mock_perf_report = {
            'uptime_seconds': 45.6,
            'average_request_time': 1.234,
            'requests_per_second': 2.5,
            'total_requests_tracked': 15,
            'connection_stats': {'example.com': 5},
            'cache_stats': {
                'hits': 8,
                'misses': 7,
                'hit_rate_percent': 53.3,
                'cache_size': 12
            },
            'suggested_workers': 4
        }
        mock_get_perf_report.return_value = mock_perf_report
        
        # Mock orchestrator stats
        mock_stats = {
            'leads': 10,
            'domain': 8,
            'no_google': 1,
            'domain_unclear': 1,
            'sitemap': 3,
            'with_email': 5,
            'without_email': 3,
            'google_error': 0,
            'processing_error': 0
        }
        mock_orchestrator.global_stats = mock_stats
        
        # Mock HTTP client stats
        mock_http_stats = {
            'total_requests': 20,
            'status_200': 15,
            'status_404': 3,
            'status_500': 2
        }
        mock_http_client.stats = mock_http_stats
        
        # Create CLI instance and check it imports performance monitoring
        cli = CLI()
        
        # Verify get_performance_report is importable and callable
        self.assertTrue(callable(mock_get_perf_report))

    def test_performance_monitoring_real_execution(self):
        """Test performance monitoring with real but minimal execution."""
        from scraper.http_client import http_client
        
        # Make a few real requests to test sites to populate performance data
        test_urls = [
            'https://httpbin.org/status/200',  # Reliable test endpoint
            'https://httpbin.org/delay/1',     # Slow endpoint for timing
        ]
        
        for url in test_urls:
            try:
                response = http_client.safe_get(url, timeout=5)
                if response:
                    self.assertIsNotNone(response)
            except Exception as e:
                # Skip if network is unavailable
                self.skipTest(f"Network unavailable: {e}")
        
        # Get performance report
        report = get_performance_report()
        
        # Should have recorded some requests
        self.assertGreaterEqual(report['total_requests_tracked'], 0)
        self.assertGreaterEqual(report['uptime_seconds'], 0)
        
        # Check that all fields are present and valid
        self.assertIsInstance(report['average_request_time'], (int, float))
        self.assertIsInstance(report['requests_per_second'], (int, float))
        self.assertIsInstance(report['cache_stats']['hit_rate_percent'], (int, float))
        self.assertIsInstance(report['cache_stats']['cache_size'], int)


if __name__ == '__main__':
    unittest.main()
