"""
Test suite for the scraper package.

This module provides comprehensive tests for all components of the scraper package.
"""

import os
import unittest
from unittest.mock import patch, MagicMock

from scraper.config import config
from scraper.http import http_client, validate_url, normalise_domain
from scraper.email_extractor import email_extractor
from scraper.google_search import google_client
from scraper.domain_scorer import domain_scorer
from scraper.sitemap import sitemap_parser
from scraper.crawler import crawler
from scraper.orchestrator import orchestrator
from scraper.cache import google_cache, domain_score_cache
from scraper.worker import WorkerPool, BatchProcessor
from scraper.progress import ProgressTracker
from scraper.google_fallback import google_fallback
from scraper.proxy import proxy_manager
from scraper.file_updater import file_updater
from scraper.rate_limiter import rate_limiter

class TestConfig(unittest.TestCase):
    """Tests for the configuration module."""
    
    def test_config_defaults(self):
        """Test default configuration values."""
        self.assertIsNotNone(config.priority_parts)
        self.assertGreater(len(config.priority_parts), 0)
        self.assertGreater(config.max_fallback_pages, 0)
        self.assertGreater(config.max_workers, 0)
        self.assertGreater(config.domain_score_threshold, 0)
        self.assertGreater(config.max_redirects, 0)
        self.assertGreater(config.max_url_length, 0)
        self.assertIsInstance(config.request_timeout, tuple)
        self.assertEqual(len(config.request_timeout), 2)
    
    def test_config_validation(self):
        """Test configuration validation."""
        # Save original values
        original_api_key = config.api_key
        original_cx_id = config.cx_id
        
        try:
            # Test with missing API key
            config.api_key = ""
            errors = config.validate()
            self.assertIn("GOOGLE_API_KEY is missing", errors)
            
            # Test with missing CX ID
            config.api_key = "test_key"
            config.cx_id = ""
            errors = config.validate()
            self.assertIn("GOOGLE_CX_ID is missing", errors)
            
            # Test with valid configuration
            config.api_key = "test_key"
            config.cx_id = "test_cx"
            errors = config.validate()
            self.assertEqual(len(errors), 0)
            
        finally:
            # Restore original values
            config.api_key = original_api_key
            config.cx_id = original_cx_id

class TestHttp(unittest.TestCase):
    """Tests for the HTTP module."""
    
    def test_validate_url(self):
        """Test URL validation."""
        # Valid URLs
        self.assertTrue(validate_url("https://example.com"))
        self.assertTrue(validate_url("http://example.com/path"))
        self.assertTrue(validate_url("https://sub.example.com/path?query=value"))
        
        # Invalid URLs
        self.assertFalse(validate_url(""))
        self.assertFalse(validate_url("example.com"))  # Missing scheme
        self.assertFalse(validate_url("https://"))  # Missing host
        self.assertFalse(validate_url("ftp://example.com"))  # Non-HTTP scheme
        
        # URL too long
        long_url = "https://example.com/" + "a" * config.max_url_length
        self.assertFalse(validate_url(long_url))
    
    def test_normalise_domain(self):
        """Test domain normalization."""
        self.assertEqual(normalise_domain("example.com"), "example.com")
        self.assertEqual(normalise_domain("www.example.com"), "example.com")
        self.assertEqual(normalise_domain("https://www.example.com"), "example.com")
        self.assertEqual(normalise_domain("http://www.example.com/path"), "example.com")
        self.assertEqual(normalise_domain("EXAMPLE.COM"), "example.com")
    
    @patch('requests.Session.get')
    def test_safe_get(self, mock_get):
        """Test safe_get method."""
        # Mock successful response
        mock_response = MagicMock()
        mock_response.ok = True
        mock_response.status_code = 200
        mock_response.url = "https://example.com"
        mock_get.return_value = mock_response
        
        # Test successful request
        response = http_client.safe_get("https://example.com")
        self.assertIsNotNone(response)
        self.assertTrue(response.ok)
        
        # Test with invalid URL
        response = http_client.safe_get("invalid-url")
        self.assertIsNone(response)
        
        # Reset mock for the next test
        mock_get.side_effect = None
        mock_get.return_value = mock_response
        
        # Create a separate test for exception handling
        
    @patch('requests.Session.get')
    def test_safe_get_with_exception(self, mock_get):
        """Test safe_get method with exception."""
        # Mock exception
        mock_get.side_effect = Exception("Test exception")
        
        # Test with request exception
        response = http_client.safe_get("https://example.com")
        self.assertIsNone(response)

class TestEmailExtractor(unittest.TestCase):
    """Tests for the email extractor module."""
    
    def test_clean_email(self):
        """Test email cleaning."""
        self.assertEqual(email_extractor.clean_email("test@example.com"), "test@example.com")
        self.assertEqual(email_extractor.clean_email("TEST@EXAMPLE.COM"), "test@example.com")
        self.assertEqual(email_extractor.clean_email("test@example.com?subject=Test"), "test@example.com")
    
    def test_extract_from_html(self):
        """Test email extraction from HTML."""
        html = """
        <html>
        <body>
            <p>Contact us at test@example.com or sales@example.com</p>
            <a href="mailto:info@example.com">Email us</a>
        </body>
        </html>
        """
        
        emails = email_extractor.extract_from_html(html)
        self.assertEqual(len(emails), 3)
        self.assertIn("test@example.com", emails)
        self.assertIn("sales@example.com", emails)
        self.assertIn("info@example.com", emails)
    
    def test_is_valid_email(self):
        """Test email validation."""
        # Valid emails
        self.assertTrue(email_extractor.is_valid_email("test@example.com"))
        self.assertTrue(email_extractor.is_valid_email("test.name@example.co.uk"))
        self.assertTrue(email_extractor.is_valid_email("test+tag@example.com"))
        
        # Invalid emails
        self.assertFalse(email_extractor.is_valid_email(""))
        self.assertFalse(email_extractor.is_valid_email("test"))
        self.assertFalse(email_extractor.is_valid_email("test@"))
        self.assertFalse(email_extractor.is_valid_email("@example.com"))
        self.assertFalse(email_extractor.is_valid_email("test@example"))

class TestDomainScorer(unittest.TestCase):
    """Tests for the domain scorer module."""
    
    def test_clean_company_name(self):
        """Test company name cleaning."""
        self.assertEqual(domain_scorer.clean_company_name("Example Inc."), "example")
        self.assertEqual(domain_scorer.clean_company_name("Example Ltd"), "example")
        self.assertEqual(domain_scorer.clean_company_name("Example GmbH"), "example")
        self.assertEqual(domain_scorer.clean_company_name("Example-Company"), "examplecompany")
    
    def test_score_domain(self):
        """Test domain scoring."""
        # High score for exact match
        score = domain_scorer.score_domain("Example", "example.com")
        self.assertGreaterEqual(score, 80)
        
        # Lower score for partial match
        score = domain_scorer.score_domain("Example Company", "example.com")
        self.assertGreaterEqual(score, 70)
        
        # Lower score for social media domains
        score = domain_scorer.score_domain("Example", "linkedin.com/company/example")
        self.assertLess(score, 80)
        
        # Zero score for unrelated domain
        score = domain_scorer.score_domain("Example", "unrelated.com")
        self.assertLess(score, 50)
    
    def test_is_domain_relevant(self):
        """Test domain relevance check."""
        # Save original threshold
        original_threshold = config.domain_score_threshold
        
        try:
            # Set threshold for testing
            config.domain_score_threshold = 70
            
            # Test relevant domain
            self.assertTrue(domain_scorer.is_domain_relevant("Example", "example.com"))
            
            # Test irrelevant domain
            self.assertFalse(domain_scorer.is_domain_relevant("Example", "unrelated.com"))
            
        finally:
            # Restore original threshold
            config.domain_score_threshold = original_threshold

class TestWorkerPool(unittest.TestCase):
    """Tests for the worker pool module."""
    
    def test_worker_pool(self):
        """Test worker pool functionality."""
        # Create a worker pool
        def process_task(task):
            return task * 2
            
        pool = WorkerPool(worker_count=2, task_processor=process_task)
        
        # Add tasks
        for i in range(5):
            pool.add_task(i)
        
        # Start processing
        pool.start()
        
        # Wait for completion
        pool.wait()
        
        # Get results
        results = pool.get_results()
        
        # Verify results
        self.assertEqual(len(results), 5)
        for result in results:
            self.assertTrue(result.success)
            self.assertEqual(result.result, result.task * 2)
        
        # Stop pool
        pool.stop()
    
    def test_batch_processor(self):
        """Test batch processor functionality."""
        # Create a batch processor
        def process_task(task):
            return task * 2
            
        processor = BatchProcessor(processor=process_task, worker_count=2)
        
        # Create tasks
        tasks = list(range(10))
        
        # Process tasks
        results = processor.process(tasks)
        
        # Verify results
        self.assertEqual(len(results), 10)
        for result in results:
            self.assertTrue(result.success)
            self.assertEqual(result.result, result.task * 2)

class TestCache(unittest.TestCase):
    """Tests for the cache module."""
    
    def setUp(self):
        """Set up test environment."""
        # Clear caches
        google_cache.clear()
        domain_score_cache.clear()
    
    def test_cache_operations(self):
        """Test basic cache operations."""
        # Set a value
        google_cache.set("test_key", [{"title": "Test", "link": "https://example.com"}])
        
        # Get the value
        value = google_cache.get("test_key")
        self.assertIsNotNone(value)
        self.assertEqual(len(value), 1)
        self.assertEqual(value[0]["title"], "Test")
        
        # Delete the value
        google_cache.delete("test_key")
        
        # Verify it's gone
        value = google_cache.get("test_key")
        self.assertIsNone(value)
    
    def test_domain_score_cache(self):
        """Test domain score cache."""
        # Set a score
        domain_score_cache.set("example.com", 85)
        
        # Get the score
        score = domain_score_cache.get("example.com")
        self.assertIsNotNone(score)
        self.assertEqual(score, 85)

class TestProgressTracker(unittest.TestCase):
    """Tests for the progress tracker module."""
    
    def test_progress_tracker(self):
        """Test progress tracker functionality."""
        # Create a progress tracker
        tracker = ProgressTracker(total=10, description="Test", console=False)
        
        # Update progress
        for i in range(10):
            tracker.update()
            
            # Check stats
            stats = tracker.get_stats()
            self.assertEqual(stats["current"], i + 1)
            self.assertEqual(stats["total"], 10)
            self.assertAlmostEqual(stats["percent"], (i + 1) * 10, delta=0.1)
        
        # Verify completion
        stats = tracker.get_stats()
        self.assertTrue(stats["completed"])
        self.assertEqual(stats["current"], 10)
        self.assertEqual(stats["percent"], 100.0)

class TestRateLimiter(unittest.TestCase):
    """Tests for the rate limiter module."""
    
    def test_rate_limiter(self):
        """Test rate limiter functionality."""
        import time
        
        # Create a rate limiter with high rate (no actual waiting)
        limiter = rate_limiter
        limiter.default_rate = 1000.0  # 1000 requests per second
        
        # Set domain-specific rate
        limiter.set_rate("example.com", 500.0)
        
        # Test waiting
        start_time = time.time()
        limiter.wait()
        limiter.wait("example.com")
        end_time = time.time()
        
        # Verify minimal waiting (should be very fast)
        self.assertLess(end_time - start_time, 0.1)
        
        # Test execute with rate limit
        result = limiter.execute_with_rate_limit(lambda: 42)
        self.assertEqual(result, 42)
        
        # Test execute with exception
        with self.assertRaises(Exception):
            limiter.execute_with_rate_limit(lambda: 1/0, retry_count=1)

if __name__ == '__main__':
    unittest.main()
