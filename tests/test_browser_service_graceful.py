#!/usr/bin/env python3
"""
Test for BrowserService graceful degradation when Playwright is unavailable.
"""

import logging
import sys
import time
from unittest.mock import patch, MagicMock

# Set up logging for test
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


def test_graceful_degradation_no_playwright():
    """
    Test that BrowserService gracefully handles missing Playwright.
    """
    log.info("Testing graceful degradation when Playwright unavailable")
    
    # Mock at the import level - remove playwright modules temporarily
    playwright_modules_to_mock = [
        'playwright', 'playwright.sync_api'
    ]
    
    original_modules = {}
    for mod in playwright_modules_to_mock:
        if mod in sys.modules:
            original_modules[mod] = sys.modules[mod]
            del sys.modules[mod]
    
    # Mock the modules to None
    with patch.dict(
        'sys.modules', {mod: None for mod in playwright_modules_to_mock}
    ):
        # Re-import the module so it picks up the mock
        if 'scraper.browser_service' in sys.modules:
            del sys.modules['scraper.browser_service']
            
        from scraper.browser_service import BrowserService
        
        # Create browser service
        browser_service = BrowserService()
        browser_service.start()
        
        # Give it a moment to start
        time.sleep(0.5)
        
        try:
            # Try to render a page - should return empty string quickly
            start_time = time.time()
            result = browser_service.render("https://example.com", timeout=3)
            end_time = time.time()
            
            assert result == "", f"Expected empty string, got: {result}"
            response_time = end_time - start_time
            assert response_time < 2, "Response took too long"
            log.info(
                "✓ Graceful degradation working - "
                "returned empty string quickly"
            )
            
        finally:
            browser_service.shutdown()
            browser_service.join(timeout=2)
    
    # Restore original modules
    for mod, original in original_modules.items():
        sys.modules[mod] = original
        
    log.info("✓ Test passed: graceful degradation works correctly")


def test_graceful_degradation_with_working_playwright():
    """
    Test that the service works correctly when Playwright IS available.
    This also tests that our graceful error handling works for network errors.
    """
    log.info("Testing graceful error handling with working Playwright")
    
    # Import normally (don't mock Playwright)
    from scraper.browser_service import BrowserService
    
    # Create browser service
    browser_service = BrowserService()
    browser_service.start()
    
    # Give it a moment to start
    time.sleep(0.5)
    
    try:
        # Try to render a page that will fail (network error)
        # This tests that network errors are handled gracefully
        invalid_domain = "https://nonexistent.domain.invalid"
        result = browser_service.render(invalid_domain, timeout=3)
        expected_msg = (
            f"Expected empty string for invalid domain, got: {result}"
        )
        assert result == "", expected_msg
        log.info("✓ Network error handled gracefully")
        
    finally:
        browser_service.shutdown()
        browser_service.join(timeout=2)
    
    log.info("✓ Test passed: network error handling works correctly")


def test_graceful_degradation_playwright_launch_fails():
    """
    Test that BrowserService handles Playwright launch failures gracefully.
    """
    log.info("Testing graceful degradation when browser launch fails")
    
    # Mock sync_playwright to raise an exception when called
    def mock_sync_playwright():
        mock = MagicMock()
        mock.start.side_effect = Exception("Browser launch failed")
        return mock
    
    with patch(
        'scraper.browser_service.sync_playwright', mock_sync_playwright
    ):
        # Re-import to pick up the mock
        if 'scraper.browser_service' in sys.modules:
            del sys.modules['scraper.browser_service']
            
        from scraper.browser_service import BrowserService
        
        # Create browser service
        browser_service = BrowserService()
        browser_service.start()
        
        # Give it a moment to start and handle the error
        time.sleep(1.0)  # More time for error handling
        
        try:
            # Try to render a page - should return empty string
            result = browser_service.render("https://example.com", timeout=3)
            assert result == "", f"Expected empty string, got: {result}"
            log.info("✓ Launch failure handled gracefully")
            
        finally:
            browser_service.shutdown()
            browser_service.join(timeout=2)
    
    log.info("✓ Test passed: launch failure handled correctly")


if __name__ == "__main__":
    test_graceful_degradation_no_playwright()
    test_graceful_degradation_with_working_playwright() 
    test_graceful_degradation_playwright_launch_fails()
    print("🎉 All browser service graceful degradation tests passed!")
