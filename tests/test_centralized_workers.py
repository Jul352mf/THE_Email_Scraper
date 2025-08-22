"""
Test centralized worker management to ensure proper allocation and no nesting.

This test validates the new WorkerManager functionality introduced in TASK-020
to prevent thread pool explosion by centralizing worker count decisions.
"""
import pytest
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from scraper.config import Config, WorkerManager


def test_worker_manager_initialization():
    """Test WorkerManager initializes with correct base workers."""
    manager = WorkerManager(8)
    assert manager.base_workers == 8
    
    # Test minimum constraint
    manager = WorkerManager(0)
    assert manager.base_workers == 1


def test_worker_allocation_strategies():
    """Test different allocation strategies for different tasks."""
    manager = WorkerManager(8)
    
    # Domain probe should use fewer workers
    probe_workers = manager.get_workers_for_task("domain_probe", 10)
    assert probe_workers == 4  # min(8//2, 4) = 4
    
    # Sitemap download should be limited
    sitemap_workers = manager.get_workers_for_task("sitemap_download", 3)
    assert sitemap_workers == 3  # min(4, 3) = 3
    
    # Main processing gets most workers
    main_workers = manager.get_workers_for_task("main_processing", 10)
    assert main_workers == 8
    
    # URL crawling uses fewer workers
    crawl_workers = manager.get_workers_for_task("url_crawling", 20)
    assert crawl_workers == 2  # min(8//3, 3) = 2
    
    # Unknown task uses default strategy
    unknown_workers = manager.get_workers_for_task("unknown_task", 10)
    assert unknown_workers == 4  # max(1, 8//2) = 4


def test_worker_allocation_with_item_limits():
    """Test worker allocation is limited by number of items."""
    manager = WorkerManager(16)
    
    # Should limit workers to number of items
    workers = manager.get_workers_for_task("main_processing", 3)
    assert workers == 3
    
    # Should ensure minimum of 1 worker even with 0 items
    workers = manager.get_workers_for_task("main_processing", 0)
    assert workers == 1


def test_worker_manager_thread_safety():
    """Test WorkerManager is thread-safe."""
    manager = WorkerManager(8)
    results = []
    
    def allocate_workers(task_name, items):
        task_key = f"{task_name}_{items}"
        workers = manager.get_workers_for_task(task_key, items)
        results.append((task_name, items, workers))
        time.sleep(0.01)  # Small delay to increase chance of race conditions
        manager.release_workers_for_task(task_key)
    
    threads = []
    for i in range(10):
        thread = threading.Thread(
            target=allocate_workers,
            args=("task", i + 1)
        )
        threads.append(thread)
        thread.start()
    
    for thread in threads:
        thread.join()
    
    # All allocations should have completed successfully
    assert len(results) == 10
    for task_name, items, workers in results:
        assert workers >= 1
        assert workers <= manager.base_workers


def test_active_allocations_tracking():
    """Test tracking of active worker allocations."""
    manager = WorkerManager(8)
    
    # Initially no active allocations
    assert len(manager.get_active_allocations()) == 0
    
    # Allocate workers for different tasks
    manager.get_workers_for_task("task1", 5)
    manager.get_workers_for_task("task2", 3)
    
    allocations = manager.get_active_allocations()
    assert len(allocations) == 2
    assert "task1" in allocations
    assert "task2" in allocations
    
    # Release workers
    manager.release_workers_for_task("task1")
    allocations = manager.get_active_allocations()
    assert len(allocations) == 1
    assert "task2" in allocations


def test_config_worker_manager_integration():
    """Test Config properly initializes and updates WorkerManager."""
    config = Config()
    
    # Should have worker manager initialized
    assert config.worker_manager is not None
    assert config.worker_manager.base_workers == config.max_workers
    
    # Test update_max_workers synchronizes both
    config.update_max_workers(12)
    assert config.max_workers == 12
    assert config.worker_manager.base_workers == 12
    
    # Test update_from_dict synchronizes worker manager
    config.update_from_dict({'max_workers': 6})
    assert config.max_workers == 6
    assert config.worker_manager.base_workers == 6


def test_no_thread_pool_nesting():
    """Test that using worker manager prevents excessive thread creation."""
    manager = WorkerManager(4)
    
    # Simulate nested calls like in the real application
    def simulate_orchestrator_probe():
        workers = manager.get_workers_for_task("domain_probe", 10)
        return workers
    
    def simulate_orchestrator_main():
        workers = manager.get_workers_for_task("main_processing", 5)
        return workers
    
    def simulate_sitemap_processing():
        workers = manager.get_workers_for_task("sitemap_download", 3)
        return workers
    
    # Get worker counts
    probe_workers = simulate_orchestrator_probe()
    main_workers = simulate_orchestrator_main()
    sitemap_workers = simulate_sitemap_processing()
    
    # Total workers should not exceed reasonable limits
    total_potential_workers = probe_workers + main_workers + sitemap_workers
    
    # Even in worst case (all running simultaneously), should be reasonable
    assert total_potential_workers <= 12  # Much better than before
    
    # Individual allocations should be sensible
    assert probe_workers <= 4
    assert main_workers <= 4
    assert sitemap_workers <= 3


def test_worker_manager_with_zero_items():
    """Test worker manager handles edge cases properly."""
    manager = WorkerManager(8)
    
    # Zero items should still get 1 worker
    workers = manager.get_workers_for_task("test", 0)
    assert workers == 1
    
    # None items should work
    workers = manager.get_workers_for_task("test", None)
    assert workers == 4  # Should fall back to default strategy


@pytest.mark.integration
def test_real_world_usage_pattern():
    """Test realistic usage pattern like in the actual scraper."""
    config = Config()
    config.update_max_workers(6)
    
    # Simulate orchestrator workflow
    
    # 1. Domain probing phase
    domains = ["example.com", "test.com", "demo.com"]
    probe_workers = config.worker_manager.get_workers_for_task(
        "domain_probe", len(domains))
    assert probe_workers <= 3  # Should be limited to number of domains
    
    # Simulate using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=probe_workers) as executor:
        # This should work without issues
        futures = [executor.submit(lambda x: x, domain) for domain in domains]
        results = [f.result() for f in futures]
        assert len(results) == 3
    
    config.worker_manager.release_workers_for_task("domain_probe")
    
    # 2. Main processing phase
    companies = ["Company A", "Company B"]
    main_workers = config.worker_manager.get_workers_for_task(
        "main_processing", len(companies))
    assert main_workers <= 6  # Should not exceed base workers
    
    # 3. Sitemap processing (nested within main processing)
    sitemap_urls = ["sitemap1.xml", "sitemap2.xml"]
    sitemap_workers = config.worker_manager.get_workers_for_task(
        "sitemap_download", len(sitemap_urls))
    assert sitemap_workers <= 4  # Should be reasonable
    
    # Total allocated workers should be manageable
    total_active = len(config.worker_manager.get_active_allocations())
    assert total_active == 2  # main_processing and sitemap_download


if __name__ == "__main__":
    pytest.main([__file__])
