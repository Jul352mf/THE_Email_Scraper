"""
Optimized thread pool management to avoid nested pools and reduce overhead.

This module provides a centralized thread pool manager that prevents the creation
of nested thread pools and optimizes worker allocation across different tasks.
"""

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, Future, as_completed
from typing import Dict, List, Any, Optional, Callable, TypeVar, Generic
from dataclasses import dataclass
from enum import Enum
import queue
from contextlib import contextmanager

# Initialize logger
log = logging.getLogger(__name__)

T = TypeVar('T')

class TaskType(Enum):
    """Types of tasks that can be executed."""
    DOMAIN_PROBE = "domain_probe"
    COMPANY_PROCESSING = "company_processing"
    URL_CRAWLING = "url_crawling"
    EMAIL_EXTRACTION = "email_extraction"
    HTTP_REQUEST = "http_request"

@dataclass
class TaskStats:
    """Statistics for task execution."""
    submitted: int = 0
    completed: int = 0
    failed: int = 0
    total_time: float = 0.0
    
    @property
    def success_rate(self) -> float:
        total = self.completed + self.failed
        return (self.completed / total * 100) if total > 0 else 0
    
    @property
    def average_time(self) -> float:
        return (self.total_time / max(self.completed, 1)) if self.completed > 0 else 0

class OptimizedThreadPoolManager:
    """
    Centralized thread pool manager that prevents nesting and optimizes resource usage.
    """
    
    def __init__(self, max_workers: int = 8, max_queue_size: int = 1000):
        """
        Initialize the thread pool manager.
        
        Args:
            max_workers: Maximum number of worker threads
            max_queue_size: Maximum number of queued tasks
        """
        self.max_workers = max_workers
        self.max_queue_size = max_queue_size
        
        # Single thread pool for all operations
        self._executor: Optional[ThreadPoolExecutor] = None
        self._executor_lock = threading.RLock()
        self._shutdown = False
        
        # Task tracking and statistics
        self._active_tasks: Dict[TaskType, int] = {task_type: 0 for task_type in TaskType}
        self._task_stats: Dict[TaskType, TaskStats] = {task_type: TaskStats() for task_type in TaskType}
        self._stats_lock = threading.Lock()
        
        # Task queue and load balancing
        self._task_queue_sizes: Dict[TaskType, int] = {task_type: 0 for task_type in TaskType}
        
        # Initialize executor
        self._get_executor()
    
    def _get_executor(self) -> ThreadPoolExecutor:
        """Get or create the single thread pool executor."""
        with self._executor_lock:
            if self._executor is None or self._shutdown:
                if self._executor and not self._executor._shutdown:
                    self._executor.shutdown(wait=False)
                
                self._executor = ThreadPoolExecutor(
                    max_workers=self.max_workers,
                    thread_name_prefix="OptimizedPool"
                )
                self._shutdown = False
                log.info("Created optimized thread pool with %d workers", self.max_workers)
            
            return self._executor
    
    def submit_task(
        self, 
        task_type: TaskType, 
        func: Callable[..., T], 
        *args, 
        **kwargs
    ) -> Future[T]:
        """
        Submit a task to the optimized thread pool.
        
        Args:
            task_type: Type of task being submitted
            func: Function to execute
            *args: Positional arguments for the function
            **kwargs: Keyword arguments for the function
            
        Returns:
            Future object representing the pending result
        """
        if self._shutdown:
            raise RuntimeError("Thread pool manager has been shut down")
        
        executor = self._get_executor()
        
        # Check queue size limits
        with self._stats_lock:
            if self._task_queue_sizes[task_type] >= self.max_queue_size:
                log.warning("Task queue for %s is full, rejecting task", task_type.value)
                raise RuntimeError(f"Task queue for {task_type.value} is full")
            
            self._task_queue_sizes[task_type] += 1
            self._task_stats[task_type].submitted += 1
        
        # Wrap function to track execution
        def wrapped_func():
            start_time = time.time()
            try:
                with self._stats_lock:
                    self._active_tasks[task_type] += 1
                    self._task_queue_sizes[task_type] -= 1
                
                result = func(*args, **kwargs)
                
                with self._stats_lock:
                    self._task_stats[task_type].completed += 1
                    execution_time = time.time() - start_time
                    self._task_stats[task_type].total_time += execution_time
                
                return result
                
            except Exception as e:
                with self._stats_lock:
                    self._task_stats[task_type].failed += 1
                raise e
            finally:
                with self._stats_lock:
                    self._active_tasks[task_type] -= 1
        
        future = executor.submit(wrapped_func)
        return future
    
    def submit_batch(
        self,
        task_type: TaskType,
        func: Callable[..., T],
        arg_list: List[tuple],
        max_concurrent: Optional[int] = None
    ) -> List[Future[T]]:
        """
        Submit a batch of tasks with optional concurrency limiting.
        
        Args:
            task_type: Type of tasks being submitted
            func: Function to execute for each item
            arg_list: List of argument tuples for each task
            max_concurrent: Maximum concurrent tasks (defaults to max_workers)
            
        Returns:
            List of Future objects
        """
        if not arg_list:
            return []
        
        max_concurrent = max_concurrent or self.max_workers
        futures = []
        
        # Use semaphore to limit concurrent tasks if needed
        semaphore = threading.Semaphore(max_concurrent) if max_concurrent < len(arg_list) else None
        
        def wrapped_func(*args):
            try:
                if semaphore:
                    semaphore.acquire()
                return func(*args)
            finally:
                if semaphore:
                    semaphore.release()
        
        target_func = wrapped_func if semaphore else func
        
        for args in arg_list:
            future = self.submit_task(task_type, target_func, *args)
            futures.append(future)
        
        log.debug("Submitted batch of %d %s tasks", len(arg_list), task_type.value)
        return futures
    
    def execute_batch_and_wait(
        self,
        task_type: TaskType,
        func: Callable[..., T],
        arg_list: List[tuple],
        max_concurrent: Optional[int] = None,
        timeout: Optional[float] = None
    ) -> List[T]:
        """
        Execute a batch of tasks and wait for all to complete.
        
        Args:
            task_type: Type of tasks
            func: Function to execute
            arg_list: List of arguments for each task
            max_concurrent: Maximum concurrent tasks
            timeout: Timeout in seconds for all tasks
            
        Returns:
            List of results in the same order as input
        """
        futures = self.submit_batch(task_type, func, arg_list, max_concurrent)
        results = []
        
        start_time = time.time()
        
        try:
            for future in futures:
                remaining_timeout = None
                if timeout:
                    elapsed = time.time() - start_time
                    remaining_timeout = max(0, timeout - elapsed)
                    if remaining_timeout <= 0:
                        break
                
                try:
                    result = future.result(timeout=remaining_timeout)
                    results.append(result)
                except Exception as e:
                    log.warning("Task failed in batch execution: %s", e)
                    results.append(None)  # Placeholder for failed task
            
        except KeyboardInterrupt:
            log.warning("Batch execution interrupted, cancelling remaining tasks")
            for future in futures[len(results):]:
                future.cancel()
            raise
        
        return results
    
    @contextmanager
    def task_context(self, task_type: TaskType):
        """Context manager for tracking task execution."""
        with self._stats_lock:
            self._active_tasks[task_type] += 1
        try:
            yield
        finally:
            with self._stats_lock:
                self._active_tasks[task_type] -= 1
    
    def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive thread pool statistics."""
        with self._stats_lock:
            stats = {
                'max_workers': self.max_workers,
                'active_threads': self._executor._threads if self._executor else 0,
                'total_active_tasks': sum(self._active_tasks.values()),
                'active_tasks_by_type': dict(self._active_tasks),
                'queue_sizes': dict(self._task_queue_sizes),
                'task_stats': {}
            }
            
            # Add detailed stats for each task type
            for task_type, task_stats in self._task_stats.items():
                stats['task_stats'][task_type.value] = {
                    'submitted': task_stats.submitted,
                    'completed': task_stats.completed,
                    'failed': task_stats.failed,
                    'success_rate_percent': round(task_stats.success_rate, 2),
                    'average_execution_time_ms': round(task_stats.average_time * 1000, 2),
                    'total_execution_time_seconds': round(task_stats.total_time, 2)
                }
        
        return stats
    
    def get_load_info(self) -> Dict[str, Any]:
        """Get current load information for optimization decisions."""
        with self._stats_lock:
            total_active = sum(self._active_tasks.values())
            total_queued = sum(self._task_queue_sizes.values())
            
            return {
                'utilization_percent': round((total_active / self.max_workers) * 100, 2),
                'queue_utilization_percent': round((total_queued / self.max_queue_size) * 100, 2),
                'available_workers': max(0, self.max_workers - total_active),
                'queue_space_remaining': max(0, self.max_queue_size - total_queued),
                'is_overloaded': total_queued > self.max_queue_size * 0.8
            }
    
    def adjust_worker_count(self, new_count: int) -> bool:
        """
        Dynamically adjust the number of worker threads.
        
        Args:
            new_count: New number of workers
            
        Returns:
            True if adjustment was successful
        """
        if new_count == self.max_workers:
            return True
            
        if new_count < 1 or new_count > 64:
            log.warning("Invalid worker count %d, must be between 1 and 64", new_count)
            return False
        
        with self._executor_lock:
            old_count = self.max_workers
            self.max_workers = new_count
            
            # Shutdown and recreate executor with new size
            if self._executor and not self._executor._shutdown:
                self._executor.shutdown(wait=False)
            self._executor = None
            
            log.info("Adjusted thread pool size from %d to %d workers", old_count, new_count)
            return True
    
    def shutdown(self, wait: bool = True) -> None:
        """Shutdown the thread pool manager."""
        with self._executor_lock:
            self._shutdown = True
            if self._executor and not self._executor._shutdown:
                self._executor.shutdown(wait=wait)
                log.info("Thread pool manager shut down")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdown()

# Global instance
_global_pool_manager: Optional[OptimizedThreadPoolManager] = None
_global_pool_lock = threading.Lock()

def get_global_pool_manager() -> OptimizedThreadPoolManager:
    """Get or create the global thread pool manager."""
    global _global_pool_manager
    
    with _global_pool_lock:
        if _global_pool_manager is None or _global_pool_manager._shutdown:
            # Import config here to avoid circular imports
            try:
                from scraper.config import config
                max_workers = config.max_workers
            except ImportError:
                max_workers = 4
            
            _global_pool_manager = OptimizedThreadPoolManager(max_workers=max_workers)
            log.info("Created global thread pool manager with %d workers", max_workers)
        
        return _global_pool_manager

# Convenience functions
def submit_task(task_type: TaskType, func: Callable[..., T], *args, **kwargs) -> Future[T]:
    """Submit a task to the global thread pool."""
    return get_global_pool_manager().submit_task(task_type, func, *args, **kwargs)

def execute_batch(
    task_type: TaskType,
    func: Callable[..., T],
    arg_list: List[tuple],
    max_concurrent: Optional[int] = None,
    timeout: Optional[float] = None
) -> List[T]:
    """Execute a batch of tasks using the global thread pool."""
    return get_global_pool_manager().execute_batch_and_wait(
        task_type, func, arg_list, max_concurrent, timeout
    )

def get_pool_stats() -> Dict[str, Any]:
    """Get global thread pool statistics."""
    return get_global_pool_manager().get_stats()

def shutdown_global_pool() -> None:
    """Shutdown the global thread pool manager."""
    global _global_pool_manager
    with _global_pool_lock:
        if _global_pool_manager:
            _global_pool_manager.shutdown()
            _global_pool_manager = None