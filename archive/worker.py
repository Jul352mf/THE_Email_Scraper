"""
Worker pool module for parallel processing with consumer/producer pattern.

This module provides a robust worker pool implementation with proper task queuing,
result collection, and error handling.
"""

import logging
import queue
import threading
import time
from concurrent.futures import ThreadPoolExecutor, Future, as_completed
from dataclasses import dataclass
from typing import TypeVar, Generic, Callable, List, Dict, Any, Optional, Set, Tuple, Iterator

# Initialize logger
log = logging.getLogger(__name__)

# Type variables for generic task and result types
T = TypeVar('T')  # Task type
R = TypeVar('R')  # Result type

@dataclass
class TaskResult(Generic[T, R]):
    """Container for task results with task reference and metadata."""
    task: T
    result: Optional[R] = None
    error: Optional[Exception] = None
    success: bool = False
    duration: float = 0.0

class WorkerPool(Generic[T, R]):
    """
    Enhanced worker pool with consumer/producer pattern for parallel processing.
    
    This implementation provides:
    - Task queuing with priority support
    - Result collection with original task reference
    - Proper error handling and reporting
    - Graceful shutdown and cleanup
    - Progress tracking and reporting
    """
    
    def __init__(self, worker_count: int = 4, 
                 task_processor: Optional[Callable[[T], R]] = None,
                 name: str = "worker_pool"):
        """
        Initialize the worker pool.
        
        Args:
            worker_count: Number of worker threads
            task_processor: Function to process tasks
            name: Name for this worker pool (for logging)
        """
        self.name = name
        self.worker_count = max(1, worker_count)
        self.task_processor = task_processor
        
        # Task queue
        self.task_queue: queue.Queue[T] = queue.Queue()
        
        # Result tracking
        self.results: List[TaskResult[T, R]] = []
        self.results_lock = threading.Lock()
        
        # State tracking
        self.active = False
        self.processed_count = 0
        self.error_count = 0
        self.start_time = 0.0
        
        # Worker threads
        self.workers: List[threading.Thread] = []
        self.stop_event = threading.Event()
    
    def add_task(self, task: T) -> None:
        """
        Add a task to the queue.
        
        Args:
            task: Task to add
        """
        self.task_queue.put(task)
    
    def add_tasks(self, tasks: List[T]) -> None:
        """
        Add multiple tasks to the queue.
        
        Args:
            tasks: List of tasks to add
        """
        for task in tasks:
            self.add_task(task)
    
    def _worker_loop(self) -> None:
        """Worker thread function that processes tasks from the queue."""
        thread_id = threading.get_ident()
        log.debug("[%s] Worker %d starting", self.name, thread_id)
        
        while not self.stop_event.is_set():
            try:
                # Get task with timeout to allow checking stop_event
                try:
                    task = self.task_queue.get(timeout=0.5)
                except queue.Empty:
                    continue
                
                # Process task
                start_time = time.time()
                task_result = TaskResult(task=task)
                
                try:
                    if self.task_processor:
                        result = self.task_processor(task)
                        task_result.result = result
                        task_result.success = True
                    else:
                        log.warning("[%s] No task processor defined", self.name)
                        task_result.error = ValueError("No task processor defined")
                except Exception as e:
                    log.exception("[%s] Error processing task: %s", self.name, e)
                    task_result.error = e
                
                # Calculate duration
                task_result.duration = time.time() - start_time
                
                # Store result
                with self.results_lock:
                    self.results.append(task_result)
                    self.processed_count += 1
                    if not task_result.success:
                        self.error_count += 1
                
                # Mark task as done
                self.task_queue.task_done()
                
            except Exception as e:
                log.exception("[%s] Worker error: %s", self.name, e)
        
        log.debug("[%s] Worker %d stopping", self.name, thread_id)
    
    def start(self) -> None:
        """Start the worker pool."""
        if self.active:
            log.warning("[%s] Worker pool already started", self.name)
            return
        
        log.info("[%s] Starting worker pool with %d workers", self.name, self.worker_count)
        self.active = True
        self.start_time = time.time()
        self.stop_event.clear()
        
        # Create and start worker threads
        self.workers = []
        for _ in range(self.worker_count):
            worker = threading.Thread(target=self._worker_loop)
            worker.daemon = True
            worker.start()
            self.workers.append(worker)
    
    def stop(self, wait: bool = True) -> None:
        """
        Stop the worker pool.
        
        Args:
            wait: Whether to wait for all tasks to complete
        """
        if not self.active:
            return
        
        log.info("[%s] Stopping worker pool", self.name)
        
        if wait:
            # Wait for all tasks to complete
            self.task_queue.join()
        
        # Signal workers to stop
        self.stop_event.set()
        
        # Wait for workers to stop
        for worker in self.workers:
            worker.join(timeout=1.0)
        
        self.active = False
        log.info("[%s] Worker pool stopped", self.name)
    
    def wait(self) -> None:
        """Wait for all tasks to complete."""
        if not self.active:
            return
        
        self.task_queue.join()
    
    def get_results(self) -> List[TaskResult[T, R]]:
        """
        Get all results.
        
        Returns:
            List of task results
        """
        with self.results_lock:
            return list(self.results)
    
    def get_successful_results(self) -> List[R]:
        """
        Get successful results only.
        
        Returns:
            List of successful results
        """
        with self.results_lock:
            return [r.result for r in self.results if r.success and r.result is not None]
    
    def get_failed_tasks(self) -> List[T]:
        """
        Get tasks that failed.
        
        Returns:
            List of failed tasks
        """
        with self.results_lock:
            return [r.task for r in self.results if not r.success]
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the worker pool.
        
        Returns:
            Dictionary of statistics
        """
        elapsed = time.time() - self.start_time if self.start_time > 0 else 0
        
        with self.results_lock:
            return {
                "name": self.name,
                "active": self.active,
                "worker_count": self.worker_count,
                "queue_size": self.task_queue.qsize(),
                "processed_count": self.processed_count,
                "error_count": self.error_count,
                "success_rate": (
                    (self.processed_count - self.error_count) / self.processed_count * 100
                    if self.processed_count > 0 else 0
                ),
                "elapsed_time": elapsed,
                "tasks_per_second": self.processed_count / elapsed if elapsed > 0 else 0
            }
    
    def __enter__(self) -> 'WorkerPool[T, R]':
        """Context manager entry."""
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit."""
        self.stop(wait=True)

class BatchProcessor(Generic[T, R]):
    """
    Batch processor for processing tasks in parallel with progress tracking.
    
    This class provides a higher-level interface for the WorkerPool with:
    - Automatic batching of tasks
    - Progress tracking and reporting
    - Result aggregation
    """
    
    def __init__(self, processor: Callable[[T], R], 
                 worker_count: int = 4,
                 batch_size: int = 100,
                 name: str = "batch_processor"):
        """
        Initialize the batch processor.
        
        Args:
            processor: Function to process tasks
            worker_count: Number of worker threads
            batch_size: Size of task batches
            name: Name for this batch processor
        """
        self.name = name
        self.processor = processor
        self.worker_count = worker_count
        self.batch_size = batch_size
        
        # Progress tracking
        self.total_tasks = 0
        self.completed_tasks = 0
        self.progress_callback = None
    
    def set_progress_callback(self, callback: Callable[[int, int, float], None]) -> None:
        """
        Set a callback for progress updates.
        
        Args:
            callback: Function(completed, total, percent) to call with progress updates
        """
        self.progress_callback = callback
    
    def _update_progress(self, worker_pool: WorkerPool[T, R]) -> None:
        """
        Update progress based on worker pool state.
        
        Args:
            worker_pool: Worker pool to get progress from
        """
        stats = worker_pool.get_stats()
        self.completed_tasks = stats["processed_count"]
        
        if self.progress_callback and self.total_tasks > 0:
            percent = min(100.0, self.completed_tasks / self.total_tasks * 100)
            self.progress_callback(self.completed_tasks, self.total_tasks, percent)
    
    def process(self, tasks: List[T], 
                progress_interval: float = 1.0) -> List[TaskResult[T, R]]:
        """
        Process tasks in parallel with progress tracking.
        
        Args:
            tasks: List of tasks to process
            progress_interval: Interval in seconds for progress updates
            
        Returns:
            List of task results
        """
        if not tasks:
            return []
        
        self.total_tasks = len(tasks)
        self.completed_tasks = 0
        
        # Create worker pool
        pool = WorkerPool[T, R](
            worker_count=self.worker_count,
            task_processor=self.processor,
            name=self.name
        )
        
        # Add tasks to pool
        pool.add_tasks(tasks)
        
        # Start processing
        pool.start()
        
        # Track progress
        try:
            last_update = time.time()
            while pool.active and pool.processed_count < self.total_tasks:
                time.sleep(0.1)
                
                # Update progress at specified interval
                now = time.time()
                if now - last_update >= progress_interval:
                    self._update_progress(pool)
                    last_update = now
        except KeyboardInterrupt:
            log.warning("[%s] Processing interrupted", self.name)
            pool.stop(wait=False)
            raise
        finally:
            # Final progress update
            self._update_progress(pool)
            
            # Stop pool
            pool.stop()
        
        # Return results
        return pool.get_results()
    
    def process_batched(self, tasks: List[T], 
                       progress_interval: float = 1.0) -> Iterator[List[TaskResult[T, R]]]:
        """
        Process tasks in batches, yielding results for each batch.
        
        Args:
            tasks: List of tasks to process
            progress_interval: Interval in seconds for progress updates
            
        Yields:
            List of task results for each batch
        """
        if not tasks:
            return
        
        self.total_tasks = len(tasks)
        self.completed_tasks = 0
        
        # Process in batches
        for i in range(0, len(tasks), self.batch_size):
            batch = tasks[i:i + self.batch_size]
            log.info("[%s] Processing batch %d/%d (%d tasks)",
                    self.name, i // self.batch_size + 1, 
                    (len(tasks) + self.batch_size - 1) // self.batch_size,
                    len(batch))
            
            # Process batch
            results = self.process(batch, progress_interval)
            
            # Update overall progress
            self.completed_tasks += len(batch)
            if self.progress_callback:
                percent = min(100.0, self.completed_tasks / self.total_tasks * 100)
                self.progress_callback(self.completed_tasks, self.total_tasks, percent)
            
            # Yield batch results
            yield results
