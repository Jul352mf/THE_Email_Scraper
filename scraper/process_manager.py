"""
Process management utilities for preventing zombie processes and ensuring proper cleanup.

This module provides utilities to manage child processes, handle signals properly,
and prevent zombie processes from accumulating.
"""

import atexit
import logging
import signal
import threading
import time
from typing import List, Optional, Set
from multiprocessing import Process

log = logging.getLogger(__name__)

class ProcessManager:
    """Manager for tracking and cleaning up child processes."""
    
    def __init__(self):
        self._processes: Set[Process] = set()
        self._lock = threading.Lock()
        self._cleanup_registered = False
        self._signal_handlers_set = False
    
    def register_process(self, process: Process) -> None:
        """Register a process for cleanup tracking."""
        with self._lock:
            self._processes.add(process)
            log.debug("Registered process %s (PID: %s)", process.name, process.pid)
            
            # Register cleanup handlers on first process
            if not self._cleanup_registered:
                self._setup_cleanup_handlers()
                self._cleanup_registered = True
    
    def unregister_process(self, process: Process) -> None:
        """Unregister a process from cleanup tracking."""
        with self._lock:
            self._processes.discard(process)
            log.debug("Unregistered process %s (PID: %s)", process.name, process.pid)
    
    def _setup_cleanup_handlers(self) -> None:
        """Set up signal handlers and atexit cleanup."""
        # Register atexit handler for normal termination
        atexit.register(self.cleanup_all_processes)
        log.debug("Registered atexit cleanup handler")
        
        # Set up signal handlers for graceful shutdown
        if not self._signal_handlers_set:
            try:
                # Handle SIGINT (Ctrl+C) and SIGTERM
                signal.signal(signal.SIGINT, self._signal_handler)
                if hasattr(signal, 'SIGTERM'):
                    signal.signal(signal.SIGTERM, self._signal_handler)
                self._signal_handlers_set = True
                log.debug("Set up signal handlers for process cleanup")
            except (ValueError, OSError) as e:
                log.warning("Could not set up signal handlers: %s", e)
    
    def _signal_handler(self, signum, frame):
        """Handle termination signals by cleaning up processes."""
        log.warning("Received signal %d, cleaning up processes...", signum)
        self.cleanup_all_processes()
        # Re-raise the signal to allow normal termination
        signal.signal(signum, signal.SIG_DFL)
        signal.raise_signal(signum)
    
    def cleanup_all_processes(self) -> None:
        """Clean up all registered processes to prevent zombies."""
        with self._lock:
            if not self._processes:
                return
                
            log.info("Cleaning up %d child processes...", len(self._processes))
            
            # Copy the set to avoid modification during iteration
            processes_to_cleanup = self._processes.copy()
            
            for process in processes_to_cleanup:
                try:
                    self._cleanup_single_process(process)
                except Exception as e:
                    log.error("Error cleaning up process %s: %s", process.name, e)
            
            self._processes.clear()
            log.info("Process cleanup completed")
    
    def _cleanup_single_process(self, process: Process) -> None:
        """Clean up a single process with proper termination sequence."""
        if not process.is_alive():
            # Process already terminated, just join to clean up
            try:
                process.join(timeout=1.0)
                log.debug("Joined terminated process %s", process.name)
            except Exception as e:
                log.debug("Error joining terminated process %s: %s", process.name, e)
            return
        
        log.debug("Shutting down process %s (PID: %s)", process.name, process.pid)
        
        # Step 1: Try graceful shutdown if process has a shutdown method
        if hasattr(process, 'shutdown'):
            try:
                process.shutdown()
                log.debug("Called shutdown() on process %s", process.name)
                
                # Wait for graceful shutdown
                process.join(timeout=3.0)
                if not process.is_alive():
                    log.debug("Process %s shut down gracefully", process.name)
                    return
            except Exception as e:
                log.warning("Graceful shutdown failed for %s: %s", process.name, e)
        
        # Step 2: Try terminate
        try:
            process.terminate()
            log.debug("Terminated process %s", process.name)
            
            # Wait for termination
            process.join(timeout=2.0)
            if not process.is_alive():
                log.debug("Process %s terminated successfully", process.name)
                return
        except Exception as e:
            log.warning("Termination failed for %s: %s", process.name, e)
        
        # Step 3: Force kill if still alive
        if process.is_alive():
            try:
                process.kill()
                log.warning("Force killed process %s", process.name)
                process.join(timeout=1.0)
            except Exception as e:
                log.error("Force kill failed for %s: %s", process.name, e)
    
    def check_for_zombies(self) -> List[Process]:
        """Check for and clean up any zombie processes."""
        zombies = []
        
        with self._lock:
            for process in list(self._processes):
                try:
                    if process.exitcode is not None and process.is_alive() is False:
                        # This is a zombie - terminated but not joined
                        zombies.append(process)
                        try:
                            process.join(timeout=0.1)  # Clean up immediately
                            log.info("Cleaned up zombie process %s", process.name)
                        except Exception as e:
                            log.error("Failed to clean zombie %s: %s", process.name, e)
                        
                        self._processes.discard(process)
                except Exception as e:
                    log.debug("Error checking process %s: %s", process.name, e)
        
        return zombies
    
    def get_process_info(self) -> dict:
        """Get information about tracked processes."""
        with self._lock:
            info = {
                'total_processes': len(self._processes),
                'alive_processes': 0,
                'terminated_processes': 0,
                'process_details': []
            }
            
            for process in self._processes:
                try:
                    is_alive = process.is_alive()
                    if is_alive:
                        info['alive_processes'] += 1
                    else:
                        info['terminated_processes'] += 1
                    
                    info['process_details'].append({
                        'name': process.name,
                        'pid': process.pid,
                        'is_alive': is_alive,
                        'exitcode': process.exitcode
                    })
                except Exception as e:
                    log.debug("Error getting info for process %s: %s", process.name, e)
            
            return info

# Global process manager instance
_process_manager = ProcessManager()

def register_process(process: Process) -> None:
    """Register a process for cleanup tracking."""
    _process_manager.register_process(process)

def unregister_process(process: Process) -> None:
    """Unregister a process from cleanup tracking."""
    _process_manager.unregister_process(process)

def cleanup_all_processes() -> None:
    """Clean up all registered processes."""
    _process_manager.cleanup_all_processes()

def check_for_zombies() -> List[Process]:
    """Check for and clean up zombie processes."""
    return _process_manager.check_for_zombies()

def get_process_info() -> dict:
    """Get information about tracked processes."""
    return _process_manager.get_process_info()

def setup_signal_handlers() -> None:
    """Explicitly set up signal handlers for process cleanup."""
    _process_manager._setup_cleanup_handlers()