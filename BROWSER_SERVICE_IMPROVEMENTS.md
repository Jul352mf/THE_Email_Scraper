# BrowserService Improvements - Shutdown Handling & IPC/Backpressure

## Issues Identified

### 1. **Shutdown Handling Problems**
- **Problem**: `BrowserService.shutdown()` only sets stop event and sends sentinel, but doesn't wait for process termination
- **Risk**: Orphaned browser processes, resource leaks
- **Impact**: Memory leaks, zombie processes on system

### 2. **IPC Queue Backpressure**  
- **Problem**: No queue size limits, no handling of full queues
- **Risk**: Memory exhaustion with high request volume
- **Impact**: System instability under load

### 3. **Graceful Termination**
- **Problem**: No timeout for shutdown, no cleanup verification
- **Risk**: Hanging processes, incomplete cleanup
- **Impact**: Resource leaks, service unavailability

## Solutions

### **Solution 1: Enhanced Shutdown Handling**

```python
def shutdown(self, timeout: float = 10.0):
    """Shutdown browser service with proper cleanup."""
    if not self.is_alive():
        log.info("BrowserService already terminated")
        return
        
    log.info("Shutting down BrowserService...")
    
    # Signal shutdown
    self._stop_event.set()
    self._requests.put((None, None))  # Shutdown sentinel
    
    # Wait for graceful termination
    self.join(timeout=timeout)
    
    # Force termination if needed
    if self.is_alive():
        log.warning("BrowserService didn't shutdown gracefully, terminating...")
        self.terminate()
        self.join(timeout=2.0)
        
    # Cleanup resources
    if self.is_alive():
        log.error("BrowserService termination failed - killing process")
        self.kill()
        
    log.info("BrowserService shutdown complete")
```

### **Solution 2: Queue Backpressure Handling**

```python
def render(self, url, timeout=None, max_queue_size: int = 100):
    """Render with backpressure handling."""
    if timeout is None:
        timeout = self.render_timeout + self.idle_timeout
        
    # Check queue size for backpressure
    if self._requests.qsize() >= max_queue_size:
        log.warning("BrowserService queue full (%d requests), rejecting %s", 
                   max_queue_size, url)
        raise BrowserServiceUnavailableError("Request queue full")
    
    # Check if service is available
    if not self.is_alive():
        log.error("BrowserService not running")
        raise BrowserServiceUnavailableError("Service not available")
        
    req_id = str(uuid.uuid4())
    resp_q = _manager.Queue(1)
    self._responses[req_id] = resp_q
    
    try:
        self._requests.put((req_id, url), timeout=1.0)  # Timeout on put
    except Full:
        self._responses.pop(req_id, None)
        raise BrowserServiceUnavailableError("Failed to queue request")

    try:
        return resp_q.get(timeout=timeout)
    except (Empty, TimeoutError):
        log.warning("Render() timeout for %s", url)
        return ""
    finally:
        self._responses.pop(req_id, None)
```

### **Solution 3: Health Monitoring & Recovery**

```python
class BrowserServiceManager:
    """Enhanced browser service with health monitoring."""
    
    def __init__(self):
        self._service = None
        self._restart_count = 0
        self._max_restarts = 3
        self._last_restart = 0
        
    def get_service(self) -> BrowserService:
        """Get healthy browser service, restart if needed."""
        if self._should_restart():
            self._restart_service()
        return self._service
    
    def _should_restart(self) -> bool:
        """Check if service needs restart."""
        if not self._service:
            return True
        if not self._service.is_alive():
            log.warning("BrowserService died, needs restart")
            return True
        return False
    
    def _restart_service(self):
        """Restart browser service with exponential backoff."""
        import time
        
        if self._service:
            try:
                self._service.shutdown(timeout=5.0)
            except Exception as e:
                log.error("Error shutting down old service: %s", e)
                
        # Exponential backoff
        if self._restart_count > 0:
            backoff = min(2 ** self._restart_count, 30)  # Max 30s
            log.info("Backing off %ds before restart", backoff)
            time.sleep(backoff)
            
        if self._restart_count >= self._max_restarts:
            raise BrowserServiceError("Max restart attempts exceeded")
            
        log.info("Starting BrowserService (attempt %d)", self._restart_count + 1)
        self._service = BrowserService()
        self._service.start()
        self._restart_count += 1
        self._last_restart = time.time()
```

### **Solution 4: Circuit Breaker for Browser Service**

```python
class BrowserServiceCircuitBreaker:
    """Circuit breaker for browser service reliability."""
    
    def __init__(self, failure_threshold: int = 5, timeout: int = 60):
        self.failure_count = 0
        self.failure_threshold = failure_threshold  
        self.timeout = timeout
        self.last_failure_time = 0
        self.state = 'closed'  # closed, open, half-open
        
    def can_call(self) -> bool:
        """Check if browser service can be called."""
        if self.state == 'closed':
            return True
        elif self.state == 'open':
            if time.time() - self.last_failure_time >= self.timeout:
                self.state = 'half-open'
                return True
            return False
        else:  # half-open  
            return True
            
    def record_success(self):
        """Record successful browser service call."""
        self.failure_count = 0
        self.state = 'closed'
        
    def record_failure(self):
        """Record failed browser service call."""
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        if self.failure_count >= self.failure_threshold:
            self.state = 'open'
            log.warning("BrowserService circuit breaker opened after %d failures", 
                       self.failure_count)

# Usage in hybrid extractor
def _render_and_extract_with_circuit_breaker(self, url: str) -> Set[str]:
    if not circuit_breaker.can_call():
        log.warning("BrowserService circuit breaker open, skipping JS for %s", url)
        return set()
        
    try:
        html = get_browser_service().render(url)
        circuit_breaker.record_success()
        return self._static_pass(html, url)
    except BrowserServiceUnavailableError:
        circuit_breaker.record_failure() 
        return set()
    except Exception as e:
        log.error("BrowserService error: %s", e)
        circuit_breaker.record_failure()
        return set()
```

## Implementation Plan

### **Phase 1: Immediate Fixes**
1. **Enhanced Shutdown** - Add proper shutdown with timeout and cleanup verification
2. **Queue Limits** - Add max queue size and backpressure handling  
3. **Error Handling** - Add specific exceptions for service unavailability

### **Phase 2: Reliability Improvements** 
1. **Health Monitoring** - Service manager with restart capability
2. **Circuit Breaker** - Prevent cascading failures
3. **Graceful Degradation** - Continue processing without JS when service fails

### **Phase 3: Advanced Features**
1. **Connection Pooling** - Multiple browser instances for better throughput
2. **Request Prioritization** - Priority queues for important requests
3. **Performance Metrics** - Integration with existing performance monitoring

## Exception Classes Needed

```python
class BrowserServiceError(Exception):
    """Base exception for browser service errors."""
    pass

class BrowserServiceUnavailableError(BrowserServiceError):
    """Browser service is unavailable (queue full, service down, etc.)."""
    pass
    
class BrowserServiceTimeoutError(BrowserServiceError):
    """Browser service request timed out.""" 
    pass
```

## Testing Strategy

1. **Unit Tests** - Test shutdown, queue handling, circuit breaker
2. **Integration Tests** - Test with real browser service
3. **Load Tests** - Verify backpressure handling under high load
4. **Chaos Tests** - Kill browser processes, verify recovery

This improves the BrowserService reliability from the current "fire and forget" approach to a production-ready service with proper resource management and graceful degradation.
