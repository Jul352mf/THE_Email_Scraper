# TASK-045: Async I/O Migration Phase 1 - Implementation Plan

## Overview
Migrate the synchronous email scraper to async/await pattern for massive performance improvements. Based on performance analysis, this will provide **5-10x speed improvement** by eliminating I/O wait times.

## Current State Analysis
- ✅ **AsyncBrowserPool** already exists in `async_scraper.py` with aiohttp
- ✅ **Circuit breaker & retry logic** (TASK-039) ready for async integration  
- ✅ **Performance monitoring** (TASK-044) can track async performance
- ❌ **HTTP Client** is fully synchronous (`requests` library)
- ❌ **Google Search** is synchronous with rate limiting
- ❌ **Orchestrator** uses threading instead of async
- ❌ **Email extraction** is synchronous

## Phase 1 Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Async Architecture                       │
├─────────────────────────────────────────────────────────────┤
│ Phase 1: Parallel Google Searches (Biggest Win)            │
│   • async Google API calls with asyncio.gather()           │
│   • Concurrent domain scoring                              │
│   • 100 companies: 150s → 3s (50x improvement)            │
├─────────────────────────────────────────────────────────────┤
│ Phase 2: Async HTTP Client                                 │
│   • Replace requests with aiohttp                          │
│   • Async circuit breaker integration                      │
│   • Connection pooling with aiohttp.ClientSession         │
├─────────────────────────────────────────────────────────────┤
│ Phase 3: Async Email Extraction                           │
│   • Parallel URL processing per company                    │
│   • Async browser service integration                      │
│   • 20 URLs: 40s → 3s (13x improvement)                   │
└─────────────────────────────────────────────────────────────┘
```

## Implementation Strategy

### Step 1: Async Google Search Client ⭐ (Highest Impact)
**File**: `scraper/async_google_search.py`
**Impact**: 50x improvement for Google searches

```python
class AsyncGoogleSearchClient:
    def __init__(self):
        self.session = aiohttp.ClientSession()
        self.rate_limiter = AsyncRateLimiter(1.0)  # 1 req/sec
        
    async def search_async(self, query: str) -> List[str]:
        async with self.rate_limiter:
            # Async Google API call
            
    async def batch_search(self, companies: List[str]) -> Dict[str, List[str]]:
        tasks = [self.search_async(company) for company in companies]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return dict(zip(companies, results))
```

### Step 2: Async HTTP Client  
**File**: `scraper/async_http_client.py`
**Features**: 
- Async circuit breaker (port from TASK-039)
- Connection pooling with aiohttp
- Smart retry with async delays

```python
class AsyncHttpClient:
    def __init__(self):
        self.session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=100, limit_per_host=10),
            timeout=aiohttp.ClientTimeout(total=30)
        )
        self.circuit_breaker = AsyncCircuitBreaker()
        
    async def safe_get_async(self, url: str, **kwargs) -> Optional[aiohttp.ClientResponse]:
        if not self.circuit_breaker.can_request():
            return None
            
        try:
            async with self.session.get(url, **kwargs) as response:
                self.circuit_breaker.record_success()
                return response
        except Exception as e:
            self.circuit_breaker.record_failure()
            if self.retry_strategy.should_retry(str(e)):
                await asyncio.sleep(self.retry_strategy.get_delay())
                # Retry logic
```

### Step 3: Async Orchestrator
**File**: `scraper/async_orchestrator.py` 
**Key Changes**:
- Replace `ThreadPoolExecutor` with `asyncio.gather()`
- Async Phase 1: Parallel Google searches
- Async Phase 3: Parallel URL processing per company

```python
class AsyncOrchestrator:
    async def process_companies_async(self, companies: List[str]) -> Tuple[Counter, List[Dict]]:
        # Phase 1: Parallel Google searches (MASSIVE IMPROVEMENT)
        search_tasks = [self.google_client.search_async(company) for company in companies]
        search_results = await asyncio.gather(*search_tasks, return_exceptions=True)
        
        # Phase 2: Batch domain probing (already parallel)
        domains = self.extract_domains(search_results)
        await self.batch_probe_domains_async(domains)
        
        # Phase 3: Parallel company processing  
        company_tasks = [self.process_company_async(company, domain) for company, domain in valid_pairs]
        results = await asyncio.gather(*company_tasks)
        
        return self.aggregate_results(results)
        
    async def process_company_async(self, company: str, domain: str) -> Tuple[Counter, List[Dict]]:
        # Get main page
        main_response = await self.http_client.safe_get_async(f"https://{domain}")
        
        # Get sitemap URLs
        sitemap_urls = await self.sitemap_parser.get_priority_urls_async(domain)
        
        # Process all URLs in parallel (MAJOR IMPROVEMENT)
        url_tasks = [self.extract_emails_from_url_async(url) for url in sitemap_urls]
        email_sets = await asyncio.gather(*url_tasks, return_exceptions=True)
        
        return self.combine_email_results(email_sets)
```

### Step 4: Async Email Extractor
**File**: `scraper/async_hybrid_email_extractor.py`
**Integration**: Use existing `AsyncBrowserPool` from `async_scraper.py`

```python
class AsyncHybridEmailExtractor:
    def __init__(self, browser_pool: AsyncBrowserPool):
        self.browser_pool = browser_pool
        self.http_client = AsyncHttpClient()
        
    async def extract_from_url_async(self, url: str) -> Set[str]:
        # Async HTTP request
        response = await self.http_client.safe_get_async(url)
        if not response:
            return set()
            
        # Static extraction (CPU-bound, still sync)
        emails = self._static_pass(await response.text(), url)
        
        # Async JS fallback if needed
        if not emails and self.should_use_js_fallback(url):
            html = await self.browser_pool.render(url)
            emails = self._static_pass(html, url)
            
        return emails
```

## Implementation Timeline

### Week 1: Foundation
- [ ] Create `async_google_search.py` with batch search capability
- [ ] Create `async_http_client.py` with circuit breaker
- [ ] Port existing circuit breaker to async version
- [ ] Basic async rate limiting

### Week 2: Core Migration  
- [ ] Create `async_orchestrator.py` with Phase 1 parallel searches
- [ ] Create `async_hybrid_email_extractor.py` 
- [ ] Integration testing with small datasets
- [ ] Performance benchmarking

### Week 3: Integration & Testing
- [ ] CLI integration with `--async` flag for A/B testing
- [ ] Comprehensive testing with real datasets
- [ ] Performance monitoring integration
- [ ] Error handling and graceful degradation

### Week 4: Production Readiness
- [ ] Memory optimization and connection management
- [ ] Rate limiting fine-tuning
- [ ] Documentation and examples
- [ ] Migration strategy for existing users

## Technical Implementation Details

### Async Circuit Breaker
```python
class AsyncCircuitBreaker:
    def __init__(self):
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.last_failure_time = 0
        self._lock = asyncio.Lock()
        
    async def call_async(self, coro):
        async with self._lock:
            if not self.can_request():
                raise CircuitBreakerOpenError()
                
        try:
            result = await coro
            await self._record_success()
            return result
        except Exception as e:
            await self._record_failure()
            raise
```

### Async Rate Limiting
```python
class AsyncRateLimiter:
    def __init__(self, rate: float):
        self.rate = rate  # requests per second
        self.last_request = 0
        self._lock = asyncio.Lock()
        
    async def __aenter__(self):
        async with self._lock:
            now = time.time()
            time_since_last = now - self.last_request
            min_interval = 1.0 / self.rate
            
            if time_since_last < min_interval:
                sleep_time = min_interval - time_since_last
                await asyncio.sleep(sleep_time)
                
            self.last_request = time.time()
            
    async def __aexit__(self, *args):
        pass
```

## Performance Expectations

### Before (Current Sync Implementation)
```
100 Companies Processing:
├── Phase 1 (Google Searches): 150s (sequential)
├── Phase 2 (Domain Probing): 10s (already parallel)  
├── Phase 3 (Email Extraction): 30-60s per company (sequential URLs)
└── Total: ~60+ minutes
```

### After (Async Implementation)  
```
100 Companies Processing:
├── Phase 1 (Google Searches): 3-5s (parallel with rate limiting)
├── Phase 2 (Domain Probing): 5-10s (improved connection pooling)
├── Phase 3 (Email Extraction): 3-5s per company (parallel URLs)  
└── Total: ~6-10 minutes (10x improvement!)
```

## Risk Mitigation

### 1. Gradual Migration
- Implement async versions alongside sync versions
- Use CLI flag `--async` for opt-in testing  
- Keep sync as fallback during transition period

### 2. Rate Limiting & Server Overload
- Implement adaptive rate limiting
- Monitor server response times
- Circuit breaker prevents overwhelming failing servers
- Respect robots.txt and rate limits

### 3. Memory Management
- Connection pool limits (max 100 concurrent connections)
- Request semaphores to control concurrency
- Proper cleanup of async resources

### 4. Error Handling
- Comprehensive exception handling for async operations
- Graceful degradation when async fails
- Detailed logging for debugging async issues

## Testing Strategy

### Unit Tests
- Async circuit breaker functionality
- Rate limiting behavior
- Connection pool management
- Error handling scenarios

### Integration Tests  
- Full async pipeline with test data
- Performance comparison sync vs async
- Memory usage monitoring
- Concurrent request handling

### Performance Benchmarks
```python
async def benchmark_google_searches():
    companies = ["Microsoft", "Apple", "Google"] * 33  # 100 companies
    
    # Sync version timing
    start = time.time()
    sync_results = sync_orchestrator.process_companies(companies)
    sync_time = time.time() - start
    
    # Async version timing  
    start = time.time()
    async_results = await async_orchestrator.process_companies_async(companies)
    async_time = time.time() - start
    
    print(f"Sync: {sync_time:.1f}s | Async: {async_time:.1f}s | Improvement: {sync_time/async_time:.1f}x")
```

## Migration Strategy

### Phase 1: Proof of Concept (This implementation)
- Core async infrastructure
- Google search parallelization  
- Basic performance validation

### Phase 2: Production Integration
- CLI integration with feature flags
- A/B testing with real workloads
- Performance optimization based on results

### Phase 3: Full Migration
- Default to async implementation
- Deprecate sync version
- Remove legacy threading code

## Success Metrics

### Primary Metrics
- **Processing Speed**: 10x improvement for 100+ company datasets
- **Google Search Phase**: 50x improvement (150s → 3s)
- **URL Processing Phase**: 13x improvement (40s → 3s)

### Secondary Metrics  
- **Memory Usage**: Should remain stable or improve
- **Error Rates**: Should remain same or better (circuit breaker helps)
- **Server Impact**: Respectful rate limiting, no server overload

### Validation
- Process same dataset with sync and async versions
- Compare total runtime, error rates, result quality
- Monitor memory usage and connection behavior
- Validate against existing performance monitoring metrics

---

This plan provides a clear path to achieving the 10x performance improvement identified in the performance analysis by leveraging async/await for I/O operations while maintaining all existing reliability features.
