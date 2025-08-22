 1. Google API Sequential Processing with Rate Limiting        │ │
│ │ - Current: 0.8s mandatory delay between each Google search    │ │
│ │ (GOOGLE_SAFE_INTERVAL)                                        │ │
│ │ - Impact: For 1000 companies = 800+ seconds just in API       │ │
│ │ delays                                                        │ │
│ │ - Root cause: google_search.py:141-151 - synchronous rate     │ │
│ │ limiting with thread locks                                    │ │
│ │                                                               │ │
│ │ 2. Thread Pool Nesting & Synchronization Overhead             │ │
│ │ - Current: Multiple nested ThreadPoolExecutors in             │ │
│ │ orchestrator.py:165-169                                       │ │
│ │ - Impact: Thread creation/destruction overhead, context       │ │
│ │ switching, lock contention                                    │ │
│ │ - Root cause: Mixed sync/async architecture causing blocking  │ │
│ │ operations                                                    │ │
│ │                                                               │ │
│ │ 3. HTTP Client Session Management Inefficiency                │ │
│ │ - Current: Per-domain sessions with limited connection        │ │
│ │ pooling (http_client.py:397-427)                              │ │
│ │ - Impact: Connection setup/teardown overhead, no HTTP/2       │ │
│ │ multiplexing                                                  │ │
│ │ - Root cause: Requests library limitations, no async I/O      │ │
│ │                                                               │ │
│ │ 4. Memory Accumulation in Global State                        │ │
│ │ - Current: _global_seen and _global_in_progress sets grow     │ │
│ │ indefinitely (orchestrator.py:30-31)                          │ │
│ │ - Impact: Memory leaks causing GC pressure and slowdowns over │ │
│ │  time                                                         │ │
│ │ - Root cause: No cleanup mechanism for processed domains      │ │
│ │                                                               │ │
│ │ 5. TokenBucket Rate Limiting Implementation                   │ │
│ │ - Current: Per-domain token buckets with thread locks         │ │
│ │ (http_client.py:308-347)                                      │ │
│ │ - Impact: Lock contention when processing many domains        │ │
│ │ concurrently                                                  │ │
│ │ - Root cause: Inefficient lock-based implementation           │ │
│ │                                                               │ │
│ │ 🟡 SECONDARY BOTTLENECKS (2-10x slowdown impact)              │ │
│ │                                                               │ │
│ │ 6. Email Extraction Multiple Regex Passes                     │ │
│ │ - Current: Multiple regex compilations and HTML parsing       │ │
│ │ passes                                                        │ │
│ │ - Impact: CPU waste on repetitive pattern matching            │ │
│ │ - Root cause: Non-optimized pattern caching                   │ │
│ │                                                               │ │
│ │ 7. Sitemap XML Processing Overhead                            │ │
│ │ - Current: DOM parsing instead of streaming (sitemap.py)      │ │
│ │ - Impact: Memory usage spikes, slow parsing of large sitemaps │ │
│ │ - Root cause: BeautifulSoup XML parsing                       │ │
│ │                                                               │ │
│ │ 8. No Request Batching or Multiplexing                        │ │
│ │ - Current: Sequential HTTP requests even within threads       │ │
│ │ - Impact: Network round-trip delays accumulate                │ │
│ │ - Root cause: Synchronous HTTP architecture                   │ │
│ │                                                               │ │
│ │ 100x Performance Optimization Implementation Plan             │ │
│ │                                                               │ │
│ │ Phase 1: Foundation Improvements (5-10x gains)                │ │
│ │                                                               │ │
│ │ Estimated Timeline: 1-2 weeks                                 │ │
│ │                                                               │ │
│ │ 1. Async HTTP Architecture Migration                          │ │
│ │   - Replace requests with aiohttp for async I/O               │ │
│ │   - Implement HTTP/2 connection pooling and multiplexing      │ │
│ │   - Create async versions of all HTTP operations              │ │
│ │ 2. Advanced Caching Layer Implementation                      │ │
│ │   - Multi-tier caching: Memory → Redis → PostgreSQL           │ │
│ │   - Google API result caching with 24h TTL                    │ │
│ │   - Domain pattern caching with intelligent refresh           │ │
│ │   - HTTP response caching with compression                    │ │
│ │ 3. Memory Management & State Cleanup                          │ │
│ │   - Implement periodic cleanup of global domain tracking sets │ │
│ │   - Add LRU eviction for connection pools and caches          │ │
│ │   - Memory-mapped file processing for large datasets          │ │
│ │                                                               │ │
│ │ Phase 2: Algorithmic Optimizations (10-20x gains)             │ │
│ │                                                               │ │
│ │ Estimated Timeline: 2-3 weeks                                 │ │
│ │                                                               │ │
│ │ 4. Concurrent Domain Processing Pipeline                      │ │
│ │   - Implement true async/await pipeline for domain processing │ │
│ │   - Batch Google API requests where possible                  │ │
│ │   - Parallel sitemap downloading and parsing                  │ │
│ │   - Concurrent email extraction across domains                │ │
│ │ 5. Smart Request Batching & Multiplexing                      │ │
│ │   - HTTP/2 request multiplexing for same-domain requests      │ │
│ │   - Request queue with intelligent batching                   │ │
│ │   - Connection warming and reuse optimization                 │ │
│ │ 6. Optimized Email Extraction                                 │ │
│ │   - Single-pass regex compilation and caching                 │ │
│ │   - Streaming HTML/PDF processing                             │ │
│ │   - Parallel email validation and deduplication               │ │
│ │                                                               │ │
│ │ Phase 3: Advanced Architecture (50-100x gains)                │ │
│ │                                                               │ │
│ │ Estimated Timeline: 3-4 weeks                                 │ │
│ │                                                               │ │
│ │ 7. Distributed Processing System                              │ │
│ │   - Task queue implementation (Redis/RabbitMQ)                │ │
│ │   - Horizontal worker scaling                                 │ │
│ │   - Load balancing and failover                               │ │
│ │ 8. Database-Backed Persistence                                │ │
│ │   - PostgreSQL for result storage and caching                 │ │
│ │   - Advanced query optimization                               │ │
│ │   - Cross-session result sharing                              │ │
│ │ 9. Machine Learning Optimization                              │ │
│ │   - Domain relevance scoring ML model                         │ │
│ │   - Email extraction priority learning                        │ │
│ │   - Dynamic stopping criteria optimization                    │ │
│ │                                                               │ │
│ │ Technical Implementation Details                              │ │
│ │                                                               │ │
│ │ New Architecture Components                                   │ │
│ │                                                               │ │
│ │ AsyncHttpClient (replaces http_client.py)                     │ │
│ │ class AsyncHttpClient:                                        │ │
│ │     def __init__(self):                                       │ │
│ │         self.connector = aiohttp.TCPConnector(                │ │
│ │             limit=500,  # Total connection pool               │ │
│ │             limit_per_host=20,  # Per domain                  │ │
│ │             ttl_dns_cache=300,                                │ │
│ │             use_dns_cache=True,                               │ │
│ │             enable_cleanup_closed=True                        │ │
│ │         )                                                     │ │
│ │                                                               │ │
│ │ MultiLayerCache (new caching system)                          │ │
│ │ class MultiLayerCache:                                        │ │
│ │     - L1: In-memory LRU (fast access)                         │ │
│ │     - L2: Redis (shared across instances)                     │ │
│ │     - L3: PostgreSQL (persistent, searchable)                 │ │
│ │                                                               │ │
│ │ BatchProcessor (new concurrent processing)                    │ │
│ │ async def process_companies_batch(companies: List[str]):      │ │
│ │     # Batch Google searches                                   │ │
│ │     search_tasks = [google_client.search_async(c) for c in    │ │
│ │ companies]                                                    │ │
│ │     search_results = await asyncio.gather(*search_tasks)      │ │
│ │                                                               │ │
│ │     # Concurrent domain processing                            │ │
│ │     domain_tasks = [process_domain_async(result) for result   │ │
│ │ in search_results]                                            │ │
│ │     return await asyncio.gather(*domain_tasks)                │ │
│ │                                                               │ │
│ │ Performance Monitoring Integration                            │ │
│ │                                                               │ │
│ │ The codebase already has performance monitoring hooks in      │ │
│ │ cli.py:483-484. We'll enhance this with:                      │ │
│ │                                                               │ │
│ │ - Real-time throughput metrics                                │ │
│ │ - Memory usage tracking                                       │ │
│ │ - Cache hit/miss ratios                                       │ │
│ │ - Request latency percentiles                                 │ │
│ │ - Bottleneck identification                                   │ │
│ │                                                               │ │
│ │ Risk Mitigation Strategy                                      │ │
│ │                                                               │ │
│ │ 1. Backward Compatibility: Maintain existing sync API during  │ │
│ │ transition                                                    │ │
│ │ 2. Feature Flags: Enable/disable optimizations individually   │ │
│ │ 3. Comprehensive Testing: Performance benchmarks at each      │ │
│ │ phase                                                         │ │
│ │ 4. Gradual Migration: Module-by-module async conversion       │ │
│ │                                                               │ │
│ │ Expected Performance Gains                                    │ │
│ │                                                               │ │
│ │ | Optimization    | Current Time | Optimized Time | Speedup | │ │
│ │ |-----------------|--------------|----------------|---------| │ │
│ │ | 100 companies   | ~300s        | ~30s           | 10x     | │ │
│ │ | 1000 companies  | ~3000s       | ~30-60s        | 50-100x | │ │
│ │ | 10000 companies | ~30000s      | ~300-600s      | 50-100x | │ │
│ │                                                               │ │
│ │ Implementation Priority                                       │ │
│ │                                                               │ │
│ │ Week 1-2: Async HTTP client + basic caching (immediate 5-10x  │ │
│ │ gains)                                                        │ │
│ │ Week 3-4: Concurrent processing + request batching            │ │
│ │ (additional 2-5x gains)                                       │ │
│ │ Week 5-8: Advanced architecture + distributed processing      │ │
│ │ (additional 10-20x gains)                                     │ │
│ │                                                               │ │
│ │ This plan addresses the root causes of slowdowns while        │ │
│ │ providing a clear migration path and measurable performance   │ │
│ │ improvements at each phase.