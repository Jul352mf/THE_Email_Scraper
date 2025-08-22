import asyncio
from scraper.async_orchestrator import AsyncOrchestrator, async_process_companies

async def test():
    print("Testing async orchestrator...")
    
    # Test with a small batch
    test_companies = ["Test Company", "Example Corp"]
    
    try:
        stats, results = await async_process_companies(
            test_companies, 
            max_concurrent=2
        )
        
        print(f"Processed {len(test_companies)} companies")
        print(f"Stats: {dict(stats)}")
        print(f"Results: {len(results)} emails found")
        
        # Test individual orchestrator
        async with AsyncOrchestrator(max_concurrent_companies=5) as orchestrator:
            perf_stats = await orchestrator.get_performance_stats()
            print(f"Performance stats available: {list(perf_stats.keys())}")
        
        print("✅ Async orchestrator test passed!")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test())
