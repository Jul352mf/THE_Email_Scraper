import asyncio
from scraper.async_google_search import AsyncGoogleSearchClient

async def test():
    async with AsyncGoogleSearchClient(max_concurrent_requests=2) as client:
        # Test single search
        result = await client.search_single('test query')
        print(f'Single search returned {len(result)} results')
        
        # Test batch search with 2 queries
        results = await client.search_batch(['test company A', 'test company B'])
        print(f'Batch search returned {len(results)} result sets: {[len(r) for r in results]}')
        
        # Test company search
        company_results = await client.search_companies(['Test Corp'])
        print(f'Company search returned {len(company_results)} result sets: {[len(r) for r in company_results]}')
        
        # Get performance stats
        stats = client.get_performance_stats()
        hit_rate = stats['cache_performance']['hit_rate_percent']
        print(f'Cache hit rate: {hit_rate}%')

if __name__ == "__main__":
    asyncio.run(test())
