import asyncio
from scraper.async_http_client import AsyncHttpClient

async def test():
    async with AsyncHttpClient(max_concurrent_requests=5) as client:
        # Test single request
        response = await client.safe_get('https://httpbin.org/json')
        if response:
            print(f'Single request: HTTP {response.status}')
        else:
            print('Single request failed')
        
        # Test batch requests
        test_urls = [
            'https://httpbin.org/status/200',
            'https://httpbin.org/delay/1', 
            'https://httpbin.org/headers'
        ]
        
        results = await client.batch_get(test_urls)
        successful = sum(1 for _, resp in results if resp is not None)
        print(f'Batch request: {successful}/{len(test_urls)} successful')
        
        # Test domain probing
        pattern = await client.probe_domain_access('example.com')
        if pattern:
            print(f'Domain probe: Found pattern {pattern.method.value}')
        else:
            print('Domain probe: No pattern found')
        
        # Get performance stats
        stats = await client.get_performance_stats()
        print(f'Performance: {stats["request_stats"]["total_requests"]} total requests')

if __name__ == "__main__":
    asyncio.run(test())
