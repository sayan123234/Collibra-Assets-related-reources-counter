import asyncio
import aiohttp
from typing import List, Dict, Any
import json

async def make_concurrent_requests(urls: List[Dict[str, Any]], auth: aiohttp.BasicAuth, chunk_size: int = 10) -> List[Dict[str, Any]]:
    """
    Make concurrent API requests with rate limiting.
    
    Args:
        urls: List of dictionaries containing URL and request details
        auth: aiohttp BasicAuth credentials
        chunk_size: Number of concurrent requests to make at once
    
    Returns:
        List of response data
    """
    async def fetch(session: aiohttp.ClientSession, request_info: Dict[str, Any]) -> Dict[str, Any]:
        try:
            async with session.request(
                method=request_info.get('method', 'GET'),
                url=request_info['url'],
                params=request_info.get('params'),
                json=request_info.get('json'),
                headers=request_info.get('headers', {'Content-Type': 'application/json'})
            ) as response:
                return await response.json()
        except Exception as e:
            print(f"Error fetching {request_info['url']}: {str(e)}")
            return {'error': str(e)}

    async def process_chunk(session: aiohttp.ClientSession, chunk: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        tasks = [fetch(session, request_info) for request_info in chunk]
        return await asyncio.gather(*tasks)

    results = []
    async with aiohttp.ClientSession(auth=auth) as session:
        for i in range(0, len(urls), chunk_size):
            chunk = urls[i:i + chunk_size]
            chunk_results = await process_chunk(session, chunk)
            results.extend(chunk_results)
            
            # Add a small delay between chunks to prevent overwhelming the server
            if i + chunk_size < len(urls):
                await asyncio.sleep(0.5)

    return results