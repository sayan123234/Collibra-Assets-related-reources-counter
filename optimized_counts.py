import asyncio
import aiohttp
from typing import List, Dict
from async_utils import make_concurrent_requests

async def get_counts_async(asset_ids: List[str], base_url: str, bearer_token: str) -> Dict[str, Dict[str, int]]:
    """
    Get all counts concurrently using OAuth authentication.
    """
    headers = {
        'Authorization': f'Bearer {bearer_token}',
        'Content-Type': 'application/json'
    }
    
    # Prepare all requests
    requests = []
    
    # Attributes requests
    for asset_id in asset_ids:
        requests.append({
            'url': f"{base_url}/rest/2.0/attributes",
            'params': {
                "offset": "0",
                "limit": "0",
                "countLimit": "-1",
                "assetId": asset_id,
                "sortOrder": "DESC",
                "sortField": "LAST_MODIFIED"
            },
            'headers': headers,
            'type': 'attribute',
            'asset_id': asset_id
        })
    
    # Incoming relations requests
    for asset_id in asset_ids:
        requests.append({
            'url': f"{base_url}/rest/2.0/relations",
            'params': {
                "offset": "0",
                "limit": "0",
                "countLimit": "-1",
                "targetId": asset_id,
                "sourceTargetLogicalOperator": "AND"
            },
            'headers': headers,
            'type': 'incoming',
            'asset_id': asset_id
        })
    
    # Outgoing relations requests
    for asset_id in asset_ids:
        requests.append({
            'url': f"{base_url}/rest/2.0/relations",
            'params': {
                "offset": "0",
                "limit": "0",
                "countLimit": "-1",
                "sourceId": asset_id,
                "sourceTargetLogicalOperator": "AND"
            },
            'headers': headers,
            'type': 'outgoing',
            'asset_id': asset_id
        })
    
    # Responsibilities requests
    for asset_id in asset_ids:
        requests.append({
            'url': f"{base_url}/rest/2.0/responsibilities",
            'params': {
                "offset": "0",
                "limit": "0",
                "countLimit": "-1",
                "resourceIds": asset_id,
                "includeInherited": "true",
                "sortField": "LAST_MODIFIED",
                "sortOrder": "DESC"
            },
            'headers': headers,
            'type': 'responsibility',
            'asset_id': asset_id
        })
    
    # Make concurrent requests without auth (since we're using bearer token in headers)
    responses = await make_concurrent_requests(requests, None)
    
    # Process results
    results = {asset_id: {'attributes': 0, 'incoming': 0, 'outgoing': 0, 'responsibilities': 0} 
              for asset_id in asset_ids}
    
    for request, response in zip(requests, responses):
        asset_id = request['asset_id']
        count_type = request['type']
        
        if 'total' in response:
            if count_type == 'attribute':
                results[asset_id]['attributes'] = response['total']
            elif count_type == 'incoming':
                results[asset_id]['incoming'] = response['total']
            elif count_type == 'outgoing':
                results[asset_id]['outgoing'] = response['total']
            elif count_type == 'responsibility':
                results[asset_id]['responsibilities'] = response['total']
    
    return results

def get_all_counts(asset_ids: List[str], base_url: str, bearer_token: str) -> Dict[str, Dict[str, int]]:
    """
    Synchronous wrapper for async get_counts function.
    """
    return asyncio.run(get_counts_async(asset_ids, base_url, bearer_token))