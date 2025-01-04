import asyncio
import aiohttp
from typing import List
import json

async def get_all_assets_async(asset_type_id: str, base_url: str, bearer_token: str) -> List[str]:
    """
    Get all asset IDs using async GraphQL query with OAuth authentication.
    """
    url = f"{base_url}/graphql/knowledgeGraph/v1"
    
    query = """
    query Assets($typeId: UUID!) {
        assets(
            where: { type: { id: { eq: $typeId } } }
            limit: 50000
        ) {
            id
        }
    }
    """
    
    payload = {
        "query": query,
        "variables": {"typeId": asset_type_id}
    }
    
    headers = {
        'Authorization': f'Bearer {bearer_token}',
        'Content-Type': 'application/json'
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            print(f"Making request to {url}")
            print(f"Query payload: {json.dumps(payload, indent=2)}")
            
            async with session.post(url, json=payload, headers=headers) as response:
                print(f"Response status: {response.status}")
                
                if response.status != 200:
                    print(f"HTTP Error: {response.status}")
                    response_text = await response.text()
                    print(f"Response body: {response_text}")
                    return []
                    
                data = await response.json()
                
                if "errors" in data:
                    print("GraphQL Errors:", json.dumps(data["errors"], indent=2))
                    return []
                    
                if "data" in data and "assets" in data["data"]:
                    assets = data["data"]["assets"]
                    print(f"Successfully retrieved {len(assets)} assets")
                    return [asset["id"] for asset in assets]
                else:
                    print(f"Unexpected response structure: {json.dumps(data, indent=2)}")
                    return []
                
    except aiohttp.ClientError as e:
        print(f"Network error in get_all_assets: {str(e)}")
        return []
    except Exception as e:
        print(f"Unexpected error in get_all_assets: {str(e)}")
        print(f"Error type: {type(e)}")
        return []

def get_all_assets(asset_type_id: str, base_url: str, bearer_token: str) -> List[str]:
    """
    Synchronous wrapper for async get_all_assets function.
    """
    return asyncio.run(get_all_assets_async(asset_type_id, base_url, bearer_token))