"""HTTP access to the Hacker News API (https://github.com/HackerNews/API).

Returns raw bytes for items: validation is the model's job (models/hn.py), so this module stays
a thin, easily-mocked I/O layer. Any non-2xx status or transport error raises; there are no
retries until Phase 3.
"""

import httpx

from ingest.models.hn import parse_top_ids


async def fetch_top_ids(client: httpx.AsyncClient, base_url: str, *, limit: int) -> list[int]:
    """GET /topstories.json and return the first `limit` ids, in HN's ranking order."""
    response = await client.get(f"{base_url}/topstories.json")
    response.raise_for_status()
    return parse_top_ids(response.content)[:limit]


async def fetch_item(client: httpx.AsyncClient, base_url: str, item_id: int) -> bytes:
    """GET /item/<id>.json and return the body unparsed (may be the JSON literal `null`)."""
    response = await client.get(f"{base_url}/item/{item_id}.json")
    response.raise_for_status()
    return response.content
