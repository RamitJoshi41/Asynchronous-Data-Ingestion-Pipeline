"""Local HN-shaped HTTP server for benchmarks. A MOCK: synthetic items, injected latency.

    uv run python -m benchmarks.mock_hn --port 8765 --items 100 --latency-ms 50

Serves /v0/topstories.json, /v0/item/<id>.json (JSON `null` for unknown ids) and /health.
Responses are pre-encoded JSON so FastAPI's own validation doesn't add server-side cost.
Explainer: docs/explain/benchmarks/mock_hn.md   Decision: ADR-0012
"""

import argparse
import asyncio
import json

import uvicorn
from fastapi import FastAPI, Response

JSON = "application/json"


def synthetic_item(item_id: int) -> dict[str, object]:
    """Deterministic story roughly the size and shape of a real HN item (valid for HNItem)."""
    return {
        "by": f"user{item_id % 50}",
        "descendants": item_id % 40,
        "id": item_id,
        "kids": [item_id * 10 + k for k in range(item_id % 5)],
        "score": item_id % 500,
        "time": 1_700_000_000 + item_id,
        "title": f"Synthetic story number {item_id} for benchmarking",
        "type": "story",
        "url": f"https://example.com/story/{item_id}",
    }


def create_app(*, n_items: int, latency_s: float) -> FastAPI:
    app = FastAPI(openapi_url=None)  # no docs routes: keep the server minimal
    top_ids = json.dumps(list(range(1, n_items + 1))).encode()

    @app.get("/health")
    async def health() -> Response:
        return Response(content=b'{"ok": true}', media_type=JSON)

    @app.get("/v0/topstories.json")
    async def topstories() -> Response:
        await asyncio.sleep(latency_s)  # non-blocking: the server stays concurrent
        return Response(content=top_ids, media_type=JSON)

    @app.get("/v0/item/{item_id}.json")
    async def item(item_id: int) -> Response:
        await asyncio.sleep(latency_s)
        body = synthetic_item(item_id) if 1 <= item_id <= n_items else None
        return Response(content=json.dumps(body).encode(), media_type=JSON)

    return app


def main() -> None:
    parser = argparse.ArgumentParser(description="Mock HN API for benchmarks (synthetic data).")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--items", type=int, default=100)
    parser.add_argument("--latency-ms", type=float, default=50.0)
    args = parser.parse_args()
    app = create_app(n_items=args.items, latency_s=args.latency_ms / 1000)
    uvicorn.run(app, host="127.0.0.1", port=args.port, log_level="warning", access_log=False)


if __name__ == "__main__":
    main()
