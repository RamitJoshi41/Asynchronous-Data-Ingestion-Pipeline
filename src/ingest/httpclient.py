"""The shared httpx.AsyncClient factory (ADR-0010). Every source uses one client built here."""

import httpx

from ingest.config import Settings

# Identify ourselves honestly to API operators; no browser impersonation (CLAUDE.md §8).
USER_AGENT = "PipeLine/0.1 (student data-ingestion project; python-httpx)"


def http_limits(settings: Settings) -> httpx.Limits:
    """Connection limits matched to our own fetch concurrency (ADR-0019).

    httpx's defaults are 100 connections but only 20 kept alive: at concurrency 50 that would
    close and re-open (TCP + TLS) 30 connections over and over. Our fetcher pool is the real
    limit; httpx's pool is sized so it never becomes a hidden second bottleneck.
    """
    return httpx.Limits(
        max_connections=settings.fetch_concurrency,
        max_keepalive_connections=settings.fetch_concurrency,
    )


def build_http_client(settings: Settings) -> httpx.AsyncClient:
    """Create the client. The caller owns it: use `async with build_http_client(s) as client:`."""
    timeout = httpx.Timeout(
        connect=settings.http_connect_timeout_s,  # TCP + TLS handshake
        read=settings.http_read_timeout_s,  # max gap between received bytes
        write=settings.http_write_timeout_s,  # max gap between sent bytes
        pool=settings.http_pool_timeout_s,  # waiting for a free connection in httpx's own pool
    )
    return httpx.AsyncClient(
        timeout=timeout,
        limits=http_limits(settings),
        headers={"User-Agent": USER_AGENT},
        # An API that suddenly redirects is a change we want to see, not silently follow.
        follow_redirects=False,
    )
