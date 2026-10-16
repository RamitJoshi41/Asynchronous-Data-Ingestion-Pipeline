import asyncio

import pytest
from structlog.testing import capture_logs

from ingest.toy_pipeline import FetchError, ToyPipeline

# Every run is wrapped in a timeout: a shutdown bug shows up as a hang, and a hang
# should fail the test instead of freezing the suite.
TIMEOUT_S = 3.0


def other_tasks() -> set[asyncio.Task[object]]:
    return asyncio.all_tasks() - {asyncio.current_task()}


async def test_semaphore_limit_is_reached_and_never_exceeded() -> None:
    # 50 items arrive at once, so all 5 slots must fill: assert ==, not <=.
    pipeline = ToyPipeline(concurrency=5, queue_maxsize=100, num_workers=5, fetch_delay=0.02)
    async with asyncio.timeout(TIMEOUT_S):
        report = await pipeline.run(range(50))
    assert report.peak_in_flight == 5
    assert report.processed == tuple(range(50))


async def test_concurrency_one_is_sequential_fetching() -> None:
    pipeline = ToyPipeline(concurrency=1, queue_maxsize=100, num_workers=2)
    async with asyncio.timeout(TIMEOUT_S):
        report = await pipeline.run(range(10))
    assert report.peak_in_flight == 1
    assert report.processed == tuple(range(10))


async def test_slow_consumer_fills_queue_and_blocks_producers() -> None:
    pipeline = ToyPipeline(
        concurrency=10, queue_maxsize=5, num_workers=1, fetch_delay=0.001, process_delay=0.02
    )
    with capture_logs() as logs:
        async with asyncio.timeout(TIMEOUT_S):
            report = await pipeline.run(range(20))

    assert report.peak_queue_depth == 5  # filled to the bound, never past it
    assert report.blocked_puts > 0
    blocked_events = [e for e in logs if e["event"] == "producer_blocked"]
    assert len(blocked_events) == report.blocked_puts
    assert all(e["queue_depth"] == 5 for e in blocked_events)
    assert report.processed == tuple(range(20))  # backpressure slows, never drops


async def test_invalid_record_is_recorded_and_run_continues() -> None:
    pipeline = ToyPipeline(concurrency=5, queue_maxsize=10, num_workers=2, fail_process={5, 8})
    async with asyncio.timeout(TIMEOUT_S):
        report = await pipeline.run(range(10))
    assert report.failed == (5, 8)
    assert report.processed == (0, 1, 2, 3, 4, 6, 7, 9)


async def test_fetch_failure_aborts_run_and_leaks_no_tasks() -> None:
    # Regression test: the Phase 0 draft used gather(), which left 9 fetchers running here.
    pipeline = ToyPipeline(
        concurrency=2, queue_maxsize=10, num_workers=2, fetch_delay=0.02, fail_fetch={0}
    )
    with pytest.RaisesGroup(FetchError, flatten_subgroups=True):
        async with asyncio.timeout(TIMEOUT_S):
            await pipeline.run(range(10))
    assert other_tasks() == set()


async def test_unexpected_worker_error_aborts_run_even_with_blocked_producers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Tiny queue + crashing worker: producers are blocked on put() when the worker dies.
    # Without TaskGroup cancellation they would wait forever for space that never frees.
    pipeline = ToyPipeline(concurrency=10, queue_maxsize=1, num_workers=1, fetch_delay=0.001)

    async def crash(item: int) -> None:
        await asyncio.sleep(0.01)
        raise RuntimeError("bug in worker")

    monkeypatch.setattr(pipeline, "_process", crash)
    with pytest.RaisesGroup(RuntimeError):
        async with asyncio.timeout(TIMEOUT_S):
            await pipeline.run(range(20))
    assert other_tasks() == set()


async def test_sentinels_stop_every_worker_after_queue_drains() -> None:
    pipeline = ToyPipeline(concurrency=4, queue_maxsize=3, num_workers=4)
    with capture_logs() as logs:
        async with asyncio.timeout(TIMEOUT_S):
            report = await pipeline.run(range(25))
    stopped = sorted(e["worker_id"] for e in logs if e["event"] == "worker_stopped")
    assert stopped == [0, 1, 2, 3]
    assert report.processed == tuple(range(25))  # each item exactly once, none lost
    assert other_tasks() == set()


async def test_empty_input_finishes() -> None:
    pipeline = ToyPipeline(concurrency=2, queue_maxsize=2, num_workers=3)
    async with asyncio.timeout(TIMEOUT_S):
        report = await pipeline.run([])
    assert report.processed == ()
    assert report.blocked_puts == 0


async def test_pipeline_is_single_use() -> None:
    pipeline = ToyPipeline(concurrency=1, queue_maxsize=1, num_workers=1)
    await pipeline.run([1])
    with pytest.raises(RuntimeError, match="single-use"):
        await pipeline.run([1])


@pytest.mark.parametrize(
    ("concurrency", "queue_maxsize", "num_workers"),
    [(0, 1, 1), (1, 0, 1), (1, 1, 0)],
)
def test_rejects_non_positive_limits(
    concurrency: int, queue_maxsize: int, num_workers: int
) -> None:
    with pytest.raises(ValueError):
        ToyPipeline(concurrency=concurrency, queue_maxsize=queue_maxsize, num_workers=num_workers)
