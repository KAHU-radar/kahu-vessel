"""Relay tests.

Covers:
  - FileSource: yields every line from a fixture file, CRLF stripped.
  - relay.run: forwards all lines (no filtering) to connected TCP clients.
  - relay.run: multiple clients all receive every line.
  - relay.run: handles client disconnect without crashing or dropping other clients.

Integration tests use QueueSource (defined below) instead of FileSource.
QueueSource blocks the relay's source loop until lines are explicitly fed,
so the test can connect before any data flows — no timing races.
"""

import asyncio
from pathlib import Path
from typing import AsyncIterator

import pytest

from relay.sources import Source
from relay.sources.file_source import FileSource
from relay.relay import run

FIXTURES = Path(__file__).parent / "fixtures"
SAMPLE = FIXTURES / "sample.nmea"

EXPECTED_LINES = [
    "$RATTM,01,1.500,045.0,135.0,5.2,0.50,1.20,,T,TGT01,T*36",
    "$TTTTM,02,2.300,180.0,090.0,3.1,1.00,2.50,,T,TGT02,T*23",
    "$GPHDT,045.0,T*34",
    "$GPRMC,123519,A,4807.038,N,01131.000,E,022.4,084.4,230394,003.1,W*6A",
    "$GPGSV,2,1,08,01,40,083,46,02,17,308,41,12,07,344,39,14,22,228,45*75",
    "$IIDBT,010.5,f,003.2,M,001.7,F*2D",
    "$WIMWV,274.0,R,12.5,N,A*20",
]


# ---------------------------------------------------------------------------
# Test helper: QueueSource
# ---------------------------------------------------------------------------

class QueueSource(Source):
    """Test-only source backed by an asyncio.Queue.

    The relay's source loop blocks at `await self._queue.get()` until the
    test feeds lines via put(). This eliminates timing races: the relay won't
    send any data until the test is ready.
    """

    def __init__(self) -> None:
        self._queue: asyncio.Queue[str | None] = asyncio.Queue()

    async def put(self, line: str) -> None:
        await self._queue.put(line)

    async def finish(self) -> None:
        """Signal EOF; the relay's source loop will exit cleanly."""
        await self._queue.put(None)

    async def lines(self) -> AsyncIterator[str]:
        while True:
            line = await self._queue.get()
            if line is None:
                return
            yield line


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _collect_n(reader: asyncio.StreamReader, n: int) -> list[str]:
    """Read exactly n CRLF-terminated lines from *reader*."""
    lines = []
    for _ in range(n):
        raw = await asyncio.wait_for(reader.readline(), timeout=3.0)
        lines.append(raw.decode().rstrip("\r\n"))
    return lines


async def _feed(source: QueueSource, lines: list[str]) -> None:
    for line in lines:
        await source.put(line)
    await source.finish()


# ---------------------------------------------------------------------------
# FileSource unit tests
# ---------------------------------------------------------------------------

async def test_file_source_yields_all_lines():
    source = FileSource(SAMPLE)
    received = [line async for line in source.lines()]
    assert received == EXPECTED_LINES


async def test_file_source_strips_crlf():
    source = FileSource(SAMPLE)
    async for line in source.lines():
        assert not line.endswith("\r")
        assert not line.endswith("\n")


# ---------------------------------------------------------------------------
# Relay integration tests (QueueSource — no timing races)
# ---------------------------------------------------------------------------

async def test_relay_single_client(unused_tcp_port):
    """Single daemon client receives every line from the source."""
    source = QueueSource()
    task = asyncio.create_task(run(source, sink_port=unused_tcp_port))
    await asyncio.sleep(0.05)  # let the TCP server bind

    reader, writer = await asyncio.open_connection("127.0.0.1", unused_tcp_port)

    received, _ = await asyncio.gather(
        _collect_n(reader, len(EXPECTED_LINES)),
        _feed(source, EXPECTED_LINES),
    )

    writer.close()
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert received == EXPECTED_LINES


async def test_relay_multiple_clients(unused_tcp_port):
    """Every connected client receives every line."""
    source = QueueSource()
    task = asyncio.create_task(run(source, sink_port=unused_tcp_port))
    await asyncio.sleep(0.05)

    r1, w1 = await asyncio.open_connection("127.0.0.1", unused_tcp_port)
    r2, w2 = await asyncio.open_connection("127.0.0.1", unused_tcp_port)

    received1, received2, _ = await asyncio.gather(
        _collect_n(r1, len(EXPECTED_LINES)),
        _collect_n(r2, len(EXPECTED_LINES)),
        _feed(source, EXPECTED_LINES),
    )

    w1.close()
    w2.close()
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert received1 == EXPECTED_LINES
    assert received2 == EXPECTED_LINES


async def test_relay_survives_client_disconnect(unused_tcp_port):
    """Relay keeps forwarding to remaining clients when one disconnects mid-stream."""
    source = QueueSource()
    task = asyncio.create_task(run(source, sink_port=unused_tcp_port))
    await asyncio.sleep(0.05)

    r1, w1 = await asyncio.open_connection("127.0.0.1", unused_tcp_port)
    r2, w2 = await asyncio.open_connection("127.0.0.1", unused_tcp_port)

    async def feed_and_disconnect_r1():
        # Feed line 1 — both clients receive it; then disconnect r1.
        await source.put(EXPECTED_LINES[0])
        await _collect_n(r1, 1)
        w1.close()
        # Feed remaining lines — only r2 is still connected.
        for line in EXPECTED_LINES[1:]:
            await source.put(line)
        await source.finish()

    received, _ = await asyncio.gather(
        _collect_n(r2, len(EXPECTED_LINES)),
        feed_and_disconnect_r1(),
    )

    w2.close()
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass

    assert received == EXPECTED_LINES
