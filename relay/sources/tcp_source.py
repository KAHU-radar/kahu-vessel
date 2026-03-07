"""TCPSource — connects to a remote NMEA TCP server (e.g. Bridge Command).

Reconnects automatically with exponential backoff on connection loss.
"""

import asyncio
import logging
from typing import AsyncIterator

from relay.sources import Source

log = logging.getLogger(__name__)

_INITIAL_BACKOFF = 1.0
_MAX_BACKOFF = 30.0


class TCPSource(Source):
    def __init__(self, host: str, port: int) -> None:
        self.host = host
        self.port = port

    async def lines(self) -> AsyncIterator[str]:
        backoff = _INITIAL_BACKOFF
        while True:
            try:
                reader, _ = await asyncio.open_connection(self.host, self.port)
                log.info("connected to %s:%d", self.host, self.port)
                backoff = _INITIAL_BACKOFF  # reset on successful connect
                async for raw in reader:
                    yield raw.decode(errors="replace").rstrip("\r\n")
                log.warning("connection closed by remote")
            except (ConnectionRefusedError, OSError) as exc:
                log.warning(
                    "cannot connect to %s:%d — %s; retrying in %.0fs",
                    self.host, self.port, exc, backoff,
                )
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, _MAX_BACKOFF)
