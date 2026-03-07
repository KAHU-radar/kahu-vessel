"""FileSource — replays a recorded .nmea fixture file line by line.

Used for testing and offline development. Set line_delay > 0 to simulate
real-time arrival rates.
"""

import asyncio
import logging
from pathlib import Path
from typing import AsyncIterator

from relay.sources import Source

log = logging.getLogger(__name__)


class FileSource(Source):
    def __init__(self, path: str | Path, line_delay: float = 0.0) -> None:
        self.path = Path(path)
        self.line_delay = line_delay

    async def lines(self) -> AsyncIterator[str]:
        log.info("replaying %s", self.path)
        with open(self.path) as f:
            for raw in f:
                yield raw.rstrip("\r\n")
                # sleep(0) yields control to the event loop even with no delay,
                # keeping the generator cooperative with other tasks.
                await asyncio.sleep(self.line_delay)
        log.info("replay complete: %s", self.path)
