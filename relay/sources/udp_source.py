"""UDPSource — receives NMEA datagrams from a navigation system (e.g. Bridge Command).

Bridge Command sends one or more NMEA sentences per UDP packet. This source
binds to a local port and yields complete, stripped lines from every packet.
"""

import asyncio
import logging
import socket
from typing import AsyncIterator

from relay.sources import Source

log = logging.getLogger(__name__)

_RECV_BUFFER = 4096


class UDPSource(Source):
    def __init__(self, port: int) -> None:
        self.port = port

    async def lines(self) -> AsyncIterator[str]:
        loop = asyncio.get_event_loop()

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("0.0.0.0", self.port))
        sock.setblocking(False)
        log.info("listening for NMEA on UDP port %d", self.port)

        try:
            while True:
                data = await loop.sock_recv(sock, _RECV_BUFFER)
                # A single UDP packet may contain multiple NMEA sentences.
                for line in data.decode(errors="replace").splitlines():
                    line = line.strip()
                    if line:
                        yield line
        finally:
            sock.close()
