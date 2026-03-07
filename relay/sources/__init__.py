"""Source ABC and factory.

A Source is an async line generator — callers do:

    async for line in source.lines():
        ...

Lines are yielded as raw strings with the trailing CRLF stripped.
"""

from abc import ABC, abstractmethod
from typing import AsyncIterator


class Source(ABC):
    @abstractmethod
    def lines(self) -> AsyncIterator[str]:
        ...


def make_source(config: dict) -> Source:
    """Construct the Source specified in *config*."""
    kind = config["relay"]["source"]
    if kind == "tcp":
        from relay.sources.tcp_source import TCPSource
        return TCPSource(config["relay"]["tcp_host"], config["relay"]["tcp_port"])
    elif kind == "file":
        from relay.sources.file_source import FileSource
        return FileSource(config["relay"]["file_path"])
    elif kind == "serial":
        from relay.sources.serial_source import SerialSource
        return SerialSource(config["relay"]["serial_device"])
    elif kind == "udp":
        from relay.sources.udp_source import UDPSource
        return UDPSource(config["relay"]["udp_port"])
    else:
        raise ValueError(f"Unknown source type: {kind!r}")
