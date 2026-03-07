"""KAHU relay — HAL + TCP server (Seam 1 → Seam 2).

Reads lines from a configured Source and forwards every line to connected
daemon clients over TCP. No filtering, no parsing, no checksum validation.
All NMEA intelligence lives in the daemon.
"""

import asyncio
import logging
import tomllib
from pathlib import Path

from relay.sources import Source, make_source

log = logging.getLogger(__name__)


async def run(source: Source, sink_port: int) -> None:
    clients: set[asyncio.StreamWriter] = set()

    async def handle_client(
        reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        addr = writer.get_extra_info("peername")
        log.info("daemon connected from %s", addr)
        clients.add(writer)
        try:
            await reader.read()  # block until client disconnects
        finally:
            clients.discard(writer)
            writer.close()
            log.info("daemon disconnected from %s", addr)

    server = await asyncio.start_server(handle_client, "0.0.0.0", sink_port)
    log.info("relay listening on :%d", sink_port)

    async with server:
        async for line in source.lines():
            dead: set[asyncio.StreamWriter] = set()
            for writer in clients:
                try:
                    writer.write((line + "\r\n").encode())
                    await writer.drain()
                except OSError:
                    dead.add(writer)
            clients -= dead


def load_config(path: Path) -> dict:
    with open(path, "rb") as f:
        return tomllib.load(f)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )
    config_path = Path(__file__).parent.parent / "config.toml"
    config = load_config(config_path)
    source = make_source(config)
    sink_port = config["sink"]["port"]
    asyncio.run(run(source, sink_port))


if __name__ == "__main__":
    main()
