"""KAHU daemon — Seam 2 client + Seam 3 (runs on Pi).

Connects to the relay over TCP, processes incoming NMEA sentences, fuses
TTM + RMC into absolute target positions, and submits them to TrackServer.

Per line pipeline:
  raw line → preprocess (checksum / normalise / filter)
           → parse (TTM / HDT / RMC)
           → update local state
           → if TTM + RMC available: fuse → submit
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import tomllib
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from daemon.fusion import compute_target_position
from daemon.nmea import HDT, RMC, TTM, parse_hdt, parse_rmc, parse_ttm, preprocess
from daemon.submit import init as _submit_init, submit

log = logging.getLogger(__name__)

_INITIAL_BACKOFF = 1.0
_MAX_BACKOFF = 30.0
_STALENESS_THRESHOLD = 10.0  # seconds; see ARCHITECTURE.md §5


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

@dataclass
class _State:
    last_hdt: HDT | None = None
    last_hdt_at: datetime | None = None
    last_rmc: RMC | None = None
    last_rmc_at: datetime | None = None


def _age(ts: datetime | None) -> float:
    """Seconds since *ts*, or infinity if ts is None."""
    if ts is None:
        return float("inf")
    return (datetime.now(timezone.utc) - ts).total_seconds()


# ---------------------------------------------------------------------------
# Line processing (pure functions — easy to test without network)
# ---------------------------------------------------------------------------

def process_line(line: str, state: _State, use_system_time: bool = False) -> None:
    """Run one raw NMEA line through the full pipeline, updating *state*."""
    sentence = preprocess(line)
    if sentence is None:
        return

    if sentence.startswith("$TTTTM,"):
        ttm = parse_ttm(sentence)
        if ttm is not None:
            _handle_ttm(ttm, state, use_system_time)

    elif sentence.startswith("$GPHDT,"):
        hdt = parse_hdt(sentence)
        if hdt is not None:
            state.last_hdt = hdt
            state.last_hdt_at = datetime.now(timezone.utc)

    elif sentence.startswith("$GPRMC,"):
        rmc = parse_rmc(sentence)
        if rmc is not None:
            state.last_rmc = rmc
            state.last_rmc_at = datetime.now(timezone.utc)


def _handle_ttm(ttm: TTM, state: _State, use_system_time: bool = False) -> None:
    if state.last_rmc is None:
        log.debug("no RMC fix yet — skipping TTM target %02d", ttm.number)
        return

    # Warn on stale data but still compute — better than silently dropping.
    if _age(state.last_rmc_at) > _STALENESS_THRESHOLD:
        log.warning("RMC fix is stale (%.0fs old)", _age(state.last_rmc_at))
    if _age(state.last_hdt_at) > _STALENESS_THRESHOLD:
        log.warning("HDT is stale (%.0fs old)", _age(state.last_hdt_at))

    lat, lon = compute_target_position(state.last_rmc, ttm)
    timestamp = datetime.now(timezone.utc) if use_system_time else state.last_rmc.timestamp
    submit(ttm, lat, lon, timestamp)


# ---------------------------------------------------------------------------
# TCP client
# ---------------------------------------------------------------------------

async def run(relay_host: str, relay_port: int, use_system_time: bool = False) -> None:
    state = _State()
    backoff = _INITIAL_BACKOFF

    while True:
        try:
            reader, _ = await asyncio.open_connection(relay_host, relay_port)
            log.info("connected to relay at %s:%d", relay_host, relay_port)
            backoff = _INITIAL_BACKOFF

            async for raw in reader:
                line = raw.decode(errors="replace").rstrip("\r\n")
                process_line(line, state, use_system_time)

            log.warning("relay connection closed")

        except (ConnectionRefusedError, OSError) as exc:
            log.warning(
                "cannot connect to relay — %s; retrying in %.0fs", exc, backoff
            )

        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, _MAX_BACKOFF)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def load_config(path: Path) -> dict:
    with open(path, "rb") as f:
        return tomllib.load(f)


def _resolve_config_path() -> Path:
    """Return config path: ~/.kahu/config.toml if it exists, else package default."""
    user_config = Path.home() / ".kahu" / "config.toml"
    if user_config.exists():
        return user_config
    return Path(__file__).parent.parent / "config.toml"


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )

    parser = argparse.ArgumentParser(description="KAHU vessel daemon")
    parser.add_argument("--api-key", metavar="UUID", help="API key (overrides config and KAHU_API_KEY env var)")
    parser.add_argument("--config", metavar="PATH", help="Path to config.toml (default: ~/.kahu/config.toml)")
    args = parser.parse_args()

    config_path = Path(args.config) if args.config else _resolve_config_path()
    config = load_config(config_path)
    log.info("using config: %s", config_path)
    relay_host = config["daemon"]["relay_host"]
    relay_port = config["sink"]["port"]
    use_system_time = config["daemon"].get("use_system_time", False)

    upload_cfg = config.get("upload", {})
    api_key = args.api_key or os.environ.get("KAHU_API_KEY") or upload_cfg.get("api_key", "")
    if api_key:
        _submit_init(
            host=upload_cfg.get("host", "crowdsource.kahu.earth"),
            port=int(upload_cfg.get("port", 9900)),
            api_key=api_key,
            points_per_track=int(upload_cfg.get("points_per_track", 10)),
        )
    else:
        log.warning("KAHU_API_KEY not set — upload disabled (log-only mode)")

    asyncio.run(run(relay_host, relay_port, use_system_time))


if __name__ == "__main__":
    main()
