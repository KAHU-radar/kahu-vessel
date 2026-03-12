"""KAHU daemon — Seam 2 client + Seam 3 (runs on Pi).

Connects to the relay over TCP, processes incoming NMEA sentences, fuses
TTM + RMC into absolute target positions, and submits them to TrackServer.

Pipeline (one NMEA line at a time):

  relay TCP
    → preprocess()       nmea.py   checksum / normalise / type-filter
    → parse()            nmea.py   TTM / HDT / RMC → typed dataclasses
    → update _State               store latest fix + heading
    → compute_target_position()
                         fusion.py range + true-bearing → lat/lon
    → smooth_position()  daemon.py per-target rolling average (kill quant. noise)
    → submit()           submit.py batch + upload to TrackServer

If you need to add something, ask yourself which stage it belongs to:
  • New NMEA sentence type?  → nmea.py
  • Different coordinate math?  → fusion.py
  • Filter on the output position?  → smooth_position() in this file
  • Upload / batching logic?  → submit.py
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import math
import os
import time
import tomllib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from daemon.fusion import compute_target_position
from daemon.nmea import HDT, RMC, TTM, parse_hdt, parse_rmc, parse_ttm, preprocess
from daemon.submit import init as _submit_init, submit

log = logging.getLogger(__name__)

_INITIAL_BACKOFF = 1.0
_MAX_BACKOFF = 30.0
_STALENESS_THRESHOLD = 10.0  # seconds; see ARCHITECTURE.md §5

# Measurement weight for smooth_position().
# 1.0 = raw measurement (no smoothing).  Lower = smoother but slightly more lag.
_SMOOTH_ALPHA = 0.25


# ---------------------------------------------------------------------------
# State
# ---------------------------------------------------------------------------

@dataclass
class _TargetTrack:
    """Dead-reckoning state for one ARPA target, used by smooth_position()."""
    lat: float          # last smoothed position (degrees)
    lon: float
    vel_lat: float      # velocity (degrees/second)
    vel_lon: float
    last_t: float       # time.monotonic() of last update


@dataclass
class _State:
    last_hdt: HDT | None = None
    last_hdt_at: datetime | None = None
    last_rmc: RMC | None = None
    last_rmc_at: datetime | None = None
    # Per-target dead-reckoning state for smooth_position().
    target_tracks: dict[int, _TargetTrack] = field(default_factory=dict)


def _age(ts: datetime | None) -> float:
    """Seconds since *ts*, or infinity if ts is None."""
    if ts is None:
        return float("inf")
    return (datetime.now(timezone.utc) - ts).total_seconds()


# ---------------------------------------------------------------------------
# Position smoother
# ---------------------------------------------------------------------------

def smooth_position(
    state: _State, target_num: int,
    lat: float, lon: float,
    speed_kts: float, course_deg: float,
) -> tuple[float, float]:
    """Velocity-assisted complementary filter for one ARPA target.

    Uses the TTM speed/course to dead-reckon where the target should be,
    then blends the prediction with the (quantized) measurement:

        output = _SMOOTH_ALPHA * measurement + (1 - _SMOOTH_ALPHA) * prediction

    For a moving target the velocity prediction absorbs most of the motion,
    so quantization jumps (0.1 nm ≈ 185 m) are a small correction rather
    than the whole signal.  Lag ≈ 2-3 samples instead of N/2 for a rolling
    average — no need for a large window.

    If you need more smoothing, lower _SMOOTH_ALPHA.  Don't add another filter
    on top of this one — fix the root cause instead.
    """
    now = time.monotonic()

    # Convert TTM velocity to degrees/second.
    speed_ms = speed_kts * 1852.0 / 3600.0
    c = math.radians(course_deg)
    m_lat = 111_320.0
    m_lon = 111_320.0 * math.cos(math.radians(lat))
    vel_lat = (speed_ms * math.cos(c)) / m_lat
    vel_lon = (speed_ms * math.sin(c)) / m_lon

    track = state.target_tracks.get(target_num)
    if track is None:
        # First fix for this target — no history to predict from.
        state.target_tracks[target_num] = _TargetTrack(lat, lon, vel_lat, vel_lon, now)
        return lat, lon

    # Predict where the target should be based on last known velocity.
    dt = now - track.last_t
    pred_lat = track.lat + track.vel_lat * dt
    pred_lon = track.lon + track.vel_lon * dt

    # Blend prediction with measurement.
    out_lat = _SMOOTH_ALPHA * lat + (1.0 - _SMOOTH_ALPHA) * pred_lat
    out_lon = _SMOOTH_ALPHA * lon + (1.0 - _SMOOTH_ALPHA) * pred_lon

    state.target_tracks[target_num] = _TargetTrack(out_lat, out_lon, vel_lat, vel_lon, now)
    return out_lat, out_lon


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
    lat, lon = smooth_position(state, ttm.number, lat, lon, ttm.speed, ttm.course)
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
    """Return the package config path (~/kahu-vessel/config.toml)."""
    return Path(__file__).parent.parent / "config.toml"


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )

    parser = argparse.ArgumentParser(description="KAHU vessel daemon")
    parser.add_argument("--api-key", metavar="UUID", help="API key (overrides config and KAHU_API_KEY env var)")
    parser.add_argument("--config", metavar="PATH", help="Path to config.toml (default: ~/kahu-vessel/config.toml)")
    args = parser.parse_args()

    config_path = Path(args.config) if args.config else _resolve_config_path()
    config = load_config(config_path)
    log.info("using config: %s", config_path)
    relay_host = config["daemon"]["radar_host"]
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
