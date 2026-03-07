"""Seam 3 — upload derived target positions to TrackServer.

Currently a stub: logs positions to stdout.
Avro/TCP upload to crowdsource.kahu.earth:9900 is a future milestone.
"""

from __future__ import annotations

import logging
from datetime import datetime

from daemon.nmea import TTM

log = logging.getLogger(__name__)


def submit(ttm: TTM, lat: float, lon: float, timestamp: datetime) -> None:
    log.info(
        "target %02d | lat=%+.6f lon=%+.6f | cog=%.1f° sog=%.1f kn | "
        "status=%s | t=%s",
        ttm.number, lat, lon, ttm.course, ttm.speed,
        ttm.status, timestamp.isoformat(),
    )
