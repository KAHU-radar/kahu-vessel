"""Target position fusion — Seam 2 math.

Derives an absolute target lat/lon from own-ship position (RMC) and a
tracked target bearing + range (TTM).

See ARCHITECTURE.md §5 for the full derivation and approximation notes.
"""

from __future__ import annotations

import math

from daemon.nmea import RMC, TTM


def compute_target_position(rmc: RMC, ttm: TTM) -> tuple[float, float]:
    """Return (target_lat, target_lon) in decimal degrees.

    Uses equirectangular approximation, accurate to well under 1% for
    ranges under 24 nm — sufficient for all practical ARPA ranges.

    TTM bearing is already True (validated upstream), so own-ship heading
    (HDT) is not needed in this calculation.
    """
    bearing_rad = math.radians(ttm.bearing)

    # 1 nautical mile = 1/60 degree latitude (exact by definition).
    delta_lat = (ttm.distance / 60.0) * math.cos(bearing_rad)

    # Longitude degrees shrink toward the poles — equirectangular correction.
    delta_lon = (ttm.distance / 60.0) * math.sin(bearing_rad) / math.cos(
        math.radians(rmc.lat)
    )

    return rmc.lat + delta_lat, rmc.lon + delta_lon
