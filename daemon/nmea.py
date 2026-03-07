"""NMEA 0183 processing — all NMEA intelligence lives here.

The relay forwards raw bytes. This module handles everything downstream:
  1. Checksum validation
  2. Dialect normalisation  ($RATTM → $TTTTM, recomputed checksum)
  3. Sentence type filtering (only $TTTTM / $GPHDT / $GPRMC pass)
  4. Field parsing into typed dataclasses

See ARCHITECTURE.md §4 for field specs and validation rules.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone

log = logging.getLogger(__name__)

_ACCEPTED = frozenset(["$TTTTM", "$GPHDT", "$GPRMC"])


# ---------------------------------------------------------------------------
# Parsed sentence dataclasses
# ---------------------------------------------------------------------------

@dataclass
class TTM:
    """Tracked Target Message — one ARPA contact."""
    number: int      # target number 00–99
    distance: float  # nautical miles from own ship
    bearing: float   # degrees True
    speed: float     # knots
    course: float    # degrees True
    status: str      # 'T' = tracking, 'Q' = acquiring, 'L' = lost


@dataclass
class HDT:
    """Heading True."""
    heading: float   # degrees True


@dataclass
class RMC:
    """Recommended Minimum — own-ship position fix."""
    lat: float           # decimal degrees (negative = South)
    lon: float           # decimal degrees (negative = West)
    timestamp: datetime  # UTC


# ---------------------------------------------------------------------------
# Checksum
# ---------------------------------------------------------------------------

def compute_checksum(body: str) -> int:
    """XOR of all bytes in *body* (characters between '$' and '*')."""
    result = 0
    for ch in body:
        result ^= ord(ch)
    return result


def validate_checksum(line: str) -> bool:
    """Return True if *line* has a well-formed and correct NMEA checksum."""
    if not line.startswith("$") or "*" not in line:
        return False
    try:
        star = line.rindex("*")
        body = line[1:star]
        cs_hex = line[star + 1: star + 3]
        if len(cs_hex) != 2:
            return False
        return compute_checksum(body) == int(cs_hex, 16)
    except (ValueError, IndexError):
        return False


# ---------------------------------------------------------------------------
# Normalisation
# ---------------------------------------------------------------------------

def normalize(line: str) -> str:
    """Rewrite $RATTM sentences as $TTTTM with a recomputed checksum.

    All other sentence types are returned unchanged.
    """
    if not line.startswith("$RATTM,"):
        return line
    star = line.rindex("*")
    body = "TTTTM" + line[6:star]   # swap "RATTM" → "TTTTM", drop old checksum
    return f"${body}*{compute_checksum(body):02X}"


# ---------------------------------------------------------------------------
# Filtering
# ---------------------------------------------------------------------------

def is_accepted(line: str) -> bool:
    """Return True if the sentence type is in the accepted set."""
    for prefix in _ACCEPTED:
        if line.startswith(prefix):
            rest = line[len(prefix):]
            if rest and rest[0] in (",", "*"):
                return True
    return False


# ---------------------------------------------------------------------------
# Full preprocessing pipeline
# ---------------------------------------------------------------------------

def preprocess(line: str) -> str | None:
    """Validate checksum → normalise → filter.

    Returns the normalised sentence string, or None if the line is dropped.
    """
    if not validate_checksum(line):
        return None
    line = normalize(line)
    if not is_accepted(line):
        return None
    return line


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------

def parse_ttm(line: str) -> TTM | None:
    """Parse a $TTTTM sentence into a TTM dataclass.

    Field layout (0-indexed after split on ','):
      [1] number  [2] distance  [3] bearing  [4] bearing_ref
      [5] speed   [6] course    [7] course_ref  ...  [12] status*cs
    """
    try:
        parts = line.split(",")
        # Bearing reference must be True; drop relative-bearing sentences.
        if parts[4].upper() == "R":
            log.warning("TTM bearing is relative — dropping: %s", line)
            return None
        status = parts[12].split("*")[0]
        return TTM(
            number=int(parts[1]),
            distance=float(parts[2]),
            bearing=float(parts[3]),
            speed=float(parts[5]),
            course=float(parts[6]),
            status=status,
        )
    except (IndexError, ValueError) as exc:
        log.warning("failed to parse TTM — %s: %s", exc, line)
        return None


def parse_hdt(line: str) -> HDT | None:
    """Parse a $GPHDT sentence. Drops magnetic-reference sentences."""
    try:
        parts = line.split(",")
        ref = parts[2].split("*")[0].strip()
        if ref.upper() != "T":
            log.warning("HDT reference is not True (%r) — dropping: %s", ref, line)
            return None
        return HDT(heading=float(parts[1]))
    except (IndexError, ValueError) as exc:
        log.warning("failed to parse HDT — %s: %s", exc, line)
        return None


def parse_rmc(line: str) -> RMC | None:
    """Parse a $GPRMC sentence. Drops void ('V') fixes."""
    try:
        parts = line.split(",")
        if parts[2].upper() != "A":
            return None   # void fix — silent drop, not a warning
        lat = _nmea_lat(parts[3], parts[4])
        lon = _nmea_lon(parts[5], parts[6])
        ts = _parse_rmc_time(parts[1], parts[9])
        return RMC(lat=lat, lon=lon, timestamp=ts)
    except (IndexError, ValueError) as exc:
        log.warning("failed to parse RMC — %s: %s", exc, line)
        return None


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _nmea_lat(lat_str: str, ns: str) -> float:
    """Convert NMEA ddmm.mmmm + N/S to decimal degrees."""
    deg = int(lat_str[:2])
    minutes = float(lat_str[2:])
    value = deg + minutes / 60.0
    return value if ns.upper() == "N" else -value


def _nmea_lon(lon_str: str, ew: str) -> float:
    """Convert NMEA dddmm.mmmm + E/W to decimal degrees."""
    deg = int(lon_str[:3])
    minutes = float(lon_str[3:])
    value = deg + minutes / 60.0
    return value if ew.upper() == "E" else -value


def _parse_rmc_time(time_str: str, date_str: str) -> datetime:
    """Parse RMC time (hhmmss.ss) + date (ddmmyy) into a UTC datetime."""
    h = int(time_str[0:2])
    m = int(time_str[2:4])
    s = int(time_str[4:6])
    day = int(date_str[0:2])
    month = int(date_str[2:4])
    yy = int(date_str[4:6])
    year = (1900 + yy) if yy >= 80 else (2000 + yy)
    return datetime(year, month, day, h, m, s, tzinfo=timezone.utc)
