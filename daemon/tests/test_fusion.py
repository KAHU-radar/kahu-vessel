"""Unit tests for daemon/fusion.py.

Each cardinal direction is tested independently so failures are easy to
diagnose. Tolerances are set to 1e-4 degrees (~10m), well within the
equirectangular approximation's accuracy at ARPA ranges.
"""

import math
import pytest

from daemon.nmea import RMC, TTM
from daemon.fusion import compute_target_position
from datetime import datetime, timezone

# Own-ship fix at the equator — simple baseline for direction tests.
_T = datetime(2024, 1, 1, tzinfo=timezone.utc)
OWN_EQUATOR = RMC(lat=0.0, lon=0.0, timestamp=_T)

# Own-ship fix at mid-latitude — tests the longitude cosine correction.
OWN_MID = RMC(lat=48.0, lon=11.0, timestamp=_T)


def _ttm(bearing: float, distance: float) -> TTM:
    return TTM(number=1, distance=distance, bearing=bearing,
               speed=0.0, course=0.0, status="T")


# ---------------------------------------------------------------------------
# Cardinal directions (equator — no cosine correction needed)
# ---------------------------------------------------------------------------

def test_due_north_increases_lat():
    lat, lon = compute_target_position(OWN_EQUATOR, _ttm(0.0, 1.0))
    assert lat > OWN_EQUATOR.lat
    assert lon == pytest.approx(OWN_EQUATOR.lon, abs=1e-9)

def test_due_south_decreases_lat():
    lat, lon = compute_target_position(OWN_EQUATOR, _ttm(180.0, 1.0))
    assert lat < OWN_EQUATOR.lat
    assert lon == pytest.approx(OWN_EQUATOR.lon, abs=1e-9)

def test_due_east_increases_lon():
    lat, lon = compute_target_position(OWN_EQUATOR, _ttm(90.0, 1.0))
    assert lon > OWN_EQUATOR.lon
    assert lat == pytest.approx(OWN_EQUATOR.lat, abs=1e-9)

def test_due_west_decreases_lon():
    lat, lon = compute_target_position(OWN_EQUATOR, _ttm(270.0, 1.0))
    assert lon < OWN_EQUATOR.lon
    assert lat == pytest.approx(OWN_EQUATOR.lat, abs=1e-9)


# ---------------------------------------------------------------------------
# Magnitude checks
# ---------------------------------------------------------------------------

def test_one_nm_north_equals_one_arcminute():
    # 1 NM due north = exactly 1/60 degree latitude by definition.
    lat, lon = compute_target_position(OWN_EQUATOR, _ttm(0.0, 1.0))
    assert lat == pytest.approx(1.0 / 60.0, abs=1e-9)

def test_zero_range_returns_own_position():
    lat, lon = compute_target_position(OWN_MID, _ttm(45.0, 0.0))
    assert lat == pytest.approx(OWN_MID.lat, abs=1e-9)
    assert lon == pytest.approx(OWN_MID.lon, abs=1e-9)


# ---------------------------------------------------------------------------
# Cosine (longitude) correction at mid-latitude
# ---------------------------------------------------------------------------

def test_east_lon_delta_shrinks_at_mid_latitude():
    # At lat=48°, 1 NM east should produce a larger lon delta than at lat=0°
    # (inverted: same NM covers fewer degrees at higher lat, so delta is smaller).
    _, lon_equator = compute_target_position(OWN_EQUATOR, _ttm(90.0, 1.0))
    _, lon_mid     = compute_target_position(OWN_MID,     _ttm(90.0, 1.0))
    delta_equator = lon_equator - OWN_EQUATOR.lon
    delta_mid     = lon_mid     - OWN_MID.lon
    # lon delta at equator should be smaller than at mid-lat... wait,
    # higher lat → 1/cos(lat) is larger → lon delta is LARGER, not smaller.
    # cos(48°) ≈ 0.669, so delta_mid = delta_equator / 0.669 > delta_equator.
    assert delta_mid > delta_equator

def test_cosine_correction_value():
    # At lat=60°, cos(60°)=0.5, so lon delta should be 2x the equatorial value.
    own_60 = RMC(lat=60.0, lon=0.0, timestamp=_T)
    _, lon_60  = compute_target_position(own_60,      _ttm(90.0, 1.0))
    _, lon_eq  = compute_target_position(OWN_EQUATOR, _ttm(90.0, 1.0))
    assert lon_60 == pytest.approx(lon_eq * 2.0, abs=1e-6)
