"""Unit tests for daemon/nmea.py.

Every test uses inline sentence strings — no file I/O, no network.
Checksums were computed with the same algorithm being tested (XOR of body),
which is the only correct NMEA checksum algorithm.
"""

from datetime import datetime, timezone

import pytest

from daemon.nmea import (
    HDT,
    RMC,
    TTM,
    compute_checksum,
    is_accepted,
    normalize,
    parse_hdt,
    parse_rmc,
    parse_ttm,
    preprocess,
    validate_checksum,
)

# ---------------------------------------------------------------------------
# Fixture sentences (valid, real NMEA standard format)
# ---------------------------------------------------------------------------

RATTM_01  = "$RATTM,01,1.500,045.0,T,5.2,135.0,T,0.50,1.20,N,TGT01,T*00"
TTTTM_02  = "$TTTTM,02,2.300,180.0,T,3.1,090.0,T,1.00,2.50,N,TGT02,T*1D"
RATTM_REL = "$RATTM,03,0.800,270.0,R,8.0,090.0,T,0.10,0.50,N,TGT03,T*0D"  # relative bearing

HDT_TRUE  = "$GPHDT,045.0,T*34"
HDT_MAG   = "$GPHDT,045.0,M*2D"

RMC_ACTIVE = "$GPRMC,123519,A,4807.038,N,01131.000,E,022.4,084.4,230394,003.1,W*6A"
RMC_VOID   = "$GPRMC,123519,V,4807.038,N,01131.000,E,022.4,084.4,230394,003.1,W*7D"

NOISE = "$GPGSV,2,1,08,01,40,083,46,02,17,308,41,12,07,344,39,14,22,228,45*75"


# ---------------------------------------------------------------------------
# compute_checksum
# ---------------------------------------------------------------------------

def test_compute_checksum_known_value():
    # XOR of "GPHDT,045.0,T" == 0x34 == 52
    assert compute_checksum("GPHDT,045.0,T") == 0x34


# ---------------------------------------------------------------------------
# validate_checksum
# ---------------------------------------------------------------------------

def test_validate_checksum_valid_rattm():
    assert validate_checksum(RATTM_01) is True

def test_validate_checksum_valid_hdt():
    assert validate_checksum(HDT_TRUE) is True

def test_validate_checksum_valid_rmc():
    assert validate_checksum(RMC_ACTIVE) is True

def test_validate_checksum_wrong_cs():
    bad = RATTM_01[:-2] + "FF"
    assert validate_checksum(bad) is False

def test_validate_checksum_no_star():
    assert validate_checksum("$GPHDT,045.0,T") is False

def test_validate_checksum_no_dollar():
    assert validate_checksum("GPHDT,045.0,T*34") is False


# ---------------------------------------------------------------------------
# normalize
# ---------------------------------------------------------------------------

def test_normalize_rattm_becomes_ttttm():
    assert normalize(RATTM_01).startswith("$TTTTM,")

def test_normalize_rattm_checksum_is_valid():
    assert validate_checksum(normalize(RATTM_01)) is True

def test_normalize_rattm_preserves_fields():
    result = normalize(RATTM_01)
    # All fields after the sentence type except the last (status*cs) must match
    assert RATTM_01.split(",")[1:-1] == result.split(",")[1:-1]

def test_normalize_non_rattm_unchanged():
    assert normalize(HDT_TRUE) == HDT_TRUE
    assert normalize(RMC_ACTIVE) == RMC_ACTIVE
    assert normalize(TTTTM_02) == TTTTM_02


# ---------------------------------------------------------------------------
# is_accepted
# ---------------------------------------------------------------------------

def test_is_accepted_ttttm():
    assert is_accepted("$TTTTM,01,...") is True

def test_is_accepted_gphdt():
    assert is_accepted(HDT_TRUE) is True

def test_is_accepted_gprmc():
    assert is_accepted(RMC_ACTIVE) is True

def test_is_accepted_noise_rejected():
    assert is_accepted(NOISE) is False

def test_is_accepted_rattm_rejected():
    # Raw $RATTM must be normalised first — it is not in the accepted set
    assert is_accepted(RATTM_01) is False


# ---------------------------------------------------------------------------
# preprocess — gate for random / garbage data
# ---------------------------------------------------------------------------

def test_preprocess_rattm_normalised_and_accepted():
    result = preprocess(RATTM_01)
    assert result is not None
    assert result.startswith("$TTTTM,")

def test_preprocess_bad_checksum_dropped():
    bad = RATTM_01[:-2] + "FF"
    assert preprocess(bad) is None

def test_preprocess_noise_dropped():
    assert preprocess(NOISE) is None

def test_preprocess_hdt_passes():
    assert preprocess(HDT_TRUE) == HDT_TRUE

def test_preprocess_rmc_passes():
    assert preprocess(RMC_ACTIVE) == RMC_ACTIVE

def test_preprocess_empty_string_dropped():
    assert preprocess("") is None

def test_preprocess_garbage_dropped():
    assert preprocess("XYZABC!@#$%^&*()") is None

def test_preprocess_partial_sentence_dropped():
    # Valid-looking prefix but no checksum
    assert preprocess("$GPHDT,045.0") is None


# ---------------------------------------------------------------------------
# parse_ttm
# ---------------------------------------------------------------------------

def test_parse_ttm_valid():
    ttm = parse_ttm(normalize(RATTM_01))
    assert ttm is not None
    assert ttm.number == 1
    assert ttm.distance == pytest.approx(1.5)
    assert ttm.bearing == pytest.approx(45.0)
    assert ttm.speed == pytest.approx(5.2)
    assert ttm.course == pytest.approx(135.0)
    assert ttm.status == "T"

def test_parse_ttm_relative_bearing_returns_none():
    assert parse_ttm(normalize(RATTM_REL)) is None

def test_parse_ttm_direct_ttttm():
    ttm = parse_ttm(TTTTM_02)
    assert ttm is not None
    assert ttm.number == 2
    assert ttm.status == "T"


# ---------------------------------------------------------------------------
# parse_hdt
# ---------------------------------------------------------------------------

def test_parse_hdt_true():
    hdt = parse_hdt(HDT_TRUE)
    assert hdt is not None
    assert hdt.heading == pytest.approx(45.0)

def test_parse_hdt_magnetic_returns_none():
    assert parse_hdt(HDT_MAG) is None


# ---------------------------------------------------------------------------
# parse_rmc
# ---------------------------------------------------------------------------

def test_parse_rmc_active():
    rmc = parse_rmc(RMC_ACTIVE)
    assert rmc is not None
    # 4807.038 N = 48 + 7.038/60 = 48.1173°N
    assert rmc.lat == pytest.approx(48.1173, abs=1e-4)
    # 01131.000 E = 11 + 31.000/60 = 11.5167°E
    assert rmc.lon == pytest.approx(11.5167, abs=1e-4)
    assert rmc.timestamp == datetime(1994, 3, 23, 12, 35, 19, tzinfo=timezone.utc)

def test_parse_rmc_void_returns_none():
    assert parse_rmc(RMC_VOID) is None

def test_parse_rmc_west_longitude_is_negative():
    body = "GPRMC,123519,A,4807.038,N,07000.000,W,000.0,000.0,230394,,"
    line = f"${body}*{compute_checksum(body):02X}"
    rmc = parse_rmc(line)
    assert rmc is not None
    assert rmc.lon < 0

def test_parse_rmc_south_latitude_is_negative():
    body = "GPRMC,123519,A,3300.000,S,01131.000,E,000.0,000.0,230394,,"
    line = f"${body}*{compute_checksum(body):02X}"
    rmc = parse_rmc(line)
    assert rmc is not None
    assert rmc.lat < 0
