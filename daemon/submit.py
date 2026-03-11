"""Seam 3 — upload derived target positions to TrackServer via Avro/TCP.

Each ARPA target gets its own route on the server, identified by a UUID that
persists for the lifetime of this process.  Points are batched per target
number and flushed when the batch is full.

Call init() once at startup before any submit() calls.  If init() is never
called the module falls back to logging only (useful during development and
in tests that don't exercise the upload path).
"""

from __future__ import annotations

import logging
import math
import queue
import socket
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

import avro.io
import avro.schema

from daemon.nmea import TTM

log = logging.getLogger(__name__)

_SCHEMA_PATH = Path(__file__).parent.parent / "proto_avro.json"

# ---------------------------------------------------------------------------
# Module-level uploader — set by init()
# ---------------------------------------------------------------------------

_uploader: AvroUploader | None = None


def init(host: str, port: int, api_key: str, points_per_track: int = 10) -> None:
    """Create and start the background Avro uploader.  Call once at startup."""
    global _uploader
    _uploader = AvroUploader(host, port, api_key, points_per_track)
    _uploader.start()


def submit(ttm: TTM, lat: float, lon: float, timestamp: datetime) -> None:
    log.info(
        "target %02d | lat=%+.6f lon=%+.6f | cog=%.1f° sog=%.1f kn | status=%s | t=%s",
        ttm.number, lat, lon, ttm.course, ttm.speed, ttm.status, timestamp.isoformat(),
    )
    if _uploader is None:
        return
    _uploader.add_point(ttm.number, lat, lon, timestamp)


# ---------------------------------------------------------------------------
# Per-target track buffer
# ---------------------------------------------------------------------------

_MAX_SPEED_MS = 30 * 1852 / 3600  # 30 knots in m/s — reject jumps faster than this


@dataclass
class _TrackBuffer:
    track_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    start: datetime | None = None
    points: list = field(default_factory=list)
    last_lat: float | None = None
    last_lon: float | None = None
    last_time: datetime | None = None


# ---------------------------------------------------------------------------
# Avro uploader
# ---------------------------------------------------------------------------

_Batch = tuple[str, int, list]   # (track_id, start_ms, points)


class AvroUploader:
    """Background-threaded Avro/TCP uploader.

    add_point() is designed to be called from the asyncio event loop —
    it is non-blocking.  It accumulates points in per-target buffers and
    enqueues completed batches.  A single background thread drains the
    queue and performs all blocking Avro I/O.
    """

    def __init__(self, host: str, port: int, api_key: str, points_per_track: int) -> None:
        self._host = host
        self._port = port
        self._api_key = api_key
        self._points_per_track = points_per_track
        self._schema = avro.schema.parse(_SCHEMA_PATH.read_text())
        self._buffers: dict[int, _TrackBuffer] = {}
        self._queue: queue.Queue[_Batch] = queue.Queue()
        # Avro connection state — owned exclusively by the background thread.
        self._sock: socket.socket | None = None
        self._encoder: avro.io.BinaryEncoder | None = None
        self._decoder: avro.io.BinaryDecoder | None = None
        self._writer = avro.io.DatumWriter(self._schema)
        self._reader = avro.io.DatumReader(self._schema)
        self._call_id = 0

    # ------------------------------------------------------------------
    # Called from the asyncio event loop (non-blocking)
    # ------------------------------------------------------------------

    def add_point(
        self, target_num: int, lat: float, lon: float, timestamp: datetime
    ) -> None:
        buf = self._buffers.setdefault(target_num, _TrackBuffer())

        # Reject implausible jumps (ARPA scan-cycle noise from simulator/radar).
        if buf.last_lat is not None:
            dt = (timestamp - buf.last_time).total_seconds()
            if dt > 0:
                dlat = (lat - buf.last_lat) * 111_000
                dlon = (lon - buf.last_lon) * 111_000 * abs(math.cos(math.radians(lat)))
                dist_m = math.hypot(dlat, dlon)
                if dist_m / dt > _MAX_SPEED_MS:
                    log.debug(
                        "target %02d — rejected outlier: %.0fm in %.1fs (%.0f kn)",
                        target_num, dist_m, dt, (dist_m / dt) / (1852 / 3600),
                    )
                    return

        buf.last_lat = lat
        buf.last_lon = lon
        buf.last_time = timestamp

        if buf.start is None:
            buf.start = timestamp
        offset_s = (timestamp - buf.start).total_seconds()
        buf.points.append(
            {"lat": float(lat), "lon": float(lon), "timestamp": float(offset_s)}
        )
        if len(buf.points) >= self._points_per_track:
            self._flush_buffer(target_num, buf)

    def _flush_buffer(self, target_num: int, buf: _TrackBuffer) -> None:
        track_id = buf.track_id
        start_ms = int(buf.start.timestamp() * 1000)
        points = buf.points[:]
        # Reuse the same track_id so the server appends and time-sorts all
        # batches into one route rather than creating a new route per batch.
        # Carry last position forward so the outlier filter works across batches.
        new_buf = _TrackBuffer()
        new_buf.track_id = track_id
        new_buf.last_lat = buf.last_lat
        new_buf.last_lon = buf.last_lon
        new_buf.last_time = buf.last_time
        self._buffers[target_num] = new_buf
        self._queue.put((track_id, start_ms, points))

    # ------------------------------------------------------------------
    # Background thread
    # ------------------------------------------------------------------

    def start(self) -> None:
        t = threading.Thread(target=self._run, daemon=True, name="kahu-avro-upload")
        t.start()

    def _run(self) -> None:
        self._connect_with_retry()
        while True:
            batch = self._queue.get()
            self._submit_with_retry(*batch)

    def _connect_with_retry(self) -> None:
        backoff = 1
        while True:
            try:
                self._connect()
                return
            except Exception as exc:
                log.warning("connect failed: %s — retrying in %ds", exc, backoff)
                time.sleep(backoff)
                backoff = min(backoff * 2, 30)

    def _connect(self) -> None:
        raw = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        raw.settimeout(10)
        raw.connect((self._host, self._port))
        self._sock = raw
        self._encoder = avro.io.BinaryEncoder(raw.makefile("wb", buffering=0))
        self._decoder = avro.io.BinaryDecoder(raw.makefile("rb", buffering=0))
        self._call_id = 0

        self._call_id += 1
        log.info("sending Login to %s:%d...", self._host, self._port)
        self._writer.write(
            {"Message": {"Call": {"id": self._call_id, "Call": {
                "Login": {"apikey": self._api_key}
            }}}},
            self._encoder,
        )
        resp = self._reader.read(self._decoder)
        raw.settimeout(None)
        log.info("connected to %s:%d — login: %s", self._host, self._port, resp)

    def _submit_with_retry(self, track_id: str, start_ms: int, points: list) -> None:
        while True:
            try:
                self._call_id += 1
                self._writer.write(
                    {"Message": {"Call": {"id": self._call_id, "Call": {
                        "Submit": {
                            "uuid":  track_id,
                            "route": points,
                            "nmea":  None,
                            "start": start_ms,
                        }
                    }}}},
                    self._encoder,
                )
                resp = self._reader.read(self._decoder)
                log.info(
                    "submitted %d-pt track %s: %s", len(points), track_id[:8], resp
                )
                return
            except Exception as exc:
                log.warning("submit failed: %s — reconnecting", exc)
                self._connect_with_retry()
