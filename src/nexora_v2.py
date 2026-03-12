"""
Nexora v2 scaffolding primitives.

This module provides the initial implementation layer for v2:
- envelope/frame header primitives
- carrier state tracking
- stream id allocation/multiplex bookkeeping

Data-plane integration is intentionally incremental and will be added in
follow-up commits while v1 stays operational.
"""

from __future__ import annotations

import struct
import time
from dataclasses import dataclass
from enum import Enum
from threading import Lock

MAGIC_V2 = b"NX"
V2_VERSION = 2

_ENV_HDR = struct.Struct(">2sBBIIBBH")
_FRAME_HDR = struct.Struct(">BBIIIHHH")


@dataclass(frozen=True)
class EnvelopeHeader:
    flags: int
    carrier_id: int
    epoch: int
    frame_count: int
    reserved: int
    envelope_len: int


@dataclass(frozen=True)
class FrameHeader:
    frame_type: int
    frame_flags: int
    stream_id: int
    seq: int
    ack_base: int
    ack_bitmap16: int
    window: int
    payload_len: int


def pack_envelope_header(h: EnvelopeHeader) -> bytes:
    return _ENV_HDR.pack(
        MAGIC_V2,
        V2_VERSION,
        h.flags & 0xFF,
        h.carrier_id & 0xFFFFFFFF,
        h.epoch & 0xFFFFFFFF,
        h.frame_count & 0xFF,
        h.reserved & 0xFF,
        h.envelope_len & 0xFFFF,
    )


def unpack_envelope_header(raw: bytes) -> EnvelopeHeader:
    if len(raw) < _ENV_HDR.size:
        raise ValueError("v2 envelope header too short")
    magic, version, flags, carrier_id, epoch, frame_count, reserved, env_len = _ENV_HDR.unpack(
        raw[: _ENV_HDR.size]
    )
    if magic != MAGIC_V2:
        raise ValueError("bad v2 envelope magic")
    if version != V2_VERSION:
        raise ValueError(f"unsupported v2 envelope version={version}")
    return EnvelopeHeader(
        flags=flags,
        carrier_id=carrier_id,
        epoch=epoch,
        frame_count=frame_count,
        reserved=reserved,
        envelope_len=env_len,
    )


def pack_frame_header(h: FrameHeader) -> bytes:
    return _FRAME_HDR.pack(
        h.frame_type & 0xFF,
        h.frame_flags & 0xFF,
        h.stream_id & 0xFFFFFFFF,
        h.seq & 0xFFFFFFFF,
        h.ack_base & 0xFFFFFFFF,
        h.ack_bitmap16 & 0xFFFF,
        h.window & 0xFFFF,
        h.payload_len & 0xFFFF,
    )


def unpack_frame_header(raw: bytes) -> FrameHeader:
    if len(raw) < _FRAME_HDR.size:
        raise ValueError("v2 frame header too short")
    frame_type, frame_flags, stream_id, seq, ack_base, ack_bitmap16, window, payload_len = _FRAME_HDR.unpack(
        raw[: _FRAME_HDR.size]
    )
    return FrameHeader(
        frame_type=frame_type,
        frame_flags=frame_flags,
        stream_id=stream_id,
        seq=seq,
        ack_base=ack_base,
        ack_bitmap16=ack_bitmap16,
        window=window,
        payload_len=payload_len,
    )


class CarrierState(str, Enum):
    INIT = "init"
    SELECTING_RESOLVERS = "selecting_resolvers"
    OPENING = "opening"
    ESTABLISHED = "established"
    DEGRADED = "degraded"
    RECOVERING = "recovering"
    DRAINING = "draining"
    CLOSED = "closed"


@dataclass
class CarrierRecord:
    carrier_id: int
    resolver: str
    state: CarrierState
    created_at: float
    last_transition_at: float


class CarrierManager:
    """Tracks carrier control-plane state for v2 rollout scaffolding."""

    def __init__(self, resolvers: list[str], max_carriers: int = 3) -> None:
        cleaned = [x.strip() for x in resolvers if x and x.strip()]
        if not cleaned:
            raise ValueError("no resolvers provided for v2 carrier manager")
        self._resolvers = cleaned
        self._max_carriers = max(1, min(int(max_carriers), len(cleaned)))
        self._lock = Lock()
        self._epoch = int(time.time())
        self._next_carrier_id = 1
        self._carriers: dict[int, CarrierRecord] = {}

    def _create_carrier_locked(self, resolver: str) -> CarrierRecord:
        cid = self._next_carrier_id
        self._next_carrier_id += 1
        now = time.time()
        rec = CarrierRecord(
            carrier_id=cid,
            resolver=resolver,
            state=CarrierState.INIT,
            created_at=now,
            last_transition_at=now,
        )
        self._carriers[cid] = rec
        return rec

    def _set_state_locked(self, carrier_id: int, state: CarrierState) -> None:
        rec = self._carriers.get(carrier_id)
        if rec is None:
            raise KeyError(f"carrier_id={carrier_id} not found")
        if rec.state == state:
            return
        rec.state = state
        rec.last_transition_at = time.time()

    def bootstrap(self) -> list[CarrierRecord]:
        """Create and mark initial carriers as established (scaffold mode)."""
        with self._lock:
            if self._carriers:
                return list(self._carriers.values())
            selected = self._resolvers[: self._max_carriers]
            out: list[CarrierRecord] = []
            for resolver in selected:
                rec = self._create_carrier_locked(resolver)
                self._set_state_locked(rec.carrier_id, CarrierState.SELECTING_RESOLVERS)
                self._set_state_locked(rec.carrier_id, CarrierState.OPENING)
                self._set_state_locked(rec.carrier_id, CarrierState.ESTABLISHED)
                out.append(rec)
            return out

    def mark_degraded(self, carrier_id: int) -> None:
        with self._lock:
            self._set_state_locked(carrier_id, CarrierState.DEGRADED)

    def mark_recovering(self, carrier_id: int) -> None:
        with self._lock:
            self._set_state_locked(carrier_id, CarrierState.RECOVERING)

    def mark_draining(self, carrier_id: int) -> None:
        with self._lock:
            self._set_state_locked(carrier_id, CarrierState.DRAINING)

    def mark_closed(self, carrier_id: int) -> None:
        with self._lock:
            self._set_state_locked(carrier_id, CarrierState.CLOSED)

    @property
    def epoch(self) -> int:
        return self._epoch

    def snapshot(self) -> list[CarrierRecord]:
        with self._lock:
            return list(self._carriers.values())


class StreamMux:
    """Allocates and tracks v2 stream ids."""

    def __init__(self) -> None:
        self._lock = Lock()
        self._next_stream_id = 1
        self._active: dict[int, float] = {}

    def open_stream(self) -> int:
        with self._lock:
            sid = self._next_stream_id
            self._next_stream_id += 1
            self._active[sid] = time.time()
            return sid

    def close_stream(self, stream_id: int) -> None:
        with self._lock:
            self._active.pop(stream_id, None)

    def active_count(self) -> int:
        with self._lock:
            return len(self._active)

    def active_stream_ids(self) -> list[int]:
        with self._lock:
            return sorted(self._active.keys())
