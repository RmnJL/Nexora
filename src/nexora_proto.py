"""
Nexora protocol primitives (phase 1).
Signature: Rmn JL
"""

from __future__ import annotations

import base64
import secrets
import struct
from dataclasses import dataclass

MAGIC = b"NXR1"
TYPE_HELLO = 1
TYPE_HELLO_ACK = 2
TYPE_DATA = 3
TYPE_DATA_ACK = 4
TYPE_STREAM_OPEN = 10
TYPE_STREAM_OPEN_ACK = 11
TYPE_STREAM_SEND = 12
TYPE_STREAM_RECV = 13
TYPE_STREAM_CLOSE = 14

# Protocol v1 header (legacy):
# magic(4) | msg_type(1) | session_id(4) | nonce(4) | payload_len(2)
_HDR_V1 = struct.Struct(">4sBIIH")
# Protocol v2 header:
# magic(4) | control(1: flags+type) | session_id(4) | nonce(4) | payload_len(2) | hdr_crc8(1)
_HDR_V2 = struct.Struct(">4sBIIHB")

# control byte layout:
# high nibble: flags
# low  nibble: msg_type (0..15)
FLAG_RETRY_COUNT = 0x10


@dataclass
class Packet:
    msg_type: int
    session_id: int
    nonce: int
    payload: bytes
    retry_count: int = 0
    flags: int = 0


def _crc8(data: bytes) -> int:
    """CRC-8 (poly 0x07, init 0x00, no xorout)."""
    crc = 0
    for b in data:
        crc ^= b
        for _ in range(8):
            if crc & 0x80:
                crc = ((crc << 1) ^ 0x07) & 0xFF
            else:
                crc = (crc << 1) & 0xFF
    return crc


def _msg_supports_retry_count(msg_type: int) -> bool:
    # TYPE_DATA carries explicit retry metadata by design.
    # TYPE_STREAM_SEND also benefits from retry observability/dedup context.
    return msg_type in (TYPE_DATA, TYPE_STREAM_SEND)


def pack_packet(
    msg_type: int,
    session_id: int,
    nonce: int,
    payload: bytes,
    retry_count: int = 0,
) -> bytes:
    if msg_type < 0 or msg_type > 0x0F:
        raise ValueError("msg_type out of range for v2 control byte")

    flags = 0
    body = payload
    if _msg_supports_retry_count(msg_type):
        flags |= FLAG_RETRY_COUNT
        body = bytes([max(0, min(255, int(retry_count)))]) + payload

    control = (flags & 0xF0) | (msg_type & 0x0F)
    payload_len = len(body)
    hdr_no_crc = _HDR_V1.pack(MAGIC, control, session_id, nonce, payload_len)
    hdr_crc = _crc8(hdr_no_crc)
    return _HDR_V2.pack(MAGIC, control, session_id, nonce, payload_len, hdr_crc) + body


def unpack_packet(raw: bytes) -> Packet:
    if len(raw) < _HDR_V1.size:
        raise ValueError("packet too short")
    # Try v2 first.
    if len(raw) >= _HDR_V2.size:
        magic, control, session_id, nonce, payload_len, hdr_crc = _HDR_V2.unpack(
            raw[: _HDR_V2.size]
        )
        if magic == MAGIC and len(raw) == _HDR_V2.size + payload_len:
            hdr_no_crc = _HDR_V1.pack(MAGIC, control, session_id, nonce, payload_len)
            expect_crc = _crc8(hdr_no_crc)
            if hdr_crc != expect_crc:
                raise ValueError("bad header crc8")

            flags = control & 0xF0
            msg_type = control & 0x0F
            body = raw[_HDR_V2.size :]
            retry_count = 0
            payload = body
            if flags & FLAG_RETRY_COUNT:
                if not _msg_supports_retry_count(msg_type):
                    raise ValueError("retry flag set for unsupported message type")
                if not body:
                    raise ValueError("retry metadata missing")
                retry_count = body[0]
                payload = body[1:]

            return Packet(
                msg_type=msg_type,
                session_id=session_id,
                nonce=nonce,
                payload=payload,
                retry_count=retry_count,
                flags=flags,
            )

    # Fallback: decode legacy v1 packet.
    magic, msg_type, session_id, nonce, payload_len = _HDR_V1.unpack(raw[: _HDR_V1.size])
    if magic != MAGIC:
        raise ValueError("bad magic")
    if len(raw) != _HDR_V1.size + payload_len:
        raise ValueError("bad payload length")
    return Packet(
        msg_type=msg_type,
        session_id=session_id,
        nonce=nonce,
        payload=raw[_HDR_V1.size :],
        retry_count=0,
        flags=0,
    )


def encode_dns_data(data: bytes) -> str:
    # RFC-compliant chars for labels (base32 lowercase, no padding)
    return base64.b32encode(data).decode("ascii").lower().rstrip("=")


def decode_dns_data(s: str) -> bytes:
    compact = s.replace(".", "").strip().upper()
    pad = "=" * ((8 - (len(compact) % 8)) % 8)
    return base64.b32decode(compact + pad, casefold=True)


def random_nonce() -> int:
    return secrets.randbits(32)
