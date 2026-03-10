"""
Nexora protocol primitives (phase 1).
Signature: Rmn JL
"""

from __future__ import annotations

import base64
import random
import struct
from dataclasses import dataclass

MAGIC = b"NXR1"
TYPE_HELLO = 1
TYPE_HELLO_ACK = 2
TYPE_DATA = 3
TYPE_DATA_ACK = 4

_HDR = struct.Struct(">4sBIIH")


@dataclass
class Packet:
    msg_type: int
    session_id: int
    nonce: int
    payload: bytes


def pack_packet(msg_type: int, session_id: int, nonce: int, payload: bytes) -> bytes:
    return _HDR.pack(MAGIC, msg_type, session_id, nonce, len(payload)) + payload


def unpack_packet(raw: bytes) -> Packet:
    if len(raw) < _HDR.size:
        raise ValueError("packet too short")
    magic, msg_type, session_id, nonce, payload_len = _HDR.unpack(raw[: _HDR.size])
    if magic != MAGIC:
        raise ValueError("bad magic")
    if len(raw) != _HDR.size + payload_len:
        raise ValueError("bad payload length")
    return Packet(
        msg_type=msg_type,
        session_id=session_id,
        nonce=nonce,
        payload=raw[_HDR.size :],
    )


def encode_dns_data(data: bytes) -> str:
    # RFC-compliant chars for labels (base32 lowercase, no padding)
    return base64.b32encode(data).decode("ascii").lower().rstrip("=")


def decode_dns_data(s: str) -> bytes:
    compact = s.replace(".", "").strip().upper()
    pad = "=" * ((8 - (len(compact) % 8)) % 8)
    return base64.b32decode(compact + pad, casefold=True)


def random_nonce() -> int:
    return random.getrandbits(32)
