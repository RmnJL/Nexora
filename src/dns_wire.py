"""
Minimal DNS wire helpers for Nexora phase 1.
Signature: Rmn JL
"""

from __future__ import annotations

import secrets
import struct
from typing import Tuple

TYPE_TXT = 16
TYPE_A = 1
TYPE_CNAME = 5
CLASS_IN = 1


def _encode_name(name: str) -> bytes:
    labels = [x for x in name.strip(".").split(".") if x]
    out = bytearray()
    for label in labels:
        b = label.encode("ascii")
        if len(b) > 63:
            raise ValueError("label too long")
        out.append(len(b))
        out.extend(b)
    out.append(0)
    return bytes(out)


def _decode_name(packet: bytes, offset: int) -> Tuple[str, int]:
    labels = []
    jumped = False
    next_offset = offset
    steps = 0
    while True:
        if offset >= len(packet):
            raise ValueError("truncated name")
        ln = packet[offset]
        if ln == 0:
            offset += 1
            if not jumped:
                next_offset = offset
            break
        # compression pointer
        if (ln & 0xC0) == 0xC0:
            if offset + 1 >= len(packet):
                raise ValueError("truncated pointer")
            ptr = ((ln & 0x3F) << 8) | packet[offset + 1]
            if ptr >= len(packet):
                raise ValueError("bad pointer")
            if not jumped:
                next_offset = offset + 2
            offset = ptr
            jumped = True
            steps += 1
            if steps > 20:
                raise ValueError("pointer loop")
            continue
        offset += 1
        if offset + ln > len(packet):
            raise ValueError("truncated label")
        labels.append(packet[offset : offset + ln].decode("ascii", errors="ignore"))
        offset += ln
        if not jumped:
            next_offset = offset
    return ".".join(labels), next_offset


def build_query(name: str, qtype: int = TYPE_TXT) -> tuple[int, bytes]:
    qid = secrets.randbits(16)
    header = struct.pack(">HHHHHH", qid, 0x0100, 1, 0, 0, 0)
    q = _encode_name(name) + struct.pack(">HH", qtype, CLASS_IN)
    return qid, header + q


def parse_query(packet: bytes) -> tuple[int, str, int]:
    if len(packet) < 12:
        raise ValueError("short dns packet")
    qid, _flags, qd, _an, _ns, _ar = struct.unpack(">HHHHHH", packet[:12])
    if qd != 1:
        raise ValueError("only one question supported")
    name, off = _decode_name(packet, 12)
    if off + 4 > len(packet):
        raise ValueError("missing qtype/qclass")
    qtype, _qclass = struct.unpack(">HH", packet[off : off + 4])
    return qid, name, qtype


def parse_answer_data(packet: bytes, expected_qid: int) -> str:
    if len(packet) < 12:
        raise ValueError("short dns answer")
    qid, flags, qd, an, _ns, _ar = struct.unpack(">HHHHHH", packet[:12])
    if qid != expected_qid:
        raise ValueError("mismatched id")
    rcode = flags & 0x0F
    if rcode != 0:
        _rcode_names = {1: "FORMERR", 2: "SERVFAIL", 3: "NXDOMAIN", 5: "REFUSED"}
        raise ValueError(f"dns rcode={rcode} ({_rcode_names.get(rcode, 'UNKNOWN')})")
    if qd != 1 or an < 1:
        raise ValueError("no answer")

    _qname, off = _decode_name(packet, 12)
    off += 4
    # first answer only
    _aname, off = _decode_name(packet, off)
    if off + 10 > len(packet):
        raise ValueError("short rr header")
    atype, _aclass, _ttl, rdlen = struct.unpack(">HHIH", packet[off : off + 10])
    off += 10
    if off + rdlen > len(packet):
        raise ValueError("bad answer rdata")

    if atype == TYPE_TXT:
        rdata = packet[off : off + rdlen]
        if not rdata:
            return ""
        txt_len = rdata[0]
        if txt_len + 1 > len(rdata):
            raise ValueError("bad txt len")
        return rdata[1 : 1 + txt_len].decode("ascii", errors="ignore")

    if atype == TYPE_CNAME:
        cname, _ = _decode_name(packet, off)
        return cname

    raise ValueError(f"unsupported answer type: {atype}")


def build_servfail(request: bytes) -> bytes:
    qid = request[:2]
    flags = struct.pack(">H", 0x8002)
    counts = struct.pack(">HHHH", 1, 0, 0, 0)
    # copy question section
    _, off = _decode_name(request, 12)
    end = off + 4
    return qid + flags + counts + request[12:end]


def build_txt_answer(request: bytes, txt_data: str, ttl: int = 0) -> bytes:
    qid = request[:2]
    flags = struct.pack(">H", 0x8180)
    counts = struct.pack(">HHHH", 1, 1, 0, 0)
    qname, off = _decode_name(request, 12)
    qsec = request[12 : off + 4]
    name_ptr = struct.pack(">H", 0xC00C)
    rr_hdr = struct.pack(">HHIH", TYPE_TXT, CLASS_IN, ttl, len(txt_data) + 1)
    rdata = bytes([len(txt_data)]) + txt_data.encode("ascii")
    return qid + flags + counts + qsec + name_ptr + rr_hdr + rdata


def build_cname_answer(request: bytes, cname_target: str, ttl: int = 0) -> bytes:
    qid = request[:2]
    flags = struct.pack(">H", 0x8180)
    counts = struct.pack(">HHHH", 1, 1, 0, 0)
    _qname, off = _decode_name(request, 12)
    qsec = request[12 : off + 4]
    name_ptr = struct.pack(">H", 0xC00C)
    encoded_cname = _encode_name(cname_target)
    rr_hdr = struct.pack(">HHIH", TYPE_CNAME, CLASS_IN, ttl, len(encoded_cname))
    return qid + flags + counts + qsec + name_ptr + rr_hdr + encoded_cname
