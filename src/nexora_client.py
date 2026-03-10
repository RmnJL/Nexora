"""
Nexora phase-1 client:
- builds HELLO packet
- sends DNS TXT query
- validates HELLO_ACK
Signature: Rmn JL
"""

from __future__ import annotations

import argparse
import socket

from dns_wire import build_txt_query, parse_txt_answer
from nexora_proto import (
    TYPE_DATA,
    TYPE_DATA_ACK,
    TYPE_HELLO,
    TYPE_HELLO_ACK,
    decode_dns_data,
    encode_dns_data,
    pack_packet,
    random_nonce,
    unpack_packet,
)


def chunk_label(s: str, size: int = 50) -> str:
    return ".".join(s[i : i + size] for i in range(0, len(s), size))


def _query_txt(server: str, port: int, zone: str, timeout: float, payload: bytes) -> tuple[int, object]:
    encoded = encode_dns_data(payload)
    fqdn = f"{chunk_label(encoded)}.{zone.strip('.')}"
    qid, query = build_txt_query(fqdn)

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(timeout)
    sock.sendto(query, (server, port))
    resp, _ = sock.recvfrom(4096)
    txt = parse_txt_answer(resp, qid)
    return qid, unpack_packet(decode_dns_data(txt))


def run_client(server: str, port: int, zone: str, timeout: float) -> int:
    nonce = random_nonce()
    hello = pack_packet(TYPE_HELLO, 0, nonce, b"NEXORA_HELLO")
    _, pkt = _query_txt(server, port, zone, timeout, hello)

    if pkt.msg_type != TYPE_HELLO_ACK:
        raise RuntimeError("unexpected packet type")
    if pkt.nonce != nonce:
        raise RuntimeError("nonce mismatch")
    sid = pkt.session_id
    print(f"[nexora-client] handshake ok, session_id={sid}, payload={pkt.payload!r}")

    # Phase-2 data exchange test
    dnonce = random_nonce()
    data_pkt = pack_packet(TYPE_DATA, sid, dnonce, b"phase2_data")
    _, dpkt = _query_txt(server, port, zone, timeout, data_pkt)
    if dpkt.msg_type != TYPE_DATA_ACK:
        raise RuntimeError("unexpected data-ack type")
    if dpkt.session_id != sid or dpkt.nonce != dnonce:
        raise RuntimeError("data-ack mismatch")
    print(f"[nexora-client] data ack ok, payload={dpkt.payload!r}")
    return sid


def main() -> None:
    p = argparse.ArgumentParser(description="Nexora phase-1 client")
    p.add_argument("--server", required=True, help="DNS server IP")
    p.add_argument("--port", type=int, default=53)
    p.add_argument("--zone", required=True, help="example: t1.phonexpress.ir")
    p.add_argument("--timeout", type=float, default=2.0)
    args = p.parse_args()
    run_client(args.server, args.port, args.zone, args.timeout)


if __name__ == "__main__":
    main()
