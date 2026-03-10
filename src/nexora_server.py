"""
Nexora phase-1 server:
- UDP/53 DNS loop
- HELLO/HELLO_ACK session init
Signature: Rmn JL
"""

from __future__ import annotations

import argparse
import socket
import threading
import time

from dns_wire import TYPE_TXT, build_servfail, build_txt_answer, parse_query
from nexora_proto import (
    TYPE_DATA,
    TYPE_DATA_ACK,
    TYPE_HELLO,
    TYPE_HELLO_ACK,
    decode_dns_data,
    encode_dns_data,
    pack_packet,
    unpack_packet,
)


class SessionStore:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._next = 1
        self._items: dict[int, dict] = {}

    def new(self, addr: tuple[str, int]) -> int:
        with self._lock:
            sid = self._next
            self._next += 1
            self._items[sid] = {"addr": addr, "ts": time.time()}
            return sid

    def exists(self, sid: int) -> bool:
        with self._lock:
            return sid in self._items


def _extract_encoded_chunk(qname: str, zone: str) -> str:
    base = qname.rstrip(".").lower()
    zone_l = zone.rstrip(".").lower()
    if not base.endswith(zone_l):
        return ""
    head = base[: -len(zone_l)].strip(".")
    return head.replace(".", "")


def run_server(bind: str, port: int, zone: str) -> None:
    sessions = SessionStore()
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((bind, port))
    print(f"[nexora-server] listening on {bind}:{port} zone={zone}")

    while True:
        data, addr = sock.recvfrom(4096)
        try:
            qid, qname, qtype = parse_query(data)
            if qtype != TYPE_TXT:
                sock.sendto(build_servfail(data), addr)
                continue
            encoded = _extract_encoded_chunk(qname, zone)
            if not encoded:
                sock.sendto(build_servfail(data), addr)
                continue

            packet = unpack_packet(decode_dns_data(encoded))
            if packet.msg_type == TYPE_HELLO:
                sid = sessions.new(addr)
                ack = pack_packet(TYPE_HELLO_ACK, sid, packet.nonce, b"OK")
                txt = encode_dns_data(ack)
                answer = build_txt_answer(data, txt, ttl=0)
                sock.sendto(answer, addr)
                print(
                    f"[nexora-server] hello from {addr[0]}:{addr[1]} -> session={sid}"
                )
                continue

            if packet.msg_type == TYPE_DATA and sessions.exists(packet.session_id):
                # Phase-2 behavior: echo payload back in DATA_ACK.
                ack_data = b"ACK:" + packet.payload
                ack = pack_packet(
                    TYPE_DATA_ACK, packet.session_id, packet.nonce, ack_data
                )
                txt = encode_dns_data(ack)
                answer = build_txt_answer(data, txt, ttl=0)
                sock.sendto(answer, addr)
                continue

            sock.sendto(build_servfail(data), addr)
        except Exception:
            try:
                sock.sendto(build_servfail(data), addr)
            except Exception:
                pass


def main() -> None:
    p = argparse.ArgumentParser(description="Nexora phase-1 server")
    p.add_argument("--bind", default="0.0.0.0")
    p.add_argument("--port", type=int, default=53)
    p.add_argument("--zone", required=True, help="example: t1.phonexpress.ir")
    args = p.parse_args()
    run_server(args.bind, args.port, args.zone)


if __name__ == "__main__":
    main()
