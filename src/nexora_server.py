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

from dns_wire import (
    TYPE_A,
    TYPE_TXT,
    build_cname_answer,
    build_servfail,
    build_txt_answer,
    parse_query,
)
from nexora_proto import (
    TYPE_DATA,
    TYPE_DATA_ACK,
    TYPE_HELLO,
    TYPE_HELLO_ACK,
    TYPE_STREAM_CLOSE,
    TYPE_STREAM_OPEN,
    TYPE_STREAM_OPEN_ACK,
    TYPE_STREAM_RECV,
    TYPE_STREAM_SEND,
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
            self._items[sid] = {"addr": addr, "ts": time.time(), "stream_sock": None}
            return sid

    def exists(self, sid: int) -> bool:
        with self._lock:
            return sid in self._items

    def get(self, sid: int) -> dict | None:
        with self._lock:
            return self._items.get(sid)


def _extract_encoded_chunk(qname: str, zone: str) -> str:
    base = qname.rstrip(".").lower()
    zone_l = zone.rstrip(".").lower()
    if not base.endswith(zone_l):
        return ""
    head = base[: -len(zone_l)].strip(".")
    return head.replace(".", "")


def _as_labels(s: str, size: int = 50) -> str:
    return ".".join(s[i : i + size] for i in range(0, len(s), size))


def _make_dns_answer(request: bytes, qtype: int, packet: bytes) -> bytes:
    txt = encode_dns_data(packet)
    if qtype == TYPE_TXT:
        return build_txt_answer(request, txt, ttl=0)
    return build_cname_answer(request, _as_labels(txt) + ".x.")


def run_server(bind: str, port: int, zone: str) -> None:
    sessions = SessionStore()
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((bind, port))
    print(f"[nexora-server] listening on {bind}:{port} zone={zone}")

    while True:
        data, addr = sock.recvfrom(4096)
        try:
            qid, qname, qtype = parse_query(data)
            if qtype not in (TYPE_TXT, TYPE_A):
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
                answer = _make_dns_answer(data, qtype, ack)
                sock.sendto(answer, addr)
                print(
                    f"[nexora-server] hello from {addr[0]}:{addr[1]} -> session={sid}"
                )
                continue

            if packet.msg_type == TYPE_DATA and sessions.exists(packet.session_id):
                # Keep ACK payload tiny to survive strict DNS relays.
                ack_data = b"K"
                ack = pack_packet(
                    TYPE_DATA_ACK, packet.session_id, packet.nonce, ack_data
                )
                sock.sendto(_make_dns_answer(data, qtype, ack), addr)
                continue

            if packet.msg_type == TYPE_STREAM_OPEN and sessions.exists(packet.session_id):
                sess = sessions.get(packet.session_id)
                if sess is None:
                    sock.sendto(build_servfail(data), addr)
                    continue

                # payload format: b"host:port"
                try:
                    target = packet.payload.decode("ascii", errors="ignore")
                    host, p = target.rsplit(":", 1)
                    tport = int(p)
                    stream_sock = socket.create_connection((host, tport), timeout=1.5)
                    # Keep recv non-blocking enough for DNS round-trip timing.
                    stream_sock.settimeout(0.2)
                    old = sess.get("stream_sock")
                    if old is not None:
                        try:
                            old.close()
                        except Exception:
                            pass
                    sess["stream_sock"] = stream_sock
                    ack = pack_packet(
                        TYPE_STREAM_OPEN_ACK, packet.session_id, packet.nonce, b"OK"
                    )
                    print(
                        f"[nexora-server] stream open session={packet.session_id} target={host}:{tport}"
                    )
                except Exception:
                    ack = pack_packet(
                        TYPE_STREAM_OPEN_ACK,
                        packet.session_id,
                        packet.nonce,
                        b"ER",
                    )
                    print(
                        f"[nexora-server] stream open failed session={packet.session_id}"
                    )
                sock.sendto(_make_dns_answer(data, qtype, ack), addr)
                continue

            if packet.msg_type == TYPE_STREAM_SEND and sessions.exists(packet.session_id):
                sess = sessions.get(packet.session_id)
                if sess is None or sess.get("stream_sock") is None:
                    sock.sendto(build_servfail(data), addr)
                    continue
                st = sess["stream_sock"]
                try:
                    if packet.payload:
                        st.sendall(packet.payload)
                    recv_chunks = []
                    for _ in range(4):
                        try:
                            part = st.recv(96)
                            if not part:
                                break
                            recv_chunks.append(part)
                            if len(part) < 96:
                                break
                        except socket.timeout:
                            break
                    recv_data = b"".join(recv_chunks)
                    print(
                        f"[nexora-server] stream send session={packet.session_id} recv={len(recv_data)}"
                    )
                    ack = pack_packet(
                        TYPE_STREAM_RECV, packet.session_id, packet.nonce, recv_data
                    )
                except Exception:
                    ack = pack_packet(
                        TYPE_STREAM_RECV, packet.session_id, packet.nonce, b""
                    )
                sock.sendto(_make_dns_answer(data, qtype, ack), addr)
                continue

            if packet.msg_type == TYPE_STREAM_CLOSE and sessions.exists(packet.session_id):
                sess = sessions.get(packet.session_id)
                if sess is not None and sess.get("stream_sock") is not None:
                    try:
                        sess["stream_sock"].close()
                    except Exception:
                        pass
                    sess["stream_sock"] = None
                ack = pack_packet(TYPE_STREAM_CLOSE, packet.session_id, packet.nonce, b"K")
                sock.sendto(_make_dns_answer(data, qtype, ack), addr)
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
