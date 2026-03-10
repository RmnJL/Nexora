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
from collections import deque

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
    def __init__(self, session_ttl: float = 900.0) -> None:
        self._lock = threading.Lock()
        self._next = 1
        self._session_ttl = max(30.0, session_ttl)
        self._items: dict[int, dict] = {}

    def new(self, addr: tuple[str, int]) -> int:
        with self._lock:
            sid = self._next
            self._next += 1
            self._items[sid] = {
                "addr": addr,
                "ts": time.time(),
                "stream_sock": None,
                "down_q": deque(),
                "down_seq": 1,
                "resp_cache": {},
            }
            return sid

    def exists(self, sid: int) -> bool:
        stale_sock = None
        exists_ok = False
        with self._lock:
            item = self._items.get(sid)
            if item is None:
                exists_ok = False
            elif time.time() - float(item.get("ts", 0.0)) > self._session_ttl:
                stale_sock = item.get("stream_sock")
                self._items.pop(sid, None)
                exists_ok = False
            else:
                exists_ok = True
        if stale_sock is not None:
            try:
                stale_sock.close()
            except Exception:
                pass
        return exists_ok

    def get(self, sid: int) -> dict | None:
        stale_sock = None
        out: dict | None = None
        with self._lock:
            item = self._items.get(sid)
            if item is None:
                out = None
            elif time.time() - float(item.get("ts", 0.0)) > self._session_ttl:
                stale_sock = item.get("stream_sock")
                self._items.pop(sid, None)
                out = None
            else:
                item["ts"] = time.time()
                out = item
        if stale_sock is not None:
            try:
                stale_sock.close()
            except Exception:
                pass
        return out

    def touch(self, sid: int) -> None:
        with self._lock:
            item = self._items.get(sid)
            if item is not None:
                item["ts"] = time.time()

    def cleanup_expired(self) -> int:
        now = time.time()
        stale_socks = []
        with self._lock:
            stale_ids = [
                sid
                for sid, item in self._items.items()
                if now - float(item.get("ts", 0.0)) > self._session_ttl
            ]
            for sid in stale_ids:
                item = self._items.pop(sid, None)
                if item is not None and item.get("stream_sock") is not None:
                    stale_socks.append(item["stream_sock"])
        for st in stale_socks:
            try:
                st.close()
            except Exception:
                pass
        return len(stale_socks) if stale_socks else len(stale_ids)


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


def _cache_get(sess: dict, msg_type: int, nonce: int) -> bytes | None:
    return sess.get("resp_cache", {}).get((msg_type, nonce))


def _cache_put(sess: dict, msg_type: int, nonce: int, packet: bytes) -> None:
    cache = sess.setdefault("resp_cache", {})
    cache[(msg_type, nonce)] = packet
    if len(cache) > 256:
        # Trim old items in insertion order for bounded memory.
        for k in list(cache.keys())[:64]:
            cache.pop(k, None)


def _enqueue_downstream(sess: dict, raw: bytes, chunk_size: int = 48) -> None:
    if not raw:
        return
    q = sess.setdefault("down_q", deque())
    seq = int(sess.get("down_seq", 1))
    for i in range(0, len(raw), chunk_size):
        chunk = raw[i : i + chunk_size]
        q.append((seq, chunk))
        seq += 1
    sess["down_seq"] = seq


def run_server(
    bind: str,
    port: int,
    zone: str,
    session_ttl: float = 900.0,
    cleanup_interval: float = 60.0,
) -> None:
    sessions = SessionStore(session_ttl=session_ttl)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((bind, port))
    print(
        f"[nexora-server] listening on {bind}:{port} zone={zone} session_ttl={session_ttl}s"
    )
    last_cleanup = time.time()

    while True:
        data, addr = sock.recvfrom(4096)
        now = time.time()
        if now - last_cleanup >= max(5.0, cleanup_interval):
            removed = sessions.cleanup_expired()
            if removed:
                print(f"[nexora-server] cleanup expired sessions={removed}")
            last_cleanup = now
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
                sess = sessions.get(packet.session_id)
                if sess is None:
                    sock.sendto(build_servfail(data), addr)
                    continue
                sessions.touch(packet.session_id)
                cached = _cache_get(sess, TYPE_DATA, packet.nonce)
                if cached is not None:
                    sock.sendto(_make_dns_answer(data, qtype, cached), addr)
                    continue
                # Keep ACK payload tiny to survive strict DNS relays.
                ack_data = b"K"
                ack = pack_packet(
                    TYPE_DATA_ACK, packet.session_id, packet.nonce, ack_data
                )
                _cache_put(sess, TYPE_DATA, packet.nonce, ack)
                sock.sendto(_make_dns_answer(data, qtype, ack), addr)
                continue

            if packet.msg_type == TYPE_STREAM_OPEN and sessions.exists(packet.session_id):
                sess = sessions.get(packet.session_id)
                if sess is None:
                    sock.sendto(build_servfail(data), addr)
                    continue
                sessions.touch(packet.session_id)
                cached = _cache_get(sess, TYPE_STREAM_OPEN, packet.nonce)
                if cached is not None:
                    sock.sendto(_make_dns_answer(data, qtype, cached), addr)
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
                    sess["down_q"] = deque()
                    sess["down_seq"] = 1
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
                _cache_put(sess, TYPE_STREAM_OPEN, packet.nonce, ack)
                sock.sendto(_make_dns_answer(data, qtype, ack), addr)
                continue

            if packet.msg_type == TYPE_STREAM_SEND and sessions.exists(packet.session_id):
                sess = sessions.get(packet.session_id)
                if sess is None or sess.get("stream_sock") is None:
                    sock.sendto(build_servfail(data), addr)
                    continue
                sessions.touch(packet.session_id)
                cached = _cache_get(sess, TYPE_STREAM_SEND, packet.nonce)
                if cached is not None:
                    sock.sendto(_make_dns_answer(data, qtype, cached), addr)
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
                    _enqueue_downstream(sess, recv_data)
                    q = sess.get("down_q", deque())
                    if q:
                        seq, out_chunk = q.popleft()
                        out_payload = seq.to_bytes(2, "big") + out_chunk
                    else:
                        out_payload = b""
                    print(
                        f"[nexora-server] stream send session={packet.session_id} recv={len(recv_data)} out={len(out_payload)}"
                    )
                    ack = pack_packet(
                        TYPE_STREAM_RECV, packet.session_id, packet.nonce, out_payload
                    )
                except Exception:
                    ack = pack_packet(
                        TYPE_STREAM_RECV, packet.session_id, packet.nonce, b""
                    )
                _cache_put(sess, TYPE_STREAM_SEND, packet.nonce, ack)
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
                    sess["down_q"] = deque()
                ack = pack_packet(TYPE_STREAM_CLOSE, packet.session_id, packet.nonce, b"K")
                if sess is not None:
                    sessions.touch(packet.session_id)
                    _cache_put(sess, TYPE_STREAM_CLOSE, packet.nonce, ack)
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
    p.add_argument(
        "--session-ttl",
        type=float,
        default=900.0,
        help="expire inactive sessions after this many seconds",
    )
    p.add_argument(
        "--cleanup-interval",
        type=float,
        default=60.0,
        help="run session cleanup every N seconds",
    )
    args = p.parse_args()
    run_server(
        args.bind,
        args.port,
        args.zone,
        session_ttl=args.session_ttl,
        cleanup_interval=args.cleanup_interval,
    )


if __name__ == "__main__":
    main()
