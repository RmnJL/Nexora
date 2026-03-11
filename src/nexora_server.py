"""
Nexora phase-1 server:
- UDP/53 DNS loop
- HELLO/HELLO_ACK session init
Signature: Rmn JL
"""

from __future__ import annotations

import argparse
import logging
import secrets
import socket
import threading
import time
from collections import deque

from dns_wire import (
    TYPE_A,
    TYPE_NS,
    TYPE_SOA,
    TYPE_TXT,
    build_cname_answer,
    build_noerror_empty,
    build_ns_answer,
    build_soa_answer,
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

log = logging.getLogger("nexora-server")

# Single-threaded server: balance between catching backend responses
# and not stalling the DNS loop.  With query pacer on the client side
# limiting to ~7 qps, 0.1 s per recv is acceptable.
STREAM_SOCK_TIMEOUT = 0.3
STREAM_RECV_SLICE = 4096
STREAM_RECV_ROUNDS = 3
STREAM_RECV_MAX_BYTES = 8192
# Downstream chunk MUST stay small enough so the base32-encoded response
# fits in a DNS answer.  TXT: max 254 base32 chars -> 158 raw -> 143 payload.
# CNAME: max ~220 base32 chars -> 137 raw -> 122 payload.
DOWNSTREAM_CHUNK_SIZE = 100

# Rate limiting: max HELLO requests per source IP within window.
# NOTE: in DNS tunneling, source IP = resolver, not real client.
# Each resolver relays traffic for many clients, so limit must be high.
HELLO_RATE_WINDOW = 60.0   # seconds
HELLO_RATE_LIMIT = 120     # max new sessions per resolver-IP per window


class _HelloRateLimiter:
    """Per-IP rate limiter for HELLO requests with log throttling."""

    def __init__(self, window: float = HELLO_RATE_WINDOW, limit: int = HELLO_RATE_LIMIT) -> None:
        self._lock = threading.Lock()
        self._window = window
        self._limit = limit
        self._buckets: dict[str, list[float]] = {}
        self._suppressed: dict[str, int] = {}  # ip -> suppressed count

    def allow(self, ip: str) -> bool:
        now = time.time()
        with self._lock:
            times = self._buckets.get(ip)
            if times is None:
                self._buckets[ip] = [now]
                return True
            # Purge timestamps outside window.
            cutoff = now - self._window
            times[:] = [t for t in times if t > cutoff]
            if len(times) >= self._limit:
                self._suppressed[ip] = self._suppressed.get(ip, 0) + 1
                return False
            times.append(now)
            return True

    def pop_suppressed(self) -> dict[str, int]:
        """Return and reset suppressed counts for log throttling."""
        with self._lock:
            result = dict(self._suppressed)
            self._suppressed.clear()
        return result

    def cleanup(self) -> None:
        now = time.time()
        cutoff = now - self._window
        with self._lock:
            stale = [ip for ip, ts in self._buckets.items() if not ts or ts[-1] <= cutoff]
            for ip in stale:
                del self._buckets[ip]


class SessionStore:
    def __init__(self, session_ttl: float = 900.0) -> None:
        self._lock = threading.Lock()
        self._session_ttl = max(30.0, session_ttl)
        self._items: dict[int, dict] = {}

    def _generate_sid(self) -> int:
        """Generate a cryptographically random session ID."""
        while True:
            sid = secrets.randbits(32)
            if sid != 0 and sid not in self._items:
                return sid

    def new(self, addr: tuple[str, int]) -> int:
        with self._lock:
            sid = self._generate_sid()
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
        if len(txt) > 255:
            log.warning("TXT payload too large (%d chars), truncating", len(txt))
            txt = txt[:255]
        return build_txt_answer(request, txt, ttl=0)
    cname = _as_labels(txt) + ".x."
    return build_cname_answer(request, cname)


def _cache_get(sess: dict, msg_type: int, nonce: int) -> bytes | None:
    return sess.get("resp_cache", {}).get((msg_type, nonce))


def _cache_put(sess: dict, msg_type: int, nonce: int, packet: bytes) -> None:
    cache = sess.setdefault("resp_cache", {})
    cache[(msg_type, nonce)] = packet
    if len(cache) > 256:
        # Trim old items in insertion order for bounded memory.
        for k in list(cache.keys())[:64]:
            cache.pop(k, None)


def _enqueue_downstream(sess: dict, raw: bytes, chunk_size: int = DOWNSTREAM_CHUNK_SIZE) -> None:
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
    hello_limiter = _HelloRateLimiter()
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((bind, port))
    log.info(
        "listening on %s:%d zone=%s session_ttl=%ss", bind, port, zone, session_ttl
    )
    last_cleanup = time.time()

    while True:
        data, addr = sock.recvfrom(4096)
        now = time.time()
        if now - last_cleanup >= max(5.0, cleanup_interval):
            removed = sessions.cleanup_expired()
            hello_limiter.cleanup()
            suppressed = hello_limiter.pop_suppressed()
            if removed:
                log.info("cleanup expired sessions=%d", removed)
            for sip, scnt in suppressed.items():
                log.warning("hello rate limited ip=%s suppressed=%d in last interval", sip, scnt)
            last_cleanup = now
        try:
            qid, qname, qtype = parse_query(data)
            if qtype == TYPE_NS:
                sock.sendto(build_ns_answer(data, zone), addr)
                continue
            if qtype == TYPE_SOA:
                sock.sendto(build_soa_answer(data, zone), addr)
                continue
            if qtype not in (TYPE_TXT, TYPE_A):
                sock.sendto(build_noerror_empty(data), addr)
                continue
            encoded = _extract_encoded_chunk(qname, zone)
            if not encoded:
                sock.sendto(build_noerror_empty(data), addr)
                continue

            # Probe echo: scanner sends nxprobe-<nonce>, server echoes it
            if encoded.startswith("nxprobe"):
                sock.sendto(build_txt_answer(data, encoded, ttl=0), addr)
                continue

            packet = unpack_packet(decode_dns_data(encoded))
            if packet.msg_type == TYPE_HELLO:
                if not hello_limiter.allow(addr[0]):
                    sock.sendto(build_noerror_empty(data), addr)
                    continue
                sid = sessions.new(addr)
                ack = pack_packet(TYPE_HELLO_ACK, sid, packet.nonce, b"OK")
                answer = _make_dns_answer(data, qtype, ack)
                sock.sendto(answer, addr)
                log.info(
                    "hello from %s:%d -> session=%d", addr[0], addr[1], sid
                )
                continue

            if packet.msg_type == TYPE_DATA and sessions.exists(packet.session_id):
                sess = sessions.get(packet.session_id)
                if sess is None:
                    sock.sendto(build_noerror_empty(data), addr)
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
                    sock.sendto(build_noerror_empty(data), addr)
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
                    stream_sock.settimeout(STREAM_SOCK_TIMEOUT)
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
                    log.info(
                        "stream open session=%d target=%s:%d", packet.session_id, host, tport
                    )
                except Exception:
                    ack = pack_packet(
                        TYPE_STREAM_OPEN_ACK,
                        packet.session_id,
                        packet.nonce,
                        b"ER",
                    )
                    log.warning(
                        "stream open failed session=%d", packet.session_id
                    )
                _cache_put(sess, TYPE_STREAM_OPEN, packet.nonce, ack)
                sock.sendto(_make_dns_answer(data, qtype, ack), addr)
                continue

            if packet.msg_type == TYPE_STREAM_SEND and sessions.exists(packet.session_id):
                sess = sessions.get(packet.session_id)
                if sess is None or sess.get("stream_sock") is None:
                    log.warning("stream_send: session %d has no stream_sock", packet.session_id)
                    sock.sendto(build_noerror_empty(data), addr)
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
                    recv_total = 0
                    for _ in range(STREAM_RECV_ROUNDS):
                        if recv_total >= STREAM_RECV_MAX_BYTES:
                            break
                        try:
                            part = st.recv(min(STREAM_RECV_SLICE, STREAM_RECV_MAX_BYTES - recv_total))
                            if not part:
                                break
                            recv_chunks.append(part)
                            recv_total += len(part)
                            if len(part) < STREAM_RECV_SLICE:
                                break
                        except socket.timeout:
                            break
                    recv_data = b"".join(recv_chunks)
                    _enqueue_downstream(sess, recv_data, chunk_size=DOWNSTREAM_CHUNK_SIZE)
                    q = sess.get("down_q", deque())
                    # Pack as many queued chunks as fit in one DNS response.
                    # TXT: 254 base32 chars -> 158 raw -> 143 payload, use 140.
                    # CNAME: ~220 base32 chars -> 137 raw -> 122 payload, use 120.
                    max_payload = 140 if qtype == TYPE_TXT else 120
                    out_payload = b""
                    while q:
                        seq, out_chunk = q[0]
                        entry_body = seq.to_bytes(2, "big") + out_chunk
                        entry = bytes([len(entry_body)]) + entry_body
                        if len(out_payload) + len(entry) > max_payload:
                            break
                        q.popleft()
                        out_payload += entry
                    log.info(
                        "stream data sid=%d up=%d down=%d q=%d",
                        packet.session_id, len(packet.payload), len(recv_data), len(q),
                    )
                    ack = pack_packet(
                        TYPE_STREAM_RECV, packet.session_id, packet.nonce, out_payload
                    )
                except (BrokenPipeError, ConnectionResetError, OSError) as e:
                    log.warning("stream send error session=%d: %s", packet.session_id, e)
                    try:
                        st.close()
                    except Exception:
                        pass
                    sess["stream_sock"] = None
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

            sock.sendto(build_noerror_empty(data), addr)
        except Exception:
            log.warning("packet parse/handle error from %s", addr, exc_info=True)
            try:
                sock.sendto(build_noerror_empty(data), addr)
            except Exception:
                pass


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )
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
