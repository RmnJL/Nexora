"""
Nexora phase-1 client:
- builds HELLO packet
- sends DNS TXT query
- validates HELLO_ACK
Signature: Rmn JL
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import socket
import threading
import time
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, wait as _futures_wait, FIRST_COMPLETED
from threading import Lock

from dns_wire import TYPE_A, TYPE_TXT, build_query, parse_answer_data
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
    random_nonce,
    unpack_packet,
)

log = logging.getLogger("nexora-client")

# Maximum outstanding out-of-order sequence entries before dropping oldest.
SEQ_MAP_MAX_SIZE = 512


def chunk_label(s: str, size: int = 44) -> str:
    return ".".join(s[i : i + size] for i in range(0, len(s), size))


class ResolverSelector:
    def __init__(self, servers: list[str], fail_cooldown: float = 5.0) -> None:
        uniq = []
        for s in servers:
            s = s.strip()
            if s and s not in uniq:
                uniq.append(s)
        if not uniq:
            raise ValueError("no resolver servers provided")
        self._servers = uniq
        self._fail_cooldown = max(1.0, fail_cooldown)
        self._lock = Lock()
        self._active = uniq[0]
        self._probe_idx = 0
        self._last_switch = time.time()
        self._health_thread_started = False
        self._fails: dict[str, int] = {s: 0 for s in uniq}
        self._bad_until: dict[str, float] = {s: 0.0 for s in uniq}
        self._order: dict[str, int] = {s: i for i, s in enumerate(uniq)}
        self._rr_idx = 0

    def _preferred_server_locked(self, now: float) -> str:
        good = [s for s in self._servers if now >= self._bad_until.get(s, 0.0)]
        pool = good or self._servers
        return min(
            pool,
            key=lambda s: (
                self._fails.get(s, 0),
                self._bad_until.get(s, 0.0),
                self._order.get(s, 0),
            ),
        )

    def choose(self) -> str:
        now = time.time()
        with self._lock:
            preferred = self._preferred_server_locked(now)
            active_bad = now < self._bad_until.get(self._active, 0.0)
            active_fails = self._fails.get(self._active, 0)
            pref_fails = self._fails.get(preferred, 0)
            if active_bad or pref_fails + 1 < active_fails:
                self._active = preferred
            return self._active

    def choose_next(self) -> str:
        """Round-robin across healthy resolvers to distribute load."""
        now = time.time()
        with self._lock:
            pool = [s for s in self._servers if now >= self._bad_until.get(s, 0.0)]
            if not pool:
                pool = list(self._servers)
            server = pool[self._rr_idx % len(pool)]
            self._rr_idx += 1
            return server

    def report_success(self, server: str) -> None:
        with self._lock:
            self._fails[server] = 0
            self._bad_until[server] = 0.0

    def report_failure(self, server: str) -> None:
        now = time.time()
        with self._lock:
            f = self._fails.get(server, 0) + 1
            self._fails[server] = f
            cooldown = self._fail_cooldown * min(f, 6)
            self._bad_until[server] = now + cooldown
            if server == self._active:
                self._active = self._preferred_server_locked(now)

    def rotate_active(self) -> str:
        now = time.time()
        with self._lock:
            self._active = self._preferred_server_locked(now)
            return self._active

    def start_background_health_loop(
        self,
        probe_fn,
        interval_sec: float,
        switch_sec: float,
    ) -> None:
        if len(self._servers) <= 1:
            return
        with self._lock:
            if self._health_thread_started:
                return
            self._health_thread_started = True
            self._last_switch = time.time()
            self._probe_idx = 0

        def _loop() -> None:
            while True:
                with self._lock:
                    s = self._servers[self._probe_idx % len(self._servers)]
                    self._probe_idx += 1
                ok = False
                try:
                    ok = bool(probe_fn(s))
                except Exception:
                    ok = False
                if ok:
                    self.report_success(s)
                else:
                    self.report_failure(s)

                now = time.time()
                with self._lock:
                    do_switch = now - self._last_switch >= switch_sec
                    if do_switch:
                        self._last_switch = now
                if do_switch:
                    nxt = self.rotate_active()
                    log.info("resolver rotate -> %s", nxt)
                time.sleep(max(1.0, interval_sec))

        t = threading.Thread(target=_loop, daemon=True)
        t.start()

    def update_servers(self, new_servers: list[str]) -> None:
        """Hot-reload the resolver list without restarting."""
        uniq = []
        for s in new_servers:
            s = s.strip()
            if s and s not in uniq:
                uniq.append(s)
        if not uniq:
            return
        with self._lock:
            old = set(self._servers)
            self._servers = uniq
            for s in uniq:
                if s not in old:
                    self._fails[s] = 0
                    self._bad_until[s] = 0.0
                    self._order[s] = len(self._order)
            if self._active not in uniq:
                self._active = uniq[0]
            self._rr_idx = 0
        log.info("resolver list updated: %s", ",".join(uniq))

    @property
    def servers(self) -> list[str]:
        return list(self._servers)

    @property
    def active(self) -> str:
        with self._lock:
            return self._active


class _DnsQueryPacer:
    """Per-resolver rate limiter.

    Each resolver gets its own independent pacing so that N resolvers
    yield N times the throughput of a single resolver.
    """

    def __init__(self, min_interval: float = 0.15) -> None:
        self._lock = Lock()
        self._interval = min_interval
        self._next_send: dict[str, float] = {}  # per-resolver

    def pace(self, resolver: str = "") -> None:
        with self._lock:
            now = time.time()
            ns = self._next_send.get(resolver, 0.0)
            if now < ns:
                wait = ns - now
                self._next_send[resolver] = ns + self._interval
            else:
                wait = 0.0
                self._next_send[resolver] = now + self._interval
        if wait > 0:
            time.sleep(wait)


_dns_pacer: _DnsQueryPacer | None = None


def _query_pkt_direct(
    server: str,
    port: int,
    zone: str,
    timeout: float,
    payload: bytes,
    qtype: int,
) -> tuple[int, object]:
    encoded = encode_dns_data(payload)
    fqdn = f"{chunk_label(encoded)}.{zone.strip('.')}"
    qid, query = build_query(fqdn, qtype=qtype)
    if _dns_pacer is not None:
        _dns_pacer.pace(server)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(timeout)
    try:
        sock.sendto(query, (server, port))
        resp, _ = sock.recvfrom(4096)
        txt = parse_answer_data(resp, qid)
        if txt.endswith(".nexora"):
            txt = txt[: -len(".nexora")].strip(".")
        elif txt.endswith(".x"):
            txt = txt[: -len(".x")].strip(".")
        return qid, unpack_packet(decode_dns_data(txt))
    finally:
        sock.close()


def _query_txt(
    selector: ResolverSelector,
    port: int,
    zone: str,
    timeout: float,
    payload: bytes,
    attempts: int,
    qtype: int,
) -> tuple[int, object]:
    last_err = None
    last_server = ""
    for idx in range(max(1, attempts)):
        server = selector.choose_next()
        last_server = server
        try:
            qid, pkt = _query_pkt_direct(server, port, zone, timeout, payload, qtype)
            selector.report_success(server)
            return qid, pkt
        except Exception as e:
            last_err = e
            selector.report_failure(server)
            log.warning("query attempt %d/%d failed server=%s: %s", idx + 1, attempts, server, e)
            time.sleep(0.03)
    raise TimeoutError(
        f"dns query failed after {attempts} attempts (last resolver {last_server}): {last_err}"
    )


def run_client(
    selector: ResolverSelector,
    port: int,
    zone: str,
    timeout: float,
    attempts: int,
    qtype: int,
) -> int:
    nonce = random_nonce()
    hello = pack_packet(TYPE_HELLO, 0, nonce, b"NEXORA_HELLO")
    _, pkt = _query_txt(selector, port, zone, timeout, hello, attempts, qtype)

    if pkt.msg_type != TYPE_HELLO_ACK:
        raise RuntimeError("unexpected packet type")
    if pkt.nonce != nonce:
        raise RuntimeError("nonce mismatch")
    sid = pkt.session_id
    log.info("handshake ok, session_id=%d, payload=%r", sid, pkt.payload)

    # Phase-2 data exchange test
    time.sleep(0.25)
    dnonce = random_nonce()
    data_pkt = pack_packet(TYPE_DATA, sid, dnonce, b"phase2_data")
    _, dpkt = _query_txt(selector, port, zone, timeout, data_pkt, attempts, qtype)
    if dpkt.msg_type != TYPE_DATA_ACK:
        raise RuntimeError("unexpected data-ack type")
    if dpkt.session_id != sid or dpkt.nonce != dnonce:
        raise RuntimeError("data-ack mismatch")
    log.info("data ack ok, payload=%r", dpkt.payload)
    return sid


def _establish_session(
    selector: ResolverSelector,
    port: int,
    zone: str,
    timeout: float,
    attempts: int,
    qtype: int,
) -> int:
    nonce = random_nonce()
    hello = pack_packet(TYPE_HELLO, 0, nonce, b"NEXORA_HELLO")
    _, pkt = _query_txt(selector, port, zone, timeout, hello, attempts, qtype)
    if pkt.msg_type != TYPE_HELLO_ACK or pkt.nonce != nonce:
        raise RuntimeError("session handshake failed")
    return pkt.session_id


def _stream_open(
    sid: int,
    selector: ResolverSelector,
    port: int,
    zone: str,
    timeout: float,
    attempts: int,
    qtype: int,
    target_host: str,
    target_port: int,
    open_retries: int = 1,
    retry_delay: float = 0.2,
) -> None:
    last_err: Exception | None = None
    for idx in range(max(1, open_retries)):
        n = random_nonce()
        try:
            op = pack_packet(
                TYPE_STREAM_OPEN, sid, n, f"{target_host}:{target_port}".encode("ascii")
            )
            _, rp = _query_txt(selector, port, zone, timeout, op, attempts, qtype)
            if rp.msg_type == TYPE_STREAM_OPEN_ACK and rp.nonce == n and rp.payload == b"OK":
                return
            last_err = RuntimeError(f"stream open failed: {rp.payload!r}")
        except Exception as e:
            last_err = e
        if idx + 1 < open_retries:
            time.sleep(retry_delay * (idx + 1))
    raise RuntimeError(str(last_err) if last_err else "stream open failed")


def _stream_close(
    sid: int,
    selector: ResolverSelector,
    port: int,
    zone: str,
    timeout: float,
    attempts: int,
    qtype: int,
) -> None:
    n = random_nonce()
    cp = pack_packet(TYPE_STREAM_CLOSE, sid, n, b"")
    try:
        _, rp = _query_txt(selector, port, zone, timeout, cp, attempts, qtype)
        if rp.msg_type != TYPE_STREAM_CLOSE or rp.nonce != n:
            return
    except Exception:
        return


def _extract_seq_chunk(payload: bytes) -> tuple[int | None, bytes]:
    """Extract a single (seq, chunk) from length-prefixed format: [1B len][2B seq][data]."""
    if len(payload) < 3:
        return None, b""
    entry_len = payload[0]
    if entry_len < 2 or len(payload) < 1 + entry_len:
        return None, b""
    seq = int.from_bytes(payload[1:3], "big")
    return seq, payload[3:1 + entry_len]


def _extract_seq_chunks(payload: bytes) -> list[tuple[int, bytes]]:
    """Extract multiple (seq, chunk) pairs from length-prefixed packed response.

    Format: [1B entry_len][2B seq][data] repeated.
    """
    results: list[tuple[int, bytes]] = []
    offset = 0
    while offset < len(payload):
        if offset + 1 > len(payload):
            break
        entry_len = payload[offset]
        offset += 1
        if entry_len < 2 or offset + entry_len > len(payload):
            break
        seq = int.from_bytes(payload[offset:offset + 2], "big")
        data = payload[offset + 2:offset + entry_len]
        offset += entry_len
        if data:
            results.append((seq, data))
    return results


def _handle_forward_conn(
    local_conn: socket.socket,
    client_addr: tuple[str, int],
    selector: ResolverSelector,
    port: int,
    zone: str,
    timeout: float,
    attempts: int,
    qtype: int,
    target_host: str,
    target_port: int,
    chunk_size: int,
    stream_open_retries: int,
    poll_min_interval: float,
    poll_max_interval: float,
    idle_timeout: float,
    pipeline_depth: int = 1,
) -> None:
    sid = None
    try:
        local_conn.settimeout(0.05)
        # Create a fresh session and retry stream-open a few times.
        for _ in range(max(1, stream_open_retries)):
            sid = _establish_session(selector, port, zone, timeout, attempts, qtype)
            try:
                _stream_open(
                    sid,
                    selector,
                    port,
                    zone,
                    timeout,
                    attempts,
                    qtype,
                    target_host,
                    target_port,
                    open_retries=2,
                )
                break
            except Exception:
                _stream_close(sid, selector, port, zone, timeout, attempts, qtype)
                sid = None
        if sid is None:
            raise RuntimeError("stream open failed")
        log.info(
            "forward open local=%s:%d sid=%d target=%s:%d",
            client_addr[0], client_addr[1], sid, target_host, target_port,
        )

        seq_map: "OrderedDict[int, bytes]" = OrderedDict()
        next_seq = 1
        local_closed = False
        idle_rounds = 0
        last_activity = time.time()
        poll_wait = max(0.02, poll_min_interval)
        poll_ceiling = max(poll_wait, poll_max_interval)
        eager_pulls = 0
        next_pull_at = time.time()

        pool = ThreadPoolExecutor(max_workers=max(1, pipeline_depth))
        pending = {}  # {future: (nonce, up_len)}
        try:
            while True:
                now = time.time()
                if idle_timeout > 0 and now - last_activity >= idle_timeout:
                    break

                # --- Fill pipeline with new queries ---
                while len(pending) < pipeline_depth:
                    outbound = b""
                    if not local_closed:
                        try:
                            outbound = local_conn.recv(chunk_size)
                            if outbound == b"":
                                local_closed = True
                            elif outbound:
                                eager_pulls = max(eager_pulls, 4)
                        except socket.timeout:
                            outbound = b""

                    now2 = time.time()
                    should_send = bool(outbound) or now2 >= next_pull_at
                    if not should_send:
                        break

                    nonce = random_nonce()
                    pkt = pack_packet(TYPE_STREAM_SEND, sid, nonce, outbound)
                    fut = pool.submit(
                        _query_txt, selector, port, zone, timeout,
                        pkt, attempts, qtype,
                    )
                    pending[fut] = (nonce, len(outbound))
                    next_pull_at = now2 + poll_wait
                    if not outbound:
                        break  # at most one empty poll per fill cycle

                # --- Wait for results ---
                if not pending:
                    time.sleep(min(0.05, max(0.0, next_pull_at - time.time())))
                    continue

                done_set, _ = _futures_wait(
                    pending.keys(),
                    return_when=FIRST_COMPLETED,
                    timeout=timeout * max(1, attempts) + 3,
                )
                if not done_set:
                    raise TimeoutError("all DNS queries timed out")

                for f in done_set:
                    exp_nonce, up_len = pending.pop(f)
                    try:
                        _, resp = f.result()
                    except TimeoutError:
                        for pf in pending:
                            pf.cancel()
                        pending.clear()
                        raise
                    log.info(
                        "stream xfer sid=%d up=%d resp_type=%d resp_pay=%d",
                        sid, up_len, resp.msg_type, len(resp.payload),
                    )
                    if resp.msg_type != TYPE_STREAM_RECV or resp.nonce != exp_nonce:
                        raise RuntimeError("stream recv mismatch")

                    seq, chunk = _extract_seq_chunk(resp.payload)
                    got_new = False
                    if seq is not None and chunk and seq not in seq_map:
                        seq_map[seq] = chunk
                        got_new = True

                    if len(resp.payload) > (1 + 2 + len(chunk if chunk else b"")):
                        for mseq, mchunk in _extract_seq_chunks(resp.payload):
                            if mseq not in seq_map:
                                seq_map[mseq] = mchunk
                                got_new = True

                    while len(seq_map) > SEQ_MAP_MAX_SIZE:
                        seq_map.popitem(last=False)

                    while next_seq in seq_map:
                        cdata = seq_map.pop(next_seq)
                        try:
                            local_conn.sendall(cdata)
                        except (BrokenPipeError, ConnectionResetError, OSError):
                            local_closed = True
                            break
                        next_seq += 1

                    if got_new or up_len > 0:
                        idle_rounds = 0
                        last_activity = time.time()
                        poll_wait = max(0.02, poll_min_interval)
                        next_pull_at = time.time() + poll_wait
                    else:
                        idle_rounds += 1
                        if eager_pulls > 0:
                            eager_pulls -= 1
                            poll_wait = max(0.02, poll_min_interval)
                        else:
                            poll_wait = min(poll_ceiling, max(0.02, poll_wait * 1.8))
                        next_pull_at = time.time() + poll_wait

                if local_closed and idle_rounds >= 3 and not pending:
                    break
        finally:
            for f in pending:
                f.cancel()
            pool.shutdown(wait=False)

    except TimeoutError as e:
        log.warning("forward timeout %s sid=%s: %s", client_addr, sid, e)
    except (BrokenPipeError, ConnectionResetError) as e:
        log.info("forward pipe closed %s sid=%s: %s", client_addr, sid, e)
    except OSError as e:
        log.warning("forward os-error %s sid=%s: %s", client_addr, sid, e)
    except Exception as e:
        log.warning("forward error %s sid=%s: %s", client_addr, sid, e)
    finally:
        if sid is not None:
            _stream_close(sid, selector, port, zone, timeout, attempts, qtype)
        try:
            local_conn.close()
        except Exception:
            pass


def run_forward_server(
    selector: ResolverSelector,
    port: int,
    zone: str,
    timeout: float,
    attempts: int,
    qtype: int,
    listen_host: str,
    listen_port: int,
    target_host: str,
    target_port: int,
    chunk_size: int,
    max_conns: int,
    max_conns_per_ip: int,
    stream_open_retries: int,
    poll_min_interval: float,
    poll_max_interval: float,
    idle_timeout: float,
    pipeline_depth: int = 1,
) -> None:
    lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    lsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    lsock.bind((listen_host, listen_port))
    lsock.listen(256)
    log.info(
        "forward server listening on %s:%d -> %s:%d via resolvers=%s:%d",
        listen_host, listen_port, target_host, target_port,
        ','.join(selector.servers), port,
    )
    sem = threading.BoundedSemaphore(max(1, max_conns))
    ip_counts: dict[str, int] = {}
    ip_lock = Lock()
    while True:
        conn, addr = lsock.accept()
        peer_ip = addr[0]

        if not sem.acquire(timeout=2.0):
            log.warning("forward reject local=%s:%d reason=max_conns", peer_ip, addr[1])
            try:
                conn.close()
            except Exception:
                pass
            continue

        # Track per-IP count for observability (no longer rejects).
        with ip_lock:
            ip_counts[peer_ip] = ip_counts.get(peer_ip, 0) + 1

        def _worker(
            local_conn: socket.socket = conn,
            local_addr: tuple[str, int] = addr,
            local_peer_ip: str = peer_ip,
        ) -> None:
            try:
                _handle_forward_conn(
                    local_conn,
                    local_addr,
                    selector,
                    port,
                    zone,
                    timeout,
                    attempts,
                    qtype,
                    target_host,
                    target_port,
                    chunk_size,
                    stream_open_retries,
                    poll_min_interval,
                    poll_max_interval,
                    idle_timeout,
                    pipeline_depth,
                )
            finally:
                with ip_lock:
                    cur = ip_counts.get(local_peer_ip, 0) - 1
                    if cur > 0:
                        ip_counts[local_peer_ip] = cur
                    else:
                        ip_counts.pop(local_peer_ip, None)
                sem.release()

        t = threading.Thread(
            target=_worker,
            daemon=True,
        )
        t.start()


def run_tcp_test(
    selector: ResolverSelector,
    port: int,
    zone: str,
    timeout: float,
    attempts: int,
    qtype: int,
    target_host: str,
    target_port: int,
    request_data: str,
    chunk_size: int,
) -> int:
    sid = run_client(selector, port, zone, timeout, attempts, qtype)

    n1 = random_nonce()
    open_pkt = pack_packet(
        TYPE_STREAM_OPEN, sid, n1, f"{target_host}:{target_port}".encode("ascii")
    )
    _, op = _query_txt(selector, port, zone, timeout, open_pkt, attempts, qtype)
    if op.msg_type != TYPE_STREAM_OPEN_ACK or op.nonce != n1 or op.payload != b"OK":
        raise RuntimeError(f"stream open failed: {op.payload!r}")
    log.info("stream open ok")

    req_bytes = request_data.encode("utf-8")
    if chunk_size < 1:
        chunk_size = len(req_bytes)

    recv_parts: "OrderedDict[int, bytes]" = OrderedDict()

    def _add_recv(payload: bytes) -> None:
        if len(payload) < 3:
            return
        for seq, body in _extract_seq_chunks(payload):
            if body and seq not in recv_parts:
                recv_parts[seq] = body

    for idx in range(0, len(req_bytes), chunk_size):
        chunk = req_bytes[idx : idx + chunk_size]
        n2 = random_nonce()
        send_pkt = pack_packet(TYPE_STREAM_SEND, sid, n2, chunk)
        _, rp = _query_txt(selector, port, zone, timeout, send_pkt, attempts, qtype)
        if rp.msg_type != TYPE_STREAM_RECV or rp.nonce != n2:
            raise RuntimeError("stream recv mismatch")
        _add_recv(rp.payload)

    # Pull extra downstream data with empty sends.
    empty_rounds = 0
    for _ in range(20):
        n4 = random_nonce()
        pull_pkt = pack_packet(TYPE_STREAM_SEND, sid, n4, b"")
        _, pr = _query_txt(selector, port, zone, timeout, pull_pkt, attempts, qtype)
        if pr.msg_type != TYPE_STREAM_RECV or pr.nonce != n4:
            raise RuntimeError("stream pull mismatch")
        before = len(recv_parts)
        _add_recv(pr.payload)
        if len(recv_parts) > before:
            empty_rounds = 0
        else:
            empty_rounds += 1
            if empty_rounds >= 2:
                break
        time.sleep(0.08)

    recv_data = b"".join(recv_parts[k] for k in sorted(recv_parts.keys()))
    decoded = recv_data.decode("utf-8", errors="replace")
    log.info("stream recv (%d bytes):\n%s", len(recv_data), decoded)

    n3 = random_nonce()
    close_pkt = pack_packet(TYPE_STREAM_CLOSE, sid, n3, b"")
    _, cp = _query_txt(selector, port, zone, timeout, close_pkt, attempts, qtype)
    if cp.msg_type != TYPE_STREAM_CLOSE or cp.nonce != n3:
        raise RuntimeError("stream close mismatch")
    log.info("stream close ok")
    return sid


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )
    p = argparse.ArgumentParser(description="Nexora phase-1 client")
    p.add_argument(
        "--server",
        required=True,
        help="DNS server IP or comma-separated list of resolver IPs",
    )
    p.add_argument("--port", type=int, default=53)
    p.add_argument("--zone", required=True, help="example: t1.phonexpress.ir")
    p.add_argument("--timeout", type=float, default=2.0)
    p.add_argument("--attempts", type=int, default=3)
    p.add_argument("--qtype", choices=["TXT", "A"], default="TXT")
    p.add_argument("--tcp-test-host", default="")
    p.add_argument("--tcp-test-port", type=int, default=80)
    p.add_argument(
        "--tcp-test-request",
        default="GET / HTTP/1.0\r\nHost: example.com\r\nConnection: close\r\n\r\n",
    )
    p.add_argument("--tcp-chunk-size", type=int, default=35)
    p.add_argument("--forward-listen-host", default="")
    p.add_argument("--forward-listen-port", type=int, default=0)
    p.add_argument("--forward-target-host", default="")
    p.add_argument("--forward-target-port", type=int, default=0)
    p.add_argument("--forward-max-conns", type=int, default=4)
    p.add_argument("--forward-max-conns-per-ip", type=int, default=64)
    p.add_argument("--stream-open-retries", type=int, default=2)
    p.add_argument(
        "--dns-query-interval",
        type=float,
        default=0.05,
        help="minimum seconds between DNS queries (global rate limit)",
    )
    p.add_argument(
        "--forward-poll-min-interval",
        type=float,
        default=0.2,
        help="minimum delay between empty downstream polls (seconds)",
    )
    p.add_argument(
        "--forward-poll-max-interval",
        type=float,
        default=3.0,
        help="maximum delay between empty downstream polls under idle pressure (seconds)",
    )
    p.add_argument(
        "--forward-idle-timeout",
        type=float,
        default=25.0,
        help="close forward stream after this many idle seconds; 0 disables",
    )
    p.add_argument(
        "--pipeline-depth",
        type=int,
        default=0,
        help="concurrent DNS queries per stream; 0=auto (match resolver count)",
    )
    p.add_argument(
        "--resolver-file", default="",
        help="path to resolver JSON file (auto-updated by scanner)",
    )
    p.add_argument("--resolver-fail-cooldown", type=float, default=5)
    p.add_argument("--resolver-health-interval", type=float, default=90.0)
    p.add_argument("--resolver-switch-interval", type=float, default=180.0)
    p.add_argument("--resolver-probe-timeout", type=float, default=1.6)
    p.add_argument("--resolver-probe-qtype", choices=["TXT", "A"], default="TXT")
    args = p.parse_args()
    qtype = TYPE_A if args.qtype == "A" else TYPE_TXT
    resolver_list = [x.strip() for x in args.server.split(",") if x.strip()]

    # Try loading from resolver file (scanner output) first
    if args.resolver_file and os.path.isfile(args.resolver_file):
        try:
            with open(args.resolver_file, "r") as f:
                data = json.load(f)
            file_resolvers = data.get("resolver_list", [])
            if file_resolvers:
                log.info("loaded %d resolvers from %s", len(file_resolvers), args.resolver_file)
                resolver_list = file_resolvers
        except Exception as e:
            log.warning("failed to read resolver file %s: %s", args.resolver_file, e)

    selector = ResolverSelector(resolver_list, fail_cooldown=args.resolver_fail_cooldown)

    # Background watcher: reload resolvers when scanner updates the file
    if args.resolver_file:
        def _watch_resolver_file(path: str, sel: ResolverSelector) -> None:
            last_mtime = 0.0
            while True:
                time.sleep(30)
                try:
                    mtime = os.path.getmtime(path)
                    if mtime <= last_mtime:
                        continue
                    last_mtime = mtime
                    with open(path, "r") as f:
                        data = json.load(f)
                    new_list = data.get("resolver_list", [])
                    if new_list and new_list != sel.servers:
                        sel.update_servers(new_list)
                except Exception:
                    pass

        wt = threading.Thread(
            target=_watch_resolver_file,
            args=(args.resolver_file, selector),
            daemon=True,
        )
        wt.start()
        log.info("resolver file watcher started: %s", args.resolver_file)
    global _dns_pacer
    _dns_pacer = _DnsQueryPacer(min_interval=args.dns_query_interval)
    # Auto pipeline depth: match number of resolvers for maximum parallelism
    if args.pipeline_depth <= 0:
        args.pipeline_depth = max(2, len(resolver_list))
        log.info("auto pipeline_depth=%d (from %d resolvers)", args.pipeline_depth, len(resolver_list))
    # Auto-scale attempts: try at least as many resolvers as available
    if len(resolver_list) > args.attempts:
        args.attempts = len(resolver_list)
        log.info("auto attempts=%d (from %d resolvers)", args.attempts, len(resolver_list))
    probe_qtype = TYPE_A if args.resolver_probe_qtype == "A" else TYPE_TXT

    if len(resolver_list) > 1:
        def _probe(resolver_ip: str) -> bool:
            n = random_nonce()
            hello = pack_packet(TYPE_HELLO, 0, n, b"HP")
            _, pkt = _query_pkt_direct(
                resolver_ip,
                args.port,
                args.zone,
                args.resolver_probe_timeout,
                hello,
                probe_qtype,
            )
            return pkt.msg_type == TYPE_HELLO_ACK and pkt.nonce == n

        selector.start_background_health_loop(
            _probe,
            interval_sec=args.resolver_health_interval,
            switch_sec=args.resolver_switch_interval,
        )
        log.info(
            "resolver loop active=%s list=%s",
            selector.active, ','.join(selector.servers),
        )
    if args.forward_listen_port > 0 and args.forward_target_host and args.forward_target_port > 0:
        run_forward_server(
            selector,
            args.port,
            args.zone,
            args.timeout,
            args.attempts,
            qtype,
            args.forward_listen_host or "0.0.0.0",
            args.forward_listen_port,
            args.forward_target_host,
            args.forward_target_port,
            args.tcp_chunk_size,
            args.forward_max_conns,
            args.forward_max_conns_per_ip,
            args.stream_open_retries,
            args.forward_poll_min_interval,
            args.forward_poll_max_interval,
            args.forward_idle_timeout,
            args.pipeline_depth,
        )
    elif args.tcp_test_host:
        run_tcp_test(
            selector,
            args.port,
            args.zone,
            args.timeout,
            args.attempts,
            qtype,
            args.tcp_test_host,
            args.tcp_test_port,
            args.tcp_test_request,
            args.tcp_chunk_size,
        )
    else:
        run_client(selector, args.port, args.zone, args.timeout, args.attempts, qtype)


if __name__ == "__main__":
    main()
