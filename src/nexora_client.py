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
import threading
import time
from collections import OrderedDict
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


def chunk_label(s: str, size: int = 50) -> str:
    return ".".join(s[i : i + size] for i in range(0, len(s), size))


class ResolverSelector:
    def __init__(self, servers: list[str], fail_cooldown: float = 20.0) -> None:
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
        self._idx = 0
        self._active = uniq[0]
        self._probe_idx = 0
        self._last_switch = time.time()
        self._health_thread_started = False
        self._fails: dict[str, int] = {s: 0 for s in uniq}
        self._bad_until: dict[str, float] = {s: 0.0 for s in uniq}

    def choose(self) -> str:
        now = time.time()
        with self._lock:
            if now >= self._bad_until.get(self._active, 0.0):
                return self._active
            n = len(self._servers)
            start = self._idx % n
            self._idx = (self._idx + 1) % n
            for i in range(n):
                s = self._servers[(start + i) % n]
                if now >= self._bad_until[s]:
                    self._active = s
                    return s
            return self._active

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
                for s in self._servers:
                    if now >= self._bad_until.get(s, 0.0):
                        self._active = s
                        break

    def rotate_active(self) -> str:
        now = time.time()
        with self._lock:
            good = [s for s in self._servers if now >= self._bad_until.get(s, 0.0)]
            pool = good or self._servers
            if self._active in pool:
                idx = (pool.index(self._active) + 1) % len(pool)
            else:
                idx = 0
            self._active = pool[idx]
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
                    print(f"[nexora-client] resolver rotate -> {nxt}")
                time.sleep(max(1.0, interval_sec))

        t = threading.Thread(target=_loop, daemon=True)
        t.start()

    @property
    def servers(self) -> list[str]:
        return list(self._servers)

    @property
    def active(self) -> str:
        with self._lock:
            return self._active


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
        server = selector.choose()
        last_server = server
        try:
            qid, pkt = _query_pkt_direct(server, port, zone, timeout, payload, qtype)
            selector.report_success(server)
            return qid, pkt
        except Exception as e:
            last_err = e
            selector.report_failure(server)
            # Small jitter-like backoff to avoid resolver burst drop.
            time.sleep(0.15 * (idx + 1))
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
    print(f"[nexora-client] handshake ok, session_id={sid}, payload={pkt.payload!r}")

    # Phase-2 data exchange test
    time.sleep(0.25)
    dnonce = random_nonce()
    data_pkt = pack_packet(TYPE_DATA, sid, dnonce, b"phase2_data")
    _, dpkt = _query_txt(selector, port, zone, timeout, data_pkt, attempts, qtype)
    if dpkt.msg_type != TYPE_DATA_ACK:
        raise RuntimeError("unexpected data-ack type")
    if dpkt.session_id != sid or dpkt.nonce != dnonce:
        raise RuntimeError("data-ack mismatch")
    print(f"[nexora-client] data ack ok, payload={dpkt.payload!r}")
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
    if len(payload) < 2:
        return None, b""
    seq = int.from_bytes(payload[:2], "big")
    return seq, payload[2:]


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
    idle_timeout: float,
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
        print(
            f"[nexora-client] forward open local={client_addr[0]}:{client_addr[1]} sid={sid} target={target_host}:{target_port}"
        )

        seq_map: "OrderedDict[int, bytes]" = OrderedDict()
        next_seq = 1
        local_closed = False
        idle_rounds = 0
        last_activity = time.time()
        next_pull_at = 0.0

        while True:
            now = time.time()
            if idle_timeout > 0 and now - last_activity >= idle_timeout:
                break

            outbound = b""
            if not local_closed:
                try:
                    outbound = local_conn.recv(chunk_size)
                    if outbound == b"":
                        local_closed = True
                except socket.timeout:
                    outbound = b""

            # Do not hammer resolvers with tight empty polling loops.
            now = time.time()
            should_poll = bool(outbound) or local_closed or now >= next_pull_at
            if not should_poll:
                time.sleep(min(0.05, max(0.0, next_pull_at - now)))
                continue

            nonce = random_nonce()
            pkt = pack_packet(TYPE_STREAM_SEND, sid, nonce, outbound)
            _, resp = _query_txt(selector, port, zone, timeout, pkt, attempts, qtype)
            if resp.msg_type != TYPE_STREAM_RECV or resp.nonce != nonce:
                raise RuntimeError("stream recv mismatch")

            seq, chunk = _extract_seq_chunk(resp.payload)
            got_new = False
            if seq is not None and chunk and seq not in seq_map:
                seq_map[seq] = chunk
                got_new = True

            # Flush in-order chunks to local socket
            while next_seq in seq_map:
                chunk = seq_map.pop(next_seq)
                try:
                    local_conn.sendall(chunk)
                except (BrokenPipeError, ConnectionResetError, OSError):
                    local_closed = True
                    break
                next_seq += 1

            if got_new or outbound:
                idle_rounds = 0
                last_activity = time.time()
            else:
                idle_rounds += 1
                next_pull_at = time.time() + max(0.02, poll_min_interval)

            if local_closed and idle_rounds >= 3:
                break

    except (BrokenPipeError, ConnectionResetError, OSError):
        pass
    except Exception as e:
        print(f"[nexora-client] forward error {client_addr}: {e}")
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
    stream_open_retries: int,
    poll_min_interval: float,
    idle_timeout: float,
) -> None:
    lsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    lsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    lsock.bind((listen_host, listen_port))
    lsock.listen(32)
    print(
        f"[nexora-client] forward server listening on {listen_host}:{listen_port} -> {target_host}:{target_port} via resolvers={','.join(selector.servers)}:{port}"
    )
    sem = threading.BoundedSemaphore(max(1, max_conns))
    while True:
        conn, addr = lsock.accept()

        if not sem.acquire(blocking=False):
            print(f"[nexora-client] forward reject local={addr[0]}:{addr[1]} reason=max_conns")
            try:
                conn.close()
            except Exception:
                pass
            continue

        def _worker() -> None:
            try:
                _handle_forward_conn(
                    conn,
                    addr,
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
                    idle_timeout,
                )
            finally:
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
    print("[nexora-client] stream open ok")

    req_bytes = request_data.encode("utf-8")
    if chunk_size < 1:
        chunk_size = len(req_bytes)

    recv_parts: "OrderedDict[int, bytes]" = OrderedDict()

    def _add_recv(payload: bytes) -> None:
        if len(payload) < 2:
            return
        seq = int.from_bytes(payload[:2], "big")
        body = payload[2:]
        if not body:
            return
        if seq not in recv_parts:
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
    print(f"[nexora-client] stream recv ({len(recv_data)} bytes):")
    print(decoded)

    n3 = random_nonce()
    close_pkt = pack_packet(TYPE_STREAM_CLOSE, sid, n3, b"")
    _, cp = _query_txt(selector, port, zone, timeout, close_pkt, attempts, qtype)
    if cp.msg_type != TYPE_STREAM_CLOSE or cp.nonce != n3:
        raise RuntimeError("stream close mismatch")
    print("[nexora-client] stream close ok")
    return sid


def main() -> None:
    p = argparse.ArgumentParser(description="Nexora phase-1 client")
    p.add_argument(
        "--server",
        required=True,
        help="DNS server IP or comma-separated list of resolver IPs",
    )
    p.add_argument("--port", type=int, default=53)
    p.add_argument("--zone", required=True, help="example: t1.phonexpress.ir")
    p.add_argument("--timeout", type=float, default=2.0)
    p.add_argument("--attempts", type=int, default=4)
    p.add_argument("--qtype", choices=["TXT", "A"], default="A")
    p.add_argument("--tcp-test-host", default="")
    p.add_argument("--tcp-test-port", type=int, default=80)
    p.add_argument(
        "--tcp-test-request",
        default="GET / HTTP/1.0\r\nHost: example.com\r\nConnection: close\r\n\r\n",
    )
    p.add_argument("--tcp-chunk-size", type=int, default=24)
    p.add_argument("--forward-listen-host", default="")
    p.add_argument("--forward-listen-port", type=int, default=0)
    p.add_argument("--forward-target-host", default="")
    p.add_argument("--forward-target-port", type=int, default=0)
    p.add_argument("--forward-max-conns", type=int, default=24)
    p.add_argument("--stream-open-retries", type=int, default=3)
    p.add_argument(
        "--forward-poll-min-interval",
        type=float,
        default=0.12,
        help="minimum delay between empty downstream polls (seconds)",
    )
    p.add_argument(
        "--forward-idle-timeout",
        type=float,
        default=25.0,
        help="close forward stream after this many idle seconds; 0 disables",
    )
    p.add_argument("--resolver-fail-cooldown", type=float, default=20.0)
    p.add_argument("--resolver-health-interval", type=float, default=90.0)
    p.add_argument("--resolver-switch-interval", type=float, default=180.0)
    p.add_argument("--resolver-probe-timeout", type=float, default=1.6)
    p.add_argument("--resolver-probe-qtype", choices=["TXT", "A"], default="A")
    args = p.parse_args()
    qtype = TYPE_A if args.qtype == "A" else TYPE_TXT
    resolver_list = [x.strip() for x in args.server.split(",") if x.strip()]
    selector = ResolverSelector(resolver_list, fail_cooldown=args.resolver_fail_cooldown)
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
        print(
            f"[nexora-client] resolver loop active={selector.active} list={','.join(selector.servers)}"
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
            args.stream_open_retries,
            args.forward_poll_min_interval,
            args.forward_idle_timeout,
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
