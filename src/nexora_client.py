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
import time
from collections import OrderedDict

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


def _query_txt(
    server: str,
    port: int,
    zone: str,
    timeout: float,
    payload: bytes,
    attempts: int,
    qtype: int,
) -> tuple[int, object]:
    encoded = encode_dns_data(payload)
    fqdn = f"{chunk_label(encoded)}.{zone.strip('.')}"
    qid, query = build_query(fqdn, qtype=qtype)

    last_err = None
    for idx in range(max(1, attempts)):
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
        except Exception as e:
            last_err = e
            # Small jitter-like backoff to avoid resolver burst drop.
            time.sleep(0.15 * (idx + 1))
        finally:
            sock.close()
    raise TimeoutError(f"dns query failed after {attempts} attempts: {last_err}")


def run_client(
    server: str, port: int, zone: str, timeout: float, attempts: int, qtype: int
) -> int:
    nonce = random_nonce()
    hello = pack_packet(TYPE_HELLO, 0, nonce, b"NEXORA_HELLO")
    _, pkt = _query_txt(server, port, zone, timeout, hello, attempts, qtype)

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
    _, dpkt = _query_txt(server, port, zone, timeout, data_pkt, attempts, qtype)
    if dpkt.msg_type != TYPE_DATA_ACK:
        raise RuntimeError("unexpected data-ack type")
    if dpkt.session_id != sid or dpkt.nonce != dnonce:
        raise RuntimeError("data-ack mismatch")
    print(f"[nexora-client] data ack ok, payload={dpkt.payload!r}")
    return sid


def run_tcp_test(
    server: str,
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
    sid = run_client(server, port, zone, timeout, attempts, qtype)

    n1 = random_nonce()
    open_pkt = pack_packet(
        TYPE_STREAM_OPEN, sid, n1, f"{target_host}:{target_port}".encode("ascii")
    )
    _, op = _query_txt(server, port, zone, timeout, open_pkt, attempts, qtype)
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
        _, rp = _query_txt(server, port, zone, timeout, send_pkt, attempts, qtype)
        if rp.msg_type != TYPE_STREAM_RECV or rp.nonce != n2:
            raise RuntimeError("stream recv mismatch")
        _add_recv(rp.payload)

    # Pull extra downstream data with empty sends.
    empty_rounds = 0
    for _ in range(20):
        n4 = random_nonce()
        pull_pkt = pack_packet(TYPE_STREAM_SEND, sid, n4, b"")
        _, pr = _query_txt(server, port, zone, timeout, pull_pkt, attempts, qtype)
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
    _, cp = _query_txt(server, port, zone, timeout, close_pkt, attempts, qtype)
    if cp.msg_type != TYPE_STREAM_CLOSE or cp.nonce != n3:
        raise RuntimeError("stream close mismatch")
    print("[nexora-client] stream close ok")
    return sid


def main() -> None:
    p = argparse.ArgumentParser(description="Nexora phase-1 client")
    p.add_argument("--server", required=True, help="DNS server IP")
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
    args = p.parse_args()
    qtype = TYPE_A if args.qtype == "A" else TYPE_TXT
    if args.tcp_test_host:
        run_tcp_test(
            args.server,
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
        run_client(args.server, args.port, args.zone, args.timeout, args.attempts, qtype)


if __name__ == "__main__":
    main()
