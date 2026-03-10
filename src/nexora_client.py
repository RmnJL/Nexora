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

from dns_wire import TYPE_A, TYPE_TXT, build_query, parse_answer_data
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


def main() -> None:
    p = argparse.ArgumentParser(description="Nexora phase-1 client")
    p.add_argument("--server", required=True, help="DNS server IP")
    p.add_argument("--port", type=int, default=53)
    p.add_argument("--zone", required=True, help="example: t1.phonexpress.ir")
    p.add_argument("--timeout", type=float, default=2.0)
    p.add_argument("--attempts", type=int, default=4)
    p.add_argument("--qtype", choices=["TXT", "A"], default="A")
    args = p.parse_args()
    qtype = TYPE_A if args.qtype == "A" else TYPE_TXT
    run_client(args.server, args.port, args.zone, args.timeout, args.attempts, qtype)


if __name__ == "__main__":
    main()
