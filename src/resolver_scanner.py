"""
Nexora Resolver Scanner — background daemon that discovers and ranks
DNS resolvers compatible with the Nexora tunnel.

Runs periodically, tests resolvers for tunnel compatibility, and writes
the best ones to a JSON file that the client reads automatically.

Signature: Rmn JL
"""

from __future__ import annotations

import argparse
import base64
import heapq
import ipaddress
import json
import logging
import os
import random
import secrets
import socket
import struct
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict, field
from threading import Event, Lock
from typing import Optional

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
    pack_packet,
    unpack_packet,
)

log = logging.getLogger("nexora-scanner")

# ---------------------------------------------------------------------------
# DNS wire helpers (self-contained, no imports from project modules)
# ---------------------------------------------------------------------------

_DNS_TYPE_A = 1
_DNS_TYPE_TXT = 16
_DNS_TYPE_NS = 2


def _encode_dns_name(name: str) -> bytes:
    out = bytearray()
    for label in name.strip(".").split("."):
        b = label.encode("ascii")
        out.append(len(b))
        out.extend(b)
    out.append(0)
    return bytes(out)


def _build_dns_query(domain: str, qtype: int = _DNS_TYPE_TXT) -> tuple[int, bytes]:
    qid = secrets.randbits(16)
    header = struct.pack(">HHHHHH", qid, 0x0100, 1, 0, 0, 0)
    q = _encode_dns_name(domain) + struct.pack(">HH", qtype, 1)
    return qid, header + q


def _parse_rcode(packet: bytes) -> int:
    if len(packet) < 12:
        return -1
    return packet[3] & 0x0F


def _dns_query(resolver: str, port: int, domain: str,
               qtype: int, timeout: float) -> tuple[int, bytes]:
    """Send a single DNS query and return (rcode, raw_response)."""
    qid, query = _build_dns_query(domain, qtype)
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(timeout)
    try:
        sock.sendto(query, (resolver, port))
        resp, _ = sock.recvfrom(4096)
        if len(resp) < 12:
            raise ValueError("short response")
        resp_qid = struct.unpack(">H", resp[:2])[0]
        if resp_qid != qid:
            raise ValueError("qid mismatch")
        return _parse_rcode(resp), resp
    finally:
        sock.close()


def _b32_encode_nopad(data: bytes) -> str:
    return base64.b32encode(data).decode("ascii").lower().rstrip("=")


def _b32_decode_loose(data: bytes) -> bytes:
    txt = data.decode("ascii", errors="ignore")
    compact = txt.replace(".", "").strip().upper()
    if not compact:
        raise ValueError("empty base32 text")
    pad = "=" * ((8 - (len(compact) % 8)) % 8)
    return base64.b32decode(compact + pad, casefold=True)


def _chunk_label(s: str, size: int = 44) -> str:
    return ".".join(s[i:i + size] for i in range(0, len(s), size))


# ---------------------------------------------------------------------------
# Probe tests
# ---------------------------------------------------------------------------

def _rand_label(n: int = 12) -> str:
    chars = "abcdefghijklmnopqrstuvwxyz0123456789"
    return "".join(chars[secrets.randbelow(len(chars))] for _ in range(n))


def _rand_base32_payload(length: int = 80) -> str:
    """Generate a realistic tunnel-style base32 subdomain."""
    alpha = "abcdefghijklmnopqrstuvwxyz234567"
    raw = "".join(alpha[secrets.randbelow(len(alpha))] for _ in range(length))
    # Split into 44-char labels (matching nexora chunk_label)
    parts = [raw[i:i + 44] for i in range(0, len(raw), 44)]
    return ".".join(parts)


@dataclass
class ProbeResult:
    resolver: str
    port: int
    reachable: bool = False
    random_subdomain: bool = False
    tunnel_realistic: bool = False
    nxdomain_correct: bool = False
    bidirectional: bool = False
    protocol_roundtrip: bool = False
    stream_roundtrip: bool = False
    latency_ms: float = 9999.0
    score: int = 0
    error: Optional[str] = None


@dataclass
class ResolverRuntime:
    resolver: str
    probes: int = 0
    pass_count: int = 0
    fail_count: int = 0
    ewma_score: float = 0.0
    ewma_latency_ms: float = 9999.0
    last_probe_ts: float = 0.0
    last_pass_ts: float = 0.0
    last_error: str = ""
    last_pass_result: Optional[ProbeResult] = None
    consecutive_failures: int = 0
    consecutive_successes: int = 0
    quarantine_until_ts: float = 0.0
    circuit_open_count: int = 0
    next_probe_ts: float = 0.0
    in_flight: int = 0


def _test_reachable(resolver: str, port: int, timeout: float) -> bool:
    """Test basic DNS connectivity with a simple A query."""
    try:
        rcode, _ = _dns_query(resolver, port, f"{_rand_label()}.google.com",
                              _DNS_TYPE_A, timeout)
        return rcode in (0, 3)  # NOERROR or NXDOMAIN both mean it responded
    except Exception:
        return False


def _test_random_subdomain(resolver: str, port: int,
                           zone: str, timeout: float) -> bool:
    """Test that resolver forwards random subdomain queries to our zone."""
    sub = _rand_label(16)
    try:
        rcode, _ = _dns_query(resolver, port, f"{sub}.{zone}",
                              _DNS_TYPE_TXT, timeout)
        # For Nexora zones the authoritative server returns NOERROR-empty for
        # unknown names. NXDOMAIN here is a strong sign of bad resolver path.
        return rcode == 0
    except Exception:
        return False


def _test_tunnel_realistic(resolver: str, port: int,
                          zone: str, timeout: float) -> bool:
    """Send a long base32-encoded TXT query mimicking real tunnel traffic."""
    payload = _rand_base32_payload(80)
    fqdn = f"{payload}.{zone}"
    try:
        rcode, _ = _dns_query(resolver, port, fqdn, _DNS_TYPE_TXT, timeout)
        return rcode == 0
    except Exception:
        return False


def _test_tunnel_stability(
    resolver: str,
    port: int,
    zone: str,
    timeout: float,
    rounds: int = 3,
    min_success: int = 2,
) -> bool:
    """Run realistic tunnel query multiple times to avoid one-shot false positives."""
    ok = 0
    total = max(1, rounds)
    need = max(1, min(min_success, total))
    for _ in range(total):
        if _test_tunnel_realistic(resolver, port, zone, timeout):
            ok += 1
        time.sleep(0.03)
    return ok >= need


def _test_nxdomain(resolver: str, port: int, timeout: float) -> bool:
    """Verify the resolver returns proper NXDOMAIN (not hijacked)."""
    domain = f"nxd-{_rand_label(10)}.invalid"
    try:
        rcode, _ = _dns_query(resolver, port, domain, _DNS_TYPE_A, timeout)
        return rcode == 3  # Must be NXDOMAIN
    except Exception:
        return False


def _extract_txt_strings(packet: bytes) -> list[bytes]:
    """Extract TXT record strings from a DNS response."""
    if len(packet) < 12:
        return []
    qdcount = struct.unpack(">H", packet[4:6])[0]
    ancount = struct.unpack(">H", packet[6:8])[0]
    if ancount == 0:
        return []
    # Skip header (12 bytes) then skip question section
    pos = 12
    for _ in range(qdcount):
        while pos < len(packet):
            length = packet[pos]
            if length == 0:
                pos += 1
                break
            if length >= 0xC0:  # compression pointer
                pos += 2
                break
            pos += 1 + length
        pos += 4  # QTYPE + QCLASS
    # Parse answer RRs looking for TXT (type 16)
    results: list[bytes] = []
    for _ in range(ancount):
        if pos >= len(packet):
            break
        # Skip name (handle compression)
        while pos < len(packet):
            length = packet[pos]
            if length == 0:
                pos += 1
                break
            if length >= 0xC0:
                pos += 2
                break
            pos += 1 + length
        if pos + 10 > len(packet):
            break
        rr_type = struct.unpack(">H", packet[pos:pos+2])[0]
        rdlength = struct.unpack(">H", packet[pos+8:pos+10])[0]
        pos += 10
        if rr_type == _DNS_TYPE_TXT and pos + rdlength <= len(packet):
            rdata = packet[pos:pos+rdlength]
            # TXT RDATA: one or more <length><string> chunks
            rpos = 0
            while rpos < len(rdata):
                slen = rdata[rpos]
                rpos += 1
                if rpos + slen <= len(rdata):
                    results.append(rdata[rpos:rpos+slen])
                rpos += slen
        pos += rdlength
    return results


def _extract_nexora_packets(packet: bytes) -> list[object]:
    """Decode TXT answers into Nexora protocol packets."""
    out: list[object] = []
    for txt_chunk in _extract_txt_strings(packet):
        try:
            raw = _b32_decode_loose(txt_chunk)
            out.append(unpack_packet(raw))
        except Exception:
            continue
    return out


def _test_bidirectional(resolver: str, port: int,
                       zone: str, timeout: float) -> bool:
    """Verify full Nexora protocol round-trip (HELLO -> HELLO_ACK)."""
    nonce = secrets.randbits(32)
    hello = pack_packet(TYPE_HELLO, 0, nonce, b"HP")
    fqdn = f"{_chunk_label(_b32_encode_nopad(hello))}.{zone}"
    try:
        rcode, resp = _dns_query(resolver, port, fqdn, _DNS_TYPE_TXT, timeout)
        if rcode != 0:
            return False
        for pkt in _extract_nexora_packets(resp):
            if pkt.msg_type == TYPE_HELLO_ACK and pkt.nonce == nonce and pkt.session_id > 0:
                # Valid Nexora HELLO_ACK response proves true bi-directional path.
                return True
        return False
    except Exception:
        return False


def _probe_open_session(
    resolver: str,
    port: int,
    zone: str,
    timeout: float,
) -> tuple[int, int]:
    """Return (sid, nonce) from a valid HELLO_ACK, or (0, 0) on failure."""
    hello_nonce = secrets.randbits(32)
    hello_pkt = pack_packet(TYPE_HELLO, 0, hello_nonce, b"SCN_HELLO")
    hello_q = f"{_chunk_label(_b32_encode_nopad(hello_pkt))}.{zone}"
    rcode, resp = _dns_query(resolver, port, hello_q, _DNS_TYPE_TXT, timeout)
    if rcode != 0:
        return 0, 0
    for pkt in _extract_nexora_packets(resp):
        if (
            pkt.msg_type == TYPE_HELLO_ACK
            and pkt.nonce == hello_nonce
            and pkt.session_id > 0
        ):
            return pkt.session_id, hello_nonce
    return 0, 0


def _test_protocol_roundtrip(resolver: str, port: int,
                            zone: str, timeout: float) -> bool:
    """Verify HELLO_ACK + DATA_ACK path using same session (real protocol path)."""
    try:
        sid, _ = _probe_open_session(resolver, port, zone, timeout)
        if sid <= 0:
            return False

        data_nonce = secrets.randbits(32)
        data_pkt = pack_packet(TYPE_DATA, sid, data_nonce, b"SCN_DATA")
        data_q = f"{_chunk_label(_b32_encode_nopad(data_pkt))}.{zone}"
        rcode2, resp2 = _dns_query(resolver, port, data_q, _DNS_TYPE_TXT, timeout)
        if rcode2 != 0:
            return False
        for pkt in _extract_nexora_packets(resp2):
            if (
                pkt.msg_type == TYPE_DATA_ACK
                and pkt.session_id == sid
                and pkt.nonce == data_nonce
            ):
                return True
        return False
    except Exception:
        return False


def _test_stream_roundtrip(
    resolver: str,
    port: int,
    zone: str,
    timeout: float,
) -> bool:
    """Verify STREAM_OPEN_ACK + STREAM_RECV/CLOSE using realistic payload sizes."""
    try:
        sid, _ = _probe_open_session(resolver, port, zone, timeout)
        if sid <= 0:
            return False

        open_nonce = secrets.randbits(32)
        # Intentional local target; ER is acceptable as long as protocol ACK returns.
        open_pkt = pack_packet(TYPE_STREAM_OPEN, sid, open_nonce, b"127.0.0.1:1")
        open_q = f"{_chunk_label(_b32_encode_nopad(open_pkt))}.{zone}"
        rcode_o, resp_o = _dns_query(resolver, port, open_q, _DNS_TYPE_TXT, timeout)
        if rcode_o != 0:
            return False
        got_open_ack = False
        for pkt in _extract_nexora_packets(resp_o):
            if (
                pkt.msg_type == TYPE_STREAM_OPEN_ACK
                and pkt.session_id == sid
                and pkt.nonce == open_nonce
            ):
                got_open_ack = True
                break
        if not got_open_ack:
            return False

        send_nonce = secrets.randbits(32)
        # Near runtime payload size to catch resolvers that fail on long labels.
        send_payload = b"S" * 100
        send_pkt = pack_packet(TYPE_STREAM_SEND, sid, send_nonce, send_payload)
        send_q = f"{_chunk_label(_b32_encode_nopad(send_pkt))}.{zone}"
        rcode_s, resp_s = _dns_query(resolver, port, send_q, _DNS_TYPE_TXT, timeout)
        if rcode_s != 0:
            return False
        for pkt in _extract_nexora_packets(resp_s):
            if pkt.session_id != sid or pkt.nonce != send_nonce:
                continue
            if pkt.msg_type in (TYPE_STREAM_RECV, TYPE_STREAM_CLOSE):
                return True
        return False
    except Exception:
        return False


def _test_stream_roundtrip_stability(
    resolver: str,
    port: int,
    zone: str,
    timeout: float,
    rounds: int = 2,
    min_success: int = 2,
) -> bool:
    ok = 0
    total = max(1, rounds)
    need = max(1, min(min_success, total))
    for _ in range(total):
        if _test_stream_roundtrip(resolver, port, zone, timeout):
            ok += 1
        time.sleep(0.03)
    return ok >= need


def _test_protocol_roundtrip_stability(
    resolver: str,
    port: int,
    zone: str,
    timeout: float,
    rounds: int = 2,
    min_success: int = 2,
) -> bool:
    """Run protocol roundtrip more than once to avoid one-shot false positives."""
    ok = 0
    total = max(1, rounds)
    need = max(1, min(min_success, total))
    for _ in range(total):
        if _test_protocol_roundtrip(resolver, port, zone, timeout):
            ok += 1
        time.sleep(0.03)
    return ok >= need


def _measure_latency(resolver: str, port: int,
                     zone: str, timeout: float, rounds: int = 3) -> float:
    """Measure median latency over multiple TXT queries to the zone."""
    times = []
    for _ in range(rounds):
        sub = _rand_label(12)
        t0 = time.monotonic()
        try:
            _dns_query(resolver, port, f"{sub}.{zone}", _DNS_TYPE_TXT, timeout)
            dt = (time.monotonic() - t0) * 1000
            times.append(dt)
        except Exception:
            times.append(timeout * 1000)
        time.sleep(0.05)  # Small gap between probes
    times.sort()
    return times[len(times) // 2]  # Median


def probe_resolver(resolver: str, port: int, zone: str,
                   timeout: float) -> ProbeResult:
    """Run all tests on a single resolver and return scored result."""
    result = ProbeResult(resolver=resolver, port=port)

    # Test 1: Basic reachability
    result.reachable = _test_reachable(resolver, port, timeout)
    if not result.reachable:
        result.error = "unreachable"
        return result

    # Test 2: Random subdomain forwarding
    result.random_subdomain = _test_random_subdomain(
        resolver, port, zone, timeout
    )

    # Test 3: Tunnel-realistic long query
    if result.random_subdomain:
        result.tunnel_realistic = _test_tunnel_stability(
            resolver, port, zone, timeout
        )

    # Test 4: NXDOMAIN correctness
    result.nxdomain_correct = _test_nxdomain(resolver, port, timeout)

    # Test 5: Bidirectional — confirm our server echoes a nonce
    if result.random_subdomain:
        result.bidirectional = _test_bidirectional(
            resolver, port, zone, timeout
        )

    # Test 6: Real protocol path with sessioned DATA roundtrip.
    if result.bidirectional:
        result.protocol_roundtrip = _test_protocol_roundtrip_stability(
            resolver, port, zone, timeout
        )

    # Test 7: Stream path roundtrip (OPEN/SEND) with runtime-like payload sizes.
    if result.protocol_roundtrip:
        result.stream_roundtrip = _test_stream_roundtrip_stability(
            resolver, port, zone, timeout
        )

    # Measure latency only if resolver passes subdomain test
    if result.random_subdomain:
        result.latency_ms = _measure_latency(resolver, port, zone, timeout)

    # Score: 0-7 (each passed test = 1 point)
    result.score = sum([
        result.reachable,
        result.random_subdomain,
        result.tunnel_realistic,
        result.nxdomain_correct,
        result.bidirectional,
        result.protocol_roundtrip,
        result.stream_roundtrip,
    ])

    return result


# ---------------------------------------------------------------------------
# Resolver list management
# ---------------------------------------------------------------------------

# Local resolver list shipped with the project (data/resolvers.txt).
# Contains ~33K IPs in 3 tiers from SlipNet.
_LOCAL_RESOLVERS_FILE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "data", "resolvers.txt",
)

# Hardcoded fallback: known-good resolvers from Iran
_FALLBACK_RESOLVERS = [
    "178.22.122.100",
    "185.51.200.2",
    "185.55.226.26",
    "185.55.225.25",
    "78.157.42.100",
    "78.157.42.101",
    "185.49.84.2",
    "217.218.26.77",
    "217.218.26.78",
    "185.181.182.209",
    "94.103.125.157",
    "94.103.125.158",
    "46.245.90.90",
    "82.99.213.58",
    "82.99.213.59",
    "82.99.213.60",
    "82.99.213.61",
    "82.99.214.108",
    "185.164.74.183",
]


def _is_valid_ip(ip: str) -> bool:
    parts = ip.split(".")
    if len(parts) != 4:
        return False
    return all(p.isdigit() and 0 <= int(p) <= 255 for p in parts)


def _is_public_ipv4(ip: str) -> bool:
    """Accept only globally-routable IPv4 resolvers."""
    try:
        addr = ipaddress.ip_address(ip)
        return addr.version == 4 and addr.is_global
    except ValueError:
        return False


def _parse_resolver_text(text: str, tier3_sample: int = 200) -> list[str]:
    """Parse resolver list text, split into tiers, sample tier 3."""
    resolvers: list[str] = []
    boundaries: list[int] = []
    seen: set[str] = set()

    for line in text.splitlines():
        trimmed = line.strip()
        if trimmed == "# SHUFFLE_BELOW":
            boundaries.append(len(resolvers))
            continue
        if not trimmed or trimmed.startswith("#"):
            continue
        if _is_valid_ip(trimmed) and _is_public_ipv4(trimmed) and trimmed not in seen:
            seen.add(trimmed)
            resolvers.append(trimmed)

    if not resolvers:
        return []

    # Split into tiers based on SHUFFLE_BELOW markers
    if len(boundaries) >= 2:
        tier1 = resolvers[:boundaries[0]]
        tier2 = resolvers[boundaries[0]:boundaries[1]]
        tier3 = resolvers[boundaries[1]:]
    elif len(boundaries) == 1:
        tier1 = resolvers[:boundaries[0]]
        tier2 = []
        tier3 = resolvers[boundaries[0]:]
    else:
        tier1 = resolvers
        tier2 = []
        tier3 = []

    # Shuffle tier2 and tier3, keep tier1 order
    random.shuffle(tier2)
    if tier3_sample <= 0 or tier3_sample >= len(tier3):
        sampled_tier3 = list(tier3)
    else:
        sampled_tier3 = random.sample(tier3, tier3_sample)

    result = tier1 + tier2 + sampled_tier3
    log.info(
        "resolver list: tier1=%d tier2=%d tier3_sampled=%d/%d total=%d",
        len(tier1), len(tier2), len(sampled_tier3), len(tier3), len(result),
    )
    return result


def load_resolvers(resolver_file: str = "",
                   tier3_sample: int = 200) -> list[str]:
    """Load resolver list from local file with fallback to hardcoded list."""
    # Try explicit path first, then bundled data/resolvers.txt
    paths_to_try = []
    if resolver_file:
        paths_to_try.append(resolver_file)
    paths_to_try.append(_LOCAL_RESOLVERS_FILE)

    resolvers: list[str] = []
    for path in paths_to_try:
        if os.path.isfile(path):
            try:
                with open(path, "r", encoding="utf-8", errors="ignore") as f:
                    text = f.read()
                resolvers = _parse_resolver_text(text, tier3_sample=tier3_sample)
                if resolvers:
                    log.info("loaded %d resolvers from %s", len(resolvers), path)
                    break
            except OSError as e:
                log.warning("failed to read %s: %s", path, e)

    # Keep only globally-routable IPv4s.
    resolvers = [ip for ip in resolvers if _is_public_ipv4(ip)]
    fallback_public = [ip for ip in _FALLBACK_RESOLVERS if _is_public_ipv4(ip)]

    if not resolvers:
        log.warning("using fallback resolver list (%d IPs)", len(fallback_public))
        resolvers = list(fallback_public)

    # Ensure fallback resolvers are always included (at front).
    seen = set(resolvers)
    for fb in fallback_public:
        if fb not in seen:
            resolvers.insert(0, fb)
            seen.add(fb)
    return resolvers


# ---------------------------------------------------------------------------
# Scan orchestration
# ---------------------------------------------------------------------------

_CB_FAIL_THRESHOLD = 3
_CB_BASE_COOLDOWN_SEC = 120.0
_CB_MAX_COOLDOWN_SEC = 1800.0
_HYSTERESIS_SCORE_DELTA = 0.35
_PUBLISH_MIN_PROBES = 3
_PUBLISH_MIN_PASS_RATE = 0.45
_PUBLISH_MAX_LATENCY_MS = 900.0


@dataclass
class ScanReport:
    timestamp: str
    zone: str
    total_scanned: int
    total_working: int
    resolvers: list[dict[str, object]]
    metrics: dict[str, object] = field(default_factory=dict)
    pools: dict[str, list[str]] = field(default_factory=dict)
    generation: int = 0
    rollback_used: bool = False


def _is_working_result(r: ProbeResult) -> bool:
    return (
        r.reachable
        and r.tunnel_realistic
        and r.bidirectional
        and r.protocol_roundtrip
        and r.stream_roundtrip
        and r.nxdomain_correct
    )


def _runtime_pass_rate(state: ResolverRuntime) -> float:
    if state.probes <= 0:
        return 0.0
    return state.pass_count / state.probes


def _runtime_quality_score(state: ResolverRuntime, now_ts: float) -> float:
    """Composite quality score for ranking (higher is better)."""
    pass_rate = _runtime_pass_rate(state)
    recency_penalty = min(1.5, max(0.0, (now_ts - state.last_probe_ts) / 900.0))
    latency_penalty = min(3.0, state.ewma_latency_ms / 250.0)
    instability_penalty = min(2.0, state.consecutive_failures * 0.4)
    if state.quarantine_until_ts > now_ts:
        instability_penalty += 2.0
    return (
        (state.ewma_score * 1.8)
        + (pass_rate * 3.0)
        - latency_penalty
        - instability_penalty
        - recency_penalty
    )


def _sorted_runtime_states(
    stats_snapshot: list[ResolverRuntime],
    now_ts: float,
) -> list[ResolverRuntime]:
    rows = [s for s in stats_snapshot if s.last_pass_result is not None]
    rows.sort(
        key=lambda s: (
            -_runtime_quality_score(s, now_ts),
            -_runtime_pass_rate(s),
            s.ewma_latency_ms,
            -s.last_pass_ts,
            s.resolver,
        )
    )
    return rows


def _classify_pools(
    stats_snapshot: list[ResolverRuntime],
    now_ts: float,
    active_pool_size: int,
    standby_pool_size: int,
) -> tuple[set[str], set[str], set[str]]:
    sorted_rows = _sorted_runtime_states(stats_snapshot, now_ts)
    quarantine = {
        s.resolver for s in stats_snapshot if s.quarantine_until_ts > now_ts
    }

    active: set[str] = set()
    standby: set[str] = set()
    for s in sorted_rows:
        if s.resolver in quarantine:
            continue
        pass_rate = _runtime_pass_rate(s)
        if s.probes < 2:
            continue
        if pass_rate < 0.12:
            continue
        if len(active) < max(1, active_pool_size):
            active.add(s.resolver)
            continue
        if len(standby) < max(0, standby_pool_size):
            standby.add(s.resolver)
            continue
        break

    # Safety fallback: never leave active pool empty when we have successful rows.
    if not active and sorted_rows:
        for s in sorted_rows[: max(1, active_pool_size)]:
            if s.resolver not in quarantine:
                active.add(s.resolver)
    standby -= active
    return active, standby, quarantine


def run_scan(zone: str, port: int = 53, timeout: float = 3.0,
             concurrency: int = 10, top_n: int = 5,
             resolver_file: str = "",
             tier3_sample: int = 200) -> ScanReport:
    """Scan resolvers and return ranked results."""
    candidates = load_resolvers(resolver_file, tier3_sample=tier3_sample)
    log.info("scanning %d resolvers for zone=%s ...", len(candidates), zone)

    results: list[ProbeResult] = []
    scanned = 0

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = {
            pool.submit(probe_resolver, ip, port, zone, timeout): ip
            for ip in candidates
        }
        for future in as_completed(futures):
            scanned += 1
            try:
                result = future.result()
                results.append(result)
                if result.score >= 2:
                    log.info(
                        "  [%d/%d] %s score=%d latency=%.0fms%s",
                        scanned, len(candidates), result.resolver,
                        result.score, result.latency_ms,
                        f" ({result.error})" if result.error else "",
                    )
            except Exception as e:
                ip = futures[future]
                log.debug("probe %s raised: %s", ip, e)
            # Progress log every 50 resolvers
            if scanned % 50 == 0:
                log.info("  progress: %d/%d scanned", scanned, len(candidates))

    # Rank: keep only resolvers that proved real tunnel round-trip.
    working = [r for r in results if _is_working_result(r)]
    working.sort(key=lambda r: (-r.score, r.latency_ms))
    top = working[:top_n]

    log.info(
        "scan complete: %d scanned, %d working, top %d selected",
        scanned, len(working), len(top),
    )
    for i, r in enumerate(top):
        log.info(
            "  #%d %s score=%d/7 latency=%.0fms tunnel=%s nxdomain=%s bidir=%s proto=%s stream=%s",
            i + 1, r.resolver, r.score, r.latency_ms,
            r.tunnel_realistic, r.nxdomain_correct, r.bidirectional, r.protocol_roundtrip, r.stream_roundtrip,
        )

    return ScanReport(
        timestamp=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        zone=zone,
        total_scanned=scanned,
        total_working=len(working),
        resolvers=[asdict(r) for r in top],
    )


def _update_runtime_stats(
    state: ResolverRuntime,
    result: ProbeResult,
    timeout: float,
    now_ts: float,
    alpha: float = 0.25,
) -> None:
    state.probes += 1
    state.last_probe_ts = now_ts
    pass_ok = _is_working_result(result)

    sample_score = float(result.score if pass_ok else 0.0)
    if state.probes == 1:
        state.ewma_score = sample_score
    else:
        state.ewma_score = (1.0 - alpha) * state.ewma_score + alpha * sample_score

    lat_sample = result.latency_ms if result.latency_ms < 9999 else (timeout * 1000.0)
    if state.probes == 1:
        state.ewma_latency_ms = lat_sample
    else:
        state.ewma_latency_ms = (1.0 - alpha) * state.ewma_latency_ms + alpha * lat_sample

    if pass_ok:
        state.pass_count += 1
        state.consecutive_successes += 1
        state.consecutive_failures = 0
        state.last_pass_ts = now_ts
        state.last_pass_result = result
        state.last_error = ""
        if state.quarantine_until_ts > 0.0:
            state.quarantine_until_ts = 0.0
    else:
        state.fail_count += 1
        state.consecutive_failures += 1
        state.consecutive_successes = 0
        state.last_error = result.error or "probe_failed"
        if state.consecutive_failures >= _CB_FAIL_THRESHOLD:
            # Exponential cooldown for resolvers that repeatedly fail.
            state.circuit_open_count += 1
            backoff = min(
                _CB_MAX_COOLDOWN_SEC,
                _CB_BASE_COOLDOWN_SEC * (2 ** min(5, state.circuit_open_count - 1)),
            )
            state.quarantine_until_ts = max(state.quarantine_until_ts, now_ts + backoff)


def _build_continuous_report(
    zone: str,
    stats_snapshot: list[ResolverRuntime],
    active_pool: set[str],
    standby_pool: set[str],
    quarantine_pool: set[str],
    generation: int,
    top_n: int,
    stale_after_sec: float = 1800.0,
    min_probes: int = _PUBLISH_MIN_PROBES,
    min_success_rate: float = _PUBLISH_MIN_PASS_RATE,
    max_latency_ms: float = _PUBLISH_MAX_LATENCY_MS,
) -> ScanReport:
    now_ts = time.time()
    ranked_rows: list[tuple[float, dict[str, object]]] = []

    for st in stats_snapshot:
        if st.last_pass_result is None:
            continue
        if st.resolver in quarantine_pool:
            continue
        if st.probes < max(1, min_probes):
            continue
        if stale_after_sec > 0 and (now_ts - st.last_probe_ts) > stale_after_sec:
            continue

        success_rate = _runtime_pass_rate(st)
        if success_rate < min_success_rate:
            continue
        if max_latency_ms > 0 and st.ewma_latency_ms > max_latency_ms:
            continue

        quality = _runtime_quality_score(st, now_ts)
        row = asdict(st.last_pass_result)
        row["score"] = max(0.0, min(7.0, round(st.ewma_score, 2)))
        row["latency_ms"] = round(st.ewma_latency_ms, 1)
        row["runtime_quality"] = round(quality, 3)
        row["runtime_probes"] = st.probes
        row["runtime_pass_rate"] = round(success_rate, 3)
        row["runtime_consecutive_failures"] = st.consecutive_failures
        row["runtime_last_probe_ts"] = int(st.last_probe_ts)
        row["pool"] = (
            "active"
            if st.resolver in active_pool
            else "standby"
            if st.resolver in standby_pool
            else "cold"
        )
        ranked_rows.append((quality, row))

    ranked_rows.sort(
        key=lambda item: (
            -item[0],
            -item[1]["runtime_pass_rate"],
            item[1]["latency_ms"],
            item[1]["resolver"],
        )
    )
    top_rows = [row for _, row in ranked_rows[:top_n]]

    # Soft fallback: if strict filter is empty, publish last successful rows.
    if not top_rows:
        soft_rows: list[tuple[float, dict[str, object]]] = []
        soft_latency_cap = (
            max(1200.0, max_latency_ms * 1.5) if max_latency_ms > 0 else 1400.0
        )
        soft_min_pass_rate = max(0.25, min_success_rate * 0.6)
        for st in stats_snapshot:
            if st.last_pass_result is None or st.resolver in quarantine_pool:
                continue
            success_rate = _runtime_pass_rate(st)
            if success_rate < soft_min_pass_rate:
                continue
            if st.ewma_latency_ms > soft_latency_cap:
                continue
            quality = _runtime_quality_score(st, now_ts)
            row = asdict(st.last_pass_result)
            row["score"] = max(0.0, min(7.0, round(st.ewma_score, 2)))
            row["latency_ms"] = round(st.ewma_latency_ms, 1)
            row["runtime_quality"] = round(quality, 3)
            row["runtime_probes"] = st.probes
            row["runtime_pass_rate"] = round(success_rate, 3)
            row["runtime_consecutive_failures"] = st.consecutive_failures
            row["runtime_last_probe_ts"] = int(st.last_probe_ts)
            row["pool"] = "fallback"
            soft_rows.append((quality, row))
        soft_rows.sort(key=lambda item: (-item[0], item[1]["latency_ms"], item[1]["resolver"]))
        top_rows = [row for _, row in soft_rows[:top_n]]

    total_scanned = sum(st.probes for st in stats_snapshot)
    total_success = sum(st.pass_count for st in stats_snapshot)
    global_success_rate = (float(total_success) / float(total_scanned)) if total_scanned > 0 else 0.0

    metrics = {
        "global_success_rate": round(global_success_rate, 4),
        "total_successful_probes": total_success,
        "total_failed_probes": sum(st.fail_count for st in stats_snapshot),
        "circuit_open_total": sum(st.circuit_open_count for st in stats_snapshot),
        "active_pool_size": len(active_pool),
        "standby_pool_size": len(standby_pool),
        "quarantine_pool_size": len(quarantine_pool),
        "candidate_count": len(stats_snapshot),
    }

    return ScanReport(
        timestamp=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        zone=zone,
        total_scanned=total_scanned,
        total_working=len(ranked_rows),
        resolvers=top_rows,
        metrics=metrics,
        pools={
            "active": sorted(active_pool),
            "standby": sorted(standby_pool),
            "quarantine": sorted(quarantine_pool),
        },
        generation=generation,
    )


def _row_quality(row: dict[str, object]) -> float:
    raw = row.get("runtime_quality", row.get("score", 0.0))
    try:
        return float(raw)
    except (TypeError, ValueError):
        return 0.0


def _apply_publish_hysteresis(
    new_rows: list[dict[str, object]],
    previous_rows: list[dict[str, object]],
    top_n: int,
) -> list[dict[str, object]]:
    if not new_rows:
        return []
    if not previous_rows:
        return list(new_rows[:top_n])

    new_by_resolver = {str(r.get("resolver", "")): r for r in new_rows}
    chosen: list[dict[str, object]] = []
    used: set[str] = set()

    max_slots = min(top_n, max(len(new_rows), len(previous_rows)))
    for idx in range(max_slots):
        cand = new_rows[idx] if idx < len(new_rows) else None
        prev = previous_rows[idx] if idx < len(previous_rows) else None

        picked: Optional[dict[str, object]] = None
        if prev is not None:
            prev_resolver = str(prev.get("resolver", ""))
            prev_live = new_by_resolver.get(prev_resolver)
            if prev_live is not None and prev_resolver not in used:
                if cand is None:
                    picked = prev_live
                else:
                    if _row_quality(prev_live) + _HYSTERESIS_SCORE_DELTA >= _row_quality(cand):
                        picked = prev_live

        if picked is None and cand is not None:
            cand_resolver = str(cand.get("resolver", ""))
            if cand_resolver and cand_resolver not in used:
                picked = cand

        if picked is not None:
            picked_resolver = str(picked.get("resolver", ""))
            if picked_resolver and picked_resolver not in used:
                used.add(picked_resolver)
                chosen.append(picked)

    for row in new_rows:
        if len(chosen) >= top_n:
            break
        rname = str(row.get("resolver", ""))
        if not rname or rname in used:
            continue
        used.add(rname)
        chosen.append(row)
    return chosen[:top_n]


def _is_report_good(
    rows: list[dict[str, object]],
    min_count: int = 2,
    min_avg_pass_rate: float = 0.22,
) -> bool:
    if len(rows) < min_count:
        return False
    rates = []
    for r in rows:
        try:
            rates.append(float(r.get("runtime_pass_rate", 0.0)))
        except (TypeError, ValueError):
            rates.append(0.0)
    if not rates:
        return False
    avg_rate = sum(rates) / len(rates)
    return avg_rate >= min_avg_pass_rate


def _load_previous_publish(
    output_path: str,
    top_n: int,
) -> tuple[list[dict[str, object]], int]:
    """Load previous resolver rows from output JSON for warm startup."""
    try:
        if not os.path.isfile(output_path):
            return [], 0
        with open(output_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        rows_raw = data.get("resolvers", [])
        if not isinstance(rows_raw, list):
            return [], 0
        rows: list[dict[str, object]] = []
        for item in rows_raw:
            if not isinstance(item, dict):
                continue
            resolver = str(item.get("resolver", "")).strip()
            if not _is_public_ipv4(resolver):
                continue
            row = dict(item)
            row["resolver"] = resolver
            rows.append(row)
        generation = int(data.get("generation", 0) or 0)
        return rows[: max(0, top_n)], generation
    except Exception:
        return [], 0


def run_continuous_scan(
    zone: str,
    output_path: str,
    port: int = 53,
    timeout: float = 3.0,
    concurrency: int = 10,
    top_n: int = 5,
    resolver_file: str = "",
    tier3_sample: int = 200,
    publish_interval: float = 300.0,
    active_pool_size: int = 24,
    standby_pool_size: int = 200,
    hot_probe_interval: float = 12.0,
    standby_probe_interval: float = 120.0,
    cold_probe_interval: float = 1800.0,
    max_inflight_per_resolver: int = 1,
    rollback_ttl_sec: float = 1800.0,
    alert_min_active: int = 6,
    publish_min_probes: int = _PUBLISH_MIN_PROBES,
    publish_min_pass_rate: float = _PUBLISH_MIN_PASS_RATE,
    publish_max_latency_ms: float = _PUBLISH_MAX_LATENCY_MS,
) -> None:
    candidates = load_resolvers(resolver_file, tier3_sample=tier3_sample)
    if not candidates:
        raise RuntimeError("no resolver candidates loaded")

    log.info(
        (
            "continuous mode: candidates=%d concurrency=%d publish=%.0fs "
            "active=%d standby=%d hot=%.1fs standby=%.1fs cold=%.1fs"
        ),
        len(candidates),
        concurrency,
        publish_interval,
        active_pool_size,
        standby_pool_size,
        hot_probe_interval,
        standby_probe_interval,
        cold_probe_interval,
    )

    lock = Lock()
    stop_event = Event()
    states: dict[str, ResolverRuntime] = {
        ip: ResolverRuntime(resolver=ip) for ip in candidates
    }
    active_pool: set[str] = set()
    standby_pool: set[str] = set()
    quarantine_pool: set[str] = set()
    heap_items: list[tuple[float, int, str]] = []
    heap_seq = 0
    generation = 0
    started_ts = time.time()
    last_published_rows: list[dict[str, object]] = []
    last_good_rows: list[dict[str, object]] = []
    last_good_generation = 0
    last_good_ts = 0.0

    prev_rows, prev_generation = _load_previous_publish(output_path, top_n=top_n)
    if prev_rows:
        last_published_rows = [dict(r) for r in prev_rows]
        generation = max(generation, prev_generation)
        if _is_report_good(prev_rows, min_count=max(2, min(4, top_n))):
            last_good_rows = [dict(r) for r in prev_rows]
            last_good_generation = prev_generation
            last_good_ts = started_ts
        log.info(
            "bootstrap previous publish rows=%d generation=%d",
            len(prev_rows),
            prev_generation,
        )

    def _push_state_locked(state: ResolverRuntime) -> None:
        nonlocal heap_seq
        heapq.heappush(heap_items, (state.next_probe_ts, heap_seq, state.resolver))
        heap_seq += 1

    def _seed_schedule_locked(now_ts: float) -> None:
        warm_count = max(concurrency * 2, active_pool_size)
        preferred = {ip for ip in _FALLBACK_RESOLVERS if ip in states}
        for idx, ip in enumerate(candidates):
            st = states[ip]
            if ip in preferred or idx < warm_count:
                st.next_probe_ts = now_ts + random.uniform(0.0, min(8.0, hot_probe_interval))
            else:
                st.next_probe_ts = now_ts + random.uniform(1.0, max(5.0, cold_probe_interval))
            _push_state_locked(st)

    def _priority_locked(state: ResolverRuntime, now_ts: float) -> float:
        pool_bonus = (
            2.5
            if state.resolver in active_pool
            else 1.2
            if state.resolver in standby_pool
            else 0.0
        )
        return _runtime_quality_score(state, now_ts) + pool_bonus - (state.in_flight * 0.9)

    def _pop_probe_target_locked(now_ts: float) -> tuple[Optional[str], float]:
        due_states: list[ResolverRuntime] = []
        wait_hint = 0.2

        while heap_items and len(due_states) < 24:
            due_ts, _, ip = heap_items[0]
            if due_ts > now_ts:
                wait_hint = max(0.05, due_ts - now_ts)
                break

            heapq.heappop(heap_items)
            st = states.get(ip)
            if st is None:
                continue
            if abs(due_ts - st.next_probe_ts) > 0.001:
                continue  # stale heap item
            if st.in_flight >= max(1, max_inflight_per_resolver):
                st.next_probe_ts = now_ts + random.uniform(0.15, 0.35)
                _push_state_locked(st)
                continue
            due_states.append(st)

        if not due_states:
            return None, wait_hint

        if len(due_states) == 1:
            chosen = due_states[0]
        else:
            a = random.choice(due_states)
            b = random.choice(due_states)
            if len(due_states) > 1:
                while b.resolver == a.resolver:
                    b = random.choice(due_states)
            chosen = a if _priority_locked(a, now_ts) >= _priority_locked(b, now_ts) else b

        for st in due_states:
            if st.resolver == chosen.resolver:
                continue
            st.next_probe_ts = now_ts + random.uniform(0.05, 0.25)
            _push_state_locked(st)

        chosen.in_flight += 1
        return chosen.resolver, 0.0

    def _schedule_next_probe_locked(state: ResolverRuntime, now_ts: float) -> None:
        if state.quarantine_until_ts > now_ts:
            state.next_probe_ts = state.quarantine_until_ts
            _push_state_locked(state)
            return

        if state.resolver in active_pool:
            base_interval = max(1.0, hot_probe_interval)
        elif state.resolver in standby_pool:
            base_interval = max(2.0, standby_probe_interval)
        else:
            base_interval = max(5.0, cold_probe_interval)

        jitter = random.uniform(base_interval * 0.1, base_interval * 0.35)
        state.next_probe_ts = now_ts + (base_interval * 0.75) + jitter
        _push_state_locked(state)

    def _expedite_pool_locked(pool: set[str], max_delay: float, now_ts: float) -> None:
        if not pool:
            return
        ceiling = max(0.5, max_delay)
        for ip in pool:
            st = states.get(ip)
            if st is None:
                continue
            if st.in_flight > 0:
                continue
            target = now_ts + random.uniform(0.0, ceiling)
            if st.next_probe_ts > target:
                st.next_probe_ts = target
                _push_state_locked(st)

    with lock:
        _seed_schedule_locked(time.time())

    def _worker() -> None:
        while not stop_event.is_set():
            with lock:
                now_ts = time.time()
                ip, wait_for = _pop_probe_target_locked(now_ts)
            if ip is None:
                stop_event.wait(min(0.5, wait_for))
                continue

            try:
                res = probe_resolver(ip, port, zone, timeout)
            except Exception as e:
                res = ProbeResult(resolver=ip, port=port, error=str(e))
            finished_ts = time.time()
            with lock:
                st = states[ip]
                was_quarantined_until = st.quarantine_until_ts
                _update_runtime_stats(st, res, timeout, finished_ts)
                if st.quarantine_until_ts > finished_ts and st.quarantine_until_ts > was_quarantined_until:
                    log.info(
                        "circuit open resolver=%s fail_streak=%d cooldown=%.0fs",
                        ip,
                        st.consecutive_failures,
                        st.quarantine_until_ts - finished_ts,
                    )
                st.in_flight = max(0, st.in_flight - 1)
                _schedule_next_probe_locked(st, finished_ts)

    workers = max(1, concurrency)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(_worker) for _ in range(workers)]
        try:
            interval = max(5.0, publish_interval)
            delay = min(10.0, interval)
            while not stop_event.is_set():
                time.sleep(delay)
                delay = interval
                if stop_event.is_set():
                    break

                with lock:
                    now_ts = time.time()
                    snapshot = [
                        ResolverRuntime(
                            resolver=s.resolver,
                            probes=s.probes,
                            pass_count=s.pass_count,
                            fail_count=s.fail_count,
                            ewma_score=s.ewma_score,
                            ewma_latency_ms=s.ewma_latency_ms,
                            last_probe_ts=s.last_probe_ts,
                            last_pass_ts=s.last_pass_ts,
                            last_error=s.last_error,
                            last_pass_result=s.last_pass_result,
                            consecutive_failures=s.consecutive_failures,
                            consecutive_successes=s.consecutive_successes,
                            quarantine_until_ts=s.quarantine_until_ts,
                            circuit_open_count=s.circuit_open_count,
                            next_probe_ts=s.next_probe_ts,
                            in_flight=s.in_flight,
                        )
                        for s in states.values()
                    ]
                    active_pool, standby_pool, quarantine_pool = _classify_pools(
                        snapshot,
                        now_ts,
                        active_pool_size=active_pool_size,
                        standby_pool_size=standby_pool_size,
                    )
                    _expedite_pool_locked(active_pool, hot_probe_interval, now_ts)
                    _expedite_pool_locked(standby_pool, standby_probe_interval, now_ts)
                    generation += 1

                report = _build_continuous_report(
                    zone,
                    snapshot,
                    active_pool=active_pool,
                    standby_pool=standby_pool,
                    quarantine_pool=quarantine_pool,
                    generation=generation,
                    top_n=top_n,
                    min_probes=publish_min_probes,
                    min_success_rate=publish_min_pass_rate,
                    max_latency_ms=publish_max_latency_ms,
                )
                report.resolvers = _apply_publish_hysteresis(
                    report.resolvers,
                    last_published_rows,
                    top_n=top_n,
                )

                if _is_report_good(report.resolvers, min_count=max(2, min(4, top_n))):
                    last_good_rows = [dict(r) for r in report.resolvers]
                    last_good_generation = report.generation
                    last_good_ts = time.time()
                elif last_good_rows and (time.time() - last_good_ts) <= max(60.0, rollback_ttl_sec):
                    report.rollback_used = True
                    report.resolvers = [dict(r) for r in last_good_rows[:top_n]]
                    report.metrics["rollback_from_generation"] = last_good_generation
                    report.metrics["rollback_age_sec"] = round(time.time() - last_good_ts, 1)
                    report.metrics["rollback_reason"] = "low_quality_publish"
                elif not report.resolvers and last_published_rows:
                    report.rollback_used = True
                    report.resolvers = [dict(r) for r in last_published_rows[:top_n]]
                    report.metrics["rollback_from_generation"] = generation
                    report.metrics["rollback_reason"] = "empty_publish_reuse_previous"

                last_published_rows = [dict(r) for r in report.resolvers]
                uptime = max(1.0, time.time() - started_ts)
                report.metrics["probes_per_sec"] = round(report.total_scanned / uptime, 3)
                report.metrics["uptime_sec"] = int(uptime)

                write_report(report, output_path)

                log.info(
                    (
                        "publish gen=%d scanned=%d working=%d top=%d "
                        "active=%d standby=%d quarantine=%d rollback=%s"
                    ),
                    report.generation,
                    report.total_scanned,
                    report.total_working,
                    len(report.resolvers),
                    len(active_pool),
                    len(standby_pool),
                    len(quarantine_pool),
                    report.rollback_used,
                )
                if len(active_pool) < max(1, alert_min_active):
                    log.warning(
                        "alert: active pool low (%d < %d), tunnel may degrade",
                        len(active_pool),
                        alert_min_active,
                    )
                if float(report.metrics.get("global_success_rate", 0.0)) < 0.15:
                    log.warning(
                        "alert: low probe success rate=%.3f (scanned=%d)",
                        float(report.metrics.get("global_success_rate", 0.0)),
                        report.total_scanned,
                    )
                for idx, row in enumerate(
                    report.resolvers[: min(5, len(report.resolvers))], start=1
                ):
                    log.info(
                        (
                            "  #%d %s quality=%s score=%s pass_rate=%s "
                            "latency=%sms probes=%s pool=%s"
                        ),
                        idx,
                        row.get("resolver"),
                        row.get("runtime_quality"),
                        row.get("score"),
                        row.get("runtime_pass_rate"),
                        row.get("latency_ms"),
                        row.get("runtime_probes"),
                        row.get("pool"),
                    )
        finally:
            stop_event.set()
            for future in futures:
                try:
                    future.result(timeout=max(5.0, timeout * 4))
                except Exception:
                    pass


def write_report(report: ScanReport, output_path: str) -> None:
    """Atomically write scan report to JSON file."""
    out_dir = os.path.dirname(output_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    data = asdict(report)
    # Also write a flat list for easy consumption by the client
    data["resolver_list"] = [r["resolver"] for r in report.resolvers]

    fd, tmp_path = tempfile.mkstemp(
        dir=out_dir or ".", suffix=".tmp", prefix=".resolvers-"
    )
    try:
        with os.fdopen(fd, "w") as f:
            json.dump(data, f, indent=2)
            f.write("\n")
        os.replace(tmp_path, output_path)
        log.info("wrote %s (%d resolvers)", output_path, len(report.resolvers))
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    )

    p = argparse.ArgumentParser(
        description="Nexora Resolver Scanner — discover best DNS resolvers"
    )
    p.add_argument(
        "--zone", required=True,
        help="DNS tunnel zone (e.g. t1.phonexpress.ir)",
    )
    p.add_argument(
        "--output", default="/etc/nexora/resolvers.json",
        help="output JSON file path (default: /etc/nexora/resolvers.json)",
    )
    p.add_argument(
        "--port", type=int, default=53,
        help="DNS port (default: 53)",
    )
    p.add_argument(
        "--timeout", type=float, default=3.0,
        help="per-probe timeout in seconds (default: 3.0)",
    )
    p.add_argument(
        "--concurrency", type=int, default=10,
        help="max concurrent probes (default: 10)",
    )
    p.add_argument(
        "--top", type=int, default=5,
        help="number of top resolvers to keep (default: 5)",
    )
    p.add_argument(
        "--tier3-sample", type=int, default=200,
        help="how many tier-3 resolvers to sample (default: 200, <=0 means all)",
    )
    p.add_argument(
        "--resolver-file", default="",
        help="path to resolvers.txt (default: data/resolvers.txt in project)",
    )
    p.add_argument(
        "--loop", type=float, default=0,
        help="if >0, run continuous async scanning and publish every N seconds",
    )
    p.add_argument(
        "--active-pool-size", type=int, default=24,
        help="target active resolver pool size in continuous mode (default: 24)",
    )
    p.add_argument(
        "--standby-pool-size", type=int, default=200,
        help="target standby resolver pool size in continuous mode (default: 200)",
    )
    p.add_argument(
        "--hot-probe-interval", type=float, default=12.0,
        help="active pool probe interval in seconds (default: 12)",
    )
    p.add_argument(
        "--standby-probe-interval", type=float, default=120.0,
        help="standby pool probe interval in seconds (default: 120)",
    )
    p.add_argument(
        "--cold-probe-interval", type=float, default=1800.0,
        help="cold pool probe interval in seconds (default: 1800)",
    )
    p.add_argument(
        "--max-inflight-per-resolver", type=int, default=1,
        help="max concurrent probes per resolver (default: 1)",
    )
    p.add_argument(
        "--rollback-ttl", type=float, default=1800.0,
        help="max seconds to reuse last known-good resolver list on bad publish",
    )
    p.add_argument(
        "--alert-min-active", type=int, default=6,
        help="alert threshold for active pool size (default: 6)",
    )
    p.add_argument(
        "--publish-min-probes", type=int, default=_PUBLISH_MIN_PROBES,
        help="minimum probes before resolver is publish-eligible (default: 3)",
    )
    p.add_argument(
        "--publish-min-pass-rate", type=float, default=_PUBLISH_MIN_PASS_RATE,
        help="minimum pass rate [0..1] for publish-eligible resolvers (default: 0.45)",
    )
    p.add_argument(
        "--publish-max-latency-ms", type=float, default=_PUBLISH_MAX_LATENCY_MS,
        help="maximum EWMA latency in ms for publish-eligible resolvers (default: 900)",
    )
    args = p.parse_args()

    if args.loop > 0:
        log.info(
            "continuous mode: publish interval %.0f seconds (%.1f hours)",
            args.loop, args.loop / 3600,
        )
        run_continuous_scan(
            zone=args.zone,
            output_path=args.output,
            port=args.port,
            timeout=args.timeout,
            concurrency=args.concurrency,
            top_n=args.top,
            resolver_file=args.resolver_file,
            tier3_sample=args.tier3_sample,
            publish_interval=args.loop,
            active_pool_size=args.active_pool_size,
            standby_pool_size=args.standby_pool_size,
            hot_probe_interval=args.hot_probe_interval,
            standby_probe_interval=args.standby_probe_interval,
            cold_probe_interval=args.cold_probe_interval,
            max_inflight_per_resolver=args.max_inflight_per_resolver,
            rollback_ttl_sec=args.rollback_ttl,
            alert_min_active=args.alert_min_active,
            publish_min_probes=args.publish_min_probes,
            publish_min_pass_rate=args.publish_min_pass_rate,
            publish_max_latency_ms=args.publish_max_latency_ms,
        )
    else:
        report = run_scan(
            zone=args.zone,
            port=args.port,
            timeout=args.timeout,
            concurrency=args.concurrency,
            top_n=args.top,
            resolver_file=args.resolver_file,
            tier3_sample=args.tier3_sample,
        )
        write_report(report, args.output)


if __name__ == "__main__":
    main()
