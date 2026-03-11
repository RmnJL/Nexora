"""
Nexora Resolver Scanner — background daemon that discovers and ranks
DNS resolvers compatible with the Nexora tunnel.

Runs periodically, tests resolvers for tunnel compatibility, and writes
the best ones to a JSON file that the client reads automatically.

Signature: Rmn JL
"""

from __future__ import annotations

import argparse
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
from dataclasses import dataclass, asdict
from typing import Optional
from urllib.request import urlopen, Request
from urllib.error import URLError

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
    latency_ms: float = 9999.0
    score: int = 0
    error: Optional[str] = None


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
        # Any response (even NXDOMAIN) means the resolver processed the query
        # and reached our authoritative server (or cached the zone).
        return rcode in (0, 3)
    except Exception:
        return False


def _test_tunnel_realistic(resolver: str, port: int,
                           zone: str, timeout: float) -> bool:
    """Send a long base32-encoded TXT query mimicking real tunnel traffic."""
    payload = _rand_base32_payload(80)
    fqdn = f"{payload}.{zone}"
    try:
        rcode, _ = _dns_query(resolver, port, fqdn, _DNS_TYPE_TXT, timeout)
        return rcode in (0, 3)
    except Exception:
        return False


def _test_nxdomain(resolver: str, port: int, timeout: float) -> bool:
    """Verify the resolver returns proper NXDOMAIN (not hijacked)."""
    domain = f"nxd-{_rand_label(10)}.invalid"
    try:
        rcode, _ = _dns_query(resolver, port, domain, _DNS_TYPE_A, timeout)
        return rcode == 3  # Must be NXDOMAIN
    except Exception:
        return False


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
        result.tunnel_realistic = _test_tunnel_realistic(
            resolver, port, zone, timeout
        )

    # Test 4: NXDOMAIN correctness
    result.nxdomain_correct = _test_nxdomain(resolver, port, timeout)

    # Measure latency only if resolver passes subdomain test
    if result.random_subdomain:
        result.latency_ms = _measure_latency(resolver, port, zone, timeout)

    # Score: 0-4 (each passed test = 1 point)
    result.score = sum([
        result.reachable,
        result.random_subdomain,
        result.tunnel_realistic,
        result.nxdomain_correct,
    ])

    return result


# ---------------------------------------------------------------------------
# Resolver list management
# ---------------------------------------------------------------------------

# Tier 1+2 from SlipNet: known public + Iranian ISP resolvers (~230 IPs).
# Tier 1 is mostly international (blocked from Iran), Tier 2 has Iranian
# ISP resolvers which are the primary candidates.
_SEED_RESOLVERS_URL = (
    "https://raw.githubusercontent.com/anonvector/SlipNet"
    "/main/app/src/main/res/raw/resolvers.txt"
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
    "10.202.10.202",
    "10.202.10.102",
    "10.202.10.10",
    "10.202.10.11",
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


def _fetch_resolver_list(url: str, timeout: float = 15.0,
                         tier3_sample: int = 200) -> list[str]:
    """Download resolver list from URL, parse tiers, sample tier 3."""
    try:
        req = Request(url, headers={"User-Agent": "Nexora-Scanner/1.0"})
        with urlopen(req, timeout=timeout) as resp:
            text = resp.read().decode("utf-8", errors="ignore")
    except (URLError, OSError) as e:
        log.warning("failed to fetch resolver list: %s", e)
        return []

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
        if _is_valid_ip(trimmed) and trimmed not in seen:
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
    sampled_tier3 = random.sample(tier3, min(tier3_sample, len(tier3)))

    result = tier1 + tier2 + sampled_tier3
    log.info(
        "resolver list: tier1=%d tier2=%d tier3_sampled=%d/%d total=%d",
        len(tier1), len(tier2), len(sampled_tier3), len(tier3), len(result),
    )
    return result


def load_resolvers(url: str, tier3_sample: int = 200) -> list[str]:
    """Load resolver list from URL with fallback to hardcoded list."""
    resolvers = _fetch_resolver_list(url, tier3_sample=tier3_sample)
    if not resolvers:
        log.warning("using fallback resolver list (%d IPs)", len(_FALLBACK_RESOLVERS))
        return list(_FALLBACK_RESOLVERS)

    # Ensure fallback resolvers are always included (at front)
    seen = set(resolvers)
    for fb in _FALLBACK_RESOLVERS:
        if fb not in seen:
            resolvers.insert(0, fb)
    return resolvers


# ---------------------------------------------------------------------------
# Scan orchestration
# ---------------------------------------------------------------------------

@dataclass
class ScanReport:
    timestamp: str
    zone: str
    total_scanned: int
    total_working: int
    resolvers: list[dict[str, object]]


def run_scan(zone: str, port: int = 53, timeout: float = 3.0,
             concurrency: int = 10, top_n: int = 5,
             url: str = _SEED_RESOLVERS_URL,
             tier3_sample: int = 200) -> ScanReport:
    """Scan resolvers and return ranked results."""
    candidates = load_resolvers(url, tier3_sample=tier3_sample)
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

    # Rank: highest score first, then lowest latency
    working = [r for r in results if r.score >= 2 and r.random_subdomain]
    working.sort(key=lambda r: (-r.score, r.latency_ms))
    top = working[:top_n]

    log.info(
        "scan complete: %d scanned, %d working, top %d selected",
        scanned, len(working), len(top),
    )
    for i, r in enumerate(top):
        log.info(
            "  #%d %s score=%d/4 latency=%.0fms tunnel=%s nxdomain=%s",
            i + 1, r.resolver, r.score, r.latency_ms,
            r.tunnel_realistic, r.nxdomain_correct,
        )

    return ScanReport(
        timestamp=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        zone=zone,
        total_scanned=scanned,
        total_working=len(working),
        resolvers=[asdict(r) for r in top],
    )


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
        help="how many tier-3 resolvers to sample (default: 200)",
    )
    p.add_argument(
        "--url", default=_SEED_RESOLVERS_URL,
        help="URL to fetch resolver list from",
    )
    p.add_argument(
        "--loop", type=float, default=0,
        help="if >0, repeat scan every N seconds (daemon mode)",
    )
    args = p.parse_args()

    if args.loop > 0:
        log.info(
            "daemon mode: scanning every %.0f seconds (%.1f hours)",
            args.loop, args.loop / 3600,
        )
        while True:
            try:
                report = run_scan(
                    zone=args.zone,
                    port=args.port,
                    timeout=args.timeout,
                    concurrency=args.concurrency,
                    top_n=args.top,
                    url=args.url,
                    tier3_sample=args.tier3_sample,
                )
                write_report(report, args.output)
            except Exception:
                log.exception("scan cycle failed")
            time.sleep(args.loop)
    else:
        report = run_scan(
            zone=args.zone,
            port=args.port,
            timeout=args.timeout,
            concurrency=args.concurrency,
            top_n=args.top,
            url=args.url,
            tier3_sample=args.tier3_sample,
        )
        write_report(report, args.output)


if __name__ == "__main__":
    main()
