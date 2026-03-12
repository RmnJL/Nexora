"""
Nexora phase-1 client:
- builds HELLO packet
- sends DNS TXT query
- validates HELLO_ACK
Signature: Rmn JL
"""

from __future__ import annotations

import argparse
import ipaddress
import json
import logging
import os
import random
import socket
import threading
import time
from collections import OrderedDict, deque
from concurrent.futures import (
    FIRST_COMPLETED,
    ThreadPoolExecutor,
    as_completed,
    wait as _futures_wait,
)
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


def _percentile(sorted_values: list[float], pct: float) -> float:
    if not sorted_values:
        return 0.0
    pct = max(0.0, min(100.0, float(pct)))
    if len(sorted_values) == 1:
        return float(sorted_values[0])
    rank = (pct / 100.0) * (len(sorted_values) - 1)
    lo = int(rank)
    hi = min(len(sorted_values) - 1, lo + 1)
    frac = rank - lo
    return float(sorted_values[lo] + (sorted_values[hi] - sorted_values[lo]) * frac)


class _RuntimeKpi:
    def __init__(self, latency_window: int = 4096) -> None:
        self._lock = Lock()
        self._latency_ms: deque[float] = deque(maxlen=max(64, int(latency_window)))
        self.query_total = 0
        self.query_success = 0
        self.query_fail = 0
        self.resolver_switch_count = 0
        self.retry_delay_count = 0
        self.retry_delay_total_s = 0.0
        self.broadcast_success_count = 0
        self.broadcast_fail_count = 0
        self.fallback_success_count = 0
        self.fallback_fail_count = 0
        self.max_conns_reject_count = 0
        self.max_conns_per_ip_reject_count = 0
        self.data_loss_incidents = 0
        self.active_stream_count = 0
        self.active_stream_peak = 0
        self.retry_count_distribution: dict[int, int] = {}
        self.failure_type_histogram: dict[str, int] = {}
        self.success_per_resolver: dict[str, int] = {}
        self.fail_per_resolver: dict[str, int] = {}
        self._started_at = time.time()

    def _inc_map(self, target: dict, key: str | int, amount: int = 1) -> None:
        target[key] = target.get(key, 0) + amount

    def record_query(
        self,
        success: bool,
        latency_ms: float,
        attempts_used: int,
        resolver: str = "",
        failure_type: str = "",
    ) -> None:
        with self._lock:
            self.query_total += 1
            if success:
                self.query_success += 1
                if resolver:
                    self._inc_map(self.success_per_resolver, resolver)
            else:
                self.query_fail += 1
                if resolver:
                    self._inc_map(self.fail_per_resolver, resolver)
                if failure_type:
                    self._inc_map(self.failure_type_histogram, failure_type)
            self._latency_ms.append(max(0.0, float(latency_ms)))
            self._inc_map(self.retry_count_distribution, max(1, int(attempts_used)))

    def record_failure(self, resolver: str, failure_type: str) -> None:
        with self._lock:
            if resolver:
                self._inc_map(self.fail_per_resolver, resolver)
            if failure_type:
                self._inc_map(self.failure_type_histogram, failure_type)

    def record_retry_delay(self, wait_s: float) -> None:
        with self._lock:
            self.retry_delay_count += 1
            self.retry_delay_total_s += max(0.0, float(wait_s))

    def record_resolver_switch(self) -> None:
        with self._lock:
            self.resolver_switch_count += 1

    def record_broadcast(self, success: bool) -> None:
        with self._lock:
            if success:
                self.broadcast_success_count += 1
            else:
                self.broadcast_fail_count += 1

    def record_fallback(self, success: bool) -> None:
        with self._lock:
            if success:
                self.fallback_success_count += 1
            else:
                self.fallback_fail_count += 1

    def record_reject(self, reason: str) -> None:
        with self._lock:
            if reason == "max_conns":
                self.max_conns_reject_count += 1
            elif reason == "max_conns_per_ip":
                self.max_conns_per_ip_reject_count += 1

    def record_data_loss(self, dropped_chunks: int = 1) -> None:
        with self._lock:
            self.data_loss_incidents += max(1, int(dropped_chunks))

    def stream_open(self) -> None:
        with self._lock:
            self.active_stream_count += 1
            if self.active_stream_count > self.active_stream_peak:
                self.active_stream_peak = self.active_stream_count

    def stream_close(self) -> None:
        with self._lock:
            self.active_stream_count = max(0, self.active_stream_count - 1)

    def snapshot(self) -> dict[str, object]:
        with self._lock:
            lat_values = list(self._latency_ms)
            lat_sorted = sorted(lat_values)
            p50 = round(_percentile(lat_sorted, 50.0), 2)
            p95 = round(_percentile(lat_sorted, 95.0), 2)
            p99 = round(_percentile(lat_sorted, 99.0), 2)
            uptime = max(1.0, time.time() - self._started_at)
            success_rate = (
                float(self.query_success) / float(self.query_total)
                if self.query_total > 0
                else 0.0
            )
            avg_retry_delay_ms = (
                (self.retry_delay_total_s / self.retry_delay_count) * 1000.0
                if self.retry_delay_count > 0
                else 0.0
            )
            return {
                "uptime_sec": int(uptime),
                "query_total": self.query_total,
                "query_success": self.query_success,
                "query_fail": self.query_fail,
                "success_rate": round(success_rate, 4),
                "latency_p50_ms": p50,
                "latency_p95_ms": p95,
                "latency_p99_ms": p99,
                "resolver_switch_count": self.resolver_switch_count,
                "retry_delay_count": self.retry_delay_count,
                "retry_delay_avg_ms": round(avg_retry_delay_ms, 2),
                "retry_count_distribution": dict(self.retry_count_distribution),
                "failure_type_histogram": dict(self.failure_type_histogram),
                "broadcast_success_count": self.broadcast_success_count,
                "broadcast_fail_count": self.broadcast_fail_count,
                "fallback_success_count": self.fallback_success_count,
                "fallback_fail_count": self.fallback_fail_count,
                "max_conns_reject_count": self.max_conns_reject_count,
                "max_conns_per_ip_reject_count": self.max_conns_per_ip_reject_count,
                "data_loss_incidents": self.data_loss_incidents,
                "active_stream_count": self.active_stream_count,
                "active_stream_peak": self.active_stream_peak,
                "success_count_per_resolver": dict(self.success_per_resolver),
                "fail_count_per_resolver": dict(self.fail_per_resolver),
            }


def chunk_label(s: str, size: int = 44) -> str:
    return ".".join(s[i : i + size] for i in range(0, len(s), size))


def _is_public_ipv4(ip: str) -> bool:
    try:
        addr = ipaddress.ip_address(ip.strip())
        return addr.version == 4 and addr.is_global
    except ValueError:
        return False


def _sanitize_resolvers(items: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw in items:
        ip = str(raw).strip()
        if not ip or ip in seen:
            continue
        if not _is_public_ipv4(ip):
            continue
        seen.add(ip)
        out.append(ip)
    return out


def _extract_resolvers_from_scan_json(
    data: dict,
    min_pass_rate: float,
    max_latency_ms: float,
    min_score: float,
    max_consecutive_failures: int,
    max_stale_sec: float,
    allowed_pools: set[str] | None,
) -> list[str]:
    """Prefer quality-filtered resolver rows from scanner JSON.

    Falls back to resolver_list only when quality rows are not present.
    """
    rows = data.get("resolvers", [])
    filtered: list[str] = []
    seen: set[str] = set()
    now_ts = time.time()
    if isinstance(rows, list):
        for row in rows:
            if not isinstance(row, dict):
                continue
            ip = str(row.get("resolver", "")).strip()
            if not _is_public_ipv4(ip) or ip in seen:
                continue

            # Scanner compatibility flags: enforce only resolvers that proved
            # full tunnel behavior to this zone.
            if not bool(row.get("random_subdomain", False)):
                continue
            if not bool(row.get("tunnel_realistic", False)):
                continue
            if not bool(row.get("nxdomain_correct", False)):
                continue
            if not bool(row.get("bidirectional", False)):
                continue
            # New scanner signal: full sessioned DATA roundtrip.
            # Keep backward compatibility with old reports that don't include it.
            proto_roundtrip = row.get("protocol_roundtrip")
            if proto_roundtrip is False:
                continue

            try:
                pass_rate = float(row.get("runtime_pass_rate"))
            except (TypeError, ValueError):
                pass_rate = None
            try:
                latency = float(row.get("latency_ms"))
            except (TypeError, ValueError):
                latency = None
            try:
                score = float(row.get("score"))
            except (TypeError, ValueError):
                score = None
            try:
                consec_fail = int(row.get("runtime_consecutive_failures"))
            except (TypeError, ValueError):
                consec_fail = None
            try:
                last_probe_ts = float(row.get("runtime_last_probe_ts"))
            except (TypeError, ValueError):
                last_probe_ts = None
            pool = str(row.get("pool", "")).strip().lower()

            if pass_rate is not None and pass_rate < min_pass_rate:
                continue
            if latency is not None and max_latency_ms > 0 and latency > max_latency_ms:
                continue
            if score is not None and score < min_score:
                continue
            if consec_fail is not None and consec_fail > max_consecutive_failures:
                continue
            if (
                max_stale_sec > 0
                and last_probe_ts is not None
                and now_ts - last_probe_ts > max_stale_sec
            ):
                continue
            if allowed_pools is not None and (not pool or pool not in allowed_pools):
                continue

            seen.add(ip)
            filtered.append(ip)

    # If scanner rows exist, trust strict filtering result (even empty).
    if isinstance(rows, list) and rows:
        return filtered
    # Compatibility fallback for very old scanner JSON schema.
    return _sanitize_resolvers(data.get("resolver_list", []))


def _ensure_min_resolver_count(
    primary: list[str],
    fallback: list[str],
    min_count: int = 2,
) -> list[str]:
    """Ensure resolver set does not collapse below a safe floor.

    When file-driven resolvers become too small, supplement from CLI seed
    resolvers to keep query path alive.
    """
    min_count = max(1, int(min_count))
    out = _sanitize_resolvers(list(primary))
    if len(out) >= min_count:
        return out
    for ip in _sanitize_resolvers(list(fallback)):
        if ip in out:
            continue
        out.append(ip)
        if len(out) >= min_count:
            break
    return out


class ResolverSelector:
    def __init__(
        self,
        servers: list[str],
        fail_cooldown: float = 5.0,
        max_inflight_per_resolver: int = 2,
        ewma_alpha: float = 0.2,
        fail_streak_before_blacklist: int = 3,
    ) -> None:
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
        self._timeout_streak: dict[str, int] = {s: 0 for s in uniq}
        self._max_inflight_per_resolver = max(1, max_inflight_per_resolver)
        self._ewma_alpha = min(0.8, max(0.05, ewma_alpha))
        self._fail_streak_before_blacklist = max(1, int(fail_streak_before_blacklist))
        self._inflight: dict[str, int] = {s: 0 for s in uniq}
        self._success_ewma: dict[str, float] = {s: 0.5 for s in uniq}
        self._latency_ewma_ms: dict[str, float] = {s: 700.0 for s in uniq}
        self._soft_fail_streak: dict[str, int] = {s: 0 for s in uniq}

    def _set_active_locked(self, server: str) -> None:
        if server == self._active:
            return
        self._active = server
        try:
            _runtime_kpi.record_resolver_switch()
        except Exception:
            pass

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
                self._set_active_locked(preferred)
            return self._active

    def choose_next(self, exclude: set | None = None) -> str | None:
        """Sticky active resolver with fallback only when active is unusable."""
        now = time.time()
        with self._lock:
            excluded = exclude or set()
            active = self._active
            active_bad = now < self._bad_until.get(active, 0.0)
            active_at_cap = (
                self._inflight.get(active, 0) >= self._max_inflight_per_resolver
            )

            # Keep using the current resolver while it is healthy.
            if (not active_bad) and (active not in excluded) and (not active_at_cap):
                self._inflight[active] = self._inflight.get(active, 0) + 1
                return active

            pool = [
                s
                for s in self._servers
                if s != active
                and now >= self._bad_until.get(s, 0.0)
                and self._inflight.get(s, 0) < self._max_inflight_per_resolver
                and s not in excluded
            ]
            if not pool:
                pool = [
                    s
                    for s in self._servers
                    if s != active
                    and now >= self._bad_until.get(s, 0.0)
                    and s not in excluded
                ]
            if not pool:
                # Degraded fallback: when all resolvers are temporarily
                # blacklisted, keep traffic alive by picking the least-bad
                # candidate (while still respecting per-query hard exclusions).
                pool = [
                    s
                    for s in self._servers
                    if s != active
                    and self._inflight.get(s, 0) < self._max_inflight_per_resolver
                    and s not in excluded
                ]
            if not pool:
                pool = [s for s in self._servers if s != active and s not in excluded]
            if not pool:
                # Absolute degraded fallback: keep traffic alive even when every
                # candidate is capped/blacklisted for a short period.
                if (not active_bad) and (active not in excluded):
                    self._inflight[active] = self._inflight.get(active, 0) + 1
                    return active
                return None

            if len(pool) == 1:
                chosen = pool[0]
            else:
                # Prefer earliest blacklisting expiry, then lower failure count.
                chosen = min(
                    pool,
                    key=lambda s: (
                        self._bad_until.get(s, 0.0),
                        self._fails.get(s, 0),
                        self._order.get(s, 0),
                    ),
                )

            self._inflight[chosen] = self._inflight.get(chosen, 0) + 1
            # Promote fallback only when active resolver is actually bad.
            if active_bad:
                self._set_active_locked(chosen)
            return chosen

    def release(self, server: str) -> None:
        with self._lock:
            if server not in self._inflight:
                return
            cur = self._inflight.get(server, 0)
            self._inflight[server] = cur - 1 if cur > 0 else 0

    def report_success(self, server: str, latency_ms: float | None = None) -> None:
        with self._lock:
            self._fails[server] = 0
            self._bad_until[server] = 0.0
            self._timeout_streak[server] = 0
            self._soft_fail_streak[server] = 0
            prev_succ = self._success_ewma.get(server, 0.5)
            self._success_ewma[server] = (1.0 - self._ewma_alpha) * prev_succ + self._ewma_alpha
            if latency_ms is not None:
                prev_lat = self._latency_ewma_ms.get(server, 700.0)
                self._latency_ewma_ms[server] = (
                    (1.0 - self._ewma_alpha) * prev_lat + self._ewma_alpha * max(1.0, latency_ms)
                )

    def report_failure(
        self,
        server: str,
        is_timeout: bool = False,
        latency_ms: float | None = None,
    ) -> None:
        now = time.time()
        with self._lock:
            f = self._fails.get(server, 0) + 1
            self._fails[server] = f
            prev_succ = self._success_ewma.get(server, 0.5)
            self._success_ewma[server] = (1.0 - self._ewma_alpha) * prev_succ
            if latency_ms is not None:
                prev_lat = self._latency_ewma_ms.get(server, 700.0)
                self._latency_ewma_ms[server] = (
                    (1.0 - self._ewma_alpha) * prev_lat + self._ewma_alpha * max(1.0, latency_ms)
                )
            streak = self._soft_fail_streak.get(server, 0) + 1
            self._soft_fail_streak[server] = streak
            if is_timeout:
                timeout_streak = self._timeout_streak.get(server, 0) + 1
                self._timeout_streak[server] = timeout_streak
            else:
                self._timeout_streak[server] = 0
                timeout_streak = 0

            should_blacklist = streak >= self._fail_streak_before_blacklist
            if is_timeout and timeout_streak >= 3:
                should_blacklist = True

            if should_blacklist:
                cooldown = self._fail_cooldown * min(f, 3)
                if is_timeout and timeout_streak >= 3:
                    cooldown = max(cooldown, 60.0)
                self._bad_until[server] = max(self._bad_until.get(server, 0.0), now + cooldown)
                self._soft_fail_streak[server] = 0
                if server == self._active:
                    self._set_active_locked(self._preferred_server_locked(now))

    def report_nxdomain(self, server: str) -> None:
        """NXDOMAIN means resolver cannot resolve our zone. Long blacklist."""
        now = time.time()
        with self._lock:
            was_new = self._fails.get(server, 0) < 100
            self._fails[server] = 100
            self._bad_until[server] = now + 300.0
            self._soft_fail_streak[server] = 0
            prev_succ = self._success_ewma.get(server, 0.5)
            self._success_ewma[server] = (1.0 - self._ewma_alpha) * prev_succ
            if server == self._active:
                self._set_active_locked(self._preferred_server_locked(now))
        if was_new:
            log.info("resolver NXDOMAIN blacklist %s for 300s", server)

    def report_no_answer(self, server: str) -> None:
        """Server returns empty answer — likely can't resolve our zone."""
        now = time.time()
        with self._lock:
            was_new = self._fails.get(server, 0) < 50
            self._fails[server] = 50
            self._bad_until[server] = now + 120.0
            self._soft_fail_streak[server] = 0
            prev_succ = self._success_ewma.get(server, 0.5)
            self._success_ewma[server] = (1.0 - self._ewma_alpha) * prev_succ
            if server == self._active:
                self._set_active_locked(self._preferred_server_locked(now))
        if was_new:
            log.info("resolver no-answer blacklist %s for 120s", server)

    def report_blocked(self, server: str, reason: str = "SERVFAIL") -> None:
        """Resolver explicitly blocks/fails our zone (SERVFAIL/REFUSED)."""
        now = time.time()
        with self._lock:
            was_new = self._fails.get(server, 0) < 80
            self._fails[server] = 80
            self._bad_until[server] = now + 180.0
            self._timeout_streak[server] = 0
            self._soft_fail_streak[server] = 0
            prev_succ = self._success_ewma.get(server, 0.5)
            self._success_ewma[server] = (1.0 - self._ewma_alpha) * prev_succ
            if server == self._active:
                self._set_active_locked(self._preferred_server_locked(now))
        if was_new:
            log.info("resolver %s blacklist %s for 180s", reason, server)

    def rotate_active(self) -> str:
        now = time.time()
        with self._lock:
            self._set_active_locked(self._preferred_server_locked(now))
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
                    should_check = now - self._last_switch >= switch_sec
                    if should_check:
                        self._last_switch = now
                    preferred = self._preferred_server_locked(now)
                    active = self._active
                    active_bad = now < self._bad_until.get(self._active, 0.0)
                    active_fails = self._fails.get(active, 0)
                    pref_fails = self._fails.get(preferred, 0)
                    # Proactive rotate: if active is not blacklisted but clearly
                    # worse than preferred, switch on periodic check.
                    proactive_better = (
                        preferred != active
                        and now >= self._bad_until.get(preferred, 0.0)
                        and pref_fails + 2 < active_fails
                    )
                    do_switch = should_check and (active_bad or proactive_better)
                if do_switch:
                    nxt = self.rotate_active()
                    log.info("resolver failover -> %s", nxt)
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
                    self._timeout_streak[s] = 0
                    self._soft_fail_streak[s] = 0
                    self._inflight[s] = 0
                    self._success_ewma[s] = 0.5
                    self._latency_ewma_ms[s] = 700.0
            if self._active not in uniq:
                self._set_active_locked(uniq[0])
            # Trim stale stats for removed resolvers.
            keep = set(uniq)
            self._fails = {k: v for k, v in self._fails.items() if k in keep}
            self._bad_until = {k: v for k, v in self._bad_until.items() if k in keep}
            self._timeout_streak = {k: v for k, v in self._timeout_streak.items() if k in keep}
            self._soft_fail_streak = {k: v for k, v in self._soft_fail_streak.items() if k in keep}
            self._inflight = {k: v for k, v in self._inflight.items() if k in keep}
            self._success_ewma = {k: v for k, v in self._success_ewma.items() if k in keep}
            self._latency_ewma_ms = {
                k: v for k, v in self._latency_ewma_ms.items() if k in keep
            }
            self._order = {k: v for k, v in self._order.items() if k in keep}
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
_resolver_last_chance_fallback: bool = True
_resolver_parallel_fallback: bool = True
_resolver_broadcast_enabled: bool = False
_resolver_broadcast_fanout: int = 10
_resolver_broadcast_timeout: float = 0.0
_resolver_broadcast_per_resolver_timeout: float = 0.0
_kpi_summary_interval_sec: float = 60.0
_kpi_summary_started: bool = False
_runtime_kpi = _RuntimeKpi()


def _failure_type_from_error(err_str: str) -> str:
    s = str(err_str)
    if "NXDOMAIN" in s:
        return "NXDOMAIN"
    if "SERVFAIL" in s:
        return "SERVFAIL"
    if "REFUSED" in s:
        return "REFUSED"
    if "no answer" in s:
        return "NOANSWER"
    if "timed out" in s or "timeout" in s:
        return "TIMEOUT"
    return "OTHER"


def _start_kpi_summary_loop(interval_sec: float) -> None:
    global _kpi_summary_started
    if interval_sec <= 0:
        return
    if _kpi_summary_started:
        return
    _kpi_summary_started = True

    def _loop() -> None:
        while True:
            time.sleep(max(5.0, interval_sec))
            snap = _runtime_kpi.snapshot()
            log.info(
                (
                    "kpi_summary uptime=%ss q_total=%d q_ok=%d q_fail=%d "
                    "success_rate=%.4f p50=%.2fms p95=%.2fms p99=%.2fms "
                    "switches=%d bcast_ok=%d bcast_fail=%d "
                    "fallback_ok=%d fallback_fail=%d active_streams=%d peak_streams=%d "
                    "reject_max_conns=%d reject_max_conns_per_ip=%d data_loss=%d"
                ),
                snap["uptime_sec"],
                snap["query_total"],
                snap["query_success"],
                snap["query_fail"],
                snap["success_rate"],
                snap["latency_p50_ms"],
                snap["latency_p95_ms"],
                snap["latency_p99_ms"],
                snap["resolver_switch_count"],
                snap["broadcast_success_count"],
                snap["broadcast_fail_count"],
                snap["fallback_success_count"],
                snap["fallback_fail_count"],
                snap["active_stream_count"],
                snap["active_stream_peak"],
                snap["max_conns_reject_count"],
                snap["max_conns_per_ip_reject_count"],
                snap["data_loss_incidents"],
            )

    t = threading.Thread(target=_loop, daemon=True)
    t.start()


def _payload_with_retry_count(payload: bytes, retry_count: int) -> bytes:
    """Inject protocol retry metadata for retry-aware message types.

    If payload is not a valid Nexora packet, it is returned as-is.
    """
    try:
        pkt = unpack_packet(payload)
    except Exception:
        return payload
    # Keep message identity stable; only retry counter changes per wire attempt.
    return pack_packet(
        pkt.msg_type,
        pkt.session_id,
        pkt.nonce,
        pkt.payload,
        retry_count=max(0, min(255, int(retry_count))),
    )


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
    call_started_at = time.time()
    last_err = None
    last_server = ""
    dead_servers: set[str] = set()  # hard-dead for this call (NXDOMAIN/blocked)
    attempts_made = 0
    total_attempts = max(1, attempts)
    broadcast_fanout = max(2, int(_resolver_broadcast_fanout))

    def _record_query_result(success: bool, resolver: str = "") -> None:
        _runtime_kpi.record_query(
            success=success,
            latency_ms=(time.time() - call_started_at) * 1000.0,
            attempts_used=max(1, attempts_made),
            resolver=resolver if success else "",
            failure_type="",
        )

    def _record_failure(server: str, err: Exception, started_at: float) -> str:
        nonlocal last_err
        last_err = err
        err_str = str(err)
        is_timeout = "timed out" in err_str
        _runtime_kpi.record_failure(server, _failure_type_from_error(err_str))
        if "NXDOMAIN" in err_str:
            selector.report_nxdomain(server)
            dead_servers.add(server)
        elif "no answer" in err_str:
            selector.report_no_answer(server)
            dead_servers.add(server)
        elif "SERVFAIL" in err_str:
            selector.report_blocked(server, reason="SERVFAIL")
            dead_servers.add(server)
        elif "REFUSED" in err_str:
            selector.report_blocked(server, reason="REFUSED")
            dead_servers.add(server)
        else:
            selector.report_failure(
                server,
                is_timeout=is_timeout,
                latency_ms=(time.time() - started_at) * 1000.0,
            )
        return err_str

    def _sleep_retry_delay(server: str, idx: int) -> None:
        # Exponential backoff with jitter prevents retry storms.
        backoff = min(0.6, 0.03 * (2 ** min(4, idx)))
        jitter = backoff * 0.25 * random.random()
        wait_s = backoff + jitter
        log.info(
            "retry_delay server=%s attempt=%d total_attempts=%d backoff=%.3f jitter=%.3f wait=%.3f",
            server,
            idx + 1,
            total_attempts,
            backoff,
            jitter,
            wait_s,
        )
        _runtime_kpi.record_retry_delay(wait_s)
        time.sleep(wait_s)

    for idx in range(total_attempts):
        server = selector.choose_next(exclude=dead_servers)
        if server is None:
            # No usable resolver left for this query - bail early.
            break
        last_server = server
        # Multipath broadcast mode: send to multiple resolvers and accept first
        # successful response. Keeps nonce/session identity stable per payload.
        if _resolver_broadcast_enabled and broadcast_fanout > 1:
            chosen = [server]
            while len(chosen) < broadcast_fanout:
                nxt = selector.choose_next(exclude=dead_servers | set(chosen))
                if nxt is None:
                    break
                chosen.append(nxt)
            if len(chosen) > 1:
                sent_at: dict[str, float] = {}
                futures = {}
                per_timeout = timeout
                if _resolver_broadcast_per_resolver_timeout > 0:
                    if timeout > 0:
                        per_timeout = min(timeout, _resolver_broadcast_per_resolver_timeout)
                    else:
                        per_timeout = _resolver_broadcast_per_resolver_timeout
                budget = _resolver_broadcast_timeout if _resolver_broadcast_timeout > 0 else timeout
                budget = max(0.2, budget if budget > 0 else per_timeout)
                ex = ThreadPoolExecutor(max_workers=len(chosen))
                try:
                    for srv in chosen:
                        sent_at[srv] = time.time()
                        attempts_made += 1
                        wire_payload = _payload_with_retry_count(payload, attempts_made)
                        fut = ex.submit(
                            _query_pkt_direct,
                            srv,
                            port,
                            zone,
                            per_timeout,
                            wire_payload,
                            qtype,
                        )
                        futures[fut] = srv

                    winner: str | None = None
                    result: tuple[int, object] | None = None
                    try:
                        for fut in as_completed(futures, timeout=budget):
                            f_server = futures[fut]
                            try:
                                qid, pkt = fut.result()
                                selector.report_success(
                                    f_server,
                                    latency_ms=(time.time() - sent_at[f_server]) * 1000.0,
                                )
                                winner = f_server
                                result = (qid, pkt)
                                break
                            except Exception as e:
                                _record_failure(f_server, e, sent_at[f_server])
                    except Exception as e:
                        last_err = e

                    if result is not None:
                        for fut in futures:
                            if not fut.done():
                                fut.cancel()
                        log.info(
                            "query broadcast succeeded fanout=%d winner=%s",
                            len(chosen),
                            winner,
                        )
                        _runtime_kpi.record_broadcast(True)
                        _record_query_result(success=True, resolver=winner or server)
                        return result

                    now_t = time.time()
                    for fut, f_server in futures.items():
                        if fut.done():
                            continue
                        fut.cancel()
                        _record_failure(f_server, TimeoutError("timed out"), sent_at.get(f_server, now_t))
                    log.warning(
                        "query broadcast failed fanout=%d resolvers=%s",
                        len(chosen),
                        ",".join(chosen),
                    )
                    _runtime_kpi.record_broadcast(False)
                    # Skip delay for hard-dead outcomes (same as serial path).
                    if idx < total_attempts - 1 and (
                        last_err is None
                        or (
                            "NXDOMAIN" not in str(last_err)
                            and "no answer" not in str(last_err)
                            and "SERVFAIL" not in str(last_err)
                            and "REFUSED" not in str(last_err)
                        )
                    ):
                        _sleep_retry_delay(server, idx)
                    continue
                finally:
                    ex.shutdown(wait=False, cancel_futures=True)
                    for s in chosen:
                        selector.release(s)
        # Parallel final-attempt fallback: query primary + backup concurrently
        # and return the first successful response.
        if (
            _resolver_parallel_fallback
            and _resolver_last_chance_fallback
            and idx == total_attempts - 1
        ):
            backup = selector.choose_next(exclude=dead_servers | {server})
            if backup is not None:
                sent_at: dict[str, float] = {}
                futures = {}
                ex = ThreadPoolExecutor(max_workers=2)
                try:
                    sent_at[server] = time.time()
                    primary_payload = _payload_with_retry_count(payload, attempts_made + 1)
                    attempts_made += 1
                    futures[ex.submit(
                        _query_pkt_direct,
                        server,
                        port,
                        zone,
                        timeout,
                        primary_payload,
                        qtype,
                    )] = server

                    sent_at[backup] = time.time()
                    backup_payload = _payload_with_retry_count(payload, attempts_made + 1)
                    attempts_made += 1
                    futures[ex.submit(
                        _query_pkt_direct,
                        backup,
                        port,
                        zone,
                        timeout,
                        backup_payload,
                        qtype,
                    )] = backup

                    success = None
                    for fut in as_completed(futures):
                        f_server = futures[fut]
                        try:
                            qid, pkt = fut.result()
                            selector.report_success(
                                f_server,
                                latency_ms=(time.time() - sent_at[f_server]) * 1000.0,
                            )
                            success = (qid, pkt, f_server)
                            break
                        except Exception as e:
                            _record_failure(f_server, e, sent_at[f_server])

                    if success is not None:
                        for fut in futures:
                            if not fut.done():
                                fut.cancel()
                        ex.shutdown(wait=False, cancel_futures=True)
                        qid, pkt, winner = success
                        loser = backup if winner == server else server
                        log.warning(
                            "query parallel fallback succeeded winner=%s loser=%s",
                            winner,
                            loser,
                        )
                        _runtime_kpi.record_fallback(True)
                        _record_query_result(success=True, resolver=winner)
                        selector.release(server)
                        selector.release(backup)
                        return qid, pkt
                finally:
                    ex.shutdown(wait=False, cancel_futures=True)

                log.warning(
                    "query parallel fallback failed primary=%s backup=%s",
                    server,
                    backup,
                )
                _runtime_kpi.record_fallback(False)
                selector.release(server)
                selector.release(backup)
                continue

        attempts_made += 1
        t0 = time.time()
        try:
            wire_payload = _payload_with_retry_count(payload, attempts_made)
            qid, pkt = _query_pkt_direct(server, port, zone, timeout, wire_payload, qtype)
            selector.report_success(server, latency_ms=(time.time() - t0) * 1000.0)
            _record_query_result(success=True, resolver=server)
            return qid, pkt
        except Exception as e:
            err_str = _record_failure(server, e, t0)
            log.warning("query attempt %d/%d failed server=%s: %s", idx + 1, total_attempts, server, e)

            # Last-chance fallback: when final attempt on primary fails, try one
            # alternate resolver from pool before declaring query failure.
            if _resolver_last_chance_fallback and idx == total_attempts - 1:
                backup = selector.choose_next(exclude=dead_servers | {server})
                if backup is not None:
                    attempts_made += 1
                    t1 = time.time()
                    try:
                        backup_payload = _payload_with_retry_count(payload, attempts_made)
                        qid, pkt = _query_pkt_direct(
                            backup, port, zone, timeout, backup_payload, qtype
                        )
                        selector.report_success(
                            backup, latency_ms=(time.time() - t1) * 1000.0
                        )
                        log.warning(
                            "query last-chance fallback succeeded primary=%s backup=%s",
                            server,
                            backup,
                        )
                        _runtime_kpi.record_fallback(True)
                        _record_query_result(success=True, resolver=backup)
                        return qid, pkt
                    except Exception as e2:
                        _record_failure(backup, e2, t1)
                        log.warning(
                            "query last-chance fallback failed primary=%s backup=%s: %s",
                            server,
                            backup,
                            e2,
                        )
                        _runtime_kpi.record_fallback(False)
                    finally:
                        selector.release(backup)

            # Skip delay for hard-dead resolvers (try next immediately).
            if (
                "NXDOMAIN" not in err_str
                and "no answer" not in err_str
                and "SERVFAIL" not in err_str
                and "REFUSED" not in err_str
                and idx < total_attempts - 1
            ):
                _sleep_retry_delay(server, idx)
        finally:
            selector.release(server)
    used = attempts_made
    _runtime_kpi.record_query(
        success=False,
        latency_ms=(time.time() - call_started_at) * 1000.0,
        attempts_used=max(1, used),
        resolver="",
        failure_type="",
    )
    raise TimeoutError(
        (
            f"dns query failed after {used} wire-attempts "
            f"across {total_attempts} rounds "
            f"(last resolver {last_server}): {last_err}"
        )
    )


def run_client(selector: ResolverSelector,
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
    zombie_grace_sec: float,
    pipeline_depth: int = 1,
) -> None:
    sid = None
    try:
        local_conn.settimeout(0.05)
        # Enable TCP keepalive to detect dead peers quickly (unclean disconnect)
        local_conn.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        try:
            # Linux: start keepalive after 5s idle, probe every 3s, fail after 3 probes (~14s)
            local_conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 5)
            local_conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 3)
            local_conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)
        except (AttributeError, OSError):
            pass  # not available on all platforms
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
        local_close_ts: float | None = None
        idle_rounds = 0
        last_activity = time.time()
        poll_wait = max(0.02, poll_min_interval)
        poll_ceiling = max(poll_wait, poll_max_interval)
        eager_pulls = 0
        next_pull_at = time.time()
        ever_received_data = False
        data_in_flight = False  # ensure only 1 data query in flight (ordering)
        session_start = time.time()

        pool = ThreadPoolExecutor(max_workers=max(1, pipeline_depth))
        pending = {}  # {future: (nonce, up_len)}
        try:
            while True:
                now = time.time()
                if idle_timeout > 0 and now - last_activity >= idle_timeout:
                    break
                # Fast teardown after peer disconnect: avoid holding connection
                # slots for stale DNS in-flight work and improve reconnect latency.
                if local_closed and local_close_ts is not None and now - local_close_ts >= 1.5:
                    break
                # Kill zombie sessions if no downstream data is observed for
                # a configurable grace period after session start.
                if (
                    zombie_grace_sec > 0
                    and not ever_received_data
                    and now - session_start >= zombie_grace_sec
                ):
                    log.info(
                        "forward zombie sid=%d: no downstream after %.1fs (grace=%.1fs)",
                        sid,
                        now - session_start,
                        zombie_grace_sec,
                    )
                    break

                # --- Fill pipeline with new queries ---
                while len(pending) < pipeline_depth:
                    outbound = b""
                    if not local_closed and not data_in_flight:
                        try:
                            outbound = local_conn.recv(chunk_size)
                            if outbound == b"":
                                local_closed = True
                                local_close_ts = time.time()
                                log.info("forward local closed sid=%d (clean FIN)", sid)
                            elif outbound:
                                eager_pulls = max(eager_pulls, 4)
                        except socket.timeout:
                            outbound = b""
                        except (ConnectionResetError, BrokenPipeError, OSError):
                            local_closed = True
                            local_close_ts = time.time()
                            outbound = b""
                            log.info("forward local closed sid=%d (connection reset)", sid)

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
                    if outbound:
                        data_in_flight = True
                        # Don't advance next_pull_at here: let companion poll
                        # fire in the next fill-loop iteration so pipeline_depth
                        # slots are actually used for downstream retrieval.
                    else:
                        next_pull_at = now2 + poll_wait
                        break  # at most one empty poll per fill cycle

                # --- Wait for results ---
                if not pending:
                    time.sleep(min(0.05, max(0.0, next_pull_at - time.time())))
                    continue

                done_set, _ = _futures_wait(
                    pending.keys(),
                    return_when=FIRST_COMPLETED,
                    timeout=min(
                        timeout * max(1, attempts) + 3,
                        max(0.8, timeout + 0.6) if local_closed else (timeout * max(1, attempts) + 3),
                    ),
                )
                if not done_set:
                    raise TimeoutError("all DNS queries timed out")

                for f in done_set:
                    exp_nonce, up_len = pending.pop(f)
                    if up_len > 0:
                        data_in_flight = False
                    try:
                        _, resp = f.result()
                    except TimeoutError:
                        # One sub-query may timeout while others complete.
                        # Keep processing remaining futures in this cycle.
                        log.debug(
                            "stream subquery timeout sid=%d nonce=%d up=%d",
                            sid,
                            exp_nonce,
                            up_len,
                        )
                        continue
                    log.info(
                        "stream xfer sid=%d up=%d resp_type=%d resp_pay=%d",
                        sid, up_len, resp.msg_type, len(resp.payload),
                    )
                    if resp.msg_type == TYPE_STREAM_CLOSE:
                        log.info("server signalled stream close sid=%d", sid)
                        local_closed = True
                        local_close_ts = time.time()
                        idle_rounds = 1  # force immediate exit
                        try:
                            local_conn.shutdown(socket.SHUT_RDWR)
                        except OSError:
                            pass
                        break  # break from for-loop over done_set
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

                    dropped = 0
                    while len(seq_map) > SEQ_MAP_MAX_SIZE:
                        seq_map.popitem(last=False)
                        dropped += 1
                    if dropped > 0:
                        _runtime_kpi.record_data_loss(dropped)

                    while next_seq in seq_map:
                        cdata = seq_map.pop(next_seq)
                        try:
                            local_conn.sendall(cdata)
                        except (BrokenPipeError, ConnectionResetError, OSError):
                            local_closed = True
                            local_close_ts = time.time()
                            break
                        next_seq += 1

                    if got_new or up_len > 0:
                        idle_rounds = 0
                        last_activity = time.time()
                        poll_wait = max(0.02, poll_min_interval)
                        next_pull_at = time.time() + poll_wait
                        if got_new:
                            ever_received_data = True
                    else:
                        idle_rounds += 1
                        if eager_pulls > 0:
                            eager_pulls -= 1
                            poll_wait = max(0.02, poll_min_interval)
                        else:
                            poll_wait = min(poll_ceiling, max(0.02, poll_wait * 1.8))
                        next_pull_at = time.time() + poll_wait

                if local_closed and idle_rounds >= 1:
                    break
        finally:
            for f in pending:
                f.cancel()
            pool.shutdown(wait=False)
            if local_closed:
                log.info("forward done %s sid=%s (local closed, idle_rounds=%d)", client_addr, sid, idle_rounds)
            else:
                log.info("forward done %s sid=%s (idle timeout)", client_addr, sid)

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
            # Keep close best-effort and short to avoid reconnect delays when
            # resolvers are unstable; server-side session TTL does final cleanup.
            close_timeout = max(0.5, min(1.0, timeout * 0.5))
            _stream_close(sid, selector, port, zone, close_timeout, 1, qtype)
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
    zombie_grace_sec: float,
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
    per_ip_limit = int(max_conns_per_ip)
    while True:
        conn, addr = lsock.accept()
        peer_ip = addr[0]

        if not sem.acquire(timeout=2.0):
            log.warning("forward reject local=%s:%d reason=max_conns", peer_ip, addr[1])
            _runtime_kpi.record_reject("max_conns")
            try:
                conn.close()
            except Exception:
                pass
            continue

        # Enforce optional per-IP cap before assigning worker.
        reject_per_ip = False
        active_for_ip = 0
        with ip_lock:
            active_for_ip = ip_counts.get(peer_ip, 0)
            if per_ip_limit > 0 and active_for_ip >= per_ip_limit:
                reject_per_ip = True
            else:
                ip_counts[peer_ip] = active_for_ip + 1
                active_for_ip = active_for_ip + 1
        if reject_per_ip:
            sem.release()
            log.warning(
                "forward reject local=%s:%d reason=max_conns_per_ip active=%d limit=%d",
                peer_ip,
                addr[1],
                active_for_ip,
                per_ip_limit,
            )
            _runtime_kpi.record_reject("max_conns_per_ip")
            try:
                conn.close()
            except Exception:
                pass
            continue

        def _worker(
            local_conn: socket.socket = conn,
            local_addr: tuple[str, int] = addr,
            local_peer_ip: str = peer_ip,
        ) -> None:
            _runtime_kpi.stream_open()
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
                    zombie_grace_sec,
                    pipeline_depth,
                )
            finally:
                _runtime_kpi.stream_close()
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
    p.add_argument("--tcp-chunk-size", type=int, default=100)
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
        default=15.0,
        help="close forward stream after this many idle seconds; 0 disables",
    )
    p.add_argument(
        "--forward-zombie-grace-sec",
        type=float,
        default=8.0,
        help="close forward stream if no downstream bytes arrive for this many seconds; 0 disables",
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
    p.add_argument(
        "--resolver-fail-streak",
        type=int,
        default=3,
        help="consecutive soft failures before resolver is temporarily blacklisted",
    )
    p.add_argument(
        "--resolver-max-inflight",
        type=int,
        default=2,
        help="max concurrent DNS queries per resolver candidate",
    )
    p.add_argument(
        "--resolver-attempt-cap",
        type=int,
        default=6,
        help="upper cap for auto attempts derived from resolver count",
    )
    p.add_argument("--resolver-health-interval", type=float, default=90.0)
    p.add_argument("--resolver-switch-interval", type=float, default=180.0)
    p.add_argument(
        "--resolver-last-chance-fallback",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="on final failed attempt, try one alternate resolver before failing",
    )
    p.add_argument(
        "--resolver-parallel-fallback",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="run primary+backup in parallel on final attempt and accept first success",
    )
    p.add_argument(
        "--resolver-broadcast",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="send each query to multiple resolvers and accept first success",
    )
    p.add_argument(
        "--resolver-broadcast-fanout",
        type=int,
        default=10,
        help="number of parallel resolvers per query when broadcast mode is enabled",
    )
    p.add_argument(
        "--resolver-broadcast-timeout",
        type=float,
        default=0.0,
        help="overall timeout budget for broadcast query (<=0 uses --timeout)",
    )
    p.add_argument(
        "--resolver-broadcast-per-resolver-timeout",
        type=float,
        default=0.0,
        help="per-resolver timeout in broadcast mode (<=0 uses --timeout)",
    )
    p.add_argument(
        "--kpi-summary-interval",
        type=float,
        default=60.0,
        help="seconds between KPI summary logs (<=0 disables)",
    )
    p.add_argument("--resolver-probe-timeout", type=float, default=1.6)
    p.add_argument("--resolver-probe-qtype", choices=["TXT", "A"], default="TXT")
    p.add_argument(
        "--resolver-file-min-pass-rate",
        type=float,
        default=0.55,
        help="minimum runtime_pass_rate to accept resolver rows from resolver file",
    )
    p.add_argument(
        "--resolver-file-max-latency-ms",
        type=float,
        default=750.0,
        help="maximum latency_ms to accept resolver rows from resolver file (<=0 disables)",
    )
    p.add_argument(
        "--resolver-file-min-score",
        type=float,
        default=4.0,
        help="minimum score to accept resolver rows from resolver file",
    )
    p.add_argument(
        "--resolver-file-max-consecutive-failures",
        type=int,
        default=1,
        help="maximum runtime_consecutive_failures accepted from resolver rows",
    )
    p.add_argument(
        "--resolver-file-max-stale-sec",
        type=float,
        default=240.0,
        help="maximum allowed age of runtime_last_probe_ts in seconds (<=0 disables)",
    )
    p.add_argument(
        "--resolver-file-pools",
        default="active,standby,fallback",
        help="comma-separated allowed scanner pools (e.g. active,standby)",
    )
    args = p.parse_args()
    qtype = TYPE_A if args.qtype == "A" else TYPE_TXT
    seed_resolvers = _sanitize_resolvers([x.strip() for x in args.server.split(",") if x.strip()])
    resolver_list = list(seed_resolvers)
    allowed_pools = {
        x.strip().lower()
        for x in str(args.resolver_file_pools).split(",")
        if x.strip()
    }
    if not allowed_pools:
        allowed_pools = {"active", "standby"}

    # Load resolvers from file (if present), with safety filtering and seed fallback.
    if args.resolver_file and os.path.isfile(args.resolver_file):
        try:
            with open(args.resolver_file, "r") as f:
                data = json.load(f)
            file_resolvers = _extract_resolvers_from_scan_json(
                data,
                min_pass_rate=max(0.0, min(1.0, args.resolver_file_min_pass_rate)),
                max_latency_ms=args.resolver_file_max_latency_ms,
                min_score=max(0.0, args.resolver_file_min_score),
                max_consecutive_failures=max(0, int(args.resolver_file_max_consecutive_failures)),
                max_stale_sec=args.resolver_file_max_stale_sec,
                allowed_pools=allowed_pools,
            )
            effective = _ensure_min_resolver_count(file_resolvers, seed_resolvers, min_count=2)
            if effective:
                if len(file_resolvers) < len(effective):
                    log.warning(
                        "resolver file %s yielded %d resolvers; supplemented with CLI seeds to %d",
                        args.resolver_file,
                        len(file_resolvers),
                        len(effective),
                    )
                else:
                    log.info("loaded %d resolvers from %s", len(effective), args.resolver_file)
                resolver_list = effective
            else:
                log.warning(
                    "resolver file %s has no usable public resolvers; using CLI seeds",
                    args.resolver_file,
                )
        except Exception as e:
            log.warning("failed to read resolver file %s: %s, using CLI resolvers", args.resolver_file, e)

    if not resolver_list:
        raise RuntimeError("no usable public resolvers available (CLI + resolver file)")

    selector = ResolverSelector(
        resolver_list,
        fail_cooldown=args.resolver_fail_cooldown,
        max_inflight_per_resolver=args.resolver_max_inflight,
        fail_streak_before_blacklist=args.resolver_fail_streak,
    )
    log.info("active resolvers (%d): %s", len(resolver_list), ", ".join(resolver_list))

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
                    new_list = _extract_resolvers_from_scan_json(
                        data,
                        min_pass_rate=max(0.0, min(1.0, args.resolver_file_min_pass_rate)),
                        max_latency_ms=args.resolver_file_max_latency_ms,
                        min_score=max(0.0, args.resolver_file_min_score),
                        max_consecutive_failures=max(
                            0, int(args.resolver_file_max_consecutive_failures)
                        ),
                        max_stale_sec=args.resolver_file_max_stale_sec,
                        allowed_pools=allowed_pools,
                    )
                    effective = _ensure_min_resolver_count(new_list, seed_resolvers, min_count=2)
                    if effective:
                        if effective != sel.servers:
                            sel.update_servers(effective)
                    elif seed_resolvers and sel.servers != seed_resolvers:
                        log.warning(
                            "resolver file %s has no usable resolvers; falling back to CLI seeds",
                            path,
                        )
                        sel.update_servers(seed_resolvers)
                except Exception:
                    pass

        wt = threading.Thread(
            target=_watch_resolver_file,
            args=(args.resolver_file, selector),
            daemon=True,
        )
        wt.start()
        log.info("resolver file watcher started: %s", args.resolver_file)
    global _dns_pacer, _resolver_last_chance_fallback, _resolver_parallel_fallback
    global _resolver_broadcast_enabled, _resolver_broadcast_fanout
    global _resolver_broadcast_timeout, _resolver_broadcast_per_resolver_timeout
    global _kpi_summary_interval_sec
    _dns_pacer = _DnsQueryPacer(min_interval=args.dns_query_interval)
    _resolver_last_chance_fallback = bool(args.resolver_last_chance_fallback)
    _resolver_parallel_fallback = bool(args.resolver_parallel_fallback)
    _resolver_broadcast_enabled = bool(args.resolver_broadcast)
    _resolver_broadcast_fanout = max(2, int(args.resolver_broadcast_fanout))
    _resolver_broadcast_timeout = float(args.resolver_broadcast_timeout)
    _resolver_broadcast_per_resolver_timeout = float(
        args.resolver_broadcast_per_resolver_timeout
    )
    _kpi_summary_interval_sec = float(args.kpi_summary_interval)
    _start_kpi_summary_loop(_kpi_summary_interval_sec)
    if _resolver_broadcast_enabled:
        log.info(
            (
                "resolver broadcast enabled fanout=%d budget=%.2fs "
                "per_resolver_timeout=%.2fs"
            ),
            _resolver_broadcast_fanout,
            _resolver_broadcast_timeout,
            _resolver_broadcast_per_resolver_timeout,
        )
    # Auto pipeline depth: cap at 2 - keeps server load manageable and
    # avoids upstream data reordering (only 1 data query in flight).
    if args.pipeline_depth <= 0:
        args.pipeline_depth = max(1, min(2, len(resolver_list)))
        log.info("auto pipeline_depth=%d (from %d resolvers)", args.pipeline_depth, len(resolver_list))
    # Auto-scale attempts but keep a hard cap so one request won't block too long.
    # In strict broadcast-only mode, preserve user attempts exactly (usually 1).
    if _resolver_broadcast_enabled and (not _resolver_last_chance_fallback) and (not _resolver_parallel_fallback):
        log.info("broadcast-only mode: preserving attempts=%d", args.attempts)
    else:
        cap = max(1, int(args.resolver_attempt_cap))
        target_attempts = min(len(resolver_list), cap)
        if target_attempts > args.attempts:
            args.attempts = target_attempts
            log.info(
                "auto attempts=%d (from %d resolvers, cap=%d)",
                args.attempts,
                len(resolver_list),
                cap,
            )
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
            args.forward_zombie_grace_sec,
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
