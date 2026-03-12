"""
Unit tests for resolver selection stickiness/failover behavior.
"""

import os
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import nexora_client
from dns_wire import TYPE_TXT
from nexora_client import ResolverSelector, _query_txt
from nexora_proto import TYPE_DATA, pack_packet, unpack_packet


class TestResolverSelectorStickyPolicy(unittest.TestCase):
    def test_stays_on_active_when_healthy(self):
        selector = ResolverSelector(["1.1.1.1", "8.8.8.8"], max_inflight_per_resolver=1)
        picks = []
        for _ in range(3):
            picked = selector.choose_next()
            picks.append(picked)
            selector.release(picked)
        self.assertEqual(picks, ["1.1.1.1", "1.1.1.1", "1.1.1.1"])

    def test_active_resolver_respects_inflight_cap(self):
        selector = ResolverSelector(["1.1.1.1", "8.8.8.8"], max_inflight_per_resolver=1)
        first = selector.choose_next()
        second = selector.choose_next()
        self.assertEqual(first, "1.1.1.1")
        self.assertEqual(second, "8.8.8.8")
        selector.release(first)
        selector.release(second)

    def test_soft_failures_do_not_switch_immediately(self):
        selector = ResolverSelector(["1.1.1.1", "8.8.8.8"])
        selector.report_failure("1.1.1.1", is_timeout=True)
        selector.report_failure("1.1.1.1", is_timeout=True)

        self.assertEqual(selector.active, "1.1.1.1")
        picked = selector.choose_next()
        self.assertEqual(picked, "1.1.1.1")
        selector.release(picked)

    def test_switches_after_repeated_soft_failures(self):
        selector = ResolverSelector(["1.1.1.1", "8.8.8.8"], fail_streak_before_blacklist=3)
        selector.report_failure("1.1.1.1", is_timeout=True)
        selector.report_failure("1.1.1.1", is_timeout=True)
        selector.report_failure("1.1.1.1", is_timeout=True)

        self.assertEqual(selector.active, "8.8.8.8")
        picked = selector.choose_next()
        self.assertEqual(picked, "8.8.8.8")
        selector.release(picked)

    def test_hard_failure_switches_immediately(self):
        selector = ResolverSelector(["1.1.1.1", "8.8.8.8"])
        selector.report_nxdomain("1.1.1.1")

        self.assertEqual(selector.active, "8.8.8.8")
        picked = selector.choose_next()
        self.assertEqual(picked, "8.8.8.8")
        selector.release(picked)

    def test_excluding_active_for_one_query_does_not_switch_active(self):
        selector = ResolverSelector(["1.1.1.1", "8.8.8.8"])
        picked = selector.choose_next(exclude={"1.1.1.1"})
        self.assertEqual(picked, "8.8.8.8")
        selector.release(picked)
        self.assertEqual(selector.active, "1.1.1.1")

    def test_picks_degraded_fallback_when_all_alternates_blacklisted(self):
        selector = ResolverSelector(["1.1.1.1", "8.8.8.8"])
        # Make active unusable and blacklist the only alternate.
        selector.report_nxdomain("1.1.1.1")
        selector.report_nxdomain("8.8.8.8")
        picked = selector.choose_next()
        self.assertEqual(picked, "8.8.8.8")

    def test_update_servers_reorders_preference_and_rotates_degraded_active(self):
        selector = ResolverSelector(["1.1.1.1", "8.8.8.8", "9.9.9.9"])
        selector.report_failure("1.1.1.1", is_timeout=False)
        selector.update_servers(["9.9.9.9", "8.8.8.8", "1.1.1.1"])

        self.assertEqual(selector.active, "9.9.9.9")
        picked = selector.choose_next(exclude={"9.9.9.9"})
        self.assertEqual(picked, "8.8.8.8")
        selector.release(picked)


class TestQueryRetryStickiness(unittest.TestCase):
    def test_query_retries_same_resolver_before_failover(self):
        selector = ResolverSelector(["1.1.1.1", "8.8.8.8"], fail_streak_before_blacklist=3)
        seen_servers: list[str] = []
        attempts = {"n": 0}

        def _fake_query(server, port, zone, timeout, payload, qtype):
            seen_servers.append(server)
            attempts["n"] += 1
            if attempts["n"] < 3:
                raise TimeoutError("timed out")
            return 1234, object()

        with (
            patch("nexora_client._query_pkt_direct", side_effect=_fake_query),
            patch("nexora_client.time.sleep", return_value=None),
        ):
            qid, _ = _query_txt(
                selector=selector,
                port=53,
                zone="example.com",
                timeout=0.2,
                payload=b"x",
                attempts=4,
                qtype=TYPE_TXT,
            )

        self.assertEqual(qid, 1234)
        self.assertEqual(seen_servers, ["1.1.1.1", "1.1.1.1", "1.1.1.1"])

    def test_last_chance_fallback_uses_backup_on_final_failure(self):
        selector = ResolverSelector(["1.1.1.1", "8.8.8.8"], fail_streak_before_blacklist=10)
        seen_servers: list[str] = []

        def _fake_query(server, port, zone, timeout, payload, qtype):
            seen_servers.append(server)
            if server == "1.1.1.1":
                raise TimeoutError("timed out")
            return 4321, object()

        with (
            patch("nexora_client._query_pkt_direct", side_effect=_fake_query),
            patch.object(nexora_client, "_resolver_last_chance_fallback", True),
            patch("nexora_client.time.sleep", return_value=None),
        ):
            qid, _ = _query_txt(
                selector=selector,
                port=53,
                zone="example.com",
                timeout=0.2,
                payload=b"x",
                attempts=1,
                qtype=TYPE_TXT,
            )

        self.assertEqual(qid, 4321)
        self.assertEqual(set(seen_servers), {"1.1.1.1", "8.8.8.8"})
        self.assertEqual(len(seen_servers), 2)

    def test_retry_metadata_increments_across_retries(self):
        selector = ResolverSelector(["1.1.1.1"], fail_streak_before_blacklist=20)
        seen_retry: list[int] = []
        payload = pack_packet(TYPE_DATA, 7, 123, b"abc")

        def _fake_query(server, port, zone, timeout, wire_payload, qtype):
            pkt = unpack_packet(wire_payload)
            seen_retry.append(pkt.retry_count)
            if len(seen_retry) == 1:
                raise TimeoutError("timed out")
            return 999, object()

        with (
            patch("nexora_client._query_pkt_direct", side_effect=_fake_query),
            patch.object(nexora_client, "_resolver_last_chance_fallback", False),
            patch.object(nexora_client, "_resolver_parallel_fallback", False),
            patch("nexora_client.time.sleep", return_value=None),
        ):
            qid, _ = _query_txt(
                selector=selector,
                port=53,
                zone="example.com",
                timeout=0.2,
                payload=payload,
                attempts=2,
                qtype=TYPE_TXT,
            )

        self.assertEqual(qid, 999)
        self.assertEqual(seen_retry, [1, 2])

    def test_broadcast_mode_returns_first_success(self):
        selector = ResolverSelector(
            ["1.1.1.1", "8.8.8.8", "9.9.9.9"],
            fail_streak_before_blacklist=20,
        )
        seen_servers: list[str] = []

        def _fake_query(server, port, zone, timeout, payload, qtype):
            seen_servers.append(server)
            if server == "1.1.1.1":
                raise TimeoutError("timed out")
            return 700, object()

        with (
            patch("nexora_client._query_pkt_direct", side_effect=_fake_query),
            patch.object(nexora_client, "_resolver_broadcast_enabled", True),
            patch.object(nexora_client, "_resolver_broadcast_fanout", 3),
            patch.object(nexora_client, "_resolver_broadcast_timeout", 0.3),
            patch.object(
                nexora_client,
                "_resolver_broadcast_per_resolver_timeout",
                0.3,
            ),
            patch.object(nexora_client, "_resolver_last_chance_fallback", False),
            patch.object(nexora_client, "_resolver_parallel_fallback", False),
            patch("nexora_client.time.sleep", return_value=None),
        ):
            qid, _ = _query_txt(
                selector=selector,
                port=53,
                zone="example.com",
                timeout=0.3,
                payload=b"x",
                attempts=1,
                qtype=TYPE_TXT,
            )

        self.assertEqual(qid, 700)
        self.assertIn("1.1.1.1", seen_servers)
        self.assertGreaterEqual(len(set(seen_servers)), 2)

    def test_broadcast_failure_falls_back_to_serial_even_when_fanout_matches_pool(self):
        selector = ResolverSelector(
            ["1.1.1.1", "8.8.8.8", "9.9.9.9"],
            fail_streak_before_blacklist=20,
        )
        seen_servers: list[str] = []
        attempts = {"n": 0}
        old_kpi = nexora_client._runtime_kpi
        nexora_client._runtime_kpi = nexora_client._RuntimeKpi()

        def _fake_query(server, port, zone, timeout, payload, qtype):
            seen_servers.append(server)
            attempts["n"] += 1
            if attempts["n"] <= 3:
                raise TimeoutError("timed out")
            return 701, object()

        try:
            with (
                patch("nexora_client._query_pkt_direct", side_effect=_fake_query),
                patch.object(nexora_client, "_resolver_broadcast_enabled", True),
                patch.object(nexora_client, "_resolver_broadcast_fanout", 3),
                patch.object(nexora_client, "_resolver_broadcast_timeout", 0.3),
                patch.object(
                    nexora_client,
                    "_resolver_broadcast_per_resolver_timeout",
                    0.3,
                ),
                patch.object(nexora_client, "_resolver_last_chance_fallback", False),
                patch.object(nexora_client, "_resolver_parallel_fallback", False),
                patch("nexora_client.time.sleep", return_value=None),
            ):
                qid, _ = _query_txt(
                    selector=selector,
                    port=53,
                    zone="example.com",
                    timeout=0.3,
                    payload=b"x",
                    attempts=1,
                    qtype=TYPE_TXT,
                )
            snap = nexora_client._runtime_kpi.snapshot()
        finally:
            nexora_client._runtime_kpi = old_kpi

        self.assertEqual(qid, 701)
        self.assertEqual(len(seen_servers), 4)
        self.assertEqual(snap["broadcast_fail_count"], 1)
        self.assertEqual(snap["fallback_success_count"], 1)


if __name__ == "__main__":
    unittest.main()
