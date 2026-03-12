"""
Unit tests for resolver-file strict filtering behavior.
"""

import os
import sys
import time
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from nexora_client import _ensure_min_resolver_count, _extract_resolvers_from_scan_json


class TestResolverFileFiltering(unittest.TestCase):
    def _base_row(self, resolver: str) -> dict:
        return {
            "resolver": resolver,
            "runtime_pass_rate": 0.95,
            "latency_ms": 120.0,
            "score": 4.8,
            "runtime_consecutive_failures": 0,
            "runtime_last_probe_ts": int(time.time()),
            "pool": "active",
            "random_subdomain": True,
            "tunnel_realistic": True,
            "nxdomain_correct": True,
            "bidirectional": True,
            "protocol_roundtrip": True,
        }

    def test_accepts_strictly_good_row(self):
        data = {"resolvers": [self._base_row("1.1.1.1")]}
        got = _extract_resolvers_from_scan_json(
            data,
            min_pass_rate=0.8,
            max_latency_ms=500.0,
            min_score=4.0,
            max_consecutive_failures=1,
            max_stale_sec=300.0,
            allowed_pools={"active", "standby"},
        )
        self.assertEqual(got, ["1.1.1.1"])

    def test_rejects_row_without_bidirectional(self):
        row = self._base_row("1.1.1.1")
        row["bidirectional"] = False
        data = {"resolvers": [row]}
        got = _extract_resolvers_from_scan_json(
            data,
            min_pass_rate=0.8,
            max_latency_ms=500.0,
            min_score=4.0,
            max_consecutive_failures=1,
            max_stale_sec=300.0,
            allowed_pools={"active", "standby"},
        )
        self.assertEqual(got, [])

    def test_rejects_row_without_protocol_roundtrip(self):
        row = self._base_row("1.1.1.1")
        row["protocol_roundtrip"] = False
        data = {"resolvers": [row]}
        got = _extract_resolvers_from_scan_json(
            data,
            min_pass_rate=0.8,
            max_latency_ms=500.0,
            min_score=4.0,
            max_consecutive_failures=1,
            max_stale_sec=300.0,
            allowed_pools={"active", "standby"},
        )
        self.assertEqual(got, [])

    def test_rejects_row_with_high_consecutive_failures(self):
        row = self._base_row("1.1.1.1")
        row["runtime_consecutive_failures"] = 5
        data = {"resolvers": [row]}
        got = _extract_resolvers_from_scan_json(
            data,
            min_pass_rate=0.8,
            max_latency_ms=500.0,
            min_score=4.0,
            max_consecutive_failures=1,
            max_stale_sec=300.0,
            allowed_pools={"active", "standby"},
        )
        self.assertEqual(got, [])

    def test_rejects_stale_probe_rows(self):
        row = self._base_row("1.1.1.1")
        row["runtime_last_probe_ts"] = int(time.time()) - 7200
        data = {"resolvers": [row]}
        got = _extract_resolvers_from_scan_json(
            data,
            min_pass_rate=0.8,
            max_latency_ms=500.0,
            min_score=4.0,
            max_consecutive_failures=1,
            max_stale_sec=300.0,
            allowed_pools={"active", "standby"},
        )
        self.assertEqual(got, [])

    def test_rejects_rows_outside_allowed_pool(self):
        row = self._base_row("1.1.1.1")
        row["pool"] = "cold"
        data = {"resolvers": [row]}
        got = _extract_resolvers_from_scan_json(
            data,
            min_pass_rate=0.8,
            max_latency_ms=500.0,
            min_score=4.0,
            max_consecutive_failures=1,
            max_stale_sec=300.0,
            allowed_pools={"active", "standby"},
        )
        self.assertEqual(got, [])

    def test_no_fallback_to_resolver_list_when_rows_exist_but_all_bad(self):
        row = self._base_row("1.1.1.1")
        row["bidirectional"] = False
        data = {
            "resolvers": [row],
            "resolver_list": ["8.8.8.8", "1.1.1.1"],
        }
        got = _extract_resolvers_from_scan_json(
            data,
            min_pass_rate=0.8,
            max_latency_ms=500.0,
            min_score=4.0,
            max_consecutive_failures=1,
            max_stale_sec=300.0,
            allowed_pools={"active", "standby"},
        )
        self.assertEqual(got, [])

    def test_fallback_to_resolver_list_when_rows_key_absent(self):
        data = {"resolver_list": ["8.8.8.8", "1.1.1.1"]}
        got = _extract_resolvers_from_scan_json(
            data,
            min_pass_rate=0.8,
            max_latency_ms=500.0,
            min_score=4.0,
            max_consecutive_failures=1,
            max_stale_sec=300.0,
            allowed_pools={"active", "standby"},
        )
        self.assertEqual(got, ["8.8.8.8", "1.1.1.1"])

    def test_ensure_min_resolver_count_supplements_from_seeds(self):
        got = _ensure_min_resolver_count(
            primary=["1.1.1.1"],
            fallback=["8.8.8.8", "1.1.1.1"],
            min_count=2,
        )
        self.assertEqual(got, ["1.1.1.1", "8.8.8.8"])

    def test_ensure_min_resolver_count_keeps_primary_when_enough(self):
        got = _ensure_min_resolver_count(
            primary=["1.1.1.1", "8.8.8.8"],
            fallback=["9.9.9.9"],
            min_count=2,
        )
        self.assertEqual(got, ["1.1.1.1", "8.8.8.8"])


if __name__ == "__main__":
    unittest.main()
