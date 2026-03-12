"""
Unit tests for stream query wait-time budgeting.
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import nexora_client
from nexora_client import _estimate_query_wall_timeout


class TestStreamWaitBudget(unittest.TestCase):
    def setUp(self) -> None:
        self._saved = (
            nexora_client._resolver_broadcast_enabled,
            nexora_client._resolver_broadcast_fanout,
            nexora_client._resolver_broadcast_timeout,
            nexora_client._resolver_last_chance_fallback,
            nexora_client._resolver_parallel_fallback,
        )

    def tearDown(self) -> None:
        (
            nexora_client._resolver_broadcast_enabled,
            nexora_client._resolver_broadcast_fanout,
            nexora_client._resolver_broadcast_timeout,
            nexora_client._resolver_last_chance_fallback,
            nexora_client._resolver_parallel_fallback,
        ) = self._saved

    def test_broadcast_budget_covers_round_budget_plus_fallback(self) -> None:
        nexora_client._resolver_broadcast_enabled = True
        nexora_client._resolver_broadcast_fanout = 2
        nexora_client._resolver_broadcast_timeout = 4.0
        nexora_client._resolver_last_chance_fallback = True
        nexora_client._resolver_parallel_fallback = True

        estimate = _estimate_query_wall_timeout(timeout=4.0, attempts=6)

        # Per-round budget ~= 4s broadcast + 4s serial fallback.
        self.assertGreaterEqual(estimate, 48.0)
        # Legacy wait formula in forward loop was timeout*attempts+3 => 27s.
        self.assertGreater(estimate, 27.0)

    def test_serial_budget_includes_last_chance_fallback(self) -> None:
        nexora_client._resolver_broadcast_enabled = False
        nexora_client._resolver_last_chance_fallback = True
        nexora_client._resolver_parallel_fallback = False

        estimate = _estimate_query_wall_timeout(timeout=2.0, attempts=4)

        # 4 rounds + one final fallback query minimum.
        self.assertGreater(estimate, 10.0)

    def test_disabling_last_chance_reduces_budget(self) -> None:
        nexora_client._resolver_broadcast_enabled = False
        nexora_client._resolver_last_chance_fallback = True
        with_fallback = _estimate_query_wall_timeout(timeout=2.0, attempts=4)

        nexora_client._resolver_last_chance_fallback = False
        without_fallback = _estimate_query_wall_timeout(timeout=2.0, attempts=4)

        self.assertGreater(with_fallback, without_fallback)


if __name__ == "__main__":
    unittest.main()

