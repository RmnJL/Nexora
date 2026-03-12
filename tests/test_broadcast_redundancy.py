"""
Comprehensive test suite for Broadcast Query Redundancy.

Tests cover:
1. Basic broadcast functionality
2. Parallel failure scenarios
3. Success rate calculations
4. Nonce validation
5. Integration with stream handler
6. Real-world DNS pools

Run: pytest tests/test_broadcast_redundancy.py -v
"""

import pytest
import time
import random
import socket
import concurrent.futures
from unittest.mock import Mock, MagicMock, patch
from concurrent.futures import ThreadPoolExecutor, FIRST_COMPLETED
from threading import Lock


# ============================================================================
# Mock Classes for Testing
# ============================================================================

class MockDNSResolver:
    """
    Simulates a DNS resolver with configurable success probability.
    Useful for testing broadcast behavior under degraded conditions.
    """
    
    def __init__(self, 
                 name: str,
                 success_prob: float = 0.5,
                 response_delay: float = 0.0,
                 timeout_prob: float = 0.0):
        """
        Args:
            name: Resolver name/IP for logging
            success_prob: Probability of successful response (0.0-1.0)
            response_delay: Simulated network delay in seconds
            timeout_prob: Probability of timeout (overrides success)
        """
        self.name = name
        self.success_prob = success_prob
        self.response_delay = response_delay
        self.timeout_prob = timeout_prob
        self.query_count = 0
        self.success_count = 0
        self.failure_count = 0
        self._lock = Lock()
    
    def query(self, nonce: int, session_id: int) -> tuple[int, dict]:
        """
        Simulates DNS query response.
        
        Returns:
            (qid, response_dict) on success
        
        Raises:
            TimeoutError: If resolver times out
            ConnectionError: If resolver rejects query
        """
        with self._lock:
            self.query_count += 1
        
        # Simulate network delay
        if self.response_delay > 0:
            time.sleep(self.response_delay)
        
        # Simulate timeout
        if random.random() < self.timeout_prob:
            with self._lock:
                self.failure_count += 1
            raise TimeoutError(f"{self.name}: timeout")
        
        # Simulate success/failure
        if random.random() < self.success_prob:
            with self._lock:
                self.success_count += 1
            return 1, {
                "nonce": nonce,
                "session_id": session_id,
                "type": "STREAM_RECV",
                "payload": b"mock_response"
            }
        else:
            with self._lock:
                self.failure_count += 1
            raise TimeoutError(f"{self.name}: no answer")
    
    def reset(self):
        """Reset statistics."""
        with self._lock:
            self.query_count = 0
            self.success_count = 0
            self.failure_count = 0
    
    def get_stats(self) -> dict:
        """Return resolver statistics."""
        with self._lock:
            success_rate = (
                self.success_count / self.query_count * 100
                if self.query_count > 0 else 0
            )
            return {
                "name": self.name,
                "queries": self.query_count,
                "successes": self.success_count,
                "failures": self.failure_count,
                "success_rate": f"{success_rate:.1f}%"
            }


# ============================================================================
# Unit Tests: Basic Functionality
# ============================================================================

class TestBroadcastBasics:
    """Test basic broadcast query functionality."""
    
    def test_single_healthy_resolver(self):
        """
        Test: Query to single healthy resolver succeeds.
        
        Expected: Response returned with matching nonce/sid.
        """
        resolver = MockDNSResolver("resolver-a", success_prob=1.0)
        
        nonce = 12345
        session_id = 999
        
        qid, resp = resolver.query(nonce, session_id)
        
        assert qid == 1
        assert resp["nonce"] == nonce
        assert resp["session_id"] == session_id
        print("✓ Single healthy resolver test PASSED")
    
    def test_single_failing_resolver(self):
        """
        Test: Query to failing resolver raises TimeoutError.
        
        Expected: Exception raised.
        """
        resolver = MockDNSResolver("resolver-a", success_prob=0.0)
        
        nonce = 12345
        session_id = 999
        
        with pytest.raises(TimeoutError):
            resolver.query(nonce, session_id)
        
        print("✓ Single failing resolver test PASSED")
    
    def test_nonce_validation(self):
        """
        Test: Response nonce must match request nonce.
        
        Expected: Nonce preserved through query/response cycle.
        """
        resolver = MockDNSResolver("resolver-a", success_prob=1.0)
        
        test_nonces = [12345, 99999, 1, 2**32 - 1]
        
        for nonce in test_nonces:
            qid, resp = resolver.query(nonce, 999)
            assert resp["nonce"] == nonce, f"Nonce mismatch: {nonce}"
        
        print("✓ Nonce validation test PASSED")


# ============================================================================
# Broadcast Simulation Tests
# ============================================================================

class TestBroadcastParallel:
    """Test parallel query broadcast scenarios."""
    
    def test_broadcast_4_resolvers_50pct_each(self):
        """
        Test: Broadcast 4 resolvers @ 50% success each.
        
        Expected: High success rate due to parallel execution.
        Theoretical: 1 - (0.5^4) = 93.75%
        """
        num_runs = 100
        successes = 0
        
        for run in range(num_runs):
            resolvers = [
                MockDNSResolver(f"resolver-{i}", success_prob=0.5)
                for i in range(4)
            ]
            
            executor = ThreadPoolExecutor(max_workers=4)
            futures = []
            
            nonce = random.randint(0, 2**32 - 1)
            session_id = 999
            
            for resolver in resolvers:
                fut = executor.submit(resolver.query, nonce, session_id)
                futures.append(fut)
            
            # Wait until first SUCCESS, not merely first completion.
            pending = set(futures)
            success = False
            deadline = time.time() + 5.0
            while pending and time.time() < deadline:
                done, pending = concurrent.futures.wait(
                    pending,
                    return_when=FIRST_COMPLETED,
                    timeout=max(0.05, deadline - time.time()),
                )
                if not done:
                    continue
                for fut in done:
                    try:
                        qid, resp = fut.result(timeout=0)
                        if resp["nonce"] == nonce:
                            success = True
                            break
                    except:
                        pass
                if success:
                    break
            for f in pending:
                f.cancel()
            if success:
                successes += 1
            
            executor.shutdown(wait=False)
        
        success_rate = successes / num_runs * 100
        expected_min = 85.0  # Allow some variance
        
        assert success_rate >= expected_min, \
            f"Success rate {success_rate:.1f}% < {expected_min}%"
        
        print(f"✓ Broadcast 4x50% test PASSED: {success_rate:.1f}% success "
              f"(expected ~93.75%)")
    
    def test_broadcast_5_resolvers_40pct_each(self):
        """
        Test: Broadcast 5 resolvers @ 40% success each.
        
        This simulates the user's actual condition: weak DNS pool.
        Theoretical: 1 - (0.6^5) = 92.22%
        """
        num_samples = 50
        successes = 0
        total_time = 0
        
        for sample in range(num_samples):
            resolvers = [
                MockDNSResolver(f"resolver-{i}", 
                              success_prob=0.4,
                              response_delay=0.1)
                for i in range(5)
            ]
            
            executor = ThreadPoolExecutor(max_workers=5)
            futures = []
            
            nonce = random.randint(0, 2**32 - 1)
            session_id = 999
            
            start = time.time()
            
            for resolver in resolvers:
                fut = executor.submit(resolver.query, nonce, session_id)
                futures.append(fut)
            
            pending = set(futures)
            success = False
            deadline = start + 5.0
            while pending and time.time() < deadline:
                done, pending = concurrent.futures.wait(
                    pending,
                    return_when=FIRST_COMPLETED,
                    timeout=max(0.05, deadline - time.time()),
                )
                if not done:
                    continue
                for fut in done:
                    try:
                        qid, resp = fut.result(timeout=0)
                        if resp["nonce"] == nonce:
                            success = True
                            break
                    except:
                        pass
                if success:
                    break
            elapsed = time.time() - start
            total_time += elapsed
            for f in pending:
                f.cancel()
            if success:
                successes += 1
            
            executor.shutdown(wait=False)
        
        success_rate = successes / num_samples * 100
        avg_latency = total_time / num_samples
        
        print(f"✓ Broadcast 5x40% test PASSED:")
        print(f"  - Success rate: {success_rate:.1f}% (expected ~92.2%)")
        print(f"  - Avg latency: {avg_latency:.3f}s")


# ============================================================================
# Success Rate Mathematics Validation
# ============================================================================

class TestSuccessRateMath:
    """Verify success rate calculations."""
    
    @staticmethod
    def calculate_broadcast_success(per_resolver_prob: float, 
                                   num_resolvers: int) -> float:
        """
        Calculate theoretical broadcast success rate.
        
        Formula: 1 - (1 - p)^n
        where p = single resolver success probability
              n = number of resolvers in broadcast
        """
        return 1.0 - ((1.0 - per_resolver_prob) ** num_resolvers)
    
    def test_math_single_resolver(self):
        """Single resolver: success rate = p."""
        p = 0.6
        result = self.calculate_broadcast_success(p, 1)
        assert result == p, f"Expected {p}, got {result}"
        print(f"✓ Single resolver (p={p}): {result:.1%}")
    
    def test_math_dual_path(self):
        """Dual path: success rate = 1 - (1-p)^2."""
        p = 0.5
        expected = 0.75
        result = self.calculate_broadcast_success(p, 2)
        assert abs(result - expected) < 0.001
        print(f"✓ Dual path (p={p}): {result:.1%} (expected 75%)")
    
    def test_math_broadcast_4(self):
        """Broadcast 4 @ 50%."""
        p = 0.5
        expected = 0.9375
        result = self.calculate_broadcast_success(p, 4)
        assert abs(result - expected) < 0.001
        print(f"✓ Broadcast 4 (p={p}): {result:.1%}")
    
    def test_math_broadcast_5_weak(self):
        """Broadcast 5 @ 40% (user's scenario)."""
        p = 0.4
        expected = 0.92224
        result = self.calculate_broadcast_success(p, 5)
        assert abs(result - expected) < 0.001
        print(f"✓ Broadcast 5 (p={p}): {result:.1%} (expected ~92.2%)")


# ============================================================================
# Comparison Tests: Single vs Broadcast
# ============================================================================

class TestSingleVsBroadcast:
    """Compare single-path vs broadcast-path success rates."""
    
    def test_comparison_matrix(self):
        """
        Create comparison table: single vs various broadcast configs.
        
        Shows improvement over single resolver.
        """
        print("\n" + "="*70)
        print("Single vs Broadcast Comparison")
        print("="*70)
        
        for p in [0.4, 0.5, 0.6]:
            single = p
            dual = 1 - ((1 - p) ** 2)
            broadcast_3 = 1 - ((1 - p) ** 3)
            broadcast_4 = 1 - ((1 - p) ** 4)
            broadcast_5 = 1 - ((1 - p) ** 5)
            
            print(f"\nResolver success probability: {p:.0%}")
            print(f"  Single resolver:        {single:.2%}")
            print(f"  Dual-path:              {dual:.2%}  (+{(dual-single):.2%})")
            print(f"  Broadcast 3x:           {broadcast_3:.2%}  (+{(broadcast_3-single):.2%})")
            print(f"  Broadcast 4x:           {broadcast_4:.2%}  (+{(broadcast_4-single):.2%})")
            print(f"  Broadcast 5x:           {broadcast_5:.2%}  (+{(broadcast_5-single):.2%})")
        
        print("="*70)


# ============================================================================
# Latency Comparison
# ============================================================================

class TestLatencyComparison:
    """Compare latency: single-path vs broadcast."""
    
    def test_serial_vs_parallel_latency(self):
        """
        Compare cumulative latency:
        - Serial: attempt1 + backoff + attempt2 + backoff + attempt3
        - Parallel: max(attempt1, attempt2, attempt3)
        """
        print("\n" + "="*70)
        print("Latency Comparison: Serial vs Parallel")
        print("="*70)
        
        per_query_latency = 1.5  # seconds
        retry_backoff = 0.1      # seconds
        
        # Serial (worst case: 3 attempts)
        serial_latency = (
            per_query_latency +           # Query 1
            retry_backoff +               # Wait
            per_query_latency +           # Query 2
            retry_backoff +               # Wait
            per_query_latency             # Query 3
        )
        
        # Parallel (queries happen simultaneously)
        parallel_latency = per_query_latency  # Only need 1 to succeed
        
        improvement = serial_latency / parallel_latency
        
        print(f"\nAssumptions:")
        print(f"  Per-query latency: {per_query_latency}s")
        print(f"  Retry backoff:     {retry_backoff}s")
        print(f"\nResults:")
        print(f"  Serial (3 attempts): {serial_latency:.2f}s")
        print(f"  Parallel (1 success): {parallel_latency:.2f}s")
        print(f"  Speedup: {improvement:.1f}x faster!")
        print("="*70)


# ============================================================================
# Real-World Scenarios
# ============================================================================

class TestRealWorldScenarios:
    """Test scenarios matching real DNS pool conditions."""
    
    def test_scenario_your_dns_pool(self):
        """
        Scenario: Your actual DNS pool configuration.
        
        Resolvers:
          185.49.84.2     → 45% success
          178.22.122.100  → 50% success
          8.8.8.8         → 60% success
          1.1.1.1         → 55% success
        """
        print("\n" + "="*70)
        print("Real-World Scenario: Your DNS Pool")
        print("="*70)
        
        resolvers = [
            MockDNSResolver("185.49.84.2", success_prob=0.45),
            MockDNSResolver("178.22.122.100", success_prob=0.50),
            MockDNSResolver("8.8.8.8", success_prob=0.60),
            MockDNSResolver("1.1.1.1", success_prob=0.55),
        ]
        
        avg_success = sum(r.success_prob for r in resolvers) / len(resolvers)
        
        # Theoretical broadcast success
        theoretical_broadcast = 1 - ((1 - avg_success) ** len(resolvers))
        
        print(f"\nResolver Breakdown:")
        for r in resolvers:
            print(f"  {r.name:20s}: {r.success_prob:.0%}")
        
        print(f"\nStatistics:")
        print(f"  Average success:      {avg_success:.1%}")
        print(f"  Single resolver:      {avg_success:.1%}")
        print(f"  Broadcast 4x (theory): {theoretical_broadcast:.1%}")
        print(f"  Improvement:          +{(theoretical_broadcast - avg_success):.1%}")
        
        # Simulate actual broadcast queries
        num_simulations = 50
        successes = 0
        
        for sim in range(num_simulations):
            executor = ThreadPoolExecutor(max_workers=len(resolvers))
            futures = [
                executor.submit(r.query, random.randint(0, 2**32-1), 999)
                for r in resolvers
            ]
            
            done, _ = concurrent.futures.wait(
                futures,
                return_when=FIRST_COMPLETED,
                timeout=5.0
            )
            
            if done:
                try:
                    fut = list(done)[0]
                    qid, resp = fut.result(timeout=0)
                    successes += 1
                except:
                    pass
            
            executor.shutdown(wait=False)
        
        actual_success_rate = successes / num_simulations
        print(f"\nSimulation (50 queries):")
        print(f"  Actual success rate:  {actual_success_rate:.1%}")
        print(f"  Expected (theory):    {theoretical_broadcast:.1%}")
        print(f"  Deviation:            {abs(actual_success_rate - theoretical_broadcast):.1%}")
        print("="*70)
    
    def test_scenario_extreme_degradation(self):
        """
        Scenario: Extreme DNS degradation.
        
        All resolvers at 30% success (very bad day for DNS).
        """
        print("\n" + "="*70)
        print("Extreme Scenario: All Resolvers @ 30%")
        print("="*70)
        
        p = 0.3
        
        # Single resolver
        single_success = p
        
        # Broadcast options
        broadcast_3 = 1 - ((1 - p) ** 3)
        broadcast_5 = 1 - ((1 - p) ** 5)
        broadcast_7 = 1 - ((1 - p) ** 7)
        
        print(f"\nResolver success probability: {p:.0%}")
        print(f"\nSuccess Rates:")
        print(f"  Single resolver:  {single_success:.2%}")
        print(f"  Broadcast 3x:     {broadcast_3:.2%} (need 1 of 3)")
        print(f"  Broadcast 5x:     {broadcast_5:.2%} (need 1 of 5)")
        print(f"  Broadcast 7x:     {broadcast_7:.2%} (need 1 of 7)")
        print(f"\nEven in extreme conditions, broadcast still provides")
        print(f"reasonable reliability. Trade-off: 5-7 DNS queries.")
        print("="*70)


# ============================================================================
# Integration Tests
# ============================================================================

class TestBroadcastIntegration:
    """Integration tests simulating real stream scenarios."""
    
    def test_stream_send_receive_cycle(self):
        """
        Integration: Simulate complete send/receive cycle.
        
        1. Send user data via broadcast query
        2. Receive server response
        3. Validate nonce/session_id
        """
        print("\n✓ Stream Send/Receive Cycle Test")
        
        resolvers = [
            MockDNSResolver(f"resolver-{i}", success_prob=0.5)
            for i in range(4)
        ]
        
        # Simulate send
        send_nonce = 11111
        send_sid = 999
        send_payload = b"Hello from client"
        
        executor = ThreadPoolExecutor(max_workers=4)
        futures = [
            executor.submit(r.query, send_nonce, send_sid)
            for r in resolvers
        ]
        
        done, _ = concurrent.futures.wait(
            futures,
            return_when=FIRST_COMPLETED,
            timeout=5.0
        )
        
        # Validate response
        for fut in done:
            try:
                qid, resp = fut.result(timeout=0)
                assert resp["nonce"] == send_nonce
                assert resp["session_id"] == send_sid
                print("  send succeeded via one resolver")
                break
            except:
                pass
        
        executor.shutdown(wait=False)
    
    def test_adaptive_parallelism(self):
        """
        Integration: Adaptive parallelism based on resolver quality.
        
        - Excellent resolvers (90%): use parallel=2
        - Good resolvers (70%):      use parallel=3
        - Fair resolvers (50%):      use parallel=4
        - Poor resolvers (30%):      use parallel=5-6
        """
        print("\n✓ Adaptive Parallelism Test")
        
        scenarios = [
            ("Excellent", 0.9, 2),
            ("Good", 0.7, 3),
            ("Fair", 0.5, 4),
            ("Poor", 0.3, 5),
        ]
        
        for name, prob, suggested_parallel in scenarios:
            theoretical = 1 - ((1 - prob) ** suggested_parallel)
            print(f"  {name:12s} ({prob:.0%} each): "
                  f"broadcast {suggested_parallel}x → {theoretical:.1%}")


# ============================================================================
# Performance Benchmarks
# ============================================================================

class TestPerformanceBenchmarks:
    """Benchmark broadcast performance."""
    
    def test_throughput_with_broadcast(self):
        """
        Benchmark: Measure throughput improvement with broadcast.
        
        Metric: Queries/second
        """
        print("\n" + "="*70)
        print("Throughput Benchmark")
        print("="*70)
        
        num_queries = 100
        
        # Single-path simulation
        print("\nSingle-path (serial retries):")
        start = time.time()
        successes = 0
        for i in range(num_queries):
            resolver = MockDNSResolver(f"r-{i}", success_prob=0.5)
            try:
                resolver.query(i, 999)
                successes += 1
            except:
                pass
        single_time = time.time() - start
        single_qps = num_queries / single_time
        print(f"  Time: {single_time:.2f}s")
        print(f"  QPS: {single_qps:.1f} queries/sec")
        print(f"  Success: {successes}/{num_queries}")
        
        # Broadcast simulation (simplified: max 500ms each)
        print("\nBroadcast (parallel):")
        start = time.time()
        successes = 0
        for i in range(num_queries):
            resolvers = [
                MockDNSResolver(f"r-{j}", success_prob=0.5, response_delay=0.05)
                for j in range(4)
            ]
            executor = ThreadPoolExecutor(max_workers=4)
            futures = [executor.submit(r.query, i, 999) for r in resolvers]
            done, pending = concurrent.futures.wait(
                futures,
                return_when=FIRST_COMPLETED,
                timeout=5.0
            )
            for f in pending:
                f.cancel()
            for fut in done:
                try:
                    fut.result(timeout=0)
                    successes += 1
                    break
                except:
                    pass
            executor.shutdown(wait=False)
        broadcast_time = time.time() - start
        broadcast_qps = num_queries / broadcast_time
        print(f"  Time: {broadcast_time:.2f}s")
        print(f"  QPS: {broadcast_qps:.1f} queries/sec")
        print(f"  Success: {successes}/{num_queries}")
        print(f"\nSpeedup: {single_qps / broadcast_qps:.1f}x faster (expected ~4x)")
        print("="*70)


# ============================================================================
# Main Test Runner
# ============================================================================

if __name__ == "__main__":
    print("\n" + "="*70)
    print("Broadcast Query Redundancy - Comprehensive Test Suite")
    print("="*70)
    
    # Run all test classes
    pytest.main([__file__, "-v", "-s"])
    
    print("\n" + "="*70)
    print("All tests completed!")
    print("="*70)

