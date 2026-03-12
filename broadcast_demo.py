"""
QUICK START: Broadcast Query Redundancy for Nexora

This file contains a minimal working example that you can run immediately.
"""

import socket
import random
import time
from concurrent.futures import ThreadPoolExecutor, FIRST_COMPLETED, wait

# ============================================================================
# Minimal Mock Resolver
# ============================================================================

class SimpleResolver:
    """Mock DNS resolver with configurable success rate."""
    
    def __init__(self, name, success_prob=0.5):
        self.name = name
        self.success_prob = success_prob
        self.queries = 0
        self.successes = 0
    
    def query(self, nonce):
        """Simulate DNS query."""
        self.queries += 1
        time.sleep(0.1)  # Simulate network delay
        
        if random.random() < self.success_prob:
            self.successes += 1
            return {"nonce": nonce, "data": f"response from {self.name}"}
        else:
            raise TimeoutError(f"{self.name}: timeout")


# ============================================================================
# Broadcast Function
# ============================================================================

def broadcast_query(resolvers, nonce, timeout=3.0):
    """
    Send query to multiple resolvers in parallel.
    Return FIRST successful response.
    
    Args:
        resolvers: List of resolver objects
        nonce: Query ID
        timeout: Total timeout in seconds
    
    Returns:
        Response dict on success
    
    Raises:
        TimeoutError: All resolvers failed
    """
    executor = ThreadPoolExecutor(max_workers=len(resolvers))
    futures = {}
    
    print(f"\n📡 Broadcast query: nonce={nonce}")
    print(f"   Sending to {len(resolvers)} resolvers in parallel...")
    
    start_time = time.time()
    
    try:
        # Launch all queries
        for resolver in resolvers:
            future = executor.submit(resolver.query, nonce)
            futures[future] = resolver
        
        # Wait for FIRST success
        while True:
            elapsed = time.time() - start_time
            
            if elapsed > timeout:
                raise TimeoutError(f"Timeout after {elapsed:.2f}s")
            
            remaining = timeout - elapsed
            
            # FIRST_COMPLETED pattern
            done, pending = wait(
                futures.keys(),
                return_when=FIRST_COMPLETED,
                timeout=max(0.1, remaining)
            )
            
            # Check completed
            for future in done:
                resolver = futures[future]
                try:
                    result = future.result(timeout=0)
                    
                    # Cancel pending
                    for f in futures:
                        f.cancel()
                    
                    print(f"   ✅ Success from {resolver.name} in {elapsed:.3f}s")
                    return result
                
                except Exception as e:
                    print(f"   ❌ {resolver.name}: {e}")
            
            if len([f for f in futures if f.done()]) == len(futures):
                raise TimeoutError("All resolvers failed")
    
    finally:
        executor.shutdown(wait=False)


# ============================================================================
# Demo: Your DNS Pool Scenario
# ============================================================================

def demo_your_dns_pool():
    """
    Demo using YOUR actual resolver configuration.
    
    Your resolvers:
      185.49.84.2     → 45% success
      178.22.122.100  → 50% success
      8.8.8.8         → 60% success
      1.1.1.1         → 55% success
    """
    print("\n" + "="*70)
    print("DEMO: Your DNS Pool (45-60% individual success)")
    print("="*70)
    
    # Create resolvers matching your pool
    resolvers = [
        SimpleResolver("185.49.84.2", success_prob=0.45),
        SimpleResolver("178.22.122.100", success_prob=0.50),
        SimpleResolver("8.8.8.8", success_prob=0.60),
        SimpleResolver("1.1.1.1", success_prob=0.55),
    ]
    
    # Run 10 queries
    successes = 0
    for query_num in range(10):
        nonce = random.randint(0, 2**32-1)
        try:
            result = broadcast_query(resolvers, nonce)
            successes += 1
            print(f"   Result: {result}\n")
        except TimeoutError as e:
            print(f"   ❌ Failed: {e}\n")
    
    # Statistics
    print(f"\n{'='*70}")
    print(f"Results after 10 queries:")
    print(f"  Success rate: {successes}/10 = {successes*10}%")
    print(f"  Expected:     ~96-97% (1 - 0.525^4)")
    print(f"\n✨ Improvement: 52.5% (single) → {successes*10}% (broadcast)")
    print(f"{'='*70}")
    
    # Per-resolver stats
    print(f"\nPer-resolver statistics:")
    for r in resolvers:
        success_pct = r.successes / r.queries * 100 if r.queries > 0 else 0
        print(f"  {r.name:20s}: {r.queries} queries, "
              f"{r.successes} successes ({success_pct:.0f}%)")


# ============================================================================
# Demo 2: Extreme Degradation
# ============================================================================

def demo_extreme_degradation():
    """
    Demo: All resolvers @ 30% (very bad day).
    """
    print("\n" + "="*70)
    print("DEMO: Extreme Degradation (All resolvers @ 30%)")
    print("="*70)
    
    resolvers = [
        SimpleResolver(f"resolver-{i}", success_prob=0.30)
        for i in range(5)
    ]
    
    print(f"\nSetup: 5 resolvers @ 30% each")
    print(f"Expected single resolver: 30%")
    print(f"Expected broadcast 5x:    1 - (0.7^5) = 83.2%")
    
    successes = 0
    for query_num in range(10):
        nonce = random.randint(0, 2**32-1)
        try:
            result = broadcast_query(resolvers, nonce)
            successes += 1
        except TimeoutError:
            pass
    
    print(f"\nActual broadcast success: {successes}/10 = {successes*10}%")
    print(f"(Expected ~82-85% with 10 samples)")


# ============================================================================
# Demo 3: Success Rate Math
# ============================================================================

def demo_success_rate_math():
    """
    Show the math behind broadcast redundancy.
    """
    print("\n" + "="*70)
    print("Math Behind Broadcast Redundancy")
    print("="*70)
    
    print("\nFormula: success_rate = 1 - (1 - p)^n")
    print("where p = single resolver success probability")
    print("      n = number of resolvers in broadcast")
    
    print("\n" + "-"*70)
    print("Your DNS Pool Analysis:")
    print("-"*70)
    
    probs = [0.45, 0.50, 0.60, 0.55]  # Your resolvers
    avg_prob = sum(probs) / len(probs)
    
    print(f"\nResolver success probabilities: {[f'{p:.0%}' for p in probs]}")
    print(f"Average: {avg_prob:.0%}")
    
    print(f"\nComparison:")
    print(f"  Single resolver:         {avg_prob:.1%}")
    print(f"  Dual-path (2x):          {1 - (1-avg_prob)**2:.1%}")
    print(f"  Broadcast 3x:            {1 - (1-avg_prob)**3:.1%}")
    print(f"  Broadcast 4x:            {1 - (1-avg_prob)**4:.1%}")
    print(f"  Broadcast 5x:            {1 - (1-avg_prob)**5:.1%}")
    
    print(f"\nImprovement with broadcast 4x: +{(1 - (1-avg_prob)**4 - avg_prob):.1%}")


# ============================================================================
# Demo 4: Latency Comparison
# ============================================================================

def demo_latency():
    """
    Show latency improvement: serial vs parallel.
    """
    print("\n" + "="*70)
    print("Latency: Serial vs Parallel")
    print("="*70)
    
    per_query = 1.5  # seconds
    retry_delay = 0.1
    
    print(f"\nAssumptions:")
    print(f"  Per-query latency: {per_query}s")
    print(f"  Retry delay: {retry_delay}s")
    
    # Serial: 3 attempts
    serial = per_query + retry_delay + per_query + retry_delay + per_query
    print(f"\nSerial (3 sequential attempts):")
    print(f"  {per_query}s + {retry_delay}s + {per_query}s + {retry_delay}s + {per_query}s")
    print(f"  = {serial:.2f}s")
    
    # Parallel: 1 of 3 succeeds
    parallel = per_query
    print(f"\nParallel (1 of 3 succeeds):")
    print(f"  max({per_query}s, {per_query}s, {per_query}s)")
    print(f"  = {parallel:.2f}s")
    
    speedup = serial / parallel
    print(f"\nSpeedup: {speedup:.1f}x FASTER!")


# ============================================================================
# Main Runner
# ============================================================================

def main():
    """Run all demos."""
    print("\n" + "🚀 "*35)
    print("Broadcast Query Redundancy - Quick Start Demo")
    print("🚀 "*35)
    
    # Demo 1: Your DNS pool
    demo_your_dns_pool()
    
    # Demo 2: Extreme degradation
    demo_extreme_degradation()
    
    # Demo 3: Math
    demo_success_rate_math()
    
    # Demo 4: Latency
    demo_latency()
    
    print("\n" + "="*70)
    print("✅ All demos completed!")
    print("="*70)
    print("\nNext steps:")
    print("  1. Review docs/BROADCAST_REDUNDANCY_PROPOSAL.md")
    print("  2. Study src/broadcast_query.py")
    print("  3. Run tests: pytest tests/test_broadcast_redundancy.py -v")
    print("  4. Integrate into nexora_client.py")
    print("="*70 + "\n")


if __name__ == "__main__":
    main()
