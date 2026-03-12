"""
Broadcast Query Redundancy Implementation for Nexora.

This module provides BroadcastQueryManager class for parallel DNS queries.
Integration with nexora_client.py:
    
    from broadcast_query import BroadcastQueryManager
    from nexora_client import ResolverSelector
    
    selector = ResolverSelector(["8.8.8.8", "1.1.1.1", ...])
    broadcaster = BroadcastQueryManager(selector, num_parallel=4)
    
    # In query path:
    qid, response = broadcaster.broadcast_query(
        packet=encoded_packet,
        expected_nonce=12345,
        expected_sid=999,
        zone="t1.phonexpress.ir",
        qtype=TYPE_TXT
    )

Author: Nexora Team
Date: March 12, 2026
"""

import logging
import socket
import time
import threading
from concurrent.futures import ThreadPoolExecutor, FIRST_COMPLETED, wait as futures_wait
from typing import Optional, Tuple, Set
from threading import Lock

log = logging.getLogger("nexora-broadcast")


def chunk_label(s: str, size: int = 50) -> str:
    """Split string into DNS labels (max 63 chars each)."""
    return ".".join(s[i : i + size] for i in range(0, len(s), size))


class BroadcastQueryManager:
    """
    Manages parallel DNS queries to multiple resolvers.
    
    Core functionality:
    1. Select N diverse resolvers from pool
    2. Send identical query to all N simultaneously
    3. Return FIRST valid response (matching nonce/session_id)
    4. Cancel pending queries when winner found
    
    Usage:
        manager = BroadcastQueryManager(resolver_selector, num_parallel=4)
        qid, response = manager.broadcast_query(packet, nonce, sid, zone)
    """
    
    def __init__(self,
                 resolver_selector,
                 num_parallel: int = 4,
                 broadcast_timeout: float = 3.0,
                 per_resolver_timeout: float = 1.5):
        """
        Initialize broadcast manager.
        
        Args:
            resolver_selector: ResolverSelector instance
            num_parallel: Number of parallel resolvers (default 4)
            broadcast_timeout: Total timeout for broadcast (default 3.0s)
            per_resolver_timeout: Individual resolver timeout (default 1.5s)
        """
        self.selector = resolver_selector
        self.num_parallel = max(2, min(num_parallel, len(resolver_selector.servers)))
        self.broadcast_timeout = max(1.0, broadcast_timeout)
        self.per_resolver_timeout = max(0.5, per_resolver_timeout)
        
        self._lock = Lock()
        self._broadcast_queries_total = 0
        self._broadcast_successes = 0
        self._broadcast_failures = 0
        self._broadcast_fallbacks = 0  # Queries that fell back to serial
        
        log.info(f"BroadcastQueryManager initialized: "
                f"num_parallel={self.num_parallel}, "
                f"timeout={self.broadcast_timeout}s")
    
    def broadcast_query(self,
                       packet: bytes,
                       expected_nonce: int,
                       expected_sid: int,
                       zone: str,
                       qtype: int,
                       port: int = 53,
                       fallback_fn=None) -> Tuple[int, object]:
        """
        Send query to multiple resolvers in parallel.
        Return first valid response.
        
        Args:
            packet: Raw encoded DNS packet
            expected_nonce: Expected nonce in response (for validation)
            expected_sid: Expected session_id in response (for validation)
            zone: DNS zone (e.g., "t1.phonexpress.ir")
            qtype: Query type (TYPE_TXT or TYPE_A from dns_wire)
            port: DNS port (default 53)
            fallback_fn: Optional function to call if broadcast fails.
                        Called as: fallback_fn() → (qid, response)
        
        Returns:
            (qid, response_packet) on success
        
        Raises:
            TimeoutError: All resolvers failed
            ValueError: No resolvers available
        """
        with self._lock:
            self._broadcast_queries_total += 1
        
        # Select diverse resolvers
        resolvers = self._select_diverse_resolvers()
        if not resolvers:
            raise ValueError("No resolvers available for broadcast")
        
        log.info(f"broadcast_query: nonce={expected_nonce} sid={expected_sid} "
                f"num_resolvers={len(resolvers)} resolvers={','.join(resolvers)}")
        
        # Attempt broadcast
        try:
            return self._do_broadcast(
                packet, expected_nonce, expected_sid, zone, qtype, port, resolvers
            )
        
        except TimeoutError as e:
            with self._lock:
                self._broadcast_failures += 1
            
            log.warning(f"Broadcast failed: {e}. "
                       f"Attempting fallback to serial...")
            
            # Use fallback if provided
            if fallback_fn is not None:
                with self._lock:
                    self._broadcast_fallbacks += 1
                try:
                    log.info("Using fallback serial query...")
                    return fallback_fn()
                except Exception as fallback_err:
                    log.error(f"Fallback also failed: {fallback_err}")
                    raise TimeoutError(
                        f"Broadcast failed and fallback failed: {fallback_err}"
                    )
            else:
                raise
    
    def _do_broadcast(self,
                     packet: bytes,
                     expected_nonce: int,
                     expected_sid: int,
                     zone: str,
                     qtype: int,
                     port: int,
                     resolvers: list) -> Tuple[int, object]:
        """
        Internal: Execute parallel broadcast queries.
        Reports success/failure to ResolverSelector for quality learning.
        """
        executor = ThreadPoolExecutor(max_workers=len(resolvers))
        futures = {}
        sent_at: dict = {}
        start_time = time.time()
        
        try:
            # Launch all queries
            for resolver in resolvers:
                sent_at[resolver] = time.time()
                future = executor.submit(
                    self._query_single_resolver,
                    resolver, packet, zone, qtype, port, expected_nonce
                )
                futures[future] = resolver
            
            # Wait for FIRST success OR all timeout
            while True:
                elapsed = time.time() - start_time
                if elapsed > self.broadcast_timeout:
                    # All timed out - report failures
                    for resolver in resolvers:
                        self.selector.report_failure(
                            resolver,
                            latency_ms=(time.time() - sent_at.get(resolver, start_time)) * 1000.0
                        )
                    log.warning(f"Broadcast timeout after {elapsed:.2f}s, "
                               f"all {len(resolvers)} resolvers failed")
                    raise TimeoutError(
                        f"Broadcast timeout after {elapsed:.2f}s"
                    )
                
                remaining_timeout = self.broadcast_timeout - elapsed
                
                # Wait for next completion
                done, pending = futures_wait(
                    futures.keys(),
                    return_when=FIRST_COMPLETED,
                    timeout=max(0.1, remaining_timeout)
                )
                
                if not done:
                    continue
                
                # Check done futures for valid response
                for future in done:
                    resolver = futures[future]
                    try:
                        qid, response_packet = future.result(timeout=0)
                        
                        # Validate response
                        if (response_packet.nonce == expected_nonce and
                            response_packet.session_id == expected_sid):
                            
                            with self._lock:
                                self._broadcast_successes += 1
                            
                            # Report success for this resolver
                            self.selector.report_success(
                                resolver,
                                latency_ms=(time.time() - sent_at[resolver]) * 1000.0
                            )
                            
                            log.info(f"Broadcast success: nonce={expected_nonce} "
                                   f"resolver={resolver} elapsed={elapsed:.3f}s")
                            
                            # Cancel all pending and report their failures
                            for f in futures:
                                if not f.done():
                                    f.cancel()
                                    failed_resolver = futures[f]
                                    self.selector.report_failure(
                                        failed_resolver,
                                        latency_ms=(time.time() - sent_at.get(failed_resolver, start_time)) * 1000.0
                                    )
                            
                            return qid, response_packet
                        else:
                            log.debug(f"Response nonce mismatch from {resolver}: "
                                    f"expected {expected_nonce}, "
                                    f"got {response_packet.nonce}")
                            # Report as failure for nonce mismatch
                            self.selector.report_failure(
                                resolver,
                                latency_ms=(time.time() - sent_at[resolver]) * 1000.0
                            )
                    
                    except Exception as e:
                        log.debug(f"Query from {resolver} failed: {e}")
                        self.selector.report_failure(
                            resolver,
                            latency_ms=(time.time() - sent_at.get(resolver, start_time)) * 1000.0
                        )
                
                # Check if all done futures processed
                if len([f for f in futures if f.done()]) == len(futures):
                    raise TimeoutError("All resolvers returned invalid responses")
        
        finally:
            # Cleanup
            for future in futures:
                future.cancel()
            executor.shutdown(wait=False)
    
    def _query_single_resolver(self,
                              resolver: str,
                              packet: bytes,
                              zone: str,
                              qtype: int,
                              port: int,
                              expected_nonce: int) -> Tuple[int, object]:
        """
        Query single resolver. Return decoded response.
        
        Raises: TimeoutError, ConnectionError, etc.
        """
        from dns_wire import build_query, parse_answer_data
        from nexora_proto import encode_dns_data, decode_dns_data, unpack_packet
        
        try:
            # Encode for DNS
            encoded = encode_dns_data(packet)
            fqdn = f"{chunk_label(encoded)}.{zone.strip('.')}"
            qid, query = build_query(fqdn, qtype=qtype)
            
            # Send query
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.settimeout(self.per_resolver_timeout)
            
            try:
                sock.sendto(query, (resolver, port))
                resp_data, _ = sock.recvfrom(4096)
                
                # Parse response
                txt = parse_answer_data(resp_data, qid)
                
                # Strip DNS labels
                for suffix in [".nexora", ".x"]:
                    if txt.endswith(suffix):
                        txt = txt[:-len(suffix)].strip(".")
                
                # Decode packet
                packet_data = unpack_packet(decode_dns_data(txt))
                
                log.debug(f"Query success from {resolver}")
                return qid, packet_data
            
            finally:
                sock.close()
        
        except socket.timeout:
            raise TimeoutError(f"{resolver}: socket timeout")
        except Exception as e:
            raise RuntimeError(f"{resolver}: {e}")
    
    def _select_diverse_resolvers(self, exclude: Optional[Set[str]] = None) -> list:
        """
        Select N diverse resolvers from active pool.
        Prioritize healthy resolvers based on quality scores, skip excluded ones.
        """
        exclude = exclude or set()
        
        # Get all resolvers
        all_resolvers = self.selector.servers
        
        # Filter available candidates
        candidates = [r for r in all_resolvers if r not in exclude]
        if not candidates:
            candidates = all_resolvers
        
        # Score candidates by health (prefer working resolvers)
        # Each resolver tracks: success_rate, latency, failure_streak
        candidates_scored = []
        for resolver in candidates:
            # Get health metrics from selector
            success_rate = self.selector._success_ewma.get(resolver, 0.5)
            latency = self.selector._latency_ewma_ms.get(resolver, 700.0)
            fail_streak = self.selector._soft_fail_streak.get(resolver, 0)
            
            # Quality score: prefer high success, low latency, low failures
            quality_score = success_rate - (latency / 1000.0) - (fail_streak * 0.1)
            candidates_scored.append((resolver, quality_score))
        
        # Sort by quality (higher is better)
        candidates_scored.sort(key=lambda x: x[1], reverse=True)
        
        # Select top N
        selected = [r[0] for r in candidates_scored[:min(self.num_parallel, len(candidates_scored))]]
        
        return selected
    
    def get_stats(self) -> dict:
        """Return broadcast statistics."""
        with self._lock:
            success_rate = (
                self._broadcast_successes / self._broadcast_queries_total * 100
                if self._broadcast_queries_total > 0 else 0
            )
            return {
                "broadcast_queries_total": self._broadcast_queries_total,
                "broadcast_successes": self._broadcast_successes,
                "broadcast_failures": self._broadcast_failures,
                "broadcast_fallbacks": self._broadcast_fallbacks,
                "success_rate": f"{success_rate:.1f}%"
            }
    
    def reset_stats(self):
        """Reset statistics."""
        with self._lock:
            self._broadcast_queries_total = 0
            self._broadcast_successes = 0
            self._broadcast_failures = 0
            self._broadcast_fallbacks = 0
    
    def set_num_parallel(self, n: int):
        """Dynamically adjust parallelism."""
        n = max(1, min(n, len(self.selector.servers)))
        with self._lock:
            self.num_parallel = n
        log.info(f"Broadcast parallelism updated to {n}")


# ============================================================================
# Integration Functions
# ============================================================================

def create_broadcast_query_manager(selector, config: dict) -> BroadcastQueryManager:
    """
    Factory function to create manager from config dict.
    
    Config keys:
        "broadcast_enabled": bool (default True)
        "broadcast_num_parallel": int (default 4)
        "broadcast_timeout": float (default 3.0)
        "per_resolver_timeout": float (default 1.5)
    """
    if not config.get("broadcast_enabled", True):
        return None
    
    return BroadcastQueryManager(
        selector,
        num_parallel=config.get("broadcast_num_parallel", 4),
        broadcast_timeout=config.get("broadcast_timeout", 3.0),
        per_resolver_timeout=config.get("per_resolver_timeout", 1.5)
    )


# ============================================================================
# CLI Integration
# ============================================================================

def add_broadcast_arguments(parser):
    """Add broadcast-related arguments to argparse parser."""
    parser.add_argument(
        "--broadcast-enable",
        action="store_true",
        default=True,
        help="Enable broadcast query redundancy (default: True)"
    )
    parser.add_argument(
        "--broadcast-disable",
        action="store_true",
        help="Disable broadcast query redundancy"
    )
    parser.add_argument(
        "--broadcast-num-parallel",
        type=int,
        default=4,
        help="Number of parallel resolvers (default: 4)"
    )
    parser.add_argument(
        "--broadcast-timeout",
        type=float,
        default=3.0,
        help="Broadcast total timeout in seconds (default: 3.0)"
    )
    parser.add_argument(
        "--broadcast-per-resolver-timeout",
        type=float,
        default=1.5,
        help="Per-resolver timeout in seconds (default: 1.5)"
    )


# ============================================================================
# Example: How to use in nexora_client.py
# ============================================================================

"""
Example integration in nexora_client.py:

    from broadcast_query import BroadcastQueryManager, add_broadcast_arguments
    
    # In main():
    parser = argparse.ArgumentParser(...)
    add_broadcast_arguments(parser)
    args = parser.parse_args()
    
    # Create selector
    selector = ResolverSelector(args.server.split(","))
    
    # Create broadcaster
    broadcast_enabled = args.broadcast_enable and not args.broadcast_disable
    broadcaster = None
    if broadcast_enabled:
        broadcaster = BroadcastQueryManager(
            selector,
            num_parallel=args.broadcast_num_parallel,
            broadcast_timeout=args.broadcast_timeout,
            per_resolver_timeout=args.broadcast_per_resolver_timeout
        )
    
    # Use in query path:
    def query_with_broadcast(payload: bytes, nonce: int, sid: int) -> tuple:
        if broadcaster is None:
            # Fall back to serial
            return _query_txt(selector, port, zone, timeout, payload, attempts, qtype)
        
        def fallback() -> tuple:
            return _query_txt(selector, port, zone, timeout, payload, attempts, qtype)
        
        try:
            return broadcaster.broadcast_query(
                packet=pack_packet(TYPE_STREAM_SEND, sid, nonce, payload),
                expected_nonce=nonce,
                expected_sid=sid,
                zone=zone,
                qtype=qtype,
                port=port,
                fallback_fn=fallback
            )
        except Exception as e:
            log.warning(f"Broadcast query failed: {e}")
            return fallback()
"""
