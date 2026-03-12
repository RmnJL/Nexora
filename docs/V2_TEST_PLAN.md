# Nexora v2 Test Plan

**Status:** Execution plan  
**Date:** 2026-03-12  
**Related:** `docs/V2_PROTOCOL_SPEC.md`, `docs/V2_FLOW_STATE_MACHINE.md`

## 1) Objective

Validate that v2 (carrier + multiplex) is production-ready for multi-user real traffic with stable success rate, bounded latency tails, and safe rollback.

## 2) Test Scope

In scope:
1. Carrier lifecycle stability.
2. Stream multiplex correctness.
3. Reliability logic (`ack`, retransmit, reorder, duplicate handling).
4. Behavior under resolver degradation and packet loss.
5. End-user experience for web and Telegram-like traffic.

Out of scope:
1. Full cryptographic redesign.
2. Non-DNS transport alternatives.

## 3) Environments

1. `dev-local`: functional and deterministic unit/integration tests.
2. `staging-net`: realistic resolver path with controlled traffic generation.
3. `canary-prod`: limited real users (1 -> 5) before full rollout.

## 4) Success Criteria (Go/No-Go)

1. Success rate >= 95% under target load.
2. `sid=None` events reduced by at least 80% vs v1 baseline.
3. `forward timeout` rate reduced by at least 60% vs v1 baseline.
4. p95 latency increase <= 30% vs v1 at equivalent load.
5. No unbounded memory growth over 24h soak.

## 5) Test Categories

## 5.1 Unit Tests

1. Frame parser/packer coverage:
   - valid envelope/frame decode
   - malformed headers
   - payload length mismatch
2. Sequence and ack logic:
   - in-order
   - out-of-order
   - duplicate frames
   - gap detection
3. Retransmit logic:
   - RTO backoff bounds
   - retransmit cancellation on ack
4. Flow control:
   - window respect
   - zero-window pause/resume
5. State transitions:
   - carrier states
   - stream states

## 5.2 Integration Tests

1. IGW <-> OGW handshake and steady data transfer.
2. Multi-stream multiplex on one carrier.
3. Carrier replacement while streams stay alive.
4. Stream close/reset paths with explicit reason codes.
5. v1/v2 feature-flag coexistence.

## 5.3 Fault Injection Tests

1. Packet loss: 1%, 3%, 5%, 10%.
2. Jitter: 50ms, 150ms, 300ms.
3. Burst loss windows (2s, 5s).
4. Resolver hard-fail and cooldown behavior.
5. DNS timeout spikes and recovery.

## 5.4 Load and Soak Tests

1. 1 user, 5 users, 20 users profiles.
2. Mixed workloads:
   - short HTTP requests
   - persistent sessions
   - bursty app traffic
3. Soak durations:
   - 30 minutes
   - 4 hours
   - 24 hours

## 5.5 User-Visible Validation

1. Browser open/login/basic browsing.
2. Telegram connect/send/receive media thumbnail.
3. Session continuity after temporary resolver degradation.

## 6) KPIs and Measurements

Mandatory metrics:
1. `query_total`, `query_success`, `query_fail`.
2. `forward_timeout_total`.
3. `sid_none_open_fail_total`.
4. `carrier_open_total`, `carrier_drop_total`, `carrier_recover_total`.
5. `stream_open_total`, `stream_reset_total`.
6. `retransmit_total`, `duplicate_rx_total`, `reorder_drop_total`.
7. p50/p95/p99 latency.
8. active streams and peak streams.
9. memory and CPU per process.

Sampling:
1. KPI summary every 60s.
2. Per-test snapshots at start, midpoint, end.

## 7) Baseline Comparison Rules

1. Run same workload on v1 tag `v1-backup-2026-03-12`.
2. Run same workload on v2 build.
3. Compare:
   - success rate
   - timeout rate
   - tail latency
   - throughput stability after 30s
4. Reject v2 if any stop condition is triggered.

## 8) Stop Conditions

1. Timeout rate grows by >20% for 10 consecutive minutes.
2. Fail rate grows by >10% for 10 consecutive minutes.
3. Process memory growth remains monotonic for >30 minutes.
4. Any reproducible protocol corruption bug.

## 9) Required Artifacts Per Test Run

1. Config snapshot (all runtime flags/env).
2. Git commit hash and protocol version.
3. KPI logs (`--since` bounded window).
4. Summary table (pass/fail + deltas vs baseline).
5. Known issues list with severity.

## 10) Exit Criteria for Test Phase

1. All unit and integration suites green.
2. Fault-injection matrix completed.
3. Soak test (24h) passes without critical issues.
4. Baseline delta report approved.
5. Rollout plan sign-off complete.

