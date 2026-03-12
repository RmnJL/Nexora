# Nexora v2 Flow and State Machine

**Status:** Draft for implementation kickoff  
**Date:** 2026-03-12  
**Related:** `docs/V2_PROTOCOL_SPEC.md`

## 1) Runtime Components

1. `InsideGateway` (IGW): accepts user TCP connections on inside host.
2. `CarrierManager`: owns carrier lifecycle and resolver cohorts.
3. `StreamMux`: maps user socket <-> `stream_id` on carriers.
4. `OutsideGateway` (OGW): terminates carrier protocol and manages target sockets.
5. `TargetConnector`: OGW module that opens TCP sessions to final target service.

## 2) High-Level Flow

```text
User TCP -> IGW Listener -> StreamMux -> Carrier Envelope -> DNS -> OGW
OGW -> TargetConnector -> target TCP service
target response -> OGW -> Carrier Envelope -> DNS -> IGW -> User TCP
```

## 3) Carrier State Machine

States:
1. `C_INIT`
2. `C_SELECTING_RESOLVERS`
3. `C_OPENING`
4. `C_ESTABLISHED`
5. `C_DEGRADED`
6. `C_RECOVERING`
7. `C_DRAINING`
8. `C_CLOSED`

Transitions:
1. `C_INIT -> C_SELECTING_RESOLVERS`
   - trigger: process start or explicit rebuild request.
2. `C_SELECTING_RESOLVERS -> C_OPENING`
   - trigger: cohort chosen and initial probe success.
3. `C_OPENING -> C_ESTABLISHED`
   - trigger: `C_HELLO_ACK` received in deadline.
4. `C_OPENING -> C_RECOVERING`
   - trigger: open timeout or protocol reject.
5. `C_ESTABLISHED -> C_DEGRADED`
   - trigger: RTT/fail thresholds exceeded.
6. `C_DEGRADED -> C_RECOVERING`
   - trigger: consecutive envelope failures beyond threshold.
7. `C_RECOVERING -> C_ESTABLISHED`
   - trigger: replacement carrier opened successfully.
8. `C_RECOVERING -> C_CLOSED`
   - trigger: retry budget exhausted.
9. `C_ESTABLISHED -> C_DRAINING`
   - trigger: planned rotation or shutdown.
10. `C_DRAINING -> C_CLOSED`
    - trigger: all streams closed or drain timeout.

## 4) Stream State Machine

States:
1. `S_IDLE`
2. `S_OPEN_SENT`
3. `S_OPEN`
4. `S_HALF_CLOSED_LOCAL`
5. `S_HALF_CLOSED_REMOTE`
6. `S_RESET`
7. `S_CLOSED`

Transitions:
1. `S_IDLE -> S_OPEN_SENT`
   - trigger: new user socket accepted and carrier available.
2. `S_OPEN_SENT -> S_OPEN`
   - trigger: `S_OPEN_ACK(ok)`.
3. `S_OPEN_SENT -> S_RESET`
   - trigger: `S_OPEN_ACK(error)` or open timeout.
4. `S_OPEN -> S_HALF_CLOSED_LOCAL`
   - trigger: user socket EOF (local close intent).
5. `S_OPEN -> S_HALF_CLOSED_REMOTE`
   - trigger: OGW signals close on remote side.
6. `S_OPEN -> S_RESET`
   - trigger: protocol error, flow violation, target failure.
7. `S_HALF_CLOSED_LOCAL -> S_CLOSED`
   - trigger: remote close arrives and pending data flushed.
8. `S_HALF_CLOSED_REMOTE -> S_CLOSED`
   - trigger: local close arrives and pending data flushed.
9. `S_RESET -> S_CLOSED`
   - trigger: cleanup complete.

## 5) Timers

Carrier timers:
1. `T_C_OPEN = 3.0s`
2. `T_C_PING = 15s`
3. `T_C_PONG_DEADLINE = 5s`
4. `T_C_DRAIN = 20s`

Stream timers:
1. `T_S_OPEN = 4.0s`
2. `T_S_IDLE = 75s`
3. `T_S_ZOMBIE = 90s` (no downstream progress)

Reliability timers:
1. `RTO_INIT = 600ms`
2. `RTO_MIN = 250ms`
3. `RTO_MAX = 4s`

## 6) Failure Escalation Policy

1. Envelope failure increments carrier fail score.
2. Soft threshold:
   - move `C_ESTABLISHED -> C_DEGRADED`.
3. Hard threshold:
   - open replacement carrier in parallel.
   - route new streams to replacement.
   - old carrier enters `C_DRAINING`.
4. If no healthy carrier:
   - reject new streams with backpressure signal.
   - keep retry loop bounded, no infinite storm.

## 7) Scheduling and Fairness

1. Control frames always have priority over data.
2. Data scheduler is weighted round-robin across streams.
3. One noisy stream must not starve others.
4. Retransmissions are prioritized before fresh data for same stream.

## 8) Reorder and Duplicate Handling

1. Per-stream expected `next_seq` is tracked.
2. `seq < next_seq`:
   - treat as duplicate, ack only.
3. `seq > next_seq`:
   - buffer in reorder map if within window.
4. Gaps beyond reorder window:
   - emit `S_RETX_HINT`.
   - optionally drop oldest buffered out-of-order entries.

## 9) Backpressure Model

1. Receiver updates `window` based on free buffer slots.
2. Sender pauses new `S_DATA` when `window=0`.
3. User socket read on IGW is throttled when stream tx queue is full.
4. OGW target read is throttled when upstream credits are exhausted.

## 10) Operational Guardrails

1. Max carriers per process is hard-capped.
2. Max streams per carrier is hard-capped.
3. Per-stream and global memory caps are enforced.
4. Every cap breach emits explicit metric and reason code.

## 11) Observability Mapping

Carrier-level:
1. state transitions with reason.
2. RTT, retransmit rate, resolver cohort id.

Stream-level:
1. open latency.
2. close reason.
3. bytes up/down.
4. reset reason code.

## 12) Boot Sequence

1. Start process.
2. Build resolver cohort.
3. Open primary carrier.
4. Verify keepalive loop.
5. Enable user listener only after carrier ready.

## 13) Shutdown Sequence

1. Stop accepting new user sockets.
2. Mark carriers `C_DRAINING`.
3. Flush/close active streams with timeout.
4. Send `C_GOAWAY`.
5. Final cleanup to `C_CLOSED`.

## 14) v1/v2 Coexistence Rule

1. Runtime flag decides protocol path per process.
2. Rollback from v2 to v1 must not require code rollback.
3. Shared metrics namespace must distinguish `protocol_version`.

## 15) Acceptance Criteria for this document

1. Every component has explicit states and transitions.
2. Timers and thresholds are concrete, not implicit.
3. Failure behavior is bounded and deterministic.

