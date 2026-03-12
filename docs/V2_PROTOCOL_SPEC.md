# Nexora v2 Protocol Specification

**Status:** Draft for implementation kickoff  
**Date:** 2026-03-12  
**Scope:** Application protocol between inside gateway (`nexora-client-forward`) and outside gateway (`nexora-server`) over DNS transport on UDP/53.

## 1) Goals

1. Provide stable multi-user service under resolver instability.
2. Remove per-connection handshake storms by using persistent carrier sessions.
3. Multiplex many user streams over a small number of long-lived carriers.
4. Add protocol-level reliability (`ack + retransmit`) and optional FEC.
5. Keep backward-compatibility path to v1 for rollback.

## 2) Non-goals

1. Perfect throughput for large bulk downloads over weak resolver networks.
2. Cryptographic privacy layer redesign in this phase.
3. Replacing DNS transport in v2.0.

## 3) Terminology

1. `IGW`: Inside gateway, user-facing TCP listener.
2. `OGW`: Outside gateway, authoritative DNS backend with target TCP access.
3. `Carrier`: Long-lived virtual channel between IGW and OGW.
4. `Stream`: One logical proxied TCP flow multiplexed over a carrier.
5. `Envelope`: One DNS payload unit containing one or more protocol frames.

## 4) Transport Model

1. DNS remains the only network transport (UDP/53 path).
2. v2 payload is embedded inside existing DNS encoding (`base32` labels).
3. One DNS query/response exchanges exactly one `Envelope`.
4. Envelope may contain multiple frames (`frame_count >= 1`).
5. IGW keeps 1-3 carriers alive and maps all user streams onto them.

## 5) Wire Layout

## 5.1 Envelope Header (fixed 16 bytes)

```text
0..1   magic           = 0x4E58 ("NX")
2      version         = 0x02
3      flags           (bitfield)
4..7   carrier_id      uint32
8..11  epoch           uint32
12     frame_count     uint8
13     reserved        uint8
14..15 envelope_len    uint16  (bytes after this header)
```

`flags`:
1. bit0: `ACK_ONLY` (no user data in envelope)
2. bit1: `FEC_PRESENT`
3. bit2: `CONTROL_ONLY`
4. bit3..7: reserved

## 5.2 Frame Header (fixed 20 bytes)

```text
0      frame_type      uint8
1      frame_flags     uint8
2..5   stream_id       uint32  (0 means carrier-control frame)
6..9   seq             uint32
10..13 ack_base        uint32
14..15 ack_bitmap16    uint16  (acks for ack_base+1 .. ack_base+16)
16..17 window          uint16  (receiver advertised credits)
18..19 payload_len     uint16
```

Immediately followed by `payload_len` bytes.

## 5.3 Frame Types

Carrier control (`stream_id=0`):
1. `0x01 C_HELLO`
2. `0x02 C_HELLO_ACK`
3. `0x03 C_PING`
4. `0x04 C_PONG`
5. `0x05 C_GOAWAY`

Stream control:
1. `0x10 S_OPEN`
2. `0x11 S_OPEN_ACK`
3. `0x12 S_CLOSE`
4. `0x13 S_RESET`

Stream data:
1. `0x20 S_DATA`
2. `0x21 S_DATA_ACK`
3. `0x22 S_RETX_HINT`
4. `0x23 S_WINDOW_UPDATE`

FEC:
1. `0x30 S_PARITY`

## 6) Reliability Model

1. `seq` is monotonic per `stream_id`.
2. Receiver acknowledges progress using `(ack_base, ack_bitmap16)`.
3. Sender keeps a retransmit buffer for unacked frames.
4. RTO is adaptive per carrier:
   - initial `600ms`
   - min `250ms`
   - max `4s`
5. If frame not acked before RTO expiry, retransmit with same `seq`.
6. Duplicate `seq` must be accepted idempotently and not re-applied.
7. Out-of-order frames are buffered up to `reorder_window` limit.

## 7) Flow Control

1. `window` advertises receiver credits in units of frames.
2. Sender must not exceed advertised credits per stream.
3. Global per-carrier inflight cap is mandatory to prevent storms.
4. On zero window, sender sends only control + keepalive frames.

## 8) Optional FEC (v2.1-ready, v2.0 optional)

1. For every `N=4` `S_DATA` frames, sender may emit one `S_PARITY`.
2. Receiver can reconstruct one missing frame per block.
3. FEC usage is adaptive and disabled under low loss.

## 9) Carrier Lifecycle

1. IGW selects resolver cohort and sends `C_HELLO`.
2. OGW responds `C_HELLO_ACK` with negotiated limits.
3. Carrier enters established mode; streams can open.
4. Keepalive:
   - ping every `15s` idle
   - close carrier after `3` missed pong cycles
5. On severe degradation, IGW opens replacement carrier before teardown.

## 10) Stream Lifecycle

1. IGW sends `S_OPEN` with target metadata.
2. OGW returns `S_OPEN_ACK(ok|error)`.
3. Bidirectional `S_DATA` exchange starts.
4. Normal close via `S_CLOSE` handshake.
5. Abnormal conditions use `S_RESET` with reason code.

## 11) Resolver Cohort Policy

1. Each carrier binds to a small stable resolver cohort (2-4 resolvers).
2. Resolver switch is cohort-level, not per-frame random churn.
3. Hard-fail resolver is quarantined with cooldown.
4. Cohort re-selection is rate-limited to avoid oscillation.

## 12) Backward Compatibility

1. `--protocol-version {1|2}` feature flag at IGW and OGW.
2. v1 and v2 code paths coexist during migration.
3. Emergency rollback must be one-command switch to v1.

## 13) Limits and Defaults (initial)

1. Max carriers per IGW process: `3`
2. Max active streams per carrier: `64`
3. Max frame payload target: `96` bytes (fit DNS budget safely)
4. Retransmit buffer per stream: `128` frames
5. Reorder buffer per stream: `64` frames

## 14) Error Codes

`S_OPEN_ACK` / `S_RESET` reason examples:
1. `0x01 TARGET_CONNECT_FAIL`
2. `0x02 TARGET_TIMEOUT`
3. `0x03 FLOW_CONTROL_VIOLATION`
4. `0x04 PROTOCOL_ERROR`
5. `0x05 RESOURCE_LIMIT`

## 15) Metrics Requirements

Mandatory counters/histograms:
1. `carrier_open_total`, `carrier_recover_total`, `carrier_drop_total`
2. `stream_open_total`, `stream_reset_total`, `stream_close_total`
3. `frame_retx_total`, `frame_dup_rx_total`, `reorder_drop_total`
4. `carrier_rtt_ms` p50/p95/p99
5. `resolver_cohort_switch_total`

## 16) Acceptance Criteria for this spec

1. IGW and OGW engineers can implement independently from this document.
2. No undefined behavior remains for open/data/close/reset/retransmit.
3. All frame types include clear ownership and retry semantics.

