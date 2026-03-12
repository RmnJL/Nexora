# Nexora Stability TODO Checklist

Source document: `STABILITY_ENHANCEMENT_PROPOSAL.md`
Last sync: 2026-03-12

## How to use this file
- Use `[x]` only when code/config is merged and running on target host.
- Keep each task small and atomic. If a task is too big, split it.
- For every completed task, add one line in `Work Log` with date and evidence.

---

## 0) Baseline and evidence
- [x] Collect real failure logs from `nexora-client-forward`.
- [x] Confirm dominant failure types from logs (TIMEOUT, NXDOMAIN, SERVFAIL, no answer).
- [ ] Save a baseline KPI snapshot before each tuning round.
- [ ] Save a post-change KPI snapshot after each tuning round.
- [ ] Track improvement delta per round (timeout rate, fallback success, reject rate).

---

## 1) Layer 1 - Sticky resolver with health-aware failover

### 1.1 ResolverSelector core behavior
- [x] Keep one active resolver as sticky primary while healthy.
- [x] Avoid random resolver churn between attempts.
- [x] Keep active resolver unless it becomes unusable/blacklisted.
- [x] Maintain per-resolver in-flight guard.
- [x] Keep per-resolver latency EWMA.
- [x] Keep per-resolver success EWMA.

### 1.2 Failure handling policy
- [x] Add soft failure streak counter per resolver.
- [x] Add configurable soft-failure threshold before blacklist.
- [x] Blacklist timeout/no-answer only after streak threshold.
- [x] Keep hard failure handling for NXDOMAIN/SERVFAIL/REFUSED.
- [x] Reset soft-failure streak on success.
- [x] Clear/reset resolver state when resolver list updates.

### 1.3 Config and flags
- [x] Add `--resolver-fail-streak` CLI flag.
- [x] Wire `--resolver-fail-streak` into `ResolverSelector`.

### 1.4 Validation
- [x] Test: sticky resolver stays active when healthy.
- [x] Test: no switch before threshold.
- [x] Test: switch occurs after threshold reached.
- [x] Test: immediate switch on hard failure (NXDOMAIN).

---

## 2) Layer 2 - Conservative backoff with jitter

### 2.1 Retry timing
- [x] Replace fixed retry delay pattern with exponential backoff.
- [x] Add jitter to retry delay to avoid synchronized retry bursts.
- [x] Cap backoff to avoid unbounded wait.
- [x] Skip delay for hard-dead resolver outcomes where useful.

### 2.2 Retry accounting
- [x] Track actual attempts made for accurate final errors.
- [x] Include attempts used in final timeout exception message.
- [ ] Emit structured retry-delay metric/log field for observability.

### 2.3 Validation
- [x] Test: query retries primary resolver before failover.
- [ ] Test: jitter/backoff timing window under load simulation.

---

## 3) Gap-closure feature - Last-chance fallback (implemented)

### 3.1 Behavior
- [x] Add final-attempt backup resolver fallback.
- [x] Keep primary flow unchanged before final attempt.
- [x] Log fallback success event.
- [x] Log fallback failure event.

### 3.2 Config and tests
- [x] Add `--resolver-last-chance-fallback` CLI flag.
- [x] Add `--no-resolver-last-chance-fallback` CLI flag.
- [x] Test: fallback uses backup when final primary fails.

---

## 4) Protocol metadata (proposal item: `src/nexora_proto.py`)
- [ ] Define retry metadata fields in packet format.
- [ ] Add backward-compatible protocol version/feature guard.
- [ ] Update client pack/unpack to include retry metadata.
- [ ] Update server handling to parse/use metadata.
- [ ] Add tests for protocol metadata compatibility.

---

## 5) Layer 3 - Dual-path parallel delivery (optional)
- [ ] Define activation policy (always vs critical-only).
- [ ] Create `DualPathQueryManager`.
- [ ] Choose primary and secondary resolvers deterministically.
- [ ] Dispatch parallel queries with shared timeout budget.
- [ ] Return first valid response and cancel loser.
- [ ] Protect against duplicate side effects.
- [ ] Add per-query traffic guard to control 2x overhead.
- [ ] Add unit tests for dual-path success/failure/cancel path.
- [ ] Add integration test under degraded resolver pool.

---

## 6) Layer 4 - Negative cache
- [ ] Create `NegativeAnswerCache`.
- [ ] Key cache by `(resolver, query_hash)`.
- [ ] Add TTL expiration cleanup path.
- [ ] Cache NXDOMAIN short-term.
- [ ] Cache SERVFAIL short-term.
- [ ] Read cache before retrying same resolver/query pair.
- [ ] Add CLI/env knobs for negative cache TTL.
- [ ] Add tests: cache hit, cache miss, TTL expiry, stale invalidation.

---

## 7) Layer 5 - FEC (optional, long-term)
- [ ] Define FEC scope (which streams / which packets).
- [ ] Design packet overhead budget and framing.
- [ ] Implement encoder (fountain/Reed-Solomon decision).
- [ ] Implement decoder and recovery thresholds.
- [ ] Add per-stream FEC enable policy.
- [ ] Add compatibility guard with non-FEC peers.
- [ ] Add performance benchmark (CPU/memory).
- [ ] Add reliability benchmark under 20-30% loss.

---

## 8) Observability and metrics
- [ ] Add `success_rate_per_resolver`.
- [ ] Add `failure_type_histogram` (NXDOMAIN/SERVFAIL/TIMEOUT/NOANSWER).
- [ ] Add `retry_count_distribution`.
- [ ] Add query latency percentiles (p50/p95/p99).
- [ ] Add `resolver_switch_count`.
- [ ] Add `fallback_success_count` and `fallback_fail_count`.
- [ ] Add `data_loss_incidents`.
- [ ] Add `active_stream_count`.
- [ ] Add periodic summary log line for KPI export.

---

## 9) Testing strategy from proposal

### 9.1 Synthetic failure scenarios
- [ ] Scenario 1: single resolver hard failure, verify failover <500ms.
- [ ] Scenario 2: inject 20% DNS response loss, measure recovery time.
- [ ] Scenario 3: staged timeout cascade (500ms -> 1s -> 2s), verify no retry storm.
- [ ] Scenario 4: concurrent stream spike, capture throughput and p99 latency.

### 9.2 Test automation
- [ ] Add repeatable scripts for synthetic DNS failure injection.
- [ ] Add CI target for resolver stability tests.
- [ ] Capture artifacts (logs + KPIs) per test run.

---

## 10) Deployment checklist
- [ ] Code review assigned and approved.
- [ ] Unit tests pass locally.
- [ ] Integration tests pass on staging.
- [ ] Load test with synthetic DNS failures pass.
- [ ] Canary rollout (10% traffic) completed.
- [ ] One-week canary monitoring completed.
- [ ] Full rollout completed.
- [ ] Post-deployment review completed.

---

## 11) Runtime tuning tasks (inside server)
- [x] Confirm `EnvironmentFile` path and behavior for client service.
- [x] Create `/etc/default/nexora-client-forward` when missing.
- [x] Tune resolver file filters (`min_pass_rate`, `max_latency`, `min_score`).
- [x] Enforce scanner compatibility gates from resolver rows (`random_subdomain`, `tunnel_realistic`, `nxdomain_correct`, `bidirectional`).
- [x] Enforce resolver-row freshness (`runtime_last_probe_ts`) and low consecutive-failure threshold.
- [x] Enforce allowed scanner pools (default: `active,standby`) when loading resolver rows.
- [ ] Tune attempts and attempt cap for lower stall time.
- [ ] Tune max connections to reduce `reason=max_conns`.
- [ ] Tune idle/poll intervals for better stream continuity.
- [ ] Re-run KPI extraction after each tuning change.

---

## 12) Session 2026-03-12 - Tier A Reliability Sprint (execution + result)
- [x] A1.1 Protocol v2 header with CRC8 implemented in `src/nexora_proto.py`. Result: PASS (`tests/test_nexora_proto.py::test_header_crc8_rejects_corruption`).
- [x] A1.2 Backward compatibility decode for legacy v1 packets retained. Result: PASS (`tests/test_nexora_proto.py::test_legacy_v1_packet_is_still_accepted`).
- [x] A2.1 Retry metadata field added (retry_count) for retry-aware packets. Result: PASS (`tests/test_nexora_proto.py::test_retry_metadata_roundtrip_for_data`).
- [x] A2.2 Client injects retry_count per wire attempt in `_query_txt`. Result: PASS (`tests/test_resolver_selector.py::test_retry_metadata_increments_across_retries`).
- [x] A2.3 Server logs retry metadata and keeps nonce-based dedup cache path. Result: PASS (regression tests green, no protocol break).
- [x] A3.1 Final-attempt parallel fallback (primary+backup) added in client query path. Result: PASS (`tests/test_resolver_selector.py::test_last_chance_fallback_uses_backup_on_final_failure`).
- [x] A3.2 Retry backoff retuned with lower cap and jitter preserved. Result: PASS (all unit tests green, no retry regression detected).
- [x] A4.1 Active health loop upgraded with proactive switch when active is clearly worse. Result: PASS (selector tests + full suite green).
- [x] A4.2 Resolver floor guard kept (`_ensure_min_resolver_count`) to avoid runtime resolver collapse. Result: PASS (`tests/test_resolver_file_filter.py` cases).
- [x] Session code validation completed. Result: PASS (`pytest -q` => `48 passed`).
- [ ] Session runtime validation on real server (2min/5min KPIs) pending. Result: N/A (awaiting deployment).

## 13) Work Log (smallest actions)
- [x] 2026-03-12: Added sticky resolver policy and failure-streak threshold in `src/nexora_client.py`.
- [x] 2026-03-12: Added final-attempt backup resolver fallback and logs in `src/nexora_client.py`.
- [x] 2026-03-12: Added CLI flags `--resolver-fail-streak` and last-chance fallback toggle.
- [x] 2026-03-12: Added selector/fallback tests in `tests/test_resolver_selector.py`.
- [x] 2026-03-12: Verified service env file is optional (`EnvironmentFile=-/etc/default/nexora-client-forward`).
- [x] 2026-03-12: Added strict resolver-file filtering (compatibility flags + staleness + pool + consecutive failure) in `src/nexora_client.py`.
- [x] 2026-03-12: Added resolver-file filter tests in `tests/test_resolver_file_filter.py` (now `40 passed`).
- [x] 2026-03-12: Added explicit resolver-file strict-filter env/args wiring in `deploy/systemd/nexora-client-forward.service`.
- [x] 2026-03-12: Enabled fail-fast behavior when all alternates are blacklisted (avoid retrying known-bad resolvers); tests now `41 passed`.
- [x] 2026-03-12: Implemented protocol v2 CRC8 header + legacy v1 compatibility in `src/nexora_proto.py`; tests updated.
- [x] 2026-03-12: Added retry metadata injection per-attempt in `_query_txt` and retry visibility in `src/nexora_server.py`.
- [x] 2026-03-12: Added final-attempt parallel fallback (`--resolver-parallel-fallback`) and proactive health switching; full tests `48 passed`.
- [ ] YYYY-MM-DD: (next tiny task) ...
