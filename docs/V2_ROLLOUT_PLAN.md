# Nexora v2 Rollout Plan

**Status:** Execution plan  
**Date:** 2026-03-12  
**Related:** `docs/V2_TEST_PLAN.md`

## 1) Rollout Strategy

Rollout is phased and reversible. v1 remains available for immediate rollback at all times.

Reference rollback baseline:
1. Tag: `v1-backup-2026-03-12`
2. Protocol flag default remains v1 until Stage-2 acceptance.

## 2) Preconditions

1. `docs/V2_PROTOCOL_SPEC.md` approved.
2. `docs/V2_FLOW_STATE_MACHINE.md` approved.
3. Test plan executed up to staging pass.
4. Feature flag `--protocol-version` is implemented.
5. Runtime metrics and dashboards are available.

## 3) Phases

## 3.1 Stage-0: Dev Validation

Target:
1. Functional validation in controlled environment.

Actions:
1. Run full unit/integration suite.
2. Run synthetic loss/jitter tests.
3. Fix all critical and high defects.

Exit:
1. No critical issues open.
2. Test report signed.

## 3.2 Stage-1: Canary Single User

Target:
1. One real user on real network path.

Actions:
1. Enable v2 for one canary instance only.
2. Keep v1 running on standby instance.
3. Observe at least 60 minutes.

Watch metrics:
1. success rate
2. timeout rate
3. stream reset rate
4. user-visible stalls

Exit:
1. Meets Go criteria for 60 minutes continuous.

## 3.3 Stage-2: Limited Canary (5 users)

Target:
1. Small multi-user validation.

Actions:
1. Expand v2 to up to 5 users.
2. Keep v1 rollback instance hot.
3. Observe 4-24 hours (includes peak period).

Exit:
1. No stop condition triggered.
2. KPIs stay within allowed deltas.

## 3.4 Stage-3: Progressive Production

Target:
1. Controlled expansion to full user base.

Steps:
1. 10% traffic for 2 hours.
2. 25% traffic for 4 hours.
3. 50% traffic for 8 hours.
4. 100% traffic after sign-off.

At each step:
1. Compare KPIs against prior step and v1 baseline.
2. Continue only on explicit go decision.

## 4) Go/No-Go Criteria Per Stage

Go:
1. success rate >= 95%
2. timeout rate does not exceed threshold
3. no memory leak pattern
4. no critical user-facing regressions

No-Go:
1. stop condition hit
2. reproducible protocol corruption
3. sustained user-facing breakage

## 5) Stop Conditions (Immediate Halt)

1. timeout rate +20% over baseline for 10 minutes.
2. fail rate +10% over baseline for 10 minutes.
3. crash loop or resource saturation.
4. rollback path unavailable.

## 6) Rollback Procedure

1. Set protocol flag back to v1.
2. Restart only affected services.
3. Verify health with bounded KPI window.
4. If needed, checkout and deploy backup tag:

```bash
git fetch --tags
git checkout v1-backup-2026-03-12
```

5. Announce rollback complete and freeze rollout.

## 7) Communication Plan

1. Pre-rollout checkpoint message.
2. Stage transition announcements.
3. Incident/rollback notice template.
4. Final rollout completion note.

## 8) Ownership

1. Release Owner: approves stage transition.
2. On-call Operator: executes rollout/rollback commands.
3. QA Owner: validates KPI and user impact.
4. Incident Commander: handles stop-condition events.

## 9) Runbook Checklist (Operator)

Before each stage:
1. Confirm config snapshot.
2. Confirm active commit hash.
3. Confirm dashboard and alert pipeline.
4. Confirm rollback command tested.

During each stage:
1. Monitor KPI every 5 minutes.
2. Capture logs for anomalies.
3. Track user feedback.

After each stage:
1. Write stage summary.
2. Decide go/no-go explicitly.
3. Archive evidence.

## 10) Definition of Rollout Done

1. v2 at 100% traffic for at least 7 days.
2. KPI targets met for the whole window.
3. No unresolved critical issues.
4. Final v2 release tag created and documented.

