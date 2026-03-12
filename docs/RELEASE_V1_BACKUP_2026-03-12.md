# RELEASE NOTE: v1 Backup Before v2

**Date:** 2026-03-12  
**Release Type:** Backup Baseline (pre-v2)  
**Tag:** `v1-backup-2026-03-12`

## Summary

This release freezes the current v1 implementation as a rollback-safe baseline before starting Nexora v2.

## Included

- Broadcast and resolver-selection stability improvements from recent commits.
- Stream wait-budget fix in forward path.
- Resolver scanner/client alignment updates.
- Master v2 execution checklist:
  - [`V2_MASTER_CHECKLIST_FA.md`](/V2_MASTER_CHECKLIST_FA.md)

## Known Limitations (v1)

- Not production-stable for multi-user sustained traffic.
- Intermittent `sid=None` and `forward timeout` under resolver degradation.
- Throughput can stall after initial fast bursts.

## Rollback Reference

```bash
git fetch --tags
git checkout v1-backup-2026-03-12
```

## Next Step

Start v2 implementation based on multiplexed carrier-session architecture and phased rollout plan.

