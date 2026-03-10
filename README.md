# Nexora

Nexora is an experimental DNS transport research project.

Author and Maintainer: **Rmn JL**

## Important Purpose Statement

Nexora is built **for scientific and engineering testing** of network behavior under constrained UDP/DNS conditions (loss, MTU pressure, retransmission behavior, and protocol resilience).

It is **not** published as an "internet bypass tool".

## Legal and Compliance Notice

By using this repository, you agree to:

- comply with all applicable local, national, and international laws
- comply with ISP, hosting provider, and network operator terms
- only test on infrastructure and traffic you are authorized to control
- avoid any unauthorized access, abuse, or disruption

You are fully responsible for how you run this software.

## Project Scope

Current implementation is an early protocol prototype with:

- session handshake: `HELLO -> HELLO_ACK`
- basic data exchange: `DATA -> DATA_ACK`
- minimal DNS wire encode/decode path for TXT payloads

Core files:

- `src/nexora_server.py`
- `src/nexora_client.py`
- `src/nexora_proto.py`
- `src/dns_wire.py`

## Quick Local Test

```powershell
cd src
powershell -ExecutionPolicy Bypass -File .\smoke_test_local.ps1
```

Expected output:

- `handshake ok`
- `data ack ok`

## Status

This is a research-stage codebase, not a production system.
