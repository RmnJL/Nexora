# Nexora Architecture (Draft v0)

## Ownership

- Signature: Rmn JL

## Components

- `nexora-server`: authoritative DNS tunnel endpoint on UDP/53
- `nexora-client`: local proxy endpoint (SOCKS5) + DNS transport
- `nexora-proto`: wire protocol and framing rules

## Product Direction (Recorded Requirement)

- Target use-case: run an X-UI/VLESS service on the outside server and let users reach it through the Nexora DNS tunnel path instead of direct outside-IP exposure.
- Owner signature requirement: Rmn JL

## Data Path

1. App connects to local SOCKS5 on client.
2. Client maps TCP flows to multiplexed stream IDs.
3. Stream chunks are encrypted and packed into DNS queries.
4. Server reconstructs chunks, forwards traffic, and returns responses in DNS answers.

## Protocol Goals

- Reliable transfer over lossy DNS paths.
- Stream multiplexing on a single session.
- Per-resolver adaptive MTU.
- Optional duplication/FEC for severe packet loss.

## Security Baseline

- Key exchange: X25519
- Payload AEAD: XChaCha20-Poly1305
- Replay protection: per-session nonce window
