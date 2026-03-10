# Nexora Build Roadmap (Phase 1)

Signature: Rmn JL

## P1 - Minimal Working Tunnel

1. UDP DNS listener loop (server)
2. DNS packet parser/encoder (minimal supported types)
3. Session init/accept packets
4. Single stream data + ACK + resend
5. Local SOCKS5 listener on client

## P1 Exit Criteria

- Client establishes session through DNS.
- One TCP stream can pass end-to-end.
- Basic retransmission works under packet loss.

## Current Progress

- Phase 1 done: HELLO/HELLO_ACK.
- Phase 2 done: DATA/DATA_ACK.
- Phase 3 MVP done: STREAM_OPEN, STREAM_SEND, STREAM_RECV, STREAM_CLOSE.
- Phase 3.2 done: downstream queue with sequence chunks and duplicate-safe replay cache.
- Phase 4 MVP done: reverse local TCP forward mode (inside listen -> outside target over DNS tunnel).
