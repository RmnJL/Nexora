"""
Unit tests for v2 scaffolding primitives.
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from nexora_v2 import (  # noqa: E402
    CarrierManager,
    CarrierState,
    EnvelopeHeader,
    FrameHeader,
    StreamMux,
    pack_envelope_header,
    pack_frame_header,
    unpack_envelope_header,
    unpack_frame_header,
)


class TestV2HeaderRoundtrip(unittest.TestCase):
    def test_envelope_header_roundtrip(self) -> None:
        h = EnvelopeHeader(
            flags=0x03,
            carrier_id=12345,
            epoch=67890,
            frame_count=4,
            reserved=0,
            envelope_len=512,
        )
        raw = pack_envelope_header(h)
        got = unpack_envelope_header(raw)
        self.assertEqual(got, h)

    def test_frame_header_roundtrip(self) -> None:
        h = FrameHeader(
            frame_type=0x20,
            frame_flags=0x01,
            stream_id=77,
            seq=1200,
            ack_base=1189,
            ack_bitmap16=0x00FF,
            window=64,
            payload_len=96,
        )
        raw = pack_frame_header(h)
        got = unpack_frame_header(raw)
        self.assertEqual(got, h)


class TestV2CarrierAndMux(unittest.TestCase):
    def test_carrier_manager_bootstrap(self) -> None:
        mgr = CarrierManager(["1.1.1.1", "8.8.8.8", "9.9.9.9"], max_carriers=2)
        carriers = mgr.bootstrap()
        self.assertEqual(len(carriers), 2)
        self.assertTrue(all(c.state == CarrierState.ESTABLISHED for c in carriers))
        self.assertEqual(len(mgr.snapshot()), 2)

    def test_stream_mux_open_close(self) -> None:
        mux = StreamMux()
        s1 = mux.open_stream()
        s2 = mux.open_stream()
        self.assertEqual(mux.active_count(), 2)
        self.assertEqual(mux.active_stream_ids(), [s1, s2])
        mux.close_stream(s1)
        self.assertEqual(mux.active_count(), 1)
        self.assertEqual(mux.active_stream_ids(), [s2])


if __name__ == "__main__":
    unittest.main()

