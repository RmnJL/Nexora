"""
Unit tests for nexora_proto module.
"""

import sys
import os
import struct
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from nexora_proto import (
    FLAG_RETRY_COUNT,
    MAGIC,
    TYPE_DATA,
    TYPE_HELLO,
    TYPE_HELLO_ACK,
    TYPE_STREAM_SEND,
    Packet,
    pack_packet,
    unpack_packet,
    encode_dns_data,
    decode_dns_data,
    random_nonce,
)


class TestPackPacket(unittest.TestCase):
    def test_roundtrip(self):
        payload = b"hello world"
        raw = pack_packet(TYPE_HELLO, 42, 999, payload)
        pkt = unpack_packet(raw)
        self.assertEqual(pkt.msg_type, TYPE_HELLO)
        self.assertEqual(pkt.session_id, 42)
        self.assertEqual(pkt.nonce, 999)
        self.assertEqual(pkt.payload, payload)
        self.assertEqual(pkt.retry_count, 0)
        self.assertEqual(pkt.flags, 0)

    def test_empty_payload(self):
        raw = pack_packet(TYPE_DATA, 1, 0, b"")
        pkt = unpack_packet(raw)
        self.assertEqual(pkt.payload, b"")

    def test_bad_magic(self):
        raw = pack_packet(TYPE_HELLO, 1, 1, b"x")
        bad = b"XXXX" + raw[4:]
        with self.assertRaises(ValueError):
            unpack_packet(bad)

    def test_truncated_packet(self):
        raw = pack_packet(TYPE_HELLO, 1, 1, b"data")
        with self.assertRaises(ValueError):
            unpack_packet(raw[:5])

    def test_bad_payload_length(self):
        raw = pack_packet(TYPE_HELLO, 1, 1, b"data")
        # Append extra bytes to make payload length mismatch.
        with self.assertRaises(ValueError):
            unpack_packet(raw + b"extra")

    def test_retry_metadata_roundtrip_for_data(self):
        raw = pack_packet(TYPE_DATA, 7, 55, b"abc", retry_count=9)
        pkt = unpack_packet(raw)
        self.assertEqual(pkt.msg_type, TYPE_DATA)
        self.assertEqual(pkt.payload, b"abc")
        self.assertEqual(pkt.retry_count, 9)
        self.assertEqual(pkt.flags & FLAG_RETRY_COUNT, FLAG_RETRY_COUNT)

    def test_stream_send_includes_retry_field_even_at_zero(self):
        raw = pack_packet(TYPE_STREAM_SEND, 3, 4, b"x")
        pkt = unpack_packet(raw)
        self.assertEqual(pkt.msg_type, TYPE_STREAM_SEND)
        self.assertEqual(pkt.payload, b"x")
        self.assertEqual(pkt.retry_count, 0)
        self.assertEqual(pkt.flags & FLAG_RETRY_COUNT, FLAG_RETRY_COUNT)

    def test_header_crc8_rejects_corruption(self):
        raw = bytearray(pack_packet(TYPE_HELLO, 1, 2, b"ok"))
        raw[15] ^= 0x01
        with self.assertRaises(ValueError):
            unpack_packet(bytes(raw))

    def test_legacy_v1_packet_is_still_accepted(self):
        payload = b"legacy"
        hdr_v1 = struct.pack(">4sBIIH", MAGIC, TYPE_HELLO_ACK, 21, 88, len(payload))
        raw = hdr_v1 + payload
        pkt = unpack_packet(raw)
        self.assertEqual(pkt.msg_type, TYPE_HELLO_ACK)
        self.assertEqual(pkt.session_id, 21)
        self.assertEqual(pkt.nonce, 88)
        self.assertEqual(pkt.payload, payload)
        self.assertEqual(pkt.retry_count, 0)


class TestDnsDataEncoding(unittest.TestCase):
    def test_roundtrip(self):
        data = b"binary\x00\xff\x01data"
        encoded = encode_dns_data(data)
        decoded = decode_dns_data(encoded)
        self.assertEqual(decoded, data)

    def test_empty(self):
        encoded = encode_dns_data(b"")
        decoded = decode_dns_data(encoded)
        self.assertEqual(decoded, b"")

    def test_encoded_is_lowercase_ascii(self):
        encoded = encode_dns_data(b"test data 123")
        self.assertTrue(encoded.isascii())
        self.assertEqual(encoded, encoded.lower())
        self.assertNotIn("=", encoded)

    def test_dots_ignored_in_decode(self):
        data = b"some binary data"
        encoded = encode_dns_data(data)
        # Insert dots as if split into DNS labels.
        with_dots = ".".join(encoded[i:i+5] for i in range(0, len(encoded), 5))
        decoded = decode_dns_data(with_dots)
        self.assertEqual(decoded, data)


class TestRandomNonce(unittest.TestCase):
    def test_is_32bit(self):
        for _ in range(100):
            n = random_nonce()
            self.assertGreaterEqual(n, 0)
            self.assertLess(n, 2**32)

    def test_not_all_same(self):
        nonces = {random_nonce() for _ in range(20)}
        self.assertGreater(len(nonces), 1)


if __name__ == "__main__":
    unittest.main()
