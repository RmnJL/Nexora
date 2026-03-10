"""
Unit tests for dns_wire module.
"""

import sys
import os
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from dns_wire import (
    TYPE_TXT,
    TYPE_A,
    TYPE_CNAME,
    build_query,
    parse_query,
    build_txt_answer,
    build_cname_answer,
    build_servfail,
    parse_answer_data,
    _encode_name,
    _decode_name,
)


class TestEncodeName(unittest.TestCase):
    def test_simple(self):
        result = _encode_name("example.com")
        # 7 e x a m p l e 3 c o m 0
        self.assertEqual(result, b"\x07example\x03com\x00")

    def test_subdomain(self):
        result = _encode_name("sub.example.com")
        self.assertEqual(result, b"\x03sub\x07example\x03com\x00")

    def test_trailing_dot(self):
        result = _encode_name("example.com.")
        self.assertEqual(result, b"\x07example\x03com\x00")

    def test_label_too_long(self):
        with self.assertRaises(ValueError):
            _encode_name("a" * 64 + ".com")


class TestDecodeName(unittest.TestCase):
    def test_simple(self):
        encoded = b"\x07example\x03com\x00"
        name, offset = _decode_name(encoded, 0)
        self.assertEqual(name, "example.com")
        self.assertEqual(offset, len(encoded))

    def test_with_prefix(self):
        prefix = b"\x00\x00"
        encoded = prefix + b"\x03foo\x03bar\x00"
        name, offset = _decode_name(encoded, 2)
        self.assertEqual(name, "foo.bar")

    def test_compression_pointer(self):
        # "\x07example\x03com\x00" at offset 0
        # Then a pointer to offset 0
        packet = b"\x07example\x03com\x00\xc0\x00"
        name, offset = _decode_name(packet, len(packet) - 2)
        self.assertEqual(name, "example.com")

    def test_pointer_loop_detected(self):
        # Two pointers that point to each other.
        packet = b"\xc0\x02\xc0\x00"
        with self.assertRaises(ValueError):
            _decode_name(packet, 0)


class TestBuildParseQuery(unittest.TestCase):
    def test_roundtrip(self):
        qid, raw = build_query("test.example.com", qtype=TYPE_TXT)
        parsed_qid, name, qtype = parse_query(raw)
        self.assertEqual(parsed_qid, qid)
        self.assertEqual(name, "test.example.com")
        self.assertEqual(qtype, TYPE_TXT)

    def test_a_query(self):
        qid, raw = build_query("host.test.net", qtype=TYPE_A)
        parsed_qid, name, qtype = parse_query(raw)
        self.assertEqual(qtype, TYPE_A)
        self.assertEqual(name, "host.test.net")


class TestBuildTxtAnswer(unittest.TestCase):
    def test_roundtrip(self):
        qid, query = build_query("test.example.com", qtype=TYPE_TXT)
        txt = "hello world"
        answer = build_txt_answer(query, txt, ttl=0)
        parsed = parse_answer_data(answer, qid)
        self.assertEqual(parsed, txt)


class TestBuildCnameAnswer(unittest.TestCase):
    def test_roundtrip(self):
        qid, query = build_query("test.example.com", qtype=TYPE_CNAME)
        cname = "other.example.com"
        answer = build_cname_answer(query, cname, ttl=0)
        parsed = parse_answer_data(answer, qid)
        self.assertEqual(parsed, cname)


class TestBuildServfail(unittest.TestCase):
    def test_returns_bytes(self):
        _, query = build_query("test.example.com")
        sf = build_servfail(query)
        self.assertIsInstance(sf, bytes)
        self.assertGreater(len(sf), 12)


class TestParseAnswerEdgeCases(unittest.TestCase):
    def test_wrong_qid(self):
        qid, query = build_query("test.example.com", qtype=TYPE_TXT)
        answer = build_txt_answer(query, "data", ttl=0)
        with self.assertRaises(ValueError):
            parse_answer_data(answer, qid + 1)

    def test_short_packet(self):
        with self.assertRaises(ValueError):
            parse_answer_data(b"\x00" * 5, 0)


if __name__ == "__main__":
    unittest.main()
