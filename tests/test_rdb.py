import io
import unittest

from miniredis.engine import Entry
from miniredis.hashtable import HashTable
from miniredis.rdb import crc64, read_snapshot, write_snapshot


def snapshot(entries):
    """키와 엔트리를 메모리 버퍼에 직렬화해 RDB 바이트열을 반환한다.

    Args:
        entries: 바이트 키를 스냅샷에 담을 엔트리에 대응시킨 사전.
    """
    stream = io.BytesIO()
    write_snapshot(stream, entries)
    return stream.getvalue()


class SnapshotTests(unittest.TestCase):
    def test_snapshot_binary_types_lengths_and_checksum(self):
        """다양한 길이의 바이너리 문자열과 해시를 복구하고 CRC64로 손상된 스냅샷을 검출하는지 검증한다."""
        entries = {b'': Entry(b'\xff\0\r\n'), b'long': Entry(b'a' * 80000, 1234),
                   b'h': Entry({b'': b'', b'k' * 80: b'v\x80'})}
        encoded = snapshot(entries)
        self.assertEqual(crc64(b'123456789'), 0xe9c6d914c4b8d9ca)
        restored = read_snapshot(io.BytesIO(encoded))
        self.assertIsInstance(restored[b'h'].value, HashTable)
        self.assertEqual(restored, entries)
        with self.assertRaisesRegex(ValueError, "checksum"):
            read_snapshot(io.BytesIO(encoded[:-1] + bytes([encoded[-1] ^ 1])))

    def test_native_packed_hash_integer_and_lzf(self):
        """Redis의 listpack·ziplist 해시와 정수 인코딩 및 LZF 압축 문자열의 복원을 검증한다."""
        # Redis emits compact field/value encodings for a two-element hash ("x", "3").
        lp = b'\x0c\0\0\0\x02\0\x81x\x02\x03\x01\xff'
        zl = b'\x10\0\0\0\x0d\0\0\0\x02\0\0\x01x\x03\xf4\xff'
        encoded = (b'REDIS0011\x10\x01a' + bytes([len(lp)]) + lp
                   + b'\x0d\x01b' + bytes([len(zl)]) + zl
                   + b'\x00\x01c\xc3\x04\x05\x00x\x40\x00'  # one literal, four-byte back reference
                   + b'\x00\x01d\xc1\xfe\xff\xff' + bytes(8))
        restored = read_snapshot(io.BytesIO(encoded))
        for key in (b'a', b'b'):
            self.assertIsInstance(restored[key].value, HashTable)
        self.assertEqual(restored,
                         {b'a': Entry({b'x': b'3'}), b'b': Entry({b'x': b'3'}),
                          b'c': Entry(b'xxxxx'), b'd': Entry(b'-2')})
        with self.assertRaises(ValueError):
            read_snapshot(io.BytesIO(encoded.replace(lp, lp[:-2] + b'\x07\xff')))
