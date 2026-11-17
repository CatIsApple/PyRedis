"""키 충돌과 버킷 크기 변경이 해시 테이블의 값과 순서에 미치는 영향을 검증한다."""

import unittest

from miniredis.hashtable import HashTable


def bucket_keys(index, count):
    """버킷이 32개일 때 같은 인덱스로 모이는 서로 다른 바이너리 키를 고른다.

    Args:
        index: 해시의 하위 5비트로 선택할 버킷 번호.
        count: 찾을 키의 개수.
    """
    keys = []
    number = 0
    while len(keys) < count:
        key = b"\x00\xff\r\n" + number.to_bytes(4, "big")
        if hash(key) & 31 == index:
            keys.append(key)
        number += 1
    return keys


class HashTableTests(unittest.TestCase):
    def assert_contents(self, table, expected):
        """값의 일치와 순회의 삽입 순서를 별도로 검증한다.

        Args:
            table: 연산 결과를 확인할 해시 테이블.
            expected: 같은 변경을 적용한 기준 사전.
        """
        self.assertEqual(len(table), len(expected))
        self.assertEqual(table, dict(reversed(list(expected.items()))))
        self.assertEqual(list(table), list(expected))
        self.assertEqual(list(table.items()), list(expected.items()))
        for key, value in expected.items():
            # 서로 다른 TCP 요청에서 다시 읽은 바이트열도 같은 키로 해석한다.
            self.assertEqual(table[bytes(bytearray(key))], value)

    def test_collision_chain_update_delete_and_reinsert(self):
        """빈 키와 바이너리 키가 한 버킷에 모여도 각 항목을 독립적으로 갱신하고 삭제할 수 있는지 확인한다."""
        keys = [b""] + bucket_keys(hash(b"") & 31, 8)
        table = HashTable()
        expected = {}
        for key in keys:
            table[key] = expected[key] = b"original:" + key
        self.assert_contents(table, expected)

        for key in (keys[0], keys[4], keys[-1]):
            table[key] = expected[key] = b"updated:" + key
        self.assert_contents(table, expected)

        for key in (keys[4], keys[0], keys[-1]):
            del table[key]
            del expected[key]
            with self.assertRaises(KeyError):
                _ = table[key]
            self.assert_contents(table, expected)
        table[keys[4]] = expected[keys[4]] = b"reinserted"
        self.assert_contents(table, expected)

    def test_identical_full_hash_still_compares_key_bytes(self):
        """전체 해시값이 같아도 키의 내용에 따라 값을 따로 조회하고 수정하는지 확인한다."""
        class CollidingKey(bytes):
            """모든 인스턴스에 같은 해시값을 부여해 완전한 충돌을 재현한다."""

            def __hash__(self):
                """키 내용과 관계없이 동일한 해시값을 반환한다."""
                return 7

        table = HashTable()
        first, second = CollidingKey(b"a"), CollidingKey(b"b")
        table[first] = 1
        table[second] = 2
        self.assertEqual(len(table), 2)
        table[CollidingKey(b"a")] = 3
        self.assertEqual(table[CollidingKey(b"a")], 3)
        self.assertEqual(table[CollidingKey(b"b")], 2)
        del table[CollidingKey(b"a")]
        with self.assertRaises(KeyError):
            _ = table[first]
        self.assertEqual(list(table.items()), [(second, 2)])

    def test_resize_thresholds_and_bucket_redistribution(self):
        """리사이즈 경계에서 용량을 확인하고 확장과 축소로 위치가 달라지는 모든 키를 다시 조회한다."""
        # 하위 3비트가 같아 최초에는 한 버킷에 모이고 이후에는 네 버킷으로 나뉜다.
        groups = [bucket_keys(bucket, 4) for bucket in (3, 11, 19, 27)]
        keys = [key for group in zip(*groups) for key in group][:13]
        table = HashTable()
        expected = {}
        self.assertEqual(len(table._buckets), 8)
        for count, key in enumerate(keys, 1):
            table[key] = expected[key] = key
            capacity = 8 if count <= 6 else 16 if count <= 12 else 32
            self.assertEqual(len(table._buckets), capacity)
            if count in (6, 12):
                table[key] = expected[key] = b"overwritten"
                self.assertEqual(len(table._buckets), capacity)
            if count == 6:
                table.move_to_end(keys[0])
                expected[keys[0]] = expected.pop(keys[0])
            self.assert_contents(table, expected)

        for key in reversed(keys):
            del table[key]
            del expected[key]
            count = len(expected)
            capacity = 32 if count >= 8 else 16 if count >= 4 else 8
            self.assertEqual(len(table._buckets), capacity)
            self.assert_contents(table, expected)

    def test_move_to_end_overwrite_and_reinsertion_order(self):
        """첫 항목과 중간 항목을 끝으로 옮긴 뒤 덮어쓰기와 재삽입의 순서가 유지되는지 확인한다."""
        table = HashTable()
        expected = {}
        keys = [b"", b"A\x00", b"B\xff", b"C", b"D", b"E", b"F"]
        for key in keys:
            table[key] = expected[key] = key
        for key in (keys[0], keys[3], keys[3]):
            table.move_to_end(key)
            expected[key] = expected.pop(key)
            self.assert_contents(table, expected)
        table[keys[2]] = expected[keys[2]] = b"updated"
        del table[keys[1]]
        del expected[keys[1]]
        table[keys[1]] = expected[keys[1]] = b"reinserted"
        self.assert_contents(table, expected)
