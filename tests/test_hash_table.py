"""HashTable 단위 테스트: 기본 동작, 충돌 체인, rehash, 특수 키, 잘못된 키 거절."""
import unittest

from src.hash_table import HashTable, fnv1a


class HashTableTest(unittest.TestCase):
    def test_set_get_delete(self):
        t = HashTable()
        t.set("a", 1)
        self.assertEqual(t.get("a"), 1)
        self.assertTrue(t.delete("a"))
        self.assertIsNone(t.get("a"))
        self.assertFalse(t.delete("a"))

    def test_overwrite_keeps_size(self):
        t = HashTable()
        t.set("a", 1)
        t.set("a", 2)
        self.assertEqual((t.get("a"), t.size), (2, 1))

    def test_collisions_in_one_bucket(self):
        t = HashTable(capacity=8, hash_fn=lambda _key: 0)  # 모든 키를 한 칸에 몰아서 체인 경로를 검증
        for i in range(5):
            t.set(f"k{i}", i)
        self.assertEqual([t.get(f"k{i}") for i in range(5)], [0, 1, 2, 3, 4])
        t.delete("k2")
        self.assertFalse(t.has("k2"))
        self.assertEqual(t.get("k4"), 4)

    def test_rehash_grows_and_keeps_entries(self):
        t = HashTable(capacity=4)
        for i in range(100):
            t.set(f"key{i}", i)
        self.assertGreater(t.capacity, 4)
        self.assertTrue(all(t.get(f"key{i}") == i for i in range(100)))

    def test_special_keys_are_plain_keys(self):
        t = HashTable()
        t.set("__proto__", "x")
        t.set("constructor", "y")
        self.assertEqual((t.get("__proto__"), t.get("constructor")), ("x", "y"))

    def test_rejects_invalid_keys(self):
        t = HashTable()
        with self.assertRaises(TypeError):
            t.set("", 1)
        with self.assertRaises(TypeError):
            t.get(5)

    def test_fnv1a_is_32bit(self):
        self.assertTrue(0 <= fnv1a("hello") <= 0xFFFFFFFF)
        self.assertEqual(fnv1a("hello"), fnv1a("hello"))


if __name__ == "__main__":
    unittest.main()
