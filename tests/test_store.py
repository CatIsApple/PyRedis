"""Store 단위 테스트: set/get/del, 값 복사, TTL과 lazy 만료, incr, sweep, 잘못된 ttl."""
import unittest

from src.store import Store


def make_store(start=1000):
    clock = {"now": start}
    return Store(now=lambda: clock["now"]), clock


class StoreTest(unittest.TestCase):
    def test_set_get_delete(self):
        store, _ = make_store()
        store.set("a", "hello")
        self.assertEqual(store.get("a"), "hello")
        self.assertTrue(store.del_("a"))
        self.assertIsNone(store.get("a"))
        self.assertFalse(store.del_("a"))

    def test_values_are_copied_on_store(self):
        store, _ = make_store()
        obj = {"name": "kim", "tags": ["a"]}
        store.set("o", obj)
        obj["name"] = "changed"
        self.assertEqual(store.get("o"), {"name": "kim", "tags": ["a"]})

    def test_unserializable_value_rejected(self):
        store, _ = make_store()
        with self.assertRaises(TypeError):
            store.set("a", float("nan"))
        self.assertFalse(store.exists("a"))

    def test_lazy_expiration_at_exact_deadline(self):
        store, clock = make_store()
        store.set("a", "v", 500)
        clock["now"] += 499
        self.assertEqual(store.get("a"), "v")
        clock["now"] += 1
        self.assertIsNone(store.get("a"))
        self.assertFalse(store.exists("a"))

    def test_ttl_values(self):
        store, clock = make_store()
        store.set("a", 1)
        store.set("b", 1, 300)
        self.assertEqual(store.ttl("a"), -1)
        self.assertEqual(store.ttl("b"), 300)
        self.assertEqual(store.ttl("missing"), -2)
        clock["now"] += 300
        self.assertEqual(store.ttl("b"), -2)

    def test_set_without_ttl_clears_old_ttl(self):
        store, _ = make_store()
        store.set("a", 1, 100)
        store.set("a", 2)
        self.assertEqual(store.ttl("a"), -1)

    def test_expire_on_existing_key_only(self):
        store, clock = make_store()
        self.assertFalse(store.expire("nope", 100))
        store.set("a", 1)
        self.assertTrue(store.expire("a", 50))
        clock["now"] += 50
        self.assertFalse(store.exists("a"))

    def test_incr_starts_at_zero_and_keeps_expiry(self):
        store, clock = make_store()
        self.assertEqual(store.incr("c"), 1)
        store.expire("c", 100)
        self.assertEqual(store.incr("c"), 2)
        self.assertLessEqual(store.ttl("c"), 100)
        clock["now"] += 100
        self.assertEqual(store.incr("c"), 1)  # 만료됐으면 0부터 다시 센다

    def test_incr_non_number_counts_as_zero(self):
        store, _ = make_store()
        store.set("s", "text")
        self.assertEqual(store.incr("s"), 1)

    def test_sweep_removes_only_expired(self):
        store, clock = make_store()
        store.set("a", 1, 100)
        store.set("b", 1, 1000)
        store.set("c", 1)
        clock["now"] += 500
        self.assertEqual(store.size(), 2)
        self.assertEqual(store.sweep(), 1)
        self.assertEqual(store.size(), 2)

    def test_invalid_ttl_rejected(self):
        store, _ = make_store()
        for bad in (0, -1, float("inf"), True, "10"):
            with self.assertRaises(ValueError):
                store.set("a", 1, bad)


if __name__ == "__main__":
    unittest.main()
