"""미니 Redis 서버 API 테스트(HTTP 실제 호출)와 클라이언트 테스트, 413·동시 incr·연결 실패 포함."""
import json
import socket
import threading
import unittest
import urllib.error
import urllib.request

from src.client import MiniRedisClient, RedisHttpError, RedisUnavailableError
from src.server import create_server
from src.store import Store


class ServerTestBase(unittest.TestCase):
    def setUp(self):
        self.store = Store()
        self.server = create_server(self.store, "127.0.0.1", 0)
        self.url = f"http://127.0.0.1:{self.server.server_address[1]}"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()

    def call(self, method, path, body=None):
        data = None if body is None else json.dumps(body).encode("utf-8")
        req = urllib.request.Request(self.url + path, data=data, method=method, headers={"Content-Type": "application/json"} if data else {})
        try:
            with urllib.request.urlopen(req, timeout=5) as res:
                raw = res.read()
                return res.status, (json.loads(raw) if raw else None)
        except urllib.error.HTTPError as err:
            raw = err.read()
            return err.code, (json.loads(raw) if raw else None)


class ServerApiTest(ServerTestBase):
    def test_put_get_delete(self):
        self.assertEqual(self.call("PUT", "/keys/a", {"value": "hi"})[0], 201)
        self.assertEqual(self.call("PUT", "/keys/a", {"value": "again"})[0], 200)
        status, body = self.call("GET", "/keys/a")
        self.assertEqual((status, body["value"], body["ttlMs"]), (200, "again", None))
        self.assertEqual(self.call("DELETE", "/keys/a")[0], 200)
        self.assertEqual(self.call("GET", "/keys/a")[0], 404)

    def test_missing_value_is_400(self):
        self.assertEqual(self.call("PUT", "/keys/a", {"ttlMs": 5})[0], 400)

    def test_bad_ttl_is_400(self):
        self.assertEqual(self.call("PUT", "/keys/a", {"value": 1, "ttlMs": -5})[0], 400)

    def test_expire_ttl_and_incr(self):
        self.call("PUT", "/keys/n", {"value": 0})
        self.assertEqual(self.call("POST", "/keys/n/expire", {"ttlMs": 5000})[0], 200)
        status, body = self.call("GET", "/keys/n/ttl")
        self.assertEqual(status, 200)
        self.assertLessEqual(body["ttlMs"], 5000)
        self.assertEqual(self.call("POST", "/keys/n/incr")[1]["value"], 1)
        self.assertEqual(self.call("POST", "/keys/missing/expire", {"ttlMs": 5})[0], 404)

    def test_unknown_route_and_method(self):
        self.assertEqual(self.call("GET", "/nope")[0], 404)
        self.assertEqual(self.call("PATCH", "/keys/a")[0], 405)

    def test_empty_key_is_400(self):
        self.assertEqual(self.call("GET", "/keys/")[0], 400)

    def test_concurrent_incr_is_exact(self):
        threads = [threading.Thread(target=self.call, args=("POST", "/keys/hits/incr")) for _ in range(50)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(self.store.get("hits"), 50)

    def test_oversized_body_is_413(self):
        import http.client

        self.server.max_body = 10
        conn = http.client.HTTPConnection("127.0.0.1", self.server.server_address[1], timeout=5)
        conn.putrequest("PUT", "/keys/a")
        conn.putheader("Content-Type", "application/json")
        conn.putheader("Content-Length", "100")  # 본문은 보내지 않는다: 서버는 길이만 보고 413을 낸다
        conn.endheaders()
        res = conn.getresponse()
        self.assertEqual(res.status, 413)
        conn.close()


class ClientTest(ServerTestBase):
    def test_client_round_trip(self):
        client = MiniRedisClient(self.url)
        client.set("k", {"n": 1}, 10_000)
        self.assertEqual(client.get("k"), {"n": 1})
        self.assertGreater(client.ttl("k"), 0)
        self.assertTrue(client.expire("k", 20_000))
        self.assertTrue(client.delete("k"))
        self.assertIsNone(client.get("k"))
        self.assertEqual(client.ttl("k"), -2)
        self.assertEqual(client.incr("c"), 1)

    def test_client_raises_http_error_for_bad_request(self):
        with self.assertRaises(RedisHttpError) as ctx:
            MiniRedisClient(self.url).set("k", 1, -1)
        self.assertEqual(ctx.exception.status, 400)

    def test_unreachable_server_is_unavailable(self):
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            port = s.getsockname()[1]  # 아무도 듣지 않는 포트
        with self.assertRaises(RedisUnavailableError):
            MiniRedisClient(f"http://127.0.0.1:{port}", timeout=1).get("k")


if __name__ == "__main__":
    unittest.main()
