"""데모 테스트: 인증 해시, DB 시드·읽기, 정적 경로 탈출 차단, 로그인→대시보드 MISS/HIT→로그아웃 흐름."""
import json
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from pathlib import Path

from demo.auth import hash_password, parse_cookies, verify_password
from demo.db import DbNotSeededError, open_db
from demo.seed_db import seed_db
from demo.server import create_demo_server, resolve_inside
from src.client import MiniRedisClient
from src.server import create_server
from src.store import Store


class AuthTest(unittest.TestCase):
    def test_password_hash_round_trip(self):
        cred = hash_password("secret")
        self.assertTrue(verify_password("secret", cred))
        self.assertFalse(verify_password("wrong", cred))

    def test_cookie_parsing(self):
        self.assertEqual(parse_cookies("a=1; sid=abc; sid=other"), {"a": "1", "sid": "abc"})
        self.assertEqual(parse_cookies(None), {})


class DbTest(unittest.TestCase):
    def test_seed_then_query_is_deterministic(self):
        with tempfile.TemporaryDirectory() as tmp:
            file = Path(tmp) / "t.db"
            seed_db(file=file, rows=500, index=True)
            db = open_db(file)
            first = db.get_dashboard_stats("demo")
            self.assertEqual(first, db.get_dashboard_stats("demo"))
            self.assertEqual(db.info()["indexed"], "on")
            db.close()

    def test_missing_file_is_not_created(self):
        with tempfile.TemporaryDirectory() as tmp:
            file = Path(tmp) / "none.db"
            with self.assertRaises(DbNotSeededError):
                open_db(file)
            self.assertFalse(file.exists())


class StaticPathTest(unittest.TestCase):
    def test_traversal_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "ok.html").write_text("x", encoding="utf-8")
            self.assertEqual(resolve_inside(root, "ok.html"), (root / "ok.html").resolve())
            for bad in ("../x", "%2e%2e/x", ".env", "a%5cb", "con"):
                with self.assertRaises(Exception):
                    resolve_inside(root, bad)


class DemoFlowTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        tmp = Path(cls.tmp.name)
        cls.db_file = tmp / "demo.db"
        seed_db(file=cls.db_file, rows=2000)
        cls.store = Store()
        cls.redis = create_server(cls.store, "127.0.0.1", 0)
        threading.Thread(target=cls.redis.serve_forever, daemon=True).start()
        redis_url = f"http://127.0.0.1:{cls.redis.server_address[1]}"
        users = {"demo": hash_password("demo1234")}
        cls.demo = create_demo_server(
            ("127.0.0.1", 0),
            redis_client=MiniRedisClient(redis_url),
            users=users,
            db=open_db(cls.db_file),
            web_root=Path(__file__).resolve().parent.parent / "demo" / "web",
        )
        cls.url = f"http://127.0.0.1:{cls.demo.server_address[1]}"
        threading.Thread(target=cls.demo.serve_forever, daemon=True).start()

    @classmethod
    def tearDownClass(cls):
        cls.demo.shutdown()
        cls.demo.server_close()
        cls.demo.app.db.close()  # Windows는 열린 DB 파일을 지울 수 없으므로 먼저 닫는다
        cls.redis.shutdown()
        cls.redis.server_close()
        cls.tmp.cleanup()

    def request(self, method, path, body=None, cookie=None, content_type="application/json"):
        headers = {}
        if body is not None:
            headers["Content-Type"] = content_type
        if cookie:
            headers["Cookie"] = cookie
        data = None if body is None else json.dumps(body).encode("utf-8")
        req = urllib.request.Request(self.url + path, data=data, method=method, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=5) as res:
                return res.status, res.headers, res.read()
        except urllib.error.HTTPError as err:
            return err.code, err.headers, err.read()

    def test_login_dashboard_cache_logout(self):
        status, headers, _ = self.request("POST", "/api/login", {"username": "demo", "password": "demo1234"})
        self.assertEqual(status, 200)
        cookie = headers.get_all("Set-Cookie")[0].split(";")[0]

        status, headers, _ = self.request("GET", "/api/dashboard?mode=redis", cookie=cookie)
        self.assertEqual((status, headers.get("X-Cache")), (200, "MISS"))
        status, headers, _ = self.request("GET", "/api/dashboard?mode=redis", cookie=cookie)
        self.assertEqual(headers.get("X-Cache"), "HIT")
        status, headers, _ = self.request("GET", "/api/dashboard?mode=db", cookie=cookie)
        self.assertEqual(headers.get("X-Cache"), "BYPASS")

        self.assertEqual(self.request("GET", "/api/me", cookie=cookie)[0], 200)
        self.assertEqual(self.request("POST", "/api/logout", {}, cookie=cookie)[0], 200)
        self.assertEqual(self.request("GET", "/api/me", cookie=cookie)[0], 401)

    def test_wrong_password_is_401_and_form_post_is_415(self):
        status, _, _ = self.request("POST", "/api/login", {"username": "demo", "password": "nope"})
        self.assertEqual(status, 401)
        status, _, _ = self.request("POST", "/api/login", {"username": "demo", "password": "x"}, content_type="text/plain")
        self.assertEqual(status, 415)

    def test_static_index_has_csp(self):
        status, headers, body = self.request("GET", "/")
        self.assertEqual(status, 200)
        self.assertIn("default-src 'self'", headers.get("Content-Security-Policy"))
        self.assertIn(b"<", body)


if __name__ == "__main__":
    unittest.main()
