"""실제 HTTP API와 단일 노드 MiniRedis 사이의 주문 집계 캐시를 검증한다."""

from contextlib import closing, contextmanager
import http.client
import json
from pathlib import Path
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest

from examples.cache_api import report_cache_key
from tests.test_redis_compat import ROOT, server


@contextmanager
def running_api(db_path, redis_port, cache="on", ttl=60):
    """HTTP API를 임시 포트에 실행하고 테스트가 끝나면 프로세스를 종료한다.

    Args:
        db_path: 주문을 저장할 SQLite 파일 경로.
        redis_port: API가 접속할 로컬 MiniRedis 포트.
        cache: 캐시를 사용할 때는 on, 우회할 때는 off.
        ttl: 캐시 항목의 유효 기간(초).

    Yields:
        실행 중인 HTTP 서버의 포트 번호.
    """
    args = [sys.executable, "-m", "examples.cache_api", "--host", "127.0.0.1",
            "--port", "0", "--db", str(db_path), "--redis-host", "127.0.0.1",
            "--redis-port", str(redis_port), "--cache", cache, "--ttl", str(ttl)]
    with tempfile.TemporaryDirectory() as log_dir:
        log_path = Path(log_dir) / "server.log"
        with log_path.open("wb") as output:
            process = subprocess.Popen(args, cwd=ROOT, stdout=output, stderr=subprocess.STDOUT)
        try:
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline and process.poll() is None:
                lines = log_path.read_text(errors="replace").splitlines()
                ready = next((line for line in lines if line.startswith("READY ")), None)
                if ready:
                    port = int(ready.split()[1])
                    break
                time.sleep(0.02)
            else:
                raise RuntimeError("API가 준비되지 않았습니다.\n" + log_path.read_text(errors="replace"))
            yield port
        finally:
            if process.poll() is None:
                process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()


def get_report(port, category_id):
    """지정한 분류의 보고서를 요청해 HTTP 상태와 캐시 상태 및 본문을 읽는다.

    Args:
        port: HTTP 서버의 로컬 포트.
        category_id: 조회할 주문 분류 번호.
    """
    with closing(http.client.HTTPConnection("127.0.0.1", port, timeout=5)) as connection:
        connection.request("GET", "/reports/%s" % category_id)
        response = connection.getresponse()
        body = response.read()
        return response.status, response.getheader("X-Cache"), body


class CacheApiTests(unittest.TestCase):
    def setUp(self):
        """각 테스트에 독립된 SQLite 파일 경로를 마련한다."""
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.db = self.directory / "orders.sqlite3"

    def expected(self, category_id):
        """원본 DB에서 주문 수와 금액 합계를 계산해 HTTP 응답의 기댓값을 구한다.

        Args:
            category_id: 집계할 주문 분류 번호.
        """
        with closing(sqlite3.connect(self.db)) as connection:
            count, amount = connection.execute(
                "SELECT COUNT(*), SUM(amount) FROM orders WHERE category_id = ?",
                (category_id,),
            ).fetchone()
        return {"category_id": category_id, "order_count": count, "total_amount": amount}

    def increase_amounts(self, category_id):
        """지정한 분류의 원본 주문 금액을 변경해 이전 캐시 값과 구별한다.

        Args:
            category_id: 주문 금액을 갱신할 분류 번호.
        """
        with closing(sqlite3.connect(self.db)) as connection:
            changed = connection.execute(
                "UPDATE orders SET amount = amount + 7 WHERE category_id = ?", (category_id,)
            ).rowcount
            self.assertGreater(changed, 0)
            connection.commit()

    def check_report(self, port, category_id, cache_status, expected):
        """HTTP로 받은 집계 결과와 캐시 처리 상태를 확인한다.

        Args:
            port: API 서버의 로컬 포트.
            category_id: 조회할 주문 분류 번호.
            cache_status: X-Cache 헤더에 기대하는 값.
            expected: 원본 집계 또는 캐시에 저장된 보고서.
        """
        status, header, body = get_report(port, category_id)
        self.assertEqual((status, header), (200, cache_status))
        self.assertEqual(json.loads(body), expected)

    def test_miss_hit_and_delete_refresh(self):
        """원본이 바뀌어도 적중 시 저장한 값을 반환하고 키 삭제 후에는 다시 집계하는지 검증한다."""
        with server("mini", self.directory) as redis:
            with running_api(self.db, redis.connection.getpeername()[1]) as port:
                first = self.expected(1)
                self.assertGreater(first["order_count"], 0)
                self.check_report(port, 1, "MISS", first)

                key = report_cache_key(self.db, 1)
                tag, payload = redis.command("GET", key)
                self.assertEqual(tag, b"$")
                self.assertEqual(json.loads(payload), first)
                tag, seconds = redis.command("TTL", key)
                self.assertEqual(tag, b":")
                self.assertTrue(0 < seconds <= 60, seconds)

                self.increase_amounts(1)
                self.check_report(port, 1, "HIT", first)
                with running_api(self.db, redis.connection.getpeername()[1], cache="off") as off:
                    self.check_report(off, 1, "BYPASS", self.expected(1))
                    self.check_report(off, 2, "BYPASS", self.expected(2))
                    self.assertEqual(redis.command("EXISTS", report_cache_key(self.db, 2)), (b":", 0))
                self.check_report(port, 1, "HIT", first)
                self.assertEqual(redis.command("DEL", key), (b":", 1))
                self.check_report(port, 1, "MISS", self.expected(1))

    def test_categories_and_missing_report(self):
        """두 분류의 키가 분리되고 존재하지 않는 보고서는 캐시에 저장되지 않는지 검증한다."""
        with server("mini", self.directory) as redis:
            with running_api(self.db, redis.connection.getpeername()[1]) as port:
                for category_id in (1, 8):
                    self.check_report(port, category_id, "MISS", self.expected(category_id))
                for category_id in (8, 1):
                    self.check_report(port, category_id, "HIT", self.expected(category_id))
                self.assertEqual(get_report(port, 99)[0], 404)
                self.assertEqual(redis.command("EXISTS", report_cache_key(self.db, 99)), (b":", 0))
                with closing(sqlite3.connect(self.db)) as db:
                    deleted = db.execute("DELETE FROM orders WHERE category_id = 6").rowcount
                    self.assertGreater(deleted, 0)
                    db.commit()
                    missing_key = report_cache_key(self.db, 6)
                    for _ in range(2):
                        self.assertEqual(get_report(port, 6)[0], 404)
                        self.assertEqual(redis.command("EXISTS", missing_key), (b":", 0))
                    db.execute("INSERT INTO orders (category_id, amount) VALUES (6, 250)")
                    db.commit()
                restored = {"category_id": 6, "order_count": 1, "total_amount": 250}
                self.check_report(port, 6, "MISS", restored)
                self.check_report(port, 6, "HIT", restored)

    def test_expired_report_reads_changed_database(self):
        """TTL이 지난 뒤에는 SQLite에서 바뀐 주문 금액을 읽어 캐시에 다시 저장하는지 검증한다."""
        with server("mini", self.directory) as redis:
            with running_api(self.db, redis.connection.getpeername()[1], ttl=1) as port:
                self.check_report(port, 1, "MISS", self.expected(1))
                self.increase_amounts(1)
                key = report_cache_key(self.db, 1)
                deadline = time.monotonic() + 5
                while redis.command("EXISTS", key) == (b":", 1):
                    if time.monotonic() >= deadline:
                        self.fail("1초 TTL을 설정한 보고서 키가 만료되지 않았습니다.")
                    time.sleep(0.03)
                self.check_report(port, 1, "MISS", self.expected(1))

    def test_cache_off_and_unavailable_match_cached_response(self):
        """캐시를 끄거나 서버에 접속할 수 없어도 같은 집계를 SQLite에서 반환하는지 검증한다."""
        with server("mini", self.directory) as redis:
            with running_api(self.db, redis.connection.getpeername()[1]) as port:
                expected = self.expected(3)
                self.check_report(port, 3, "MISS", expected)
                self.check_report(port, 3, "HIT", expected)

        # 포트를 점유한 채 listen을 호출하지 않아 다른 프로세스가 이 주소를 차지할 수 없다.
        with socket.socket() as offline:
            offline.bind(("127.0.0.1", 0))
            closed_port = offline.getsockname()[1]
            for mode, status in (("off", "BYPASS"), ("on", "UNAVAILABLE")):
                with self.subTest(mode=mode), running_api(self.db, closed_port, cache=mode) as port:
                    self.check_report(port, 3, status, expected)
                    self.increase_amounts(3)
                    expected = self.expected(3)
                    self.check_report(port, 3, status, expected)
