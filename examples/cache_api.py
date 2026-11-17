"""SQLite 주문 집계를 mini-redis에 캐시하는 HTTP API 예제."""

import argparse
import hashlib
import json
import os
import re
import sqlite3
from contextlib import closing
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

from examples.cache_client import CacheClient, CacheUnavailable


def seed_orders(db):
    """주문 테이블이 없을 때 8개 카테고리의 주문 8만 건을 한 트랜잭션으로 생성한다.

    Args:
        db: 예제 데이터를 보관할 SQLite 연결.
    """
    with db:
        db.execute("BEGIN IMMEDIATE")
        exists = db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='orders'")
        if exists.fetchone():
            return
        db.execute("""CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            category_id INTEGER NOT NULL,
            amount INTEGER NOT NULL
        )""")
        rows = ((i, (i - 1) % 8 + 1, 100 + i * 37 % 10000) for i in range(1, 80001))
        db.executemany("INSERT INTO orders (id, category_id, amount) VALUES (?, ?, ?)", rows)


def report_cache_key(db_path, category_id) -> bytes:
    """SQLite 파일의 절대 경로와 카테고리 번호로 보고서 캐시 키를 만든다.

    Args:
        db_path: 주문 데이터를 읽는 SQLite 파일 경로.
        category_id: 집계할 카테고리 번호.
    """
    database = hashlib.sha256(os.fsencode(Path(db_path).resolve())).hexdigest()
    return f"mini-redis:orders:v1:{database}:{category_id}".encode("ascii")


def query_report(db, category_id):
    """카테고리별 주문 건수와 금액 합계를 조회해 JSON 바이트열로 반환한다.

    Args:
        db: 주문 테이블을 읽을 SQLite 연결.
        category_id: 집계할 카테고리 번호.

    Returns:
        주문이 있으면 UTF-8 JSON, 한 건도 없으면 None이다.
    """
    count, total = db.execute(
        "SELECT COUNT(*), SUM(amount) FROM orders WHERE category_id = ?", (category_id,)
    ).fetchone()
    if not count:
        return None
    report = {"category_id": category_id, "order_count": count, "total_amount": total}
    return json.dumps(report, separators=(",", ":")).encode("utf-8")


class ReportServer(HTTPServer):
    def __init__(self, address, db, db_path, cache, ttl):
        """HTTP 요청에 사용할 데이터베이스와 캐시를 연결하고 수신 소켓을 연다.

        Args:
            address: HTTP 요청을 수신할 호스트와 포트.
            db: 주문 테이블을 읽을 SQLite 연결.
            db_path: 캐시 키에 사용할 SQLite 파일 경로.
            cache: mini-redis 클라이언트. None이면 SQLite만 조회한다.
            ttl: 각 집계 결과를 캐시에 보관할 시간(초).
        """
        self.db = db
        self.cache = cache
        self.ttl = ttl
        self.keys = {i: report_cache_key(db_path, i) for i in range(1, 9)}
        super().__init__(address, ReportHandler)

    def report(self, category_id):
        """캐시에 집계 결과가 없으면 SQLite를 조회하고 결과에 TTL을 붙여 저장한다.

        Args:
            category_id: 요청 경로에서 확인한 1부터 8까지의 카테고리 번호.

        Returns:
            JSON 바이트열 또는 None과 X-Cache 상태를 담은 쌍이다.
        """
        if self.cache is None:
            return query_report(self.db, category_id), "BYPASS"
        key = self.keys[category_id]
        try:
            cached = self.cache.get(key)
        except CacheUnavailable:
            return query_report(self.db, category_id), "UNAVAILABLE"
        if cached is not None:
            return cached, "HIT"
        body = query_report(self.db, category_id)
        if body is not None:
            try:
                self.cache.setex(key, self.ttl, body)
            except CacheUnavailable:
                return body, "UNAVAILABLE"
        return body, "MISS"


class ReportHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        """GET /reports/1부터 /reports/8까지의 요청에 집계 결과를 응답한다."""
        path = self.path.partition("?")[0]
        route = re.fullmatch(r"/reports/([1-8])", path)
        if route is not None:
            try:
                body, cache_status = self.server.report(int(route.group(1)))
            except sqlite3.Error:
                self.send_json(503, b'{"error":"database unavailable"}')
                return
            if body is not None:
                self.send_json(200, body, cache_status)
                return
        self.send_json(404, b'{"error":"report not found"}')

    def send_json(self, status, body, cache_status=None):
        """HTTP 상태 코드와 JSON 본문을 전송한다.

        Args:
            status: HTTP 응답 상태 코드.
            body: UTF-8로 인코딩된 JSON 바이트열.
            cache_status: X-Cache 헤더에 표시할 상태. 오류 응답에서는 생략한다.
        """
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        if cache_status is not None:
            self.send_header("X-Cache", cache_status)
        self.end_headers()
        self.wfile.write(body)

    def log_request(self, code="-", size="-"):
        """벤치마크에서 요청별 로그 출력 비용을 제외한다.

        Args:
            code: HTTP 상태 코드.
            size: 응답 본문의 바이트 수.
        """


def main():
    """CLI 옵션에 따라 SQLite 예제 데이터를 준비하고 단일 스레드 HTTP 서버를 실행한다."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--db", type=Path, default=Path("orders.sqlite3"))
    parser.add_argument("--redis-host", default="127.0.0.1")
    parser.add_argument("--redis-port", type=int, default=6379)
    parser.add_argument("--cache", choices=("on", "off"), default="on")
    parser.add_argument("--ttl", type=int, default=30, help="캐시 만료 시간(초)")
    args = parser.parse_args()
    if not 0 <= args.port <= 65535 or not 1 <= args.redis_port <= 65535:
        parser.error("HTTP 포트는 0~65535, Redis 포트는 1~65535 범위여야 합니다.")
    if args.ttl < 1:
        parser.error("--ttl은 1초 이상이어야 합니다.")
    cache = CacheClient(args.redis_host, args.redis_port) if args.cache == "on" else None
    try:
        with closing(sqlite3.connect(args.db)) as db:
            seed_orders(db)
            with ReportServer((args.host, args.port), db, args.db, cache, args.ttl) as server:
                print(f"READY {server.server_port}", flush=True)
                server.serve_forever()
    except KeyboardInterrupt:
        pass
    except (OSError, sqlite3.Error) as exc:
        parser.exit(1, f"cache-api: {exc}\n")


if __name__ == "__main__":
    main()
