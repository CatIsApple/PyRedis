"""실제 HTTP 요청으로 SQLite 직접 집계와 MiniRedis 캐시 적중을 비교한다."""
import argparse
from collections import Counter
from contextlib import closing, ExitStack
from datetime import datetime, timezone
from http.client import HTTPConnection
import json
import math
from pathlib import Path
import platform
import sqlite3
import statistics
import tempfile
import time
from typing import NamedTuple

from examples.services import running_api, service

HOST = "127.0.0.1"
CATEGORIES = range(1, 9)
TTL = 3600


class Batch(NamedTuple):
    durations_ns: list
    elapsed_ns: int
    statuses: Counter


def expected_reports(db):
    """원본 주문을 분류별로 집계해 데이터 규모와 응답 검증에 사용할 기준값을 구한다.

    Args:
        db: 두 HTTP 서버가 함께 사용하는 SQLite 파일 경로.
    """
    with closing(sqlite3.connect(db)) as conn:
        rows = conn.execute("SELECT category_id, COUNT(*), SUM(amount) FROM orders GROUP BY category_id")
        reports = {key: {"category_id": key, "order_count": count, "total_amount": total}
                   for key, count, total in rows}
    if set(reports) != set(CATEGORIES) or sum(r["order_count"] for r in reports.values()) != 80_000:
        raise ValueError("예상한 8개 분류, 주문 80,000건과 다른 데이터입니다.")
    return reports


def run_batch(port, count, expected_status, reports, offset=0):
    """새 HTTP 연결로 분류를 순환 요청하며 응답 시간과 실제 캐시 상태를 수집한다.

    Args:
        port: 대상 HTTP 서버의 포트.
        count: 연속해서 전송할 요청 수.
        expected_status: 매 요청에 필요한 X-Cache 값.
        reports: 분류별로 SQLite에서 직접 계산한 응답 기댓값.
        offset: 이전 측정 라운드에서 같은 모드로 보낸 요청 수.
    """
    durations = []
    statuses = Counter()
    batch_start = time.perf_counter_ns()
    for i in range(count):
        key = CATEGORIES[(offset + i) % len(CATEGORIES)]
        path = f"/reports/{key}"
        start = time.perf_counter_ns()
        with closing(HTTPConnection(HOST, port, timeout=10)) as conn:
            conn.request("GET", path)
            response = conn.getresponse()
            body = response.read()
            duration = time.perf_counter_ns() - start
        status = response.getheader("X-Cache")
        if response.status != 200 or status != expected_status:
            raise ValueError(f"{path}: HTTP {response.status}, X-Cache={status!r}, expected {expected_status}")
        if json.loads(body) != reports[key]:
            raise ValueError(f"{path}: 응답 JSON이 SQLite 기준값과 다릅니다.")
        durations.append(duration)
        statuses[status] += 1
    return Batch(durations, time.perf_counter_ns() - batch_start, statuses)


def summarize(batches):
    """요청 표본 전체에서 중앙값, 최근접 순위 p95, 측정 구간 전체 처리량을 계산한다.

    Args:
        batches: 동일한 모드로 실행한 측정 구간들.
    """
    timings = sorted(ns for batch in batches for ns in batch.durations_ns)
    counts = Counter()
    for batch in batches:
        counts.update(batch.statuses)
    elapsed = sum(batch.elapsed_ns for batch in batches) / 1e9
    return {
        "requests": len(timings),
        "x_cache": dict(counts),
        "median_ms": statistics.median(timings) / 1e6,
        "p95_ms": timings[math.ceil(0.95 * len(timings)) - 1] / 1e6,
        "elapsed_s": elapsed,
        "requests_per_second": len(timings) / elapsed,
    }


def benchmark(requests, rounds):
    """빈 캐시에서 첫 조회를 검증한 뒤 두 모드의 측정 순서를 라운드마다 뒤집는다.

    Args:
        requests: 라운드마다 각 모드에 전송할 요청 수.
        rounds: 두 모드를 한 번씩 측정할 라운드 수.
    """
    with tempfile.TemporaryDirectory() as tmp, ExitStack() as services:
        directory = Path(tmp)
        db = directory / "orders.sqlite3"
        redis_port = services.enter_context(service(
            "miniredis", ["--addr", HOST + ":0", "--aof", ""],
            r"mini-redis listening on \('127\.0\.0\.1', (\d+)\)"))
        ports = {}
        for mode in ("off", "on"):
            ports[mode] = services.enter_context(running_api(db, redis_port, mode, TTL))
        reports = expected_reports(db)
        # 양측 모두 8개 분류를 두 번 읽고 측정을 시작한다.
        warmup = {}
        for mode, status in (("off", "BYPASS"), ("on", "MISS"), ("off", "BYPASS"), ("on", "HIT")):
            warmup.setdefault(status, []).append(run_batch(ports[mode], len(CATEGORIES), status, reports))
        batches = {"off": [], "on": []}
        details = []
        for i in range(rounds):
            order = ["off", "on"] if i % 2 == 0 else ["on", "off"]
            row = {"round": i + 1, "order": order}
            for mode in order:
                status = "BYPASS" if mode == "off" else "HIT"
                batch = run_batch(ports[mode], requests, status, reports, offset=i * requests)
                batches[mode].append(batch)
                row[mode] = summarize([batch])
            details.append(row)
    return {
        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
        "environment": {"python": platform.python_version(), "platform": platform.platform(),
                        "machine": platform.machine(), "sqlite": sqlite3.sqlite_version},
        "workload": {"rows": sum(r["order_count"] for r in reports.values()),
                     "categories": list(CATEGORIES), "requests_per_mode_per_round": requests,
                     "rounds": rounds, "concurrency": 1, "cache_ttl_seconds": TTL, "redis_aof": False},
        "method": {
            "request": "GET /reports/1..8 in round-robin order, fresh loopback HTTP connection per request.",
            "latency": "perf_counter_ns immediately before connection creation through receipt of the full body.",
            "aggregation": "Median and nearest-rank p95 over pooled per-request latencies in milliseconds.",
            "throughput": "Requests divided by summed batch wall seconds, including status and JSON checks.",
            "validation": "Every HTTP status, X-Cache header and JSON aggregate checked against SQLite.",
            "warmup": "Eight categories twice per mode. First cold cache misses reported separately. All warmup excluded.",
        },
        "warmup": {status: summarize(samples) for status, samples in warmup.items()},
        "per_round": details,
        "summary": {mode: summarize(samples) for mode, samples in batches.items()},
    }


def main():
    """명령줄에서 측정을 실행하고 결과를 화면과 지정한 JSON 파일에 기록한다."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--requests", type=int, default=120, help="라운드당 각 모드의 요청 수")
    parser.add_argument("--rounds", type=int, default=5, help="반복할 비교 라운드 수")
    parser.add_argument("--json", type=Path, metavar="PATH", help="상세 결과를 기록할 JSON 파일")
    args = parser.parse_args()
    if args.requests < 1 or args.rounds < 1:
        parser.error("--requests와 --rounds는 1 이상이어야 합니다.")
    result = benchmark(args.requests, args.rounds)
    if args.json:
        args.json.parent.mkdir(parents=True, exist_ok=True)
        args.json.write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    for mode, row in result["summary"].items():
        print(f"cache={mode}  n={row['requests']}  X-Cache={row['x_cache']}  "
              f"median={row['median_ms']:.3f} ms  p95={row['p95_ms']:.3f} ms  "
              f"{row['requests_per_second']:.1f} req/s")


if __name__ == "__main__":
    main()
