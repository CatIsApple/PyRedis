"""redis-benchmark로 Mini Redis와 실제 Redis의 SET·GET 처리량을 같은 조건에서 비교한다."""
import argparse
import csv
from datetime import datetime, timezone
import io
import json
from pathlib import Path
import platform
import shutil
import socket
import statistics
import subprocess
import sys
import tempfile
import time

from examples.services import service

HOST = "127.0.0.1"
TESTS = ("SET", "GET")


def free_port():
    """운영체제가 비어 있다고 알려 준 로컬 TCP 포트를 반환한다."""
    with socket.socket() as sock:
        sock.bind((HOST, 0))
        return sock.getsockname()[1]


def start_redis(directory):
    """디스크 저장을 끈 redis-server를 임시 포트로 실행한다.

    Args:
        directory: redis-server의 작업 디렉터리.

    Returns:
        실행한 프로세스와 포트의 튜플.
    """
    port = free_port()
    proc = subprocess.Popen(["redis-server", "--port", str(port), "--bind", HOST, "--save", "",
                             "--appendonly", "no", "--dir", str(directory)],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((HOST, port), timeout=0.2):
                return proc, port
        except OSError:
            time.sleep(0.05)
    proc.kill()
    raise RuntimeError("redis-server 기동 실패")


def run(port, requests, clients, pipeline):
    """redis-benchmark를 한 번 실행해 명령별 초당 처리량과 p50 지연(ms)을 읽는다.

    Args:
        port: 대상 서버 포트.
        requests: 명령마다 보낼 요청 수.
        clients: 동시 연결 수.
        pipeline: 한 번에 묶어 보낼 명령 수.
    """
    out = subprocess.run(["redis-benchmark", "-h", HOST, "-p", str(port), "-t", "set,get",
                          "-n", str(requests), "-c", str(clients), "-P", str(pipeline), "--csv"],
                         check=True, capture_output=True, text=True).stdout
    rows = {}
    for row in csv.DictReader(io.StringIO(out)):
        rows[row["test"]] = {"rps": float(row["rps"]), "p50_ms": float(row["p50_latency_ms"])}
    if set(rows) != set(TESTS):
        raise ValueError(f"예상하지 못한 redis-benchmark 출력: {out!r}")
    return rows


def benchmark(requests, clients, pipelines, rounds):
    """두 서버를 라운드마다 번갈아 측정하고 명령별 중앙값을 모은다."""
    samples = {}
    with tempfile.TemporaryDirectory() as tmp:
        redis_proc, redis_port = start_redis(tmp)
        try:
            with service("miniredis", ["--addr", HOST + ":0", "--aof", ""],
                         r"mini-redis listening on \('127\.0\.0\.1', (\d+)\)") as mini_port:
                ports = {"redis": redis_port, "miniredis": mini_port}
                for pipeline in pipelines:
                    for i in range(rounds):
                        order = ["miniredis", "redis"] if i % 2 == 0 else ["redis", "miniredis"]
                        for server in order:
                            for test, row in run(ports[server], requests, clients, pipeline).items():
                                samples.setdefault((server, pipeline, test), []).append(row)
        finally:
            redis_proc.terminate()
            redis_proc.wait(timeout=5)
    version = subprocess.run(["redis-server", "--version"], capture_output=True, text=True).stdout.strip()
    summary = [{"server": server, "pipeline": pipeline, "test": test,
                "rps": statistics.median(r["rps"] for r in rows),
                "p50_ms": statistics.median(r["p50_ms"] for r in rows), "rounds": rows}
               for (server, pipeline, test), rows in sorted(samples.items())]
    return {
        "completed_at_utc": datetime.now(timezone.utc).isoformat(),
        "environment": {"python": platform.python_version(), "platform": platform.platform(),
                        "machine": platform.machine(), "redis": version},
        "workload": {"requests_per_test": requests, "clients": clients, "pipelines": list(pipelines),
                     "rounds": rounds, "persistence": "off on both servers"},
        "summary": summary,
    }


def main():
    """명령줄에서 비교를 실행하고 결과를 화면과 지정한 JSON 파일에 기록한다."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--requests", type=int, default=100_000, help="명령마다 보낼 요청 수")
    parser.add_argument("--clients", type=int, default=50, help="동시 연결 수")
    parser.add_argument("--pipelines", type=int, nargs="+", default=[1, 16], help="파이프라인 깊이")
    parser.add_argument("--rounds", type=int, default=3, help="서버마다 반복할 횟수")
    parser.add_argument("--json", type=Path, metavar="PATH", help="상세 결과를 기록할 JSON 파일")
    args = parser.parse_args()
    for tool in ("redis-server", "redis-benchmark"):
        if shutil.which(tool) is None:
            sys.exit(f"{tool}이 PATH에 없습니다.")
    result = benchmark(args.requests, args.clients, args.pipelines, args.rounds)
    if args.json:
        args.json.parent.mkdir(parents=True, exist_ok=True)
        args.json.write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    for row in result["summary"]:
        print(f"{row['server']:<9} P={row['pipeline']:<3} {row['test']:<3} "
              f"{row['rps']:>10,.0f} req/s  p50={row['p50_ms']:.3f} ms")


if __name__ == "__main__":
    main()
