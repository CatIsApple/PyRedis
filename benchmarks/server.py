"""Mini Redis 서버의 동시 연결 처리, 명령 원자성, 영속화 비용, AOF 재작성 후 복구 시간을 측정한다.

    python3 -m benchmarks.server --json benchmarks/results/server.json

부하 생성기는 별도 프로세스 여러 개가 각자 asyncio 연결을 열어 요청 하나를 보내고 응답을 받은 뒤
다음 요청을 보낸다(파이프라인 없음). 서버는 examples.services로 실제 CLI를 임시 포트에 띄운다.
"""
import argparse
import asyncio
from datetime import datetime, timezone
import json
import multiprocessing as mp
import os
from pathlib import Path
import platform
import random
import statistics
import tempfile
import time

from examples.services import service
from miniredis import resp
from miniredis.engine import Engine
from miniredis.persist import Journal

HOST = "127.0.0.1"
READY = r"mini-redis listening on \('127\.0\.0\.1', (\d+)\)"
KEYS = 10_000
VALUE = b"v" * 32


def _command(kind, rng):
    """부하 종류에 맞는 다음 명령 하나를 RESP2 바이트로 만든다."""
    if kind == "incr":
        return resp.encode([b"INCR", b"counter"])
    key = b"key:%d" % rng.randrange(KEYS)
    if kind == "set" or (kind == "mixed" and rng.random() < 0.5):
        return resp.encode([b"SET", key, VALUE])
    return resp.encode([b"GET", key])


async def _reply(reader):
    """응답 하나를 읽고 오류 응답이면 False를 돌려준다."""
    line = await reader.readuntil(b"\r\n")
    if line[:1] == b"$":
        size = int(line[1:-2])
        if size >= 0:
            await reader.readexactly(size + 2)
    return line[:1] != b"-"


async def _client(port, kind, seed, start, end, count, out):
    """연결 하나로 요청-응답을 반복하고 측정 구간 안에서 시작한 요청의 지연을 모은다."""
    reader, writer = await asyncio.open_connection(HOST, port)
    rng = random.Random(seed)
    sent = 0
    try:
        while True:
            if count is None and time.perf_counter() >= end:
                break
            if count is not None and sent >= count:
                break
            payload = _command(kind, rng)
            began = time.perf_counter_ns()
            writer.write(payload)
            ok = await _reply(reader)
            sent += 1
            if not ok:
                out["errors"] += 1
            if count is not None or began >= start * 1e9:
                out["latency_ns"].append(time.perf_counter_ns() - began)
    finally:
        writer.close()
        await writer.wait_closed()


def _worker(port, kind, connections, seed, start, end, count, queue):
    """한 프로세스에서 여러 연결을 동시에 돌리고 결과를 큐로 보낸다."""
    out = {"latency_ns": [], "errors": 0}

    async def run():
        await asyncio.gather(*(_client(port, kind, seed * 1000 + i, start, end, count, out)
                               for i in range(connections)))

    asyncio.run(run())
    queue.put(out)


def load(port, kind, connections, procs, warmup=0.5, duration=3.0, count=None):
    """여러 프로세스로 연결을 나눠 부하를 걸고 처리량과 지연 백분위를 계산한다.

    Args:
        port: 대상 서버 포트.
        kind: mixed(SET·GET 반반), set, incr 중 하나.
        connections: 전체 동시 연결 수.
        procs: 부하 생성 프로세스 수의 상한.
        warmup: 측정 전에 버리는 시간(초).
        duration: 측정 시간(초). count를 주면 무시한다.
        count: 연결마다 보낼 요청 수. None이면 시간으로 측정한다.
    """
    ctx = mp.get_context("spawn")
    queue = ctx.Queue()
    procs = min(procs, connections)
    shares = [connections // procs + (i < connections % procs) for i in range(procs)]
    begin = time.perf_counter() + 1.0  # 자식 프로세스 기동 시간
    start, end = begin + warmup, begin + warmup + duration
    workers = [ctx.Process(target=_worker, args=(port, kind, n, i + 1, start, end, count, queue))
               for i, n in enumerate(shares)]
    wall = time.perf_counter()
    for w in workers:
        w.start()
    results = [queue.get() for _ in workers]
    elapsed = time.perf_counter() - wall
    for w in workers:
        w.join()
    latency = sorted(x for r in results for x in r["latency_ns"])
    errors = sum(r["errors"] for r in results)
    seconds = duration if count is None else elapsed

    def pct(p):
        return latency[min(len(latency) - 1, int(len(latency) * p))] / 1e6

    return {"connections": connections, "requests": len(latency), "errors": errors,
            "ops_per_s": len(latency) / seconds, "p50_ms": pct(0.50), "p99_ms": pct(0.99)}


async def _send(port, commands):
    reader, writer = await asyncio.open_connection(HOST, port)
    writer.write(b"".join(resp.encode(c) for c in commands))
    replies = [await resp.read(reader) for _ in commands]
    writer.close()
    await writer.wait_closed()
    return replies


def prefill(port):
    """10,000개 키를 미리 채워 GET이 실제 값을 읽게 한다."""
    keys = [b"key:%d" % i for i in range(KEYS)]
    batches = [[b"MSET", *[x for k in keys[i:i + 500] for x in (k, VALUE)]] for i in range(0, KEYS, 500)]
    asyncio.run(_send(port, batches))


def server(directory, mode):
    """메모리 전용 또는 AOF 정책별로 서버를 띄운다."""
    if mode == "memory":
        args = ["--aof", ""]
    else:
        args = ["--aof", str(Path(directory) / "appendonly.aof"), "--appendfsync", mode]
    return service("miniredis", ["--addr", HOST + ":0", *args], READY)


def median_runs(runs):
    keys = ("ops_per_s", "p50_ms", "p99_ms")
    out = {k: statistics.median(r[k] for r in runs) for k in keys}
    out.update(connections=runs[0]["connections"], errors=sum(r["errors"] for r in runs),
               requests=sum(r["requests"] for r in runs), rounds=runs)
    return out


def concurrency(levels, rounds, procs, duration):
    """연결 수를 늘려 가며 SET·GET 혼합 부하의 처리량과 지연을 잰다."""
    samples = {c: [] for c in levels}
    with tempfile.TemporaryDirectory() as tmp, server(tmp, "memory") as port:
        prefill(port)
        for r in range(rounds):
            for c in (levels if r % 2 == 0 else reversed(levels)):
                samples[c].append(load(port, "mixed", c, procs, duration=duration))
                print(f"  concurrency c={c:<4} {samples[c][-1]['ops_per_s']:>9,.0f} ops/s", flush=True)
    return [median_runs(samples[c]) for c in levels]


def atomicity(connections, per_connection, procs):
    """여러 연결이 같은 키에 INCR을 동시에 보내도 결과가 정확한지 확인한다."""
    with tempfile.TemporaryDirectory() as tmp, server(tmp, "memory") as port:
        result = load(port, "incr", connections, procs, count=per_connection)
        final = int(asyncio.run(_send(port, [[b"GET", b"counter"]]))[0])
    expected = connections * per_connection
    print(f"  atomicity expected={expected} final={final}", flush=True)
    return {"connections": connections, "incr_per_connection": per_connection, "expected": expected,
            "final": final, "lost_updates": expected - final, "ops_per_s": result["ops_per_s"]}


def durability(modes, connections, rounds, procs, duration):
    """같은 SET 부하를 메모리 전용과 AOF 동기화 정책별로 비교한다."""
    samples = {m: [] for m in modes}
    for r in range(rounds):
        for m in (modes if r % 2 == 0 else list(reversed(modes))):
            with tempfile.TemporaryDirectory() as tmp, server(tmp, m) as port:
                samples[m].append(load(port, "set", connections, procs, duration=duration))
            print(f"  durability {m:<8} {samples[m][-1]['ops_per_s']:>9,.0f} ops/s", flush=True)
    return [dict(mode=m, **median_runs(samples[m])) for m in modes]


def _dir_size(path):
    return sum(f.stat().st_size for f in path.iterdir() if f.is_file())


def _restore(path, rounds):
    times, keys = [], 0
    for _ in range(rounds):
        began = time.perf_counter()
        engine = Engine()
        journal = Journal(str(path), "no", engine)
        times.append(time.perf_counter() - began)
        keys = len(engine.data)
        journal.close()
    return statistics.median(times), keys


def recovery(writes, keys, rounds):
    """같은 키를 여러 번 덮어쓴 AOF를 재작성 전후로 복구해 파일 크기와 시간을 비교한다."""
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "appendonly.aof"
        engine = Engine()
        journal = Journal(str(path), "no", engine)
        engine.journal = journal
        for i in range(writes):
            engine.execute([b"SET", b"key:%d" % (i % keys), b"%032d" % i])
        directory = journal.dir
        journal.close()
        before_size = _dir_size(directory)
        before_time, before_keys = _restore(path, rounds)

        engine = Engine()
        journal = Journal(str(path), "no", engine)
        engine.journal = journal
        journal.rewrite()
        while journal.step_rewrite():
            pass
        journal.close()
        after_size = _dir_size(directory)
        after_time, after_keys = _restore(path, rounds)
    if before_keys != keys or after_keys != keys:
        raise ValueError(f"복구된 키 수가 다릅니다: {before_keys}, {after_keys}")
    print(f"  recovery before {before_size:,}B {before_time:.3f}s / after {after_size:,}B {after_time:.3f}s", flush=True)
    return {"writes": writes, "keys": keys,
            "before_rewrite": {"bytes": before_size, "restore_s": before_time},
            "after_rewrite": {"bytes": after_size, "restore_s": after_time}}


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--connections", type=int, nargs="+", default=[1, 4, 16, 64, 256])
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--duration", type=float, default=3.0, help="측정 한 번의 길이(초)")
    parser.add_argument("--procs", type=int, default=max(1, (os.cpu_count() or 2) - 1),
                        help="부하 생성 프로세스 수 (기본: CPU 코어 수 - 1)")
    parser.add_argument("--only", nargs="+", choices=["concurrency", "atomicity", "durability", "recovery"],
                        default=["concurrency", "atomicity", "durability", "recovery"])
    parser.add_argument("--json", type=Path, metavar="PATH", help="결과를 기록할 JSON 파일")
    args = parser.parse_args()

    result = {
        "completed_at_utc": None,
        "environment": {"python": platform.python_version(), "platform": platform.platform(),
                        "machine": platform.machine(), "cpus": os.cpu_count()},
        "settings": {"rounds": args.rounds, "duration_s": args.duration, "load_procs": args.procs,
                     "keys": KEYS, "value_bytes": len(VALUE), "pipeline": 1},
    }
    if "concurrency" in args.only:
        result["concurrency"] = concurrency(args.connections, args.rounds, args.procs, args.duration)
    if "atomicity" in args.only:
        result["atomicity"] = atomicity(256, 400, args.procs)
    if "durability" in args.only:
        result["durability"] = durability(["memory", "everysec", "always"], 64, args.rounds, args.procs, args.duration)
    if "recovery" in args.only:
        result["recovery"] = recovery(200_000, 20_000, args.rounds)
    result["completed_at_utc"] = datetime.now(timezone.utc).isoformat()
    if args.json:
        args.json.parent.mkdir(parents=True, exist_ok=True)
        previous = json.loads(args.json.read_text(encoding="utf-8")) if args.json.exists() else {}
        previous.update(result)
        args.json.write_text(json.dumps(previous, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
