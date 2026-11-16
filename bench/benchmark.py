"""성능 벤치마크 CLI: python -m bench.benchmark --spawn
데모 앱(GET /api/dashboard)을 HTTP로 불러 "DBMS 직접"과 "Redis 캐시"의 응답 시간을 잰다.

  --spawn              미니 Redis와 데모 앱을 이 스크립트가 직접 (빈 포트로) 띄우고, 끝나면 종료한다
  --demo-url <url>     --spawn이 없을 때 이미 떠 있는 데모 앱 주소 (기본 http://127.0.0.1:4000)
  --requests <n>       측정할 요청 수 (기본 200)        --warmup <n>   워밍업 요청 수 (기본 20, 통계에서 제외)
  --concurrency <a,b>  동시 요청 수 목록 (기본 1,20)    --db <파일>    DB 파일 (기본 demo/data/demo.db)
  --out <폴더>         결과 저장 폴더 (기본 bench/results)   --no-save   파일로 저장하지 않음

주요 함수: parse_args(옵션), run_phase(동시 작업자), login/timed_get(HTTP 호출·시간 측정),
run_benchmark(DBMS / HIT / MISS 시나리오), format_markdown(표 출력), save_result(json+md 저장),
spawn_servers(--spawn 시 빈 포트에 미니 Redis·데모 서버 기동).
"""

import json
import os
import platform
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from demo.db import DEFAULT_DB_PATH, open_db
from bench.stats import summarize

ROOT = Path(__file__).resolve().parent.parent
USERNAME = "demo"
PASSWORD = "demo1234"  # 로컬 데모용 테스트 값 (demo/seed_users.py)
USAGE = "사용법: python -m bench.benchmark --spawn [--requests 200] [--warmup 20] [--concurrency 1,20] [--db <파일>] [--out <폴더>] [--no-save]"


def default_options():
    return {
        "requests": 200,
        "warmup": 20,
        "concurrency": [1, 20],
        "demo_url": "http://127.0.0.1:4000",
        "db": os.environ.get("DEMO_DB_PATH", str(DEFAULT_DB_PATH)),
        "out": str(ROOT / "bench" / "results"),
        "spawn": False,
        "save": True,
        "help": False,
    }


def parse_args(argv):
    options = default_options()

    def positive_int(name, raw):
        try:
            value = int(raw)
        except ValueError:
            value = 0
        if value < 1:
            raise ValueError(f"{name}는 1 이상의 정수여야 합니다 (받은 값: {raw})")
        return value

    i = 0
    while i < len(argv):
        arg = argv[i]

        def value():
            nonlocal i
            if i + 1 >= len(argv):
                raise ValueError(f"{arg}에는 값이 필요합니다")
            i += 1
            return argv[i]

        if arg == "--spawn":
            options["spawn"] = True
        elif arg == "--no-save":
            options["save"] = False
        elif arg == "--requests":
            options["requests"] = positive_int(arg, value())
        elif arg == "--warmup":
            raw = value()
            try:
                warmup = int(raw)
            except ValueError:
                warmup = -1
            if warmup < 0:
                raise ValueError(f"--warmup은 0 이상의 정수여야 합니다 (받은 값: {raw})")
            options["warmup"] = warmup
        elif arg == "--concurrency":
            options["concurrency"] = [positive_int("--concurrency", v.strip()) for v in value().split(",")]
        elif arg == "--demo-url":
            options["demo_url"] = value().rstrip("/")
        elif arg == "--db":
            options["db"] = value()
        elif arg == "--out":
            options["out"] = value()
        elif arg in ("--help", "-h"):
            options["help"] = True
        else:
            raise ValueError(f"알 수 없는 옵션: {arg}")
        i += 1
    return options


# ---------- 측정 ----------


def run_phase(count, concurrency, task):
    """concurrency개의 작업자가 count개의 요청을 나눠서 처리한다. 걸린 전체 시간(wall_ms)도 돌려준다."""
    results = [None] * count
    next_index = [0]
    lock = threading.Lock()

    def worker():
        while True:
            with lock:
                i = next_index[0]
                if i >= count:
                    return
                next_index[0] += 1
            results[i] = task()

    started = time.perf_counter()
    threads = [threading.Thread(target=worker) for _ in range(min(concurrency, count))]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return results, (time.perf_counter() - started) * 1000


def login(demo_url):
    body = json.dumps({"username": USERNAME, "password": PASSWORD}).encode("utf-8")
    req = urllib.request.Request(f"{demo_url}/api/login", data=body, method="POST", headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as res:
            return res.headers.get_all("Set-Cookie")[0].split(";")[0]
    except urllib.error.HTTPError as err:
        raise RuntimeError(f"로그인에 실패했습니다 (HTTP {err.code}). 데모 앱과 미니 Redis가 떠 있는지, python -m demo.seed_users로 계정을 만들었는지 확인하세요.")


def timed_get(demo_url, cookie, query):
    """요청 하나의 걸린 시간(ms)을 잰다: 요청을 보내고 응답 본문을 다 읽을 때까지."""
    started = time.perf_counter()
    req = urllib.request.Request(f"{demo_url}/api/dashboard?{query}", headers={"Cookie": cookie})
    try:
        with urllib.request.urlopen(req, timeout=10) as res:
            payload = json.loads(res.read().decode("utf-8"))
            headers = res.headers
            status = res.status
    except urllib.error.HTTPError as err:
        raise RuntimeError(f"대시보드 요청이 실패했습니다 (HTTP {err.code})")
    ms = (time.perf_counter() - started) * 1000
    return {"ms": ms, "serverMs": payload["elapsedMs"], "cache": headers.get("X-Cache"), "source": headers.get("X-Source"), "status": status}


def run_benchmark(*, demo_url, requests, warmup, concurrency, db_info=None, delay_ms=None):
    cookie = login(demo_url)

    def clear_cache():
        req = urllib.request.Request(f"{demo_url}/api/dashboard/cache", method="DELETE", headers={"Cookie": cookie})
        with urllib.request.urlopen(req, timeout=10):
            pass

    def miss_task():
        clear_cache()
        return timed_get(demo_url, cookie, "mode=redis")

    scenarios = [
        {"id": "db", "label": "DBMS 직접 (SQLite)", "expect": "BYPASS", "levels": concurrency, "task": lambda: timed_get(demo_url, cookie, "mode=db")},
        {"id": "hit", "label": "Redis 캐시 HIT", "expect": "HIT", "levels": concurrency, "prepare": clear_cache, "task": lambda: timed_get(demo_url, cookie, "mode=redis")},
        {
            # 요청마다 캐시를 지운 뒤 호출한다. 지우는 시간은 재지 않는다. 동시 요청이면 MISS가 보장되지 않으므로 동시성 1에서만 잰다.
            "id": "miss",
            "label": "Redis 캐시 MISS (DBMS + Redis)",
            "expect": "MISS",
            "levels": [1],
            "task": miss_task,
        },
    ]

    rows = []
    for scenario in scenarios:
        for level in scenario["levels"]:
            if "prepare" in scenario:
                scenario["prepare"]()
            run_phase(warmup, level, scenario["task"])  # 워밍업: 연결과 캐시를 데우는 용도라 통계에서 제외한다
            results, wall_ms = run_phase(requests, level, scenario["task"])
            unexpected = sum(1 for r in results if r["cache"] != scenario["expect"])
            rows.append(
                {
                    "scenario": scenario["id"],
                    "label": scenario["label"],
                    "concurrency": level,
                    "requests": requests,
                    "warmup": warmup,
                    "client": summarize([r["ms"] for r in results]),
                    "server": summarize([r["serverMs"] for r in results]),
                    "throughput": None if scenario["id"] == "miss" else requests / (wall_ms / 1000),
                    "unexpected": unexpected,
                }
            )

    return {
        "measuredAt": datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "command": f"python -m bench.benchmark {' '.join(sys.argv[1:])}".strip(),
        "options": {"requests": requests, "warmup": warmup, "concurrency": concurrency},
        "env": {
            "python": platform.python_version(),
            "os": f"{platform.system()} {platform.release()} {platform.machine()}",
            "cpu": platform.processor() or "알 수 없음",
            "cores": os.cpu_count() or 0,
            "dbRows": db_info["rows"] if db_info else None,
            "dbIndex": db_info["indexed"] if db_info else None,
            "delayMs": delay_ms,
        },
        "rows": rows,
    }


# ---------- 출력 ----------


def _ms(value):
    return "-" if value is None else f"{value:.2f}"


def format_markdown(result):
    env, options = result["env"], result["options"]
    db_rows = "확인 못 함" if env["dbRows"] is None else f"{env['dbRows']:,}"
    delay = "확인 못 함 (외부에서 실행한 서버)" if env["delayMs"] is None else f"{env['delayMs']}ms"
    lines = [
        f"# 벤치마크 결과 ({result['measuredAt']})",
        "",
        "## 환경",
        "",
        "| 항목 | 값 |",
        "|---|---|",
        f"| 측정 일시 | {result['measuredAt']} |",
        f"| 명령 | `{result['command']}` |",
        f"| Python | {env['python']} |",
        f"| OS | {env['os']} |",
        f"| CPU | {env['cpu']} ({env['cores']}코어) |",
        f"| DB 행 수 | {db_rows} |",
        f"| username 인덱스 (DB_INDEX) | {env['dbIndex'] or '확인 못 함'} |",
        f"| 인위적 지연 (DASHBOARD_DELAY_MS) | {delay} |",
        f"| 요청 수 / 워밍업 | {options['requests']} / {options['warmup']} (워밍업은 통계에서 제외) |",
        "",
        "## 결과 (ms, 클라이언트에서 잰 HTTP 요청 왕복 시간)",
        "",
        "| 시나리오 | 동시성 | 평균 | p50 | p95 | p99 | 최소 | 최대 | 처리량 (req/s) | 서버 내부 p50 |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for r in result["rows"]:
        c = r["client"]
        throughput = "-" if r["throughput"] is None else f"{r['throughput']:.0f}"
        lines.append(f"| {r['label']} | {r['concurrency']} | {_ms(c['mean'])} | {_ms(c['p50'])} | {_ms(c['p95'])} | {_ms(c['p99'])} | {_ms(c['min'])} | {_ms(c['max'])} | {throughput} | {_ms(r['server']['p50'])} |")
    bad = [r for r in result["rows"] if r["unexpected"] > 0]
    if bad:
        lines += ["", "> 주의: 예상과 다른 응답이 있었습니다: " + ", ".join(f"{r['label']}(동시성 {r['concurrency']}) {r['unexpected']}건" for r in bad)]
    lines += [
        "",
        "- 서버 내부 p50: 데모 앱이 응답 본문에 담아 준 `elapsedMs`의 중앙값 (HTTP 왕복을 뺀, 서버 안에서 조회에 쓴 시간).",
        "- 처리량은 측정 구간의 전체 시간으로 나눈 값입니다. MISS는 요청마다 캐시 삭제 호출이 끼어서 구하지 않습니다.",
        "- 인덱스 유무에 따라 결과가 크게 달라지므로 `DB_INDEX=off`와 `DB_INDEX=on`으로 각각 시드하고 두 번 재서 비교하세요.",
    ]
    return "\n".join(lines) + "\n"


def save_result(result, directory):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    stamp = result["measuredAt"].replace(":", "-").replace(".", "-").replace("T", "_").replace("Z", "")
    base = directory / f"benchmark_{stamp}_index-{result['env']['dbIndex'] or 'unknown'}"
    base.with_suffix(".json").write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    base.with_suffix(".md").write_text(format_markdown(result), encoding="utf-8")
    return base


# ---------- 서버 띄우기 (--spawn) ----------


def free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def wait_until_ready(url, proc, timeout=15.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"서버가 시작하지 못하고 종료했습니다 ({url}):\n{proc.stderr.read().decode('utf-8', 'replace')}")
        try:
            with urllib.request.urlopen(url, timeout=0.5) as res:
                if res.status == 200:
                    return
        except (urllib.error.URLError, OSError):
            pass  # 아직 준비 중
        time.sleep(0.1)
    raise RuntimeError(f"서버가 {timeout}초 안에 준비되지 않았습니다 ({url})")


def spawn_servers(db_path):
    redis_port, demo_port = free_port(), free_port()
    redis_url = f"http://127.0.0.1:{redis_port}"
    demo_url = f"http://127.0.0.1:{demo_port}"
    procs = []

    def start(module, extra_env):
        env = {**os.environ, **extra_env, "DASHBOARD_DELAY_MS": "0", "PYTHONUNBUFFERED": "1"}
        proc = subprocess.Popen([sys.executable, "-m", module], cwd=ROOT, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        procs.append(proc)
        return proc

    def stop():
        for proc in procs:
            if proc.poll() is None:
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()

    redis_proc = start("src.server", {"PORT": str(redis_port)})
    demo_proc = start("demo.server", {"PORT": str(demo_port), "REDIS_URL": redis_url, "DEMO_DB_PATH": db_path})
    try:
        wait_until_ready(f"{redis_url}/health", redis_proc)
        wait_until_ready(f"{demo_url}/", demo_proc)
    except RuntimeError:
        stop()
        raise
    return demo_url, stop


def main(argv=None):
    try:
        options = parse_args(sys.argv[1:] if argv is None else argv)
    except ValueError as err:
        print(f"{err}\n{USAGE}", file=sys.stderr)
        return 1
    if options["help"]:
        print(USAGE)
        return 0

    db_info = None
    try:
        db = open_db(options["db"])
        db_info = db.info()
        db.close()
    except Exception as err:
        print(str(err), file=sys.stderr)
        if options["spawn"]:
            return 1  # 직접 띄울 때는 DB가 없으면 데모 앱도 못 뜬다

    stop = None
    try:
        if options["spawn"]:
            demo_url, stop = spawn_servers(options["db"])
        else:
            demo_url = options["demo_url"]
        result = run_benchmark(
            demo_url=demo_url,
            requests=options["requests"],
            warmup=options["warmup"],
            concurrency=options["concurrency"],
            db_info=db_info,
            delay_ms=0 if options["spawn"] else None,
        )
        print(format_markdown(result))
        if options["save"]:
            print(f"결과를 저장했습니다: {save_result(result, options['out'])}.{{json,md}}")
        return 0
    except (RuntimeError, OSError) as err:
        print(str(err), file=sys.stderr)
        return 1
    finally:
        if stop:
            stop()


if __name__ == "__main__":
    sys.exit(main())
