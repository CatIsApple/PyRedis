"""Redis 해시 필드 저장소를 바꿨을 때 실제 HSET과 HGET의 실행 시간을 비교한다."""
import argparse
import gc
import hashlib
import io
import json
import os
from pathlib import Path
import platform
import random
import shutil
import statistics
import subprocess
import sys
import tempfile
import time
import zipfile

ROOT = Path(__file__).resolve().parents[1]
REVISIONS = {
    "dict": "9f67b743ae79e2bc80a1fcaf267a7589c0a1b260",
    "HashTable": "de7f275253c67b587bd757079fd3bf20b3deb30b",
}
SEED = 271828
PHASES = ("hset_insert", "hset_update", "hget_hit", "hget_miss")


def commands(size, count):
    """각 필드를 한 번 삽입·갱신·조회하는 동일한 바이너리 요청을 미리 구성한다.

    Args:
        size: Redis 해시 키 하나에 저장할 필드 수.
        count: 한 단계에서 처리할 총 명령 수. size의 배수이다.
    """
    rng = random.Random(SEED)
    result = {}
    for phase in PHASES:
        order = list(range(count))
        if phase != "hset_insert":
            rng.shuffle(order)
        trace, expected = [], []
        for index in order:
            key = b"k:" + (index // size).to_bytes(4, "big")
            field = (b"\xff" if phase == "hget_miss" else b"\x00") + (index % size).to_bytes(4, "big")
            suffix = index.to_bytes(4, "big")
            if phase == "hset_insert":
                trace.append((b"HSET", key, field, b"v1:" + suffix))
                expected.append(1)
            elif phase == "hset_update":
                trace.append((b"HSET", key, field, b"v2:" + suffix))
                expected.append(0)
            else:
                trace.append((b"HGET", key, field))
                expected.append(None if phase == "hget_miss" else b"v2:" + suffix)
        result[phase] = trace, expected
    return result


def run_size(engine_class, table_class, backend, size, count):
    """빈 엔진에서 네 단계를 측정하고 모든 응답과 마지막 필드 내용을 검증한다.

    Args:
        engine_class: 선택한 구현에서 가져온 Engine 클래스.
        table_class: 두 구현이 공유하는 외부 키 공간의 HashTable 클래스.
        backend: 예상하는 필드 저장소 이름(dict 또는 HashTable).
        size: 해시 키 한 개에 삽입할 필드 수.
        count: 단계당 명령 수.
    """
    traces = commands(size, count)
    engine = engine_class()
    if type(engine.data) is not table_class or engine.journal is not None:
        raise ValueError("공통 HashTable 키 공간과 메모리 전용 엔진이 필요합니다.")
    execute = engine.execute
    timings = {}
    for phase, (trace, expected) in traces.items():
        gc.collect()
        start = time.perf_counter_ns()
        replies = [execute(cmd, now=1) for cmd in trace]
        elapsed = time.perf_counter_ns() - start
        if replies != expected:
            raise ValueError(f"{backend}, {size}개 필드, {phase}: 명령 응답 불일치")
        timings[phase] = elapsed / count
        del replies
    fields_class = dict if backend == "dict" else table_class
    groups = count // size
    if len(engine.data) != groups:
        raise ValueError("최종 해시 키 수 불일치")
    for group in range(groups):
        fields = engine.data[b"k:" + group.to_bytes(4, "big")].value
        if type(fields) is not fields_class or len(fields) != size:
            raise ValueError(f"{backend}: 최종 필드 저장소 또는 필드 수 불일치")
        for i in range(size):
            index = group * size + i
            if fields[b"\x00" + i.to_bytes(4, "big")] != b"v2:" + index.to_bytes(4, "big"):
                raise ValueError(f"{backend}: 최종 필드 값 불일치")
    return timings


def run_worker(source, backend, sizes, operations):
    """선택한 임시 소스만 임포트한 프로세스에서 준비 운동과 실제 측정을 실행한다.

    Args:
        source: 이 프로세스에서 불러올 miniredis 패키지의 상위 디렉터리.
        backend: 이 구현에서 사용할 필드 저장소의 이름.
        sizes: 키 하나당 필드 수를 나타내는 시나리오 목록.
        operations: 시나리오별 단계당 목표 명령 수.
    """
    sys.path.insert(0, str(source))
    from miniredis import engine, hashtable
    if Path(engine.__file__).resolve() != source / "miniredis" / "engine.py":
        raise RuntimeError("잘못된 소스 트리에서 엔진을 불러왔습니다.")
    run_size(engine.Engine, hashtable.HashTable, backend, 8, 256)
    return {str(size): run_size(engine.Engine, hashtable.HashTable, backend, size,
                                max(1, operations // size) * size) for size in sizes}


def benchmark(rounds, sizes, operations):
    """두 커밋의 엔진을 임시 디렉터리에 복원하고 실행 순서를 번갈아 바꿔 측정한다.

    Args:
        rounds: 두 구현을 한 번씩 실행할 비교 라운드 수.
        sizes: 한 해시 키에 저장할 필드 수 목록.
        operations: 시나리오와 명령 단계마다 실행할 목표 명령 수.
    """
    with tempfile.TemporaryDirectory() as temp:
        root = Path(temp)
        versions = {}
        for name, revision in REVISIONS.items():
            archive = subprocess.check_output(["git", "-C", str(ROOT), "archive", "--format=zip",
                                               revision, "miniredis"])
            with zipfile.ZipFile(io.BytesIO(archive)) as src:
                src.extractall(root / name)
            content = (root / name / "miniredis" / "engine.py").read_bytes()
            versions[name] = {"engine_revision": revision, "engine_sha256": hashlib.sha256(content).hexdigest()}
        # 최초 측정과 같은 외부 키 조회 코드를 두 엔진에 적용한다.
        shared = root / "HashTable" / "miniredis" / "hashtable.py"
        shutil.copyfile(shared, root / "dict" / "miniredis" / "hashtable.py")
        shared_digest = hashlib.sha256(shared.read_bytes()).hexdigest()
        samples = []
        for i in range(rounds):
            order = ["dict", "HashTable"] if i % 2 == 0 else ["HashTable", "dict"]
            row = {"round": i + 1, "order": order, "ns_per_op": {}}
            for backend in order:
                cmd = [sys.executable, "-B", str(Path(__file__).resolve()),
                       "--worker-root", str(root / backend), "--backend", backend,
                       "--operations", str(operations), "--sizes", *map(str, sizes)]
                output = subprocess.check_output(cmd, cwd=root, text=True,
                                                 env=dict(os.environ, PYTHONHASHSEED=str(SEED)))
                row["ns_per_op"][backend] = json.loads(output)
            samples.append(row)
    summary = {}
    for size in map(str, sizes):
        summary[size] = {}
        for phase in PHASES:
            medians = {name: statistics.median(s["ns_per_op"][name][size][phase] for s in samples)
                       for name in ("dict", "HashTable")}
            summary[size][phase] = {
                "dict_ns_per_op": medians["dict"], "hashtable_ns_per_op": medians["HashTable"],
                "hashtable_over_dict": medians["HashTable"] / medians["dict"]}
    return {
        "python": sys.version, "platform": platform.platform(), "machine": platform.machine(),
        "pythonhashseed": SEED, "trace_seed": SEED, "sources": versions,
        "shared_hashtable_sha256": shared_digest,
        "method": [
            "두 엔진은 각각 고정된 커밋에서 복원하며, 외부 키 저장소는 모두 de7f275 커밋의 HashTable을 쓴다.",
            "라운드별·구현별 독립 프로세스에서 256개 명령씩 네 단계를 예열한 후 새 엔진으로 측정한다.",
            "바이너리 인자를 사전 생성한다. 삽입은 해시 키마다 순차 실행하고 갱신·조회는 같은 시드로 섞는다.",
            "단계마다 GC를 먼저 수행한다. 시간에는 GC가 활성화된 실제 Engine.execute 호출과 응답 목록 수집이 포함된다.",
            "고정 시각, 단일 프로세스, 저널 미설정. 네트워크·RESP 파싱·디스크 저장은 측정에 포함되지 않는다.",
            "각 단계 총 경과 시간을 명령 수로 나눈 ns/op를 라운드별로 기록한다. 요약은 라운드 중앙값이다.",
            "모든 응답을 예상값과 비교하고, 측정 후 키 수·필드 수·저장소 종류와 모든 최종 필드 값을 확인한다.",
        ],
        "rounds": rounds,
        "scenarios": [{"fields_per_hash": n, "hash_keys": max(1, operations // n),
                       "commands_per_phase_per_round": max(1, operations // n) * n} for n in sizes],
        "per_round": samples, "summary": summary,
    }


def main():
    """명령줄에서 측정 조건을 읽고 결과를 터미널과 지정한 JSON 파일에 기록한다."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rounds", type=int, default=8, help="각 구현의 측정 라운드 수")
    parser.add_argument("--operations", type=int, default=16384, help="단계당 목표 명령 수")
    parser.add_argument("--sizes", type=int, nargs="+", default=[16, 4096], help="키당 해시 필드 수")
    parser.add_argument("--json", type=Path, metavar="PATH", help="전체 측정 결과를 저장할 JSON 경로")
    parser.add_argument("--worker-root", type=Path, help=argparse.SUPPRESS)
    parser.add_argument("--backend", choices=("dict", "HashTable"), help=argparse.SUPPRESS)
    args = parser.parse_args()
    if min(args.rounds, args.operations, *args.sizes) < 1 or len(set(args.sizes)) != len(args.sizes):
        parser.error("라운드, 명령 수, 필드 수는 양수이며 필드 수는 중복될 수 없습니다.")
    if args.worker_root:
        print(json.dumps(run_worker(args.worker_root.resolve(), args.backend, args.sizes, args.operations)))
        return
    result = benchmark(args.rounds, args.sizes, args.operations)
    if args.json:
        args.json.parent.mkdir(parents=True, exist_ok=True)
        args.json.write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print("fields  phase          dict ns/op  HashTable ns/op  ratio")
    for size, phases in result["summary"].items():
        for phase, vals in phases.items():
            print(f"{size:>6}  {phase:12}  {vals['dict_ns_per_op']:10.1f}  "
                  f"{vals['hashtable_ns_per_op']:15.1f}  {vals['hashtable_over_dict']:5.2f}x")


if __name__ == "__main__":
    main()
