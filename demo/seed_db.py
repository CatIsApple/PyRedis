"""벤치마크용 SQLite DB를 만든다: python -m demo.seed_db
  SEED_ROWS=300000 (기본)   DB_INDEX=off (기본) | on     DEMO_DB_PATH=<파일 경로>
같은 SEED_ROWS와 같은 시드면 항상 같은 데이터가 만들어진다 (재현 가능).

주요 함수: create_rng(mulberry32 의사난수), seed_db(events 표 생성·행 삽입·선택적 username 인덱스·meta 기록).
"""

import os
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path

from .db import DEFAULT_DB_PATH

DEFAULT_ROWS = 300_000
DEFAULT_SEED = 20261113
USERNAMES = ["demo", "alice", "bob", "carol", "dave", "erin", "frank", "grace"]
EVENT_TYPES = ["view", "click", "purchase", "refund", "login"]
MASK32 = 0xFFFFFFFF


def create_rng(seed):
    """mulberry32: 같은 시드면 같은 수열을 만드는 간단한 의사난수 생성기 (0 이상 1 미만)."""
    state = [seed & MASK32]

    def rng():
        state[0] = (state[0] + 0x6D2B79F5) & MASK32
        t = state[0]
        t = ((t ^ (t >> 15)) * (t | 1)) & MASK32
        t ^= (t + (((t ^ (t >> 7)) * (t | 61)) & MASK32)) & MASK32
        return ((t ^ (t >> 14)) & MASK32) / 4294967296

    return rng


def seed_db(file=DEFAULT_DB_PATH, rows=DEFAULT_ROWS, index=False, seed=DEFAULT_SEED):
    if isinstance(rows, bool) or not isinstance(rows, int) or rows < 0:
        raise ValueError("rows는 0 이상의 정수여야 합니다")
    file = Path(file)
    file.parent.mkdir(parents=True, exist_ok=True)
    started = time.perf_counter()
    conn = sqlite3.connect(file, isolation_level=None)  # 트랜잭션은 BEGIN/COMMIT으로 직접 연다
    try:
        conn.executescript(
            """
            DROP TABLE IF EXISTS events;
            DROP TABLE IF EXISTS meta;
            CREATE TABLE events (id INTEGER PRIMARY KEY, username TEXT, type TEXT, amount INTEGER, created_at INTEGER);
            CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT);
            """
        )
        rng = create_rng(seed)
        base_time = int(datetime(2026, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
        batch = [
            (
                USERNAMES[int(rng() * len(USERNAMES))],
                EVENT_TYPES[int(rng() * len(EVENT_TYPES))],
                int(rng() * 10_000),
                base_time + int(rng() * 300 * 24 * 3600 * 1000),
            )
            for _ in range(rows)
        ]
        conn.execute("BEGIN")  # 한 트랜잭션으로 넣어야 행마다 디스크에 쓰지 않아서 빠르다
        conn.executemany("INSERT INTO events (username, type, amount, created_at) VALUES (?, ?, ?, ?)", batch)
        conn.execute("COMMIT")
        if index:  # 인덱스는 데이터를 다 넣은 뒤에 만드는 편이 빠르다
            conn.execute("CREATE INDEX idx_events_username ON events (username)")
        meta = [("rows", str(rows)), ("indexed", "on" if index else "off"), ("seed", str(seed)), ("seeded_at", datetime.now(timezone.utc).isoformat())]
        conn.executemany("INSERT INTO meta (key, value) VALUES (?, ?)", meta)
    finally:
        conn.close()
    return {"file": str(file), "rows": rows, "indexed": "on" if index else "off", "ms": round((time.perf_counter() - started) * 1000)}


def _env_int(name, fallback):
    raw = os.environ.get(name)
    if raw is None:
        return fallback
    try:
        value = int(raw)
    except ValueError:
        value = -1
    if value < 0:
        raise ValueError(f"{name}는 0 이상의 정수여야 합니다 (받은 값: {raw})")
    return value


if __name__ == "__main__":
    index = os.environ.get("DB_INDEX", "off").lower()
    if index not in ("on", "off"):
        raise SystemExit(f"DB_INDEX는 on 또는 off여야 합니다 (받은 값: {os.environ.get('DB_INDEX')})")
    result = seed_db(
        file=os.environ.get("DEMO_DB_PATH", DEFAULT_DB_PATH),
        rows=_env_int("SEED_ROWS", DEFAULT_ROWS),
        index=index == "on",
    )
    print(f"{result['file']}: events {result['rows']:,}행, username 인덱스 {result['indexed']}, {result['ms']}ms")
