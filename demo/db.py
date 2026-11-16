"""대시보드의 "원본" 데이터 계층: 표준 라이브러리 sqlite3(SQLite, 디스크 파일 기반 SQL DBMS)를 쓴다.

get_dashboard_stats가 데이터 출처의 경계다. 다른 DBMS로 바꿀 때 이 함수만 같은 모양으로 구현하면 된다.

주요 함수: open_db(읽기 전용으로 열고 시드 여부 확인), DashboardDb.get_dashboard_stats(사용자별 집계),
DashboardDb.info(seed 때 남긴 meta 읽기), DbNotSeededError(DB 없음/시드 안 됨 안내).
"""

import sqlite3
import threading
from pathlib import Path

DEFAULT_DB_PATH = Path(__file__).resolve().parent / "data" / "demo.db"


class DbNotSeededError(Exception):
    """시드가 안 된 DB를 열려고 했다. 메시지는 사용자에게 그대로 보여 준다."""

    def __init__(self, file):
        super().__init__(f"데모 DB가 준비되지 않았습니다 ({file}). `python -m demo.seed_db`를 먼저 실행하세요.")


class DashboardDb:
    def __init__(self, conn):
        self._conn = conn
        self._lock = threading.Lock()  # sqlite 연결 하나를 여러 스레드가 함께 쓰므로 직렬화한다

    def get_dashboard_stats(self, username):
        """사용자별 통계. 같은 DB와 같은 사용자면 항상 같은 값을 돌려준다 (캐시와 직접 조회의 내용이 같아야 하므로 시각 같은 값은 넣지 않는다)."""
        with self._lock:
            rows = self._conn.execute(
                "SELECT type, COUNT(*) AS cnt, SUM(amount) AS total FROM events WHERE username = ? GROUP BY type ORDER BY type",
                (username,),
            ).fetchall()
        return {
            "username": username,
            "totalEvents": sum(r[1] for r in rows),
            "totalAmount": sum(r[2] for r in rows),
            "byType": [{"type": r[0], "count": r[1], "total": r[2]} for r in rows],
        }

    def info(self):
        with self._lock:
            meta = dict(self._conn.execute("SELECT key, value FROM meta").fetchall())
        return {"rows": int(meta["rows"]), "indexed": meta["indexed"], "seed": int(meta["seed"]), "seededAt": meta["seeded_at"]}

    def close(self):
        with self._lock:
            self._conn.close()


def open_db(file=DEFAULT_DB_PATH):
    file = Path(file)
    # 파일이 없을 때 connect하면 빈 DB 파일이 새로 만들어지므로, 먼저 존재를 확인한다
    if not file.exists():
        raise DbNotSeededError(file)
    # 앱은 읽기만 하므로 읽기 전용으로 연다 (URI 모드 mode=ro)
    conn = sqlite3.connect(file.resolve().as_uri() + "?mode=ro", uri=True, check_same_thread=False)
    try:
        names = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type = 'table' AND name IN ('events', 'meta')")}
        if names != {"events", "meta"}:
            raise DbNotSeededError(file)
    except sqlite3.Error:
        conn.close()
        raise DbNotSeededError(file)
    except DbNotSeededError:
        conn.close()
        raise
    return DashboardDb(conn)
