"""Durable HardState, log and snapshot for a single static Raft group."""

import sqlite3
from tempfile import SpooledTemporaryFile
from pathlib import Path


SNAPSHOT_CHUNK = 1 << 20


class RaftStorage:
    def __init__(self, directory, local, topology):
        """노드 전용 SQLite 저장소를 열고 디렉터리의 소유 노드와 클러스터 배치를 검증한다.

        Args:
            directory: Raft 로그와 스냅샷을 저장할 디렉터리.
            local: 이 노드를 식별하는 클라이언트 주소.
            topology: 현재 클러스터 배치의 식별자.

        Raises:
            ValueError: SQLite 파일 없이 GoRedis의 Raft 파일만 있거나 기존 저장소의 노드 또는 배치가 다른 경우.
        """
        directory = Path(directory)
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / "raft.sqlite3"
        if not path.exists() and (directory / "raft.db").exists():
            raise ValueError("this directory contains GoRedis BoltDB Raft data")
        self.db = sqlite3.connect(path, timeout=0.1)
        try:
            # SQLite retains the exclusive lock until close, preventing two FSMs
            # from writing the same term, vote or log from different processes.
            self.db.execute("PRAGMA locking_mode=EXCLUSIVE")
            self.db.execute("PRAGMA synchronous=FULL")
            with self.db:
                self.db.execute("BEGIN EXCLUSIVE")
                self.db.execute("CREATE TABLE IF NOT EXISTS state (key TEXT PRIMARY KEY, value)")
                self.db.execute("""CREATE TABLE IF NOT EXISTS log
                                   (idx INTEGER PRIMARY KEY, term INTEGER NOT NULL,
                                    command BLOB NOT NULL)""")
                self.db.execute("""CREATE TABLE IF NOT EXISTS snapshot
                                   (part INTEGER PRIMARY KEY, data BLOB NOT NULL)""")
                identity = topology + "\0" + local
                stored = self.get("identity")
                if stored is None:
                    self.set("identity", identity)
                elif stored != identity:
                    raise ValueError("Raft directory belongs to another node or cluster topology")
            options = (row[0] for row in self.db.execute("PRAGMA compile_options"))
            length = next((int(opt.split("=")[1]) for opt in options if opt.startswith("MAX_LENGTH=")), 1000000000)
            self.max_record = min(length, 1 << 30) - (1 << 20)
        except BaseException:
            self.db.close()
            raise

    def get(self, key, default=None):
        """Raft 상태 테이블에서 지정한 키의 값을 조회한다.

        Args:
            key: 읽을 상태 항목의 이름.
            default: 키가 없을 때 반환할 값.
        """
        row = self.db.execute("SELECT value FROM state WHERE key=?", (key,)).fetchone()
        return default if row is None else row[0]

    def set(self, key, value):
        """현재 트랜잭션에서 Raft 상태 항목의 값을 저장하거나 교체한다.

        Args:
            key: 저장할 상태 항목의 이름.
            value: SQLite 상태 테이블에 기록할 값.
        """
        self.db.execute("INSERT OR REPLACE INTO state VALUES (?, ?)", (key, value))

    def last_index(self):
        """마지막 로그 인덱스를 반환하고 남은 항목이 없으면 스냅샷의 기준 인덱스를 사용한다."""
        return self.db.execute("SELECT max(idx) FROM log").fetchone()[0] or self.get("base", 0)

    def term_at(self, index):
        """지정한 위치에 기록된 로그 또는 스냅샷 기준점의 임기를 조회한다.

        Args:
            index: 조회할 로그 인덱스.

        Returns:
            해당 위치의 임기. 초기 기준점은 0이며 보관하지 않는 위치는 None이다.
        """
        if index == self.get("base", 0):
            return self.get("base_term", 0)
        row = self.db.execute("SELECT term FROM log WHERE idx=?", (index,)).fetchone()
        return None if row is None else row[0]

    def entries(self, start, count):
        """시작 인덱스부터 로그를 오름차순으로 조회한다.

        Args:
            start: 조회를 시작할 로그 인덱스.
            count: 가져올 최대 항목 수.

        Returns:
            항목별 임기와 직렬화된 명령 바이트의 쌍을 담은 목록.
        """
        return self.db.execute(
            "SELECT term, command FROM log WHERE idx>=? ORDER BY idx LIMIT ?", (start, count)
        ).fetchall()

    def append(self, start, entries):
        """주어진 항목을 시작 인덱스부터 연속된 위치에 기록한다.

        Args:
            start: 첫 항목에 부여할 로그 인덱스.
            entries: 순서대로 기록할 임기와 명령 바이트의 쌍.
        """
        self.db.executemany("INSERT INTO log VALUES (?, ?, ?)",
                            ((i, term, cmd) for i, (term, cmd) in enumerate(entries, start)))

    def truncate(self, start):
        """지정한 인덱스부터 로그의 끝까지 삭제한다.

        Args:
            start: 삭제할 첫 번째 로그 인덱스.
        """
        self.db.execute("DELETE FROM log WHERE idx>=?", (start,))

    def batch(self, start, count, budget):
        """항목 수와 바이트 예산에 맞춰 한 번에 복제할 로그를 읽는다.

        Args:
            start: 복제를 시작할 로그 인덱스.
            count: 읽을 수 있는 최대 항목 수.
            budget: 명령 바이트 합계의 목표 상한. 첫 항목이 더 크면 그 항목 하나를 읽는다.
        """
        sizes = self.db.execute("SELECT length(command) FROM log WHERE idx>=? ORDER BY idx LIMIT ?",
                                (start, count))
        length, total = 0, 0
        for (size,) in sizes:
            if length and total + size > budget:
                break
            length += 1
            total += size
        return self.entries(start, length)

    def snapshot_part(self, part):
        """현재 스냅샷에서 지정한 번호의 조각을 읽는다.

        Args:
            part: 0부터 시작하는 저장된 조각 번호.
        """
        return self.db.execute("SELECT data FROM snapshot WHERE part=?", (part,)).fetchone()[0]

    def snapshot_stream(self):
        """저장된 조각을 순서대로 이어 붙여 처음부터 읽을 수 있는 임시 스트림을 반환한다.

        Returns:
            호출자가 사용한 뒤 닫아야 하는 바이너리 스트림.
        """
        stream = SpooledTemporaryFile(max_size=SNAPSHOT_CHUNK)
        try:
            for (data,) in self.db.execute("SELECT data FROM snapshot ORDER BY part"):
                stream.write(data)
            stream.seek(0)
            return stream
        except BaseException:
            stream.close()
            raise

    def checkpoint(self, index, term, clock, stream):
        """호출자의 트랜잭션 안에서 스냅샷과 기준점을 교체하고 포함된 로그를 삭제한다.

        Args:
            index: 스냅샷이 포함하는 마지막 로그 인덱스.
            term: 해당 로그 인덱스의 임기.
            clock: 상태 머신에서 마지막으로 적용한 밀리초 단위 논리 시각.
            stream: 처음으로 되감아 읽을 수 있는 RDB 스냅샷 스트림.
        """
        self.set("base", index)
        self.set("base_term", term)
        self.set("clock", clock)
        self.db.execute("DELETE FROM snapshot")
        stream.seek(0)
        part = 0
        while True:
            chunk = stream.read(SNAPSHOT_CHUNK)
            if not chunk:
                break
            self.db.execute("INSERT INTO snapshot VALUES (?, ?)", (part, chunk))
            part += 1
        self.set("snapshot_parts", part)
        self.db.execute("DELETE FROM log WHERE idx<=?", (index,))

    def close(self):
        """SQLite 연결을 닫고 저장소의 독점 잠금을 해제한다."""
        self.db.close()
