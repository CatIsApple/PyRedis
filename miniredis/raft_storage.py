"""Durable HardState, log and snapshot for a single static Raft group."""

import sqlite3
from tempfile import SpooledTemporaryFile
from pathlib import Path


SNAPSHOT_CHUNK = 1 << 20


class RaftStorage:
    def __init__(self, directory, local, topology):
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
        row = self.db.execute("SELECT value FROM state WHERE key=?", (key,)).fetchone()
        return default if row is None else row[0]

    def set(self, key, value):
        self.db.execute("INSERT OR REPLACE INTO state VALUES (?, ?)", (key, value))

    def last_index(self):
        return self.db.execute("SELECT max(idx) FROM log").fetchone()[0] or self.get("base", 0)

    def term_at(self, index):
        if index == self.get("base", 0):
            return self.get("base_term", 0)
        row = self.db.execute("SELECT term FROM log WHERE idx=?", (index,)).fetchone()
        return None if row is None else row[0]

    def entries(self, start, count):
        return self.db.execute(
            "SELECT term, command FROM log WHERE idx>=? ORDER BY idx LIMIT ?", (start, count)
        ).fetchall()

    def append(self, start, entries):
        self.db.executemany("INSERT INTO log VALUES (?, ?, ?)",
                            ((i, term, cmd) for i, (term, cmd) in enumerate(entries, start)))

    def truncate(self, start):
        self.db.execute("DELETE FROM log WHERE idx>=?", (start,))

    def batch(self, start, count, budget):
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
        return self.db.execute("SELECT data FROM snapshot WHERE part=?", (part,)).fetchone()[0]

    def snapshot_stream(self):
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
        self.db.close()
