"""Set GOREDIS_SERVER to the original GoRedis executable to test both replication directions."""

import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest

from tests.test_cluster import ROOT, command, free_addresses


GOREDIS_SERVER = os.environ.get("GOREDIS_SERVER")


@unittest.skipUnless(GOREDIS_SERVER, "set GOREDIS_SERVER to the original Go executable")
class GoReplicationInterop(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)

    def start(self, executable, address, *options):
        log_path = self.directory / (address.rsplit(":", 1)[-1] + ".log")
        with log_path.open("wb") as log:
            process = subprocess.Popen(
                [*executable, "-addr", address, "-aof=", *options], cwd=self.directory,
                env=dict(os.environ, PYTHONPATH=str(ROOT)), stdout=log, stderr=log,
            )

        def stop():
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()

        self.addCleanup(stop)
        deadline = time.monotonic() + 5
        while process.poll() is None and time.monotonic() < deadline:
            try:
                if command(address, b"PING") == b"PONG":
                    return
            except ConnectionRefusedError:
                pass
            time.sleep(0.02)
        self.fail(f"server at {address} failed to start: {log_path.read_text(errors='replace')}")

    def replicated(self, address, expected, *args):
        deadline = time.monotonic() + 5
        while True:
            actual = command(address, *args)
            if actual == expected or time.monotonic() >= deadline:
                self.assertEqual(actual, expected, args)
                return
            time.sleep(0.02)

    def run_pair(self, go_primary):
        go = [str(Path(GOREDIS_SERVER).resolve())]
        python = [sys.executable, "-m", "miniredis"]
        primary, follower = free_addresses(2)
        self.start(go if go_primary else python, primary)
        # 1,025 pairs force the snapshot to cross a 1,024-field wire block.
        value = b"\xff\x00\r\n"
        fields = [part for n in range(1025) for part in (str(n).encode(), value)]
        self.assertEqual(command(primary, b"HSET", b"h", *fields), 1025)
        self.assertEqual(command(primary, b"EXPIRE", b"h", b"120"), 1)
        self.assertEqual(command(primary, b"SET", b"empty", b""), b"OK")
        self.start(python if go_primary else go, follower, "-replicaof", primary)
        self.replicated(follower, value, b"HGET", b"h", b"1024")
        self.assertEqual(command(follower, b"GET", b"empty"), b"")
        ttl = command(follower, b"TTL", b"h")
        self.assertGreater(ttl, 0)
        self.assertAlmostEqual(ttl, command(primary, b"TTL", b"h"), delta=1)

        self.assertEqual(command(primary, b"HSET", b"h", b"1", b"new"), 0)
        self.replicated(follower, b"new", b"HGET", b"h", b"1")
        self.assertEqual(command(primary, b"MSET", b"k", b"1", b"m", b"2", b"k", b"3"), b"OK")
        self.replicated(follower, [b"3", b"2"], b"MGET", b"k", b"m")
        self.assertEqual(command(primary, b"DEL", b"k", b"h"), 2)
        self.replicated(follower, 0, b"EXISTS", b"k", b"h")
        self.assertEqual(command(primary, b"HSET", b"h", b"n", b"new"), 1)
        self.replicated(follower, b"new", b"HGET", b"h", b"n")
        self.assertIsNone(command(follower, b"HGET", b"h", b"1"))
        self.assertTrue(command(follower, b"SET", b"k", b"v").startswith(b"READONLY"))

    def test_go_primary_python_replica(self):
        self.run_pair(go_primary=True)

    def test_python_primary_go_replica(self):
        self.run_pair(go_primary=False)
