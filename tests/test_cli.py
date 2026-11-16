"""Subprocess tests for the default on-disk server and its shutdown behavior."""
import asyncio
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
import unittest

from miniredis import resp

ROOT = Path(__file__).resolve().parents[1]


class CliTests(unittest.TestCase):
    def test_mode_flags(self):
        for flags in [("-cluster-dir", "."), ("-cluster-config", "x", "-aof", ""),
                      ("-replicaof", "localhost:6379", "-aof", "x")]:
            with self.subTest(flags=flags):
                result = subprocess.run([sys.executable, "-m", "miniredis", *flags],
                                        cwd=ROOT, capture_output=True)
                self.assertEqual(result.returncode, 2)

    def test_parallel_clients_then_crash_and_recover(self):
        with tempfile.TemporaryDirectory() as directory:
            with socket.socket() as sock:
                sock.bind(("127.0.0.1", 0))
                port = sock.getsockname()[1]
            args = [sys.executable, "-m", "miniredis", "-addr", f"127.0.0.1:{port}",
                    "-appendfsync", "always"]

            def start():
                env = dict(os.environ, PYTHONPATH=str(ROOT))
                process = subprocess.Popen(args, cwd=directory, env=env,
                                           stderr=subprocess.PIPE, text=True)
                def stop():
                    if process.poll() is None:
                        process.terminate()
                    try:
                        process.wait(timeout=5)
                    finally:
                        if process.poll() is None:
                            process.kill()
                            process.wait()
                        process.stderr.close()
                self.addCleanup(stop)
                deadline = time.monotonic() + 5
                while time.monotonic() < deadline:
                    if process.poll() is not None:
                        self.fail(process.stderr.read())
                    try:
                        with socket.create_connection(("127.0.0.1", port), timeout=1):
                            return process
                    except ConnectionRefusedError:
                        time.sleep(0.01)
                self.fail("server did not listen")

            async def batch(requests):
                reader, writer = await asyncio.open_connection("127.0.0.1", port)
                try:
                    writer.write(b"".join(map(resp.encode, requests)))
                    await writer.drain()
                    return [await asyncio.wait_for(resp.read(reader), 5) for _ in requests]
                finally:
                    writer.close()
                    await writer.wait_closed()

            async def increment():
                return await asyncio.gather(*(batch([[b"INCR", b"n"]] * 25) for _ in range(8)))

            first = start()
            replies = asyncio.run(increment())
            self.assertEqual(sorted(n for client in replies for n in client), list(range(1, 201)))
            self.assertEqual(asyncio.run(batch([[b"SET", b"last", b"\x00\xff"]])), [b"OK"])
            first.kill()
            first.wait(timeout=5)
            second = start()
            self.assertEqual(asyncio.run(batch([[b"MGET", b"n", b"last"]])), [[b"200", b"\x00\xff"]])
            second.terminate()
            self.assertEqual(second.wait(timeout=5), 0)
