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
        """함께 사용할 수 없는 CLI 옵션을 전달하면 종료 코드 2를 반환하는지 확인한다."""
        for flags in [("-cluster-dir", "."), ("-cluster-config", "x", "-aof", ""),
                      ("-replicaof", "localhost:6379", "-aof", "x")]:
            with self.subTest(flags=flags):
                result = subprocess.run([sys.executable, "-m", "miniredis", *flags],
                                        cwd=ROOT, capture_output=True)
                self.assertEqual(result.returncode, 2)

    def test_parallel_clients_then_crash_and_recover(self):
        """여러 연결의 증가 연산과 마지막 쓰기가 강제 종료 후에도 복구되는지 검증한다."""
        with tempfile.TemporaryDirectory() as directory:
            with socket.socket() as sock:
                sock.bind(("127.0.0.1", 0))
                port = sock.getsockname()[1]
            args = [sys.executable, "-m", "miniredis", "-addr", f"127.0.0.1:{port}",
                    "-appendfsync", "always"]

            def start():
                """임시 디렉터리에서 서버를 띄우고 TCP 연결을 받을 수 있게 되면 프로세스를 반환한다."""
                env = dict(os.environ, PYTHONPATH=str(ROOT))
                process = subprocess.Popen(args, cwd=directory, env=env,
                                           stderr=subprocess.PIPE, text=True)
                def stop():
                    """테스트 서버를 종료하고 표준 오류 스트림을 닫는다."""
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
                """한 TCP 연결에 여러 명령을 보내고 요청 순서대로 응답을 모은다.

                Args:
                    requests: 각 명령을 바이트 인자 목록으로 표현한 파이프라인.
                """
                reader, writer = await asyncio.open_connection("127.0.0.1", port)
                try:
                    writer.write(b"".join(map(resp.encode, requests)))
                    await writer.drain()
                    return [await asyncio.wait_for(resp.read(reader), 5) for _ in requests]
                finally:
                    writer.close()
                    await writer.wait_closed()

            async def increment():
                """클라이언트 여덟 개로 같은 키를 각각 25번 증가시키고 연결별 응답을 모은다."""
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
