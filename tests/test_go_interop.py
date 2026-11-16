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
        """두 서버의 작업 파일과 로그를 보관할 임시 디렉터리를 만든다."""
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)

    def start(self, executable, address, *options):
        """서버를 별도 프로세스로 실행하고 PING 응답으로 준비 상태를 확인한다.

        Args:
            executable: 프로그램 경로와 서버 실행에 필요한 기본 인자의 목록
            address: 테스트 서버가 수신할 호스트와 포트
            options: 복제 대상 주소 등 서버에 추가로 전달할 실행 인자
        """
        log_path = self.directory / (address.rsplit(":", 1)[-1] + ".log")
        with log_path.open("wb") as log:
            process = subprocess.Popen(
                [*executable, "-addr", address, "-aof=", *options], cwd=self.directory,
                env=dict(os.environ, PYTHONPATH=str(ROOT)), stdout=log, stderr=log,
            )

        def stop():
            """테스트 서버에 종료를 요청하고 제한 시간이 지나면 프로세스를 강제 종료한다."""
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
        """replica에서 같은 명령을 반복해 변경이 전파됐는지 확인한다.

        Args:
            address: 응답을 확인할 replica의 호스트와 포트
            expected: 복제가 반영됐을 때의 예상 응답
            args: replica에 전송할 명령 이름과 인자
        """
        deadline = time.monotonic() + 5
        while True:
            actual = command(address, *args)
            if actual == expected or time.monotonic() >= deadline:
                self.assertEqual(actual, expected, args)
                return
            time.sleep(0.02)

    def run_pair(self, go_primary):
        """Go와 Python 서버 사이에서 전체 스냅샷과 후속 쓰기가 복제되는지 검증한다.

        Args:
            go_primary: 참이면 Go를 primary로, 거짓이면 Python을 primary로 실행할지 지정하는 값
        """
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
        """Go primary의 스냅샷과 증분 변경을 Python replica가 읽는지 검증한다."""
        self.run_pair(go_primary=True)

    def test_python_primary_go_replica(self):
        """Python primary의 스냅샷과 증분 변경을 Go replica가 읽는지 검증한다."""
        self.run_pair(go_primary=False)
