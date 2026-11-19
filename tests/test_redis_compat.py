"""Black-box compatibility tests. Set REDIS_SERVER or put redis-server 7.2+ on PATH."""

import contextlib
import os
from pathlib import Path
import random
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import unittest


ROOT = Path(__file__).resolve().parents[1]
REDIS_SERVER = os.environ.get("REDIS_SERVER") or shutil.which("redis-server")


def frame(parts):
    """Redis 명령 하나를 RESP2 벌크 문자열 배열로 인코딩한다.

    Args:
        parts: 명령 이름과 뒤따르는 인자를 담은 시퀀스.
    """
    parts = [part if isinstance(part, bytes) else str(part).encode() for part in parts]
    return b"*%d\r\n" % len(parts) + b"".join(
        b"$%d\r\n" % len(part) + part + b"\r\n" for part in parts
    )


def read_reply(reader):
    """스트림에서 RESP2 응답 하나를 읽어 타입과 값을 반환한다.

    Args:
        reader: 서버 응답을 읽을 바이너리 스트림.

    Returns:
        응답 타입 바이트와 해석한 값의 쌍.
    """
    header = reader.readline()
    if not header.endswith(b"\r\n"):
        raise EOFError("incomplete RESP2 reply: %r" % header)
    tag, value = header[:1], header[1:-2]
    if tag in (b"+", b"-"):
        return tag, value
    if tag == b":":
        return tag, int(value)
    size = int(value)
    if size == -1:
        return tag, None
    if tag == b"$":
        data = reader.read(size)
        if len(data) != size or reader.read(2) != b"\r\n":
            raise ValueError("invalid bulk string")
        return tag, data
    if tag == b"*":
        return tag, [read_reply(reader) for _ in range(size)]
    raise ValueError("unexpected RESP2 tag: %r" % tag)


class Client:
    def __init__(self, connection, reader):
        """명령을 보낼 소켓과 응답을 읽을 스트림을 연결한다.

        Args:
            connection: 테스트 서버에 연결한 TCP 소켓.
            reader: 같은 소켓의 바이너리 입력 스트림.
        """
        self.connection = connection
        self.reader = reader

    def pipeline(self, commands):
        """여러 명령을 한 번에 전송하고 각 응답을 요청 순서대로 읽는다.

        Args:
            commands: 전송할 명령별 인자 시퀀스 목록.
        """
        self.connection.sendall(b"".join(frame(parts) for parts in commands))
        return [read_reply(self.reader) for _ in commands]

    def command(self, *parts):
        """명령 하나를 전송하고 해당 응답을 반환한다.

        Args:
            *parts: 명령 이름과 뒤따르는 인자.
        """
        return self.pipeline([parts])[0]


@contextlib.contextmanager
def server(implementation, directory, appendonly=False):
    """독립된 서버 프로세스를 실행하고 테스트가 끝나면 종료한다.

    Args:
        implementation: 실행할 서버 종류로, redis는 공식 Redis이고 mini는 Python 구현이다.
        directory: 서버가 영속화 파일을 읽고 쓸 디렉터리.
        appendonly: AOF를 활성화할지 여부.

    Yields:
        새 서버에 연결한 RESP2 클라이언트.
    """
    directory.mkdir(parents=True, exist_ok=True)
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = reservation.getsockname()[1]
    if implementation == "redis":
        args = [REDIS_SERVER, "--bind", "127.0.0.1", "--port", str(port),
                "--dir", str(directory), "--save", "", "--appendonly",
                "yes" if appendonly else "no", "--appendfsync", "always"]
    else:
        aof = str(directory / "appendonly.aof") if appendonly else ""
        args = [sys.executable, "-m", "miniredis", "-addr", "127.0.0.1:%s" % port,
                "-aof", aof, "-appendfsync", "always"]
    with tempfile.TemporaryFile() as log:
        process = subprocess.Popen(args, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
        try:
            timeout = time.monotonic() + 8
            while process.poll() is None and time.monotonic() < timeout:
                try:
                    connection = socket.create_connection(("127.0.0.1", port), timeout=3)
                    break
                except ConnectionRefusedError:
                    time.sleep(0.02)
            else:
                log.seek(0)
                raise RuntimeError("%s failed to start: %s" % (implementation, log.read().decode(errors="replace")))
            with connection, connection.makefile("rb") as reader:
                yield Client(connection, reader)
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
                    raise


@unittest.skipUnless(REDIS_SERVER, "set REDIS_SERVER to run the real Redis differential suite")
class RedisCompatibility(unittest.TestCase):
    def setUp(self):
        """두 서버의 파일을 테스트마다 분리할 임시 디렉터리를 준비한다."""
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)

    @contextlib.contextmanager
    def pair(self):
        """공식 Redis와 mini-redis를 각각 메모리 모드로 실행한다.

        Yields:
            공식 Redis와 mini-redis 순서로 나열한 클라이언트 쌍.
        """
        with server("redis", self.directory / "redis") as redis:
            with server("mini", self.directory / "mini") as mini:
                yield redis, mini

    def compare(self, commands, pair):
        """두 서버에 같은 파이프라인을 보내 명령마다 응답 타입과 값을 비교한다.

        Args:
            commands: 두 서버에 동일한 순서로 보낼 명령 목록.
            pair: 공식 Redis와 mini-redis의 클라이언트 쌍.
        """
        redis, mini = pair
        expected = redis.pipeline(commands)
        actual = mini.pipeline(commands)
        for index, (command, reference, result) in enumerate(zip(commands, expected, actual)):
            with self.subTest(index=index, command=command):
                self.assertEqual(result, reference)

    def test_binary_pipeline(self):
        """바이너리 키와 빈 값이 섞인 파이프라인에서 두 서버의 응답을 비교한다."""
        binary = b"\xff\x00\r\n*4\r\n$3\r\n"
        with self.pair() as pair:
            self.compare([
                ("pInG",), ("PING", b""), ("PING", binary),
                ("SET", binary, b""), ("GET", binary), ("GET", "missing"),
                ("MSET", "", binary, "b", "old", "b", "new"),
                ("mGeT", binary, "", "b", "missing", ""),
                ("TYPE", binary), ("TYPE", "missing"), ("EXISTS", "b", "b", ""),
                ("DEL", "b", "missing", "b"), ("EXISTS", "b"),
            ], pair)

    def test_hashes_and_type_changes(self):
        """해시 필드를 갱신하거나 문자열로 덮어쓸 때의 응답과 만료 시간을 비교한다."""
        with self.pair() as pair:
            self.compare([
                ("HGET", "h", "missing"), ("HSET", "h", "", "", "a", "1", "a", "2"),
                ("HSET", "h", "a", "3", b"\xff", b"\x00"), ("TYPE", "h"),
                ("HGET", "h", ""), ("HGET", "h", "a"), ("HGET", "h", b"\xff"),
                ("GET", "h"), ("INCR", "h"), ("MGET", "h", "missing"), ("EXPIRE", "h", 60),
                ("HSET", "h", "a", "4"), ("TTL", "h"), ("SET", "h", "text", "nx"),
                ("MSET", "h", "text"), ("TYPE", "h"), ("TTL", "h"),
                ("HSET", "h", "f", "v"), ("HGET", "h", "f"), ("DEL", "h", "h"),
            ], pair)

    def test_numbers_and_set_options(self):
        """정수 범위의 경계값과 SET의 NX·EX 옵션 조합에 대한 응답을 비교한다."""
        with self.pair() as pair:
            commands = [("INCR", "new"), ("SET", "n", "-1", "EX", 60),
                        ("INCR", "n"), ("GET", "n"), ("TTL", "n")]
            for number in ("01", "", "+1", "-0", " 1", "1.0", "9223372036854775807",
                           "9223372036854775808", "-9223372036854775808"):
                commands.extend([("SET", "n", number), ("INCR", "n"), ("GET", "n")])
            commands.extend([
                ("SET", "k", "a", "ex", 60, "Nx"), ("SET", "k", "b", "NX"),
                ("SET", "k", "b", "nx", "ex", 0), ("SET", "k", "b", "EX", "01"),
                ("SET", "k", "b", "EX"), ("SET", "k", "b", "BAD"),
                ("SET", "k", "b", "EX", "bad", "EX", 45), ("TTL", "k"),
                ("SET", "k", "c", "NX", "NX"), ("GET", "k"),
                ("EXPIRE", "no_key", 60), ("EXPIRE", "k", "+1"),
                ("EXPIRE", "k", "9223372036854775807"), ("EXPIRE", "k", 0), ("TTL", "k"),
            ])
            self.compare(commands, pair)

    def test_mixed_operations(self):
        """고정 시드로 생성한 512개 명령을 같은 키에 섞어 실행하고 응답을 비교한다."""
        rng = random.Random(0xA0F)
        keys = [b"", b"num", b"hash", b"key with spaces", b"\x00", b"\xff"]
        values = [b"", b"0", b"1", b"-1", b"1.0", b"23", b"\xff\r\n"]
        commands = []
        for _ in range(512):
            a, b = rng.choices(keys, k=2)
            x, y = rng.choices(values, k=2)
            command = rng.choice([
                ("SET", a, x), ("SET", a, x, "NX"), ("SET", a, x, "EX", 120),
                ("GET", a), ("MSET", a, x, b, y), ("MGET", a, b, a),
                ("HSET", a, x, y, y, x), ("HGET", a, x), ("INCR", a),
                ("TYPE", a), ("TTL", a), ("EXPIRE", a, rng.choice((-1, 0, 120))),
                ("EXISTS", a, a, b), ("DEL", a, b, a),
            ])
            commands.append(command)
        with self.pair() as pair:
            self.compare(commands, pair)

    def test_wrong_arity(self):
        """명령별로 인자 수를 틀리게 보냈을 때 두 서버의 오류 응답을 비교한다."""
        with self.pair() as pair:
            self.compare([
                ("PING", 1, 2), ("SET", "k"), ("GET",), ("MSET", "a", "b", "c"),
                ("MGET",), ("HSET", "h", "f", "v", "missing"), ("HGET", "h"),
                ("INCR",), ("DEL",), ("EXISTS",), ("EXPIRE", "k"), ("TTL",), ("TYPE",),
                ("BGREWRITEAOF", 1),
            ], pair)

    def test_expiration(self):
        """만료 전후의 TTL과 만료된 키에 대한 조회 및 조건부 쓰기 결과를 비교한다."""
        with self.pair() as pair:
            self.compare([("SET", "temp", "v", "EX", 1), ("TTL", "temp")], pair)
            time.sleep(1.1)
            self.compare([("GET", "temp"), ("TTL", "temp"), ("TYPE", "temp"),
                          ("EXISTS", "temp"), ("SET", "temp", "new", "NX")], pair)


@unittest.skipUnless(REDIS_SERVER, "set REDIS_SERVER to run the real Redis persistence suite")
class RedisPersistence(unittest.TestCase):
    compressed_value = b"x\x00\xff\r\n" * 1024

    def setUp(self):
        """두 서버가 차례로 같은 영속화 파일을 열 수 있도록 임시 디렉터리를 준비한다."""
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)

    def seed(self, client):
        """스냅샷 복구를 확인할 문자열과 해시 및 만료 기한이 있는 키를 저장한다.

        Args:
            client: 테스트 데이터를 기록할 서버의 클라이언트.
        """
        self.assertEqual(client.command("MSET", "digits", -12345, "binary", self.compressed_value),
                         (b"+", b"OK"))
        self.assertEqual(client.command("HSET", "small", b"\x00", 123, "e", b""), (b":", 2))
        self.assertEqual(client.command("HSET", "large", "big", self.compressed_value), (b":", 1))
        self.assertEqual(client.command("SET", "expiring", "live", "EX", 900), (b"+", b"OK"))

    def check_snapshot(self, client):
        """복구한 문자열과 해시 필드의 값 및 만료 키의 남은 수명을 검증한다.

        Args:
            client: 저장 파일을 읽고 기동한 서버의 클라이언트.
        """
        self.assertEqual(client.command("GET", "digits"), (b"$", b"-12345"))
        self.assertEqual(client.command("GET", "binary"), (b"$", self.compressed_value))
        self.assertEqual(client.command("HGET", "small", b"\x00"), (b"$", b"123"))
        self.assertEqual(client.command("HGET", "small", "e"), (b"$", b""))
        self.assertEqual(client.command("HGET", "large", "big"), (b"$", self.compressed_value))
        self.assertEqual(client.command("GET", "expiring"), (b"$", b"live"))
        tag, ttl = client.command("TTL", "expiring")
        self.assertEqual(tag, b":")
        self.assertGreater(ttl, 0)
        self.assertLessEqual(ttl, 900)

    def rewrite(self, client):
        """AOF 재작성을 요청하고 매니페스트에 새 RDB가 등록될 때까지 기다린다.

        Args:
            client: BGREWRITEAOF 명령을 보낼 클라이언트.

        Returns:
            재작성으로 생성된 기본 RDB 파일의 경로.
        """
        path = self.directory / "appendonlydir" / "appendonly.aof.manifest"
        before = self.base_from_manifest(path)
        self.assertEqual(client.command("BGREWRITEAOF"),
                         (b"+", b"Background append only file rewriting started"))
        deadline = time.monotonic() + 6
        while time.monotonic() < deadline:
            after = self.base_from_manifest(path)
            if after and after != before:
                self.assertTrue(after.read_bytes().startswith(b"REDIS"))
                return after
            time.sleep(0.025)
        self.fail("rewritten RDB was not installed in the multipart manifest")

    @staticmethod
    def base_from_manifest(path):
        """AOF 매니페스트에서 현재 기본 파일의 경로를 찾는다.

        Args:
            path: 읽을 매니페스트 파일의 경로.

        Returns:
            기본 파일 경로이며, 파일 또는 항목을 찾지 못하면 None.
        """
        if path.exists():
            for line in path.read_text().splitlines():
                words = line.split()
                if "type" in words and words[words.index("type") + 1] == "b":
                    return path.parent / words[words.index("file") + 1]
        return None

    def test_redis_rdb_in_mini(self):
        """공식 Redis가 SAVE로 작성한 RDB를 mini-redis에서 복구한다."""
        with server("redis", self.directory) as redis:
            self.seed(redis)
            self.assertEqual(redis.command("SAVE"), (b"+", b"OK"))
        # The CLI imports an RDB preamble from an existing single-file AOF.
        shutil.copyfile(self.directory / "dump.rdb", self.directory / "appendonly.aof")
        with server("mini", self.directory, appendonly=True) as mini:
            self.check_snapshot(mini)

    def test_mini_rdb_in_redis(self):
        """mini-redis가 재작성한 기본 RDB를 공식 Redis에서 복구한다."""
        with server("mini", self.directory, appendonly=True) as mini:
            self.seed(mini)
            base = self.rewrite(mini)
        shutil.copyfile(base, self.directory / "dump.rdb")
        with server("redis", self.directory) as redis:
            self.check_snapshot(redis)

    def cross_load_multipart(self, writer, reader):
        """한 서버의 기본 RDB와 그 뒤의 증분 AOF를 다른 서버에서 복구해 검증한다.

        Args:
            writer: RDB와 AOF를 작성할 서버 종류.
            reader: 작성된 파일을 불러올 서버 종류.
        """
        with server(writer, self.directory, appendonly=True) as source:
            self.seed(source)
            self.rewrite(source)
            self.assertEqual(source.command("MSET", "tail", "v", "counter", 8), (b"+", b"OK"))
            self.assertEqual(source.command("INCR", "counter"), (b":", 9))
            self.assertEqual(source.command("DEL", "tail"), (b":", 1))
            self.assertEqual(source.command("HSET", "small", "tail", b"\r\n"), (b":", 1))
            self.assertEqual(source.command("EXPIRE", "counter", 900), (b":", 1))
        with server(reader, self.directory, appendonly=True) as recovered:
            self.check_snapshot(recovered)
            self.assertEqual(recovered.command("HGET", "small", "tail"), (b"$", b"\r\n"))
            self.assertEqual(recovered.command("GET", "tail"), (b"$", None))
            self.assertEqual(recovered.command("GET", "counter"), (b"$", b"9"))
            tag, ttl = recovered.command("TTL", "counter")
            self.assertEqual(tag, b":")
            self.assertGreater(ttl, 0)
            self.assertLessEqual(ttl, 900)

    def test_redis_multipart_in_mini(self):
        """공식 Redis의 다중 파일 AOF를 mini-redis가 올바른 순서로 복구하는지 검증한다."""
        self.cross_load_multipart("redis", "mini")

    def test_mini_multipart_in_redis(self):
        """mini-redis의 다중 파일 AOF를 공식 Redis가 올바른 순서로 복구하는지 검증한다."""
        self.cross_load_multipart("mini", "redis")


if __name__ == "__main__":
    unittest.main()
