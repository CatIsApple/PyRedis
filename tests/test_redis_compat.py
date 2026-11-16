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
    parts = [part if isinstance(part, bytes) else str(part).encode() for part in parts]
    return b"*%d\r\n" % len(parts) + b"".join(
        b"$%d\r\n" % len(part) + part + b"\r\n" for part in parts
    )


def read_reply(reader):
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
        self.connection = connection
        self.reader = reader

    def pipeline(self, commands):
        self.connection.sendall(b"".join(frame(parts) for parts in commands))
        return [read_reply(self.reader) for _ in commands]

    def command(self, *parts):
        return self.pipeline([parts])[0]


@contextlib.contextmanager
def server(implementation, directory, appendonly=False):
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
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)

    @contextlib.contextmanager
    def pair(self):
        with server("redis", self.directory / "redis") as redis:
            with server("mini", self.directory / "mini") as mini:
                yield redis, mini

    def compare(self, commands, pair):
        redis, mini = pair
        expected = redis.pipeline(commands)
        actual = mini.pipeline(commands)
        for index, (command, reference, result) in enumerate(zip(commands, expected, actual)):
            with self.subTest(index=index, command=command):
                self.assertEqual(result, reference)

    def test_binary_pipeline(self):
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
        with self.pair() as pair:
            self.compare([
                ("PING", 1, 2), ("SET", "k"), ("GET",), ("MSET", "a", "b", "c"),
                ("MGET",), ("HSET", "h", "f", "v", "missing"), ("HGET", "h"),
                ("INCR",), ("DEL",), ("EXISTS",), ("EXPIRE", "k"), ("TTL",), ("TYPE",),
                ("BGREWRITEAOF", 1),
            ], pair)

    def test_expiration(self):
        with self.pair() as pair:
            self.compare([("SET", "temp", "v", "EX", 1), ("TTL", "temp")], pair)
            time.sleep(1.1)
            self.compare([("GET", "temp"), ("TTL", "temp"), ("TYPE", "temp"),
                          ("EXISTS", "temp"), ("SET", "temp", "new", "NX")], pair)


@unittest.skipUnless(REDIS_SERVER, "set REDIS_SERVER to run the real Redis persistence suite")
class RedisPersistence(unittest.TestCase):
    compressed_value = b"x\x00\xff\r\n" * 1024

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)

    def seed(self, client):
        self.assertEqual(client.command("MSET", "digits", -12345, "binary", self.compressed_value),
                         (b"+", b"OK"))
        self.assertEqual(client.command("HSET", "small", b"\x00", 123, "e", b""), (b":", 2))
        self.assertEqual(client.command("HSET", "large", "big", self.compressed_value), (b":", 1))
        self.assertEqual(client.command("SET", "expiring", "live", "EX", 900), (b"+", b"OK"))

    def check_snapshot(self, client):
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
        if path.exists():
            for line in path.read_text().splitlines():
                words = line.split()
                if "type" in words and words[words.index("type") + 1] == "b":
                    return path.parent / words[words.index("file") + 1]
        return None

    def test_redis_rdb_in_mini(self):
        with server("redis", self.directory) as redis:
            self.seed(redis)
            self.assertEqual(redis.command("SAVE"), (b"+", b"OK"))
        # The CLI imports an RDB preamble from an existing single-file AOF.
        shutil.copyfile(self.directory / "dump.rdb", self.directory / "appendonly.aof")
        with server("mini", self.directory, appendonly=True) as mini:
            self.check_snapshot(mini)

    def test_mini_rdb_in_redis(self):
        with server("mini", self.directory, appendonly=True) as mini:
            self.seed(mini)
            base = self.rewrite(mini)
        shutil.copyfile(base, self.directory / "dump.rdb")
        with server("redis", self.directory) as redis:
            self.check_snapshot(redis)

    def cross_load_multipart(self, writer, reader):
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
        self.cross_load_multipart("redis", "mini")

    def test_mini_multipart_in_redis(self):
        self.cross_load_multipart("mini", "redis")


if __name__ == "__main__":
    unittest.main()
