import binascii
import contextlib
import json
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

from miniredis import resp
from miniredis.cluster import SLOT_COUNT, Topology, endpoint, first_slot, key_slot


ROOT = Path(__file__).resolve().parents[1]
REDIS_SERVER = os.environ.get("REDIS_SERVER") or shutil.which("redis-server")


def free_addresses(count):
    with contextlib.ExitStack() as stack:
        sockets = [stack.enter_context(socket.socket()) for _ in range(count)]
        for sock in sockets:
            sock.bind(("127.0.0.1", 0))
        return [f"127.0.0.1:{sock.getsockname()[1]}" for sock in sockets]


def command(addr, *args):
    with socket.create_connection(endpoint(addr), timeout=4) as client:
        client.sendall(resp.encode(list(args)))
        buf = bytearray()
        while True:
            block = client.recv(65536)
            if not block:
                raise EOFError("server disconnected before sending a complete response")
            buf.extend(block)
            try:
                return resp.decode(buf)[0]
            except resp.Incomplete:
                pass


class SlotTests(unittest.TestCase):
    def test_slot_ranges_and_hash_tags(self):
        self.assertEqual([first_slot(i, 3) for i in range(4)], [0, 5462, 10923, 16384])
        self.assertEqual(key_slot(b"123456789"), 0x31C3)
        self.assertEqual(key_slot(b"{user:42}:name"), 15880)
        self.assertEqual(key_slot(b"x{{bar}}"), key_slot(b"{bar"))
        self.assertEqual(key_slot(b"\xff{bin\0}other"), key_slot(b"bin\0"))
        for key in (b"foo{}{bar}", b"{", b"no tag", b"\0", b""):
            self.assertEqual(key_slot(key), binascii.crc_hqx(key, 0) % SLOT_COUNT)

    def test_topology_validation_and_identity(self):
        config = json.loads((ROOT / "examples/cluster.json").read_text())
        local = config["shards"][0]["nodes"][0]["addr"]
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "config.json"
            path.write_text(json.dumps(config))
            layout = Topology.load(path, local)
            self.assertEqual(layout.local_shard, 0)
            self.assertEqual(len(layout.shards), 3)
            node = config["shards"][1]["nodes"][0]
            config["shards"][1]["nodes"][0] = dict(reversed(list(node.items())))
            path.write_text(json.dumps(config, indent=2))
            self.assertEqual(Topology.load(path, local).identity, layout.identity)
            with self.assertRaisesRegex(ValueError, "missing"):
                Topology.load(path, "127.0.0.1:1")
            for invalid in ("0.0.0.0:1234", "[::]:1234", "host:0", "host:65536", "host:x"):
                with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                    endpoint(invalid)
            config["shards"][1]["nodes"][0]["raft"] = local
            path.write_text(json.dumps(config))
            with self.assertRaisesRegex(ValueError, "duplicate"):
                Topology.load(path, local)


class ClusterProcessTests(unittest.TestCase):
    """Two real shards with three independent Python processes and disks each."""

    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.dir = Path(tmp.name)
        endpoints = free_addresses(12)
        self.shards = [endpoints[:3], endpoints[3:6]]
        config = {"shards": [
            {"nodes": [{"addr": addr, "raft": endpoints[6 + 3 * i + j]}
                       for j, addr in enumerate(shard)]}
            for i, shard in enumerate(self.shards)
        ]}
        self.config = self.dir / "cluster.json"
        self.config.write_text(json.dumps(config))
        self.procs = {}
        self.addCleanup(self.stop_all)
        for shard in self.shards:
            for addr in shard:
                self.start(addr)

    def start(self, addr):
        port = endpoint(addr)[1]
        with (self.dir / f"{port}.log").open("ab") as log:
            self.procs[addr] = subprocess.Popen([
                sys.executable, "-m", "miniredis", "-addr", addr,
                "-cluster-config", str(self.config), "-cluster-dir", str(self.dir / str(port)),
            ], cwd=ROOT, stdout=log, stderr=log)

    def stop(self, addr):
        process = self.procs.pop(addr)
        process.kill()
        process.wait(timeout=5)

    def stop_all(self):
        for addr in list(self.procs):
            self.stop(addr)

    def wait_for(self, probe, description):
        deadline = time.monotonic() + 12
        while time.monotonic() < deadline:
            try:
                result = probe()
                if result:
                    return result
            except (OSError, EOFError):
                pass
            time.sleep(0.06)
        logs = [(path.name, path.read_text()[-600:]) for path in self.dir.glob("*.log")]
        self.fail(f"Timed out waiting for {description}: {logs!r}")

    def owners(self, observer, excluded=None):
        def probe():
            rows = command(observer, b"CLUSTER", b"SLOTS")
            if not isinstance(rows, list) or len(rows) != 2:
                return False
            owners = [f"{row[2][0].decode()}:{row[2][1]}" for row in rows]
            if excluded in owners:
                return False
            return owners

        return self.wait_for(probe, "two advertised primary addresses")

    @unittest.skipUnless(REDIS_SERVER, "set REDIS_SERVER to compare with upstream cluster mode")
    def test_upstream_slots_and_redis_cli_redirection(self):
        observer = self.shards[1][0]
        first, second = self.owners(observer)
        reference, bus = free_addresses(2)
        with (self.dir / "redis.log").open("ab") as log:
            redis = subprocess.Popen([
                REDIS_SERVER, "--bind", "127.0.0.1", "--port", str(endpoint(reference)[1]),
                "--cluster-enabled", "yes", "--cluster-port", str(endpoint(bus)[1]),
                "--dir", str(self.dir), "--save", "", "--appendonly", "no",
            ], stdout=log, stderr=log)
        try:
            self.wait_for(lambda: command(reference, b"PING") == b"PONG", "upstream Redis")
            random_bytes = random.Random(42)
            keys = [b"", b"foo{}{bar}", b"foo{{bar}}zap", b"foo{bar}{zap}", b"{}{tag}"]
            for _ in range(256):
                key = random_bytes.randbytes(random_bytes.randrange(1, 80))
                keys.extend((key, b"prefix{" + key + b"}suffix"))
            for key in keys:
                self.assertEqual(command(observer, b"CLUSTER", b"KEYSLOT", key),
                                 command(reference, b"CLUSTER", b"KEYSLOT", key))
            cli = shutil.which("redis-cli") or Path(REDIS_SERVER).with_name("redis-cli")
            if Path(cli).is_file():
                def cli_command(addr, *args):
                    return subprocess.check_output([
                        str(cli), "--raw", "-c", "-h", "127.0.0.1",
                        "-p", str(endpoint(addr)[1]), *args,
                    ], timeout=4).rstrip()
                self.assertEqual(cli_command(second, "SET", "{b}:via-cli", "value"), b"OK")
                self.assertEqual(cli_command(first, "GET", "{b}:via-cli"), b"value")
        finally:
            redis.terminate()
            redis.wait(timeout=5)

    def test_routing_failover_quorum_and_restart(self):
        observer = self.shards[1][0]
        first, second = self.owners(observer)
        self.assertIn(first, self.shards[0])
        self.assertIn(second, self.shards[1])
        self.assertIn(b"cluster_state:ok", command(observer, b"CLUSTER", b"INFO"))
        rows = command(observer, b"CLUSTER", b"SLOTS")
        self.assertEqual([row[:2] for row in rows], [[0, 8191], [8192, 16383]])
        for row in rows:
            self.assertEqual(len(row), 5)
            self.assertEqual(len({member[2] for member in row[2:]}), 3)
        key, other = b"{b}:count", b"{b}:\xff\x00"
        slot = command(observer, b"CLUSTER", b"KEYSLOT", key)
        self.assertEqual(slot, key_slot(key))
        redirect = f"MOVED {slot} {first}".encode()
        self.assertEqual(command(second, b"GET", key), redirect)
        self.assertEqual(command(first, b"MSET", key, b"2", other, b"\xff"), b"OK")
        self.assertEqual(command(first, b"MGET", key, other), [b"2", b"\xff"])
        self.assertTrue(command(first, b"MSET", key, b"4", b"{a}", b"5").startswith(b"CROSSSLOT"))
        self.assertEqual(command(first, b"INCR", key), 3)
        # The second shard learns the promoted address through the client ports.
        self.stop(first)
        new, still_second = self.owners(observer, excluded=first)
        self.assertEqual(still_second, second)
        self.assertEqual(command(new, b"GET", key), b"3")
        self.assertEqual(command(new, b"INCR", key), 4)
        # Losing a second voter prevents even the remaining leader from acknowledging writes.
        third = next(addr for addr in self.shards[0] if addr not in (first, new))
        self.stop(third)
        denied = command(new, b"SET", b"{b}:minority", b"1")
        self.assertIsInstance(denied, resp.Error)
        self.assertEqual(command(second, b"SET", b"{a}:other", b"online"), b"OK")
        self.start(first)
        self.start(third)
        self.wait_for(lambda: any(command(a, b"GET", key) == b"4" for a in self.shards[0]),
                      "the rejoined majority to retain the acknowledged counter")
        self.stop_all()
        for shard in self.shards:
            for addr in shard:
                self.start(addr)
        first, second = self.owners(observer)
        self.assertEqual(command(first, b"MGET", key, other), [b"4", b"\xff"])
        self.assertEqual(command(second, b"GET", b"{a}:other"), b"online")
