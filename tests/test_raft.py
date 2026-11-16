import asyncio
import socket
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from miniredis import raft, raft_storage, resp
from miniredis.raft import LeadershipLost, NotLeader, Raft


def endpoints(count):
    reserved = []
    try:
        for _ in range(count):
            sock = socket.socket()
            sock.bind(("127.0.0.1", 0))
            reserved.append(sock)
        return [f"127.0.0.1:{s.getsockname()[1]}" for s in reserved]
    finally:
        for sock in reserved:
            sock.close()


async def until(predicate, timeout=6):
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        result = predicate()
        if result:
            return result
        await asyncio.sleep(0.03)
    raise AssertionError("timed out waiting for Raft state")


class RaftTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        ports = endpoints(6)
        self.peers = dict(zip(ports[:3], ports[3:]))
        self.names = list(self.peers)
        self.live = {}

    async def start(self, name):
        self.live[name] = await Raft.open(name, self.peers, self.root / name, "layout")
        return self.live[name]

    async def stop(self, name):
        await self.live.pop(name).close()

    async def asyncTearDown(self):
        for name in list(self.live):
            await self.stop(name)

    async def leader(self):
        def get_leader():
            leaders = [n for n in self.live.values() if n.leader == n.local]
            return leaders[0] if len(leaders) == 1 else None
        return await until(get_leader)

    async def test_snapshot_catchup_failover_and_full_restart(self):
        # Write past compaction while the third voter is offline.
        with patch.object(raft, "SNAPSHOT_INTERVAL", 4):
            for name in self.names[:2]:
                await self.start(name)
            first = await self.leader()
            key = b"\xff\r\n"
            for i in range(9):
                self.assertEqual(i + 1, await first.execute([b"INCR", key]))
            self.assertEqual(1, await first.execute([b"HSET", b"h", b"f", b"\0"]))
            third = await self.start(self.names[2])
            await until(lambda: third.commit == first.commit and third.base > 0)
            self.assertEqual(third.machine.execute([b"HGET", b"h", b"f"]), b"\0")
            await self.stop(first.local)
            second = await self.leader()
            self.assertEqual(b"9", await second.execute([b"GET", key]))
            self.assertEqual(10, await second.execute([b"INCR", key]))
            for name in list(self.live):
                await self.stop(name)
            for name in self.names:
                await self.start(name)
            recovered = await self.leader()
            self.assertEqual(b"10", await recovered.execute([b"GET", key]))
            self.assertEqual(b"\0", await recovered.execute([b"HGET", b"h", b"f"]))

    async def test_log_and_snapshot_larger_than_one_bulk(self):
        # Lower wire limits to exercise the full-size path without allocating 512 MiB.
        with patch.object(resp, "MAX_BULK", 128), patch.object(raft, "SNAPSHOT_INTERVAL", 3), \
                patch.object(raft_storage, "SNAPSHOT_CHUNK", 64):
            for name in self.names[:2]:
                await self.start(name)
            leader = await self.leader()
            value = bytes(range(128))
            self.assertEqual(b"OK", await leader.execute([b"MSET", b"a", value, b"b", value]))
            self.assertEqual(value, await leader.execute([b"GET", b"a"]))
            self.assertGreater(leader.disk.get("snapshot_parts"), 1)
            last = await self.start(self.names[2])
            await until(lambda: last.commit >= leader.commit)
            self.assertEqual(last.machine.execute([b"GET", b"b"]), value)
            # A request too large for a single SQLite row must not enter the log.
            with patch.object(leader.disk, "max_record", 32):
                index = leader.last
                self.assertIsInstance(await leader.execute([b"SET", b"blocked", value]), resp.Error)
                self.assertEqual(index, leader.last)
            self.assertEqual(value, await leader.execute([b"GET", b"b"]))

    async def test_minority_cannot_ack_read_or_write(self):
        for name in self.names:
            await self.start(name)
        leader = await self.leader()
        await leader.execute([b"SET", b"key", b"v"])
        for name in self.names:
            if name != leader.local:
                await self.stop(name)
        for args in ([b"GET", b"key"], [b"INCR", b"counter"]):
            with self.assertRaises((LeadershipLost, NotLeader)):
                await leader.execute(args)
        self.assertIsNone(leader.leader)

    async def test_directory_and_peer_identity(self):
        node = await self.start(self.names[0])
        with self.assertRaises(sqlite3.OperationalError):
            await Raft.open(node.local, self.peers, self.root / node.local, "layout")
        # A connection for a different destination at this endpoint cannot vote.
        reader, writer = await asyncio.open_connection(*raft.address_parts(self.peers[node.local]))
        try:
            await raft.write_frame(writer, [b"layout", self.names[1].encode(),
                                            self.names[2].encode(), [b"vote", 1000, 0, 0]])
            self.assertEqual(await asyncio.wait_for(reader.read(1), 1), b"")
            self.assertEqual(node.term, 0)
        finally:
            writer.close()
            await writer.wait_closed()
        await self.stop(node.local)
        with self.assertRaisesRegex(ValueError, "topology"):
            await Raft.open(node.local, self.peers, self.root / node.local, "different")
