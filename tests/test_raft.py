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
    """서로 다른 로컬 TCP 포트를 예약한 뒤 테스트에 사용할 주소를 반환한다.

    Args:
        count: 확보할 포트 수
    """
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
    """조건 함수가 참인 결과를 낼 때까지 기다렸다가 그 결과를 반환한다.

    Args:
        predicate: 기대 상태에 도달하면 참으로 평가되는 값을 반환하는 함수
        timeout: 조건을 기다릴 최대 시간(초)
    """
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        result = predicate()
        if result:
            return result
        await asyncio.sleep(0.03)
    raise AssertionError("timed out waiting for Raft state")


class RaftTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        """세 투표 노드의 주소와 영속 상태를 저장할 임시 디렉터리를 준비한다."""
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        ports = endpoints(6)
        self.peers = dict(zip(ports[:3], ports[3:]))
        self.names = list(self.peers)
        self.live = {}

    async def start(self, name):
        """지정한 노드를 해당 데이터 디렉터리에서 시작해 실행 중인 목록에 추가한다.

        Args:
            name: 노드의 식별자로 쓰이는 클라이언트 주소
        """
        self.live[name] = await Raft.open(name, self.peers, self.root / name, "layout")
        return self.live[name]

    async def stop(self, name):
        """지정한 노드를 실행 목록에서 빼고 Raft 연결과 저장소를 닫는다.

        Args:
            name: 종료할 노드의 클라이언트 주소
        """
        await self.live.pop(name).close()

    async def asyncTearDown(self):
        """테스트가 끝날 때까지 남아 있는 Raft 노드를 모두 종료한다."""
        for name in list(self.live):
            await self.stop(name)

    async def leader(self):
        """실행 중인 노드 중 한 곳만 자신을 리더로 인식할 때까지 기다린다."""
        def get_leader():
            """자신을 리더로 인식하는 노드가 정확히 하나면 그 노드를 반환한다."""
            leaders = [n for n in self.live.values() if n.leader == n.local]
            return leaders[0] if len(leaders) == 1 else None
        return await until(get_leader)

    async def test_snapshot_catchup_failover_and_full_restart(self):
        """늦게 합류한 노드의 스냅샷 복구와 리더 교체 및 전체 재시작 후의 값 보존을 검증한다."""
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
        """벌크 한도를 넘는 로그와 스냅샷이 분할 전송되고 과도한 요청은 로그 추가 전에 거절되는지 검증한다."""
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
        """과반수를 잃은 리더가 읽기와 쓰기 요청을 승인하지 않는지 검증한다."""
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
        """데이터 디렉터리 중복 사용과 다른 토폴로지의 재사용을 막고 잘못된 수신자에게 온 투표 요청을 거부하는지 검증한다."""
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
