import asyncio
from contextlib import suppress
import unittest

from miniredis import resp
from miniredis.engine import Engine
from miniredis.replica import Primary, QUEUE_SIZE, SYNC_COMMAND, follow


async def eventually(predicate):
    """조건이 참이 될 때까지 최대 4초 동안 기다린다.

    Args:
        predicate: 현재 테스트 상태가 기대 조건에 맞으면 참을 반환하는 함수
    """
    async def poll():
        """조건을 주기적으로 다시 확인하며 다른 비동기 작업에 실행 기회를 준다."""
        while not predicate():
            await asyncio.sleep(0.01)
    await asyncio.wait_for(poll(), 4)


class ReplicationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        """빈 replica 실행기와 연결 추적 상태를 준비한다."""
        self.replica = Engine(follower=True)
        self.handlers = set()
        self.connections = 0

    async def connect(self, serve):
        """임시 primary를 열고 replica의 동기화 작업을 시작한다.

        Args:
            serve: 동기화 요청을 받은 연결에서 응답을 보낼 비동기 함수
        """
        async def handle(reader, writer):
            """연결의 첫 동기화 요청을 검사하고 테스트용 복제 스트림을 실행한다.

            Args:
                reader: replica의 요청을 읽을 입력 스트림
                writer: 테스트용 primary가 응답을 보낼 출력 스트림
            """
            self.connections += 1
            self.handlers.add(asyncio.current_task())
            try:
                self.assertEqual(await resp.read(reader), [SYNC_COMMAND])
                await serve(reader, writer)
            finally:
                self.handlers.discard(asyncio.current_task())
                writer.close()
                with suppress(OSError):
                    await writer.wait_closed()

        self.listener = await asyncio.start_server(handle, '127.0.0.1', 0)
        port = self.listener.sockets[0].getsockname()[1]
        self.follow_task = asyncio.create_task(follow(self.replica, '127.0.0.1', port))

    async def asyncTearDown(self):
        """테스트 리스너와 연결 처리 작업 및 replica 동기화 작업을 종료한다."""
        if hasattr(self, 'listener'):
            self.listener.close()
            self.follow_task.cancel()
            with suppress(asyncio.CancelledError):
                await self.follow_task
            handlers = list(self.handlers)
            for task in handlers:
                task.cancel()
            await asyncio.gather(*handlers, return_exceptions=True)
            await self.listener.wait_closed()

    async def test_snapshot_hash_blocks_and_ordered_changes(self):
        """여러 블록에 걸친 해시와 후속 쓰기가 순서대로 복제되고 읽기 전용 상태가 유지되는지 검증한다."""
        primary = Engine()
        wire = Primary(primary)
        fields = {str(i).encode(): b'\x00\r\n' + str(i).encode() for i in range(1100)}
        args = [part for pair in fields.items() for part in pair]
        primary.execute([b'HSET', b'h', *args])
        primary.execute([b'EXPIRE', b'h', b'60'])
        primary.execute([b'SET', b'empty', b''])
        self.assertTrue(self.replica.execute([b'GET', b'h']).startswith(b'LOADING'))
        await self.connect(wire.stream)
        await eventually(lambda: self.replica.loaded)
        self.assertEqual(self.replica.data, primary.data)
        self.assertTrue(self.replica.execute([b'SET', b'no', b'write']).startswith(b'READONLY'))

        primary.execute([b'HSET', b'h', b'0', b'later', b'0', b'final'])
        primary.execute([b'EXPIRE', b'h', b'70'])
        primary.execute([b'HSET', b'new', b'f', b'v'])
        primary.execute([b'MSET', b'a', b'1', b'b', b'2', b'a', b'3'])
        primary.execute([b'DEL', b'empty', b'b'])
        await eventually(lambda: self.replica.data == primary.data)
        self.assertEqual(self.replica.execute([b'HGET', b'h', b'1000']), fields[b'1000'])
        self.assertEqual(self.replica.execute([b'MGET', b'a', b'b']), [b'3', None])

    async def test_reconnect_preserves_previous_snapshot_until_ready(self):
        """재접속 후 새 스냅샷의 READY 표식이 오기 전까지 기존 데이터가 유지되는지 검증한다."""
        partial_sent = asyncio.Event()
        finish_snapshot = asyncio.Event()
        async def serve(reader, writer):
            """첫 연결에 기존 값을 보내고 다음 연결에서는 새 스냅샷의 완료 신호를 지연한다.

            Args:
                reader: replica의 연결 종료를 기다릴 입력 스트림
                writer: 스냅샷과 READY를 보낼 출력 스트림
            """
            writer.write(resp.encode(resp.Simple(b'FULLSYNC')))
            if self.connections == 1:
                writer.write(resp.encode([b'old', b'v', 0]) + resp.encode(resp.Simple(b'READY')))
                await writer.drain()
                return
            writer.write(resp.encode([b'new', b'v2', 0]))
            await writer.drain()
            partial_sent.set()
            await finish_snapshot.wait()
            writer.write(resp.encode(resp.Simple(b'READY')))
            await writer.drain()
            await reader.read()

        await self.connect(serve)
        await asyncio.wait_for(partial_sent.wait(), 4)
        self.assertTrue(self.replica.loaded)
        self.assertEqual(self.replica.execute([b'MGET', b'old', b'new']), [b'v', None])
        finish_snapshot.set()
        await eventually(lambda: self.replica.execute([b'GET', b'new']) == b'v2')
        self.assertIsNone(self.replica.execute([b'GET', b'old']))

    async def test_incomplete_hash_delta_is_never_visible(self):
        """전송 도중 끊긴 해시 변경이 replica의 기존 값을 덮어쓰지 않는지 검증한다."""
        closed = asyncio.Event()
        async def serve(reader, writer):
            """완성된 해시를 보낸 뒤 다음 변경의 일부만 전송하고 연결을 닫는다.

            Args:
                reader: 재접속한 replica의 종료를 기다릴 입력 스트림
                writer: 스냅샷과 불완전한 변경을 보낼 출력 스트림
            """
            writer.write(resp.encode(resp.Simple(b'FULLSYNC')))
            if self.connections == 1:
                for frame in ([resp.Simple(b'HASH'), b'h', 0, 1], [b'f', b'old'],
                              resp.Simple(b'READY'), [resp.Simple(b'HSET'), b'h', 0, 2],
                              [b'f', b'new']):
                    writer.write(resp.encode(frame))
                await writer.drain()
                return
            closed.set()
            await reader.read()

        await self.connect(serve)
        await asyncio.wait_for(closed.wait(), 4)
        self.assertEqual(self.replica.execute([b'HGET', b'h', b'f']), b'old')

    async def test_queue_overflow_forces_a_new_complete_snapshot(self):
        """구독 큐가 가득 차면 replica가 재접속해 최신 상태를 복원하는지 검증한다."""
        primary = Engine()
        wire = Primary(primary)
        await self.connect(wire.stream)
        await eventually(lambda: self.replica.loaded)
        # Occupy one event-loop turn so the subscriber cannot consume its queue.
        for i in range(QUEUE_SIZE + 2):
            primary.execute([b'INCR', b'counter'])
        await eventually(lambda: self.connections >= 2 and self.replica.data == primary.data)
