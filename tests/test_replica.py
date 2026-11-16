import asyncio
from contextlib import suppress
import unittest

from miniredis import resp
from miniredis.engine import Engine
from miniredis.replica import Primary, QUEUE_SIZE, SYNC_COMMAND, follow


async def eventually(predicate):
    async def poll():
        while not predicate():
            await asyncio.sleep(0.01)
    await asyncio.wait_for(poll(), 4)


class ReplicationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.replica = Engine(follower=True)
        self.handlers = set()
        self.connections = 0

    async def connect(self, serve):
        async def handle(reader, writer):
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
        if hasattr(self, 'listener'):
            self.listener.close()
            await self.listener.wait_closed()
            self.follow_task.cancel()
            with suppress(asyncio.CancelledError):
                await self.follow_task
            handlers = list(self.handlers)
            for task in handlers:
                task.cancel()
            await asyncio.gather(*handlers, return_exceptions=True)

    async def test_snapshot_hash_blocks_and_ordered_changes(self):
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
        partial_sent = asyncio.Event()
        finish_snapshot = asyncio.Event()
        async def serve(reader, writer):
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
        closed = asyncio.Event()
        async def serve(reader, writer):
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
        primary = Engine()
        wire = Primary(primary)
        await self.connect(wire.stream)
        await eventually(lambda: self.replica.loaded)
        # Occupy one event-loop turn so the subscriber cannot consume its queue.
        for i in range(QUEUE_SIZE + 2):
            primary.execute([b'INCR', b'counter'])
        await eventually(lambda: self.connections >= 2 and self.replica.data == primary.data)
