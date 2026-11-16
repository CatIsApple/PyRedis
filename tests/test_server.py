import asyncio
import unittest

from miniredis import resp
from miniredis.server import Server


class ServerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.service = Server(lambda args: args)
        await self.service.start("127.0.0.1", 0)
        self.address = self.service.listener.sockets[0].getsockname()
        self.connections = []

    async def connect(self):
        reader, writer = await asyncio.open_connection(*self.address)
        self.connections.append(writer)
        return reader, writer

    async def asyncTearDown(self):
        for writer in self.connections:
            writer.close()
            await writer.wait_closed()
        await self.service.close()

    async def test_idle_partial_request_does_not_block_pipeline_on_other_socket(self):
        stalled, first = await self.connect()
        first.write(b"*1\r\n$4\r\nPI")
        reader, writer = await self.connect()
        pipeline = [[b"PING", b"\x00\xff"], [b"GET", b""]]
        writer.write(b"".join(resp.encode(request) for request in pipeline))
        for request in pipeline:
            self.assertEqual(await asyncio.wait_for(resp.read(reader), 1), request)
        first.write(b"NG\r\n")
        self.assertEqual(await resp.read(stalled), [b"PING"])

    async def test_malformed_commands_and_protocol_close(self):
        reader, writer = await self.connect()
        writer.write(b"*1\r\n+PING\r\n" + resp.encode([b"PING"]) + b"?")
        self.assertIsInstance(await resp.read(reader), resp.Error)
        self.assertEqual(await resp.read(reader), [b"PING"])
        self.assertEqual(await resp.read(reader), b"ERR Protocol error")
        self.assertEqual(await reader.read(), b"")

    async def test_shutdown_with_idle_connection(self):
        reader, _ = await self.connect()
        await self.service.close()
        self.assertEqual(await asyncio.wait_for(reader.read(), 1), b"")
