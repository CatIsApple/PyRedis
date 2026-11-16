import asyncio
import socket
import unittest

from miniredis import resp
from miniredis.server import Server


class ServerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        """빈 포트에 요청 인자를 그대로 돌려주는 테스트 서버를 띄운다."""
        self.service = Server(lambda args: args)
        await self.service.start("127.0.0.1", 0)
        self.address = self.service.listener.sockets[0].getsockname()
        self.connections = []

    async def connect(self):
        """테스트 서버에 새 연결을 열고 입력·출력 스트림 쌍을 반환한다."""
        reader, writer = await asyncio.open_connection(*self.address)
        self.connections.append(writer)
        return reader, writer

    async def asyncTearDown(self):
        """테스트에서 연 클라이언트 연결과 서버를 모두 닫는다."""
        for writer in self.connections:
            writer.close()
            await writer.wait_closed()
        await self.service.close()

    async def test_idle_partial_request_does_not_block_pipeline_on_other_socket(self):
        """한 연결이 추가 바이트를 기다리는 동안 다른 연결의 파이프라인이 처리되는지 확인한다."""
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
        """명령 인자 오류 후에는 연결이 유지되고 프레임 형식 오류 후에는 닫히는지 확인한다."""
        reader, writer = await self.connect()
        writer.write(b"*1\r\n+PING\r\n" + resp.encode([b"PING"]) + b"?")
        self.assertIsInstance(await resp.read(reader), resp.Error)
        self.assertEqual(await resp.read(reader), [b"PING"])
        self.assertEqual(await resp.read(reader), b"ERR Protocol error")
        self.assertEqual(await reader.read(), b"")

    async def test_shutdown_when_peer_never_reads(self):
        """클라이언트가 큰 응답을 읽지 않아도 서버가 제한 시간 안에 종료되는지 확인한다."""
        self.service.execute = lambda args: b"x" * (8 << 20)
        _, writer = await self.connect()
        writer.get_extra_info("socket").setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
        writer.write(resp.encode([b"GET", b"large"]))
        await writer.drain()
        async def wait_for_blocked_output():
            """클라이언트가 아직 읽지 않은 응답이 서버의 전송 버퍼에 쌓일 때까지 기다린다."""
            while not any(w.transport.get_write_buffer_size() for w in self.service.clients.values()):
                await asyncio.sleep(0.001)
        await asyncio.wait_for(wait_for_blocked_output(), 2)
        await asyncio.wait_for(self.service.close(), 1)

    async def test_shutdown_with_idle_connection(self):
        """명령을 보내지 않은 클라이언트도 서버 종료 후 연결 종료를 감지하는지 확인한다."""
        reader, _ = await self.connect()
        await self.service.close()
        self.assertEqual(await asyncio.wait_for(reader.read(), 1), b"")
