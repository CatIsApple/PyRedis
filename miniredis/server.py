"""단일 서버의 로컬 실행기에 RESP2 요청을 전달하는 TCP 연결 처리기."""
import asyncio
import logging

from . import resp


def endpoint(address):
    """TCP 주소에서 호스트와 정수 포트를 분리한다.

    Args:
        address: host:port 또는 [IPv6]:port 형식의 문자열. 수신 주소는 호스트를 생략할 수 있다.

    Returns:
        (호스트, 포트) 튜플. 호스트를 생략하면 첫 번째 원소는 빈 문자열이다.
    """
    if address.startswith("["):
        host, sep, port = address[1:].partition("]:")
    else:
        host, sep, port = address.rpartition(":")
        if ":" in host:
            raise ValueError("IPv6 addresses must use [host]:port")
    if not sep or not port.isascii() or not port.isdigit() or int(port) > 65535:
        raise ValueError(f"invalid TCP address {address!r}")
    return host, int(port)


class Server:
    def __init__(self, engine):
        """모든 TCP 연결이 공유할 로컬 명령 실행기를 설정한다.

        Args:
            engine: 클라이언트 명령을 처리하고 이 노드의 키 공간을 관리하는 Engine 인스턴스.
        """
        self.engine = engine
        self.listener = None
        self.clients = {}
        self.closing = False

    async def start(self, host, port):
        """지정한 주소에서 클라이언트 연결을 받기 시작한다.

        Args:
            host: 수신할 호스트 주소. 빈 문자열이면 사용 가능한 인터페이스에 바인딩한다.
            port: TCP 포트 번호. 0이면 운영체제가 빈 포트를 배정한다.
        """
        self.listener = await asyncio.start_server(
            self._accept, host or None, port, limit=resp.MAX_LINE + 2)

    def _accept(self, reader, writer):
        """새 연결의 처리 작업을 등록하고 서버가 종료 중이면 소켓을 닫는다.

        Args:
            reader: 새 클라이언트 연결의 입력 스트림.
            writer: 같은 연결의 응답 전송과 종료에 쓸 출력 스트림.
        """
        if self.closing:
            writer.close()
            return
        task = asyncio.create_task(self._client(reader, writer))
        self.clients[task] = writer
        task.add_done_callback(self.clients.pop)

    async def _client(self, reader, writer):
        """한 연결에서 명령을 읽고 로컬 실행기의 응답을 도착 순서대로 전송한다.

        Args:
            reader: 클라이언트가 보낸 RESP2 명령을 읽는 스트림.
            writer: 명령 응답을 보내는 스트림.
        """
        try:
            count = 0
            while True:
                try:
                    args = await resp.read(reader)
                except (resp.ProtocolError, resp.Incomplete):
                    writer.write(resp.encode(resp.Error(b"ERR Protocol error")))
                    await writer.drain()
                    return
                reply = self.engine.execute(args)
                writer.write(resp.encode(reply))
                await writer.drain()
                count += 1
                # Buffered pipelines can otherwise run without yielding to other sockets.
                if count % 64 == 0:
                    await asyncio.sleep(0)
        except (EOFError, ConnectionError):
            pass
        except Exception:
            logging.exception("client connection failed")
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except ConnectionError:
                pass

    async def close(self):
        """새 연결 수락을 중단하고 열린 소켓과 연결 처리 작업을 정리한다."""
        self.closing = True
        if self.listener is not None:
            self.listener.close()
        tasks = list(self.clients)
        for task in tasks:
            # A slow peer can leave megabytes buffered. Shutdown must also release
            # sockets whose peers never drain that output.
            self.clients[task].transport.abort()
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if self.listener is not None:
            await self.listener.wait_closed()
