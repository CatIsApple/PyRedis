"""TCP transport shared by standalone and clustered nodes."""
import asyncio
import inspect
import logging

from . import resp


def endpoint(address):
    """Split host:port or [IPv6]:port, also accepting :port for listeners."""
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
    def __init__(self, execute, primary=None):
        self.execute = execute
        self.primary = primary
        self.listener = None
        self.clients = {}
        self.closing = False

    async def start(self, host, port):
        self.listener = await asyncio.start_server(
            self._accept, host or None, port, limit=resp.MAX_LINE + 2)

    def _accept(self, reader, writer):
        if self.closing:
            writer.close()
            return
        task = asyncio.create_task(self._client(reader, writer))
        self.clients[task] = writer
        task.add_done_callback(self.clients.pop)

    async def _client(self, reader, writer):
        try:
            count = 0
            while True:
                try:
                    args = await resp.read(reader)
                except (resp.ProtocolError, resp.Incomplete):
                    writer.write(resp.encode(resp.Error(b"ERR Protocol error")))
                    await writer.drain()
                    return
                if not isinstance(args, list) or not args:
                    reply = resp.Error(b"ERR expected a non-empty command array")
                elif any(type(arg) is not bytes for arg in args):
                    reply = resp.Error(b"ERR expected a non-null bulk string")
                elif len(args) == 1 and args[0].upper() == b"GOREDIS.SYNC":
                    if self.primary is not None:
                        await self.primary.stream(reader, writer)
                    else:
                        writer.write(resp.encode(resp.Error(b"ERR replication source must be a primary")))
                        await writer.drain()
                    return
                else:
                    reply = self.execute(args)
                    if inspect.isawaitable(reply):
                        reply = await reply
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
