import asyncio
import unittest

from miniredis import resp


class RespTests(unittest.IsolatedAsyncioTestCase):
    async def test_all_types_with_fragmented_pipeline(self):
        frames = [resp.Simple(b"OK"), resp.Error(b"ERR wrong"), b"\x00\r\n\xff",
                  0, -(1 << 63), None, resp.NULL_ARRAY, [], [b"", [b"x", None]]]
        wire = b"".join(map(resp.encode, frames))
        stream = asyncio.StreamReader()
        async def feed():
            for byte in wire:
                stream.feed_data(bytes([byte]))
                await asyncio.sleep(0)
            stream.feed_eof()

        feeder = asyncio.create_task(feed())
        offset = 0
        for expected in frames:
            value, offset = resp.decode(wire, offset)
            streamed = await resp.read(stream)
            self.assertEqual(resp.encode(streamed), resp.encode(expected))
            self.assertEqual(resp.encode(value), resp.encode(expected))
        self.assertEqual(offset, len(wire))
        await feeder
        with self.assertRaises(EOFError):
            await resp.read(stream)

    async def test_truncated_record_is_distinct_from_corruption(self):
        for wire in [b"*1\r\n", b"$5\r\n123", b"$1\r\na\r", b"+OK"]:
            stream = asyncio.StreamReader()
            stream.feed_data(wire)
            stream.feed_eof()
            with self.assertRaises(resp.Incomplete):
                await resp.read(stream)
            with self.assertRaises(resp.Incomplete):
                resp.decode(wire)
        for wire in [b"x", b"+hello\n", b"$-2\r\n", b"$9_9\r\n",
                     b"$1\r\naXX", b"*1\r\n" * 65, b":9223372036854775808\r\n"]:
            stream = asyncio.StreamReader()
            stream.feed_data(wire)
            stream.feed_eof()
            with self.assertRaises(resp.ProtocolError):
                await resp.read(stream)
            with self.assertRaises(resp.ProtocolError):
                resp.decode(wire)
