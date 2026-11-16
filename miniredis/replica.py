"""Full snapshots and ordered changes using GoRedis's RESP2 replication protocol."""

import asyncio
import logging
import time

from . import resp
from .engine import Entry

SYNC_COMMAND = b"GOREDIS.SYNC"
FRAME_TIMEOUT = 30
HEARTBEAT = 2
QUEUE_SIZE = 256
HASH_BLOCK_PAIRS = 1024


class ReplicationError(ValueError):
    """The peer sent an invalid or inapplicable replication message."""


def _now():
    return time.time_ns() // 1_000_000


def _marker(value, name):
    return isinstance(value, resp.Simple) and value == name


def _hash_frames(kind, key, expires, fields):
    yield [resp.Simple(kind), key, expires or 0, len(fields)]
    block = []
    for field, value in fields.items():
        block.extend((field, value))
        if len(block) == 2 * HASH_BLOCK_PAIRS:
            yield block
            block = []
    if block:
        yield block


def _entry_frames(key, entry, now):
    if entry is None or (entry.expires is not None and entry.expires <= now):
        yield [key, None, 0]
    elif isinstance(entry.value, dict):
        yield from _hash_frames(b"HASH", key, entry.expires, entry.value)
    else:
        yield [key, entry.value, entry.expires or 0]


async def _send(writer, data):
    writer.write(data)
    await asyncio.wait_for(writer.drain(), FRAME_TIMEOUT)


async def _read(reader):
    return await asyncio.wait_for(resp.read(reader), FRAME_TIMEOUT)


class Primary:
    def __init__(self, engine):
        self.engine = engine
        self.subscribers = {}
        engine.on_change = self.publish

    def publish(self, args, keys, now, hash_created=False):
        if not self.subscribers or not keys:
            return
        command = args[0].upper()
        if command == b"MSET" and len(args) > 3:
            frames = [[resp.Simple(b"MSET"), *args[1:]]]
        elif command == b"DEL" and len(keys) > 1:
            frames = [[resp.Simple(b"DEL"), *keys]]
        elif command == b"HSET" and not hash_created:
            fields = dict(zip(args[2::2], args[3::2]))
            frames = _hash_frames(b"HSET", keys[0], self.engine.data[keys[0]].expires, fields)
        else:
            frames = _entry_frames(keys[0], self.engine.data.get(keys[0]), now)

        # Share an immutable change across streams; later writes cannot alter queued fields.
        change = tuple(resp.encode(frame) for frame in frames)
        for queue, writer in list(self.subscribers.items()):
            try:
                queue.put_nowait(change)
            except asyncio.QueueFull:
                # A missing change requires the follower to start a new full sync.
                del self.subscribers[queue]
                writer.close()

    async def stream(self, reader, writer):
        # No await between taking the snapshot and subscribing to later writes.
        now = _now()
        snapshot = self.engine.snapshot(now)
        queue = asyncio.Queue(maxsize=QUEUE_SIZE)
        self.subscribers[queue] = writer
        try:
            await _send(writer, resp.encode(resp.Simple(b"FULLSYNC")))
            for key, entry in snapshot.items():
                for frame in _entry_frames(key, entry, now):
                    await _send(writer, resp.encode(frame))
            del snapshot
            await _send(writer, resp.encode(resp.Simple(b"READY")))
            while queue in self.subscribers and not reader.at_eof():
                try:
                    change = await asyncio.wait_for(queue.get(), HEARTBEAT)
                except asyncio.TimeoutError:
                    await _send(writer, resp.encode(resp.Simple(b"PING")))
                else:
                    for data in change:
                        await _send(writer, data)
        except (OSError, asyncio.TimeoutError):
            pass
        finally:
            self.subscribers.pop(queue, None)


def _replace(data, key, value, expiry):
    if value is None or (expiry and expiry <= _now()):
        data.pop(key, None)
    else:
        data[key] = Entry(value, expiry or None)


def _bulk(value):
    # Simple strings and errors also subclass bytes but have different RESP tags.
    return type(value) is bytes


async def _read_hash(reader, count):
    fields = {}
    while count:
        block = await _read(reader)
        if (not isinstance(block, list) or not block or len(block) % 2
                or len(block) // 2 > min(HASH_BLOCK_PAIRS, count)
                or not all(_bulk(item) for item in block)):
            raise ReplicationError("invalid hash block")
        fields.update(zip(block[::2], block[1::2]))
        count -= len(block) // 2
    return fields


async def _apply(reader, data, frame, snapshot):
    if not isinstance(frame, list) or not frame:
        raise ReplicationError("expected a replication entry")

    head = frame[0]
    if _bulk(head):
        if (len(frame) != 3 or (frame[1] is not None and not _bulk(frame[1]))
                or type(frame[2]) is not int or frame[2] < 0):
            raise ReplicationError("invalid string entry")
        if frame[1] is None and (snapshot or frame[2] != 0):
            raise ReplicationError("invalid deletion")
        _replace(data, head, frame[1], frame[2])
        return

    if _marker(head, b"HASH") or _marker(head, b"HSET"):
        delta = head == b"HSET"
        if (len(frame) != 4 or not _bulk(frame[1]) or type(frame[2]) is not int
                or frame[2] < 0 or type(frame[3]) is not int or frame[3] <= 0
                or (delta and snapshot)):
            raise ReplicationError("invalid hash header")
        _, key, expiry, count = frame
        fields = await _read_hash(reader, count)
        if not delta:
            _replace(data, key, fields, expiry)
            return
        base = data.get(key)
        now = _now()
        if base and base.expires is not None and base.expires <= now:
            data.pop(key)
            base = None
        if base is None and expiry and expiry <= now:
            return
        if base is None or not isinstance(base.value, dict) or base.expires != (expiry or None):
            raise ReplicationError("hash delta has no matching base")
        base.value.update(fields)
        return

    if snapshot or len(frame) < 2 or not all(_bulk(item) for item in frame[1:]):
        raise ReplicationError("invalid change in replication stream")
    if _marker(head, b"MSET") and len(frame) >= 3 and len(frame) % 2 == 1:
        data.update((key, Entry(value, None)) for key, value in zip(frame[1::2], frame[2::2]))
    elif _marker(head, b"DEL"):
        for key in frame[1:]:
            data.pop(key, None)
    else:
        raise ReplicationError("unknown change in replication stream")


async def _receive(engine, reader, writer):
    await _send(writer, resp.encode([SYNC_COMMAND]))
    if not _marker(await _read(reader), b"FULLSYNC"):
        raise ReplicationError("expected FULLSYNC")
    staging = {}
    while True:
        frame = await _read(reader)
        if staging is not None and _marker(frame, b"READY"):
            engine.data = staging
            engine.loaded = True
            staging = None
        elif staging is None and _marker(frame, b"PING"):
            continue
        else:
            await _apply(reader, staging if staging is not None else engine.data,
                         frame, snapshot=staging is not None)


async def follow(engine, host, port):
    """Retry a broken stream; the previous complete snapshot stays readable."""
    delay = 0.25
    last_log = float("-inf")
    while True:
        writer = None
        started = time.monotonic()
        try:
            reader, writer = await asyncio.wait_for(asyncio.open_connection(host, port), 5)
            await _receive(engine, reader, writer)
        except (OSError, EOFError, asyncio.TimeoutError, ReplicationError,
                resp.Incomplete, resp.ProtocolError) as exc:
            if time.monotonic() - last_log >= 10:
                logging.getLogger(__name__).info("Retrying primary %s:%s (%s)", host, port, exc)
                last_log = time.monotonic()
        finally:
            if writer is not None:
                writer.close()
                try:
                    await writer.wait_closed()
                except OSError:
                    pass
        if time.monotonic() - started >= FRAME_TIMEOUT:
            delay = 0.25
        await asyncio.sleep(delay)
        delay = min(2 * delay, 3)
