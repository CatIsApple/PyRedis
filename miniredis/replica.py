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
    """현재 유닉스 시각을 밀리초로 반환한다."""
    return time.time_ns() // 1_000_000


def _marker(value, name):
    """RESP 단순 문자열의 값이 지정한 복제 표식과 일치하는지 검사한다.

    Args:
        value: 검사할 RESP 값
        name: 기대하는 표식의 바이트열
    """
    return isinstance(value, resp.Simple) and value == name


def _hash_frames(kind, key, expires, fields):
    """해시 헤더를 먼저 내보내고 필드와 값을 블록 단위로 나누어 보낸다.

    Args:
        kind: 전체 해시를 뜻하는 HASH 또는 변경된 필드만 뜻하는 HSET
        key: 복제할 해시 키
        expires: 유닉스 밀리초 단위 만료 시각 또는 만료가 없을 때 None
        fields: 전송할 필드와 값의 매핑
    """
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
    """키의 현재 상태를 문자열, 해시 또는 삭제 프레임으로 변환한다.

    Args:
        key: 전송할 키
        entry: 키의 값과 만료 시각 또는 삭제된 키의 None
        now: 키의 만료 여부를 판단할 유닉스 시각(밀리초)
    """
    if entry is None or (entry.expires is not None and entry.expires <= now):
        yield [key, None, 0]
    elif isinstance(entry.value, dict):
        yield from _hash_frames(b"HASH", key, entry.expires, entry.value)
    else:
        yield [key, entry.value, entry.expires or 0]


async def _send(writer, data):
    """연결에 인코딩된 복제 데이터를 쓰고 제한 시간 동안 버퍼가 비워지기를 기다린다.

    Args:
        writer: 복제 연결의 출력 스트림
        data: 전송할 RESP 인코딩 바이트열
    """
    writer.write(data)
    await asyncio.wait_for(writer.drain(), FRAME_TIMEOUT)


async def _read(reader):
    """제한 시간 안에 복제 연결에서 RESP 프레임 하나를 읽는다.

    Args:
        reader: 복제 연결의 입력 스트림
    """
    return await asyncio.wait_for(resp.read(reader), FRAME_TIMEOUT)


class Primary:
    def __init__(self, engine):
        """쓰기 명령의 결과를 구독자에게 전달하도록 실행기에 콜백을 등록한다.

        Args:
            engine: 원본 키 공간을 소유한 primary 실행기
        """
        self.engine = engine
        self.subscribers = {}
        engine.on_change = self.publish

    def publish(self, args, keys, now, hash_created=False):
        """한 명령의 변경분을 구독자별 큐에 넣고 큐가 가득 찬 연결을 닫는다.

        Args:
            args: 완료된 명령 이름과 원래 인자 목록
            keys: 명령이 실제로 변경한 키 목록
            now: 명령 실행 당시의 유닉스 시각(밀리초)
            hash_created: 이번 HSET이 해시를 새로 만들었는지 여부
        """
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
        """스냅샷을 보내고 그 사이에 쌓인 변경부터 순서대로 전송한다.

        Args:
            reader: replica의 연결 종료 여부를 확인할 입력 스트림
            writer: 스냅샷과 변경 프레임을 보낼 출력 스트림
        """
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
    """복제된 키를 교체하고 삭제되었거나 이미 만료된 키는 제거한다.

    Args:
        data: 변경을 적용할 키 공간
        key: 교체하거나 삭제할 키
        value: 문자열 또는 해시 값이며 삭제 통지는 None
        expiry: 유닉스 밀리초 단위 만료 시각이며 0은 영구 키
    """
    if value is None or (expiry and expiry <= _now()):
        data.pop(key, None)
    else:
        data[key] = Entry(value, expiry or None)


def _bulk(value):
    """복제 필드가 RESP의 널이 아닌 벌크 문자열인지 검사한다.

    Args:
        value: 디코딩한 RESP 값
    """
    # Simple strings and errors also subclass bytes but have different RESP tags.
    return type(value) is bytes


async def _read_hash(reader, count):
    """헤더에 명시된 필드 쌍을 모두 읽어 해시를 조립한다.

    Args:
        reader: 해시 데이터 블록을 읽을 복제 입력 스트림
        count: 헤더가 예고한 필드와 값 쌍의 수
    """
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
    """복제 프레임 한 건을 검증한 뒤 해당 키 공간에 적용한다.

    Args:
        reader: 해시 헤더 뒤의 필드 블록을 읽을 스트림
        data: 스냅샷을 임시로 조립하거나 실시간 변경을 반영할 키 공간
        frame: 이미 읽은 키 항목 또는 변경 명령 프레임
        snapshot: 전체 스냅샷을 수신 중인지 여부
    """
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
    """primary에 동기화를 요청하고 스냅샷이 완성되면 공개한 뒤 변경을 반영한다.

    Args:
        engine: 스냅샷과 변경을 적용할 replica 실행기
        reader: primary가 보낸 프레임을 읽을 입력 스트림
        writer: 동기화 요청을 보낼 출력 스트림
    """
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
    """primary 연결이 끊기면 마지막 완전한 데이터를 유지한 채 재접속한다.

    Args:
        engine: primary를 따라갈 읽기 전용 실행기
        host: primary 서버의 호스트 이름 또는 IP 주소
        port: primary가 복제 요청을 받을 TCP 포트
    """
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
