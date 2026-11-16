"""Redis database 0 snapshots for binary strings, hashes and key expiration."""
from __future__ import annotations

import io

from .engine import Entry


def _crc_table():
    """Redis의 CRC64 다항식으로 바이트별 체크섬 조회표를 만든다."""
    table = []
    for value in range(256):
        for _ in range(8):
            value = (value >> 1) ^ (0x95AC9329AC4BC9B5 if value & 1 else 0)
        table.append(value)
    return table


_TABLE = _crc_table()
_MAX_STRING = 512 << 20


def crc64(data, previous=0):
    """입력 바이트를 이전 CRC64 체크섬에 누적한다.

    Args:
        data: 파일에 기록된 순서의 바이트열.
        previous: 앞 구간의 누적 체크섬. 첫 구간에서는 기본값 0을 사용한다.

    Returns:
        입력의 마지막 바이트까지 반영한 체크섬.
    """
    for byte in data:
        previous = _TABLE[(previous ^ byte) & 255] ^ (previous >> 8)
    return previous


def _length(n):
    """길이를 Redis RDB의 가변 길이 형식으로 인코딩한다.

    Args:
        n: 저장할 음이 아닌 길이 또는 개수.

    Returns:
        길이 접두사와 필요한 추가 바이트.
    """
    if n < 64:
        return bytes([n])
    if n < 16384:
        return (0x4000 | n).to_bytes(2, "big")
    if n <= 0xFFFFFFFF:
        return b"\x80" + n.to_bytes(4, "big")
    return b"\x81" + n.to_bytes(8, "big")


def _string_parts(value):
    """RDB 문자열을 길이 접두사와 원본 바이트열로 나누어 생성한다.

    Args:
        value: 바이너리 문자열.

    Yields:
        문자열의 바이트 길이를 인코딩한 접두사와 문자열 본문.
    """
    yield _length(len(value))
    yield value


def _parts(entries):
    """DB 0의 키 공간을 RDB 헤더부터 종료 표식까지 순서대로 인코딩한다.

    Args:
        entries: 키를 문자열 또는 해시 값과 절대 만료 시각이 담긴 항목에 대응시킨 스냅샷.

    Yields:
        헤더, 만료 시각, 자료형, 키와 값, 종료 표식을 구성하는 바이트열.
    """
    yield b"REDIS0011\xfe\x00"
    for key, entry in entries.items():
        if entry.expires is not None:
            yield b"\xfc" + entry.expires.to_bytes(8, "little", signed=True)
        is_hash = isinstance(entry.value, dict)
        yield b"\x04" if is_hash else b"\x00"
        yield from _string_parts(key)
        if is_hash:
            if not entry.value:
                raise ValueError("empty hash in RDB")
            yield _length(len(entry.value))
            for field, value in entry.value.items():
                yield from _string_parts(field)
                yield from _string_parts(value)
        else:
            yield from _string_parts(entry.value)
    yield b"\xff"


def snapshot_chunks(entries, chunk_size=64 << 10):
    """스냅샷을 조각별로 인코딩하면서 CRC64를 계산해 마지막에 덧붙인다.

    Args:
        entries: 한 시점에 복사한 키별 값과 절대 만료 시각.
        chunk_size: 본문을 나눌 양의 바이트 수. 기본값은 64 KiB다.

    Yields:
        RDB 본문 조각들. 마지막 조각에는 8바이트 체크섬이 추가된다.
    """
    buffer = bytearray()
    checksum = 0
    for part in _parts(entries):
        view = memoryview(part)
        while view:
            size = min(len(view), chunk_size - len(buffer))
            buffer.extend(view[:size])
            view = view[size:]
            if len(buffer) == chunk_size:
                checksum = crc64(buffer, checksum)
                yield bytes(buffer)
                buffer.clear()
    checksum = crc64(buffer, checksum)
    yield bytes(buffer) + checksum.to_bytes(8, "little")


def write_snapshot(stream, entries):
    """키 공간 전체를 체크섬이 있는 RDB 스냅샷으로 기록한다.

    Args:
        stream: RDB를 순서대로 기록할 바이너리 출력 스트림.
        entries: 스냅샷에 보관할 키별 값과 절대 만료 시각.
    """
    for chunk in snapshot_chunks(entries):
        stream.write(chunk)


def _exact(stream, n):
    """지정한 수만큼 바이트를 읽고 길이가 일치하는지 검사한다.

    Args:
        stream: 읽을 바이너리 입력 스트림.
        n: 필요한 바이트 수.

    Raises:
        ValueError: 스트림에서 요청한 수만큼 읽지 못했다.
    """
    value = stream.read(n)
    if len(value) != n:
        raise ValueError("incomplete RDB")
    return value


class _Reader:
    def __init__(self, stream):
        """RDB 입력 스트림을 연결하고 누적 체크섬을 0으로 초기화한다.

        Args:
            stream: RDB 헤더부터 읽을 바이너리 입력 스트림.
        """
        self.stream = stream
        self.checksum = 0

    def take(self, n):
        """다음 바이트를 읽으면서 누적 체크섬을 갱신한다.

        Args:
            n: 읽어서 반환할 바이트 수.
        """
        value = _exact(self.stream, n)
        self.checksum = crc64(value, self.checksum)
        return value

    def size(self, allow_encoded=False):
        """RDB 길이 접두사를 읽어 길이 또는 특수 인코딩 번호를 복원한다.

        Args:
            allow_encoded: 참이면 특수 인코딩 접두사를 허용한다. 기본값은 거짓이다.

        Returns:
            음이 아닌 길이 또는 인코딩 번호를 음수로 나타낸 값.
        """
        tag = self.take(1)[0]
        if tag < 64:
            return tag
        if tag < 128:
            return ((tag & 63) << 8) + self.take(1)[0]
        if tag in (128, 129):
            return int.from_bytes(self.take(4 if tag == 128 else 8), "big")
        if allow_encoded and tag >= 192:
            return -(tag & 63) - 1
        raise ValueError("invalid RDB length")

    def string(self):
        """일반 문자열, 정수 인코딩 또는 LZF 압축 문자열을 원래 바이트열로 복원한다.

        Raises:
            ValueError: 인코딩이 올바르지 않거나 문자열이 최대 크기를 초과했다.
        """
        size = self.size(allow_encoded=True)
        if 0 <= size <= _MAX_STRING:
            return self.take(size)
        if size in (-1, -2, -3):
            number = int.from_bytes(self.take(1 << (-size - 1)), "little", signed=True)
            return str(number).encode()
        if size == -4:
            packed, original = self.size(), self.size()
            if max(packed, original) <= _MAX_STRING:
                return _unlzf(self.take(packed), original)
        raise ValueError("unsupported or oversized RDB string")


def _unlzf(data, size):
    """LZF 리터럴과 역참조를 해석해 원래 문자열을 복원한다.

    Args:
        data: RDB에 저장된 LZF 압축 바이트열.
        size: 압축 전 문자열의 바이트 길이.

    Raises:
        ValueError: 역참조나 압축 해제 결과의 길이가 올바르지 않다.
    """
    stream = io.BytesIO(data)
    output = bytearray()
    while stream.tell() < len(data):
        tag = _exact(stream, 1)[0]
        if tag < 32:
            output.extend(_exact(stream, tag + 1))
        else:
            length = tag >> 5
            if length == 7:
                length += _exact(stream, 1)[0]
            length += 2
            distance = ((tag & 31) << 8) + _exact(stream, 1)[0] + 1
            if distance > len(output) or len(output) + length > size:
                raise ValueError("invalid RDB LZF reference")
            # A reference can overlap its destination, e.g. one byte repeated.
            pattern = output[-distance:][:length]
            output.extend((pattern * ((length + len(pattern) - 1) // len(pattern)))[:length])
        if len(output) > size:
            raise ValueError("RDB LZF output exceeds declared length")
    if len(output) != size:
        raise ValueError("invalid RDB LZF length")
    return bytes(output)


def _integer(stream, width):
    """리틀 엔디언 정수를 읽어 10진 문자열 바이트열로 변환한다.

    Args:
        stream: 부호 있는 정수 본문을 읽을 바이너리 스트림.
        width: 저장된 정수의 바이트 수.
    """
    return str(int.from_bytes(_exact(stream, width), "little", signed=True)).encode()


def _listpack_value(stream):
    """listpack 원소 하나를 복원하고 원소 뒤에 적힌 역방향 탐색용 길이를 검증한다.

    Args:
        stream: 읽을 원소의 인코딩 태그를 가리키는 바이너리 스트림.

    Returns:
        원소의 원래 문자열 바이트열.
    """
    start = stream.tell()
    tag = _exact(stream, 1)[0]
    if tag < 128:
        value = str(tag).encode()
    elif tag < 192:
        value = _exact(stream, tag & 63)
    elif tag < 224:
        number = ((tag & 31) << 8) + _exact(stream, 1)[0]
        value = str(number - 8192 if number & 4096 else number).encode()
    elif tag < 240:
        size = ((tag & 15) << 8) + _exact(stream, 1)[0]
        value = _exact(stream, size)
    elif tag == 240:
        value = _exact(stream, int.from_bytes(_exact(stream, 4), "little"))
    elif tag <= 244:
        value = _integer(stream, (2, 3, 4, 8)[tag - 241])
    else:
        raise ValueError("invalid listpack tag")
    size = stream.tell() - start
    head = _exact(stream, 1)[0]
    width = (size.bit_length() + 6) // 7
    # Redis 7 permits an extra leading zero at these exact boundaries.
    if head == 0 and size in (16383, 2097151, 268435455):
        width += 1
    suffix = bytes([head]) + _exact(stream, width - 1)
    for i, byte in enumerate(suffix):
        expected = ((size >> (7 * (width - i - 1))) & 127) | (128 if i else 0)
        if byte != expected:
            raise ValueError("invalid listpack back length")
    return value


def _ziplist_value(stream, previous):
    """ziplist 원소 앞에 기록된 이전 길이를 검사한 뒤 현재 값을 복원한다.

    Args:
        stream: 원소의 이전 길이 필드를 가리키는 바이너리 스트림.
        previous: 직전에 읽은 원소의 전체 바이트 수. 첫 원소에서는 0이다.

    Returns:
        현재 원소의 문자열 바이트열.
    """
    prefix = _exact(stream, 1)[0]
    prior = int.from_bytes(_exact(stream, 4), "little") if prefix == 254 else prefix
    if prefix == 255 or previous != prior:
        raise ValueError("invalid ziplist previous length")
    tag = _exact(stream, 1)[0]
    if tag < 64:
        return _exact(stream, tag)
    if tag < 128:
        return _exact(stream, ((tag & 63) << 8) + _exact(stream, 1)[0])
    if tag == 128:
        return _exact(stream, int.from_bytes(_exact(stream, 4), "big"))
    if 241 <= tag <= 253:
        return str(tag - 241).encode()
    widths = {192: 2, 208: 4, 224: 8, 240: 3, 254: 1}
    if tag not in widths:
        raise ValueError("invalid ziplist tag")
    return _integer(stream, widths[tag])


def _packed_hash(data, listpack):
    """listpack 또는 ziplist에 번갈아 저장한 필드명과 값을 해시로 복원한다.

    Args:
        data: 헤더와 종료 표식을 포함한 해시 본문.
        listpack: 참이면 listpack, 거짓이면 ziplist 형식으로 읽는다.

    Returns:
        필드명과 값을 바이트열로 보관한 딕셔너리.
    """
    stream = io.BytesIO(data)
    if int.from_bytes(_exact(stream, 4), "little") != len(data) or data[-1:] != b"\xff":
        raise ValueError("invalid packed hash length or terminator")
    tail = None if listpack else int.from_bytes(_exact(stream, 4), "little")
    expected = int.from_bytes(_exact(stream, 2), "little")
    fields, field, count, previous = {}, None, 0, 0
    start = stream.tell()
    while stream.tell() < len(data) - 1:
        start = stream.tell()
        value = _listpack_value(stream) if listpack else _ziplist_value(stream, previous)
        previous = stream.tell() - start
        if count % 2 == 0:
            if value in fields:
                raise ValueError("duplicate packed hash field")
            field = value
        else:
            fields[field] = value
        count += 1
    if (not fields or count % 2 or expected not in (65535, count)
            or tail not in (None, start) or stream.tell() != len(data) - 1):
        raise ValueError("invalid packed hash entries")
    return fields


def read_snapshot(stream):
    """RDB 한 개를 체크섬까지 읽고 DB 0의 키 공간을 복원한다.

    Args:
        stream: RDB 헤더에서 시작하는 바이너리 입력 스트림.

    Returns:
        키별 문자열 또는 해시 값과 절대 만료 시각. 스트림 위치는 체크섬 바로 다음이다.

    Raises:
        ValueError: 형식이나 자료형을 지원하지 않거나 파일이 손상되었다.
    """
    r = _Reader(stream)
    header = r.take(9)
    if header[:5] != b"REDIS" or not header[5:].isdigit() or not 9 <= int(header[5:]) <= 15:
        raise ValueError("unsupported RDB header")
    data, expires = {}, None
    while True:
        opcode = r.take(1)[0]
        if opcode == 255:
            if expires is not None:
                raise ValueError("missing key after RDB expiry")
            checksum = int.from_bytes(_exact(stream, 8), "little")
            if checksum not in (0, r.checksum):
                raise ValueError("RDB checksum mismatch")
            return data
        if opcode == 254:
            if r.size() != 0:
                raise ValueError("only RDB database 0 is supported")
        elif opcode == 253:
            expires = int.from_bytes(r.take(4), "little", signed=True) * 1000
        elif opcode == 252:
            expires = int.from_bytes(r.take(8), "little", signed=True)
        elif opcode == 251:
            r.size()
            r.size()
        elif opcode == 250:
            r.string()
            r.string()
        elif opcode == 249:
            r.take(1)
        elif opcode == 248:
            r.size()
        elif opcode in (0, 4, 13, 16):
            key = r.string()
            if key in data:
                raise ValueError("duplicate RDB key")
            if opcode == 0:
                value = r.string()
            elif opcode == 4:
                value = {}
                for _ in range(r.size()):
                    field = r.string()
                    if field in value:
                        raise ValueError("duplicate RDB hash field")
                    value[field] = r.string()
                if not value:
                    raise ValueError("empty RDB hash")
            else:
                value = _packed_hash(r.string(), opcode == 16)
            if expires is None or expires > 0:
                data[key] = Entry(value, expires)
            expires = None
        else:
            raise ValueError(f"unsupported RDB type or opcode {opcode}")
