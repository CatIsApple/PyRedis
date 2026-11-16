"""Redis database 0 snapshots for binary strings, hashes and key expiration."""
from __future__ import annotations

import io

from .engine import Entry


def _crc_table():
    table = []
    for value in range(256):
        for _ in range(8):
            value = (value >> 1) ^ (0x95AC9329AC4BC9B5 if value & 1 else 0)
        table.append(value)
    return table


_TABLE = _crc_table()
_MAX_STRING = 512 << 20


def crc64(data, previous=0):
    for byte in data:
        previous = _TABLE[(previous ^ byte) & 255] ^ (previous >> 8)
    return previous


def _length(n):
    if n < 64:
        return bytes([n])
    if n < 16384:
        return (0x4000 | n).to_bytes(2, "big")
    if n <= 0xFFFFFFFF:
        return b"\x80" + n.to_bytes(4, "big")
    return b"\x81" + n.to_bytes(8, "big")


def _string_parts(value):
    yield _length(len(value))
    yield value


def _parts(entries):
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
    """Bound encoding and checksum work so a rewrite can yield to the event loop."""
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
    for chunk in snapshot_chunks(entries):
        stream.write(chunk)


def _exact(stream, n):
    value = stream.read(n)
    if len(value) != n:
        raise ValueError("incomplete RDB")
    return value


class _Reader:
    def __init__(self, stream):
        self.stream = stream
        self.checksum = 0

    def take(self, n):
        value = _exact(self.stream, n)
        self.checksum = crc64(value, self.checksum)
        return value

    def size(self, allow_encoded=False):
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
    return str(int.from_bytes(_exact(stream, width), "little", signed=True)).encode()


def _listpack_value(stream):
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
    """Read exactly one RDB including its CRC; a legacy AOF can follow it."""
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
