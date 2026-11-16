"""Binary-safe RESP2 frames for clients, replication and the append-only log."""
import asyncio

MAX_LINE = 64 << 10
MAX_BULK = 512 << 20
MAX_ARRAY = 1 << 20
MAX_DEPTH = 64
CRLF = b"\r\n"


class ProtocolError(ValueError):
    """Malformed frame or a value outside the supported wire limits."""


class Incomplete(EOFError):
    """A frame has started but more bytes are required."""


class Simple(bytes):
    """RESP simple string (+)."""


class Error(bytes):
    """RESP error reply (-)."""


class _NullArray:
    pass


NULL_ARRAY = _NullArray()


def _number(line, low, high, signed=False):
    digits = line[1:] if signed and line.startswith((b"+", b"-")) else line
    if not digits.isdigit():
        raise ProtocolError("invalid numeric header")
    significant = digits.lstrip(b"0") or b"0"
    if len(significant) > 19:
        raise ProtocolError("number out of range")
    n = int(significant)
    if line.startswith(b"-"):
        n = -n
    if not low <= n <= high:
        raise ProtocolError("number out of range")
    return n


def _prefix(tag, depth):
    if tag not in (b"+", b"-", b":", b"$", b"*"):
        raise ProtocolError("invalid type byte")
    if tag == b"*" and depth >= MAX_DEPTH:
        raise ProtocolError("array nesting limit exceeded")


def _header(tag, line):
    if len(line) > MAX_LINE or b"\r" in line:
        raise ProtocolError("invalid header")
    if tag == b"+":
        return Simple(line)
    if tag == b"-":
        return Error(line)
    if tag == b":":
        return _number(line, -(1 << 63), (1 << 63) - 1, signed=True)
    if line == b"-1":
        return None if tag == b"$" else NULL_ARRAY
    return _number(line, 0, MAX_BULK if tag == b"$" else MAX_ARRAY)


def decode(data, offset=0, _depth=0):
    """Return (value, next offset). Incomplete means a truncated record."""
    if offset >= len(data):
        raise Incomplete()
    tag = bytes(data[offset:offset + 1])
    _prefix(tag, _depth)
    start = offset + 1
    end = data.find(b"\n", start, start + MAX_LINE + 2)
    if end < 0:
        if len(data) - start >= MAX_LINE + 2:
            raise ProtocolError("header exceeds line limit")
        raise Incomplete()
    if end == start or data[end - 1] != 13:
        raise ProtocolError("expected CRLF")
    value = _header(tag, bytes(data[start:end - 1]))
    pos = end + 1
    if tag == b"$" and value is not None:
        end = pos + value
        if data[end:end + 2] != CRLF:
            if data[end:end + 1] in (b"", b"\r") and len(data) < end + 2:
                raise Incomplete()
            raise ProtocolError("invalid bulk terminator")
        return bytes(data[pos:end]), end + 2
    if tag == b"*" and value is not NULL_ARRAY:
        items = []
        for _ in range(value):
            item, pos = decode(data, pos, _depth + 1)
            items.append(item)
        return items, pos
    return value, pos


async def read(reader, _depth=0):
    """Read one streaming frame. Clean EOF between frames raises EOFError."""
    tag = await reader.read(1)
    if not tag:
        if _depth:
            raise Incomplete()
        raise EOFError()
    _prefix(tag, _depth)
    try:
        line = await reader.readuntil(b"\n")
        if not line.endswith(CRLF):
            raise ProtocolError("expected CRLF")
        value = _header(tag, line[:-2])
        if tag == b"$" and value is not None:
            data = await reader.readexactly(value)
            if await reader.readexactly(2) != CRLF:
                raise ProtocolError("invalid bulk terminator")
            return data
        if tag == b"*" and value is not NULL_ARRAY:
            return [await read(reader, _depth + 1) for _ in range(value)]
        return value
    except asyncio.IncompleteReadError as exc:
        raise Incomplete() from exc
    except asyncio.LimitOverrunError as exc:
        raise ProtocolError("header exceeds line limit") from exc


def encode(value, _depth=0):
    if isinstance(value, (Simple, Error)):
        if len(value) > MAX_LINE or b"\r" in value or b"\n" in value:
            raise ProtocolError("invalid simple string")
        tag = b"-" if isinstance(value, Error) else b"+"
        return tag + value + CRLF
    if value is None:
        return b"$-1\r\n"
    if value is NULL_ARRAY:
        return b"*-1\r\n"
    if isinstance(value, bytes):
        if len(value) > MAX_BULK:
            raise ProtocolError("bulk exceeds size limit")
        return b"$%d\r\n" % len(value) + value + CRLF
    if type(value) is int and -(1 << 63) <= value < (1 << 63):
        return b":%d\r\n" % value
    if isinstance(value, (list, tuple)):
        if len(value) > MAX_ARRAY or _depth >= MAX_DEPTH:
            raise ProtocolError("array exceeds limit")
        return b"*%d\r\n" % len(value) + b"".join(encode(v, _depth + 1) for v in value)
    raise ProtocolError("unsupported RESP value")
