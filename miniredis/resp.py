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
    """RESP 헤더의 십진수 바이트를 범위 안의 정수로 변환한다.

    Args:
        line: 타입 바이트와 줄 끝을 제외한 숫자 부분.
        low: 허용하는 최솟값(포함).
        high: 허용하는 최댓값(포함).
        signed: 참이면 숫자 앞의 부호를 허용한다.

    Raises:
        ProtocolError: 숫자 형식이 잘못됐거나 값이 허용 범위를 벗어난 경우.
    """
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
    """타입 바이트와 배열 중첩 깊이가 RESP2 제한에 맞는지 검사한다.

    Args:
        tag: 프레임 종류를 나타내는 첫 바이트.
        depth: 이 값을 감싸는 배열의 수.
    """
    if tag not in (b"+", b"-", b":", b"$", b"*"):
        raise ProtocolError("invalid type byte")
    if tag == b"*" and depth >= MAX_DEPTH:
        raise ProtocolError("array nesting limit exceeded")


def _header(tag, line):
    """RESP 헤더를 단순 값, null 또는 뒤따를 데이터의 길이로 해석한다.

    Args:
        tag: 응답의 자료형을 나타내는 첫 바이트.
        line: 타입 바이트와 CRLF 사이의 바이트.
    """
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
    """버퍼에서 RESP2 프레임 하나를 읽고 다음 프레임의 시작 위치를 구한다.

    Args:
        data: 이어진 프레임이 담긴 바이트 버퍼 또는 메모리 매핑 파일.
        offset: 읽기 시작할 바이트 위치.
        _depth: 배열을 재귀적으로 읽을 때의 중첩 깊이. 최상위는 0이다.

    Returns:
        해석한 값과 다음 바이트 위치를 담은 튜플.

    Raises:
        Incomplete: 프레임을 완성할 바이트가 부족한 경우.
        ProtocolError: 잘못된 형식이거나 크기·중첩 제한을 넘은 경우.
    """
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
    """스트림에서 RESP2 프레임 하나가 도착할 때까지 기다려 값을 반환한다.

    Args:
        reader: 같은 연결의 다음 요청에도 재사용하는 입력 스트림.
        _depth: 배열을 재귀적으로 읽을 때의 중첩 깊이. 최상위는 0이다.

    Raises:
        EOFError: 새 프레임이 시작되기 전에 연결이 닫힌 경우.
        Incomplete: 프레임을 읽던 도중 연결이 닫힌 경우.
        ProtocolError: 프레임 형식이나 크기가 허용 범위를 벗어난 경우.
    """
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
    """Python 값을 전송하거나 AOF에 기록할 RESP2 바이트로 직렬화한다.

    Args:
        value: 정수, 바이트 문자열, 배열 또는 RESP 응답 값. None은 null 벌크 문자열이다.
        _depth: 배열을 재귀적으로 인코딩할 때의 중첩 깊이. 최상위는 0이다.

    Raises:
        ProtocolError: 표현할 수 없는 값이거나 프로토콜 제한을 넘은 경우.
    """
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
