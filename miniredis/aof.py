"""Replay the string/hash subset of Redis AOFs without applying today's clock."""
import mmap
import os

from . import resp
from .engine import Entry
from .rdb import read_snapshot


def _int64(arg):
    """AOF에 적힌 10진 정수를 부호 있는 64비트 범위로 해석한다.

    Args:
        arg: 선택적 부호와 숫자로 이루어진 바이트열.

    Raises:
        ValueError: 숫자 형식이 잘못되었거나 허용 범위를 벗어났다.
    """
    digits = arg[1:] if arg[:1] in (b"-", b"+") else arg
    if not digits.isdigit() or len(digits) > 19:
        raise ValueError("invalid AOF integer")
    n = int(arg)
    if not -(1 << 63) <= n < 1 << 63:
        raise ValueError("AOF integer out of range")
    return n


def _replay_set(db, args):
    """로그를 기록한 시점의 키 상태와 SET 옵션을 적용해 값을 복원한다.

    Args:
        db: 앞선 AOF 명령까지 반영한 키 공간.
        args: 명령 이름, 키, 값, 선택적 NX·XX·KEEPTTL·PXAT 옵션을 담은 배열.
    """
    key, value = args[1:3]
    options = iter(args[3:])
    flags = set()
    expires = None
    for raw in options:
        flag = raw.split(b"\x00", 1)[0].upper()
        if flag == b"PXAT":
            expires = _int64(next(options, b""))
            if expires <= 0:
                raise ValueError("invalid SET expiration in AOF")
        elif flag not in (b"NX", b"XX", b"KEEPTTL"):
            raise ValueError("unsupported SET option in AOF")
        flags.add(flag)
    if {b"NX", b"XX"} <= flags or {b"KEEPTTL", b"PXAT"} <= flags:
        raise ValueError("incompatible SET options in AOF")
    if (b"NX" in flags and key in db) or (b"XX" in flags and key not in db):
        return
    if b"KEEPTTL" in flags and key in db:
        expires = db[key].expires
    db[key] = Entry(value, expires)


def _apply(db, args):
    """AOF 명령 하나를 복구 중인 키 공간에 적용한다.

    Args:
        db: 만료 기한을 보존하며 복구하는 데이터베이스 0의 키 공간.
        args: 명령 이름을 첫 항목으로 하는 RESP 벌크 문자열 배열.

    Raises:
        ValueError: 명령이나 인자, 대상 값의 자료형이 복구 규칙에 맞지 않는다.
    """
    cmd = args[0].upper()
    if cmd == b"SET" and len(args) >= 3:
        _replay_set(db, args)
    elif cmd == b"SELECT" and args[1:] == [b"0"]:
        return
    elif cmd == b"INCR" and len(args) == 2:
        entry = db.get(args[1], Entry(b"0", None))
        if not isinstance(entry.value, bytes):
            raise ValueError("INCR on hash in AOF")
        number = _int64(entry.value)
        if str(number).encode() != entry.value or number == (1 << 63) - 1:
            raise ValueError("invalid INCR in AOF")
        db[args[1]] = Entry(str(number + 1).encode(), entry.expires)
    elif cmd == b"MSET" and len(args) >= 3 and len(args) % 2:
        for key, value in zip(args[1::2], args[2::2]):
            db[key] = Entry(value, None)
    elif cmd in (b"HSET", b"HMSET") and len(args) >= 4 and len(args) % 2 == 0:
        entry = db.get(args[1])
        if entry is None:
            db[args[1]] = entry = Entry({})
        elif isinstance(entry.value, bytes):
            raise ValueError("HSET on string in AOF")
        entry.value.update(zip(args[2::2], args[3::2]))
    elif cmd == b"PEXPIREAT" and len(args) == 3:
        expires = _int64(args[2])
        if expires <= 0:
            db.pop(args[1], None)
        elif args[1] in db:
            db[args[1]] = Entry(db[args[1]].value, expires)
    elif cmd in (b"DEL", b"UNLINK") and len(args) >= 2:
        for key in args[1:]:
            db.pop(key, None)
    else:
        raise ValueError(f"unsupported or malformed AOF command {cmd!r}")


def _scan(data, offset, db):
    """지정한 위치부터 AOF를 읽어 완전한 명령과 트랜잭션을 순서대로 적용한다.

    Args:
        data: 메모리에 매핑한 AOF 바이트열.
        offset: 첫 RESP 명령을 읽을 바이트 위치.
        db: 재생한 명령을 반영할 키 공간.

    Returns:
        불완전한 레코드나 트랜잭션의 시작 위치. 끝까지 재생했으면 파일 크기다.
    """
    start_transaction = None
    queued = []
    while offset < len(data):
        start = offset
        if data[start] == ord("#"):
            end = data.find(b"\n", start + 1)
            if end < 0:
                break
            offset = end + 1
            continue
        if data[start] != ord("*"):
            raise ValueError(f"expected an AOF command array at byte {start}")
        try:
            args, offset = resp.decode(data, start)
        except resp.Incomplete:
            break
        if not isinstance(args, list) or not args or any(type(arg) is not bytes for arg in args):
            raise ValueError(f"malformed AOF command at byte {start}")
        name = args[0].upper()
        if name == b"MULTI":
            if len(args) != 1 or start_transaction is not None:
                raise ValueError(f"unexpected MULTI at byte {start}")
            start_transaction = start
        elif name == b"EXEC":
            if len(args) != 1 or start_transaction is None:
                raise ValueError(f"unexpected EXEC at byte {start}")
            for pending in queued:
                _apply(db, pending)
            queued.clear()
            start_transaction = None
        elif start_transaction is not None:
            queued.append(args)
        else:
            _apply(db, args)
    return offset if start_transaction is None else start_transaction


def replay(file, db, *, preamble=False, tail="strict"):
    """필요하면 RDB 프리앰블을 복원한 뒤 AOF 명령을 키 공간에 재생한다.

    Args:
        file: 열려 있는 바이너리 AOF 파일. repair 모드에서는 쓰기도 가능해야 한다.
        db: 스냅샷과 증분 명령을 반영할 키 공간.
        preamble: 참이면 파일 앞의 RDB 헤더를 검사한다. 기본값은 거짓이다.
        tail: 불완전한 끝부분의 처리 방식. strict는 오류, repair는 절단, ignore는 건너뛰기다. 기본값은 strict다.
    """
    size = os.fstat(file.fileno()).st_size
    offset = 0
    if preamble:
        file.seek(0)
        magic = file.read(5)
        file.seek(0)
        if magic == b"REDIS":
            db.update(read_snapshot(file))
            offset = file.tell()
    if offset == size:
        return
    # Map the file to avoid copying an entire AOF into process memory to parse it.
    with mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ) as mapped:
        valid = _scan(mapped, offset, db)
    if valid < size:
        if tail == "strict":
            raise ValueError(f"incomplete AOF at byte {valid}")
        if tail == "repair":
            file.truncate(valid)
            file.flush()
            os.fsync(file.fileno())
