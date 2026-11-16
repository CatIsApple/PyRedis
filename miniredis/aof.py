"""Replay the string/hash subset of Redis AOFs without applying today's clock."""
import mmap
import os

from . import resp
from .engine import Entry
from .rdb import read_snapshot


def _int64(arg):
    digits = arg[1:] if arg[:1] in (b"-", b"+") else arg
    if not digits.isdigit() or len(digits) > 19:
        raise ValueError("invalid AOF integer")
    n = int(arg)
    if not -(1 << 63) <= n < 1 << 63:
        raise ValueError("AOF integer out of range")
    return n


def _replay_set(db, args):
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
        entry = db.setdefault(args[1], Entry({}, None))
        if not isinstance(entry.value, dict):
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
    """Return the first uncommitted offset (partial frame or transaction)."""
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
