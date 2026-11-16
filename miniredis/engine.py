"""Synchronous execution of a single Redis command at a fixed point in time."""

import logging
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Optional, Union

from .resp import Error, Simple


@dataclass
class Entry:
    value: Union[bytes, dict[bytes, bytes]]
    expires: Optional[int] = None  # Absolute Unix time in milliseconds.


def now_ms():
    return time.time_ns() // 1_000_000


_MIN_INT = -(1 << 63)
_MAX_INT = (1 << 63) - 1
_INTEGER_ERROR = b"ERR value is not an integer or out of range"
_WRONGTYPE = b"WRONGTYPE Operation against a key holding the wrong kind of value"
_WRITES = {b'SET', b'MSET', b'INCR', b'HSET', b'EXPIRE', b'DEL'}
# Bounds include the command name. MSET/HSET check pair counts after write access.
_ARITY = {
    b'PING': (1, 2), b'BGREWRITEAOF': (1, 1), b'SET': (3, None), b'GET': (2, 2),
    b'MSET': (3, None), b'MGET': (2, None), b'INCR': (2, 2), b'TYPE': (2, 2),
    b'HSET': (4, None), b'HGET': (3, 3), b'DEL': (2, None), b'EXISTS': (2, None),
    b'EXPIRE': (3, 3), b'TTL': (2, 2),
}


class CommandFailure(Exception):
    """A command failed before modifying any live value."""


def _arity_error(name):
    return Error(b"ERR wrong number of arguments for '" + name.lower() + b"' command")


def _integer(raw):
    # Round-tripping rejects leading zeroes, whitespace, plus signs and negative zero.
    if len(raw) <= 20:
        try:
            value = int(raw)
        except ValueError:
            pass
        else:
            if _MIN_INT <= value <= _MAX_INT and str(value).encode() == raw:
                return value
    raise CommandFailure(_INTEGER_ERROR)


def _deadline(raw, now, command):
    duration = _integer(raw) * 1000
    if not _MIN_INT <= duration <= _MAX_INT or now + duration > _MAX_INT:
        raise CommandFailure(b"ERR invalid expire time in '" + command + b"' command")
    return now + duration if duration > 0 else 0


class Engine:
    def __init__(self, follower=False):
        self._data = OrderedDict()
        self.follower = follower
        self.loaded = not follower
        self.journal = None
        self.on_change = None
        self.rewrite = None

    @property
    def data(self):
        return self._data

    @data.setter
    def data(self, entries):
        self._data = OrderedDict(entries)

    def _lookup(self, key, now):
        entry = self.data.get(key)
        if entry is not None and entry.expires is not None and entry.expires <= now:
            del self.data[key]
            return None
        return entry

    def purge(self, now=None, limit=20):
        """Sweep a bounded rotating window so idle expired keys are reclaimed."""
        if now is None:
            now = now_ms()
        for _ in range(min(limit, len(self.data))):
            key = next(iter(self.data))
            if self._lookup(key, now) is not None:
                self.data.move_to_end(key)

    def snapshot(self, now=None):
        if now is None:
            now = now_ms()
        entries = {}
        for key, entry in self.data.items():
            if entry.expires is None or entry.expires > now:
                value = entry.value.copy() if isinstance(entry.value, dict) else entry.value
                entries[key] = Entry(value, entry.expires)
        return entries

    def execute(self, args, now=None):
        if not isinstance(args, (list, tuple)) or not args:
            return Error(b'ERR expected a non-empty command array')
        if any(type(arg) is not bytes for arg in args):
            return Error(b'ERR expected a non-null bulk string')
        name = args[0].upper()
        if name not in _ARITY:
            return Error(b'ERR unknown command')
        minimum, maximum = _ARITY[name]
        if len(args) < minimum or (maximum is not None and len(args) > maximum):
            return _arity_error(name)
        if name == b'PING':
            return Simple(b'PONG') if len(args) == 1 else args[1]
        if name == b'BGREWRITEAOF':
            return self._bgrewriteaof()
        if self.follower and name in _WRITES:
            return Error(b"READONLY You can't write against a read only replica.")
        if not self.loaded:
            return Error(b'LOADING Replica is waiting for its initial snapshot')
        handler = getattr(self, '_' + name.decode('ascii').lower())
        try:
            return handler(args, now_ms() if now is None else now)
        except CommandFailure as exc:
            return Error(exc.args[0])

    def _bgrewriteaof(self):
        if self.rewrite is None:
            return Error(b'ERR AOF persistence is disabled')
        try:
            self.rewrite()
        except BlockingIOError:
            return Error(b'ERR Background append only file rewriting already in progress')
        except OSError:
            logging.exception('AOF rewrite failed')
            return Error(b"ERR Can't execute an AOF background rewriting. Please check the server logs for more information.")
        return Simple(b'Background append only file rewriting started')

    def _record(self, *commands):
        if self.journal is not None:
            try:
                self.journal.append(list(commands))
            except OSError as exc:
                raise CommandFailure(b'MISCONF failed to persist write to AOF') from exc

    def _publish(self, args, keys, now, **metadata):
        if self.on_change is not None:
            self.on_change(args, keys, now, **metadata)

    def _write_string(self, args, value, deadline, now):
        key = args[1]
        record = [b'SET', key, value]
        if deadline is not None:
            record.extend([b'PXAT', str(deadline).encode()])
        self._record(record)
        self.data[key] = Entry(value, deadline)
        self._publish(args, [key], now)

    def _set(self, args, now):
        nx = False
        seconds = None
        i = 3
        while i < len(args):
            # Redis treats a NUL immediately after an option as the end of its name.
            option = args[i].partition(b'\0')[0].upper()
            i += 1
            if option == b'NX':
                nx = True
            elif option == b'EX' and i < len(args):
                seconds = args[i]
                i += 1
            else:
                return Error(b'ERR syntax error')
        deadline = None
        if seconds is not None:
            deadline = _deadline(seconds, now, b'set')
            if deadline == 0:
                return Error(b"ERR invalid expire time in 'set' command")
        if nx and self._lookup(args[1], now) is not None:
            return None
        self._write_string(args, args[2], deadline, now)
        return Simple(b'OK')

    def _get(self, args, now):
        entry = self._lookup(args[1], now)
        if entry is None:
            return None
        if isinstance(entry.value, dict):
            return Error(_WRONGTYPE)
        return entry.value

    def _incr(self, args, now):
        entry = self._lookup(args[1], now)
        if entry is not None and isinstance(entry.value, dict):
            return Error(_WRONGTYPE)
        previous = 0 if entry is None else _integer(entry.value)
        if previous == _MAX_INT:
            return Error(b'ERR increment or decrement would overflow')
        value = previous + 1
        self._write_string(args, str(value).encode(), entry.expires if entry else None, now)
        return value

    def _mset(self, args, now):
        if len(args) % 2 != 1:
            return _arity_error(b'mset')
        self._record([b'MSET', *args[1:]])
        for key, value in zip(args[1::2], args[2::2]):
            self.data[key] = Entry(value)
        self._publish(args, list(dict.fromkeys(args[1::2])), now)
        return Simple(b'OK')

    def _mget(self, args, now):
        values = []
        for key in args[1:]:
            entry = self._lookup(key, now)
            values.append(entry.value if entry and isinstance(entry.value, bytes) else None)
        return values

    def _hset(self, args, now):
        if len(args) % 2 != 0:
            return _arity_error(b'hset')
        key = args[1]
        entry = self._lookup(key, now)
        if entry is not None and not isinstance(entry.value, dict):
            return Error(_WRONGTYPE)
        created = entry is None
        record = [b'HSET', *args[1:]]
        if created:
            # An expired hash might still exist during replay, before the final expiry sweep.
            self._record([b'DEL', key], record)
            self.data[key] = entry = Entry({})
        else:
            self._record(record)
        before = len(entry.value)
        entry.value.update(zip(args[2::2], args[3::2]))
        self._publish(args, [key], now, hash_created=created)
        return len(entry.value) - before

    def _hget(self, args, now):
        entry = self._lookup(args[1], now)
        if entry is None:
            return None
        if not isinstance(entry.value, dict):
            return Error(_WRONGTYPE)
        return entry.value.get(args[2])

    def _type(self, args, now):
        entry = self._lookup(args[1], now)
        if entry is None:
            return Simple(b'none')
        return Simple(b'hash' if isinstance(entry.value, dict) else b'string')

    def _exists(self, args, now):
        return sum(self._lookup(key, now) is not None for key in args[1:])

    def _del(self, args, now):
        keys = [key for key in dict.fromkeys(args[1:]) if self._lookup(key, now) is not None]
        if keys:
            self._record([b'DEL', *keys])
            for key in keys:
                del self.data[key]
            self._publish(args, keys, now)
        return len(keys)

    def _expire(self, args, now):
        deadline = _deadline(args[2], now, b'expire')
        key = args[1]
        entry = self._lookup(key, now)
        if entry is None:
            return 0
        self._record([b'PEXPIREAT', key, str(deadline).encode()])
        if deadline == 0:
            del self.data[key]
        else:
            entry.expires = deadline
        self._publish(args, [key], now)
        return 1

    def _ttl(self, args, now):
        entry = self._lookup(args[1], now)
        if entry is None:
            return -2
        if entry.expires is None:
            return -1
        return (entry.expires - now + 500) // 1000
