"""Synchronous execution of a single Redis command at a fixed point in time."""

import logging
import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Optional, Union

from .hashtable import HashTable
from .resp import Error, Simple


@dataclass(init=False)
class Entry:
    value: Union[bytes, dict[bytes, bytes]]
    expires: Optional[int] = None

    def __init__(self, value: Union[bytes, Mapping], expires: Optional[int] = None):
        """문자열 또는 해시 값과 키의 절대 만료 시각을 보관한다.

        Args:
            value: 바이트 문자열 또는 필드와 값의 매핑. 해시 필드는 dict에 보관한다.
            expires: 절대 만료 시각(유닉스 밀리초). None이면 만료 기한이 없다.
        """
        if isinstance(value, (bytes, dict)):
            self.value = value
        else:
            self.value = dict(value.items())
        self.expires = expires


def now_ms():
    """현재 유닉스 시각을 밀리초로 반환한다."""
    return time.time_ns() // 1_000_000


_MIN_INT = -(1 << 63)
_MAX_INT = (1 << 63) - 1
_INTEGER_ERROR = b"ERR value is not an integer or out of range"
_WRONGTYPE = b"WRONGTYPE Operation against a key holding the wrong kind of value"
# Bounds include the command name. MSET and HSET also require complete pairs.
_ARITY = {
    b'PING': (1, 2), b'BGREWRITEAOF': (1, 1), b'SET': (3, None), b'GET': (2, 2),
    b'MSET': (3, None), b'MGET': (2, None), b'INCR': (2, 2), b'TYPE': (2, 2),
    b'HSET': (4, None), b'HGET': (3, 3), b'DEL': (2, None), b'EXISTS': (2, None),
    b'EXPIRE': (3, 3), b'TTL': (2, 2),
}


class CommandFailure(Exception):
    """A command failed before modifying any live value."""


def _arity_error(name):
    """인자 개수가 잘못된 명령의 RESP 오류 응답을 만든다.

    Args:
        name: 오류 메시지에 표시할 명령 이름.
    """
    return Error(b"ERR wrong number of arguments for '" + name.lower() + b"' command")


def _integer(raw):
    """Redis 정수 표기의 바이트열을 부호 있는 64비트 정수로 변환한다.

    Args:
        raw: 10진 정수를 나타내는 명령 인자 또는 저장된 값.

    Raises:
        CommandFailure: 정수 표기나 값의 범위가 올바르지 않을 때 발생한다.
    """
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
    """초 단위 유효 시간을 절대 만료 시각으로 변환한다.

    Args:
        raw: 유효 시간(초)을 나타내는 바이트열.
        now: 명령의 기준 시각(유닉스 밀리초).
        command: 만료 시간 오류에 표시할 명령 이름.

    Returns:
        양수인 유효 시간의 절대 만료 시각(유닉스 밀리초). 0초 이하는 즉시 만료를 뜻하는 0이다.

    Raises:
        CommandFailure: 정수 표기가 잘못되거나 계산 결과가 64비트 범위를 벗어날 때 발생한다.
    """
    duration = _integer(raw) * 1000
    if not _MIN_INT <= duration <= _MAX_INT or now + duration > _MAX_INT:
        raise CommandFailure(b"ERR invalid expire time in '" + command + b"' command")
    return now + duration if duration > 0 else 0


class Engine:
    def __init__(self):
        """빈 키 공간을 만들고 AOF 기록과 재작성 설정을 초기화한다."""
        self._data = HashTable()
        self.journal = None
        self.rewrite = None

    @property
    def data(self):
        """키별 값과 만료 시각을 담은 저장소를 반환한다."""
        return self._data

    @data.setter
    def data(self, entries):
        """키 공간을 전달받은 항목으로 교체하고 만료 검사 순서를 초기화한다.

        Args:
            entries: 바이트 키와 Entry 값의 매핑 또는 순서쌍 모음.
        """
        self._data = HashTable(entries)

    def _lookup(self, key, now):
        """살아 있는 키의 항목을 조회하고 만료된 항목은 저장소에서 삭제한다.

        Args:
            key: 조회할 바이트 키.
            now: 명령의 기준 시각(유닉스 밀리초).

        Returns:
            키의 Entry. 키가 없거나 만료됐으면 None이다.
        """
        entry = self.data.get(key)
        if entry is not None and entry.expires is not None and entry.expires <= now:
            del self.data[key]
            return None
        return entry

    def purge(self, now=None, limit=20):
        """키를 순환하며 한 번에 정해진 개수까지 검사해 만료된 항목을 삭제한다.

        Args:
            now: 기준 시각(유닉스 밀리초). 생략하면 현재 시각을 사용한다.
            limit: 이번 호출에서 검사할 최대 키 개수.
        """
        if now is None:
            now = now_ms()
        for _ in range(min(limit, len(self.data))):
            key = next(iter(self.data))
            if self._lookup(key, now) is not None:
                self.data.move_to_end(key)

    def snapshot(self, now=None):
        """기준 시각에 만료된 키를 삭제하고 살아 있는 값과 만료 시각의 사본을 만든다.

        Args:
            now: 기준 시각(유닉스 밀리초). 생략하면 현재 시각을 사용한다.

        Returns:
            키별 Entry 사전. 해시 필드도 복사하므로 이후 쓰기와 독립적이다.
        """
        if now is None:
            now = now_ms()
        entries = {}
        expired = []
        for key, entry in self.data.items():
            if entry.expires is not None and entry.expires <= now:
                expired.append(key)
                continue
            value = entry.value if isinstance(entry.value, bytes) else entry.value.copy()
            entries[key] = Entry(value, entry.expires)
        for key in expired:
            del self.data[key]
        return entries

    def execute(self, args, now=None):
        """요청 형식과 인자 개수를 확인하고 명령 하나를 실행한다.

        Args:
            args: 명령 이름과 인자를 순서대로 담은 RESP 벌크 문자열 배열.
            now: 기준 시각(유닉스 밀리초). 생략하면 현재 시각을 사용한다.

        Returns:
            명령 응답을 나타내는 RESP 값. 잘못된 요청에는 Error를 반환한다.
        """
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
        handler = getattr(self, '_' + name.decode('ascii').lower())
        try:
            return handler(args, now_ms() if now is None else now)
        except CommandFailure as exc:
            return Error(exc.args[0])

    def _bgrewriteaof(self):
        """BGREWRITEAOF 요청을 처리하고 재작성 시작 여부를 응답한다."""
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
        """AOF가 설정돼 있으면 메모리를 변경하기 전에 명령 묶음을 기록한다.

        Args:
            *commands: 재생 순서대로 정렬된 명령들로, 각 명령은 바이트 인자의 목록이다.

        Raises:
            CommandFailure: 디스크 기록이 실패해 쓰기를 중단할 때 발생한다.
        """
        if self.journal is not None:
            try:
                self.journal.append(list(commands))
            except OSError as exc:
                raise CommandFailure(b'MISCONF failed to persist write to AOF') from exc

    def _write_string(self, key, value, deadline):
        """문자열의 최종 값과 만료 시각을 AOF에 기록한 뒤 메모리에 반영한다.

        Args:
            key: 저장할 키의 바이트열.
            value: 저장할 최종 문자열의 바이트열.
            deadline: 절대 만료 시각(유닉스 밀리초). None이면 만료 기한이 없다.
        """
        record = [b'SET', key, value]
        if deadline is not None:
            record.extend([b'PXAT', str(deadline).encode()])
        self._record(record)
        self.data[key] = Entry(value, deadline)

    def _set(self, args, now):
        """SET의 NX 조건과 EX 기한을 검사한 뒤 문자열을 저장한다.

        Args:
            args: SET, 키, 값과 선택적 NX 또는 EX 초 인자.
            now: 명령의 기준 시각(유닉스 밀리초).

        Returns:
            성공하면 OK, NX 조건이 충족되지 않으면 None, 옵션이 잘못되면 오류 응답이다.
        """
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
        self._write_string(args[1], args[2], deadline)
        return Simple(b'OK')

    def _get(self, args, now):
        """GET으로 문자열 값을 읽는다.

        Args:
            args: GET과 조회할 키.
            now: 명령의 기준 시각(유닉스 밀리초).

        Returns:
            문자열의 바이트열. 없는 키는 None, 해시 키는 WRONGTYPE 오류다.
        """
        entry = self._lookup(args[1], now)
        if entry is None:
            return None
        if not isinstance(entry.value, bytes):
            return Error(_WRONGTYPE)
        return entry.value

    def _incr(self, args, now):
        """정수 문자열을 1 증가시키고 기존 키의 만료 시각을 유지한다.

        Args:
            args: INCR과 대상 키. 없는 키의 초기값은 0이다.
            now: 명령의 기준 시각(유닉스 밀리초).
        """
        entry = self._lookup(args[1], now)
        if entry is not None and not isinstance(entry.value, bytes):
            return Error(_WRONGTYPE)
        previous = 0 if entry is None else _integer(entry.value)
        if previous == _MAX_INT:
            return Error(b'ERR increment or decrement would overflow')
        value = previous + 1
        self._write_string(args[1], str(value).encode(), entry.expires if entry else None)
        return value

    def _mset(self, args, _now):
        """키와 값 쌍을 인자 순서대로 저장하고 덮어쓴 키의 만료 시각을 해제한다.

        Args:
            args: MSET과 하나 이상의 키·값 쌍.
            _now: 공통 명령 처리기가 전달하는 시각(유닉스 밀리초). MSET은 기존 값을 덮어쓰므로 사용하지 않는다.
        """
        if len(args) % 2 != 1:
            return _arity_error(b'mset')
        self._record([b'MSET', *args[1:]])
        for key, value in zip(args[1::2], args[2::2]):
            self.data[key] = Entry(value)
        return Simple(b'OK')

    def _mget(self, args, now):
        """문자열 키 여러 개를 같은 기준 시각에 조회한다.

        Args:
            args: MGET과 조회 순서대로 나열한 키.
            now: 명령의 기준 시각(유닉스 밀리초).

        Returns:
            키 순서에 대응하는 값 목록. 없는 키와 해시 키의 자리는 None이다.
        """
        values = []
        for key in args[1:]:
            entry = self._lookup(key, now)
            values.append(entry.value if entry and isinstance(entry.value, bytes) else None)
        return values

    def _hset(self, args, now):
        """해시 필드들을 갱신하고 새로 추가한 필드 수를 반환한다.

        Args:
            args: HSET, 대상 키와 하나 이상의 필드·값 쌍.
            now: 명령의 기준 시각(유닉스 밀리초).
        """
        if len(args) % 2 != 0:
            return _arity_error(b'hset')
        key = args[1]
        entry = self._lookup(key, now)
        if entry is not None and isinstance(entry.value, bytes):
            return Error(_WRONGTYPE)
        record = [b'HSET', *args[1:]]
        if entry is None:
            # An expired hash might still exist during replay, before the final expiry sweep.
            self._record([b'DEL', key], record)
            self.data[key] = entry = Entry({})
        else:
            self._record(record)
        before = len(entry.value)
        entry.value.update(zip(args[2::2], args[3::2]))
        return len(entry.value) - before

    def _hget(self, args, now):
        """HGET으로 해시의 필드 값을 조회한다.

        Args:
            args: HGET, 대상 키, 필드 이름.
            now: 명령의 기준 시각(유닉스 밀리초).

        Returns:
            필드 값의 바이트열. 없는 키나 필드는 None, 문자열 키는 WRONGTYPE 오류다.
        """
        entry = self._lookup(args[1], now)
        if entry is None:
            return None
        if isinstance(entry.value, bytes):
            return Error(_WRONGTYPE)
        return entry.value.get(args[2])

    def _type(self, args, now):
        """키의 자료형을 string, hash 또는 none으로 응답한다.

        Args:
            args: TYPE과 조회할 키.
            now: 명령의 기준 시각(유닉스 밀리초).
        """
        entry = self._lookup(args[1], now)
        if entry is None:
            return Simple(b'none')
        return Simple(b'string' if isinstance(entry.value, bytes) else b'hash')

    def _exists(self, args, now):
        """요청에 나온 각 키의 존재 여부를 확인하고 중복을 포함해 개수를 센다.

        Args:
            args: EXISTS와 존재 여부를 확인할 키들.
            now: 명령의 기준 시각(유닉스 밀리초).
        """
        return sum(self._lookup(key, now) is not None for key in args[1:])

    def _del(self, args, now):
        """존재하는 키를 중복 없이 삭제하고 실제 삭제한 키 수를 반환한다.

        Args:
            args: DEL과 삭제할 키들.
            now: 명령의 기준 시각(유닉스 밀리초).
        """
        keys = [key for key in dict.fromkeys(args[1:]) if self._lookup(key, now) is not None]
        if keys:
            self._record([b'DEL', *keys])
            for key in keys:
                del self.data[key]
        return len(keys)

    def _expire(self, args, now):
        """키에 초 단위 만료 기한을 설정하고 0초 이하인 경우 즉시 삭제한다.

        Args:
            args: EXPIRE, 대상 키와 유효 시간(초).
            now: 명령의 기준 시각(유닉스 밀리초).

        Returns:
            기한을 적용했거나 즉시 삭제했으면 1, 키가 없으면 0이다.
        """
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
        return 1

    def _ttl(self, args, now):
        """키가 만료되기까지 남은 시간을 초 단위로 반올림해 반환한다.

        Args:
            args: TTL과 조회할 키.
            now: 명령의 기준 시각(유닉스 밀리초).

        Returns:
            남은 초. 기한이 없으면 -1, 키가 없거나 이미 만료됐으면 -2다.
        """
        entry = self._lookup(args[1], now)
        if entry is None:
            return -2
        if entry.expires is None:
            return -1
        return (entry.expires - now + 500) // 1000
