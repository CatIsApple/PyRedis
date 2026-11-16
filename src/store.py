"""해시 테이블 위의 키-값 저장소: TTL, lazy/active 만료, incr.

역할: 값은 JSON 문자열로 저장하고, 항목마다 만료 시각(expires_at)을 둔다.
주요 함수: set/get/read(값+ttl 한번에)/del_/exists/expire/ttl/incr/size/sweep(active 만료), start_sweeper(주기 실행 스레드),
_live(lazy 만료: 읽을 때 지움). 모든 공개 함수는 self._lock 안에서 실행된다.

동시성: 모든 명령은 self._lock 안에서 한 번에 끝난다. 여러 스레드가 불러도 명령 하나는 끼어들 수 없다
(JS 버전의 "단일 스레드 이벤트 루프" 역할을 락이 맡는다).
"""

import json
import math
import threading
import time

from .hash_table import HashTable


def _is_number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def _check_ttl(ttl_ms):
    if not _is_number(ttl_ms) or ttl_ms <= 0:
        raise ValueError("ttlMs는 양의 유한한 숫자여야 합니다")


class _Entry:
    """항목 하나. text는 JSON 문자열, expires_at은 만료 시각(ms) 또는 None(만료 없음)."""

    __slots__ = ("text", "expires_at")

    def __init__(self, text, expires_at):
        self.text = text
        self.expires_at = expires_at


class Store:
    def __init__(self, now=None):
        # 테스트에서 시간을 움직일 수 있게 시계를 주입받는다 (기본은 밀리초 단위 벽시계)
        self._table = HashTable()
        self._now = now if now is not None else (lambda: int(time.time() * 1000))
        self._lock = threading.RLock()
        self._stop = None
        self._thread = None

    def set(self, key, value, ttl_ms=None): #hash_table에 값을 넣는거임. ttl_ms는 만료시간을 밀리초 단위로 지정하는거임. 만약 None이면 만료시간이 없는거임
        if ttl_ms is not None:
            _check_ttl(ttl_ms)
        # 값은 JSON 문자열로 저장한다: 저장 뒤 호출자가 객체를 바꿔도 영향이 없고, JSON으로 못 바꾸는 값은 저장 전에 걸러진다
        try:
            text = json.dumps(value, allow_nan=False)
        except (TypeError, ValueError) as err:
            raise TypeError("JSON으로 저장할 수 없는 값입니다") from err
        with self._lock:
            expires_at = None if ttl_ms is None else self._now() + ttl_ms
            self._table.set(key, _Entry(text, expires_at))  # 새 항목으로 바꾸므로 이전 ttl은 사라진다

    def get(self, key):
        """없거나 만료됐으면 None."""
        with self._lock:
            entry = self._live(key)
            return None if entry is None else json.loads(entry.text)

    def read(self, key):
        """(값, ttl)을 한 번의 락 안에서 함께 읽는다. 없으면 None. 두 번 나눠 읽으면 그 사이에 만료될 수 있다."""
        with self._lock:
            entry = self._live(key)
            if entry is None:
                return None
            return json.loads(entry.text), self._remaining(entry)

    def del_(self, key):
        """키를 지운다. 파이썬에서 del은 예약어라 이름을 del_로 둔다."""
        with self._lock:
            if self._live(key) is None:
                return False
            return self._table.delete(key)

    def exists(self, key):
        with self._lock:
            return self._live(key) is not None

    def expire(self, key, ttl_ms):
        _check_ttl(ttl_ms)
        with self._lock:
            entry = self._live(key)
            if entry is None:
                return False
            entry.expires_at = self._now() + ttl_ms
            return True

    def ttl(self, key):
        """Redis 관례: 남은 ms, 만료 없음 -1, 키 없음(또는 만료됨) -2."""
        with self._lock:
            entry = self._live(key)
            if entry is None:
                return -2
            return self._remaining(entry)

    def incr(self, key):
        """없거나 숫자가 아니면 0에서 시작한다. 읽기와 쓰기가 같은 락 안에 있어 값이 유실되지 않는다."""
        with self._lock:
            entry = self._live(key)
            parsed = json.loads(entry.text) if entry is not None else None
            nxt = (parsed if _is_number(parsed) else 0) + 1
            if entry is not None:
                entry.text = json.dumps(nxt)  # 기존 항목의 만료 시간은 유지
            else:
                self._table.set(key, _Entry(json.dumps(nxt), None))
            return nxt

    def size(self):
        """만료된 항목은 세지 않는다. 읽기만 하고 지우지는 않는다."""
        with self._lock:
            return sum(1 for _, entry in self._table.items() if not self._is_expired(entry))

    def sweep(self):
        """active expiration: 아무도 읽지 않는 만료 항목을 한꺼번에 지우고, 지운 개수를 돌려준다."""
        removed = 0
        with self._lock:
            for key, entry in self._table.items():
                if self._is_expired(entry):
                    self._table.delete(key)
                    removed += 1
        return removed

    def start_sweeper(self, interval_ms):
        _check_ttl(interval_ms)
        self.stop_sweeper()  # 두 번 시작해도 스레드가 겹치지 않게
        stop = threading.Event()

        def loop():
            while not stop.wait(interval_ms / 1000):
                self.sweep()

        self._stop = stop
        self._thread = threading.Thread(target=loop, name="sweeper", daemon=True)  # daemon: 이 스레드 때문에 프로세스가 끝나지 않는 일을 막는다
        self._thread.start()

    def stop_sweeper(self):
        if self._stop is not None:
            self._stop.set()
        self._stop = None
        self._thread = None

    def _remaining(self, entry):
        if entry.expires_at is None:
            return -1
        return entry.expires_at - self._now()

    def _is_expired(self, entry):
        return entry.expires_at is not None and entry.expires_at <= self._now()

    def _live(self, key):
        """lazy expiration: 읽는 순간 만료를 확인하고 그 자리에서 지운다. 락을 잡은 상태에서만 부른다."""
        entry = self._table.get(key)
        if entry is None:
            return None
        if self._is_expired(entry):
            self._table.delete(key)
            return None
        return entry
