"""미니 Redis 서버를 HTTP로 호출하는 클라이언트 (urllib 기반, 외부 라이브러리 없음).

역할: set/get/delete/expire/ttl/incr 메서드가 HTTP 요청으로 바뀐다.
주요 함수: MiniRedisClient._request(공통 요청·에러 변환), _key_path(키 URL 인코딩).
에러: 연결 실패 → RedisUnavailableError, 시간 초과 → RedisTimeoutError, 4xx/5xx → RedisHttpError(status 포함).
"""
#server.py와 store.py의 메서드 이름과 인자 구조를 그대로 따라간다. (store.py는 내부적으로 해시테이블을 쓰지만, 클라이언트는 HTTP 요청만 한다.)
import json
import socket
import urllib.error
import urllib.parse
import urllib.request


class RedisUnavailableError(Exception):
    """서버에 닿지 못했다 (연결 거부, 이름 해석 실패 등). 호출자는 '저장소를 쓸 수 없음'으로 처리한다."""


class RedisTimeoutError(RedisUnavailableError):
    """응답이 제한 시간 안에 오지 않았다. 역시 '쓸 수 없음'의 한 종류다."""


class RedisHttpError(Exception):
    """서버가 4xx/5xx로 응답했다. status로 구분한다."""

    def __init__(self, status, message):
        super().__init__(message)
        self.status = status


def _is_timeout(err):
    return isinstance(err, (socket.timeout, TimeoutError))


class MiniRedisClient:
    def __init__(self, base_url, timeout=2.0):
        if not isinstance(base_url, str) or base_url == "":
            raise TypeError("base_url이 필요합니다")
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout

    def set(self, key, value, ttl_ms=None):
        body = {"value": value}
        if ttl_ms is not None:
            body["ttlMs"] = ttl_ms
        self._request("PUT", self._key_path(key), body)

    def get(self, key):
        """없거나 만료됐으면 None."""
        status, data = self._request("GET", self._key_path(key), ok_statuses=(404,))
        return None if status == 404 else data["value"]

    def delete(self, key):
        """지웠으면 True, 없었으면 False."""
        status, _ = self._request("DELETE", self._key_path(key), ok_statuses=(404,))
        return status != 404

    def expire(self, key, ttl_ms):
        status, _ = self._request("POST", self._key_path(key) + "/expire", {"ttlMs": ttl_ms}, ok_statuses=(404,))
        return status != 404

    def ttl(self, key):
        """남은 ms, 만료 없음 -1, 키 없음 -2 (Store.ttl과 같은 규칙)."""
        status, data = self._request("GET", self._key_path(key) + "/ttl", ok_statuses=(404,))
        return -2 if status == 404 else data["ttlMs"]

    def incr(self, key):
        _, data = self._request("POST", self._key_path(key) + "/incr")
        return data["value"]

    @staticmethod
    def _key_path(key):
        return "/keys/" + urllib.parse.quote(key, safe="")

    def _request(self, method, path, body=None, ok_statuses=()):
        """ok_statuses: 에러로 보지 않을 4xx 상태 (예: 404는 '없음'이라는 정상 결과)."""
        data = None if body is None else json.dumps(body).encode("utf-8")
        headers = {"Content-Type": "application/json"} if data is not None else {}
        req = urllib.request.Request(self._base_url + path, data=data, method=method, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=self._timeout) as res:
                status, raw = res.status, res.read()
        except urllib.error.HTTPError as err:  # 4xx/5xx도 응답이다. 본문을 읽어서 에러 메시지를 꺼낸다
            status, raw = err.code, err.read()
        except urllib.error.URLError as err:
            if _is_timeout(err.reason):
                raise RedisTimeoutError(f"mini redis did not respond within {self._timeout}s") from err
            raise RedisUnavailableError(f"mini redis is unreachable: {err.reason}") from err
        except (socket.timeout, TimeoutError) as err:
            raise RedisTimeoutError(f"mini redis did not respond within {self._timeout}s") from err
        except OSError as err:
            raise RedisUnavailableError(f"mini redis is unreachable: {err}") from err

        try:
            parsed = json.loads(raw.decode("utf-8")) if raw else None
        except ValueError:
            parsed = None  # JSON이 아닌 응답
        if status >= 400 and status not in ok_statuses:
            message = (parsed or {}).get("error") if isinstance(parsed, dict) else None
            raise RedisHttpError(status, message or f"mini redis responded {status}")
        return status, parsed
