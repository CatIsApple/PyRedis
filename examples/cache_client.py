"""HTTP 예제에서 GET과 SET EX를 호출하는 동기 RESP2 클라이언트."""

import socket
import time
from typing import Optional

from miniredis import resp


class CacheUnavailable(Exception):
    """캐시 서버와 통신하거나 명령의 성공을 확인할 수 없을 때 발생한다."""


class CacheClient:
    def __init__(self, host: str, port: int):
        """요청을 보낼 캐시 서버의 TCP 주소를 설정한다.

        Args:
            host: 캐시 서버의 호스트 이름 또는 IP 주소.
            port: 캐시 서버의 TCP 포트.
        """
        self.address = (host, port)

    def _request(self, args):
        """한 연결에서 명령 하나를 보내고 제한 시간 내에 완전한 응답을 읽는다.

        Args:
            args: 명령 이름과 인자를 순서대로 담은 바이트열 목록.
        """
        deadline = time.monotonic() + 2
        try:
            request = resp.encode(args)
            with socket.create_connection(self.address, timeout=2) as conn:
                conn.sendall(request)
                buffer = bytearray()
                while True:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise CacheUnavailable("캐시 응답 시간이 초과됐습니다.")
                    conn.settimeout(remaining)
                    chunk = conn.recv(65536)
                    if not chunk:
                        raise CacheUnavailable("캐시 응답이 완료되기 전에 연결이 종료됐습니다.")
                    buffer.extend(chunk)
                    if buffer[:1] not in (b"$", b"+", b"-"):
                        raise CacheUnavailable("캐시 응답의 RESP 자료형이 잘못됐습니다.")
                    try:
                        value, end = resp.decode(buffer)
                    except resp.Incomplete:
                        continue
                    if end != len(buffer):
                        raise CacheUnavailable("캐시 응답 뒤에 예상하지 않은 데이터가 있습니다.")
                    if isinstance(value, resp.Error):
                        raise CacheUnavailable(value.decode("utf-8", errors="replace"))
                    return value
        except (OSError, resp.ProtocolError) as exc:
            raise CacheUnavailable("캐시 서버와 통신할 수 없습니다.") from exc

    def get(self, key: bytes) -> Optional[bytes]:
        """문자열 값을 읽고 키가 없으면 None을 반환한다.

        Args:
            key: 캐시에서 조회할 키의 바이트열.
        """
        value = self._request([b"GET", key])
        if value is not None and type(value) is not bytes:
            raise CacheUnavailable("GET 응답이 벌크 문자열 형식이어야 합니다.")
        return value

    def setex(self, key: bytes, ttl: int, value: bytes) -> None:
        """SET EX로 문자열 값과 만료 시간을 한 번에 저장한다.

        Args:
            key: 캐시 값을 저장할 키의 바이트열.
            ttl: 값을 유지할 시간(초).
            value: 저장할 값의 바이트열.
        """
        reply = self._request([b"SET", key, value, b"EX", str(ttl).encode("ascii")])
        if not isinstance(reply, resp.Simple) or reply != b"OK":
            raise CacheUnavailable("SET EX의 성공 응답을 받지 못했습니다.")
