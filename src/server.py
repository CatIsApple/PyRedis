"""미니 Redis REST API 서버 (표준 라이브러리 http.server만 사용). 실행: python -m src.server

역할: HTTP 요청을 Store 명령으로 바꾼다. 경로와 메서드를 build_routes 표에서 찾는다.
주요 함수: build_routes(URL→명령 표), dispatch(요청 1건 처리), read_json_object(본문 읽기·1MB 제한),
_decode_key(키 디코딩·길이 검사), MiniRedisServer(ThreadingHTTPServer 상속), main(포트·종료 처리).
라우트: /health, /stats, /keys/<k> (PUT/GET/DELETE), /keys/<k>/expire, /keys/<k>/ttl, /keys/<k>/incr.

store 명령은 모두 락 안에서 끝나므로, 요청 여러 개가 동시에 와도 incr 같은 읽기-쓰기가 유실되지 않는다.
"""
#client.py와 store.py의 메서드 이름과 인자 구조를 그대로 따라간다. (store.py는 내부적으로 해시테이블을 쓰지만, 클라이언트는 HTTP 요청만 한다.)
import errno
import json
import os
import re
import signal
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import unquote

from .store import Store

MAX_KEY_LENGTH = 512
DEFAULT_MAX_BODY_BYTES = 1024 * 1024  # 1MB


class HttpError(Exception):
    """상태 코드를 가진 에러. 잘못된 요청에만 쓰고, 그 밖의 예외는 500이 된다."""

    def __init__(self, status, message, headers=None):
        super().__init__(message)
        self.status = status
        self.message = message
        self.headers = headers or {}


def _content_length(headers):
    try:
        return max(0, int(headers.get("Content-Length") or 0))
    except ValueError:
        return 0


def read_json_object(rfile, headers, limit):
    """JSON 객체 본문을 읽는다. 본문이 limit보다 크면 읽지 않고 413을 낸다 (큰 요청 하나가 메모리를 다 쓰지 못하게)."""
    length = _content_length(headers)
    if length > limit:
        raise HttpError(413, "payload too large", {"Connection": "close"})
    raw = rfile.read(length) if length > 0 else b""
    try:
        body = json.loads(raw.decode("utf-8"))
    except ValueError:
        raise HttpError(400, "invalid JSON body")
    if not isinstance(body, dict):
        raise HttpError(400, "body must be a JSON object")
    return body


def _decode_key(raw):
    # 경로를 "/"로 나눈 뒤에 디코딩해야 %2F가 들어간 키가 살아남는다. URL 객체로 정규화하면 ".." 같은 키가 사라진다
    key = unquote(raw)
    if key == "":
        raise HttpError(400, "key must not be empty")
    if len(key) > MAX_KEY_LENGTH:
        raise HttpError(400, f"key must be at most {MAX_KEY_LENGTH} characters")
    return key


def _set_or_bad_request(fn):
    # store가 던지는 ValueError(잘못된 ttl)는 클라이언트 잘못이므로 400으로 바꾼다
    try:
        return fn()
    except ValueError as err:
        raise HttpError(400, str(err))


def build_routes(store):
    """(정규식, {메서드: 핸들러}) 목록. 핸들러는 (key, read_json)을 받아 (상태 코드, 본문)을 돌려준다."""

    def put(key, read_json):
        body = read_json()
        if "value" not in body:
            raise HttpError(400, "value is required")
        ttl_ms = body.get("ttlMs")
        existed = store.exists(key)
        _set_or_bad_request(lambda: store.set(key, body["value"], ttl_ms))
        return (200 if existed else 201), {"key": key, "ttlMs": ttl_ms}

    def get(key, _read_json):
        found = store.read(key)  # 값과 ttl을 한 번에 읽는다
        if found is None:
            return 404, {"error": f"key not found: {key}"}
        value, ttl = found
        return 200, {"key": key, "value": value, "ttlMs": None if ttl == -1 else ttl}

    def delete(key, _read_json):
        if store.del_(key):
            return 200, {"deleted": True}
        return 404, {"error": f"key not found: {key}"}

    def expire(key, read_json):
        body = read_json()
        ok = _set_or_bad_request(lambda: store.expire(key, body.get("ttlMs")))
        if ok:
            return 200, {"key": key, "ttlMs": body.get("ttlMs")}
        return 404, {"error": f"key not found: {key}"}

    def ttl(key, _read_json):
        remaining = store.ttl(key)
        if remaining == -2:
            return 404, {"error": f"key not found: {key}"}
        return 200, {"key": key, "ttlMs": remaining}

    def incr(key, _read_json):
        return 200, {"key": key, "value": store.incr(key)}

    def health(_key, _read_json):
        return 200, {"status": "ok"}

    def stats(_key, _read_json):
        return 200, {"size": store.size()}

    return [
        (re.compile(r"/health"), {"GET": health}),
        (re.compile(r"/stats"), {"GET": stats}),
        (re.compile(r"/keys/([^/]*)"), {"PUT": put, "GET": get, "DELETE": delete}),
        (re.compile(r"/keys/([^/]*)/expire"), {"POST": expire}),
        (re.compile(r"/keys/([^/]*)/ttl"), {"GET": ttl}),
        (re.compile(r"/keys/([^/]*)/incr"), {"POST": incr}),
    ]


def dispatch(routes, method, raw_path, read_json):
    """요청 하나를 처리해 (상태 코드, 본문, 추가 헤더)를 돌려준다. 실패하면 HttpError를 던진다."""
    if method == "OPTIONS":
        return 204, None, {}  # CORS 프리플라이트
    path = raw_path.split("?", 1)[0]
    for pattern, methods in routes:
        match = pattern.fullmatch(path)
        if match:
            break
    else:
        raise HttpError(404, "not found")
    handler = methods.get(method)
    if handler is None:
        raise HttpError(405, "method not allowed", {"Allow": ", ".join(methods)})
    key = _decode_key(match.group(1)) if match.groups() else None
    status, body = handler(key, read_json)
    return status, body, {}


class _Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"  # 연결을 재사용하려면 항상 Content-Length를 보내야 한다

    def log_message(self, format, *args):
        return  # 기본 로그 대신 server.logger로 남긴다

    def do_GET(self):
        self._handle("GET")

    def do_PUT(self):
        self._handle("PUT")

    def do_POST(self):
        self._handle("POST")

    def do_DELETE(self):
        self._handle("DELETE")

    def do_OPTIONS(self):
        self._handle("OPTIONS")

    def do_PATCH(self):
        self._handle("PATCH")  # 지원하지 않는 메서드도 JSON 405로 답하도록 명시적으로 받는다

    def _handle(self, method):
        started = time.monotonic()
        failure = None
        self._body_consumed = False
        self._last_status = 0
        try:
            status, body, headers = dispatch(self.server.routes, method, self.path, self._read_json)
            self._send(status, body, headers)
        except HttpError as err:
            self._send(err.status, {"error": err.message}, err.headers)
        except Exception as err:  # 내부 정보는 로그로만 남기고 응답에는 노출하지 않는다
            failure = err
            self._send(500, {"error": "internal error"})
        finally:
            if self.server.logger:
                ms = round((time.monotonic() - started) * 1000)
                self.server.logger(method, self.path, self._last_status, ms, failure)

    def _read_json(self):
        body = read_json_object(self.rfile, self.headers, self.server.max_body)
        self._body_consumed = True
        return body

    def _send(self, status, body, headers=None):
        headers = headers or {}
        data = b"" if status == 204 else json.dumps(body).encode("utf-8")
        self._last_status = status
        self.send_response(status)
        self.send_header("Access-Control-Allow-Origin", self.server.cors_origin)
        self.send_header("Access-Control-Allow-Methods", "GET, PUT, POST, DELETE, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        if status != 204:
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
        for name, value in headers.items():
            self.send_header(name, value)
        # 읽지 않은 본문이 남았거나 413이면 연결을 닫아서 다음 요청과 섞이지 않게 한다
        unread = not self._body_consumed and _content_length(self.headers) > 0
        if unread or headers.get("Connection") == "close":
            self.close_connection = True
            if "Connection" not in headers:
                self.send_header("Connection", "close")
        self.end_headers()
        if data:
            self.wfile.write(data)


class MiniRedisServer(ThreadingHTTPServer):
    allow_reuse_address = False  # Windows에서 SO_REUSEADDR는 이미 쓰는 포트를 빼앗을 수 있으므로 끈다
    daemon_threads = True

    def __init__(self, address, store, *, cors_origin="*", logger=None, max_body=DEFAULT_MAX_BODY_BYTES):
        super().__init__(address, _Handler)
        self.store = store
        self.routes = build_routes(store)
        self.cors_origin = cors_origin
        self.logger = logger
        self.max_body = max_body


def create_server(store, host="127.0.0.1", port=6380, **options):
    """서버 객체만 만든다 (아직 요청을 받지 않는다). 테스트에서는 port=0으로 빈 포트를 쓴다."""
    return MiniRedisServer((host, port), store, **options)


def _log(method, path, status, ms, error):
    suffix = f" {error!r}" if error else ""
    print(f"{method} {path} {status} {ms}ms{suffix}", flush=True)


def main():
    # 기본 포트 6380: 3000번대는 개발 도구가 자주 쓰고, 6379(실제 Redis)와도 겹치지 않게 비켜 둔다
    port = int(os.environ.get("PORT", "6380"))
    # 인증이 없으므로 기본은 이 컴퓨터에서만 접속되는 127.0.0.1
    host = os.environ.get("HOST", "127.0.0.1")
    store = Store()
    store.start_sweeper(1000)
    try:
        server = create_server(store, host, port, logger=_log)
    except OSError as err:
        if err.errno not in (errno.EADDRINUSE, 10048):  # 10048: Windows의 WSAEADDRINUSE
            raise
        print(f"포트 {port}이 이미 사용 중입니다. PORT 환경변수로 다른 포트를 지정하세요 (예: PORT={port + 1} python -m src.server)", file=sys.stderr)
        sys.exit(1)

    def stop(signum, _frame):
        print(f"signal {signum} received, shutting down", flush=True)
        threading.Thread(target=server.shutdown, daemon=True).start()  # serve_forever를 도는 스레드에서 shutdown하면 교착된다

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    print(f"mini-redis listening on http://{host}:{port}", flush=True)
    try:
        server.serve_forever()
    finally:
        server.server_close()
        store.stop_sweeper()


if __name__ == "__main__":
    main()
