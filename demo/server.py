"""데모 앱 서버: 로그인·세션·대시보드 API와 정적 파일. 실행: python -m demo.server

브라우저는 이 서버(/api/*)만 부르고, 미니 Redis는 이 서버가 인증을 거친 뒤에만 쓴다.

주요 클래스/함수: DemoApp(로직: login/me/logout/dashboard/clear_dashboard_cache, _authenticate 세션 확인·연장),
DemoApp.static(정적 파일), resolve_inside(경로 탈출 차단), _make_handler(HTTP 처리·에러→상태 코드 변환: 401/415/429/503/500),
create_demo_server, check_mini_redis(시작 시 /health 확인), main(환경변수·포트·종료).
"""

import json
import os
import re
import sys
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlsplit

from src.client import MiniRedisClient, RedisHttpError, RedisUnavailableError
from src.server import HttpError, read_json_object

from .auth import DUMMY_CREDENTIAL, SESSION_ID_PATTERN, load_users, new_session_id, parse_cookies, verify_password
from .db import DEFAULT_DB_PATH, DbNotSeededError, open_db

DEMO_DIR = Path(__file__).resolve().parent
WEB_ROOT = DEMO_DIR / "web"

# 정해진 확장자만 제공한다. 목록에 없는 파일(.env, .md 등)은 있어도 404다.
MIME_TYPES = {
    ".html": "text/html; charset=utf-8",
    ".js": "text/javascript; charset=utf-8",
    ".css": "text/css; charset=utf-8",
    ".json": "application/json; charset=utf-8",
    ".svg": "image/svg+xml",
    ".png": "image/png",
    ".ico": "image/x-icon",
}
WINDOWS_RESERVED_NAME = re.compile(r"^(con|prn|aux|nul|com[1-9]|lpt[1-9])(\.|$)", re.IGNORECASE)
UNSAFE_CHARS = re.compile(r"[\\/\0:]")


def _session_key(sid):
    return f"session:{sid}"


def _not_found():
    return HttpError(404, "not found")


def resolve_inside(root, relative):
    """요청 경로를 root 안의 실제 파일 경로로 바꾼다. root 밖으로 나가려는 시도는 모두 404."""
    raw_segments = [] if relative == "" else relative.split("/")
    segments = []
    for i, raw in enumerate(raw_segments):
        if raw == "" and i == len(raw_segments) - 1:
            continue  # 끝의 "/"는 디렉터리 요청
        segment = unquote(raw)  # "/"로 먼저 나눈 뒤 조각마다 디코딩해야 %2f와 %2e%2e를 걸러낼 수 있다
        # 빈 조각, 숨김 파일과 "..", 구분자(/ \), NUL, ":"(드라이브·대체 스트림), Windows 장치 이름은 모두 거절
        if segment == "" or segment.startswith(".") or UNSAFE_CHARS.search(segment) or WINDOWS_RESERVED_NAME.match(segment):
            raise _not_found()
        segments.append(segment)
    target = root.joinpath(*segments)
    try:
        if target.is_dir():
            target = target / "index.html"  # 디렉터리는 index.html이 있을 때만 제공
        if not target.is_file():
            raise _not_found()
        real_target = target.resolve()  # 심볼릭 링크로 root 밖을 가리키는 경우까지 막는다
        if not real_target.is_relative_to(root.resolve()):
            raise _not_found()
    except OSError:
        raise _not_found()
    return real_target


def _is_json(headers):
    return (headers.get("Content-Type") or "").split(";")[0].strip().lower() == "application/json"


def _qget(query, name):
    values = query.get(name)
    return values[0] if values else None


class _Request:
    def __init__(self, headers, query, read_json):
        self.headers = headers
        self.query = query
        self.read_json = read_json


class DemoApp:
    """세션·로그인·대시보드 로직. HTTP와 분리해 두어 라우팅 없이도 이해할 수 있게 했다."""

    def __init__(self, *, redis_client, users, db, session_ttl_ms=30 * 60 * 1000, max_login_failures=5, lockout_ms=5 * 60 * 1000, dashboard_delay_ms=0, dashboard_cache_ttl_ms=30_000, web_root=WEB_ROOT):
        self.redis = redis_client
        self.users = users
        self.db = db  # { get_dashboard_stats(username) } (demo/db.py, 다른 DBMS로 교체 가능)
        self.session_ttl_ms = session_ttl_ms
        self.max_login_failures = max_login_failures
        self.lockout_ms = lockout_ms
        self.dashboard_delay_ms = dashboard_delay_ms
        self.dashboard_cache_ttl_ms = dashboard_cache_ttl_ms
        self.web_root = Path(web_root)
        self.routes = {
            "/api/login": {"POST": self.login},
            "/api/me": {"GET": self.me},
            "/api/logout": {"POST": self.logout},
            "/api/dashboard": {"GET": self.dashboard},
            "/api/dashboard/cache": {"DELETE": self.clear_dashboard_cache},
        }

    # ---- 세션 ----

    def _authenticate(self, headers):
        """쿠키의 sid로 세션을 찾고, 찾으면 만료 시간을 다시 늘린다 (sliding expiration)."""
        sid = parse_cookies(headers.get("Cookie")).get("sid")
        if not sid or not SESSION_ID_PATTERN.fullmatch(sid):
            return None  # 형식이 틀리면 저장소를 조회하지 않는다
        session = self.redis.get(_session_key(sid))
        if not isinstance(session, dict) or not isinstance(session.get("username"), str):
            return None
        if not self.redis.expire(_session_key(sid), self.session_ttl_ms):
            return None  # 그 사이에 만료됐다
        return sid, session["username"]

    def _require_session(self, headers):
        auth = self._authenticate(headers)
        if auth is None:
            raise HttpError(401, "not authenticated")
        return auth

    @staticmethod
    def _session_cookie(sid, ttl_ms):
        return f"sid={sid}; HttpOnly; SameSite=Lax; Path=/; Max-Age={ttl_ms // 1000}"

    CLEARED_COOKIE = "sid=; HttpOnly; SameSite=Lax; Path=/; Max-Age=0"

    # ---- API ----

    def login(self, req):
        body = req.read_json()
        username, password = body.get("username"), body.get("password")
        if not isinstance(username, str) or not isinstance(password, str) or username == "" or password == "" or len(username) > 128 or len(password) > 128:
            raise HttpError(400, "username and password are required")

        # 로그인 시도 제한: 아이디가 있든 없든 같은 규칙을 적용해서 응답 차이로 아이디를 알아내지 못하게 한다
        fail_key = f"login_fail:{username}"
        failures = self.redis.get(fail_key) or 0
        if failures >= self.max_login_failures:
            ttl = self.redis.ttl(fail_key)
            retry_after = -(-ttl // 1000) if ttl > 0 else -(-self.lockout_ms // 1000)  # 올림 나눗셈
            return 429, {"error": "too many failed attempts"}, {"Retry-After": str(int(retry_after))}

        user = self.users.get(username)
        # 없는 아이디에도 가짜 계정으로 같은 해시 계산을 해서 응답 시간으로 존재 여부를 알 수 없게 한다
        password_ok = verify_password(password, user or DUMMY_CREDENTIAL)
        if user is None or not password_ok:
            count = self.redis.incr(fail_key)
            # incr와 expire는 서로 다른 HTTP 호출이라 그 사이에 멈추면 만료 없는 카운터가 남을 수 있다. 만료가 없으면 다시 건다.
            if count == 1 or self.redis.ttl(fail_key) == -1:
                self.redis.expire(fail_key, self.lockout_ms)
            return 401, {"error": "invalid credentials"}, {}

        self.redis.delete(fail_key)
        sid = new_session_id()
        self.redis.set(_session_key(sid), {"username": username, "createdAt": int(time.time() * 1000)}, self.session_ttl_ms)
        return 200, {"username": username}, {"Set-Cookie": self._session_cookie(sid, self.session_ttl_ms)}

    def me(self, req):
        sid, username = self._require_session(req.headers)
        # 쿠키의 Max-Age도 같이 늘려야 브라우저가 쿠키를 먼저 버리지 않는다
        return 200, {"username": username, "expiresInMs": self.session_ttl_ms}, {"Set-Cookie": self._session_cookie(sid, self.session_ttl_ms)}

    def logout(self, req):
        sid = parse_cookies(req.headers.get("Cookie")).get("sid")
        try:
            if sid and SESSION_ID_PATTERN.fullmatch(sid):
                self.redis.delete(_session_key(sid))  # 세션이 없어도 성공으로 본다 (멱등)
        except (RedisUnavailableError, RedisHttpError):
            # 서버의 세션은 못 지웠지만 브라우저의 쿠키는 지운다. 세션은 만료 시간에 저절로 사라진다.
            return 503, {"error": "session store unavailable"}, {"Set-Cookie": self.CLEARED_COOKIE}
        return 200, {"ok": True}, {"Set-Cookie": self.CLEARED_COOKIE}

    @staticmethod
    def _dashboard_mode(query):
        mode = _qget(query, "mode")
        if mode is None:
            return "db" if _qget(query, "cache") == "off" else "redis"  # 예전의 ?cache=off는 mode=db와 같다
        if mode not in ("db", "redis"):
            raise HttpError(400, "mode must be db or redis")
        return mode

    def dashboard(self, req):
        _, username = self._require_session(req.headers)
        mode = self._dashboard_mode(req.query)
        cache_key = f"cache:dashboard:{username}"  # 사용자별 키라서 다른 사용자의 데이터가 섞이지 않는다
        started = time.perf_counter()

        def elapsed_ms():
            return round((time.perf_counter() - started) * 1000, 2)  # 반올림하지 않는다: 3ms와 0.4ms의 차이가 보여야 한다

        if mode == "redis":
            cached = self.redis.get(cache_key)
            if cached is not None:
                return 200, {"data": cached, "source": "redis", "elapsedMs": elapsed_ms()}, {"X-Source": "redis", "X-Cache": "HIT"}
        data = self.db.get_dashboard_stats(username)  # SQL 조회 (sqlite3 호출은 동기다)
        if self.dashboard_delay_ms > 0:
            time.sleep(self.dashboard_delay_ms / 1000)
        if mode == "redis":
            self.redis.set(cache_key, data, self.dashboard_cache_ttl_ms)
        return 200, {"data": data, "source": "db", "elapsedMs": elapsed_ms()}, {"X-Source": "db", "X-Cache": "MISS" if mode == "redis" else "BYPASS"}

    def clear_dashboard_cache(self, req):
        _, username = self._require_session(req.headers)
        return 200, {"deleted": self.redis.delete(f"cache:dashboard:{username}")}, {}

    def api(self, method, path, req):
        methods = self.routes.get(path)
        if methods is None:
            raise HttpError(404, "not found")
        handler = methods.get(method)
        if handler is None:
            raise HttpError(405, "method not allowed", {"Allow": ", ".join(methods)})
        # 상태를 바꾸는 POST는 application/json만 받는다. HTML 폼은 이 타입을 보낼 수 없어서 단순한 CSRF 폼 전송이 막힌다.
        if method == "POST" and not _is_json(req.headers):
            raise HttpError(415, "Content-Type must be application/json")
        return handler(req)

    # ---- 정적 파일 ----

    def static(self, method, path):
        if method not in ("GET", "HEAD"):
            raise HttpError(405, "method not allowed", {"Allow": "GET, HEAD"})
        if path == "/":
            root, relative = self.web_root, "index.html"
        elif path.startswith("/web/"):
            root, relative = self.web_root, path[len("/web/"):]
        else:
            raise _not_found()
        file = resolve_inside(root, relative)
        content_type = MIME_TYPES.get(file.suffix.lower())
        if content_type is None:
            raise _not_found()
        headers = {"Cache-Control": "no-cache", "X-Content-Type-Options": "nosniff"}
        if content_type.startswith("text/html"):
            headers["Content-Security-Policy"] = "default-src 'self'; frame-ancestors 'none'"
        return file.read_bytes(), content_type, headers


def _make_handler(app):
    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, format, *args):
            return

        def do_GET(self):
            self._handle("GET")

        def do_POST(self):
            self._handle("POST")

        def do_DELETE(self):
            self._handle("DELETE")

        def do_HEAD(self):
            self._handle("HEAD")

        def do_PUT(self):
            self._handle("PUT")

        def do_PATCH(self):
            self._handle("PATCH")

        def _handle(self, method):
            started = time.monotonic()
            failure = None
            self._status = 0
            parts = urlsplit(self.path)
            path, query = parts.path, parse_qs(parts.query, keep_blank_values=True)
            try:
                if path.startswith("/api/"):
                    req = _Request(self.headers, query, lambda: read_json_object(self.rfile, self.headers, self.server.max_body))
                    status, body, headers = app.api(method, path, req)
                    self._send_json(status, body, headers)
                else:
                    content, content_type, headers = app.static(method, path)
                    self._send_bytes(200, content, content_type, headers, head=(method == "HEAD"))
            except HttpError as err:
                self._send_json(err.status, {"error": err.message}, err.headers)
            except (RedisUnavailableError, RedisHttpError) as err:
                failure = err
                # 세션 저장소를 못 쓰면 인증을 통과시키지 않는다 (fail closed)
                self._send_json(503, {"error": "session store unavailable"})
            except Exception as err:  # 내부 정보는 로그에만 남긴다
                failure = err
                self._send_json(500, {"error": "internal error"})
            finally:
                if self.server.logger:
                    self.server.logger(method, self.path, self._status, round((time.monotonic() - started) * 1000), failure)

        def _send_json(self, status, body, headers=None):
            data = json.dumps(body).encode("utf-8")
            self._send_bytes(status, data, "application/json", {"Cache-Control": "no-store", "X-Content-Type-Options": "nosniff", **(headers or {})})

        def _send_bytes(self, status, data, content_type, headers, head=False):
            self._status = status
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(data)))
            for name, value in headers.items():
                self.send_header(name, value)
            self.end_headers()
            if not head:
                self.wfile.write(data)

    return Handler


class DemoHTTPServer(ThreadingHTTPServer):
    allow_reuse_address = False  # Windows에서 같은 포트를 빼앗지 않도록 끈다
    daemon_threads = True

    def __init__(self, address, app, *, max_body=1024 * 1024, logger=None):
        super().__init__(address, _make_handler(app))
        self.app = app
        self.max_body = max_body
        self.logger = logger


def create_demo_server(address=("127.0.0.1", 0), *, logger=None, **app_options):
    app = DemoApp(**app_options)
    return DemoHTTPServer(address, app, logger=logger)


def check_mini_redis(redis_url, timeout=2.0):
    """시작할 때 REDIS_URL이 정말 미니 Redis인지 /health로 확인한다. 결과는 (ok, message)."""
    url = f"{redis_url.rstrip('/')}/health"
    try:
        with urllib.request.urlopen(url, timeout=timeout) as res:
            body = json.loads(res.read().decode("utf-8") or "null")
            if body and body.get("status") == "ok":
                return True, ""
            return False, f"미니 Redis가 아닌 서버가 응답하고 있습니다 ({url} → HTTP {res.status}). 주소/포트를 확인하세요."
    except Exception as err:
        return False, f"미니 Redis에 연결할 수 없습니다 ({redis_url}: {err}). 먼저 `python -m src.server`로 미니 Redis를 켜고 주소/포트를 확인하세요."


def _env_number(name, fallback, allow_zero=False):
    raw = os.environ.get(name)
    if raw is None:
        return fallback
    try:
        value = float(raw)
    except ValueError:
        value = -1
    if value < 0 or (value == 0 and not allow_zero):
        raise SystemExit(f"{name}는 {'0 이상' if allow_zero else '양수'}여야 합니다 (받은 값: {raw})")
    return int(value)


def main():
    port = int(os.environ.get("PORT", "4000"))
    host = os.environ.get("HOST", "127.0.0.1")
    redis_url = os.environ.get("REDIS_URL", "http://127.0.0.1:6380")
    try:
        users = load_users(DEMO_DIR / "users.json")
    except (OSError, ValueError):
        sys.exit("demo/users.json을 읽을 수 없습니다. 먼저 `python -m demo.seed_users`로 테스트 계정을 만드세요.")
    try:
        db = open_db(os.environ.get("DEMO_DB_PATH", DEFAULT_DB_PATH))
    except DbNotSeededError as err:
        sys.exit(str(err))

    def log(method, path, status, ms, error):
        print(f"{method} {path} {status} {ms}ms{' ' + repr(error) if error else ''}", flush=True)

    try:
        server = create_demo_server(
            (host, port),
            redis_client=MiniRedisClient(redis_url),
            users=users,
            db=db,
            session_ttl_ms=_env_number("SESSION_TTL_MS", 30 * 60 * 1000),
            dashboard_delay_ms=_env_number("DASHBOARD_DELAY_MS", 0, allow_zero=True),
            dashboard_cache_ttl_ms=_env_number("DASHBOARD_CACHE_TTL_MS", 30_000),
            logger=log,
        )
    except OSError as err:
        if err.errno not in (98, 48, 10048):  # EADDRINUSE: Linux, macOS, Windows
            raise
        sys.exit(f"포트 {port}이 이미 사용 중입니다. PORT 환경변수로 다른 포트를 지정하세요 (예: PORT={port + 1} python -m demo.server)")

    print(f"demo app listening on http://{host}:{port} (mini redis: {redis_url})", flush=True)
    ok, message = check_mini_redis(redis_url)
    if not ok:
        print(f"경고: {message}", flush=True)  # 경고만 하고 서버는 계속 켠다 (미니 Redis를 나중에 켜도 동작하도록)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        db.close()


if __name__ == "__main__":
    main()
