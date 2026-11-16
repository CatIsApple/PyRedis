# mini-redis 포팅 설명

원본: `LeeKangHyun/redis` (JavaScript, Node 26). 이 폴더는 같은 기능을 Python 표준 라이브러리만으로 옮긴 것입니다. 외부 패키지는 없습니다.

## 실행

```bash
cd "C:/Users/User/Desktop/정글AI랩사이트만들기/mini-redis"
python -m demo.seed_users     # 테스트 계정 (이미 users.json 있음)
python -m demo.seed_db        # 대시보드용 DB 생성 (30만 행)
python -m src.server          # 미니 Redis, http://127.0.0.1:6380
python -m demo.server         # 데모 앱, http://127.0.0.1:4000
python -m unittest discover -s tests -t .   # 테스트
python -m bench.benchmark --spawn           # 벤치마크
```

로그인: `demo` / `demo1234`, `alice` / `alice1234`.

## 파일별 설명

### src/ (미니 Redis 코어)

| 파일 | 역할 | 주요 함수·클래스 |
|---|---|---|
| `hash_table.py` | 직접 만든 해시 테이블 (dict 미사용). 버킷 배열 + 체이닝, 적재율 0.75 초과 시 2배 rehash | `fnv1a`, `HashTable.set/get/has/delete/items`, `_resize` |
| `store.py` | 해시 테이블 위 저장소. 값은 JSON 문자열, 항목마다 만료 시각. lazy 만료(읽을 때)와 active 만료(sweep) | `Store.set/get/read/del_/exists/expire/ttl/incr/size/sweep`, `start_sweeper`, `_live` |
| `server.py` | HTTP REST 서버 (`http.server`). URL을 명령으로 바꾸고 상태 코드 결정 | `build_routes`, `dispatch`, `read_json_object`(1MB 제한), `MiniRedisServer`, `main` |
| `client.py` | 서버를 부르는 클라이언트 (`urllib`). 연결 실패·시간 초과·4xx/5xx를 예외로 구분 | `MiniRedisClient`, `RedisUnavailableError`, `RedisTimeoutError`, `RedisHttpError` |
| `__init__.py` | 패키지 표시 (빈 파일) | - |

HTTP 라우트: `/health`, `/stats`, `/keys/<k>` (PUT 저장, GET 조회, DELETE 삭제), `/keys/<k>/expire`, `/keys/<k>/ttl`, `/keys/<k>/incr`.

### demo/ (데모 앱)

| 파일 | 역할 | 주요 함수·클래스 |
|---|---|---|
| `server.py` | 로그인·세션·대시보드 API와 정적 파일 서버. 미니 Redis는 인증을 거친 뒤에만 씀 | `DemoApp` (login, me, logout, dashboard, clear_dashboard_cache), `resolve_inside`(경로 탈출 차단), `check_mini_redis`, `main` |
| `auth.py` | scrypt 비밀번호 해시·검증, 세션 id, 쿠키 파싱 | `hash_password`, `verify_password`(상수 시간 비교), `new_session_id`, `parse_cookies`, `load_users` |
| `db.py` | 대시보드 원본 데이터. SQLite 읽기 전용 연결 | `open_db`, `DashboardDb.get_dashboard_stats`, `DashboardDb.info`, `DbNotSeededError` |
| `seed_users.py` | 테스트 계정 생성 → `users.json` (평문 비밀번호 저장 안 함) | `seed_users` |
| `seed_db.py` | 벤치마크용 `events` 표 생성 (mulberry32로 재현 가능), 선택적 username 인덱스 | `create_rng`, `seed_db` |
| `users.json` | 계정 해시 (JS에서 만든 것을 그대로 가져옴, Python에서 로그인 검증 확인) | - |
| `web/` | 브라우저 화면 (JS 그대로). `index.html`, `app.js`, `style.css`, `stats.js` | - |

데모 API: `POST /api/login`, `GET /api/me`, `POST /api/logout`, `GET /api/dashboard?mode=db|redis`, `DELETE /api/dashboard/cache`.

로그인 보안 동작: 실패 5회 → 5분 잠금(429), 세션은 30분 sliding, 미니 Redis가 죽으면 503 (fail closed), POST는 `application/json`만 허용(415).

### mini_react/

원본 `react/src`를 복사한 미니 React. 데모 화면이 `/mini-react/`로 가져갑니다. `app/` 폴더는 원본에 있던 것 그대로입니다.

### bench/ (성능 측정)

| 파일 | 역할 | 주요 함수 |
|---|---|---|
| `benchmark.py` | DBMS 직접 / Redis HIT / Redis MISS를 동시성 1·20(MISS는 1)으로 측정. `--spawn`이면 서버를 직접 띄움 | `parse_args`, `run_phase`, `run_benchmark`, `format_markdown`, `save_result`, `spawn_servers` |
| `stats.py` | 평균·p50·p95·p99 계산 | `percentile`, `summarize` |

### tests/ (테스트 42개)

| 파일 | 대상 |
|---|---|
| `test_hash_table.py` | 해시 테이블 기본 동작, 충돌, rehash, 특수 키 |
| `test_store.py` | 저장소 전반: TTL, 만료, incr, sweep |
| `test_server.py` | HTTP API 실제 호출, 413, 동시 incr 정확성, 클라이언트, 연결 실패 |
| `test_bench_stats.py` | 통계, 인자 파싱, 마크다운 출력 |
| `test_demo.py` | 인증 해시, DB 시드, 경로 탈출 차단, 로그인→대시보드 흐름 |

## 원본(JS)과 다른 점

- `Store.del`은 파이썬 예약어라 `del_`로 이름을 바꿨습니다.
- Node의 단일 스레드 이벤트 루프 대신 `ThreadingHTTPServer` + `threading.RLock`으로 명령을 원자적으로 만들었습니다.
- `unquote`는 잘못된 퍼센트 인코딩에서 예외를 던지지 않습니다 (JS는 400).
- `NaN`은 `null`이 되지 않고 저장을 거절합니다.
- 해시 함수는 UTF-8 바이트 기준이라 칸 배치가 JS와 다르지만 동작 결과는 같습니다.
- 원본 `docs/` 도구, 문서 검사 테스트, `bench/results/` 과거 결과는 옮기지 않았습니다.

## 미확인 / 한계

- Windows에서 실행 확인 (Python 3.11). 다른 OS는 확인 안 함.
- 벤치마크 숫자는 이 PC의 상대 비교일 뿐입니다 (원본 README와 같은 한계).
- 세션 절대 만료 상한, 로그인 IP 제한, 디스크 지속성은 원본과 같이 없습니다.
