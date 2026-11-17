# mini-redis

GoRedis의 단일 노드 기능을 Python 표준 라이브러리로 이식한 Redis 학습용 서버입니다. 한 프로세스의 `asyncio` 이벤트 루프가 여러 TCP 연결을 처리합니다. 모든 클라이언트는 같은 키 공간을 사용하며, 서버는 완전한 명령을 하나씩 실행합니다. 외부 Python 패키지 없이 Python 3.9 이상에서 실행할 수 있습니다.

## 실행

저장소 루트에서 서버를 시작합니다.

```sh
python3 -m miniredis
```

다른 터미널에서 `redis-cli`로 접속합니다.

```sh
redis-cli PING                       # PONG
redis-cli SET visits 9 EX 60         # OK
redis-cli INCR visits                # 10
redis-cli GET visits                 # "10"
redis-cli HSET user:42 city Seoul    # (integer) 1
redis-cli HGET user:42 city          # "Seoul"
```

기본 주소는 `127.0.0.1:6379`입니다. 현재 디렉터리의 `appendonlydir/`에 RDB와 증분 AOF를 저장하며, 같은 디렉터리에서 재시작하면 데이터를 복구합니다. 메모리 전용 서버는 `python3 -m miniredis -addr 127.0.0.1:6380 -aof ''`로 실행합니다.

## 지원 범위

| 기능 | 지원 내용 |
| --- | --- |
| 클라이언트 | RESP2, 바이너리 키·값, 파이프라이닝 |
| 문자열 | `PING`, `SET` (`EX`, `NX`), `GET`, `INCR`, `MSET`, `MGET` |
| 해시·키 | `HSET`, `HGET`, `TYPE`, `DEL`, `EXISTS`, `EXPIRE`, `TTL` |
| 영속화 | DB 0의 문자열·해시·키 만료 시각, Redis 형식의 RDB와 다중 파일 AOF, `BGREWRITEAOF` |

## HTTP API 응답 캐싱

예제 API는 SQLite에 담긴 주문 8만 건을 카테고리별로 집계합니다. Mini Redis를 실행한 상태에서 두 번째 터미널로 API를 시작합니다. 처음 실행하면 예제 DB `orders.sqlite3`가 만들어집니다.

```sh
python3 -m examples.cache_api --db orders.sqlite3 --cache on --ttl 30
```

세 번째 터미널에서 같은 보고서를 두 번 요청합니다.

```sh
curl -i http://127.0.0.1:8080/reports/1
curl -i http://127.0.0.1:8080/reports/1
```

첫 응답의 `X-Cache: MISS`는 API가 SQLite에서 집계한 JSON을 Mini Redis에 30초간 저장했다는 뜻입니다. 만료 전에 다시 호출하면 `X-Cache: HIT`와 함께 같은 본문을 받습니다.

```json
{"category_id":1,"order_count":10000,"total_amount":51010000}
```

DB를 갱신한 뒤 즉시 새 결과가 필요하면 같은 DB 경로로 보고서 키를 구해 삭제합니다.

```sh
key=$(python3 -c 'from examples.cache_api import report_cache_key; print(report_cache_key("orders.sqlite3", 1).decode())')
redis-cli DEL "$key"
curl -i http://127.0.0.1:8080/reports/1
```

`--cache off`로 API를 시작하면 요청마다 SQLite에서 계산하며 `X-Cache: BYPASS`를 반환합니다. `--cache on`에서 Mini Redis에 연결할 수 없으면 SQLite에서 계산한 결과에 `X-Cache: UNAVAILABLE`을 표시합니다.

## 캐시 사용 전후 성능

같은 SQLite 파일의 주문 8만 건을 대상으로 `GET /reports/1`부터 `/reports/8`까지 순환 호출했습니다. 캐시를 끄면 매번 카테고리 인덱스가 없는 주문 테이블에서 건수와 금액 합계를 구합니다. 캐시를 켜면 첫 조회에서 계산한 JSON을 Mini Redis에서 읽습니다.

| HTTP 요청 처리 방식 | 요청 수 | 응답 시간 중앙값 | 응답 시간 p95 | 초당 처리량 |
| --- | ---: | ---: | ---: | ---: |
| 캐시 끔 · SQLite 직접 집계 | 4,800 | 1.382 ms | 1.571 ms | 704.0건 |
| 캐시 켬 · Mini Redis 적중 | 4,800 | 0.306 ms | 0.354 ms | 3,147.8건 |

이 조건에서 캐시 적중 시 응답 시간 중앙값은 **77.9% 감소**했고 처리량은 **4.47배**였습니다. 예열 중 각 카테고리를 처음 요청한 8건은 모두 `MISS`였고 중앙값은 2.990 ms였습니다. 이 요청들은 표에서 제외했습니다.

- 2026-11-17, Apple M4, macOS 15.3.2, Python 3.9.6, SQLite 3.43.2에서 실행했습니다. API와 메모리 전용 Mini Redis를 같은 기기에서 실행했고 TTL은 3,600초로 설정했습니다.
- 모드마다 8개 카테고리를 두 번씩 예열한 뒤 800건씩 6회 측정했습니다. 라운드마다 모드 순서를 바꾸고, 클라이언트 하나가 요청마다 새 HTTP 연결을 열었습니다.
- 응답 시간은 연결 생성 직전부터 본문을 모두 읽을 때까지 측정했습니다. 처리량은 전체 요청 수를 각 모드의 총 측정 시간으로 나눈 값이며, 매 응답의 상태 코드·캐시 상태·JSON과 원본 집계의 일치 여부를 검사하는 시간도 포함합니다.

아래 명령은 임시 DB와 서버를 준비하고 측정이 끝나면 정리합니다. 라운드별 수치와 첫 조회 결과는 JSON에서 확인할 수 있습니다. [측정 결과 JSON](benchmarks/results/2026-11-17-m4.json) · [측정 코드](benchmarks/cache_api.py)

```sh
python3 -m benchmarks.cache_api --requests 800 --rounds 6
```

## 테스트

```sh
python3 -m unittest discover -v
```

테스트는 임시 포트와 디렉터리를 사용합니다. `redis-server`가 PATH에 있으면 같은 명령을 실제 Redis에도 보내 RESP2 응답을 비교하고, 양쪽이 서로의 RDB와 다중 파일 AOF를 읽는지 검사합니다. 다른 경로의 실행 파일은 다음처럼 지정합니다.

```sh
REDIS_SERVER=/path/to/redis-server python3 -m unittest discover -v
```

HTTP 캐시의 저장·재사용, 만료·삭제 후 갱신, 없는 보고서, 캐시 장애 시 응답은 `python3 -m unittest -v tests.test_cache_api`로 따로 검사할 수 있습니다.

[해시 테이블 설계, 명령 실행 흐름과 데이터 복구](docs/guide.md)
