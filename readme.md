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

## 테스트

```sh
python3 -m unittest discover -v
```

테스트는 임시 포트와 디렉터리를 사용합니다. `redis-server`가 PATH에 있으면 같은 명령을 실제 Redis에도 보내 RESP2 응답을 비교하고, 양쪽이 서로의 RDB와 다중 파일 AOF를 읽는지 검사합니다. 다른 경로의 실행 파일은 다음처럼 지정합니다.

```sh
REDIS_SERVER=/path/to/redis-server python3 -m unittest discover -v
```

[해시 테이블 설계, 명령 실행 흐름과 데이터 복구](docs/guide.md)
