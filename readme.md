<div align="center">

![Mini Redis](assets/banner.png)

<img src="assets/logo.png" width="88" alt="Mini Redis 로고" />

# Mini Redis

### Python 표준 라이브러리로 다시 만든 단일 노드 Redis

**연결은 여러 개, 실행은 한 줄로. 쓰기는 먼저 기록하고, 만료는 읽을 때와 틈날 때.**

![Python 3.9+](https://img.shields.io/badge/Python-3.9+-3776AB?logo=python&logoColor=white)
![dependencies](https://img.shields.io/badge/dependencies-0-2ea44f)
![protocol](https://img.shields.io/badge/protocol-RESP2-D9534F)
![architecture](https://img.shields.io/badge/architecture-single_thread_·_asyncio-1f6feb)
![persistence](https://img.shields.io/badge/persistence-RDB_+_AOF-c99a2e)

[![Quick Start](https://img.shields.io/badge/Quick_Start-24292f?style=for-the-badge)](#quick-start)
[![Design](https://img.shields.io/badge/Design-24292f?style=for-the-badge)](#과제-의도와-설계)
[![Client](https://img.shields.io/badge/Client-24292f?style=for-the-badge)](#클라이언트-입장)
[![Engine](https://img.shields.io/badge/Engine-24292f?style=for-the-badge)](#엔진-입장)
[![Benchmark](https://img.shields.io/badge/Benchmark-24292f?style=for-the-badge)](#benchmark)

</div>

<br />

## Quick Start

Python 3.9 이상, 외부 패키지 없이 실행합니다.

```sh
python3 -m miniredis                  # 127.0.0.1:6379, ./appendonlydir/에 저장
```

```sh
redis-cli SET visits 9 EX 60          # OK
redis-cli INCR visits                 # 10
redis-cli HSET user:42 city Seoul     # (integer) 1
redis-cli TTL visits                  # (integer) 60
```

메모리 전용: `python3 -m miniredis -addr 127.0.0.1:6380 -aof ''`

<br />

## 과제 의도와 설계

| 고민 | 선택 | 코드 |
| --- | --- | --- |
| **동시성** 문제를 없애려면? | 이벤트 루프 1개 + `await` 없는 동기 `execute`. 명령 하나가 끝나기 전에 다른 명령이 끼어들 수 없어 락이 필요 없습니다. | [server.py](miniredis/server.py) · [engine.py](miniredis/engine.py) |
| **만료된 값**을 요청받으면? | 조회할 때 지우고 nil (Lazy) + 0.1초마다 20개씩 훑어 지움 (Active) | [engine.py](miniredis/engine.py) |
| 외부에서 쉽게 쓰는 **API**는? | Redis와 같은 RESP2 over TCP. `redis-cli`가 그대로 붙고, HTTP 캐시 예제를 함께 둡니다. | [resp.py](miniredis/resp.py) · [examples/](examples) |
| 데이터 **무효화**는? | `DEL` 즉시 삭제 · `EXPIRE` 기한 예약 · `SET` 덮어쓰기. 모두 AOF에 남아 재시작 뒤에도 유지됩니다. | [engine.py](miniredis/engine.py) |
| **서버 다운**에도 데이터를? | AOF에 먼저 쓰고 메모리에 반영. base RDB + 증분 AOF + manifest로 복구합니다. | [persist.py](miniredis/persist.py) |

<br />

## 클라이언트 입장

### 요청 한 건의 흐름

![클라이언트가 보낸 RESP2 프레임을 읽고, 엔진이 HashTable에서 명령을 실행하며 AOF에 기록한 뒤, RESP2로 응답하는 흐름](assets/how-it-works.png)

1. **Read** — 연결마다 코루틴이 RESP2 프레임을 하나씩 읽습니다. 바이트를 기다리는 동안 다른 연결을 처리합니다.
2. **Execute** — `Engine.execute`가 인자를 검사하고 명령 하나를 끝까지 실행합니다.
3. **Record** — 쓰기 명령은 AOF에 먼저 기록하고, 성공해야 메모리에 반영합니다.
4. **Reply** — 결과를 RESP2로 돌려줍니다. 파이프라인은 보낸 순서대로 응답합니다.

### 쓸 수 있는 명령

| 종류 | 명령 |
| --- | --- |
| 문자열 | `SET` (`EX`, `NX`) · `GET` · `INCR` · `MSET` · `MGET` |
| 해시 | `HSET` · `HGET` |
| 키 | `DEL` · `EXISTS` · `TYPE` · `EXPIRE` · `TTL` |
| 서버 | `PING` · `BGREWRITEAOF` |

### API로 쓰기 — 캐시 예제

SQLite 주문 8만 건의 집계를 Mini Redis에 캐시하는 HTTP API입니다.

```sh
python3 -m examples.cache_api --db orders.sqlite3 --cache on --ttl 30
curl -i http://127.0.0.1:8080/reports/1    # X-Cache: MISS → SQLite 집계 후 SET ... EX 30
curl -i http://127.0.0.1:8080/reports/1    # X-Cache: HIT  → Mini Redis에서 바로 응답
```

DB가 바뀌면 키를 `DEL`해 바로 무효화합니다. Mini Redis가 죽어 있으면 SQLite 결과를 `X-Cache: UNAVAILABLE`로 돌려줍니다.

<br />

## 엔진 입장

### 세 개의 레이어

![접근(server.py, resp.py), 실행(engine.py, hashtable.py), 보존(persist.py, aof.py, rdb.py) 세 층과 0.1초마다 도는 maintain 루프](assets/layers.png)

| 레이어 | 하는 일 |
| --- | --- |
| ① 접근 | TCP 연결을 받고 RESP2를 읽고 씁니다. 64개 명령마다 루프에 양보합니다. |
| ② 실행 | 명령을 검사해 키 공간을 읽고 바꿉니다. 만료도 여기서 판정합니다. |
| ③ 보존 | 쓰기를 AOF에 남기고, RDB 스냅샷으로 압축하고, 시작할 때 복구합니다. |

### 동시성 — 싱글 스레드

![세 클라이언트의 연결을 이벤트 루프 하나가 받고, 명령이 한 줄로 서서 하나씩 실행되는 모습](assets/single-thread.png)

여러 연결을 `asyncio` 이벤트 루프 하나가 맡습니다. `await`는 소켓을 기다릴 때만 있고 `execute`는 동기 함수라, **명령 단위로 원자적**입니다. `INCR`을 동시에 보내도 값이 꼬이지 않습니다. 실제 Redis도 명령 실행은 스레드 하나입니다.

### 키 공간 — HashTable

![hash(key)로 버킷을 정하고 같은 버킷은 next로 잇는 체이닝, 그리고 삽입 순서 목록](assets/hashtable.png)

바깥 키는 직접 만든 [`HashTable`](miniredis/hashtable.py)에 둡니다. 충돌은 체이닝으로 풀고, 키가 버킷의 3/4을 넘으면 두 배로 늘립니다. 별도의 순서 목록은 만료 검사가 앞에서부터 돌 수 있게 합니다. 해시의 필드는 Python `dict`에 둡니다.

### 만료

![조회할 때 지우는 Lazy와 0.1초마다 20개씩 검사하는 Active 두 경로](assets/expiry.png)

만료 시각은 키마다 **절대 시각(ms)**으로 저장합니다. 읽을 때 지나 있으면 지우고 nil을 돌려주고, 아무도 읽지 않는 키는 `purge`가 치웁니다.

### 무효화

| 명령 | 동작 | AOF 기록 |
| --- | --- | --- |
| `DEL k` | 즉시 삭제 | `DEL k` (있던 키만) |
| `EXPIRE k s` | 기한 예약, 0 이하면 즉시 삭제 | `PEXPIREAT k <절대 ms>` |
| `SET k v` | 새 값으로 교체, 기존 TTL 해제 | `SET k v [PXAT <절대 ms>]` |

### 영속화

![쓰기는 incr.aof 기록 후 메모리 반영, 복구는 manifest → base.rdb → incr.aof 재생 순서](assets/persistence.png)

`appendonlydir/`은 실제 Redis 7과 같은 구조입니다. **manifest**는 지금 읽을 파일 목록, **base.rdb**는 한 시점의 스냅샷, **incr.aof**는 그 뒤의 쓰기 기록입니다. 재작성 중에 꺼져도 manifest가 옛 세트를 가리켜 안전합니다.

| `-appendfsync` | 디스크 동기화 | 전원이 꺼지면 |
| --- | --- | --- |
| `always` | 응답 전에 매번 | 잃지 않음 |
| `everysec` (기본) | 약 1초마다 | 마지막 약 1초 |
| `no` | 운영체제에 맡김 | 운영체제에 따라 |

<br />

## Benchmark

### 캐시 사용 전후

주문 8만 건에 대한 같은 보고서 8개를 클라이언트 하나가 순환 요청했습니다. 요청마다 새 HTTP 연결을 열고, 응답 JSON을 SQLite 원본과 대조했습니다.

![캐시를 켰을 때 응답 시간 중앙값 85% 감소, 초당 처리량 6.5배](assets/bench-cache.png)

| 처리 방식 | 요청 수 | 중앙값 | p95 | 초당 처리량 |
| --- | ---: | ---: | ---: | ---: |
| 캐시 끔 · SQLite 직접 집계 | 4,800 | 1.427 ms | 1.551 ms | 691.7건 |
| 캐시 켬 · Mini Redis 적중 | 4,800 | **0.211 ms** | **0.253 ms** | **4,469.6건** |

<sub>커밋 `7cc4efa` · Mac mini의 Linux VM (aarch64, 4코어) · Python 3.10.12 · SQLite 3.37.2 · 800건 × 6라운드, 라운드마다 순서 교대 · [결과 JSON](benchmarks/results/2026-11-17-7cc4efa-cache-api.json) · [측정 코드](benchmarks/cache_api.py)</sub>

```sh
python3 -m benchmarks.cache_api --requests 800 --rounds 6
```

### Redis 7과 비교

같은 기기에서 `redis-benchmark`로 SET·GET을 보냈습니다. 양쪽 모두 디스크 저장을 껐습니다.

| 초당 처리량 | Mini Redis | Redis 7.0.15 | 차이 |
| --- | ---: | ---: | ---: |
| SET · 파이프라인 없음 | 17,989 | 104,493 | 5.8× |
| GET · 파이프라인 없음 | 19,912 | 106,045 | 5.3× |
| SET · 파이프라인 16 | 21,249 | 1,234,568 | 58× |
| GET · 파이프라인 16 | 24,612 | 1,587,302 | 64× |

한 번에 하나씩 보내면 네트워크 왕복이 대부분이라 5배 남짓 차이지만, 파이프라인으로 몰아 보내면 명령 처리 자체의 속도(Python 대 C)가 그대로 드러납니다.

<sub>Linux x86_64, 2 vCPU · Python 3.13.18 · 연결 50개 · 명령당 10만 건 × 3회 중앙값 · [결과 JSON](benchmarks/results/2026-11-17-vs-redis-linux-x64.json) · [측정 코드](benchmarks/vs_redis.py)</sub>

```sh
python3 -m benchmarks.vs_redis       # redis-server, redis-benchmark 필요
```

<details>
<summary>해시 필드 저장소: dict vs 직접 만든 HashTable</summary>

<br />

필드를 `dict`에 둔 `9f67b74`와 `HashTable`에 둔 `de7f275`를 비교한 결과입니다. 이 결과로 필드 저장소는 `dict`를 택했습니다.

| 명령 (명령당 시간) | dict | HashTable |
| --- | ---: | ---: |
| `HSET` 새 필드 | **1.518 µs** | 2.842 µs |
| `HSET` 기존 필드 | **1.505 µs** | 2.096 µs |
| `HGET` 있는 필드 | **1.104 µs** | 1.392 µs |

<sub>키 1,024개 × 필드 16개 · Apple M4 · macOS 15.3.2 · CPython 3.9.6 · 8라운드 중앙값 · [결과 JSON](benchmarks/results/hash-fields-m4-python39.json) · [측정 코드](benchmarks/hash_fields.py)</sub>

</details>

<br />

## FAQ

**왜 멀티 스레드가 아닌가요?**<br />
명령 하나는 메모리 연산 몇 µs라, 병목은 CPU가 아니라 네트워크 I/O입니다. I/O는 이벤트 루프로 겹치고 실행은 한 줄로 세워 락과 경쟁 조건을 없앴습니다.

**대신 포기한 것은?**<br />
CPU 코어를 하나만 씁니다. `HashTable` 확장이나 RDB 스냅샷 복사처럼 긴 작업이 도는 동안 다른 연결도 잠시 기다립니다.

**프로세스가 죽으면 데이터가 사라지나요?**<br />
아니요. AOF 기록은 응답 전에 운영체제로 넘어가 있습니다. 기기 전원이 꺼질 때만 `everysec` 기준 마지막 약 1초가 사라질 수 있습니다.

<br />

## Test

```sh
python3 -m unittest discover -v
```

`redis-server`가 PATH에 있으면 같은 명령을 실제 Redis에도 보내 응답을 비교하고, 서로의 RDB와 AOF를 읽는지 검사합니다.

[명령 실행 흐름, 해시 테이블, 복구 자세히 보기](docs/guide.md)
