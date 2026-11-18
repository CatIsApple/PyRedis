<div align="center">

![Mini Redis](assets/banner.png)

<img src="assets/logo.png" width="88" alt="Mini Redis logo" />

# Mini Redis

### Python 표준 라이브러리로 만든 단일 노드 Redis

**Many connections, one line of execution. Log first, expire lazily.**

![Python 3.9+](https://img.shields.io/badge/Python-3.9+-3776AB?logo=python&logoColor=white)
![dependencies](https://img.shields.io/badge/dependencies-0-2ea44f)
![protocol](https://img.shields.io/badge/protocol-RESP2-D9534F)
![architecture](https://img.shields.io/badge/architecture-single_thread_·_asyncio-1f6feb)
![persistence](https://img.shields.io/badge/persistence-RDB_+_AOF-c99a2e)

[![Quick Start](https://img.shields.io/badge/Quick_Start-24292f?style=for-the-badge)](#quick-start)
[![Design](https://img.shields.io/badge/Design-24292f?style=for-the-badge)](#design-goals)
[![Client](https://img.shields.io/badge/Client-24292f?style=for-the-badge)](#client-side)
[![Engine](https://img.shields.io/badge/Engine-24292f?style=for-the-badge)](#engine-side)
[![Benchmark](https://img.shields.io/badge/Benchmark-24292f?style=for-the-badge)](#benchmark)

</div>

<br />

## Quick Start

Python 3.9+, 외부 패키지 없음.

```sh
python3 -m miniredis                  # 127.0.0.1:6379, ./appendonlydir/에 저장
```

```sh
redis-cli SET visits 9 EX 60          # OK
redis-cli INCR visits                 # 10
redis-cli HSET user:42 city Seoul     # (integer) 1
redis-cli TTL visits                  # (integer) 60
```

Memory only: `python3 -m miniredis -addr 127.0.0.1:6380 -aof ''`

<br />

## Design Goals

| Goal | Solution | Code |
| --- | --- | --- |
| **Concurrency** | Single thread. `asyncio` event loop 1개 + `await` 없는 `execute` → 명령이 섞이지 않아 lock 불필요 | [server.py](miniredis/server.py) · [engine.py](miniredis/engine.py) |
| **Expired keys** | On access: 읽을 때 검사 후 삭제 · Every 0.1s: 20개씩 이어서 검사 | [engine.py](miniredis/engine.py) |
| **API** | Redis와 같은 RESP2 over TCP → `redis-cli` 그대로 사용. HTTP cache 예제 포함 | [resp.py](miniredis/resp.py) · [examples/](examples) |
| **Invalidation** | `DEL` · `EXPIRE` · `SET` 덮어쓰기. 모두 AOF에 남아 재시작 후에도 유지 | [engine.py](miniredis/engine.py) |
| **Crash safety** | AOF first, memory second. base RDB + incr AOF + manifest로 복구 | [persist.py](miniredis/persist.py) |

<br />

## Client Side

### Request Flow

![Client → Read → Execute → Reply](assets/how-it-works.png)

1. **Read** — 연결마다 RESP2 명령을 하나씩 읽음. 기다리는 동안 다른 연결 처리
2. **Execute** — 명령 하나를 끝까지 실행. 쓰기는 AOF에 먼저 기록 후 메모리 반영
3. **Reply** — RESP2로 응답. pipeline은 보낸 순서대로

### Commands

| Type | Commands |
| --- | --- |
| String | `SET` (`EX`, `NX`) · `GET` · `INCR` · `MSET` · `MGET` |
| Hash | `HSET` · `HGET` |
| Key | `DEL` · `EXISTS` · `TYPE` · `EXPIRE` · `TTL` |
| Server | `PING` · `BGREWRITEAOF` |

### Use as an API — Cache Example

SQLite 주문 8만 건 집계를 Mini Redis에 캐시하는 HTTP API.

```sh
python3 -m examples.cache_api --db orders.sqlite3 --cache on --ttl 30
curl -i http://127.0.0.1:8080/reports/1    # X-Cache: MISS → SQLite 집계 후 SET ... EX 30
curl -i http://127.0.0.1:8080/reports/1    # X-Cache: HIT  → Mini Redis에서 응답
```

DB가 바뀌면 `DEL`로 바로 무효화. Mini Redis가 꺼져 있으면 `X-Cache: UNAVAILABLE`로 SQLite 결과 반환.

<br />

## Engine Side

### Architecture

![mini-redis: Access, Execute, Persist layers and a 0.1s background loop](assets/layers.png)

| Layer | Role |
| --- | --- |
| ① Access | TCP 연결 · RESP2 읽기/쓰기 |
| ② Execute | 명령 검사 · keyspace 읽기/쓰기 · 만료 판정 |
| ③ Persist | AOF 기록 · RDB snapshot · 시작 시 복구 |
| Every 0.1s | 만료 정리 · fsync · rewrite 조각 쓰기 |

### Concurrency — Single Thread

![Connections A, B, C meet in one event loop and run one at a time](assets/single-thread.png)

연결은 여러 개, 실행은 한 줄. `await`는 socket I/O에만 있고 `execute`는 동기 함수라 **명령 단위로 atomic**. `INCR`을 동시에 보내도 값이 꼬이지 않음. 실제 Redis도 명령 실행은 thread 하나.

### Keyspace — HashTable

![hash(key) picks a bucket; collisions chain in the same bucket; grows ×2 over 3/4 full](assets/hashtable.png)

직접 구현한 [`HashTable`](miniredis/hashtable.py). 충돌은 같은 bucket에 `next`로 연결(chaining), 3/4 넘게 차면 bucket 2배. hash의 field는 Python `dict` 사용.

### Expiration

![On access check, and a 0.1s loop that checks 20 keys per run](assets/expiry.png)

- **On access** — 읽을 때 만료됐으면 지우고 nil
- **Every 0.1s** — 앞에서 20개씩 검사, 살아 있는 키는 뒤로 보내 다음 루프가 이어서 검사

### Invalidation

| Command | Effect | AOF |
| --- | --- | --- |
| `DEL k` | 즉시 삭제 | `DEL k` (있던 키만) |
| `EXPIRE k s` | 기한 예약, 0 이하면 즉시 삭제 | `PEXPIREAT k <ms>` |
| `SET k v` | 새 값으로 교체, TTL 해제 | `SET k v [PXAT <ms>]` |

### Persistence

![Write: append to AOF, apply, reply. Restart: manifest, base.rdb + incr.aof replay](assets/persistence.png)

Redis 7과 같은 `appendonlydir/` 구조.

- **manifest** — 지금 읽을 파일 목록
- **base.rdb** — 한 시점의 snapshot
- **incr.aof** — snapshot 이후 쓰기 기록

rewrite 중에 꺼져도 manifest가 이전 파일을 가리켜 안전.

| `-appendfsync` | fsync | On power loss |
| --- | --- | --- |
| `always` | 응답 전 매번 | 손실 없음 |
| `everysec` (default) | 약 1초마다 | 최대 약 1초 |
| `no` | OS에 맡김 | OS에 따라 |

<br />

## Benchmark

![Cache 6.5×, 63k+ ops/s up to 256 connections, 0 lost updates, 85% speed with AOF, 9.1× faster restart](assets/bench-summary.png)

<sub>All results · commit `c87d673` · Mac mini Linux VM (aarch64, 4 cores) · Python 3.10.12 · pipeline 없이 응답을 받은 뒤 다음 요청</sub>

### Cache On vs Off

![Cache on: median latency 86% faster, 6.5× throughput](assets/bench-cache.png)

주문 8만 건, 보고서 8개를 순환 요청. 캐시 적중 시 응답 시간 중앙값 86% 감소, 처리량 6.5배.

### Concurrent Connections

![Throughput by connection count and an INCR atomicity check](assets/bench-concurrency.png)

연결 16개부터 63.6k~69.1k ops/s로 포화되고 256개까지 떨어지지 않음. 늘어나는 건 대기 시간(p99)뿐. 256개 연결이 같은 키에 `INCR` 102,400번을 동시에 보내도 결과는 정확히 102,400.

### Durability Cost

![Memory only vs AOF everysec vs AOF always](assets/bench-durability.png)

기본값 `everysec`는 메모리 전용 처리량의 85% 유지. `always`는 쓰기마다 fsync라 11%.

### Restart After Rewrite

![AOF size and restart time before and after rewrite](assets/bench-recovery.png)

같은 키 20,000개를 10번씩 덮어쓴 SET 200,000건. rewrite 후 AOF 93% 감소, 재시작 9.1배 빠름.

```sh
python3 -m benchmarks.cache_api --requests 800 --rounds 6
python3 -m benchmarks.server
```

<sub>[cache result JSON](benchmarks/results/2026-11-18-cache-api.json) · [server result JSON](benchmarks/results/2026-11-18-server.json) · [cache code](benchmarks/cache_api.py) · [server code](benchmarks/server.py)</sub>

<details>
<summary>Raw numbers</summary>

<br />

| Cache | Requests | Median | p95 | Throughput |
| --- | ---: | ---: | ---: | ---: |
| off · SQLite | 4,800 | 1.514 ms | 1.645 ms | 646.2 req/s |
| on · Mini Redis hit | 4,800 | 0.216 ms | 0.274 ms | 4,192.3 req/s |

| Connections | ops/s | p50 | p99 |
| ---: | ---: | ---: | ---: |
| 1 | 18,645 | 0.051 ms | 0.067 ms |
| 4 | 52,205 | 0.073 ms | 0.088 ms |
| 16 | 63,552 | 0.248 ms | 0.291 ms |
| 64 | 64,870 | 0.985 ms | 1.074 ms |
| 256 | 69,135 | 3.703 ms | 4.099 ms |

| AOF policy | ops/s | p50 | p99 |
| --- | ---: | ---: | ---: |
| memory only | 50,564 | 1.263 ms | 1.395 ms |
| everysec | 43,102 | 1.474 ms | 1.608 ms |
| always | 5,513 | 11.573 ms | 16.101 ms |

| Rewrite | AOF size | Restart |
| --- | ---: | ---: |
| before | 13,289,008 B | 824 ms |
| after | 868,998 B | 90 ms |

<sub>median of 3 rounds × 3 s · 10,000 keys · 32 B values · SET/GET 50:50 (연결 수 테스트) · SET only, 64 connections (AOF 테스트)</sub>

</details>

<details>
<summary>Hash fields: dict vs custom HashTable</summary>

<br />

field 저장소를 `dict`(`9f67b74`)와 `HashTable`(`de7f275`)로 비교. 결과에 따라 `dict` 선택.

| Command (per op) | dict | HashTable |
| --- | ---: | ---: |
| `HSET` new field | **1.518 µs** | 2.842 µs |
| `HSET` update | **1.505 µs** | 2.096 µs |
| `HGET` hit | **1.104 µs** | 1.392 µs |

<sub>1,024 keys × 16 fields · Apple M4 · macOS 15.3.2 · CPython 3.9.6 · median of 8 rounds · [result JSON](benchmarks/results/hash-fields-m4-python39.json) · [code](benchmarks/hash_fields.py)</sub>

</details>

<br />

## FAQ

**Why not multi-thread?**<br />
명령 하나는 몇 µs라 병목은 CPU가 아니라 network I/O. I/O는 event loop로 겹치고 실행은 한 줄로 세워 lock과 race condition을 없앰.

**Trade-off?**<br />
CPU core 하나만 사용. `HashTable` resize나 RDB snapshot 복사 중에는 다른 연결도 잠시 대기.

**What if the process dies?**<br />
AOF 기록은 응답 전에 OS로 넘어가 있어 남음. 기기 전원이 꺼질 때만 `everysec` 기준 마지막 약 1초 손실 가능.

[Docs: 실행 흐름 · HashTable · 복구](docs/guide.md)
