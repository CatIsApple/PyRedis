# mini-redis

GoRedis를 Python 표준 라이브러리로 이식한 Redis 학습용 서버입니다. 노드마다 하나의 `asyncio` 이벤트 루프가 여러 TCP 연결을 받고, 키 공간의 명령을 한 번에 하나씩 실행합니다. 외부 Python 패키지 없이 Python 3.9 이상에서 실행할 수 있습니다.

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
| 복제 | 단독 primary의 전체 스냅샷과 이후 변경을 읽기 전용 replica에 전달하는 `GOREDIS.SYNC` |
| 클러스터 | 해시 슬롯 16,384개, `CLUSTER KEYSLOT/SLOTS/INFO`, `MOVED`, `CROSSSLOT`, 샤드별 Raft 선출·복구 |

## 테스트

```sh
python3 -m unittest discover -v
```

`redis-server`가 PATH에 있으면 실제 Redis와 RESP2 응답, RDB·AOF 복구, 클러스터 슬롯을 대조하는 테스트도 실행합니다. 다른 경로의 실행 파일은 `REDIS_SERVER=/path/to/redis-server python3 -m unittest discover -v`로 지정합니다. `redis-cli`는 같은 디렉터리 또는 PATH에 둘 수 있습니다. Go 원본 실행 파일의 절대 경로를 `GOREDIS_SERVER`로 지정하면 Go와 Python 사이의 복제도 양방향으로 검사합니다. 테스트 서버는 임시 포트와 데이터 디렉터리를 사용합니다.

Redis 7.2.17과 8.2.11에서 각각 명령 응답 613개, 키 517개의 슬롯, RDB·AOF 양방향 복구와 `redis-cli -c` 리디렉션을 대조했습니다.

[복제·클러스터 실행과 구현 안내](docs/guide.md)
