# 서버 실행과 복구

## 한 명령이 실행되는 경로

1. [TCP 서버](../miniredis/server.py)가 연결에서 [RESP2 프레임](../miniredis/resp.py) 하나를 읽습니다. 다음 바이트를 기다리는 동안 같은 이벤트 루프가 다른 연결을 처리합니다.
2. [명령 실행기](../miniredis/engine.py)가 인자를 검사하고 한 시점의 키 공간을 조회합니다. `SET session alice NX EX 30`은 키가 비어 있을 때만 값을 쓰고 절대 만료 시각을 정합니다.
3. 영속화가 켜져 있으면 [저널](../miniredis/persist.py)에 `SET session alice PXAT <절대 시각(ms)>`을 먼저 추가합니다. 기록에 성공하면 메모리에 반영하고 복제 구독자에게 변경을 전달합니다. 하나의 명령을 적용하는 도중 다른 클라이언트 명령이 끼어들지 않습니다.
4. 서버가 RESP2 응답을 보냅니다. `GET`은 저장한 바이트를 그대로 보내고, 만료된 키에는 null을 보냅니다. `TTL`은 없는 키에 -2, 만료 시각이 없는 키에 -1을 돌려줍니다.

## 함수 문서 읽기

함수의 첫 줄은 역할을 설명합니다. `Args`에는 호출자가 전달할 값과 단위를 적고, 반환값·생성값·예외에 설명이 필요하면 `Returns`, `Yields`, `Raises`에 적습니다. 예를 들어 `Engine.execute`의 `args`는 명령 이름부터 시작하는 바이트열 목록이고, `now`는 만료 판정에 쓰는 유닉스 시각(밀리초)입니다. 저장소 루트에서 다음 명령으로 해당 함수의 문서를 읽을 수 있습니다.

```sh
python3 -m pydoc miniredis.engine.Engine.execute
```

## RDB와 AOF

기본 경로의 `appendonlydir/appendonly.aof.manifest`는 읽어야 할 RDB 기준본과 증분 AOF의 순서를 담습니다. 복구할 때는 두 파일을 모두 적용한 뒤 현재 시각으로 키를 만료시킵니다. AOF에 만료 시각 연장이 들어 있어도 복구됩니다. 마지막 증분 파일의 끝에 쓰다 만 명령이 있으면 마지막 완전한 기록까지 복구합니다.

```sh
python3 -m miniredis -addr 127.0.0.1:6381 -aof ./example.aof -appendfsync always
# 다른 터미널
redis-cli -p 6381 SET visits 1
redis-cli -p 6381 BGREWRITEAOF
redis-cli -p 6381 INCR visits
```

서버를 종료한 뒤 같은 옵션으로 다시 시작하면 `redis-cli -p 6381 GET visits`가 `2`를 돌려줍니다. 이 예제의 파일은 `example.aof.d/`에 생깁니다. 예전 단일 AOF는 최초 실행 시 읽어 다중 파일 구성으로 옮깁니다.

`always`는 쓰기 응답 전에 디스크 동기화를 마칩니다. 기본값 `everysec`은 약 1초마다 동기화하고, `no`는 일상적인 동기화 시점을 운영체제에 맡깁니다. 자동 재작성은 파일 크기가 64 MiB를 넘고 기준 크기의 두 배 이상일 때 시작합니다. 재작성을 시작하면 새 증분 파일로 전환하고 스냅샷을 나누어 기록합니다. 스냅샷 복사와 파일 동기화 중에는 이벤트 루프가 잠시 멈출 수 있습니다.

## 단독 primary와 replica

다음 명령을 각각 별도 터미널에서 실행합니다.

```sh
python3 -m miniredis -addr 127.0.0.1:6379
python3 -m miniredis -addr 127.0.0.1:6380 -replicaof 127.0.0.1:6379
```

```sh
redis-cli -p 6379 HSET user:42 city Seoul
redis-cli -p 6380 HGET user:42 city       # 복제 반영 후 Seoul
redis-cli -p 6380 HSET user:42 city Busan # READONLY
```

replica는 첫 스냅샷 수신이 끝나면 키 공간을 한 번에 교체합니다. 그 전에는 조회에 `LOADING`을 반환합니다. 이후 연결이 끊기면 마지막으로 받은 데이터를 제공하면서 재접속합니다. 단독 복제는 비동기이며 장애가 난 primary를 자동으로 교체하는 기능은 클러스터 모드에 있습니다. 복제 연결은 [GoRedis 전용 프로토콜](../miniredis/replica.py)을 사용해 Go 원본과 Python 노드를 조합할 수 있습니다.

## 로컬 클러스터

[예제 구성](../examples/cluster.json)은 샤드 3개에 투표자 3개씩을 배치합니다. 클라이언트 포트는 7100–7108, Raft 포트는 17100–17108입니다. 저장소 루트의 Bash 또는 Zsh에서 다음 블록을 실행하면 9개 노드가 시작됩니다. 포트는 비어 있어야 합니다.

```bash
(
  run_dir=$(mktemp -d)
  echo "노드 데이터와 로그: $run_dir"
  pids=()
  trap 'kill "${pids[@]}" 2>/dev/null; wait' EXIT
  trap 'exit' INT TERM
  for port in {7100..7108}; do
    python3 -m miniredis -addr "127.0.0.1:$port" \
      -cluster-config examples/cluster.json -cluster-dir "$run_dir/$port" \
      >"$run_dir/$port.log" 2>&1 &
    pids+=("$!")
  done
  wait
)
```

다른 터미널에서 `CLUSTER INFO`가 `cluster_state:ok`를 표시한 뒤 실행합니다. 첫 터미널에서 Ctrl+C를 누르면 노드가 종료됩니다.

```sh
redis-cli -p 7100 CLUSTER INFO
redis-cli -p 7100 CLUSTER SLOTS
redis-cli -p 7100 CLUSTER KEYSLOT '{user:42}:name'                # 15880
redis-cli -c -p 7100 MSET '{user:42}:name' alice '{user:42}:city' Seoul
redis-cli -c -p 7100 MGET '{user:42}:city' '{user:42}:name'
```

같은 해시 태그를 붙인 키는 같은 슬롯을 사용합니다. 다른 슬롯의 키를 묶어 요청하면 `CROSSSLOT` 오류가 발생합니다. 담당 primary가 다른 노드에 있으면 `MOVED`를 반환하고 `redis-cli -c`가 해당 주소로 따라갑니다. 각 샤드는 읽기와 쓰기 모두 Raft 과반수 커밋 후 응답합니다. primary가 중단되면 남은 과반수가 새 primary를 선출합니다.

클러스터는 Python 노드끼리 구성합니다. [Raft](../miniredis/raft.py)는 노드별 SQLite 파일에 임기·투표·로그·스냅샷을 기록하며, Go 원본의 HashiCorp Raft 통신 및 BoltDB 파일과 형식이 다릅니다. 개별 클러스터 명령의 인코딩 크기는 SQLite 로그 레코드 한도(일반 빌드에서 약 1 GB) 이하여야 합니다. 같은 노드를 다시 띄울 때는 같은 구성·주소·데이터 디렉터리를 사용합니다. 클라이언트와 노드 간 포트는 인증이나 암호화를 제공하지 않으므로 예제는 루프백에서 실행합니다.

## Redis와 비교한 범위

RESP2에서 README에 열거한 명령의 응답과 오류, 바이너리 값, 파이프라인을 대조합니다. 원본이 정한 두 가지 동작 차이가 있습니다. 메모리 전용 모드의 `BGREWRITEAOF`는 오류를 반환하고, 미지원 명령은 `ERR unknown command`로 답합니다. Redis의 전체 명령 집합, RESP3, 공식 복제 프로토콜(PSYNC)은 이 프로젝트의 지원 범위 밖입니다.

실제 Redis가 작성한 RDB·다중 파일 AOF를 Python이 복구하고, 반대로 Python이 작성한 파일도 Redis에서 읽는지 검사합니다. 이 밖에 별도 Python 프로세스로 클러스터 장애, 재선출, 과반수 상실, 재시작을 재현합니다. [Redis 대조 테스트](../tests/test_redis_compat.py) · [클러스터 통합 테스트](../tests/test_cluster.py)
