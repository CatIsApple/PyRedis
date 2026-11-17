# 서버 실행과 복구

## 한 명령이 실행되는 경로

1. [TCP 서버](../miniredis/server.py)가 연결에서 [RESP2 프레임](../miniredis/resp.py) 하나를 읽습니다. 다음 바이트를 기다리는 동안 같은 이벤트 루프가 다른 연결을 처리합니다.
2. [명령 실행기](../miniredis/engine.py)가 인자를 검사하고 한 시점의 키 공간을 조회합니다. `SET session alice NX EX 30`은 키가 없거나 이미 만료됐을 때만 값을 쓰고 절대 만료 시각을 정합니다.
3. 영속화가 켜져 있으면 [저널](../miniredis/persist.py)에 `SET session alice PXAT <절대 시각(ms)>`을 먼저 추가합니다. 기록에 성공하면 같은 값과 만료 시각을 메모리에 반영합니다. 하나의 명령을 적용하는 도중 다른 클라이언트 명령이 끼어들지 않습니다.
4. 서버가 RESP2 응답을 보냅니다. `GET`은 저장한 바이트를 그대로 보내고, 만료된 키에는 null을 보냅니다. `TTL`은 없는 키에 -2, 만료 시각이 없는 키에 -1을 돌려줍니다.

두 터미널이 같은 주소에 접속하면 첫 번째 연결에서 쓴 값을 두 번째 연결에서 읽을 수 있습니다.

```sh
redis-cli -p 6379 SET shared hello  # 첫 번째 터미널에서 OK
redis-cli -p 6379 GET shared        # 두 번째 터미널에서 "hello"
```

## 해시 테이블로 키와 필드 찾기

바깥 `HashTable`인 `Engine._data`는 바이트 키를 `Entry`에 연결합니다. 문자열 키의 `Entry.value`에는 값의 바이트열이 들어갑니다. 해시 키의 `Entry.value`에는 바이트열 필드 이름을 바이트열 값에 연결하는 Python 내장 `dict`가 들어갑니다. [명령 실행기와 Entry](../miniredis/engine.py)

- 빈 저장소에서 `HSET user:42 city Seoul`을 실행하면 `_hset`이 바깥 테이블에 `b'user:42'`의 항목을 만들고, 그 항목의 필드 사전에 `b'city' → b'Seoul'`을 저장합니다. 새 필드가 하나이므로 응답은 `1`입니다. 이어서 `HSET user:42 city Busan`을 실행하면 같은 필드의 값만 바뀌고 응답은 `0`입니다.
- `HGET user:42 city`는 두 테이블을 한 번씩 조회합니다. `_hget`이 `_lookup`으로 바깥 테이블에서 `b'user:42'`를 찾고, 해당 항목의 `value.get(b'city')`로 필드 사전을 조회해 `b'Busan'`을 반환합니다.

`HashTable`은 처음에 버킷 8개를 준비합니다. Python의 `hash(key)`로 바이트 키의 해시값을 얻고, `hash(key) & (버킷 수 - 1)`로 버킷 번호를 계산합니다. 버킷 수가 8이면 해시값의 하위 3비트가 `101`인 키는 모두 5번 버킷으로 갑니다. 같은 버킷에 여러 키가 들어오면 항목들이 한 체인에 연결됩니다. `_locate`는 `bucket_next`를 따라가며 저장된 해시값과 키의 바이트 내용이 모두 일치하는지 확인합니다. 키 길이가 일정할 때 조회 비용은 평균 O(1)이며, n개의 키가 한 버킷에 몰리면 최악 O(n)입니다. [HashTable 구현](../miniredis/hashtable.py)

바깥 키를 새로 추가해 키 수가 버킷 수의 3/4을 넘으면 버킷 수를 두 배로 늘립니다. 예를 들어 8개 버킷에 일곱 번째 키를 넣으면 16개 버킷을 사용합니다. 삭제 후 항목 수가 버킷 수의 1/4보다 작아지면 버킷 수를 절반으로 줄이며 최소 8개를 유지합니다. `_resize`는 각 항목에 저장된 해시값에 새 마스크를 적용해 버킷을 다시 정합니다. 이 재배치는 O(n)이고, 단일 이벤트 루프에서 끝날 때까지 다른 클라이언트의 명령도 기다립니다.

바깥 `HashTable`의 `prev`와 `next`는 버킷 체인과 별개로 키의 순회 순서를 유지합니다. 바깥 테이블에서 `Engine.purge`는 앞에서부터 기본 20개까지 검사하고 살아 있는 키를 `move_to_end`로 뒤에 보냅니다. TTL은 바깥 `Entry.expires`에 키별로 하나만 저장합니다. `HSET`으로 필드를 바꿔도 기한은 유지되고, 키가 만료되거나 `DEL user:42`를 실행하면 모든 필드가 함께 사라집니다.

## 함수 문서 읽기

함수의 첫 줄은 역할을 설명합니다. `Args`에는 호출자가 전달할 값과 단위를 적고, 반환값·생성값·예외에 설명이 필요하면 `Returns`, `Yields`, `Raises`에 적습니다. 예를 들어 `Engine.execute`의 `args`는 명령 이름부터 시작하는 바이트열 목록이고, `now`는 만료 판정에 쓰는 유닉스 시각(밀리초)입니다. 저장소 루트에서 다음 명령으로 해당 함수의 문서를 읽을 수 있습니다.

```sh
python3 -m pydoc miniredis.engine.Engine.execute
```

## RDB와 AOF

기본 경로의 `appendonlydir/appendonly.aof.manifest`는 읽어야 할 RDB 기준본과 증분 AOF의 순서를 담습니다. 복구할 때는 기준본과 나열된 증분 파일을 모두 적용한 뒤 현재 시각으로 키를 만료시킵니다. AOF에 만료 시각 연장이 들어 있어도 복구됩니다. 마지막 증분 파일의 끝에 쓰다 만 명령이 있으면 마지막 완전한 기록까지 복구합니다.

`Engine.snapshot`은 살아 있는 키의 만료 시각과 값을 복사합니다. 해시 키는 필드 사전을 따로 복사하므로 스냅샷을 만든 뒤에 실행된 `HSET`은 그 사본에 영향을 주지 않습니다. RDB에는 필드 이름과 값이, 증분 AOF에는 쓰기 명령이 저장됩니다. 필드를 `HashTable`에 담던 버전이 남긴 파일도 같은 형식이므로 이전 RDB를 읽거나 AOF의 `HSET`을 재생하면 필드가 현재 서버의 `dict`에 복구됩니다.

```sh
python3 -m miniredis -addr 127.0.0.1:6381 -aof ./example.aof -appendfsync always
# 다른 터미널
redis-cli -p 6381 SET visits 1
redis-cli -p 6381 BGREWRITEAOF
redis-cli -p 6381 INCR visits
```

서버를 종료한 뒤 같은 옵션으로 다시 시작하면 `redis-cli -p 6381 GET visits`가 `2`를 돌려줍니다. 이 예제의 파일은 `example.aof.d/`에 생깁니다. 예전 단일 AOF는 최초 실행 시 읽어 다중 파일 구성으로 옮깁니다.

`always`는 쓰기 응답 전에 디스크 동기화를 마칩니다. 기본값 `everysec`은 약 1초마다 동기화하고, `no`는 일상적인 동기화 시점을 운영체제에 맡깁니다. 자동 재작성은 AOF 파일들의 전체 크기가 64 MiB를 넘고 기준 크기의 두 배 이상일 때 시작합니다. 재작성을 시작하면 새 증분 파일로 전환하고 스냅샷을 나누어 기록합니다. 스냅샷 복사와 파일 동기화 중에는 이벤트 루프가 잠시 멈출 수 있습니다.

## Redis와 비교한 범위

[Redis 대조 테스트](../tests/test_redis_compat.py)는 README의 지원 명령을 Python 서버와 실제 Redis에 같은 순서로 보냅니다. RESP2 응답의 종류와 값, 오류, 바이너리 키·값, 파이프라인의 응답 순서를 비교합니다. DB 0의 문자열과 해시, 키별 만료 시각은 양쪽 서버가 서로의 RDB와 다중 파일 AOF를 읽어 복구하는지도 확인합니다.

GoRedis에서 이어받은 동작 차이도 있습니다. 메모리 전용 모드에서 `BGREWRITEAOF`를 호출하면 오류를 반환하고, 미지원 명령에는 `ERR unknown command`로 답합니다. 지원 프로토콜은 RESP2이며, `SET`의 공개 옵션은 `EX`와 `NX`입니다. 복구 예시에 나오는 `PXAT`는 AOF에서 절대 만료 시각을 재생할 때 사용합니다.
