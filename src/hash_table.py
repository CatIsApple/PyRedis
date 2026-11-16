"""직접 만든 해시 테이블: 버킷 배열 + 체이닝. dict를 쓰지 않는다 (Map/Object 금지 조건의 파이썬 버전).

역할: 키를 칸 번호(fnv1a(key) % 버킷 수)로 바꿔 값을 찾는다. 같은 칸의 충돌은 [key, value] 목록으로 잇는다.
주요 함수: fnv1a(해시), HashTable.set/get/has/delete/items, _resize(적재율 0.75 초과 시 2배 + rehash).
"""

FNV_OFFSET = 0x811C9DC5
FNV_PRIME = 0x01000193
MASK32 = 0xFFFFFFFF
MAX_LOAD_FACTOR = 0.75


def fnv1a(text):
    """FNV-1a 32비트 해시. 문자열의 UTF-8 바이트를 섞어 0 ~ 2^32-1 정수를 만든다."""
    h = FNV_OFFSET
    for byte in text.encode("utf-8"):
        h ^= byte
        h = (h * FNV_PRIME) & MASK32  # 32비트로 잘라서 곱한다 (파이썬 정수는 무한 정밀도라 직접 잘라야 한다)
    return h


def _check_key(key):
    if not isinstance(key, str) or key == "":
        raise TypeError("키는 비어 있지 않은 문자열이어야 합니다")


class HashTable:
    """같은 칸에 걸린 항목은 [key, value] 목록(체인)에 둔다. 적재율이 0.75를 넘으면 버킷을 2배로 늘린다."""

    def __init__(self, capacity=8, hash_fn=fnv1a):
        # 테스트에서 충돌을 강제할 수 있게 초기 용량과 해시 함수를 주입받는다
        if isinstance(capacity, bool) or not isinstance(capacity, int) or capacity < 1:
            raise ValueError("capacity는 1 이상의 정수여야 합니다")
        if not callable(hash_fn):
            raise TypeError("hash_fn은 함수여야 합니다")
        self._buckets = [None] * capacity
        self._size = 0
        self._hash = hash_fn

    @property
    def size(self):
        return self._size

    @property
    def capacity(self):
        return len(self._buckets)

    def set(self, key, value):#이놈이 중점임 ㅇㅇ 이게 해시테이블에 값을 넣는거임
        #stroe.py에서 set을 호출하면 여기로 들어옴. key는 문자열, value는 _Entry임. _Entry는 text와 expires_at을 가지고 있음
        _check_key(key)
        chain = self._chain_for(key, create=True)
        for entry in chain:
            if entry[0] == key:
                entry[1] = value  # 같은 키는 덮어쓰기: 개수는 그대로
                return
        chain.append([key, value])
        self._size += 1
        if self._size / len(self._buckets) > MAX_LOAD_FACTOR:
            self._resize(len(self._buckets) * 2)

    def get(self, key):# 이건 해시테이블에서 값을 가져오는거임
        """없으면 None. 저장소는 None을 값으로 쓰지 않으므로 이 구분으로 충분하다."""
        _check_key(key)
        chain = self._chain_for(key, create=False)
        if chain:
            for entry in chain:
                if entry[0] == key:
                    return entry[1]
        return None

    def has(self, key):
        _check_key(key)
        chain = self._chain_for(key, create=False)
        return bool(chain) and any(entry[0] == key for entry in chain)

    def delete(self, key):
        _check_key(key)
        chain = self._chain_for(key, create=False)
        if not chain:
            return False
        for i, entry in enumerate(chain):
            if entry[0] == key:
                del chain[i]
                self._size -= 1
                return True
        return False

    def items(self):
        """(key, value) 목록의 복사본. 순회 중에 지워도 안전하다."""
        result = []
        for chain in self._buckets:
            if chain:
                result.extend((entry[0], entry[1]) for entry in chain)
        return result

    def _index(self, key, capacity):
        return (self._hash(key) & MASK32) % capacity

    def _chain_for(self, key, create):#이거의 역할은 해시테이블에서 특정 키에 해당하는 체인을 가져오는거임. 만약 create가 True이면 체인이 없으면 새로 만들어줌
        i = self._index(key, len(self._buckets))
        if self._buckets[i] is None and create:
            self._buckets[i] = []
        return self._buckets[i]

    def _resize(self, new_capacity):
        # 버킷 수가 바뀌면 "해시 % 버킷 수" 결과가 달라지므로 모든 항목을 새 자리에 다시 넣는다 (rehash)
        old = self._buckets
        self._buckets = [None] * new_capacity
        for chain in old:
            if not chain:
                continue
            for entry in chain:
                i = self._index(entry[0], new_capacity)
                if self._buckets[i] is None:
                    self._buckets[i] = []
                self._buckets[i].append(entry)
