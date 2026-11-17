"""바이트 키의 충돌을 체이닝으로 처리하고 삽입 순서를 보존하는 해시 테이블."""

from collections.abc import ItemsView, MutableMapping


class _Node:
    """버킷 체인과 삽입 순서 목록에서 함께 사용하는 항목."""

    __slots__ = ("key", "value", "hash_code", "bucket_next", "prev", "next")

    def __init__(self, key, value, hash_code):
        """키, 값, 해시값을 보관하고 연결 포인터를 초기화한다.

        Args:
            key: 항목의 바이트 키. 순서 목록의 경계 노드는 None을 사용한다.
            value: 키에 연결할 값.
            hash_code: 버킷 위치를 계산할 때 사용할 키의 해시값.
        """
        self.key = key
        self.value = value
        self.hash_code = hash_code
        self.bucket_next = None
        self.prev = self.next = self

    def unlink_order(self):
        """앞뒤 이웃을 연결해 이 노드를 삽입 순서 목록에서 분리한다."""
        self.prev.next = self.next
        self.next.prev = self.prev

    def insert_before(self, successor):
        """이 노드를 삽입 순서 목록의 지정한 이웃 바로 앞에 연결한다.

        Args:
            successor: 연결 후 이 노드의 다음에 올 노드.
        """
        self.prev = successor.prev
        self.next = successor
        self.prev.next = self
        successor.prev = self


class _Items(ItemsView):
    def __iter__(self):
        """버킷을 다시 조회하지 않고 삽입 순서대로 키와 값의 쌍을 순회한다."""
        return ((node.key, node.value) for node in self._mapping._nodes())


class HashTable(MutableMapping):
    """별도의 버킷 체인과 양방향 순서 목록으로 구성한 바이트 키 매핑."""

    _MIN_CAPACITY = 8

    def __init__(self, entries=()):
        """빈 버킷 8개를 만들고 전달받은 키와 값을 순서대로 저장한다.

        Args:
            entries: 초기값을 제공할 매핑 또는 바이트 키와 값의 쌍을 담은 반복 객체.
        """
        self._buckets = [None] * self._MIN_CAPACITY
        self._root = _Node(None, None, 0)
        self._size = 0
        self._version = 0
        self.update(entries)

    def __len__(self):
        """현재 저장된 서로 다른 키의 수를 반환한다."""
        return self._size

    def _locate(self, key):
        """키의 해시값과 같은 버킷 체인에서 일치하는 노드를 찾는다.

        Args:
            key: 조회할 바이트 키.

        Returns:
            해시값, 체인의 직전 노드, 일치한 노드의 튜플이다. 일치한 노드가 없으면 마지막 값은 None이다.
        """
        if not isinstance(key, bytes):
            raise TypeError("HashTable keys must be bytes")
        hash_code = hash(key)
        node = self._buckets[hash_code & (len(self._buckets) - 1)]
        previous = None
        while node is not None:
            if node.hash_code == hash_code and node.key == key:
                break
            previous, node = node, node.bucket_next
        return hash_code, previous, node

    def __getitem__(self, key):
        """버킷 체인에서 키를 찾아 연결된 값을 반환한다.

        Args:
            key: 조회할 바이트 키.

        Raises:
            KeyError: 키가 테이블에 없을 때 발생한다.
        """
        _, _, node = self._locate(key)
        if node is None:
            raise KeyError(key)
        return node.value

    def __setitem__(self, key, value):
        """기존 키는 값을 교체하고 새 키는 순서 목록의 끝에 삽입한다.

        Args:
            key: 저장할 바이트 키.
            value: 키에 연결할 값.
        """
        hash_code, _, node = self._locate(key)
        if node is not None:
            node.value = value
            return
        node = _Node(key, value, hash_code)
        capacity = len(self._buckets)
        if (self._size + 1) * 4 > capacity * 3:
            self._resize(capacity * 2)
        index = hash_code & (len(self._buckets) - 1)
        node.bucket_next = self._buckets[index]
        self._buckets[index] = node
        node.insert_before(self._root)
        self._size += 1
        self._version += 1

    def __delitem__(self, key):
        """지정한 키를 버킷 체인과 순서 목록 양쪽에서 삭제한다.

        Args:
            key: 삭제할 바이트 키.

        Raises:
            KeyError: 키가 테이블에 없을 때 발생한다.
        """
        hash_code, previous, node = self._locate(key)
        if node is None:
            raise KeyError(key)
        capacity = len(self._buckets)
        if previous is None:
            self._buckets[hash_code & (capacity - 1)] = node.bucket_next
        else:
            previous.bucket_next = node.bucket_next
        node.unlink_order()
        self._size -= 1
        self._version += 1
        if capacity > self._MIN_CAPACITY and self._size * 4 < capacity:
            self._resize(capacity // 2)

    def _resize(self, capacity):
        """기존 노드를 한 번씩 방문해 새 크기의 버킷 체인에 다시 연결한다.

        Args:
            capacity: 새 버킷 수. 8 이상의 2의 거듭제곱이다.
        """
        buckets = [None] * capacity
        node = self._root.next
        while node is not self._root:
            index = node.hash_code & (capacity - 1)
            node.bucket_next = buckets[index]
            buckets[index] = node
            node = node.next
        self._buckets = buckets

    def _nodes(self):
        """삽입 순서대로 노드를 순회하고 중간에 순서나 키 구성이 바뀌면 오류를 낸다."""
        version = self._version
        node = self._root.next
        while node is not self._root:
            yield node
            if version != self._version:
                raise RuntimeError("HashTable changed during iteration")
            node = node.next

    def __iter__(self):
        """첫 키부터 마지막 키까지 삽입 순서대로 순회한다."""
        return (node.key for node in self._nodes())

    def items(self):
        """현재 키와 값의 쌍을 삽입 순서대로 순회하는 뷰를 반환한다."""
        return _Items(self)

    def move_to_end(self, key):
        """값과 버킷 체인을 유지하면서 키를 삽입 순서 목록의 끝으로 옮긴다.

        Args:
            key: 마지막 위치로 옮길 바이트 키.

        Raises:
            KeyError: 키가 테이블에 없을 때 발생한다.
        """
        node = self._root.next
        # 만료 검사가 방금 꺼낸 첫 키는 충돌 체인을 검색할 필요가 없다.
        if node is self._root or node.key is not key:
            _, _, node = self._locate(key)
            if node is None:
                raise KeyError(key)
        if node.next is not self._root:
            node.unlink_order()
            node.insert_before(self._root)
            self._version += 1
