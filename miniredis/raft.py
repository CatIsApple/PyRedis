"""Single-event-loop Raft with a fixed voter set per shard.

All protocol state transitions run without yielding. Only peer transport and
waiting for a committed command suspend a coroutine.
"""

import asyncio
import logging
import random
import sqlite3
import time
from tempfile import SpooledTemporaryFile

from . import resp
from .engine import Engine
from .raft_storage import RaftStorage, SNAPSHOT_CHUNK


class NotLeader(Exception):
    """The command was never submitted."""


class LeadershipLost(Exception):
    """The command was submitted and its outcome may be unknown."""


HEARTBEAT = 0.10
ELECTION_WINDOW = (0.60, 1.0)
RPC_TIMEOUT = 0.45
QUORUM_TIMEOUT = 1.10
SNAPSHOT_INTERVAL = 256
BATCH_SIZE = 32


def address_parts(endpoint):
    """노드 주소를 호스트와 포트 번호로 나눈다.

    Args:
        endpoint: 호스트와 포트를 콜론으로 구분한 주소. IPv6 호스트는 대괄호로 감싼다.
    """
    host, port = endpoint.rsplit(":", 1)
    return host.strip("[]"), int(port)


def unpack(data):
    """Raft 프레임의 본문을 RESP 값 하나로 디코딩한다.

    Args:
        data: 길이 접두어를 제외한 프레임 바이트.

    Raises:
        ValueError: RESP 값 뒤에 바이트가 남아 있거나 형식이 잘못된 경우.
    """
    value, end = resp.decode(data)
    if end != len(data):
        raise ValueError("trailing data in Raft frame")
    return value


async def read_frame(reader):
    """길이 접두어와 그 길이만큼의 본문을 읽어 Raft 메시지를 반환한다.

    Args:
        reader: 피어 연결의 비동기 입력 스트림.
    """
    size = int.from_bytes(await reader.readexactly(4), "big")
    if size == 0 or size > 1 << 30:
        raise ValueError("invalid Raft frame size")
    return unpack(await reader.readexactly(size))


async def write_frame(writer, value):
    """RESP 값을 길이 접두어가 있는 Raft 프레임으로 보낸다.

    Args:
        writer: 피어 연결의 비동기 출력 스트림.
        value: 직렬화할 요청이나 응답.
    """
    data = resp.encode(value)
    writer.write(len(data).to_bytes(4, "big"))
    writer.write(data)
    await writer.drain()


def counter(value):
    """값이 0 이상의 정수인지 검사하며 불리언 값은 제외한다.

    Args:
        value: 임기나 인덱스 필드에서 읽은 값.
    """
    return type(value) is int and value >= 0


def wire_blob(data):
    """RESP 벌크 문자열의 크기 제한을 넘는 데이터를 여러 조각으로 나눈다.

    Args:
        data: 전송할 바이너리 데이터.

    Returns:
        크기 제한 이내면 원본 바이트, 초과하면 순서대로 나눈 바이트 목록.
    """
    # A legal client request can contain multiple full-size RESP bulk values.
    if len(data) <= resp.MAX_BULK:
        return data
    return [data[i:i + resp.MAX_BULK] for i in range(0, len(data), resp.MAX_BULK)]


def unwrap_blob(value):
    """단일 벌크 문자열이나 조각 목록에서 원래 바이트를 복원한다.

    Args:
        value: 피어가 보낸 바이너리 데이터 또는 조각 목록.
    """
    if type(value) is bytes:
        return value
    if isinstance(value, list) and value and all(type(chunk) is bytes for chunk in value):
        return b"".join(value)
    raise ValueError("invalid Raft payload")


def validate_request(request):
    """Raft 요청의 필드를 검증하고 로그 항목의 분할 데이터를 하나로 합친다.

    Args:
        request: 메시지에서 발신지와 목적지를 제외한 RPC 필드 목록.

    Raises:
        ValueError: RPC 종류, 임기, 필드 형식 또는 조각 크기가 유효하지 않은 경우.
    """
    if not isinstance(request, list) or len(request) < 2:
        raise ValueError("invalid Raft request")
    kind, term, *fields = request
    if not counter(term) or term == 0:
        raise ValueError("invalid Raft term")
    if kind == b"vote" and len(fields) == 2 and all(map(counter, fields)):
        return
    if kind == b"snapshot" and len(fields) == 6 and all(map(counter, fields[:5])):
        if fields[3] < fields[4] and len(unwrap_blob(fields[5])) <= SNAPSHOT_CHUNK:
            return
    if kind == b"append" and len(fields) == 4 and all(map(counter, fields[:3])):
        rows = fields[3]
        if isinstance(rows, list) and len(rows) <= BATCH_SIZE and all(
                isinstance(row, list) and len(row) == 2 and counter(row[0])
                and 0 < row[0] <= term for row in rows):
            for row in rows:
                row[1] = unwrap_blob(row[1])
            return
    raise ValueError("invalid Raft RPC fields")


def command_parts(command):
    """로그 항목에서 실행 시각과 명령 인자를 읽고 형식을 검증한다.

    Args:
        command: 직렬화된 명령 바이트.

    Returns:
        밀리초 단위 시각과 바이너리 명령 인자 목록을 담은 두 원소 배열.
    """
    value = unpack(command)
    if not (isinstance(value, list) and len(value) == 2 and counter(value[0])
            and isinstance(value[1], list) and value[1]
            and all(type(arg) is bytes for arg in value[1])):
        raise ValueError("invalid Raft log command")
    return value


class Raft:
    @classmethod
    async def open(cls, local, peers, directory, topology_id):
        """샤드의 영속 상태를 복구하고 피어 통신과 선거 타이머를 시작한다.

        Args:
            local: 이 노드의 식별자로 쓰는 클라이언트 주소.
            peers: 같은 샤드의 모든 클라이언트 주소를 Raft 통신 주소에 대응시킨 사전.
            directory: 이 노드의 Raft 저장소를 열 디렉터리.
            topology_id: 설정에 정의된 클러스터 배치를 식별하는 문자열.
        """
        disk = RaftStorage(directory, local, topology_id)
        try:
            group = cls(local, peers, topology_id, disk)
            host, port = address_parts(peers[local])
            group.server = await asyncio.start_server(group._accept, host, port)
        except BaseException:
            disk.close()
            raise
        group.tasks = [asyncio.create_task(group._tick())]
        group.tasks.extend(asyncio.create_task(group._peer(peer)) for peer in group.wakes)
        for task in group.tasks:
            task.add_done_callback(group._task_done)
        return group

    def __init__(self, local, peers, scope, disk):
        """영속 상태에서 키 공간과 마지막 커밋 위치를 복구하고 팔로어로 초기화한다.

        Args:
            local: 이 노드의 클라이언트 주소.
            peers: 샤드의 클라이언트 주소와 Raft 통신 주소의 대응표. 첫 항목이 초기 선거를 시작한다.
            scope: 피어 메시지에 포함할 클러스터 배치 식별자.
            disk: 이 노드가 독점적으로 연 Raft 저장소.
        """
        self.local = local
        self.peers = dict(peers)
        self.scope = scope.encode()
        self.quorum = len(peers) // 2 + 1
        self.disk = disk
        self.term = disk.get("term", 0)
        self.vote = disk.get("vote")
        self.commit = disk.get("commit", 0)
        self.base = disk.get("base", 0)
        self.last = disk.last_index()
        self.machine = Engine()
        if self.base:
            from .rdb import read_snapshot
            with disk.snapshot_stream() as stream:
                self.machine.data = read_snapshot(stream)
        self.clock = disk.get("clock", 0)
        self.applied = self.base
        self.installing = None
        self.pending = {}
        self._apply()
        # Only the configured bootstrap node campaigns in a brand new shard.
        self.bootstrap = local == next(iter(peers)) or bool(disk.get("joined"))
        self.role = "follower"
        self.leader = None
        self.votes = set()
        self.wakes = {addr: asyncio.Event() for addr in peers if addr != local}
        self.incoming = set()
        self.closed = False
        self._reset_timer()

    def _task_done(self, task):
        """선거나 피어 통신 태스크가 예외로 종료되면 노드를 중단한다.

        Args:
            task: 실행이 끝난 백그라운드 태스크.
        """
        if not task.cancelled() and task.exception() is not None:
            self._fail(task.exception())

    def _fail(self, exc):
        """복구할 수 없는 오류를 기록하고 투표와 요청 수락을 중단한다.

        Args:
            exc: 노드가 안전하게 동작하기 어려워진 원인 예외.
        """
        # On a disk or FSM failure this voter cannot safely attest to the log.
        if not self.closed:
            logging.error("Raft voter %s stopped: %s", self.local, exc)
            self.closed = True
            self.role, self.leader = "failed", None
            self._reject_pending()
            self.server.close()
            self._wake()

    def _reset_timer(self):
        """다음 선거의 마감 시각을 무작위로 정한다."""
        self.deadline = time.monotonic() + random.uniform(*ELECTION_WINDOW)

    def _wake(self):
        """대기 중인 모든 피어 통신 태스크를 깨운다."""
        for event in self.wakes.values():
            event.set()

    def _reject_pending(self):
        """결과를 기다리는 명령에 리더십 상실을 알리고 대기 목록을 비운다."""
        for future in self.pending.values():
            if not future.done():
                future.set_exception(LeadershipLost())
        self.pending.clear()

    def _step_down(self, term):
        """팔로어로 전환하고 더 높은 임기를 관측했다면 새 임기와 빈 투표 기록을 저장한다.

        Args:
            term: 팔로어 전환 시 적용할 임기.
        """
        if term > self.term:
            with self.disk.db:
                self.disk.set("term", term)
                self.disk.set("vote", None)
            self.term, self.vote = term, None
        self.role, self.leader = "follower", None
        self._reject_pending()
        self._reset_timer()

    def _join(self):
        """이 노드가 첫 선거에 참여했거나 리더와 접촉했음을 기록해 이후 선거를 시작할 수 있게 한다."""
        if not self.bootstrap:
            with self.disk.db:
                self.disk.set("joined", 1)
            self.bootstrap = True

    def _elect(self):
        """새 임기를 시작하고 자신에게 투표한 뒤 다른 투표자에게 표를 요청한다."""
        self._step_down(self.term + 1)
        with self.disk.db:
            self.disk.set("vote", self.local)
        self.vote = self.local
        self.votes = {self.local}
        self.role = "candidate"
        if len(self.votes) >= self.quorum:
            self._promote()
        self._wake()

    def _promote(self):
        """리더로 전환하고 현재 임기의 빈 로그 항목을 추가해 앞선 로그를 커밋할 준비를 한다."""
        self.role, self.leader = "leader", self.local
        self.next_index = {p: self.last + 1 for p in self.wakes}
        self.matched = {p: 0 for p in self.wakes}
        self.sent_parts = {}
        self.contact = {}
        self.promoted_at = time.monotonic()
        # A committed entry from this term also commits the entire preceding log.
        self._append_local(b"")
        self._advance()
        self._wake()

    def _append_local(self, command):
        """현재 임기의 항목 한 개를 로컬 로그 끝에 저장한다.

        Args:
            command: 직렬화된 명령 바이트. 빈 바이트열은 상태를 바꾸지 않는 항목이다.
        """
        with self.disk.db:
            self.disk.append(self.last + 1, [(self.term, command)])
        self.last += 1

    async def execute(self, args):
        """리더의 로그에 명령을 추가하고 과반수 커밋과 로컬 적용이 끝날 때까지 기다린다.

        Args:
            args: 명령 이름부터 순서대로 담은 바이너리 인자 목록.

        Returns:
            명령의 RESP 응답. 저장 한도를 넘으면 기록 전에 오류 응답을 반환한다.

        Raises:
            NotLeader: 명령을 받았을 때 이 노드가 활성 리더가 아닌 경우.
            LeadershipLost: 저장 오류, 리더 교체 또는 시간 초과로 결과를 확정할 수 없는 경우.
        """
        if self.role != "leader" or self.closed:
            raise NotLeader()
        try:
            record = resp.encode([time.time_ns() // 1_000_000, args])
            if len(record) > self.disk.max_record:
                return resp.Error(b"ERR command exceeds shard log record limit")
            self._append_local(record)
        except sqlite3.Error as exc:
            self._fail(exc)
            raise LeadershipLost() from exc
        index = self.last
        result = asyncio.get_running_loop().create_future()
        self.pending[index] = result
        try:
            self._advance()
            self._wake()
            return await asyncio.wait_for(result, 3.0)
        except sqlite3.Error as exc:
            self._fail(exc)
            raise LeadershipLost() from exc
        except asyncio.TimeoutError as exc:
            raise LeadershipLost() from exc
        finally:
            self.pending.pop(index, None)

    async def _tick(self):
        """선거 마감 시각과 피어의 최근 응답을 확인해 선거를 시작하거나 리더에서 물러난다."""
        while not self.closed:
            await asyncio.sleep(HEARTBEAT / 2)
            now = time.monotonic()
            if self.role == "leader":
                connected = 1 + sum(now - t <= QUORUM_TIMEOUT for t in self.contact.values())
                if now - self.promoted_at > QUORUM_TIMEOUT and connected < self.quorum:
                    self._step_down(self.term)
            elif self.bootstrap and now >= self.deadline:
                self._elect()

    def _advance(self):
        """과반수가 복제한 현재 임기의 항목까지 커밋 위치를 전진시킨다."""
        # Only entries of the leader's current term can establish a new commit.
        indexes = sorted([self.last, *self.matched.values()], reverse=True)
        index = indexes[self.quorum - 1]
        if index > self.commit and self.disk.term_at(index) == self.term:
            self._commit_to(index)
            self._wake()

    def _commit_to(self, index):
        """커밋 위치를 저장하고 로그를 적용한 뒤 필요한 경우 스냅샷을 만든다.

        Args:
            index: 새로 커밋할 마지막 로그 인덱스.
        """
        with self.disk.db:
            self.disk.set("commit", index)
        self.commit = index
        self._apply()
        if self.applied - self.base >= SNAPSHOT_INTERVAL:
            self._checkpoint()

    def _apply(self):
        """커밋된 로그를 순서대로 적용하며 명령의 논리 시각이 뒤로 가지 않도록 한다."""
        while self.applied < self.commit:
            entries = self.disk.entries(self.applied + 1, min(BATCH_SIZE, self.commit - self.applied))
            if not entries:
                raise ValueError("Raft log is missing committed entries")
            for _, command in entries:
                reply = None
                if command:
                    timestamp, args = command_parts(command)
                    self.clock = max(self.clock, timestamp)
                    reply = self.machine.execute(args, now=self.clock)
                self.applied += 1
                future = self.pending.get(self.applied)
                if future is not None and not future.done():
                    future.set_result(reply)

    def _checkpoint(self):
        """마지막 적용 시각에 살아 있는 키를 스냅샷으로 저장하고 이전 로그를 정리한다."""
        from .rdb import write_snapshot
        live = self.machine.snapshot(self.clock)
        with SpooledTemporaryFile(max_size=SNAPSHOT_CHUNK) as stream:
            write_snapshot(stream, live)
            with self.disk.db:
                self.disk.checkpoint(self.applied, self.disk.term_at(self.applied),
                                     self.clock, stream)
        self.machine.data = live
        self.base = self.applied

    def _request(self, peer):
        """현재 역할과 피어의 복제 위치에 따라 다음 RPC를 구성한다.

        Args:
            peer: 요청을 보낼 샤드 구성원의 클라이언트 주소.

        Returns:
            투표, 로그 추가 또는 스냅샷 조각 요청. 전송할 요청이 없으면 None.
        """
        if self.role == "candidate":
            if peer in self.votes:
                return None
            return [b"vote", self.term, self.last, self.disk.term_at(self.last)]
        if self.role != "leader":
            return None
        start = self.next_index[peer]
        if start <= self.base:
            base, part = self.sent_parts.get(peer, (self.base, 0))
            if base != self.base:
                part = 0
            return [b"snapshot", self.term, self.base, self.disk.term_at(self.base),
                    self.disk.get("clock"), part, self.disk.get("snapshot_parts"),
                    wire_blob(self.disk.snapshot_part(part))]
        rows = [[term, wire_blob(cmd)] for term, cmd in self.disk.batch(start, BATCH_SIZE, 4 << 20)]
        return [b"append", self.term, start - 1, self.disk.term_at(start - 1), self.commit, rows]

    async def _peer(self, peer):
        """한 피어와의 연결에서 투표 요청과 로그 또는 스냅샷 복제를 처리한다.

        Args:
            peer: 통신할 샤드 구성원의 클라이언트 주소.
        """
        wake = self.wakes[peer]
        reader = writer = None
        try:
            while not self.closed:
                wake.clear()
                request = self._request(peer)
                if request is not None:
                    try:
                        if writer is None:
                            reader, writer = await asyncio.wait_for(
                                asyncio.open_connection(*address_parts(self.peers[peer])), RPC_TIMEOUT)
                        message = [self.scope, self.local.encode(), peer.encode(), request]
                        await asyncio.wait_for(write_frame(writer, message), RPC_TIMEOUT)
                        response = await asyncio.wait_for(read_frame(reader), RPC_TIMEOUT)
                        self._response(peer, request, response)
                    except (OSError, EOFError, asyncio.TimeoutError, ValueError):
                        if writer is not None:
                            writer.close()
                            writer = None
                try:
                    await asyncio.wait_for(wake.wait(), HEARTBEAT)
                except asyncio.TimeoutError:
                    pass
        finally:
            if writer is not None:
                writer.close()

    def _response(self, peer, request, response):
        """피어 응답을 원래 요청과 대조해 임기, 득표 또는 복제 진행 상태를 갱신한다.

        Args:
            peer: 응답을 보낸 피어의 클라이언트 주소.
            request: 이 응답에 대응하는 원래 RPC.
            response: 피어의 임기, 수락 여부, 로그 인덱스 또는 조각 번호를 담은 응답.
        """
        if not (isinstance(response, list) and len(response) == 3
                and all(map(counter, response)) and response[1] in (0, 1)):
            raise ValueError("invalid Raft response")
        term, accepted, index = response
        if term > self.term:
            self._step_down(term)
            return
        if term != self.term or term != request[1]:
            return
        if request[0] == b"vote":
            if self.role == "candidate" and accepted:
                self.votes.add(peer)
                if len(self.votes) >= self.quorum:
                    self._promote()
            return
        if self.role != "leader":
            return
        self.contact[peer] = time.monotonic()
        if request[0] == b"snapshot":
            base, part, count = request[2], request[5], request[6]
            if not ((accepted and index == part + 1) or (not accepted and index <= part)):
                raise ValueError("unexpected snapshot acknowledgement")
            if base != self.base:
                self.sent_parts.pop(peer, None)
            elif index < count:
                self.sent_parts[peer] = (base, index)
            else:
                self.sent_parts.pop(peer, None)
                self.matched[peer] = base
                self.next_index[peer] = base + 1
            self.wakes[peer].set()
            return
        if accepted:
            expected = request[2] + len(request[5])
            if index != expected:
                raise ValueError("peer acknowledged an unexpected log index")
            self.matched[peer] = max(self.matched[peer], index)
            self.next_index[peer] = self.matched[peer] + 1
            self._advance()
        else:
            self.next_index[peer] = max(1, min(self.next_index[peer] - 1, index + 1))
        if self.next_index[peer] <= self.last:
            self.wakes[peer].set()

    async def _accept(self, reader, writer):
        """수신 연결의 배치 식별자와 주소를 검증한 뒤 피어의 RPC를 처리한다.

        Args:
            reader: 피어가 보낸 프레임을 읽을 입력 스트림.
            writer: 처리 결과를 돌려줄 출력 스트림.
        """
        self.incoming.add(asyncio.current_task())
        try:
            while not self.closed:
                message = await asyncio.wait_for(read_frame(reader), 5)
                if not isinstance(message, list) or len(message) != 4 or type(message[1]) is not bytes:
                    return
                scope, source, dest, request = message
                peer = source.decode()
                if scope != self.scope or dest != self.local.encode() or peer not in self.wakes:
                    return
                validate_request(request)
                response = self._receive(peer, *request)
                await asyncio.wait_for(write_frame(writer, response), RPC_TIMEOUT)
        except sqlite3.Error as exc:
            self._fail(exc)
        except (EOFError, OSError, ValueError, TypeError, IndexError, asyncio.TimeoutError):
            pass
        finally:
            self.incoming.discard(asyncio.current_task())
            writer.close()

    def _receive(self, peer, kind, term, *payload):
        """피어의 임기를 확인하고 투표, 로그 추가 또는 스냅샷 설치 요청을 처리한다.

        Args:
            peer: 요청을 보낸 구성원의 클라이언트 주소.
            kind: 투표, 로그 추가, 스냅샷 설치를 구분하는 바이트 문자열.
            term: 요청을 보낸 피어의 현재 임기.
            *payload: 요청 종류에 따른 추가 필드.
        """
        if kind not in (b"vote", b"append", b"snapshot") or term < self.term:
            return [self.term, 0, self.last]
        if term > self.term:
            self._step_down(term)
        if kind == b"vote":
            index, last_term = payload
            current = (self.disk.term_at(self.last), self.last)
            allowed = self.vote in (None, peer) and (last_term, index) >= current
            if allowed:
                with self.disk.db:
                    self.disk.set("vote", peer)
                self.vote = peer
                self._join()
                self._reset_timer()
            return [self.term, int(allowed), self.last]
        if self.role != "follower":
            self._step_down(term)
        self.leader = peer
        self._join()
        self._reset_timer()
        if kind == b"snapshot":
            return self._install(*payload)
        return self._append_from_peer(*payload)

    def _append_from_peer(self, prev, prev_term, leader_commit, rows):
        """기준 로그의 임기를 확인한 뒤 리더와 다른 구간을 교체하고 일치한 범위까지 커밋한다.

        Args:
            prev: 이번 요청의 첫 항목 바로 앞 로그 인덱스.
            prev_term: 리더가 기록한 기준 인덱스의 임기.
            leader_commit: 리더가 이미 커밋한 마지막 인덱스.
            rows: 기준 인덱스 다음부터 차례로 적용할 임기와 명령 바이트 쌍.
        """
        if self.disk.term_at(prev) != prev_term:
            return [self.term, 0, min(self.last, prev - 1)]
        for index, (term, _) in enumerate(rows, prev + 1):
            if self.disk.term_at(index) != term:
                if index <= self.commit:
                    raise ValueError("attempt to overwrite a committed Raft entry")
                suffix = rows[index - prev - 1:]
                for _, command in suffix:
                    if command:
                        command_parts(command)
                with self.disk.db:
                    self.disk.truncate(index)
                    self.disk.append(index, suffix)
                self.last = prev + len(rows)
                break
        # Only the prefix actually confirmed against this leader is safe to apply.
        matched = prev + len(rows)
        if leader_commit > self.commit:
            index = min(leader_commit, matched)
            if index > self.commit:
                self._commit_to(index)
        return [self.term, 1, matched]

    def _discard_install(self):
        """받는 중인 스냅샷의 임시 스트림을 닫고 수신 상태를 비운다."""
        if self.installing is not None:
            self.installing[2].close()
            self.installing = None

    def _install(self, index, term, clock, part, count, blob):
        """스냅샷 조각을 순서대로 받아 완성된 RDB와 커밋 위치를 함께 설치한다.

        Args:
            index: 스냅샷이 포함하는 마지막 로그 인덱스.
            term: 해당 로그 인덱스의 임기.
            clock: 스냅샷을 생성한 밀리초 단위의 논리 시각.
            part: 이번 조각의 0부터 시작하는 번호.
            count: 이번 스냅샷의 전체 조각 수.
            blob: 수신한 조각의 바이트 또는 벌크 문자열 목록.

        Returns:
            현재 임기, 수락 여부, 다음에 요청할 조각 번호를 담은 응답.
        """
        from .rdb import read_snapshot
        if index <= self.commit:
            return [self.term, 1, part + 1]
        identity = (index, term, clock, count)
        if self.installing is None or self.installing[0] != identity:
            if part:
                return [self.term, 0, 0]
            self._discard_install()
            self.installing = [identity, 0, SpooledTemporaryFile(max_size=SNAPSHOT_CHUNK)]
        _, next_part, stream = self.installing
        if part > next_part:
            return [self.term, 0, next_part]
        if part == next_part:
            stream.write(unwrap_blob(blob))
            self.installing[1] += 1
        if self.installing[1] == count:
            try:
                stream.seek(0)
                restored = read_snapshot(stream)
                with self.disk.db:
                    if self.disk.term_at(index) != term:
                        self.disk.truncate(index)
                    self.disk.checkpoint(index, term, clock, stream)
                    self.disk.set("commit", index)
                self.base = self.applied = self.commit = index
                self.last = self.disk.last_index()
                self.machine.data, self.clock = restored, clock
            finally:
                self._discard_install()
        return [self.term, 1, part + 1]

    async def close(self):
        """보류 중인 명령과 피어 작업을 종료하고 리스너, 임시 스냅샷, 저장소를 닫는다."""
        self.closed = True
        self._reject_pending()
        self.server.close()
        tasks = self.tasks + list(self.incoming)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await self.server.wait_closed()
        self._discard_install()
        self.disk.close()
