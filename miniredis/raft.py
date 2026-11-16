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
    host, port = endpoint.rsplit(":", 1)
    return host.strip("[]"), int(port)


def unpack(data):
    value, end = resp.decode(data)
    if end != len(data):
        raise ValueError("trailing data in Raft frame")
    return value


async def read_frame(reader):
    size = int.from_bytes(await reader.readexactly(4), "big")
    if size == 0 or size > 1 << 30:
        raise ValueError("invalid Raft frame size")
    return unpack(await reader.readexactly(size))


async def write_frame(writer, value):
    data = resp.encode(value)
    writer.write(len(data).to_bytes(4, "big"))
    writer.write(data)
    await writer.drain()


def counter(value):
    return type(value) is int and value >= 0


def wire_blob(data):
    # A legal client request can contain multiple full-size RESP bulk values.
    if len(data) <= resp.MAX_BULK:
        return data
    return [data[i:i + resp.MAX_BULK] for i in range(0, len(data), resp.MAX_BULK)]


def unwrap_blob(value):
    if type(value) is bytes:
        return value
    if isinstance(value, list) and value and all(type(chunk) is bytes for chunk in value):
        return b"".join(value)
    raise ValueError("invalid Raft payload")


def validate_request(request):
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
    value = unpack(command)
    if not (isinstance(value, list) and len(value) == 2 and counter(value[0])
            and isinstance(value[1], list) and value[1]
            and all(type(arg) is bytes for arg in value[1])):
        raise ValueError("invalid Raft log command")
    return value


class Raft:
    @classmethod
    async def open(cls, local, peers, directory, topology_id):
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
        if not task.cancelled() and task.exception() is not None:
            self._fail(task.exception())

    def _fail(self, exc):
        # On a disk or FSM failure this voter cannot safely attest to the log.
        if not self.closed:
            logging.error("Raft voter %s stopped: %s", self.local, exc)
            self.closed = True
            self.role, self.leader = "failed", None
            self._reject_pending()
            self.server.close()
            self._wake()

    def _reset_timer(self):
        self.deadline = time.monotonic() + random.uniform(*ELECTION_WINDOW)

    def _wake(self):
        for event in self.wakes.values():
            event.set()

    def _reject_pending(self):
        for future in self.pending.values():
            if not future.done():
                future.set_exception(LeadershipLost())
        self.pending.clear()

    def _step_down(self, term):
        if term > self.term:
            with self.disk.db:
                self.disk.set("term", term)
                self.disk.set("vote", None)
            self.term, self.vote = term, None
        self.role, self.leader = "follower", None
        self._reject_pending()
        self._reset_timer()

    def _join(self):
        if not self.bootstrap:
            with self.disk.db:
                self.disk.set("joined", 1)
            self.bootstrap = True

    def _elect(self):
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
        with self.disk.db:
            self.disk.append(self.last + 1, [(self.term, command)])
        self.last += 1

    async def execute(self, args):
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
        # Only entries of the leader's current term can establish a new commit.
        indexes = sorted([self.last, *self.matched.values()], reverse=True)
        index = indexes[self.quorum - 1]
        if index > self.commit and self.disk.term_at(index) == self.term:
            self._commit_to(index)
            self._wake()

    def _commit_to(self, index):
        with self.disk.db:
            self.disk.set("commit", index)
        self.commit = index
        self._apply()
        if self.applied - self.base >= SNAPSHOT_INTERVAL:
            self._checkpoint()

    def _apply(self):
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
        if self.installing is not None:
            self.installing[2].close()
            self.installing = None

    def _install(self, index, term, clock, part, count, blob):
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
