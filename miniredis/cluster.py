"""Fixed Redis hash-slot ranges backed by a separate Raft group per shard."""

import asyncio
import binascii
import hashlib
import ipaddress
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from . import resp
from .raft import LeadershipLost, NotLeader, Raft


SLOT_COUNT = 16384
# Positive arity is exact; negative arity is the minimum number of arguments.
# The stride is zero for one key and positive for multi-key commands.
KEY_COMMANDS = {
    b"GET": (2, 0), b"SET": (-3, 0), b"TYPE": (2, 0), b"TTL": (2, 0),
    b"INCR": (2, 0), b"EXPIRE": (3, 0), b"HGET": (3, 0), b"HSET": (-4, 0),
    b"DEL": (-2, 1), b"EXISTS": (-2, 1), b"MGET": (-2, 1), b"MSET": (-3, 2),
}


def key_slot(key: bytes) -> int:
    start = key.find(b"{")
    if start != -1:
        end = key.find(b"}", start + 1)
        if end > start + 1:
            key = key[start + 1:end]
    return binascii.crc_hqx(key, 0) % SLOT_COUNT


def first_slot(shard: int, count: int) -> int:
    return (shard * SLOT_COUNT + count - 1) // count


def node_id(addr: str) -> str:
    return hashlib.sha1(addr.encode()).hexdigest()


def endpoint(addr: str):
    if not isinstance(addr, str):
        raise ValueError(f"invalid advertised address {addr!r}")
    if addr.startswith("["):
        host, sep, port = addr[1:].partition("]:")
    else:
        host, sep, port = addr.partition(":")
    if (not sep or not host or any(c in "/\\[]" or c.isspace() for c in host)
            or not port.isascii() or not port.isdigit() or len(port) > 5
            or port.startswith("0") or int(port) > 65535):
        raise ValueError(f"invalid advertised address {addr!r}")
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        if ":" in host:
            raise ValueError(f"invalid IPv6 address {addr!r}")
    else:
        if ip.is_unspecified:
            raise ValueError(f"cannot advertise unspecified address {addr!r}")
    return host, int(port)


@dataclass
class Topology:
    shards: list
    local_shard: int
    identity: str

    @classmethod
    def load(cls, path, local):
        config = json.loads(Path(path).read_text(encoding="utf-8"))
        if not isinstance(config, dict) or set(config) != {"shards"}:
            raise ValueError("expected a cluster config containing shards")
        shards = config["shards"]
        if not isinstance(shards, list) or not 1 <= len(shards) <= SLOT_COUNT:
            raise ValueError(f"cluster config requires 1 to {SLOT_COUNT} shards")

        seen = set()
        peers_by_shard = []
        local_shard = None
        for index, shard in enumerate(shards):
            if not isinstance(shard, dict) or set(shard) != {"nodes"}:
                raise ValueError(f"shard {index} must contain nodes")
            nodes = shard["nodes"]
            if not isinstance(nodes, list) or len(nodes) < 3:
                raise ValueError(f"shard {index} requires at least three voting nodes")
            peers = {}
            for node in nodes:
                if not isinstance(node, dict) or set(node) != {"addr", "raft"}:
                    raise ValueError(f"shard {index}: expected addr and raft")
                for addr in (node["addr"], node["raft"]):
                    endpoint(addr)
                    if addr in seen:
                        raise ValueError(f"duplicate cluster address {addr!r}")
                    seen.add(addr)
                peers[node["addr"]] = node["raft"]
            peers_by_shard.append(peers)
            if local in peers:
                local_shard = index
        if local_shard is None:
            raise ValueError(f"listen address {local!r} is missing from cluster config")
        # Keep the Go implementation's canonical topology identity independent of
        # whitespace and the order of fields inside each JSON object.
        canonical = json.dumps(config, separators=(",", ":"), sort_keys=True, ensure_ascii=False)
        for character in "<>&\u2028\u2029":
            canonical = canonical.replace(character, f"\\u{ord(character):04x}")
        return cls(peers_by_shard, local_shard, hashlib.sha256(canonical.encode()).hexdigest())


def arity_error(name):
    return resp.Error(b"ERR wrong number of arguments for '" + name.lower() + b"' command")


def slot_node(addr):
    host, port = endpoint(addr)
    return [host.encode(), port, node_id(addr).encode()]


class Cluster:
    def __init__(self, local, topology, group):
        self.local = local
        self.topology = topology
        self.group = group
        self.owners = [None] * len(topology.shards)
        self.discovery = None

    @classmethod
    async def open(cls, addr, config_path, cluster_dir=None):
        topology = Topology.load(config_path, addr)
        directory = cluster_dir or Path("cluster-data") / node_id(addr)
        peers = topology.shards[topology.local_shard]
        group = await Raft.open(addr, peers, directory, topology.identity)
        cluster = cls(addr, topology, group)
        cluster.discovery = asyncio.create_task(cluster._discover())
        return cluster

    async def close(self):
        if self.discovery is not None:
            self.discovery.cancel()
            await asyncio.gather(self.discovery, return_exceptions=True)
        await self.group.close()

    def owner(self, shard) -> Optional[str]:
        if shard == self.topology.local_shard:
            return self.group.leader
        return self.owners[shard]

    async def _discover(self):
        remote = [i for i in range(len(self.owners)) if i != self.topology.local_shard]
        while True:
            await asyncio.gather(*(self._discover_shard(i) for i in remote))
            await asyncio.sleep(0.5)

    async def _discover_shard(self, shard):
        previous = self.owners[shard]
        if previous and await self._reports_leader(previous):
            return
        others = [addr for addr in self.topology.shards[shard] if addr != previous]
        states = await asyncio.gather(*(self._reports_leader(addr) for addr in others))
        leaders = [addr for addr, leader in zip(others, states) if leader]
        self.owners[shard] = leaders[0] if len(leaders) == 1 else None

    async def _reports_leader(self, addr):
        async def query():
            reader, writer = await asyncio.open_connection(*endpoint(addr), limit=4096)
            try:
                writer.write(resp.encode([b"GOREDIS.LEADER", self.topology.identity.encode()]))
                await writer.drain()
                # Only accept the queried address as a bulk string; bound the read even
                # when the remote process on a configured port is misconfigured.
                expected = resp.encode(addr.encode())
                header, body = expected.split(b"\r\n", 1)
                if await reader.readline() != header + b"\r\n":
                    return False
                return await reader.readexactly(len(body)) == body
            finally:
                writer.close()
                await writer.wait_closed()

        try:
            return await asyncio.wait_for(query(), timeout=0.35)
        except (OSError, asyncio.TimeoutError, asyncio.IncompleteReadError, ValueError):
            return False

    def _redirect(self, slot, owner):
        if owner is None or owner == self.local:
            return resp.Error(b"CLUSTERDOWN Hash slot not served")
        return resp.Error(f"MOVED {slot} {owner}".encode())

    async def execute(self, args):
        if not isinstance(args, list) or not args:
            return resp.Error(b"ERR expected a non-empty command array")
        if any(type(arg) is not bytes for arg in args):
            return resp.Error(b"ERR expected a non-null bulk string")
        name = args[0].upper()
        if name == b"GOREDIS.LEADER":
            if len(args) != 2 or args[1] != self.topology.identity.encode():
                return resp.Error(b"ERR cluster topology ID required")
            return self.local.encode() if self.group.leader == self.local else None
        if name == b"CLUSTER":
            return self._cluster_command(args)
        if name == b"PING":
            if len(args) == 1:
                return resp.Simple(b"PONG")
            if len(args) == 2:
                return args[1]
            return arity_error(name)
        if name == b"BGREWRITEAOF":
            if len(args) != 1:
                return arity_error(name)
            return resp.Error(b"ERR AOF persistence is disabled in cluster mode")

        if name not in KEY_COMMANDS:
            return resp.Error(b"ERR unknown command")
        arity, stride = KEY_COMMANDS[name]
        n = len(args)
        if ((arity > 0 and n != arity) or (arity < 0 and n < -arity)
                or (name == b"MSET" and n % 2 == 0) or (name == b"HSET" and n % 2 != 0)):
            return arity_error(name)
        slot = key_slot(args[1])
        if stride and any(key_slot(key) != slot for key in args[1 + stride::stride]):
            return resp.Error(b"CROSSSLOT Keys in request don't hash to the same slot")
        shard = slot * len(self.owners) // SLOT_COUNT
        owner = self.owner(shard)
        if shard != self.topology.local_shard or owner != self.local:
            return self._redirect(slot, owner)
        try:
            return await self.group.execute(args)
        except NotLeader:
            return self._redirect(slot, self.owner(shard))
        except LeadershipLost:
            return resp.Error(b"ERR command outcome unknown after leadership change")

    def _cluster_command(self, args):
        if len(args) < 2:
            return arity_error(b"cluster")
        sub = args[1].upper()
        if sub == b"KEYSLOT":
            return key_slot(args[2]) if len(args) == 3 else arity_error(b"cluster|keyslot")
        if sub == b"SLOTS":
            return self._slots() if len(args) == 2 else arity_error(b"cluster|slots")
        if sub == b"INFO":
            return self._info() if len(args) == 2 else arity_error(b"cluster|info")
        return resp.Error(b"ERR unknown CLUSTER subcommand")

    def _slots(self):
        rows = []
        count = len(self.owners)
        for i, peers in enumerate(self.topology.shards):
            owner = self.owner(i)
            if owner:
                row = [first_slot(i, count), first_slot(i + 1, count) - 1, slot_node(owner)]
                row.extend(slot_node(addr) for addr in peers if addr != owner)
                rows.append(row)
        return rows

    def _info(self):
        count = len(self.owners)
        healthy = [i for i in range(count) if self.owner(i)]
        slots = sum(first_slot(i + 1, count) - first_slot(i, count) for i in healthy)
        known = sum(len(peers) for peers in self.topology.shards)
        return (
            f"cluster_state:{'ok' if slots == SLOT_COUNT else 'fail'}\r\n"
            f"cluster_slots_assigned:{SLOT_COUNT}\r\ncluster_slots_ok:{slots}\r\n"
            f"cluster_slots_fail:{SLOT_COUNT - slots}\r\ncluster_known_nodes:{known}\r\n"
            f"cluster_size:{len(healthy)}\r\n"
        ).encode()
