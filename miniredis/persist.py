"""Crash-safe Redis multipart AOF with a cooperatively written RDB base."""
from __future__ import annotations

import logging
import os
import re
import stat
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

from .aof import replay
from .rdb import read_snapshot, snapshot_chunks, write_snapshot
from .resp import encode

_LOG = logging.getLogger(__name__)
_LOCK = ".goredis-lock"
_MARKER = ".goredis-initializing"
_WORD = re.compile(rb'"((?:[^"\\]|\\.)*)"|([^\s"]+)')
_ESCAPE = re.compile(rb'\\(?:x([0-9A-Fa-f]{2})|(.))')


@dataclass(frozen=True)
class Part:
    name: str
    seq: int
    kind: str


def _quote(name):
    """파일명을 AOF 매니페스트의 인용·이스케이프 규칙에 맞춰 인코딩한다.

    Args:
        name: 매니페스트에 적을 파일명.

    Returns:
        매니페스트 한 항목으로 쓸 수 있는 바이트열.
    """
    data = os.fsencode(name)
    if all(32 < b < 127 and b not in b'"\'\\' for b in data):
        return data
    parts = []
    for b in data:
        if b in b'"\\':
            parts.append(b"\\" + bytes([b]))
        elif b < 32 or b > 126:
            parts.append(b"\\x%02x" % b)
        else:
            parts.append(bytes([b]))
    return b'"' + b"".join(parts) + b'"'


def _unescape(match):
    """매니페스트의 이스케이프 시퀀스 하나를 원래 바이트로 복원한다.

    Args:
        match: 16진수 두 자리 또는 역슬래시 다음 문자를 담은 정규식 매치.
    """
    if match[1]:
        return bytes.fromhex(match[1].decode("ascii"))
    char = match[2]
    return {b"n": b"\n", b"r": b"\r", b"t": b"\t", b"b": b"\b", b"a": b"\a"}.get(char, char)


def _words(line):
    """매니페스트 한 줄에서 인용된 파일명을 해석하고 속성명과 값을 짝짓는다.

    Args:
        line: 주석을 제외한 매니페스트 한 줄의 바이트열.

    Returns:
        소문자 속성명을 속성값에 대응시킨 딕셔너리.
    """
    words = []
    end = 0
    for word in _WORD.finditer(line):
        if line[end:word.start()].strip() or (words and end == word.start()):
            raise ValueError("invalid quoted AOF manifest")
        words.append(word[2] if word[2] is not None else _ESCAPE.sub(_unescape, word[1]))
        end = word.end()
    if line[end:].strip() or len(words) < 6 or len(words) % 2:
        raise ValueError("invalid AOF manifest")
    return dict(zip((word.lower() for word in words[::2]), words[1::2]))


def _manifest(path):
    """매니페스트에 기록된 파일명과 순번을 검증해 활성 파일 목록을 읽는다.

    Args:
        path: 읽을 AOF 매니페스트 파일 경로.

    Returns:
        기준 파일(없으면 None)과 재생 순서대로 정렬된 증분 파일 목록.

    Raises:
        ValueError: 항목이 비어 있거나 경로, 유형, 순번에 오류가 있다.
    """
    base, tails, seen = None, [], set()
    with path.open("rb") as source:
        for line in source:
            if not line.endswith(b"\n"):
                raise ValueError("incomplete AOF manifest")
            if line.startswith(b"#"):
                continue
            fields = _words(line)
            try:
                part = Part(os.fsdecode(fields[b"file"]), int(fields[b"seq"]), fields[b"type"].decode("ascii"))
            except (ValueError, KeyError) as exc:
                raise ValueError("invalid AOF manifest attributes") from exc
            if (not 0 < part.seq < 1 << 63 or not part.name or Path(part.name).name != part.name
                    or part.name in {".", "..", path.name, _LOCK, _MARKER} or part.name in seen):
                raise ValueError("invalid or duplicate AOF part")
            seen.add(part.name)
            if part.kind == "b" and base is None:
                base = part
            elif part.kind == "i" and (not tails or part.seq > tails[-1].seq):
                tails.append(part)
            elif part.kind != "h":
                raise ValueError("invalid AOF base or increment sequence")
    if not base and not tails:
        raise ValueError("AOF manifest has no active files")
    return base, tails


def _sync_dir(directory):
    """POSIX에서 디렉터리 항목의 생성과 교체를 디스크에 동기화한다.

    Args:
        directory: 파일 이름 변경을 영속화할 디렉터리 경로.
    """
    if os.name == "nt":
        # Windows cannot open directories with os.open.
        return
    fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _sync(file):
    """파일의 사용자 공간 버퍼를 비우고 디스크에 동기화한다.

    Args:
        file: 버퍼 비우기와 fsync를 적용할 열린 파일.
    """
    file.flush()
    os.fsync(file.fileno())


def _lock(file):
    """다른 프로세스의 영속화 파일 접근을 막는 배타 잠금을 즉시 시도한다.

    Args:
        file: 잠금을 유지할 열린 파일.

    Raises:
        OSError: 다른 프로세스가 잠금을 보유했거나 운영체제가 잠금에 실패했다.
    """
    if os.name == "nt":
        import msvcrt
        file.seek(0)
        msvcrt.locking(file.fileno(), msvcrt.LK_NBLCK, 1)
    else:
        import fcntl
        fcntl.flock(file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)


def _open_regular(path, mode):
    """경로가 일반 파일을 가리키는지 검사한 뒤 지정한 모드로 연다.

    Args:
        path: 확인하고 열 파일 경로.
        mode: 파일 열기 모드. 복구 용도에 따라 읽기 또는 읽기·쓰기를 사용한다.

    Returns:
        호출자가 사용 후 닫을 파일 객체.
    """
    if not stat.S_ISREG(path.stat().st_mode):
        raise ValueError(f"AOF component must be a regular file: {path.name}")
    return path.open(mode)


class Journal:
    rewrite_threshold = 64 << 20

    def __init__(self, path, policy, engine):
        """AOF 디렉터리를 잠그고 스냅샷과 증분 로그에서 엔진의 키 공간을 복원한다.

        Args:
            path: AOF 경로. 파일명을 기준으로 다중 파일 AOF 디렉터리와 매니페스트를 정한다.
            policy: 쓰기 동기화 정책인 always, everysec, no 중 하나.
            engine: 복구한 키 공간을 받아 이후 스냅샷의 원본을 제공할 명령 실행기.
        """
        if policy not in {"always", "everysec", "no"}:
            raise ValueError("appendfsync must be always, everysec or no")
        legacy = Path(path).absolute()
        self.prefix = legacy.name
        self.dir = legacy.with_name("appendonlydir" if legacy.name == "appendonly.aof" else legacy.name + ".d")
        self.manifest = self.dir / (self.prefix + ".manifest")
        self.engine, self.policy = engine, policy
        self.base, self.tails = None, []
        self.file = self.pending = self.failed = self.sync_error = None
        self.dirty, self.retry_after, self.baseline, self.fixed_size = False, 0, 0, 0
        self.last_sync = time.monotonic()
        self.dir.mkdir(mode=0o700, exist_ok=True)
        _sync_dir(self.dir.parent)
        self.lock = (self.dir / _LOCK).open("a+b")
        try:
            _lock(self.lock)
            self._open(legacy)
        except BaseException:
            if self.file:
                self.file.close()
            self.lock.close()
            raise

    def _open(self, legacy):
        """활성 AOF 파일들을 재생하거나 새 세대를 초기화한 뒤 현재 시각에 만료된 키를 제거한다.

        Args:
            legacy: 매니페스트가 없을 때 가져올 기존 단일 AOF의 경로.
        """
        marker = self.dir / _MARKER
        data = {}
        if self.manifest.exists():
            self.base, self.tails = _manifest(self.manifest)
            if self.base:
                base_path = self.dir / self.base.name
                with _open_regular(base_path, "rb") as file:
                    if base_path.suffix == ".rdb":
                        data = read_snapshot(file)
                        if file.read(1):
                            raise ValueError("trailing bytes after RDB")
                    else:
                        replay(file, data, preamble=True)
                self.baseline = self.fixed_size = base_path.stat().st_size
            for i, part in enumerate(self.tails):
                last = i + 1 == len(self.tails)
                with _open_regular(self.dir / part.name, "r+b" if last else "rb") as file:
                    replay(file, data, tail="repair" if last else "strict")
                    if not last:
                        self.fixed_size += os.fstat(file.fileno()).st_size
            if self.tails:
                self.file = (self.dir / self.tails[-1].name).open("ab")
        else:
            if not marker.exists():
                if any(path.name != _LOCK for path in self.dir.iterdir()):
                    raise ValueError("missing manifest in nonempty AOF directory")
                marker.touch(mode=0o600, exist_ok=False)
                _sync_dir(self.dir)
            if legacy.exists():
                with _open_regular(legacy, "r+b") as file:
                    _lock(file)
                    replay(file, data, preamble=True, tail="ignore")
            self.base, file = self._part("base.rdb", 0, "b")
            with file:
                write_snapshot(file, data)
                _sync(file)
                self.baseline = self.fixed_size = file.tell()
        if self.file is None:
            self._rotate()
        marker.unlink(missing_ok=True)
        _sync_dir(self.dir)
        self._cleanup()
        # Later records may extend an expired timestamp in a base snapshot.
        now = time.time_ns() // 1_000_000
        self.engine.data = (
            (key, entry) for key, entry in data.items()
            if entry.expires is None or entry.expires > now
        )

    def _part(self, suffix, sequence, kind):
        """기존 파일과 이름이 겹치지 않는 다음 순번의 AOF 구성 파일을 생성한다.

        Args:
            suffix: 순번 뒤에 붙일 파일명 접미사.
            sequence: 탐색 시작점인 이전 순번. 실제 생성은 다음 번호부터 시도한다.
            kind: 매니페스트에 적을 구성 파일 유형. b는 기준 파일, i는 증분 파일이다.

        Returns:
            새 파일의 매니페스트 항목과 열린 바이너리 출력 파일.
        """
        while sequence < (1 << 63) - 1:
            sequence += 1
            part = Part(f"{self.prefix}.{sequence}.{suffix}", sequence, kind)
            try:
                file = (self.dir / part.name).open("xb")
            except FileExistsError:
                continue
            return part, file
        raise OSError("AOF sequence exhausted")

    def _publish(self, base, tails):
        """새 매니페스트를 동기화한 뒤 원자적으로 교체해 활성 파일 목록을 갱신한다.

        Args:
            base: 새 매니페스트가 가리킬 기준 파일 항목. 생략할 때는 None이다.
            tails: 새 매니페스트에 재생 순서대로 기록할 증분 파일 항목 목록.
        """
        # New files and their names must reach disk before the manifest points to them.
        _sync_dir(self.dir)
        file = tempfile.NamedTemporaryFile(dir=self.dir, prefix=".miniredis-manifest-", delete=False)
        tmp = Path(file.name)
        try:
            with file:
                for part in ([base] if base else []) + tails:
                    file.write(b"file %s seq %d type %s\n" % (_quote(part.name), part.seq, part.kind.encode()))
                _sync(file)
            os.replace(tmp, self.manifest)
            # If directory fsync fails after rename, both generations must survive.
            try:
                _sync_dir(self.dir)
            except OSError as exc:
                self.failed = exc
                raise
        finally:
            tmp.unlink(missing_ok=True)
        self.base, self.tails = base, tails

    def _rotate(self):
        """기존 쓰기를 동기화하고 다음 증분 파일을 매니페스트에 등록한 뒤 쓰기 대상을 전환한다."""
        if self.file:
            _sync(self.file)
        seq = self.tails[-1].seq if self.tails else 0
        tail, file = self._part("incr.aof", seq, "i")
        try:
            _sync(file)
            self._publish(self.base, self.tails + [tail])
        except BaseException:
            file.close()
            raise
        if self.file:
            self.fixed_size += self.file.tell()
            self.file.close()
        self.file = file
        self.dirty, self.sync_error = False, None

    def _cleanup(self, retired=()):
        """활성 매니페스트에서 빠진 관리 대상 파일과 남은 임시 파일을 정리한다.

        Args:
            retired: 이전 매니페스트에서 제외된 파일명 모음. 기본값은 빈 튜플이다.
        """
        live = {p.name for p in self.tails}
        if self.base:
            live.add(self.base.name)
        pattern = re.compile(re.escape(self.prefix) + r"\.[1-9][0-9]*\.(?:base\.rdb|base\.aof|incr\.aof)$")
        for path in self.dir.iterdir():
            if path.name in live or not path.is_file():
                continue
            if (path.name in retired or pattern.fullmatch(path.name) or
                re.fullmatch(r"\.miniredis-manifest-[a-z0-9_]{8}|\.goredis-(?:manifest|rdb)-[0-9]+", path.name)):
                try:
                    path.unlink()
                except OSError as exc:
                    _LOG.warning("AOF cleanup of %s: %s", path.name, exc)

    def append(self, commands):
        """한 쓰기 작업의 명령들을 활성 증분 AOF에 기록하고 정책에 따라 동기화한다.

        Args:
            commands: 실행 순서대로 정규화된 AOF 명령 배열들.

        Raises:
            OSError: 쓰기 또는 동기화에 실패했다. 기록 중 오류가 나면 작업 시작 위치로 되돌리기를 시도한다.
        """
        if self.failed or self.sync_error:
            raise self.failed or self.sync_error
        start = self.file.tell()
        try:
            for command in commands:
                self.file.write(encode(command))
            self.file.flush()
            if self.policy == "always":
                _sync(self.file)
            else:
                self.dirty = True
        except OSError as exc:
            self.failed = exc
            self.file.truncate(start)
            self.file.seek(start)
            _sync(self.file)
            raise

    def rewrite(self):
        """키 공간을 복사하고 증분 파일을 전환한 뒤 새 RDB 기준 파일의 재작성을 준비한다.

        Raises:
            BlockingIOError: 이미 진행 중인 AOF 재작성이 있다.
        """
        if self.pending is not None:
            raise BlockingIOError("AOF rewrite already in progress")
        if self.failed:
            raise self.failed
        entries = self.engine.snapshot()
        self._rotate()
        base, file = self._part("base.rdb", self.base.seq if self.base else 0, "b")
        self.pending = (base, file, snapshot_chunks(entries))

    def _write_chunk(self):
        """RDB 조각 하나를 쓰거나 모든 조각을 쓴 뒤 새 기준 파일을 매니페스트에 등록한다."""
        base, file, chunks = self.pending
        try:
            chunk = next(chunks, None)
            if chunk is not None:
                file.write(chunk)
                return
            _sync(file)
            size = file.tell()
            file.close()
            retired = {p.name for p in self.tails[:-1]}
            if self.base:
                retired.add(self.base.name)
            self._publish(base, self.tails[-1:])
            self.baseline = size + self.file.tell()
            self.fixed_size = size
            self._cleanup(retired)
        except (OSError, ValueError):
            file.close()
            self.pending = None
            raise
        self.pending = None

    def step_rewrite(self):
        """진행 중인 AOF 재작성을 한 단계 수행하고 실패하면 자동 재시도를 늦춘다.

        Returns:
            다음 이벤트 루프에서 이어 쓸 작업이 남아 있으면 참이다.
        """
        if self.pending is None:
            return False
        try:
            self._write_chunk()
        except (OSError, ValueError) as exc:
            self.retry_after = time.monotonic() + 60
            _LOG.error("AOF rewrite: %s", exc)
        return self.pending is not None

    def tick(self):
        """동기화 시각과 AOF 크기를 점검해 필요한 fsync 또는 자동 재작성을 시작한다."""
        now = time.monotonic()
        if self.policy == "everysec" and self.dirty and now - self.last_sync >= 1:
            self.last_sync = now
            try:
                _sync(self.file)
                self.dirty, self.sync_error = False, None
            except OSError as exc:
                self.sync_error = exc
                _LOG.error("AOF fsync: %s", exc)
        if self.failed or self.pending is not None or now < self.retry_after:
            return
        total = self.fixed_size + self.file.tell()
        if total <= self.rewrite_threshold or total < 2 * self.baseline:
            return
        try:
            self.rewrite()
        except (OSError, ValueError) as exc:
            self.retry_after = now + 60
            _LOG.error("AOF rewrite: %s", exc)

    def close(self):
        """진행 중인 재작성을 마치고 증분 파일을 동기화한 뒤 파일과 잠금을 해제한다."""
        if self.lock.closed:
            return
        try:
            try:
                while self.pending:
                    self._write_chunk()
            finally:
                _sync(self.file)
        finally:
            try:
                self.file.close()
            finally:
                self.lock.close()
