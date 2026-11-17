import io
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

from miniredis.engine import Engine, Entry
from miniredis.hashtable import HashTable
from miniredis.persist import Journal
from miniredis.rdb import write_snapshot
from miniredis.resp import Error, encode


def frames(*commands):
    """공백으로 구분된 테스트 명령들을 RESP 배열로 인코딩해 이어 붙인다.

    Args:
        *commands: 명령 이름과 인자를 공백으로 구분한 바이트열들.
    """
    return b"".join(encode(command.split()) for command in commands)


def snapshot(entries):
    """키와 엔트리를 메모리 버퍼에 직렬화해 RDB 바이트열을 반환한다.

    Args:
        entries: 바이트 키를 스냅샷에 담을 엔트리에 대응시킨 사전.
    """
    stream = io.BytesIO()
    write_snapshot(stream, entries)
    return stream.getvalue()


class JournalTests(unittest.TestCase):
    def setUp(self):
        """테스트가 사용할 빈 임시 디렉터리에 기본 AOF 경로를 지정한다."""
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.path = Path(tmp.name) / 'appendonly.aof'

    def open(self, policy="everysec"):
        """지정한 동기화 정책으로 저널을 열고 새 엔진에 연결한다.

        Args:
            policy: AOF의 fsync 시점을 정하는 정책.

        Returns:
            파일에서 복구한 엔진과 여기에 연결된 저널의 쌍.
        """
        engine = Engine()
        journal = Journal(self.path, policy, engine)
        self.addCleanup(journal.close)
        engine.journal, engine.rewrite = journal, journal.rewrite
        return engine, journal

    def test_round_trip_for_each_fsync_policy_and_rewrite(self):
        """세 가지 fsync 정책에서 재작성 도중의 변경까지 복구하고 활성 파일 수를 검증한다."""
        self.path = self.path.with_name('기록 "a b".aof')
        for policy in ('no', 'everysec', 'always'):
            with self.subTest(policy=policy):
                engine, journal = self.open(policy)
                engine.execute([b'SET', b'k\x00', b'a\r\n', b'EX', b'120'])
                engine.execute(b'HSET h f v'.split())
                journal.rewrite()
                with self.assertRaises(BlockingIOError):
                    journal.rewrite()
                engine.execute(b'HSET h g w'.split())
                engine.execute(b'MSET one 9 two 8'.split())
                journal.close()
                restored, journal = self.open(policy)
                self.assertIsInstance(restored.data, HashTable)
                self.assertEqual(restored.data, engine.data)
                self.assertEqual(len(list(journal.dir.glob('*.incr.aof'))), 1)
                self.assertEqual(len(list(journal.dir.glob('*.base.rdb'))), 1)
                journal.close()

    def test_unfinished_rewrite_is_recoverable(self):
        """재작성이 중단되어도 두 증분 파일에서 재작성 전후의 값을 복구하는지 검증한다."""
        engine, journal = self.open()
        engine.execute(b'SET old 1'.split())
        journal.rewrite()
        engine.execute(b'SET new 2'.split())
        # A crash leaves the transitional manifest pointing to both increments.
        journal.pending[1].close()
        journal.pending = None
        journal.close()
        engine, _ = self.open()
        self.assertEqual(engine.execute(b'MGET old new'.split()), [b'1', b'2'])

    def test_legacy_rdb_preamble_and_aof_transaction(self):
        """RDB로 시작하는 기존 AOF에서 완료된 트랜잭션과 연장된 만료 시각을 복구하는지 검증한다."""
        now = time.time_ns() // 1_000_000
        prefix = snapshot({b'k': Entry(b'2', 1), b'h': Entry({b'a': b'b'}, 2)})
        contents = (prefix + b'#TS:10\r\n' + frames(b'SELECT 0', b'MULTI', b'INCR k', b'HMSET h c d', b'EXEC',
                    b'PEXPIREAT k ' + str(now + 60000).encode(),
                    b'PEXPIREAT h ' + str(now + 60000).encode(), b'SET k replaced NX', b'MULTI', b'UNLINK k'))
        self.path.write_bytes(contents)
        engine, _ = self.open()
        self.assertEqual(engine.execute(b'GET k'.split()), b'3')
        self.assertEqual(engine.execute(b'HGET h c'.split()), b'd')
        self.assertEqual(engine.execute(b'TTL k'.split()), 60)
        self.assertEqual(self.path.read_bytes(), contents)

    def test_only_final_incomplete_tail_is_repaired(self):
        """마지막 증분 파일의 미완료 트랜잭션을 제거하고 잘못된 레코드는 거부하는지 검증한다."""
        engine, journal = self.open()
        engine.execute(b'SET k 1'.split())
        journal.close()
        tail = next(journal.dir.glob('*.incr.aof'))
        valid = tail.read_bytes()
        tail.write_bytes(valid + frames(b'MULTI', b'DEL k') + b'*2\r\n$3\r\nEX')
        engine, journal = self.open()
        self.assertEqual(engine.execute(b'GET k'.split()), b'1')
        self.assertEqual(tail.read_bytes(), valid)
        journal.close()
        tail.write_bytes(valid + b'!bad')
        with self.assertRaises(ValueError):
            self.open()

    def test_missing_manifest_unsafe_path_and_lock(self):
        """저널이 중복 열기, 상위 디렉터리 참조, 기존 매니페스트의 유실을 거부하는지 검증한다."""
        _, journal = self.open()
        with self.assertRaises(OSError):
            self.open()
        journal.close()
        journal.manifest.write_bytes(b'file ../outside seq 1 type b\n')
        with self.assertRaises(ValueError):
            self.open()
        journal.manifest.unlink()
        with self.assertRaisesRegex(ValueError, "missing manifest"):
            self.open()

    def test_fsync_failure_before_acknowledgment_rolls_back_append(self):
        """응답 전 fsync가 실패하면 이전 AOF 길이와 값을 유지하는지 검증한다."""
        engine, journal = self.open('always')
        engine.execute(b'SET retained old'.split())
        position = journal.file.tell()
        with patch('miniredis.persist.os.fsync', side_effect=OSError("disk error")):
            failure = engine.execute(b'SET retained new'.split())
        self.assertIsInstance(failure, Error)
        self.assertTrue(failure.startswith(b'MISCONF'))
        self.assertEqual(journal.file.tell(), position)
        self.assertEqual(engine.execute(b'GET retained'.split()), b'old')
        journal.close()
        restored, _ = self.open()
        self.assertEqual(restored.execute(b'GET retained'.split()), b'old')

    def test_auto_rewrite_and_retry_after_everysec_sync_failure(self):
        """주기적 fsync 실패 후 재시도에서 쓰기가 재개되고 AOF가 자동 재작성되는지 검증한다."""
        engine, journal = self.open()
        engine.execute([b'SET', b'k', b'A' * 10000])
        journal.last_sync = time.monotonic() - 2
        with patch('miniredis.persist.os.fsync', side_effect=OSError("disk error")):
            with self.assertLogs('miniredis.persist', level='ERROR'):
                journal.tick()
        self.assertTrue(engine.execute(b'SET k bad'.split()).startswith(b'MISCONF'))
        journal.last_sync, journal.rewrite_threshold = time.monotonic() - 2, 1
        journal.tick()
        self.assertIsNotNone(journal.pending)
        self.assertEqual(engine.execute(b'SET k ok'.split()), b'OK')
        while journal.step_rewrite():
            pass
        self.assertEqual(len(journal.tails), 1)
        journal.close()
        restored, _ = self.open()
        self.assertEqual(restored.execute(b'GET k'.split()), b'ok')

    def test_rewrite_slices_yield_between_writes_and_stop_when_idle(self):
        """RDB를 나누어 기록하는 동안 새 쓰기를 처리하고 재작성이 끝나면 작업을 멈추는지 검증한다."""
        engine, journal = self.open()
        self.assertFalse(journal.step_rewrite())
        engine.execute([b'SET', b'large', b'x' * (192 << 10)])
        journal.rewrite()
        file = journal.pending[1]
        self.assertTrue(journal.step_rewrite())
        self.assertEqual(file.tell(), 64 << 10)
        engine.execute(b'SET during yes'.split())
        while journal.step_rewrite():
            pass
        self.assertFalse(journal.step_rewrite())
        journal.close()
        restored, _ = self.open()
        self.assertEqual(restored.data, engine.data)

    def test_failed_manifest_sync_keeps_previous_manifest_and_removes_temp(self):
        """매니페스트 동기화에 실패하면 이전 파일을 보존하고 임시 파일을 정리하는지 검증한다."""
        _, journal = self.open()
        previous = journal.manifest.read_bytes()
        with patch('miniredis.persist._sync', side_effect=OSError("could not sync manifest")):
            with self.assertRaises(OSError):
                journal._publish(journal.base, journal.tails)
        self.assertEqual(journal.manifest.read_bytes(), previous)
        self.assertEqual(list(journal.dir.glob('.miniredis-manifest-*')), [])
