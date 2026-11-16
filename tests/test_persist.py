import io
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

from miniredis.engine import Engine, Entry
from miniredis.persist import Journal
from miniredis.rdb import write_snapshot
from miniredis.resp import Error, encode


def frames(*commands):
    return b"".join(encode(command.split()) for command in commands)


def snapshot(entries):
    stream = io.BytesIO()
    write_snapshot(stream, entries)
    return stream.getvalue()


class JournalTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.path = Path(tmp.name) / 'appendonly.aof'

    def open(self, policy="everysec"):
        engine = Engine()
        journal = Journal(self.path, policy, engine)
        self.addCleanup(journal.close)
        engine.journal, engine.rewrite = journal, journal.rewrite
        return engine, journal

    def test_round_trip_for_each_fsync_policy_and_rewrite(self):
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
                self.assertEqual(restored.data, engine.data)
                self.assertEqual(len(list(journal.dir.glob('*.incr.aof'))), 1)
                self.assertEqual(len(list(journal.dir.glob('*.base.rdb'))), 1)
                journal.close()

    def test_unfinished_rewrite_is_recoverable(self):
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
        while journal.pending:
            journal.tick()
        self.assertEqual(len(journal.tails), 1)
        journal.close()
        restored, _ = self.open()
        self.assertEqual(restored.execute(b'GET k'.split()), b'ok')
