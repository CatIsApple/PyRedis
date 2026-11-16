import unittest
from unittest.mock import Mock

from miniredis.engine import Engine, Entry
from miniredis.resp import Error, Simple


class CommandTests(unittest.TestCase):
    def setUp(self):
        self.engine = Engine()

    def run_at(self, *args, now=1000):
        return self.engine.execute(list(args), now)

    def test_binary_ping_and_frames(self):
        self.assertIsInstance(self.run_at(b'pInG'), Simple)
        self.assertEqual(self.run_at(b'PING', b'\0\r\n\xff'), b'\0\r\n\xff')
        self.assertIs(type(self.run_at(b'PING', b'')), bytes)
        for frame in ([], b'PING', [None], [Simple(b'PING')], [b'PING', 1]):
            with self.subTest(frame=frame):
                self.assertIsInstance(self.engine.execute(frame), Error)

    def test_strings_and_duplicate_keys(self):
        self.run_at(b'SET', b'a', b'first', b'EX', b'3')
        self.assertEqual(self.run_at(b'MSET', b'a', b'', b'\xff', b'v', b'a', b'last'), b'OK')
        self.assertEqual(self.run_at(b'MGET', b'\xff', b'a', b'?', b'a'), [b'v', b'last', None, b'last'])
        self.assertEqual(self.run_at(b'TTL', b'a'), -1)
        self.assertEqual(self.run_at(b'EXISTS', b'a', b'a', b'\xff', b'?'), 3)
        self.assertEqual(self.run_at(b'DEL', b'a', b'a', b'\xff', b'?'), 2)
        self.assertEqual(self.run_at(b'TYPE', b'a'), b'none')

    def test_conditional_set_expiry_boundaries(self):
        self.assertEqual(self.run_at(b'SET', b'k', b'v', b'nx', b'EX', b'bad', b'ex', b'1'), b'OK')
        self.assertIsNone(self.run_at(b'SET', b'k', b'w', b'NX', b'EX', b'5', now=1100))
        self.assertIsInstance(self.run_at(b'SET', b'k', b'w', b'NX', b'EX', b'0'), Error)
        self.assertEqual(self.run_at(b'TTL', b'k', now=1500), 1)
        self.assertEqual(self.run_at(b'TTL', b'k', now=1501), 0)
        self.assertEqual(self.run_at(b'GET', b'k', now=1999), b'v')
        self.assertEqual(self.run_at(b'SET', b'k', b'w', b'NX', now=2000), b'OK')
        self.assertEqual(self.run_at(b'TTL', b'k', now=2001), -1)
        self.assertEqual(self.run_at(b'EXPIRE', b'k', b'-1'), 1)
        self.assertEqual(self.run_at(b'TTL', b'k'), -2)
        self.assertEqual(self.run_at(b'EXPIRE', b'k', b'1'), 0)
        for command in ([b'EXPIRE', b'k', b'9223372036854776'],
                        [b'SET', b'k', b'v', b'EX', b'9223372036854775']):
            self.assertIsInstance(self.engine.execute(command, 1000), Error)

    def test_counter_parsing_and_unchanged_value_on_failure(self):
        self.assertEqual(self.run_at(b'INCR', b'k'), 1)
        self.run_at(b'EXPIRE', b'k', b'10')
        self.assertEqual(self.run_at(b'INCR', b'k'), 2)
        self.assertEqual(self.run_at(b'TTL', b'k', now=2000), 9)
        for value in [b'+1', b'01', b'-0', b' 1', b'', b'9' * 30, b'9223372036854775807']:
            self.run_at(b'SET', b'k', value)
            self.assertIsInstance(self.run_at(b'INCR', b'k'), Error)
            self.assertEqual(self.run_at(b'GET', b'k'), value)
        self.run_at(b'SET', b'k', b'-9223372036854775808')
        self.assertEqual(self.run_at(b'INCR', b'k'), -9223372036854775807)

    def test_hash_updates_and_type_errors(self):
        self.assertEqual(self.run_at(b'HSET', b'h', b'f', b'1', b'f', b'2', b'g', b''), 2)
        self.run_at(b'EXPIRE', b'h', b'1')
        self.assertEqual(self.run_at(b'HSET', b'h', b'f', b'3', now=1100), 0)
        self.assertEqual(self.run_at(b'HGET', b'h', b'g'), b'')
        self.assertEqual(self.run_at(b'HGET', b'h', b'f'), b'3')
        self.assertIsNone(self.run_at(b'HGET', b'h', b'?'))
        self.assertEqual(self.run_at(b'TYPE', b'h'), b'hash')
        self.assertEqual(self.run_at(b'MGET', b'h', b'?'), [None, None])
        for command in ([b'GET', b'h'], [b'INCR', b'h']):
            self.assertTrue(self.engine.execute(command, 1100).startswith(b'WRONGTYPE'))
        self.assertEqual(self.run_at(b'HSET', b'h', b'n', b'4', now=2000), 1)
        self.assertIsNone(self.run_at(b'HGET', b'h', b'f', now=2000))
        self.assertEqual(self.run_at(b'TTL', b'h', now=2000), -1)
        self.run_at(b'SET', b'h', b's')
        for command in ([b'HGET', b'h', b'f'], [b'HSET', b'h', b'f', b'1']):
            self.assertTrue(self.engine.execute(command, 1000).startswith(b'WRONGTYPE'))
        self.assertEqual(self.run_at(b'TYPE', b'h'), b'string')

    def test_journal_normalization_and_replication(self):
        self.engine.journal = Mock()
        self.engine.on_change = Mock()
        self.run_at(b'set', b'k', b'4', b'NX', b'EX', b'1')
        self.engine.journal.append.assert_called_with([[b'SET', b'k', b'4', b'PXAT', b'2000']])
        self.run_at(b'incr', b'k', now=1500)
        self.engine.journal.append.assert_called_with([[b'SET', b'k', b'5', b'PXAT', b'2000']])
        self.run_at(b'hset', b'k', b'f', b'v', now=2000)
        self.engine.journal.append.assert_called_with([[b'DEL', b'k'], [b'HSET', b'k', b'f', b'v']])
        self.engine.on_change.assert_called_with([b'hset', b'k', b'f', b'v'], [b'k'], 2000, hash_created=True)
        self.run_at(b'hset', b'k', b'f', b'w', now=2001)
        self.engine.journal.append.assert_called_with([[b'HSET', b'k', b'f', b'w']])
        self.engine.on_change.assert_called_with([b'hset', b'k', b'f', b'w'], [b'k'], 2001, hash_created=False)
        self.run_at(b'expire', b'k', b'2', now=2001)
        self.engine.journal.append.assert_called_with([[b'PEXPIREAT', b'k', b'4001']])
        self.run_at(b'del', b'k', b'k', b'?')
        self.engine.journal.append.assert_called_with([[b'DEL', b'k']])

    def test_failed_append_preserves_data_and_suppresses_replication(self):
        self.run_at(b'MSET', b'a', b'4', b'b', b'5')
        self.run_at(b'HSET', b'h', b'f', b'v')
        before = self.engine.snapshot(1000)
        self.engine.journal = Mock()
        self.engine.journal.append.side_effect = OSError('disk error')
        self.engine.on_change = Mock()
        commands = [[b'MSET', b'a', b'3', b'b', b'6'], [b'DEL', b'a', b'b'],
                    [b'EXPIRE', b'a', b'0'], [b'INCR', b'a'], [b'SET', b'c', b'x'],
                    [b'HSET', b'h', b'f', b'w'], [b'HSET', b'new', b'f', b'w']]
        for command in commands:
            with self.subTest(command=command):
                self.assertTrue(self.engine.execute(command, 1000).startswith(b'MISCONF'))
                self.assertEqual(self.engine.snapshot(1000), before)
        self.engine.on_change.assert_not_called()

    def test_snapshot_and_bounded_expiry_sweep(self):
        self.engine.data = {b'a': Entry(b'constant'), b'h': Entry({b'x': b'old'}),
                            b'dead': Entry(b'abc', 50)}
        snapshot = self.engine.snapshot(100)
        self.run_at(b'HSET', b'h', b'x', b'new')
        self.assertEqual(snapshot, {b'a': Entry(b'constant'), b'h': Entry({b'x': b'old'})})
        for _ in range(3):
            self.engine.purge(100, 1)
        self.assertNotIn(b'dead', self.engine.data)

    def test_replica_access_and_wrong_arity_precedence(self):
        self.engine = Engine(follower=True)
        self.assertTrue(self.run_at(b'GET', b'a').startswith(b'LOADING'))
        self.assertTrue(self.run_at(b'INCR', b'a').startswith(b'READONLY'))
        self.assertTrue(self.run_at(b'HSET', b'a', b'f', b'v', b'odd').startswith(b'READONLY'))
        self.assertTrue(self.run_at(b'HSET', b'a').startswith(b'ERR wrong number'))
        self.assertEqual(self.run_at(b'PING'), b'PONG')
        self.engine.data = {b'a': Entry(b'1')}
        self.engine.loaded = True
        self.assertEqual(self.run_at(b'GET', b'a'), b'1')

    def test_rewrite_availability(self):
        self.assertIsInstance(self.run_at(b'BGREWRITEAOF'), Error)
        self.engine.rewrite = Mock()
        self.assertEqual(self.run_at(b'BGREWRITEAOF'), b'Background append only file rewriting started')
        self.engine.rewrite.side_effect = BlockingIOError()
        self.assertIn(b'already in progress', self.run_at(b'BGREWRITEAOF'))
