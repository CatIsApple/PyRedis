import unittest
from unittest.mock import Mock

from miniredis.engine import Engine, Entry
from miniredis.resp import Error, Simple


class CommandTests(unittest.TestCase):
    def setUp(self):
        """각 테스트에 빈 기본 엔진을 준비한다."""
        self.engine = Engine()

    def run_at(self, *args, now=1000):
        """테스트 명령을 지정한 시각에 실행해 응답을 반환한다.

        Args:
            *args: 명령 이름부터 순서대로 전달하는 바이트 인자.
            now: 명령의 기준 시각(유닉스 밀리초). 기본값은 1000이다.
        """
        return self.engine.execute(list(args), now)

    def test_binary_ping_and_frames(self):
        """PING이 임의의 바이트를 그대로 돌려주고 잘못된 명령 배열은 거부하는지 검증한다."""
        self.assertIsInstance(self.run_at(b'pInG'), Simple)
        self.assertEqual(self.run_at(b'PING', b'\0\r\n\xff'), b'\0\r\n\xff')
        self.assertIs(type(self.run_at(b'PING', b'')), bytes)
        for frame in ([], b'PING', [None], [Simple(b'PING')], [b'PING', 1]):
            with self.subTest(frame=frame):
                self.assertIsInstance(self.engine.execute(frame), Error)

    def test_strings_and_duplicate_keys(self):
        """다중 키 명령의 중복 키 처리, 응답 순서, 덮어쓴 키의 만료 해제를 검증한다."""
        self.run_at(b'SET', b'a', b'first', b'EX', b'3')
        self.assertEqual(self.run_at(b'MSET', b'a', b'', b'\xff', b'v', b'a', b'last'), b'OK')
        self.assertEqual(self.run_at(b'MGET', b'\xff', b'a', b'?', b'a'), [b'v', b'last', None, b'last'])
        self.assertEqual(self.run_at(b'TTL', b'a'), -1)
        self.assertEqual(self.run_at(b'EXISTS', b'a', b'a', b'\xff', b'?'), 3)
        self.assertEqual(self.run_at(b'DEL', b'a', b'a', b'\xff', b'?'), 2)
        self.assertEqual(self.run_at(b'TYPE', b'a'), b'none')

    def test_conditional_set_expiry_boundaries(self):
        """SET NX와 EX의 조합, TTL 반올림 경계, 즉시 만료 및 기한 범위 오류를 검증한다."""
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
        """INCR의 정수 범위, 만료 기한 유지 및 실패 시 원래 값 보존을 검증한다."""
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
        """HSET의 필드 수와 만료 기한 유지, HGET 조회 결과와 자료형 오류를 검증한다."""
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

    def test_journal_normalization(self):
        """쓰기 명령을 재생할 때 같은 값과 만료 시각을 복원할 수 있도록 AOF 기록 형식을 검증한다."""
        self.engine.journal = Mock()
        self.run_at(b'set', b'k', b'4', b'NX', b'EX', b'1')
        self.engine.journal.append.assert_called_with([[b'SET', b'k', b'4', b'PXAT', b'2000']])
        self.run_at(b'incr', b'k', now=1500)
        self.engine.journal.append.assert_called_with([[b'SET', b'k', b'5', b'PXAT', b'2000']])
        self.run_at(b'hset', b'k', b'f', b'v', now=2000)
        self.engine.journal.append.assert_called_with([[b'DEL', b'k'], [b'HSET', b'k', b'f', b'v']])
        self.run_at(b'hset', b'k', b'f', b'w', now=2001)
        self.engine.journal.append.assert_called_with([[b'HSET', b'k', b'f', b'w']])
        self.run_at(b'expire', b'k', b'2', now=2001)
        self.engine.journal.append.assert_called_with([[b'PEXPIREAT', b'k', b'4001']])
        self.run_at(b'del', b'k', b'k', b'?')
        self.engine.journal.append.assert_called_with([[b'DEL', b'k']])

    def test_failed_append_preserves_data(self):
        """AOF 쓰기가 실패하면 명령 실행 전의 값과 만료 시각이 보존되는지 검증한다."""
        self.run_at(b'MSET', b'a', b'4', b'b', b'5')
        self.run_at(b'HSET', b'h', b'f', b'v')
        self.run_at(b'EXPIRE', b'a', b'30')
        before = self.engine.snapshot(1000)
        self.engine.journal = Mock()
        self.engine.journal.append.side_effect = OSError('disk error')
        commands = [[b'MSET', b'a', b'3', b'b', b'6'], [b'DEL', b'a', b'b'],
                    [b'EXPIRE', b'a', b'0'], [b'INCR', b'a'], [b'SET', b'c', b'x'],
                    [b'HSET', b'h', b'f', b'w'], [b'HSET', b'new', b'f', b'w']]
        for command in commands:
            with self.subTest(command=command):
                self.assertTrue(self.engine.execute(command, 1000).startswith(b'MISCONF'))
                self.assertEqual(self.engine.snapshot(1000), before)

    def test_snapshot_reclaims_expired_entries_at_the_logical_time(self):
        """스냅샷 시각에 만료된 키를 제거하고 살아 있는 해시와 기한을 독립적으로 복사하는지 검증한다."""
        self.engine.data = {
            b'constant': Entry(b'v'),
            b'live': Entry({b'x': b'old'}, 101),
            b'past': Entry(b'v', 50),
            b'boundary': Entry({b'x': b'gone'}, 100),
        }
        snapshot = self.engine.snapshot(100)
        self.assertEqual(set(self.engine.data), {b'constant', b'live'})
        self.run_at(b'HSET', b'live', b'x', b'new', now=100)
        self.run_at(b'EXPIRE', b'live', b'3', now=100)
        self.assertEqual(snapshot, {b'constant': Entry(b'v'), b'live': Entry({b'x': b'old'}, 101)})

    def test_bounded_expiry_sweep(self):
        """만료 검사가 호출별 개수 제한을 지키고 다음 호출에서 순회를 이어 가는지 검증한다."""
        self.engine.data = {b'a': Entry(b'constant'), b'h': Entry({b'x': b'old'}),
                            b'dead': Entry(b'abc', 50)}
        self.engine.purge(100, 1)
        self.assertIn(b'dead', self.engine.data)
        self.engine.purge(100, 2)
        self.assertNotIn(b'dead', self.engine.data)

    def test_wrong_arity_preserves_existing_values(self):
        """인자 개수가 잘못된 명령이 오류를 반환하고 기존 데이터를 보존하는지 검증한다."""
        self.run_at(b'SET', b'k', b'original')
        for command in ([b'GET'], [b'SET', b'k'], [b'HSET', b'h', b'f', b'v', b'odd'],
                        [b'MSET', b'k', b'new', b'odd'], [b'EXPIRE', b'k']):
            with self.subTest(command=command):
                self.assertTrue(self.engine.execute(command).startswith(b'ERR wrong number'))
        self.assertEqual(self.run_at(b'GET', b'k'), b'original')
        self.assertIsNone(self.run_at(b'HGET', b'h', b'f'))

    def test_rewrite_availability(self):
        """AOF 재작성 비활성 상태, 시작 성공, 이미 진행 중인 경우의 응답을 검증한다."""
        self.assertIsInstance(self.run_at(b'BGREWRITEAOF'), Error)
        self.engine.rewrite = Mock()
        self.assertEqual(self.run_at(b'BGREWRITEAOF'), b'Background append only file rewriting started')
        self.engine.rewrite.side_effect = BlockingIOError()
        self.assertIn(b'already in progress', self.run_at(b'BGREWRITEAOF'))
