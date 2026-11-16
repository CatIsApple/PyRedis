"""단일 노드 Redis 서버의 실행 진입점."""
import argparse
import asyncio
from contextlib import AsyncExitStack
import logging
import signal

from .engine import Engine
from .persist import Journal
from .server import Server, endpoint


def options(argv=None):
    """단일 노드의 수신 주소와 영속화 옵션을 해석한다.

    Args:
        argv: 프로그램 이름을 제외한 인자 목록. None이면 현재 프로세스의 인자를 읽는다.

    Returns:
        수신 주소, AOF 경로, 디스크 동기화 정책을 담은 argparse.Namespace.
    """
    parser = argparse.ArgumentParser(prog="python3 -m miniredis", allow_abbrev=False)
    parser.add_argument("-addr", "--addr", default="127.0.0.1:6379", metavar="HOST:PORT")
    parser.add_argument("-aof", "--aof", default="appendonly.aof",
                        help="AOF path (default appendonly.aof); empty for memory only")
    parser.add_argument("-appendfsync", "--appendfsync", default="everysec",
                        choices=("always", "everysec", "no"))
    return parser.parse_args(argv)


async def maintain(engine, journal):
    """만료 키를 주기적으로 정리하고 AOF 재작성 사이에 다른 연결을 처리할 시간을 준다.

    Args:
        engine: 만료된 키를 정리할 명령 실행기.
        journal: 디스크 동기화와 재작성을 담당하는 AOF 저널. 메모리 전용이면 None이다.
    """
    loop = asyncio.get_running_loop()
    next_tick = loop.time()
    while True:
        if loop.time() >= next_tick:
            engine.purge()
            if journal is not None:
                journal.tick()
            next_tick = loop.time() + 0.1
        rewriting = journal is not None and journal.step_rewrite()
        if rewriting:
            await asyncio.sleep(0)  # Yield to other sockets between RDB chunks.
        else:
            await asyncio.sleep(max(0, next_tick - loop.time()))


async def stop_task(task):
    """백그라운드 작업을 취소하고 정리가 끝날 때까지 기다린다.

    Args:
        task: 서버 종료 시 함께 중단할 asyncio 작업.
    """
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)


async def run(args):
    """단일 엔진으로 모든 클라이언트 요청을 처리하고 종료 시 소켓과 저장소를 정리한다.

    Args:
        args: options()가 반환한 수신 주소와 영속화 설정.
    """
    host, port = endpoint(args.addr)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    async with AsyncExitStack() as resources:
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, stop.set)
            except NotImplementedError:  # Windows event loops use KeyboardInterrupt.
                continue
            resources.callback(loop.remove_signal_handler, sig)
        engine = Engine()
        journal = None
        if args.aof:
            journal = Journal(args.aof, args.appendfsync, engine)
            resources.callback(journal.close)
            engine.journal = journal
            engine.rewrite = journal.rewrite
        maintenance = asyncio.create_task(maintain(engine, journal))
        resources.push_async_callback(stop_task, maintenance)
        server = Server(engine)
        await server.start(host, port)
        resources.push_async_callback(server.close)
        logging.info("mini-redis listening on %s", server.listener.sockets[0].getsockname())
        await stop.wait()


def main():
    """CLI 옵션으로 이벤트 루프를 시작하고 기동 오류를 종료 메시지로 전달한다."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        asyncio.run(run(options()))
    except KeyboardInterrupt:
        pass
    except (OSError, ValueError) as exc:
        raise SystemExit(f"mini-redis: {exc}")


if __name__ == "__main__":
    main()
