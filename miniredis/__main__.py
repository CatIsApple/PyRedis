"""Start a standalone primary, a read-only replica, or one cluster voter."""
import argparse
import asyncio
from contextlib import AsyncExitStack
import logging
import signal

from .engine import Engine
from .server import Server, endpoint


def options(argv=None):
    """실행 인자를 해석하고 함께 사용할 수 없는 서버 옵션을 검사한다.

    Args:
        argv: 프로그램 이름을 제외한 인자 목록. None이면 현재 프로세스의 인자를 읽는다.

    Returns:
        수신 주소, 실행 모드, 영속화 설정을 담은 argparse.Namespace.
    """
    parser = argparse.ArgumentParser(prog="python3 -m miniredis", allow_abbrev=False)
    parser.add_argument("-addr", "--addr", default="127.0.0.1:6379", metavar="HOST:PORT")
    parser.add_argument("-aof", "--aof", help="AOF path (default appendonly.aof); empty for memory only")
    parser.add_argument("-appendfsync", "--appendfsync", choices=("always", "everysec", "no"))
    parser.add_argument("-replicaof", "--replicaof", metavar="HOST:PORT", help="follow a standalone primary")
    parser.add_argument("-cluster-config", "--cluster-config", metavar="FILE", help="cluster topology JSON")
    parser.add_argument("-cluster-dir", "--cluster-dir", metavar="DIR", help="this voter's persistent state")
    args = parser.parse_args(argv)
    if args.cluster_config and (args.replicaof or args.aof is not None or args.appendfsync):
        parser.error("cluster mode uses its own Raft store; omit replica and AOF options")
    if args.cluster_dir and not args.cluster_config:
        parser.error("-cluster-dir requires -cluster-config")
    if args.replicaof and args.aof:
        parser.error("replicas keep their copy in memory; omit -aof or pass an empty path")
    return args


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
    """선택한 모드로 서버를 실행하고 종료 시 소켓과 저장소를 정리한다.

    Args:
        args: options()가 반환한 수신 주소, 복제 및 영속화 설정.
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
        if args.cluster_config:
            from .cluster import Cluster
            cluster = await Cluster.open(args.addr, args.cluster_config, args.cluster_dir)
            resources.push_async_callback(cluster.close)
            server = Server(cluster.execute)
        else:
            from .replica import Primary, follow
            engine = Engine(follower=bool(args.replicaof))
            primary = None
            journal = None
            if args.replicaof:
                task = asyncio.create_task(follow(engine, *endpoint(args.replicaof)))
                resources.push_async_callback(stop_task, task)
            else:
                path = "appendonly.aof" if args.aof is None else args.aof
                if path:
                    from .persist import Journal
                    journal = Journal(path, args.appendfsync or "everysec", engine)
                    resources.callback(journal.close)
                    engine.journal = journal
                    engine.rewrite = journal.rewrite
                primary = Primary(engine)
            maintenance = asyncio.create_task(maintain(engine, journal))
            resources.push_async_callback(stop_task, maintenance)
            server = Server(engine.execute, primary)
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
