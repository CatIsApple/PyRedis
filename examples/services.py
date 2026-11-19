"""기능 테스트와 벤치마크가 실제 CLI 서비스를 임시 포트로 실행하는 도구."""
from contextlib import contextmanager
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]


@contextmanager
def service(module, args, ready):
    """임시 포트에서 서비스를 기동하고 종료 시 자식 프로세스를 회수한다.

    Args:
        module: 자식으로 실행할 파이썬 모듈 이름.
        args: 모듈에 전달할 명령줄 인자.
        ready: 준비 완료 로그에서 할당된 포트를 추출할 정규식.
    """
    with tempfile.TemporaryDirectory() as directory:
        log_path = Path(directory) / "service.log"
        with log_path.open("wb") as output:
            proc = subprocess.Popen([sys.executable, "-u", "-m", module, *args], cwd=ROOT,
                                    stdout=output, stderr=subprocess.STDOUT)
        try:
            deadline = time.monotonic() + 20
            while proc.poll() is None and time.monotonic() < deadline:
                match = re.search(ready, log_path.read_text(errors="replace"), flags=re.MULTILINE)
                if match:
                    yield int(match.group(1))
                    return
                time.sleep(0.01)
            raise RuntimeError(f"{module} 기동 실패\n{log_path.read_text(errors='replace')[-2000:]}")
        finally:
            if proc.poll() is None:
                proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()


def running_api(db_path, redis_port, cache="on", ttl=60):
    """테스트용 HTTP API를 실행하고 실제 포트를 제공할 컨텍스트 관리자를 만든다.

    Args:
        db_path: 주문을 저장할 SQLite 파일 경로.
        redis_port: API가 접속할 로컬 Mini Redis 포트.
        cache: 캐시를 사용하면 on, 우회하면 off.
        ttl: 캐시 항목의 유효 기간(초).
    """
    args = ["--host", "127.0.0.1", "--port", "0", "--db", str(db_path),
            "--redis-host", "127.0.0.1", "--redis-port", str(redis_port),
            "--cache", cache, "--ttl", str(ttl)]
    return service("examples.cache_api", args, r"^READY (\d+)$")
