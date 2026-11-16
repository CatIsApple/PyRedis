"""로컬 데모용 테스트 계정을 만든다: python -m demo.seed_users
아래 비밀번호는 데모용 테스트 값이다. 파일에는 평문이 아니라 scrypt 해시와 salt만 저장한다.

주요 함수: seed_users(TEST_ACCOUNTS를 해시해서 users.json에 쓴다). 계정은 demo/demo1234, alice/alice1234.
"""

import json
from pathlib import Path

from .auth import hash_password

TEST_ACCOUNTS = [("demo", "demo1234"), ("alice", "alice1234")]


def seed_users(path):
    users = {name: hash_password(password) for name, password in TEST_ACCOUNTS}
    Path(path).write_text(json.dumps(users, indent=2) + "\n", encoding="utf-8")
    return list(users)


if __name__ == "__main__":
    target = Path(__file__).resolve().with_name("users.json")
    names = seed_users(target)
    print(f"테스트 계정 {', '.join(names)} 을(를) {target} 에 만들었습니다 (비밀번호는 scrypt 해시로만 저장)")
