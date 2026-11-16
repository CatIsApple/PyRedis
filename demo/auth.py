"""비밀번호 해시·검증, 세션 id 생성, 쿠키 파싱 (JS demo/auth.js와 같은 scrypt 파라미터를 써서 users.json을 그대로 쓴다).

주요 함수: hash_password(scrypt+salt), verify_password(상수 시간 비교), new_session_id(32바이트 난수 hex),
parse_cookies(Cookie 헤더 → dict), load_users(users.json 읽기).
"""

import hashlib
import hmac
import json
import os
import re
from pathlib import Path
from urllib.parse import unquote

KEY_LENGTH = 64
SCRYPT_N, SCRYPT_R, SCRYPT_P = 16384, 8, 1  # Node의 crypto.scrypt 기본값과 같다

SESSION_ID_PATTERN = re.compile(r"[0-9a-f]{64}")

# 없는 아이디에도 같은 해시 계산을 하기 위한 가짜 계정. 비교는 항상 실패한다.
DUMMY_CREDENTIAL = {"salt": "00" * 16, "hash": "00" * KEY_LENGTH}


def _scrypt(password, salt_hex):
    # 파이썬의 hashlib.scrypt는 동기 함수다. 데모 서버는 스레드마다 따로 계산하므로 다른 요청을 막지 않는다.
    return hashlib.scrypt(password.encode("utf-8"), salt=bytes.fromhex(salt_hex), n=SCRYPT_N, r=SCRYPT_R, p=SCRYPT_P, dklen=KEY_LENGTH)


def hash_password(password, salt=None):
    """사용자마다 새 랜덤 salt를 써서 같은 비밀번호도 다른 해시가 되게 한다."""
    salt = salt or os.urandom(16).hex()
    return {"salt": salt, "hash": _scrypt(password, salt).hex()}


def verify_password(password, credential):
    expected = bytes.fromhex(credential["hash"])
    actual = _scrypt(password, credential["salt"])
    # compare_digest: 앞에서부터 비교하다 멈추는 일반 비교는 응답 시간으로 해시를 알아낼 수 있어서 쓰지 않는다
    return len(expected) == len(actual) and hmac.compare_digest(expected, actual)


def new_session_id():
    """추측할 수 없도록 암호학적 난수 32바이트 (hex 64자)."""
    return os.urandom(32).hex()


def parse_cookies(header):
    """Cookie 헤더를 dict로 바꾼다. 같은 이름이 여러 번 오면 첫 번째 값을 쓴다."""
    cookies = {}
    if not isinstance(header, str):
        return cookies
    for part in header.split(";"):
        name, eq, value = part.partition("=")
        name = name.strip()
        if not eq or name == "":
            continue  # 이름이 없거나 "="가 없는 조각은 무시
        value = value.strip()
        if len(value) >= 2 and value.startswith('"') and value.endswith('"'):
            value = value[1:-1]
        value = unquote(value)
        cookies.setdefault(name, value)
    return cookies


def load_users(path):
    """users.json → {아이디: {"salt", "hash"}}. 비밀번호 평문은 어디에도 없다."""
    return json.loads(Path(path).read_text(encoding="utf-8"))
