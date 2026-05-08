"""웹 사용자 생성 / 비번 변경 CLI.

사용:
  python -m web.create_user <username> [--name "표시 이름"]

기존 사용자가 있으면 비번만 갱신.
"""
from __future__ import annotations

import argparse
import getpass
import sys

from database import init_db, upsert_user, load_user
from web.auth import hash_password


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("username", help="로그인 아이디")
    p.add_argument("--name", help="화면에 표시될 이름 (기본: username)")
    args = p.parse_args()

    init_db()  # users 테이블 보장

    pw1 = getpass.getpass("비밀번호: ")
    pw2 = getpass.getpass("비밀번호 확인: ")
    if pw1 != pw2:
        print("ERROR: 비밀번호가 일치하지 않습니다.", file=sys.stderr)
        return 1
    if len(pw1) < 6:
        print("ERROR: 비밀번호는 최소 6자 이상이어야 합니다.", file=sys.stderr)
        return 1

    existed = load_user(args.username) is not None
    upsert_user(args.username, hash_password(pw1), args.name)

    if existed:
        print(f"[OK] 사용자 '{args.username}' 비밀번호를 변경했습니다.")
    else:
        print(f"[OK] 사용자 '{args.username}' 를 생성했습니다.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
