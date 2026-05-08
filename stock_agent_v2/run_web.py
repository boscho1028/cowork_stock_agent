"""웹 서버 진입점.

기본 실행:
    python run_web.py                # 0.0.0.0:8000 (ngrok 으로 노출)
환경변수:
    WEB_HOST       (기본 0.0.0.0)
    WEB_PORT       (기본 8000)
    WEB_SECRET_KEY (세션 쿠키 서명 키 — 없으면 임시 키, 재시작 시 로그아웃)
    WEB_URL        (텔레그램 brief 알림에 붙일 외부 URL — 옵션)
"""
import os
import sys

os.environ.setdefault("PYTHONUTF8", "1")
os.environ.setdefault("PYTHONIOENCODING", "utf-8")

# Windows cp949 콘솔에서도 한글/em-dash 가 깨지지 않도록 강제 UTF-8
for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
    except Exception:
        pass

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database import init_db, load_user
from web.app import create_app


def main():
    init_db()

    # 사용자 0명일 때 친절히 알려주기 — 어차피 로그인 못 하니 안내가 필요
    if load_user("__probe__") is None:
        # probe 자체는 의미 없고, 더 직접적으로 users 비어있는지 체크
        from database import get_conn
        with get_conn() as conn:
            cnt = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        if cnt == 0:
            print("[WEB] users 테이블이 비어 있습니다.")
            print("      python -m web.create_user <username>  로 계정을 먼저 만드세요.")

    app = create_app()

    if not os.getenv("WEB_SECRET_KEY"):
        print("[WEB] WARN: WEB_SECRET_KEY 미설정 — 임시 키 사용. "
              "재시작 시 모든 세션이 무효화됩니다. .env 에 설정 권장.")

    import uvicorn
    uvicorn.run(
        app,
        host=os.getenv("WEB_HOST", "0.0.0.0"),
        port=int(os.getenv("WEB_PORT", "8000")),
        log_level="info",
    )


if __name__ == "__main__":
    main()
