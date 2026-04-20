"""
telegram_trigger.py - 텔레그램 봇 명령어로 분석 트리거
항상 백그라운드에서 실행하며 명령어를 대기

명령어 목록:
  /analyze      전체 포트폴리오 분석
  /single XXXX  단일 종목 분석 (예: /single 005930)
  /signals      universe 시그널 스캔 (규칙 기반, 빠름)
  /dart         DART/SEC 공시만 확인
  /weekly       주간 리포트
  /status       현재 실행 상태 확인
  /portfolio    보유 종목 목록
  /help         도움말

실행 방법:
  python telegram_trigger.py          # 포그라운드 (테스트용)
  pythonw telegram_trigger.py         # 백그라운드 (Windows, 창 없음)
"""

import os
import sys
import subprocess
import threading
import requests
import time
from datetime import datetime
from pathlib import Path

# ── 설정 ─────────────────────────────────────────────────────────────
BASE_DIR   = Path(__file__).parent
PYTHON_EXE = str(BASE_DIR / "venv_kis" / "Scripts" / "python.exe")

# .env 로드
try:
    from dotenv import load_dotenv
    load_dotenv(BASE_DIR / ".env")
except ImportError:
    pass

BOT_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN", "")
ALLOWED_IDS = os.getenv("TELEGRAM_ALLOWED_IDS", "")   # 허용할 chat_id (쉼표 구분)
API_URL     = f"https://api.telegram.org/bot{BOT_TOKEN}"

# 허용된 사용자 ID 파싱 (비어있으면 전체 허용)
ALLOWED_SET: set = set()
if ALLOWED_IDS:
    for id_str in ALLOWED_IDS.split(","):
        try:
            ALLOWED_SET.add(int(id_str.strip()))
        except ValueError:
            pass

# 현재 실행 중인 작업 추적
_running: dict = {}
_lock = threading.Lock()


# ── 텔레그램 API ──────────────────────────────────────────────────────

def send_message(chat_id: int, text: str):
    try:
        requests.post(f"{API_URL}/sendMessage", json={
            "chat_id":                  chat_id,
            "text":                     text[:4096],
            "disable_web_page_preview": True,
        }, timeout=10)
    except Exception as e:
        print(f"[TG] 전송 실패: {e}")


def get_updates(offset: int = 0) -> list:
    try:
        resp = requests.get(f"{API_URL}/getUpdates", params={
            "offset":  offset,
            "timeout": 30,
        }, timeout=35)
        return resp.json().get("result", [])
    except Exception:
        return []


# ── 스크립트 실행 (백그라운드 스레드) ────────────────────────────────

def run_script(chat_id: int, script: str, args: list = None, task_name: str = "분석"):
    """
    Python 스크립트를 백그라운드 스레드에서 실행
    시작 알림 → 실행 → 완료/오류 알림
    """
    with _lock:
        if task_name in _running:
            send_message(chat_id,
                f"[WARN] [{task_name}] 이미 실행 중입니다.\n"
                f"완료 후 다시 시도하세요.")
            return
        _running[task_name] = True

    def _run():
        cmd   = [PYTHON_EXE, str(BASE_DIR / script)] + (args or [])
        start = datetime.now()
        # Windows cp949 인코딩 오류 방지: UTF-8 강제 설정
        env = os.environ.copy()
        env["PYTHONIOENCODING"] = "utf-8"
        env["PYTHONUTF8"]       = "1"

        send_message(chat_id,
            f"[START] [{task_name}] 시작\n"
            f"🕐 {start.strftime('%H:%M:%S')}\n"
            f"완료되면 텔레그램 채널로 결과 전송됩니다.")

        try:
            result = subprocess.run(
                cmd,
                cwd=str(BASE_DIR),
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=600,
                env=env,
            )
            elapsed      = (datetime.now() - start).seconds
            mins, secs   = divmod(elapsed, 60)
            stdout_lines = [l for l in result.stdout.strip().split("\n") if l.strip()]
            summary      = "\n".join(stdout_lines[-5:]) if stdout_lines else "출력 없음"

            if result.returncode == 0:
                send_message(chat_id,
                    f"[OK] [{task_name}] 완료 ({mins}분 {secs}초)\n"
                    f"───────────\n{summary}")
            else:
                err = (result.stderr or "").strip()[-400:]
                send_message(chat_id,
                    f"[ERROR] [{task_name}] 실패 ({mins}분 {secs}초)\n"
                    f"───────────\n{err or summary}")

        except subprocess.TimeoutExpired:
            send_message(chat_id, f"⏱️ [{task_name}] 시간 초과 (10분)")
        except FileNotFoundError:
            send_message(chat_id,
                f"[ERROR] Python 실행 파일을 찾을 수 없습니다.\n"
                f"경로 확인: {PYTHON_EXE}")
        except Exception as e:
            send_message(chat_id, f"[ERROR] [{task_name}] 오류: {e}")
        finally:
            with _lock:
                _running.pop(task_name, None)

    threading.Thread(target=_run, daemon=True).start()


# ── 명령어 처리 ───────────────────────────────────────────────────────

def handle_command(chat_id: int, text: str, user_name: str):

    # 권한 체크
    if ALLOWED_SET and chat_id not in ALLOWED_SET:
        send_message(chat_id,
            f"⛔ 권한이 없습니다.\n"
            f"내 chat_id: {chat_id}\n"
            f".env 의 TELEGRAM_ALLOWED_IDS 에 추가 후 봇을 재시작하세요.")
        return

    parts = text.strip().split()
    cmd   = parts[0].lower().split("@")[0]
    args  = parts[1:]

    print(f"[{datetime.now():%H:%M:%S}] @{user_name}({chat_id}): {text}")

    # 전체 분석
    if cmd in ("/analyze", "/분석"):
        run_script(chat_id, "run_daily.py", task_name="전체분석")

    # 단일 종목 (포트폴리오 외 종목도 가능)
    elif cmd in ("/single", "/종목"):
        if not args:
            send_message(chat_id,
                "[사용법] /single 종목코드 [거래소]\n\n"
                "포트폴리오 종목:\n"
                "/single 005930       삼성전자\n"
                "/single NVDA         엔비디아\n\n"
                "포트폴리오 외 종목:\n"
                "/single 035720       카카오 (국내 자동인식)\n"
                "/single TSLA         테슬라 (해외 자동인식)\n"
                "/single 9988 HKEX    알리바바 (거래소 직접 지정)\n\n"
                "거래소: NASDAQ NYSE KRX HKEX TSE")
            return

        ticker   = args[0].upper()
        exchange = args[1].upper() if len(args) > 1 else None

        # 포트폴리오에 없는 종목이면 임시 등록
        import config as _cfg
        if ticker not in _cfg.get_portfolio_detail():
            # 거래소 자동 판단 또는 직접 지정
            _cfg.register_temp(ticker, exchange=exchange)
            exch_label = exchange or ("KRX" if ticker.isdigit() else "NASDAQ")
            send_message(chat_id,
                f"[{ticker}] 포트폴리오 외 종목 — {exch_label} 기준으로 조회합니다.\n"
                f"(처음 조회 시 데이터 수집으로 3~5분 소요)")

        run_script(chat_id, "run_single.py", args=[ticker],
                   task_name=f"{ticker}분석")

    # 공시 확인
    elif cmd in ("/dart", "/공시"):
        run_script(chat_id, "run_dart.py", task_name="공시확인")

    # 시그널 스캔
    #  /signals             → universe 전체
    #  /signals portfolio   → portfolio 종목만
    #  /signals TICKER      → 해당 종목만
    elif cmd in ("/signals", "/시그널"):
        if args:
            arg0  = args[0]
            lower = arg0.lower()
            if lower in ("portfolio", "universe"):
                pass_arg = lower
                task     = f"{lower}시그널"
            else:
                pass_arg = arg0.upper()
                task     = f"{pass_arg}시그널"
            run_script(chat_id, "run_signals.py", args=[pass_arg], task_name=task)
        else:
            run_script(chat_id, "run_signals.py", task_name="시그널스캔")

    # 주간 리포트
    elif cmd in ("/weekly", "/주간"):
        run_script(chat_id, "run_weekly.py", task_name="주간리포트")

    # 실행 상태
    elif cmd in ("/status", "/상태"):
        with _lock:
            running_list = list(_running.keys())
        if running_list:
            send_message(chat_id,
                f"⚙️ 현재 실행 중:\n" +
                "\n".join(f"  · {t}" for t in running_list))
        else:
            send_message(chat_id, "[OK] 현재 실행 중인 작업 없음")

    # 포트폴리오 목록
    elif cmd in ("/portfolio", "/포트"):
        try:
            import csv
            rows = []
            csv_path = BASE_DIR / "portfolio.csv"
            with open(csv_path, encoding="utf-8-sig") as f:
                for r in csv.DictReader(f):
                    rows.append(
                        f"{r['ticker']:8s} {r['name']:12s} "
                        f"{r['quantity']}주  [{r['exchange']}]"
                    )
            send_message(chat_id,
                "[DART] 현재 포트폴리오\n"
                "─────────────────\n" +
                "\n".join(rows))
        except Exception as e:
            send_message(chat_id, f"[ERROR] 포트폴리오 조회 실패: {e}")

    # 도움말
    elif cmd in ("/help", "/도움말", "/start"):
        send_message(chat_id,
            "[REPORT] 주식 AI 분석 봇\n"
            "━━━━━━━━━━━━━━━━━━\n"
            "[START] /analyze\n"
            "   전체 포트폴리오 분석\n\n"
            "[START] /single [종목코드]\n"
            "   단일 종목 즉시 분석\n"
            "   예) /single 005930\n"
            "   예) /single NVDA\n\n"
            "[START] /dart\n"
            "   DART + SEC 공시 확인\n\n"
            "[START] /signals [대상]\n"
            "   시그널 스캔 (업데이트 + 규칙 기반)\n"
            "   · 인자 없음    : universe 전체\n"
            "   · portfolio    : 보유 종목만\n"
            "   · 종목코드     : 해당 종목만\n\n"
            "[START] /weekly\n"
            "   주간 전략 리포트\n\n"
            "[START] /portfolio\n"
            "   보유 종목 목록\n\n"
            "[START] /status\n"
            "   현재 실행 상태\n"
            "━━━━━━━━━━━━━━━━━━\n"
            "📡 결과는 텔레그램 채널로 전송")

    else:
        send_message(chat_id,
            f"❓ 알 수 없는 명령어: {cmd}\n"
            "/help 로 명령어 목록 확인")


# ── 메인 루프 ─────────────────────────────────────────────────────────

def main():
    if not BOT_TOKEN:
        print("[ERROR] TELEGRAM_BOT_TOKEN 이 설정되지 않았습니다.")
        print("  .env 파일에 TELEGRAM_BOT_TOKEN 을 추가하세요.")
        sys.exit(1)

    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] 텔레그램 봇 시작")
    print(f"  허용 ID: {ALLOWED_SET if ALLOWED_SET else '전체 허용 ([WARN] 보안 주의)'}")
    print(f"  Python: {PYTHON_EXE}")
    print(f"  작업 폴더: {BASE_DIR}")
    print("  Ctrl+C 로 종료\n")

    # 봇 정보 확인
    try:
        me       = requests.get(f"{API_URL}/getMe", timeout=5).json()
        bot_name = me.get("result", {}).get("username", "unknown")
        print(f"  봇: @{bot_name}\n")
    except Exception:
        pass

    offset = 0
    while True:
        try:
            updates = get_updates(offset)
            for update in updates:
                offset = update["update_id"] + 1
                msg    = update.get("message") or update.get("edited_message")
                if not msg:
                    continue
                text = msg.get("text", "")
                if not text or not text.startswith("/"):
                    continue
                chat_id   = msg["chat"]["id"]
                user_name = msg.get("from", {}).get("username", "unknown")
                handle_command(chat_id, text, user_name)

        except KeyboardInterrupt:
            print("\n봇 종료")
            break
        except Exception as e:
            print(f"[ERROR] {e}")
            time.sleep(5)


if __name__ == "__main__":
    main()
