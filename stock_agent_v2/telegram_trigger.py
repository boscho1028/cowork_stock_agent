"""
telegram_trigger.py - 텔레그램 봇 명령어로 분석 트리거
항상 백그라운드에서 실행하며 명령어를 대기

명령어 목록:
  /analyze      전체 포트폴리오 분석
  /single XXXX  단일 종목 분석 (예: /single 005930)
  /update       universe 전체 가격 업데이트 (분석·전송 없음, 10~20분 소요)
  /signals      universe 시그널 스캔 (규칙 기반, 빠름)
  /dart         DART/SEC 공시만 확인
  /weekly       주간 리포트
  /add          포트폴리오 추가, universe 자동 포함 (/add 종목 이름 거래소 수량)
  /remove       포트폴리오에서 제거, universe 유지 (/remove 종목)
  /watch        universe 관찰 종목 추가 (/watch 종목 이름 거래소)
  /unwatch      universe 에서 제거, portfolio 에 있으면 차단 (/unwatch 종목)
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

def run_script(chat_id: int, script: str, args: list = None, task_name: str = "분석",
               timeout: int | None = 600):
    """
    Python 스크립트를 백그라운드 스레드에서 실행
    시작 알림 → 실행 → 완료/오류 알림

    timeout: 초 단위 타임아웃. None 이면 무제한 (universe 업데이트처럼
             장시간 작업에 사용).
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
                timeout=timeout,
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
            mins = (timeout or 0) // 60
            send_message(chat_id, f"⏱️ [{task_name}] 시간 초과 ({mins}분)")
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


# ── CSV 편집 헬퍼 ─────────────────────────────────────────────────────

VALID_EXCHANGES = {"KRX", "KOSPI", "KOSDAQ", "NASDAQ", "NYSE", "AMEX",
                   "HKEX", "TSE", "SHANGHAI", "SHENZHEN"}


def _append_csv_row(path: Path, header: list, row: dict) -> tuple:
    """
    CSV 에 한 줄 추가. (created_new: bool, already_exists: bool) 반환.
    중복 티커면 추가 안 하고 (False, True) 반환.
    """
    import csv
    created_new = not path.exists()

    # 중복 검사 (대소문자 무시)
    ticker_u = row["ticker"].upper()
    if not created_new:
        with open(path, encoding="utf-8-sig", newline="") as f:
            for r in csv.DictReader(f):
                if (r.get("ticker", "") or "").strip().upper() == ticker_u:
                    return False, True

    # append (신규 파일이면 헤더도 기록)
    mode = "w" if created_new else "a"
    with open(path, mode, encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=header)
        if created_new:
            w.writeheader()
        w.writerow(row)
    return True, False


def _remove_csv_row(path: Path, ticker: str) -> dict | None:
    """
    CSV 에서 ticker 행 삭제. 삭제된 행(dict) 반환, 없으면 None.
    헤더와 나머지 행 유지하며 원자적 재작성 (.tmp → rename).
    """
    import csv
    if not path.exists():
        return None

    ticker_u = ticker.upper()
    removed  = None
    kept     = []
    with open(path, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames or []
        for r in reader:
            if (r.get("ticker", "") or "").strip().upper() == ticker_u:
                removed = r
            else:
                kept.append(r)

    if removed is None:
        return None

    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        w.writerows(kept)
    tmp.replace(path)
    return removed


def _csv_contains_ticker(path: Path, ticker: str) -> bool:
    import csv
    if not path.exists():
        return False
    ticker_u = ticker.upper()
    with open(path, encoding="utf-8-sig", newline="") as f:
        for r in csv.DictReader(f):
            if (r.get("ticker", "") or "").strip().upper() == ticker_u:
                return True
    return False


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

    # 전체 분석 — 모닝 브리핑(US) + 저녁 분석(KR) 순차 실행
    if cmd in ("/analyze", "/분석"):
        run_script(chat_id, "run_daily.py", args=["full"], task_name="전체분석")

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

        # 처음 보는 종목은 KIS 5년치 풀 적재가 필요해 10분을 넘길 수 있어 무제한.
        run_script(chat_id, "run_single.py", args=[ticker],
                   task_name=f"{ticker}분석", timeout=None)

    # 공시 확인
    elif cmd in ("/dart", "/공시"):
        run_script(chat_id, "run_dart.py", task_name="공시확인")

    # 시장 경고 브리핑 (F&G + 시세 + 신용/AI 뉴스). 매일 07:25 자동 + 온디맨드.
    elif cmd in ("/warning", "/경고", "/market"):
        run_script(chat_id, "run_market_warning.py", task_name="시장경고")

    # 외국인·기관 수급 동향 (DB 읽기 · 저녁 배치가 일 1회 갱신)
    elif cmd in ("/supply", "/수급"):
        from main import _build_supply_summary, _split_portfolio
        if not args:
            _, kr = _split_portfolio()
            if not kr:
                send_message(chat_id, "[수급] 국내 포트폴리오 없음")
                return
            out = _build_supply_summary(kr)
            send_message(chat_id, out or "[수급] 데이터 없음")
        else:
            ticker = args[0].upper()
            out = _build_supply_summary([ticker])
            if out:
                send_message(chat_id, out)
            else:
                send_message(chat_id,
                    f"[수급] {ticker} 매매동향 데이터 없음\n"
                    "universe 등록된 국내 종목만 수집됩니다.\n"
                    "· portfolio/universe 밖 종목이면 /watch 로 추가 후 저녁 배치(17:00) 이후 조회\n"
                    "· 해외 종목(US 등)은 수집 대상 아님")

    # universe 전체 업데이트 (분석·전송 없음)
    # 종목 수에 따라 30분 이상 걸릴 수 있어 타임아웃 없이 끝까지 실행.
    elif cmd in ("/update", "/업데이트"):
        send_message(chat_id,
            "[UPDATE] universe 전체 업데이트 시작\n"
            "(최근 5봉 재조회 포함, 10~30분 소요)")
        run_script(chat_id, "run_update_universe.py",
                   task_name="universe업데이트", timeout=None)

    # 포트폴리오 종목 추가 (portfolio.csv)
    elif cmd in ("/add", "/추가"):
        if len(args) < 4:
            send_message(chat_id,
                "[사용법] /add 종목코드 이름 거래소 수량\n\n"
                "예) /add 005930 삼성전자 KRX 100\n"
                "예) /add NVDA NVIDIA NASDAQ 5\n"
                "예) /add 9988 알리바바 HKEX 10\n\n"
                f"거래소: {' '.join(sorted(VALID_EXCHANGES))}")
            return
        ticker   = args[0].upper()
        name     = args[1]
        exchange = args[2].upper()
        qty_raw  = args[3]
        if exchange not in VALID_EXCHANGES:
            send_message(chat_id,
                f"[ERROR] 알 수 없는 거래소: {exchange}\n"
                f"유효한 값: {' '.join(sorted(VALID_EXCHANGES))}")
            return
        try:
            qty = int(qty_raw)
        except ValueError:
            send_message(chat_id, f"[ERROR] 수량은 정수여야 합니다: {qty_raw}")
            return
        try:
            _, pf_exists = _append_csv_row(
                BASE_DIR / "portfolio.csv",
                header=["ticker", "name", "quantity", "exchange"],
                row={"ticker": ticker, "name": name, "quantity": qty, "exchange": exchange},
            )
            # portfolio ⊂ universe 불변식: universe 에도 자동 추가
            _, uv_exists = _append_csv_row(
                BASE_DIR / "universe.csv",
                header=["ticker", "name", "exchange"],
                row={"ticker": ticker, "name": name, "exchange": exchange},
            )
            uv_note = "universe 에 이미 존재" if uv_exists else "universe 에도 자동 추가"
            if pf_exists:
                send_message(chat_id,
                    f"[WARN] {ticker} 는 이미 portfolio.csv 에 있습니다.\n"
                    f"({uv_note})\n"
                    f"수정이 필요하면 CSV 를 직접 편집하세요.")
            else:
                send_message(chat_id,
                    f"[OK] portfolio 추가 완료\n"
                    f"  {ticker}  {name}  {exchange}  {qty}주\n"
                    f"  ({uv_note})\n\n"
                    f"다음 /analyze 시 자동으로 데이터 수집·분석됩니다.")
        except Exception as e:
            send_message(chat_id, f"[ERROR] 추가 실패: {e}")

    # universe 관찰 종목 추가 (universe.csv)
    elif cmd in ("/watch", "/관찰"):
        if len(args) < 3:
            send_message(chat_id,
                "[사용법] /watch 종목코드 이름 거래소\n\n"
                "예) /watch 035720 카카오 KRX\n"
                "예) /watch TSLA Tesla NASDAQ\n"
                "예) /watch 9988 알리바바 HKEX\n\n"
                f"거래소: {' '.join(sorted(VALID_EXCHANGES))}")
            return
        ticker   = args[0].upper()
        name     = args[1]
        exchange = args[2].upper()
        if exchange not in VALID_EXCHANGES:
            send_message(chat_id,
                f"[ERROR] 알 수 없는 거래소: {exchange}\n"
                f"유효한 값: {' '.join(sorted(VALID_EXCHANGES))}")
            return
        try:
            _, exists = _append_csv_row(
                BASE_DIR / "universe.csv",
                header=["ticker", "name", "exchange"],
                row={"ticker": ticker, "name": name, "exchange": exchange},
            )
            if exists:
                send_message(chat_id,
                    f"[WARN] {ticker} 는 이미 universe.csv 에 있습니다.")
            else:
                send_message(chat_id,
                    f"[OK] universe 추가 완료\n"
                    f"  {ticker}  {name}  {exchange}\n\n"
                    f"다음 /update 또는 /signals 시 데이터 수집됩니다.")
        except Exception as e:
            send_message(chat_id, f"[ERROR] 추가 실패: {e}")

    # 포트폴리오 종목 제거 (portfolio.csv)
    elif cmd in ("/remove", "/삭제"):
        if not args:
            send_message(chat_id,
                "[사용법] /remove 종목코드\n\n"
                "예) /remove 005930\n"
                "예) /remove NVDA\n\n"
                "※ universe 에서도 빼려면 이후 /unwatch 종목코드")
            return
        ticker = args[0].upper()
        try:
            removed = _remove_csv_row(BASE_DIR / "portfolio.csv", ticker)
            if not removed:
                send_message(chat_id, f"[WARN] {ticker} 는 portfolio.csv 에 없습니다.")
            else:
                still_watched = _csv_contains_ticker(BASE_DIR / "universe.csv", ticker)
                uv_note = "universe 에는 남아있음 (관찰 계속)" if still_watched \
                          else "universe 에도 없음"
                send_message(chat_id,
                    f"[OK] portfolio 제거 완료\n"
                    f"  {ticker}  {removed.get('name','')}  "
                    f"{removed.get('exchange','')}  {removed.get('quantity','')}주\n"
                    f"  ({uv_note})")
        except Exception as e:
            send_message(chat_id, f"[ERROR] 제거 실패: {e}")

    # universe 관찰 종목 제거 (universe.csv)
    elif cmd in ("/unwatch", "/관찰해제"):
        if not args:
            send_message(chat_id,
                "[사용법] /unwatch 종목코드\n\n"
                "예) /unwatch 035720\n"
                "예) /unwatch TSLA")
            return
        ticker = args[0].upper()
        try:
            # 포트폴리오에 있으면 먼저 경고·중단 (portfolio ⊂ universe 불변식)
            if _csv_contains_ticker(BASE_DIR / "portfolio.csv", ticker):
                send_message(chat_id,
                    f"[BLOCK] {ticker} 는 portfolio.csv 에 있어 universe 에서 뺄 수 없습니다.\n"
                    f"먼저 /remove {ticker} 로 portfolio 에서 제거하세요.")
                return
            removed = _remove_csv_row(BASE_DIR / "universe.csv", ticker)
            if not removed:
                send_message(chat_id, f"[WARN] {ticker} 는 universe.csv 에 없습니다.")
            else:
                send_message(chat_id,
                    f"[OK] universe 제거 완료\n"
                    f"  {ticker}  {removed.get('name','')}  {removed.get('exchange','')}")
        except Exception as e:
            send_message(chat_id, f"[ERROR] 제거 실패: {e}")

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
            "   전체 배치 (모닝브리핑+저녁분석 순차, 5~8분 소요)\n\n"
            "[START] /single [종목코드]\n"
            "   단일 종목 즉시 분석\n"
            "   예) /single 005930\n"
            "   예) /single NVDA\n\n"
            "[START] /dart\n"
            "   DART + SEC 공시 확인\n\n"
            "[START] /supply [종목]\n"
            "   외국인·기관 매매동향 (최근 3영업일 + 한달)\n"
            "   · 인자 없음: 국내 포트폴리오 전체\n"
            "   · 종목코드: 해당 종목만 (예: /supply 005930)\n\n"
            "[START] /update\n"
            "   universe 전체 가격 업데이트\n"
            "   (분석·전송 없음, 10~20분 소요)\n\n"
            "[START] /add 종목 이름 거래소 수량\n"
            "   포트폴리오 추가 (universe 자동 포함)\n"
            "   예) /add 005930 삼성전자 KRX 100\n\n"
            "[START] /remove 종목\n"
            "   포트폴리오에서 제거 (universe 유지)\n"
            "   예) /remove 005930\n\n"
            "[START] /watch 종목 이름 거래소\n"
            "   universe 관찰 종목 추가\n"
            "   예) /watch TSLA Tesla NASDAQ\n\n"
            "[START] /unwatch 종목\n"
            "   universe 에서 제거 (portfolio 에 있으면 차단)\n"
            "   예) /unwatch TSLA\n\n"
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

# Telegram "/" 자동완성 메뉴에 노출할 명령어 (setMyCommands)
BOT_COMMANDS = [
    ("analyze",   "전체 포트폴리오 분석"),
    ("single",    "단일 종목 즉시 분석 (/single 005930)"),
    ("signals",   "시그널 스캔 (/signals [portfolio|종목])"),
    ("dart",      "DART/SEC 공시 확인"),
    ("warning",   "시장 경고 브리핑 (F&G + 시세 + 신용/AI 뉴스)"),
    ("supply",    "외국인·기관 매매동향 (/supply [종목])"),
    ("update",    "universe 전체 가격 업데이트"),
    ("weekly",    "주간 전략 리포트"),
    ("portfolio", "보유 종목 목록"),
    ("add",       "포트폴리오 추가 (/add 종목 이름 거래소 수량)"),
    ("remove",    "포트폴리오에서 제거 (/remove 종목)"),
    ("watch",     "universe 관찰 추가 (/watch 종목 이름 거래소)"),
    ("unwatch",   "universe 에서 제거 (/unwatch 종목)"),
    ("status",    "현재 실행 상태"),
    ("help",      "도움말"),
]


def _register_bot_commands():
    """setMyCommands 로 텔레그램 슬래시 메뉴를 최신 명령어 목록으로 갱신."""
    try:
        resp = requests.post(
            f"{API_URL}/setMyCommands",
            json={"commands": [{"command": c, "description": d}
                               for c, d in BOT_COMMANDS]},
            timeout=10,
        )
        if resp.json().get("ok"):
            print(f"  명령어 메뉴: {len(BOT_COMMANDS)}개 등록 완료")
        else:
            print(f"  [WARN] setMyCommands 응답: {resp.text[:200]}")
    except Exception as e:
        print(f"  [WARN] 명령어 메뉴 등록 실패 (봇은 계속 동작): {e}")


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
        print(f"  봇: @{bot_name}")
    except Exception:
        pass

    _register_bot_commands()
    print()

    offset = 0
    while True:
        try:
            updates = get_updates(offset)
            for update in updates:
                offset = update["update_id"] + 1
                msg    = update.get("message") or update.get("edited_message")
                if not msg:
                    continue
                text = msg.get("text", "") or ""
                # 슬래시 없이 "help" / "도움말" / "메뉴" 입력해도 /help 로 받아들임
                if text.strip().lower() in ("help", "도움말", "메뉴", "menu"):
                    text = "/help"
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
