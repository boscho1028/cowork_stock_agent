"""
telegram_bot.py - 텔레그램 Bot API 전송
채널 / 그룹 / 개인 모두 동작 (Chat ID만 변경)
"""
import re
import time
import requests
from datetime import datetime


_BLOCK_MARKER_RE = re.compile(r'^={3}(MONTHLY|WEEKLY|DAILY)={3}\s*$', re.MULTILINE)


def _split_analysis(text: str) -> dict:
    """LLM 출력에서 ===MONTHLY=== / ===WEEKLY=== / ===DAILY=== 마커로
    구분된 블록을 분리. 마커가 하나라도 없으면 빈 dict 반환 (호출자가 폴백 처리).
    """
    parts = _BLOCK_MARKER_RE.split(text)
    # parts: ['<preamble>', 'MONTHLY', '<m>', 'WEEKLY', '<w>', 'DAILY', '<d>'] (순서 임의)
    if len(parts) < 7:
        return {}
    blocks = {}
    for i in range(1, len(parts) - 1, 2):
        key = parts[i]
        body = parts[i + 1].strip()
        if body:
            blocks[key] = body
    if {"MONTHLY", "WEEKLY", "DAILY"} <= blocks.keys():
        return blocks
    return {}


class TelegramNotifier:
    API       = "https://api.telegram.org/bot{token}/sendMessage"
    PHOTO_API = "https://api.telegram.org/bot{token}/sendPhoto"

    # 429(Too Many Requests) 시 retry_after 만큼 대기 후 재시도. 최대 N회.
    _MAX_RETRY = 3

    def __init__(self, token: str, chat_id: str):
        self.token   = token
        self.chat_id = chat_id
        self.sess    = requests.Session()

    # ── 공용: retry-aware POST ────────────────────────────────────────
    def _post(self, url: str, **kwargs) -> dict | None:
        """
        sendMessage / sendPhoto 공용 POST. Telegram 의 429 응답에 포함된
        parameters.retry_after 만큼 대기 후 재시도.

        반환:
          - 성공 시 ok=True 가 포함된 응답 dict
          - 모든 재시도 실패 시 마지막 에러 dict 또는 None(네트워크 실패)
        """
        last_data: dict | None = None
        for attempt in range(self._MAX_RETRY + 1):
            try:
                r = self.sess.post(url, timeout=30, **kwargs)
                last_data = r.json() if r.text else {}
            except Exception as e:
                if attempt < self._MAX_RETRY:
                    time.sleep(2)
                    continue
                print(f"[TG ERROR] 네트워크 실패: {e}")
                return None

            if last_data.get("ok"):
                return last_data

            # 429 — retry_after 만큼 기다린 뒤 재시도
            params = last_data.get("parameters") or {}
            retry_after = params.get("retry_after")
            if retry_after and attempt < self._MAX_RETRY:
                wait = int(retry_after) + 1   # 1초 여유
                print(f"[TG] rate limit, {wait}초 대기 후 재시도 "
                      f"({attempt + 1}/{self._MAX_RETRY})")
                time.sleep(wait)
                continue

            # 다른 에러이거나 재시도 한도 초과
            print(f"[TG ERROR] {last_data.get('description', '알 수 없는 오류')}")
            return last_data

        return last_data

    # ── 텍스트 ────────────────────────────────────────────────────────
    def send(self, text: str) -> bool:
        """텍스트 전송 (4096자 초과 시 자동 분할). 모든 청크 성공 시 True."""
        url = self.API.format(token=self.token)
        ok = True
        for chunk in self._split(text):
            data = self._post(url, json={
                "chat_id":                  self.chat_id,
                "text":                     chunk,
                "disable_web_page_preview": True,
            })
            if not (data and data.get("ok")):
                ok = False
            time.sleep(0.4)
        return ok

    # ── 사진 ──────────────────────────────────────────────────────────
    def send_photo(self, image_bytes: bytes, caption: str = "") -> bool:
        """차트 이미지 전송 (caption 최대 1024자). 성공 시 True."""
        url = self.PHOTO_API.format(token=self.token)
        data = self._post(
            url,
            data={"chat_id": self.chat_id, "caption": caption[:1024]},
            files={"photo": ("chart.png", image_bytes, "image/png")},
        )
        return bool(data and data.get("ok"))

    # ── 일괄 전송 ─────────────────────────────────────────────────────
    def send_batch(self, results: list, header: str = "", header_photo: bytes | None = None):
        """종목 리스트 일괄 전송.
        분석 텍스트는 ===MONTHLY=== / ===WEEKLY=== / ===DAILY=== 마커로 3분할되어
        각각 월봉·주봉·일봉 차트의 caption 으로 따로 발송된다.
        발송 순서: 일봉 → (엘리엇 일봉) → 주봉 → (엘리엇 주봉) → 월봉 → (엘리엇 월봉).
        엘리엇 차트는 검출된 인터벌만 발송됨.
        마커 분할이 실패하면 폴백: 일봉 차트에 전체 텍스트를 caption 으로 붙인다.
        header_photo: 헤더 텍스트 직후 발송할 추가 이미지(예: 수급 차트). None 이면 스킵.
        각 사진 사이 1.0초 대기 — 텔레그램 분당 한도 회피.
        """
        ts = datetime.now().strftime("%Y-%m-%d %H:%M")
        prefix = f"{header}\n" if header else ""
        self.send(
            f"{prefix}📡 AI 주식 분석 리포트\n"
            f"🕐 {ts}\n"
            f"총 {len(results)}종목"
        )
        time.sleep(0.6)

        if header_photo:
            self.send_photo(header_photo)
            time.sleep(1.0)

        for item in results:
            charts   = item.get("charts", {})
            analysis = item["analysis"]
            ticker   = item.get("ticker", "")

            blocks = _split_analysis(analysis)

            if blocks:
                # 정상 분할: D → E → W → E_W → M → E_M 순 (단기 → 큰 그림)
                m_cap = blocks["MONTHLY"]
                w_cap = blocks["WEEKLY"]
                d_cap = blocks["DAILY"]
                send_plan = [
                    ("D",   d_cap),
                    ("E",   f"[엘리엇 일봉] {ticker}"),
                    ("W",   w_cap),
                    ("E_W", f"[엘리엇 주봉] {ticker}"),
                    ("M",   m_cap),
                    ("E_M", f"[엘리엇 월봉] {ticker}"),
                ]
                d_sent_ok = False
                for iv, caption in send_plan:
                    img = charts.get(iv)
                    if not img:
                        # 차트 없는 인터벌은 caption 만이라도 텍스트로 발송 (M/W/D 만)
                        if iv in ("M", "W", "D") and caption:
                            self.send(caption)
                            time.sleep(0.6)
                            if iv == "D":
                                d_sent_ok = True
                        continue
                    ok = self.send_photo(img, caption=caption)
                    if iv == "D":
                        d_sent_ok = ok
                    time.sleep(1.0)

                # 일봉 발송 실패 → 일봉 블록 텍스트만이라도 보장
                if not d_sent_ok:
                    self.send(d_cap)
                    time.sleep(0.6)
            else:
                # 폴백: 마커 분할 실패 (에러 메시지 / LLM 형식 위반 등)
                d_img     = charts.get("D")
                d_caption = analysis[:1024]
                d_sent_ok = False
                if d_img:
                    d_sent_ok = self.send_photo(d_img, caption=d_caption)
                    time.sleep(1.0)
                if not d_sent_ok:
                    self.send(analysis)
                    time.sleep(0.6)

                for iv, caption in [
                    ("W",   f"[주봉] {ticker}"),
                    ("E_W", f"[엘리엇 주봉] {ticker}"),
                    ("M",   f"[월봉] {ticker}"),
                    ("E_M", f"[엘리엇 월봉] {ticker}"),
                    ("E",   f"[엘리엇 일봉] {ticker}"),
                ]:
                    img = charts.get(iv)
                    if not img:
                        continue
                    self.send_photo(img, caption=caption)
                    time.sleep(1.0)

            time.sleep(0.6)

    def send_error(self, msg: str):
        self.send(f"🚨 오류 알림\n{msg}\n🕐 {datetime.now().strftime('%H:%M:%S')}")

    @staticmethod
    def _split(text: str, n: int = 4096) -> list:
        chunks = []
        while len(text) > n:
            cut = text.rfind("\n", 0, n)
            chunks.append(text[:cut if cut != -1 else n])
            text = text[cut:].lstrip()
        if text:
            chunks.append(text)
        return chunks
