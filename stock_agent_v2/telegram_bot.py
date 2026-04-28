"""
telegram_bot.py - 텔레그램 Bot API 전송
채널 / 그룹 / 개인 모두 동작 (Chat ID만 변경)
"""
import time
import requests
from datetime import datetime


class TelegramNotifier:
    API       = "https://api.telegram.org/bot{token}/sendMessage"
    PHOTO_API = "https://api.telegram.org/bot{token}/sendPhoto"

    def __init__(self, token: str, chat_id: str):
        self.token   = token
        self.chat_id = chat_id
        self.sess    = requests.Session()

    def send(self, text: str):
        """텍스트 전송 (4096자 초과 시 자동 분할)"""
        url = self.API.format(token=self.token)
        for chunk in self._split(text):
            try:
                r = self.sess.post(url, json={
                    "chat_id":                  self.chat_id,
                    "text":                     chunk,
                    "disable_web_page_preview": True,
                }, timeout=10)
                data = r.json()
                if not data.get("ok"):
                    print(f"[TG ERROR] {data.get('description', '알 수 없는 오류')}")
            except Exception as e:
                print(f"[TG ERROR] 전송 실패: {e}")
            time.sleep(0.3)

    def send_photo(self, image_bytes: bytes, caption: str = ""):
        """차트 이미지 전송 (caption 최대 1024자)"""
        url = self.PHOTO_API.format(token=self.token)
        try:
            r = self.sess.post(
                url,
                data={"chat_id": self.chat_id, "caption": caption[:1024]},
                files={"photo": ("chart.png", image_bytes, "image/png")},
                timeout=30,
            )
            data = r.json()
            if not data.get("ok"):
                print(f"[TG ERROR] 사진 전송 실패: {data.get('description', '알 수 없는 오류')}")
        except Exception as e:
            print(f"[TG ERROR] 사진 전송 실패: {e}")

    def send_batch(self, results: list, header: str = ""):
        """종목 리스트 일괄 전송
        순서 보장: 일봉(D) → 주봉(W) → 월봉(M)
        각 전송 사이 0.6초 대기 — 텔레그램 서버에서 순서가 섞이지 않도록.
        일봉 caption = AI 분석 텍스트, 주봉/월봉은 이름 캡션.
        """
        ts = datetime.now().strftime("%Y-%m-%d %H:%M")
        prefix = f"{header}\n" if header else ""
        self.send(
            f"{prefix}📡 AI 주식 분석 리포트\n"
            f"🕐 {ts}\n"
            f"총 {len(results)}종목"
        )
        time.sleep(0.5)

        for item in results:
            charts   = item.get("charts", {})
            analysis = item["analysis"]
            ticker   = item.get("ticker", "")

            # D 없으면 분석을 먼저 텍스트로
            if not charts.get("D"):
                self.send(analysis)
                time.sleep(0.5)

            # D → W → M → E 순서대로, 각 전송 후 대기로 순서 고정
            # E (엘리엇 일봉) 는 5파 검출됐을 때만 charts 에 들어 있음
            sequence = [
                ("D", analysis[:1024]),
                ("W", f"[주봉] {ticker}"),
                ("M", f"[월봉] {ticker}"),
                ("E", f"[엘리엇 일봉] {ticker}"),
            ]
            for iv, caption in sequence:
                img = charts.get(iv)
                if not img:
                    continue
                self.send_photo(img, caption=caption)
                time.sleep(0.6)

            time.sleep(0.5)

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
