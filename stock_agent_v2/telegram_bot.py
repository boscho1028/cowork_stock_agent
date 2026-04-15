"""
telegram_bot.py - 텔레그램 Bot API 전송
채널 / 그룹 / 개인 모두 동작 (Chat ID만 변경)
"""
import time
import requests
from datetime import datetime


class TelegramNotifier:
    API = "https://api.telegram.org/bot{token}/sendMessage"

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

    def send_batch(self, results: list, header: str = ""):
        """종목 리스트 일괄 전송"""
        ts = datetime.now().strftime("%Y-%m-%d %H:%M")
        prefix = f"{header}\n" if header else ""
        self.send(
            f"{prefix}📡 AI 주식 분석 리포트\n"
            f"🕐 {ts}\n"
            f"총 {len(results)}종목"
        )
        for item in results:
            self.send(item["analysis"])
            time.sleep(0.5)   # 텔레그램 Rate Limit 방지

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
