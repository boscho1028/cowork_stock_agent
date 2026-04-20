"""
test_chart_tg.py - 일봉/주봉/월봉 차트 생성 + 텔레그램 전송 테스트

사용법:
  python test_chart_tg.py              # portfolio 첫 번째 종목
  python test_chart_tg.py 005930       # 특정 종목
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import config
from database        import load_candles
from chart_generator import generate_chart
from telegram_bot    import TelegramNotifier

INTERVALS = [
    ("D", 400, "일봉"),
    ("W", 260, "주봉"),
    ("M",  60, "월봉"),
]


def main():
    ticker = sys.argv[1] if len(sys.argv) > 1 else config.get_portfolio()[0]
    info   = config.get_portfolio_detail().get(ticker, {})
    name   = info.get("name", ticker)

    notifier = TelegramNotifier(config.TELEGRAM_BOT_TOKEN, config.TELEGRAM_CHAT_ID)
    charts   = {}

    for interval, limit, label in INTERVALS:
        print(f"[TEST] {ticker} {label} 차트 생성 중...")
        df = load_candles(ticker, interval, limit=limit)
        if df.empty:
            print(f"  [SKIP] {label} 데이터 없음")
            continue

        image_bytes = generate_chart(df, ticker, name, config.INDICATOR_CONFIG, interval=interval)
        charts[interval] = image_bytes
        print(f"  생성 완료 ({len(image_bytes):,} bytes)")

        # 로컬 저장
        out = os.path.join(os.path.dirname(__file__), f"test_chart_{ticker}_{interval}.png")
        with open(out, "wb") as f:
            f.write(image_bytes)
        print(f"  로컬 저장: {out}")

    if not config.TELEGRAM_BOT_TOKEN or not config.TELEGRAM_CHAT_ID:
        print("[SKIP] 텔레그램 미설정")
        return

    # 텔레그램 전송: 일봉(분석 텍스트 포함) → 주봉 → 월봉
    dummy_result = [{"ticker": ticker, "analysis": f"[TEST] {name}({ticker}) 차트 테스트", "charts": charts}]
    notifier.send_batch(dummy_result, header="[TEST]")
    print("텔레그램 전송 완료")


if __name__ == "__main__":
    main()
