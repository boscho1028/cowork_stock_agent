"""
debug_candle.py - DB 캔들 데이터를 HTS와 비교하기 위해 출력

사용: python debug_candle.py 005930 20
      (티커, 최근 N일)
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from database import load_candles

ticker   = sys.argv[1] if len(sys.argv) > 1 else "005930"
n        = int(sys.argv[2]) if len(sys.argv) > 2 else 20

df = load_candles(ticker, "D", limit=n + 10)
print(f"\n{'날짜':<12} {'시가':>10} {'고가':>10} {'저가':>10} {'종가':>10} {'거래량':>15} {'방향'}")
print("-" * 75)
for dt, row in df.tail(n).iterrows():
    direction = "▲양봉" if row["close"] >= row["open"] else "▼음봉"
    print(f"{dt.strftime('%Y-%m-%d')}  "
          f"{row['open']:>10,.0f} {row['high']:>10,.0f} "
          f"{row['low']:>10,.0f} {row['close']:>10,.0f} "
          f"{row['volume']:>15,}  {direction}")
"""
HTS에서 확인할 값과 위 값을 비교하세요.
- 시가/고가/저가/종가가 다르면 → 데이터 문제 (수정주가 여부 등)
- 같으면 → 렌더링 문제
"""
