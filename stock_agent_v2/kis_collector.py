"""
kis_collector.py - 국내 + 해외주식 통합 수집

[국내주식]
  엔드포인트: /uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice
  TR ID: FHKST03010100
  파라미터: FID_COND_MRKT_DIV_CODE=J, FID_PERIOD_DIV_CODE=D/W/M

[해외주식]
  엔드포인트: /uapi/overseas-price/v1/quotations/dailyprice
  TR ID: HHDFS76240000
  파라미터: AUTH, EXCD, SYMB, GUBN(0=일/1=주/2=월), BYMD, MODP=1
  주의: 해외는 1회 100개 고정, 연속 조회 불가 → BYMD로 기준일 이동
"""

import time
import requests
import pandas as pd
from datetime import datetime, timedelta
from database import upsert_candles, get_latest_candle_date, load_candles
import config

CALL_INTERVAL_REAL  = 1.0
CALL_INTERVAL_PAPER = 1.5


class KISCollector:

    REAL_URL  = "https://openapi.koreainvestment.com:9443"
    PAPER_URL = "https://openapivts.koreainvestment.com:29443"

    # 국내주식
    DOM_ENDPOINT = "/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice"
    DOM_TR_ID    = "FHKST03010100"

    # 해외주식
    OVS_ENDPOINT = "/uapi/overseas-price/v1/quotations/dailyprice"
    OVS_TR_ID    = "HHDFS76240000"
    OVS_GUBN     = {"D": "0", "W": "1", "M": "2"}   # 일/주/월

    def __init__(self):
        self.app_key    = config.KIS_APP_KEY
        self.app_secret = config.KIS_APP_SECRET
        self.is_paper   = config.KIS_PAPER_TRADING
        self.base_url   = self.PAPER_URL if self.is_paper else self.REAL_URL
        self.interval   = CALL_INTERVAL_PAPER if self.is_paper else CALL_INTERVAL_REAL
        self._token     = None
        self._token_exp = None
        self.sess       = requests.Session()
        self.sess.headers.update({"Content-Type": "application/json; charset=utf-8"})

    # ── 인증 ──────────────────────────────────────────────────────────
    def _get_token(self) -> str:
        now = datetime.now()
        if self._token and self._token_exp and now < self._token_exp:
            return self._token
        resp = self.sess.post(
            f"{self.base_url}/oauth2/tokenP",
            json={"grant_type": "client_credentials",
                  "appkey": self.app_key, "appsecret": self.app_secret},
            timeout=10,
        )
        data = resp.json()
        if "access_token" not in data:
            raise RuntimeError(f"토큰 발급 실패: {data}")
        self._token     = data["access_token"]
        self._token_exp = now + timedelta(hours=11, minutes=50)
        mode = "모의" if self.is_paper else "실전"
        print(f"  [KIS] {mode}투자 토큰 발급 (유효: {self._token_exp:%H:%M}까지)")
        return self._token

    def _dom_headers(self) -> dict:
        return {
            "authorization": f"Bearer {self._get_token()}",
            "appkey":        self.app_key,
            "appsecret":     self.app_secret,
            "tr_id":         self.DOM_TR_ID,
            "custtype":      "P",
        }

    def _ovs_headers(self) -> dict:
        return {
            "authorization": f"Bearer {self._get_token()}",
            "appkey":        self.app_key,
            "appsecret":     self.app_secret,
            "tr_id":         self.OVS_TR_ID,
            "custtype":      "P",
        }

    # ── 국내주식 1회 조회 ─────────────────────────────────────────────
    def _fetch_domestic_once(
        self, ticker: str, interval: str,
        start_date: str, end_date: str
    ) -> pd.DataFrame:
        params = {
            "FID_COND_MRKT_DIV_CODE": "J",
            "FID_INPUT_ISCD":         ticker,
            "FID_INPUT_DATE_1":       start_date,
            "FID_INPUT_DATE_2":       end_date,
            "FID_PERIOD_DIV_CODE":    interval,
            "FID_ORG_ADJ_PRC":        "0",
        }
        try:
            resp = self.sess.get(
                f"{self.base_url}{self.DOM_ENDPOINT}",
                headers=self._dom_headers(), params=params, timeout=15,
            )
            if not resp.text or not resp.text.strip():
                return pd.DataFrame()
            data = resp.json()
        except Exception as e:
            print(f"  [KIS ERROR] {ticker} {interval}: {e}")
            return pd.DataFrame()

        if data.get("rt_cd") != "0":
            msg = data.get("msg1", "")
            if "조회된 데이터가 없습니다" not in msg:
                print(f"  [KIS ERROR] {ticker} {interval}: {msg}")
            return pd.DataFrame()

        return self._parse_domestic(data.get("output2", []))

    @staticmethod
    def _parse_domestic(items: list) -> pd.DataFrame:
        rows = []
        for item in items:
            try:
                date_str = item.get("stck_bsop_date", "").strip()
                if len(date_str) != 8:
                    continue
                close = float(item.get("stck_clpr", 0) or 0)
                if close <= 0:
                    continue
                rows.append({
                    "date":   pd.to_datetime(date_str, format="%Y%m%d"),
                    "open":   float(item.get("stck_oprc", close) or close),
                    "high":   float(item.get("stck_hgpr", close) or close),
                    "low":    float(item.get("stck_lwpr", close) or close),
                    "close":  close,
                    "volume": float(item.get("acml_vol", 0) or 0),
                })
            except (ValueError, TypeError):
                continue
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows).set_index("date").sort_index()

    # ── 해외주식 1회 조회 ─────────────────────────────────────────────
    def _fetch_overseas_once(
        self, ticker: str, interval: str, bymd: str = ""
    ) -> pd.DataFrame:
        """
        해외주식 캔들 1회 조회 (최대 100개)
        bymd: 기준일 YYYYMMDD (빈 값 = 오늘 기준)
        """
        excd = config.get_excd(ticker)
        params = {
            "AUTH": "",
            "EXCD": excd,
            "SYMB": ticker,
            "GUBN": self.OVS_GUBN[interval],
            "BYMD": bymd,
            "MODP": "1",   # 1=수정주가
        }
        try:
            resp = self.sess.get(
                f"{self.base_url}{self.OVS_ENDPOINT}",
                headers=self._ovs_headers(), params=params, timeout=15,
            )
            if not resp.text or not resp.text.strip():
                return pd.DataFrame()
            data = resp.json()
        except Exception as e:
            print(f"  [KIS ERROR] {ticker} 해외 {interval}: {e}")
            return pd.DataFrame()

        if data.get("rt_cd") != "0":
            msg = data.get("msg1", "")
            if "조회된 데이터가 없습니다" not in msg:
                print(f"  [KIS ERROR] {ticker} 해외 {interval}: {msg}")
            return pd.DataFrame()

        return self._parse_overseas(data.get("output2", []))

    @staticmethod
    def _parse_overseas(items: list) -> pd.DataFrame:
        rows = []
        for item in items:
            try:
                date_str = item.get("xymd", "").strip().replace("/", "")
                if len(date_str) != 8:
                    continue
                close = float(item.get("clos", 0) or 0)
                if close <= 0:
                    continue
                rows.append({
                    "date":   pd.to_datetime(date_str, format="%Y%m%d"),
                    "open":   float(item.get("open", close) or close),
                    "high":   float(item.get("high", close) or close),
                    "low":    float(item.get("low",  close) or close),
                    "close":  close,
                    "volume": float(item.get("tvol", 0) or 0),
                })
            except (ValueError, TypeError):
                continue
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows).set_index("date").sort_index()

    # ── 전체 기간 수집 ────────────────────────────────────────────────
    def fetch_full_history(
        self, ticker: str, interval: str, from_date: str = None
    ) -> pd.DataFrame:
        overseas = config.is_overseas(ticker)
        if overseas:
            return self._fetch_overseas_history(ticker, interval, from_date)
        return self._fetch_domestic_history(ticker, interval, from_date)

    def _fetch_domestic_history(
        self, ticker: str, interval: str, from_date: str = None
    ) -> pd.DataFrame:
        start_dt  = datetime.strptime(from_date, "%Y-%m-%d") if from_date \
                    else datetime(2000, 1, 1)
        end_dt    = datetime.today()
        chunk     = {"D": 120, "W": 720, "M": 2920}[interval]
        all_frames = []

        cur = start_dt
        while cur <= end_dt:
            nxt = min(cur + timedelta(days=chunk), end_dt)
            df  = self._fetch_domestic_once(
                ticker, interval, cur.strftime("%Y%m%d"), nxt.strftime("%Y%m%d")
            )
            if not df.empty:
                all_frames.append(df)
            cur = nxt + timedelta(days=1)
            time.sleep(self.interval)

        return self._concat(all_frames)

    def _fetch_overseas_history(
        self, ticker: str, interval: str, from_date: str = None
    ) -> pd.DataFrame:
        """
        해외주식은 1회 100개 + BYMD로 기준일 이동하며 페이지네이션
        """
        cutoff = from_date or "2000-01-01"
        all_frames = []
        bymd = ""   # 첫 조회는 오늘 기준
        max_pages = 60

        for _ in range(max_pages):
            df = self._fetch_overseas_once(ticker, interval, bymd)
            if df.empty:
                break
            all_frames.append(df)
            oldest = df.index.min().strftime("%Y-%m-%d")
            if oldest <= cutoff:
                break
            # 다음 페이지: 가장 오래된 날짜 -1일
            bymd = (df.index.min() - timedelta(days=1)).strftime("%Y%m%d")
            time.sleep(self.interval)

        if not all_frames:
            return pd.DataFrame()
        result = self._concat(all_frames)
        return result[result.index >= pd.to_datetime(cutoff)]

    # ── 증분 업데이트 ─────────────────────────────────────────────────
    def fetch_incremental(
        self, ticker: str, interval: str, last_date: str
    ) -> pd.DataFrame:
        start_dt = datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)
        end_dt   = datetime.today()
        if start_dt > end_dt:
            return pd.DataFrame()

        if config.is_overseas(ticker):
            # 해외: BYMD 없이 최근 100개 조회 후 last_date 이후만 필터
            df = self._fetch_overseas_once(ticker, interval, "")
            time.sleep(self.interval)
            if df.empty:
                return df
            return df[df.index > pd.to_datetime(last_date)]
        else:
            df = self._fetch_domestic_once(
                ticker, interval,
                start_dt.strftime("%Y%m%d"),
                end_dt.strftime("%Y%m%d"),
            )
            time.sleep(self.interval)
            return df

    # ── 수정주가 감지 ─────────────────────────────────────────────────
    def check_and_fix_adjustment(self, ticker: str, interval: str) -> bool:
        db_df = load_candles(ticker, interval, limit=5)
        if db_df.empty:
            return False

        end_dt   = datetime.today()
        start_dt = end_dt - timedelta(days=30)

        if config.is_overseas(ticker):
            recent_df = self._fetch_overseas_once(ticker, interval, "")
        else:
            recent_df = self._fetch_domestic_once(
                ticker, interval,
                start_dt.strftime("%Y%m%d"),
                end_dt.strftime("%Y%m%d"),
            )
        time.sleep(self.interval)

        if recent_df.empty:
            return False

        common = db_df.index.intersection(recent_df.index)
        if len(common) == 0:
            return False

        db_close  = db_df.loc[common[-1], "close"]
        api_close = recent_df.loc[common[-1], "close"]
        diff_pct  = abs(db_close - api_close) / max(db_close, 1) * 100

        if diff_pct > 1.0:
            print(f"  [수정주가] {ticker}: DB={db_close:,.2f} "
                  f"API={api_close:,.2f} 차이={diff_pct:.1f}% → 재적재")
            df = self.fetch_full_history(ticker, interval)
            if not df.empty:
                saved = upsert_candles(df, ticker, interval, replace_all=True)
                print(f"  [수정주가] {ticker} {interval}: {saved}개 재적재 완료")
                return True
        return False

    # ── 공개 메서드 ───────────────────────────────────────────────────
    def login(self) -> bool:
        try:
            self._get_token()
            return True
        except Exception as e:
            print(f"[KIS] 로그인 실패: {e}")
            return False

    def run_initial_load(self, tickers: list, years: int = None):
        print(f"\n{'='*55}")
        print(f"KIS 초기 적재 | {len(tickers)}종목 | "
              f"{'전체 과거' if not years else f'{years}년치'}")
        print('='*55)

        if not self.login():
            print("[ERROR] 토큰 발급 실패")
            return

        for ticker in tickers:
            already = get_latest_candle_date(ticker, "D")
            if already:
                print(f"  [{ticker}] 이미 적재됨 (마지막: {already}) → SKIP")
                continue

            label_market = "해외" if config.is_overseas(ticker) else "국내"
            for interval in ("D", "W", "M"):
                label = {"D": "일봉", "W": "주봉", "M": "월봉"}[interval]
                print(f"  [{ticker}({label_market})] {label} 수집 중...",
                      end=" ", flush=True)
                try:
                    from_date = (
                        (datetime.today() - timedelta(days=365*years)).strftime("%Y-%m-%d")
                        if years else None
                    )
                    df    = self.fetch_full_history(ticker, interval, from_date)
                    saved = upsert_candles(df, ticker, interval)
                    print(f"{saved}개 저장")
                except Exception as e:
                    print(f"ERROR: {e}")

        print("\n✅ 초기 적재 완료")

    def run_daily_update(self, tickers: list):
        today = datetime.today().strftime("%Y-%m-%d")
        print(f"\n[{today}] KIS 일간 업데이트 시작")

        if not self.login():
            print("[ERROR] 토큰 발급 실패")
            return

        for ticker in tickers:
            last_d = get_latest_candle_date(ticker, "D")
            if not last_d:
                print(f"  [{ticker}] 신규 종목 → 전체 적재")
                self.run_initial_load([ticker])
                continue

            for interval in ("D", "W", "M"):
                last = get_latest_candle_date(ticker, interval)
                if last and last >= today:
                    continue
                label = {"D": "일봉", "W": "주봉", "M": "월봉"}[interval]
                print(f"  [{ticker}] {label} 증분 업데이트")
                try:
                    df = (self.fetch_incremental(ticker, interval, last)
                          if last else self.fetch_full_history(ticker, interval))
                    if not df.empty:
                        saved = upsert_candles(df, ticker, interval)
                        print(f"    → {saved}개 저장")
                    else:
                        print(f"    → 새 데이터 없음")
                except Exception as e:
                    print(f"    ERROR: {e}")

            self.check_and_fix_adjustment(ticker, "D")

        print("✅ 일간 업데이트 완료")

    @staticmethod
    def _concat(frames: list) -> pd.DataFrame:
        if not frames:
            return pd.DataFrame()
        result = pd.concat(frames).sort_index()
        return result[~result.index.duplicated(keep="last")]
