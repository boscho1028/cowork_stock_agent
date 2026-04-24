"""
investor_collector.py — 외국인·기관 매매동향 수집 (KIS inquire-investor)

KIS API 엔드포인트:
  /uapi/domestic-stock/v1/quotations/inquire-investor
  TR ID: FHKST01010900

한 번 호출하면 해당 종목 최근 **30영업일** 투자자별 매매 데이터가 한꺼번에
돌아온다. 따라서 incremental window 개념 없이 매일 재호출 + upsert (기본키
(ticker, trade_date) 충돌 시 덮어쓰기). 30일 이전 역사는 DB에 보존.

단위: KIS `*_tr_pbmn` 필드는 관례상 **백만원**. 출력 시 환산.
"""

import time
import requests

import config
from kis_collector import KISCollector, CALL_INTERVAL_REAL, CALL_INTERVAL_PAPER
from database import upsert_investor_trend


_ENDPOINT = "/uapi/domestic-stock/v1/quotations/inquire-investor"
_TR_ID    = "FHKST01010900"


def _to_int(s: str) -> int:
    """KIS 가 빈 문자열/부호 문자열을 섞어 보냄. 안전 파싱."""
    if not s:
        return 0
    try:
        return int(str(s).replace(",", ""))
    except ValueError:
        return 0


class InvestorCollector:
    """외국인·기관 순매수 수집기. KIS 토큰·세션은 KISCollector 와 공유."""

    def __init__(self):
        self.kis      = KISCollector()
        self.interval = (CALL_INTERVAL_PAPER if self.kis.is_paper
                         else CALL_INTERVAL_REAL)

    def _headers(self, token: str) -> dict:
        return {
            "authorization": f"Bearer {token}",
            "appkey":        self.kis.app_key,
            "appsecret":     self.kis.app_secret,
            "tr_id":         _TR_ID,
            "custtype":      "P",
        }

    def fetch_one(self, ticker: str) -> list:
        """단일 종목 최근 30영업일 (ticker, date, foreign, inst) 행 리스트."""
        try:
            token = self.kis._get_token()
            resp  = self.kis.sess.get(
                f"{self.kis.base_url}{_ENDPOINT}",
                headers=self._headers(token),
                params={"FID_COND_MRKT_DIV_CODE": "J",
                        "FID_INPUT_ISCD":         ticker},
                timeout=15,
            )
            data = resp.json()
        except Exception as e:
            print(f"  [INVESTOR ERROR] {ticker}: {e}")
            return []

        if data.get("rt_cd") != "0":
            msg = data.get("msg1", "")
            if "조회된 데이터가 없습니다" not in msg:
                print(f"  [INVESTOR ERROR] {ticker}: {msg}")
            return []

        out_rows: list = []
        for item in data.get("output", []):
            raw_date = (item.get("stck_bsop_date") or "").strip()
            if len(raw_date) != 8:
                continue
            trade_date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:]}"
            foreign    = _to_int(item.get("frgn_ntby_tr_pbmn"))
            inst       = _to_int(item.get("orgn_ntby_tr_pbmn"))
            # 장중 · 데이터 없는 행(둘 다 0 이고 비어있던 경우) 는 skip —
            # 나중에 확정값이 들어오면 upsert 로 덮어씀
            if foreign == 0 and inst == 0 and not item.get("frgn_ntby_tr_pbmn"):
                continue
            out_rows.append((ticker, trade_date, foreign, inst))
        return out_rows

    def fetch_all_tickers(self, tickers: list) -> int:
        """universe 등 다수 종목 수집. 반환: 저장된 행 수.

        모든 KIS 호출이 끝난 뒤 단 한 번의 bulk upsert 로 DB 에 저장.
        Turso(원격) 는 connection reset 가능성이 있어 종목별 개별 write 대신
        batch 로 줄이고, 전체 bulk upsert 는 3회까지 재시도.
        """
        if not tickers:
            return 0
        print(f"\n[INVESTOR] {len(tickers)}종목 수급 수집 시작")
        if not self.kis.login():
            print("  [INVESTOR] KIS 로그인 실패 — 스킵")
            return 0

        all_rows: list = []
        for t in tickers:
            rows = self.fetch_one(t)
            if rows:
                all_rows.extend(rows)
            time.sleep(self.interval)

        if not all_rows:
            print("[INVESTOR] 수집 데이터 없음")
            return 0

        for attempt in range(3):
            try:
                total = upsert_investor_trend(all_rows)
                print(f"[INVESTOR] {total}행 저장 완료 ({len(tickers)}종목)")
                return total
            except Exception as e:
                if attempt < 2:
                    print(f"  [INVESTOR DB] 재시도 {attempt+1}/3: {e}")
                    time.sleep(5)
                else:
                    print(f"  [INVESTOR DB] 저장 실패 — 다음 배치로 보류: {e}")
                    return 0
        return 0
