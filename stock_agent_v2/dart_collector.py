"""
dart_collector.py - DART OpenAPI 특별 공시 수집
- 2단계 필터: 유형 코드(D/A/B/C/K) + 키워드 필터
- T-1 전 영업일 공시 중심
API 키 발급: https://opendart.fss.or.kr
"""

import io
import json
import time
import zipfile
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from pathlib import Path
from database import upsert_disclosures, upsert_report, load_disclosures
import config

# stock_code → corp_code 매핑 캐시. DART 는 공시 조회 시 corp_code(8자리 고유번호)
# 를 요구하는데 종목코드(6자리 stock_code)로부터 역매핑 endpoint 가 없다.
# 전체 상장사 매핑이 들어 있는 corpCode.xml 을 1회 받아 로컬에 캐시한다.
_CORP_CODE_CACHE = Path(__file__).parent / "data" / "dart_corp_codes.json"
_CORP_CODE_TTL_DAYS = 7


# ── 수집 대상 공시 유형 (DART pblntf_ty 공식 코드) ────────────────────
#   A: 정기공시   (사업·반기·분기보고서)
#   B: 주요사항보고 (자사주·배당·M&A·증자 등 대부분 중요)
#   C: 발행공시   (증권신고서 등)
#   D: 지분공시   (5%·임원주요주주 등)
#   I: 거래소공시 (공정공시·잠정실적·주주총회 등)
# A·B 는 전부 수집, C/D/I 는 키워드 매칭된 것만 (양이 많고 잡다함).
SPECIAL_PBOARD_TYPES   = ("A", "B", "C", "D", "I")
_FILTER_REQUIRED_TYPES = ("C", "D", "I")

# 중요 키워드 (C/D/I 유형에만 적용)
IMPORTANT_KEYWORDS = [
    # 자본 변동
    "유상증자", "무상증자", "전환사채", "신주인수권", "교환사채",
    "주식분할", "주식병합", "감자",
    # 자사주
    "자기주식", "자사주",
    # M&A / 구조변경
    "합병", "분할", "영업양수", "영업양도", "주식교환", "주식이전",
    "최대주주 변경", "대표이사 변경",
    # 실적 — "(잠정)실적(공정공시)" 같은 괄호 섞인 제목도 "실적" 으로 포괄
    "실적", "잠정실적", "영업이익", "매출액", "실적공시",
    # 거래소 공정공시 전반
    "공정공시",
    # 리스크
    "관리종목", "상장폐지", "영업정지", "불성실공시",
    "횡령", "배임", "소송", "과징금", "제재",
    # 배당
    "배당", "중간배당",
    # 지분
    "대량보유", "5% 보고",
    # 신사업·계약
    "공급계약", "수주", "MOU", "업무협약", "투자", "지분취득",
]

# 제외 키워드 (단순 정정 공시)
EXCLUDE_KEYWORDS = ["첨부추가", "첨부정정", "기재정정"]

# 중요도 이모지
URGENCY_MAP = {
    "🔴": ["유상증자", "감자", "합병", "분할", "상장폐지", "관리종목",
           "영업정지", "횡령", "배임", "불성실공시", "최대주주 변경"],
    "🟠": ["전환사채", "교환사채", "신주인수권", "영업양수", "영업양도",
           "주식교환", "소송", "과징금", "제재", "대표이사 변경"],
    "🟡": ["자기주식", "자사주", "배당", "중간배당", "주식분할", "주식병합",
           "잠정실적", "공급계약", "수주", "MOU", "지분취득"],
}


class DartCollector:
    """DART OpenAPI 래퍼 — 특별 공시 위주 수집"""

    BASE = "https://opendart.fss.or.kr/api"

    def __init__(self):
        self.api_key = config.DART_API_KEY
        self.sess    = requests.Session()
        self.sess.headers.update({"User-Agent": "StockAgent/2.0"})

    # ── 특별 공시 수집 ────────────────────────────────────────────────
    def fetch_special_disclosures(
        self,
        ticker:   str,
        days_back: int = 3,     # T-1: 주말 포함 3일이면 전 영업일 커버
        bgn_de:   str = None,
    ) -> list:
        corp_code = self._get_corp_code(ticker)
        if not corp_code:
            return []

        end_de = datetime.today().strftime("%Y%m%d")
        bgn_de = bgn_de or (
            datetime.today() - timedelta(days=days_back)
        ).strftime("%Y%m%d")

        collected = []

        for ptype in SPECIAL_PBOARD_TYPES:
            page = 1
            while True:
                resp = self._get("list.json", {
                    "corp_code":   corp_code,
                    "bgn_de":      bgn_de,
                    "end_de":      end_de,
                    "pblntf_ty":   ptype,
                    "page_no":     page,
                    "page_count":  40,
                })
                if not resp or resp.get("status") != "000":
                    break

                for item in resp.get("list", []):
                    nm = item.get("report_nm", "")
                    # 단순 정정 제외
                    if any(ex in nm for ex in EXCLUDE_KEYWORDS):
                        continue
                    # C/D/I 유형은 잡다해서 키워드 필터 적용 (A/B 는 통과)
                    if ptype in _FILTER_REQUIRED_TYPES:
                        if not any(kw in nm for kw in IMPORTANT_KEYWORDS):
                            continue
                    collected.append({
                        "rcept_no":  item.get("rcept_no", ""),
                        "ticker":    ticker,
                        "corp_name": item.get("corp_name", ""),
                        "report_nm": nm,
                        "rcept_dt":  item.get("rcept_dt", ""),
                        "rm":        item.get("rm", ""),
                        "flr_nm":    item.get("flr_nm", ""),
                    })

                total_page = int(resp.get("total_page", 1))
                if page >= total_page:
                    break
                page += 1
                time.sleep(0.2)

        saved = upsert_disclosures(collected)
        if saved > 0:
            print(f"  [DART] {ticker} 특별공시 {saved}건 저장")
        return collected

    def fetch_all_tickers(self, tickers: list, days_back: int = 3):
        """포트폴리오 전체 종목 공시 수집"""
        for ticker in tickers:
            self.fetch_special_disclosures(ticker, days_back=days_back)
            time.sleep(0.4)

    # ── 재무보고서 수집 ───────────────────────────────────────────────
    def fetch_financial_report(self, ticker: str, year: int = None):
        corp_code = self._get_corp_code(ticker)
        if not corp_code:
            return None

        year = year or (datetime.today().year - 1)
        resp = self._get("fnlttSinglAcntAll.json", {
            "corp_code":  corp_code,
            "bsns_year":  str(year),
            "reprt_code": "11011",   # 사업보고서
            "fs_div":     "CFS",     # 연결재무제표
        })
        if not resp or resp.get("status") != "000":
            return None

        data = self._parse_financials(resp.get("list", []))
        if data:
            data.update({
                "ticker":      ticker,
                "report_type": "사업보고서",
                "period_end":  f"{year}1231",
                "rcept_no":    f"SYNTH_{ticker}_{year}",
                "summary_text": "",
            })
            upsert_report(data)
        return data

    # ── 공시 요약 텍스트 (AI 프롬프트용) ─────────────────────────────
    def get_disclosure_summary(self, ticker: str, limit: int = 5, since_date: str = None) -> str:
        """since_date: YYYYMMDD, None이면 전체."""
        rows = load_disclosures(ticker, limit=limit, since_date=since_date)
        if not rows:
            return "해당 기간 특별 공시 없음"

        def get_emoji(nm):
            for emoji, kws in URGENCY_MAP.items():
                if any(k in nm for k in kws):
                    return emoji
            return "🔵"

        lines = []
        for r in rows:
            dt = r["rcept_dt"]
            date_str = f"{dt[:4]}-{dt[4:6]}-{dt[6:]}" if len(dt) == 8 else dt
            emoji = get_emoji(r["report_nm"])
            lines.append(f"{emoji} {date_str}  {r['report_nm']}")
        return "\n".join(lines)

    # ── 내부 유틸 ─────────────────────────────────────────────────────
    def _get(self, endpoint: str, params: dict):
        params["crtfc_key"] = self.api_key
        try:
            r = self.sess.get(
                f"{self.BASE}/{endpoint}", params=params, timeout=10
            )
            return r.json()
        except Exception as e:
            print(f"  [DART ERROR] {endpoint}: {e}")
            return None

    def _load_corp_code_map(self) -> dict:
        """corpCode.xml (전체 상장사 매핑) 을 다운로드·파싱해 dict 로 반환.
        `data/dart_corp_codes.json` 에 7일 TTL 로 캐시한다."""
        if _CORP_CODE_CACHE.exists():
            age_days = (time.time() - _CORP_CODE_CACHE.stat().st_mtime) / 86400
            if age_days < _CORP_CODE_TTL_DAYS:
                try:
                    with _CORP_CODE_CACHE.open(encoding="utf-8") as f:
                        return json.load(f)
                except Exception as e:
                    print(f"  [DART] corp_code 캐시 읽기 실패, 재다운로드: {e}")

        print("  [DART] corp_code 매핑 갱신 중 (전체 상장사 XML)…")
        try:
            resp = self.sess.get(
                f"{self.BASE}/corpCode.xml",
                params={"crtfc_key": self.api_key}, timeout=30,
            )
            resp.raise_for_status()
            with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
                with z.open(z.namelist()[0]) as f:
                    tree = ET.parse(f)
        except Exception as e:
            print(f"  [DART ERROR] corpCode.xml 다운로드 실패: {e}")
            return {}

        mapping: dict[str, str] = {}
        for item in tree.getroot().findall("list"):
            stock = (item.findtext("stock_code") or "").strip()
            corp  = (item.findtext("corp_code")  or "").strip()
            if stock and corp:
                mapping[stock] = corp

        try:
            _CORP_CODE_CACHE.parent.mkdir(parents=True, exist_ok=True)
            tmp = _CORP_CODE_CACHE.with_suffix(".tmp")
            with tmp.open("w", encoding="utf-8") as f:
                json.dump(mapping, f, ensure_ascii=False)
            tmp.replace(_CORP_CODE_CACHE)
        except Exception as e:
            print(f"  [DART] corp_code 캐시 저장 실패 (메모리만 사용): {e}")

        print(f"  [DART] {len(mapping)}개 종목 매핑 로드")
        return mapping

    def _get_corp_code(self, ticker: str):
        if not hasattr(self, "_corp_map"):
            self._corp_map = self._load_corp_code_map()
        return self._corp_map.get(ticker)

    @staticmethod
    def _parse_financials(items: list) -> dict:
        target = {
            "매출액":     "revenue",
            "영업이익":   "op_income",
            "당기순이익": "net_income",
            "자산총계":   "total_assets",
            "자본총계":   "total_equity",
        }
        result = {}
        for item in items:
            acnt = item.get("account_nm", "")
            if acnt in target:
                try:
                    val = float(
                        item.get("thstrm_amount", "0").replace(",", "")
                    )
                    result[target[acnt]] = round(val / 1e8, 1)
                except (ValueError, AttributeError):
                    pass
        if "total_assets" in result and "total_equity" in result:
            debt = result["total_assets"] - result["total_equity"]
            if result["total_equity"] > 0:
                result["debt_ratio"] = round(
                    debt / result["total_equity"] * 100, 1
                )
        # 없는 필드 None으로 채우기
        for k in ("revenue","op_income","net_income",
                  "total_assets","total_equity","per","pbr","roe","debt_ratio"):
            result.setdefault(k, None)
        return result
