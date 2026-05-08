# Web (private share)

텔레그램 메시지 폭주를 줄이려고 만든 비공개 웹사이트. 분석 리포트, 시그널, 시장 경고, 공시를 친구 한 명과 공유하는 용도.

## 빠른 시작

### 1) 의존성 설치

```powershell
.\venv_kis\Scripts\pip install -r requirements.txt
```

### 2) 사용자 계정 생성

```powershell
.\venv_kis\Scripts\python -m web.create_user <username> --name "표시 이름"
# 비밀번호 두 번 입력
```

같은 명령으로 비번 변경도 가능. 친구 계정도 같은 방법.

### 3) (권장) 세션 키 환경변수

```powershell
# .env 에 추가 — 없으면 임시 키, 재시작 시 모든 세션 무효화
WEB_SECRET_KEY=<openssl rand -base64 36 같은 긴 랜덤 문자열>
```

### 4) 서버 실행

```powershell
.\venv_kis\Scripts\python run_web.py
# http://127.0.0.1:8000 에서 동작
```

### 5) ngrok 으로 외부 노출

```powershell
ngrok http 8000
```

ngrok 무료 플랜은 reserved domain 1개를 dashboard 에서 잡을 수 있다 (`https://<your-name>.ngrok-free.app`). 이걸 친구한테 공유.

## 라우트

| 경로 | 내용 |
|---|---|
| `/login` | 로그인 |
| `/reports` | 분석 리포트 목록 (analysis_log) |
| `/reports/{id}` | 상세 + D/W/M 차트 + 엘리엇 |
| `/signals` | 최근 30일 시그널 (날짜별 그룹) |
| `/warnings` | 시장 경고 브리핑 |
| `/filings` | DART + SEC 공시 (최근 14일) |

차트 PNG 는 `data/charts/YYYYMMDD/{ticker}_{interval}.png` 에 저장되며, 30일 지난 파일은 다음 분석 실행 시 자동 정리.

## 텔레그램 알림 모드 전환

기본은 기존 그대로 (메시지 풀 발송). 새 모드 활성화:

```powershell
# .env
WEB_ONLY=1
WEB_URL=https://<your-name>.ngrok-free.app
```

이러면 분석/시그널/경고 모두 한 줄 알림 + 웹 링크로 축소된다. 본문은 웹에서 확인.

토글이라 언제든 끌 수 있음 (`WEB_ONLY=` 또는 미설정).

## 배경 설계

- **새 테이블**: `users`, `signals`, `market_warnings`, `chart_files` (모두 `CREATE IF NOT EXISTS`, 기존 데이터 영향 없음)
- **차트는 텔레그램 발송 후 디스크에도 dump** — 텔레그램 흐름과 독립적이라 실패해도 텔레그램은 보장됨
- **세션은 itsdangerous 서명 쿠키** — 서버에 세션 저장소 없음, stateless
- **비번은 bcrypt** (passlib)
- **외부 노출은 ngrok** — Cloudflare Tunnel 도 가능. 그땐 `WEB_URL` 만 바꾸면 됨

## DB 공유 (Turso)

users / signals / market_warnings 도 `sync_after=True` 로 Turso 에 push 되므로, 다른 PC 에서도 같은 계정·시그널 이력을 본다. 차트 PNG 만 PC 로컬에 남는데, 어차피 30일 만료라 큰 의미 없음.

## 파일 위치

```
web/
  app.py              # FastAPI 앱 팩토리 + 인증/차트 서빙
  auth.py             # bcrypt + 세션 쿠키
  deps.py             # require_user_or_redirect
  create_user.py      # 계정 생성 CLI (python -m web.create_user)
  routes/
    reports.py / signals.py / warnings.py / filings.py
  templates/          # Jinja2
  static/css/style.css
run_web.py            # uvicorn 진입점
```
