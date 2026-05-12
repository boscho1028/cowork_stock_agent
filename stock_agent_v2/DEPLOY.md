# Docker 배포 가이드

새 Windows PC (Docker Desktop) 또는 GX10/Linux (Docker Engine) 에서 동일한 절차로 배포.

## 사전 준비 — 한 번만

### Windows (Docker Desktop)
1. WSL2 활성화 (Windows 10/11 최신 빌드는 기본 ON)
2. [Docker Desktop](https://www.docker.com/products/docker-desktop/) 설치 후 재부팅
3. Docker Desktop 실행 → 상태 표시줄 고래 아이콘이 안정될 때까지 대기

### GX10 / Linux
```bash
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker $USER && newgrp docker
docker --version          # 26.x+ 권장
docker compose version    # v2.x
```

## 1단계 — repo 클론 + .env 준비

```bash
# 새 호스트에서 (PowerShell 또는 bash)
git clone https://github.com/boscho1028/cowork_stock_agent.git
cd cowork_stock_agent/stock_agent_v2

# .env 만들기 — .env.example 을 복사한 뒤 값 채우기
cp .env.example .env
# 또는: 기존 PC 의 .env 를 안전한 방법(USB, 1Password, SCP)으로 옮겨오기
```

`.env` 핵심 키:
- `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`
- `KIS_APP_KEY`, `KIS_APP_SECRET`, `KIS_ACCOUNT_NO`
- `TURSO_DATABASE_URL`, `TURSO_AUTH_TOKEN`
- `ANTHROPIC_API_KEY` (또는 `GOOGLE_API_KEY`)
- `WEB_SECRET_KEY` (세션 서명. 길고 랜덤한 문자열)
- `WEB_ONLY=1` (텔레그램 한 줄 알림 모드)
- `KIS_TOKEN_CACHE_DIR` 줄은 **삭제** — 컨테이너 기본 `/app/tokens` 사용

## 2단계 — 기동

```bash
mkdir -p data logs tokens     # 첫 실행 전 디렉토리 준비 (호스트에서 보존)

docker compose up -d --build
docker compose logs -f         # 부팅 확인. Ctrl+C 로 빠져나옴
```

3개 컨테이너가 뜸:
- `stock-agent-web` (포트 8000)
- `stock-agent-scheduler` (APScheduler — 평일 07:25/07:30/17:00 KST)
- `stock-agent-tg` (telegram_trigger 명령 listener)

## 3단계 — 외부 노출 (ngrok)

`.env` 에 두 줄 추가:
```
NGROK_AUTHTOKEN=<ngrok dashboard 의 authtoken>
NGROK_DOMAIN=unbiased-tight-jay.ngrok-free.app
```

```bash
docker compose --profile ngrok up -d ngrok
docker compose logs -f ngrok
```

`https://unbiased-tight-jay.ngrok-free.app` 으로 외부 접속 가능.

## 4단계 — 운영 명령

```bash
docker compose ps                 # 서비스 상태
docker compose logs -f web        # 로그 (특정 서비스)
docker compose restart web        # 한 서비스만 재시작
docker compose down               # 전체 중지 (데이터는 볼륨에 남음)
docker compose up -d --build      # 코드 변경 후 재빌드

# 컨테이너 안에서 수동 명령 실행
docker compose exec web python -m web.create_user <username>   # 새 계정
docker compose exec web python main.py --analyze --ticker NVDA  # 단일 분석
```

## 5단계 — 기존 PC 정리 (마이그레이션 시)

```powershell
# 기존 Windows PC 에서 batch task 비활성화 (이중 발동 방지)
schtasks /Change /TN "주식 모닝브리핑" /DISABLE
schtasks /Change /TN "주식 저녁분석"   /DISABLE
schtasks /Change /TN "주식봇 텔레그램" /DISABLE
schtasks /Change /TN "ETF_Momentum_Bot" /DISABLE
```

기존 PC 의 ngrok 도 중지 (같은 reserved domain 을 두 호스트가 동시에 못 잡음).

## 데이터 보존

호스트 디렉토리가 컨테이너에 마운트되어 컨테이너 재생성·삭제에도 데이터는 호스트에 남음:

| 호스트 경로 | 컨테이너 경로 | 내용 |
|---|---|---|
| `./data/` | `/app/data/` | `stock_agent.db` (Turso embedded replica) + 차트 PNG |
| `./logs/` | `/app/logs/` | 배치 실행 로그 (`main_YYYYMMDD_HHMMSS.log`) |
| `./tokens/` | `/app/tokens/` | KIS access token JSON 캐시 |
| `./portfolio.csv` | `/app/portfolio.csv` | 포트폴리오 종목 |
| `./universe.csv` | `/app/universe.csv` | 관찰 universe |

Turso 가 클라우드 sync 라 `data/stock_agent.db` 는 다른 호스트와도 자동 공유. 새 호스트에서 첫 부팅 시 클라우드에서 받아옴.

## momentum_etf 연동 (선택)

stock_agent 의 evening 잡이 끝나면 `D:/momentum_etf/main.py` 를 subprocess 로
호출 — 이 경로는 Docker 컨테이너 안에는 없으므로:

- **간단**: `.env` 에 `ETF_ENABLED=0` → ETF 단계 건너뜀
- **함께 옮기기**: momentum_etf 도 같이 Docker 화 하거나 image 안에 함께 빌드.
  요청 시 추가 작업 가능

## 트러블슈팅

| 증상 | 원인·해결 |
|---|---|
| `web` 컨테이너 8000 응답 없음 | `docker compose logs web` 로 첫 30줄 확인. WEB_SECRET_KEY 누락 자주 |
| 차트에 한글 깨짐 | 이미지에 fonts-nanum 포함. 컨테이너 재빌드 (`--build`) |
| KIS 로그인 실패 | `./tokens/` 권한 (Linux 는 chmod 755) + KIS app_key 만료 확인 |
| scheduler 가 시각에 안 도는 듯 | 호스트 시각이 KST 인지 확인. Dockerfile 의 `TZ=Asia/Seoul` 가 컨테이너 시계 KST 로 설정 |
| ARM64 (GX10) 빌드 시 libsql wheel 없음 | Dockerfile 에 `apt-get install rust` 추가하고 재빌드 |
