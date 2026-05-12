# Stock Agent — multi-arch (amd64 + arm64) 이미지.
# GX10 (ARM64 Linux) 와 일반 Linux PC 모두 같은 Dockerfile 로 빌드.
#
# 빌드:
#   docker build -t stock-agent .
# 멀티 아키텍처 빌드(GX10용 ARM64 포함):
#   docker buildx build --platform linux/arm64,linux/amd64 -t stock-agent .

FROM python:3.12-slim AS base

# tzdata: KST 시간 정확성에 필요. fonts: matplotlib 한국어 차트 (Malgun
# Gothic 대체로 Nanum 사용). build-essential/git: libsql 등 native build
# 폴백 시 필요.
RUN apt-get update && apt-get install -y --no-install-recommends \
        tzdata \
        ca-certificates \
        fonts-nanum \
        curl \
        git \
    && rm -rf /var/lib/apt/lists/*

ENV TZ=Asia/Seoul \
    PYTHONUTF8=1 \
    PYTHONIOENCODING=utf-8 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# 의존성 캐시 효율 — requirements 만 먼저 복사
COPY requirements.txt ./
RUN pip install --upgrade pip && pip install -r requirements.txt

# 소스 복사
COPY . .

# 기본 데이터/로그/토큰 디렉토리 (실제 데이터는 볼륨 마운트로 주입)
RUN mkdir -p /app/data /app/logs /app/tokens

# 컨테이너 내 토큰 캐시 경로 (compose 에서 volume mount)
ENV KIS_TOKEN_CACHE_DIR=/app/tokens

EXPOSE 8000

# 기본 명령은 웹 서버. scheduler/tg-listener 는 compose 에서 override.
CMD ["python", "run_web.py"]
