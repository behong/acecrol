# Playwright 공식 파이썬 이미지 사용 (브라우저가 이미 포함됨)
FROM mcr.microsoft.com/playwright/python:v1.42.0-jammy

# 작업 디렉토리 설정
WORKDIR /app

# 필요 파일 복사 및 설치
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 앱 소스 복사
COPY . .

# 포트 설정
EXPOSE 8000

# 서버 실행 (uvicorn)
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]