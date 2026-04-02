import sys
import asyncio

# 윈도우 루프 정책 설정 (필수)
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

import os
import logging
from datetime import datetime
from fastapi import FastAPI, BackgroundTasks
from playwright.async_api import async_playwright
from dotenv import load_dotenv
from supabase import create_client, Client

# 1. 설정 및 로깅
load_dotenv()
app = FastAPI()

logger = logging.getLogger("EstateCrawler")
logger.setLevel(logging.INFO)
if not logger.handlers:
    stream_handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

# Supabase 및 환경변수
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

USER_ID = os.getenv("AI_PARTNER_ID")
USER_PW = os.getenv("AI_PARTNER_PW")

# --- [유틸리티 함수] ---

def safe_format_date(date_str):
    try:
        if not date_str or '~' not in date_str:
            return datetime.now().strftime("%Y-%m-%d")
        raw_date = date_str.split('~').strip()
        return datetime.strptime(raw_date, "%y.%m.%d").strftime("%Y-%m-%d")
    except:
        return datetime.now().strftime("%Y-%m-%d")

# --- [로컬 전용 수집 로직: 타임아웃 해결 버전] ---

async def crawl_ad_list_local_fix():
    logger.info("🚀 [LOCAL] 타임아웃 방지 모드로 수집을 시작합니다.")
    
    async with async_playwright() as p:
        try:
            # headless=False로 실행 (눈으로 확인)
            browser = await p.chromium.launch(headless=False)
            context = await browser.new_context(viewport={'width': 1600, 'height': 1000})
            page = await context.new_page()

            # 1. 로그인
            logger.info("🔗 로그인 페이지 접속 중...")
            await page.goto("https://www.aipartner.com/integrated/login?serviceCode=1000")
            await page.fill('input[placeholder*="아이디"]', USER_ID)
            await page.fill('input[placeholder*="비밀번호"]', USER_PW)
            await page.keyboard.press("Enter")
            
            # 로그인이 완료되어 대시보드가 뜰 때까지 대기
            await page.wait_for_timeout(3000)
            
            # 2. 내 매물 관리 페이지 이동 (로딩 방식을 domcontentloaded로 완화)
            logger.info("🔗 매물 관리 페이지로 이동합니다.")
            await page.goto("https://www.aipartner.com/offerings/ad_list", wait_until="domcontentloaded", timeout=60000)
            
            # 네트워크가 다 멈출 때까지 기다리지 말고, 실제 데이터 테이블이 나올 때까지만 대기
            logger.info("⏳ 테이블 로딩을 기다리는 중...")
            await page.wait_for_selector("table.tableAdSale", timeout=30000)

            # 3. 100개씩 보기 설정
            try:
                await page.click(".GTM_offerings_ad_list_listing_list_more a.selectInfoOrder")
                await page.wait_for_timeout(500)
                await page.click("a.perPage[data-cd='100']")
                logger.info("⚡ 100개씩 보기 설정 완료")
                await page.wait_for_timeout(4000)
            except:
                logger.warning("⚠️ 보기 설정 클릭 실패 (기본값으로 진행)")

            # 4. 데이터 추출
            logger.info("🔎 데이터를 추출합니다...")
            page_data = await page.evaluate("""() => {
                const results = [];
                const rows = Array.from(document.querySelectorAll("table.tableAdSale tbody tr"));
                rows.forEach(row => {
                    const article_no = row.querySelector(".numberA")?.innerText.trim() || "";
                    const complex_full = row.querySelector(".fullName")?.innerText.trim() || "";
                    const deal_type = row.querySelector(".dealType")?.innerText.trim() || "";
                    const price = row.querySelector(".price")?.innerText.trim() || "";
                    const reg_date = row.querySelector(".date")?.innerText.trim() || "";

                    if (article_no && article_no.length > 3) {
                        results.push({
                            "article_no": article_no,
                            "complex_nm": complex_full,
                            "deal_type": deal_type,
                            "price": price,
                            "reg_date": reg_date
                        });
                    }
                });
                return results;
            }""")

            logger.info(f"✅ 추출 성공: {len(page_data)}건 발견")

            # 5. DB 저장 가공
            if page_data:
                upload_list = []
                for item in page_data:
                    parts = item["complex_nm"].split(' ')
                    b_name = parts if parts else "아파트"
                    f_info = parts[-1] if len(parts) > 1 else ""

                    upload_list.append({
                        "article_no": item["article_no"],
                        "articlename": item["complex_nm"],
                        "realestatetypename": "아파트",
                        "tradetypename": item["deal_type"],
                        "floorinfo": f_info,
                        "dealorwarrantprc": item["price"],
                        "articleconfirmymd": safe_format_date(item["reg_date"]),
                        "buildingname": b_name,
                        "realtorname": "자이에이스",
                        "cppcarticleurl": f"https://new.land.naver.com/?articleNo={item['article_no']}",
                        "isPopular": False
                    })

                for i in range(0, len(upload_list), 100):
                    chunk = upload_list[i:i + 100]
                    supabase.table("real_estate_articles").upsert(chunk, on_conflict="article_no").execute()
                
                logger.info(f"✨ [DB 저장 완료] 총 {len(upload_list)}건")

            await page.wait_for_timeout(3000)
            await browser.close()

        except Exception as e:
            logger.error(f"❌ [에러 발생] {e}")

# --- [API] ---
@app.get("/test-crawl")
async def trigger_test(background_tasks: BackgroundTasks):
    background_tasks.add_task(crawl_ad_list_local_fix)
    return {"status": "started"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)