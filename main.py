import asyncio
import os
import logging
import random
import re
import gc # 가비지 컬렉션
from datetime import datetime, timedelta
from fastapi import FastAPI, BackgroundTasks
from playwright.async_api import async_playwright
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()
app = FastAPI()

logger = logging.getLogger("ZaiAceCrawler")
logger.setLevel(logging.INFO)
# (로깅 설정 생략...)

last_crawl_time = None
CRAWL_INTERVAL_SECONDS = 43200 
supabase: Client = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))

# --- [유틸리티 함수] ---

def safe_format_date(date_str):
    try:
        if not date_str: return datetime.now().strftime("%Y-%m-%d")
        raw = date_str.split('~').strip() if '~' in date_str else date_str.strip()
        return datetime.strptime(raw, "%y.%m.%d").strftime("%Y-%m-%d")
    except: return datetime.now().strftime("%Y-%m-%d")

# --- [메모리 최적화 핵심 함수] ---

async def block_aggressively(route):
    """이미지, 폰트, 미디어 등 메모리 먹는 리소스 차단"""
    if route.request.resource_type in ["image", "font", "media", "stylesheet"]:
        await route.abort()
    else:
        await route.continue_()

# --- [핵심 크롤링 로직] ---

async def run_optimized_crawl():
    logger.info("🚀 [CRAWL] 메모리 최적화 모드로 수집을 시작합니다.")
    
    async with async_playwright() as p:
        # 저사양 서버를 위한 브라우저 실행 옵션
        browser = await p.chromium.launch(
            headless=True,
            args=[
                '--no-sandbox', 
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage', # /dev/shm 부족 문제 해결
                '--disable-accelerated-2d-canvas',
                '--disable-gpu',           # GPU 비활성화
                '--no-first-run',
                '--no-zygote',
                '--single-process'         # 단일 프로세스 모드 (메모리 절약)
            ]
        )
        
        # 브라우저 컨텍스트 생성 (메모리 제한)
        context = await browser.new_context(viewport={'width': 800, 'height': 600}) # 뷰포트 축소
        page = await context.new_page()
        
        # 리소스 차단 적용
        await page.route("**/*", block_aggressively)

        try:
            # 1. 로그인
            logger.info("🔗 로그인 진행...")
            await page.goto("https://www.aipartner.com/integrated/login?serviceCode=1000", wait_until="domcontentloaded")
            await page.fill('input[placeholder*="아이디"]', os.getenv("AI_PARTNER_ID"))
            await page.fill('input[placeholder*="비밀번호"]', os.getenv("AI_PARTNER_PW"))
            
            async with page.expect_navigation(wait_until="domcontentloaded"):
                await page.keyboard.press("Enter")
            await page.wait_for_timeout(3000)

            # 2. 리스트 수집
            await page.goto("https://www.aipartner.com/offerings/ad_list", wait_until="domcontentloaded")
            await page.wait_for_selector("table.tableAdSale tbody tr", timeout=60000)

            list_items = await page.evaluate("""() => {
                return Array.from(document.querySelectorAll("table.tableAdSale tbody tr"))
                    .map(row => ({ "article_no": row.querySelector(".numberA")?.innerText.trim() || "" }))
                    .filter(i => i.article_no.length > 3);
            }""")

            total_len = len(list_items)
            logger.info(f"📊 대상 {total_len}건 포착. 순회 시작.")

            # 3. 상세 페이지 순회 (순차 처리)
            for idx, item in enumerate(list_items):
                article_no = item['article_no']
                detail_url = f"https://www.aipartner.com/offerings/detail/{article_no}"
                
                logger.info(f"🔎 [{idx+1}/{total_len}] {article_no} 수집 중...")
                
                try:
                    await page.goto(detail_url, wait_until="domcontentloaded", timeout=60000)
                    
                    # 데이터 추출 로직 (기존과 동일하므로 생략 처리, 실제 코드에는 포함하세요)
                    details = await page.evaluate("""() => {
                        const getV = (label) => {
                            const ths = Array.from(document.querySelectorAll('th'));
                            const target = ths.find(th => th.innerText.includes(label));
                            return target ? target.nextElementSibling.innerText.trim() : "";
                        };
                        return {
                            name: document.querySelector(".saleDetailName")?.innerText.trim() || "",
                            price: getV("매물 가격"),
                            date: getV("등록일"),
                            floor_row: getV("층"),
                            room: getV("방수"),
                            bath: getV("욕실수"),
                            dir: getV("방향"),
                            ent: getV("현관구조"),
                            p_total: getV("총 주차대수"),
                            p_per: getV("세대당주차대수"),
                            heat: getV("난방시설"),
                            fee: document.querySelector(".price-wrap.total .price")?.innerText.trim() || "0",
                            feat: getV("매물특징"),
                            memo: document.querySelector("textarea")?.value || "",
                            move: getV("입주 가능일")
                        };
                    }""")

                    # (데이터 가공 및 Supabase 저장 로직 동일...)
                    # ... payload 구성 및 supabase.upsert() ...

                except Exception as e:
                    logger.error(f"⚠️ {article_no} 에러: {e}")
                
                # [메모리 최적화 핵심 2] 매물 5개마다 파이썬 메모리 강제 비우기
                if idx % 5 == 0:
                    gc.collect()
                
                await asyncio.sleep(random.uniform(3, 6))

            logger.info("✨ 수집 완료!")

        finally:
            # 브라우저를 완전히 닫아 메모리 반환
            await context.close()
            await browser.close()
            gc.collect()

# --- (FastAPI 엔드포인트 로직은 동일) ---