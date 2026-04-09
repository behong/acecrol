import asyncio
import os
import logging
import random
import re
import gc
import base64
from datetime import datetime, timedelta
from fastapi import FastAPI, BackgroundTasks
from playwright.async_api import async_playwright
from dotenv import load_dotenv
from supabase import create_client, Client

# 1. 초기 설정 및 로깅
load_dotenv()
app = FastAPI()

logger = logging.getLogger("ZaiAceCrawler")
logger.setLevel(logging.INFO)
if not logger.handlers:
    sh = logging.StreamHandler()
    sh.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
    logger.addHandler(sh)

last_crawl_time = None
CRAWL_INTERVAL_SECONDS = 43200 # 12시간

supabase: Client = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))
USER_ID = os.getenv("AI_PARTNER_ID")
USER_PW = os.getenv("AI_PARTNER_PW")

# --- [유틸리티 함수] ---

def safe_format_date(date_str):
    try:
        if not date_str: return datetime.now().strftime("%Y-%m-%d")
        raw = date_str.split('~').strip() if '~' in date_str else date_str.strip()
        return datetime.strptime(raw, "%y.%m.%d").strftime("%Y-%m-%d")
    except: return datetime.now().strftime("%Y-%m-%d")

async def block_aggressively(route):
    """메모리 절약을 위해 이미지, 폰트, CSS 등 모든 미디어 차단"""
    if route.request.resource_type in ["image", "media", "font", "stylesheet", "other"]:
        await route.abort()
    else:
        await route.continue_()

# --- [핵심 크롤링 로직] ---

async def run_optimized_crawl():
    logger.info("🚀 [CRAWL] 초경량 메모리 최적화 수집을 시작합니다.")
    
    async with async_playwright() as p:
        # Render 무료 플랜 맞춤형 브라우저 인자
        browser = await p.chromium.launch(
            headless=True,
            args=[
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--disable-gpu',
                '--single-process' # 프로세스 단일화로 메모리 절약
            ]
        )
        context = await browser.new_context(viewport={'width': 800, 'height': 600})
        page = await context.new_page()
        
        # 리소스 차단 적용 (메모리 사용량 50% 감소)
        await page.route("**/*", block_aggressively)

        try:
            # 1. 로그인
            logger.info("🔗 로그인 시도...")
            await page.goto("https://www.aipartner.com/integrated/login?serviceCode=1000", wait_until="domcontentloaded")
            await page.fill('input[placeholder*="아이디"]', USER_ID)
            await page.fill('input[placeholder*="비밀번호"]', USER_PW)
            
            async with page.expect_navigation(wait_until="load", timeout=60000):
                await page.keyboard.press("Enter")
            await page.wait_for_timeout(5000)

            # 2. 리스트 페이지 이동
            logger.info("🔗 매물 목록으로 이동...")
            await page.goto("https://www.aipartner.com/offerings/ad_list", wait_until="load", timeout=90000)
            await page.wait_for_selector("table.tableAdSale tbody tr", timeout=60000)

            # 3. 매물번호 수집
            list_items = await page.evaluate("""() => {
                return Array.from(document.querySelectorAll("table.tableAdSale tbody tr"))
                    .map(row => ({ "article_no": row.querySelector(".numberA")?.innerText.trim() || "" }))
                    .filter(i => i.article_no.length > 3);
            }""")

            total_len = len(list_items)
            logger.info(f"📊 대상 {total_len}건 수집 시작.")

            # 4. 상세 페이지 순회
            for idx, item in enumerate(list_items):
                article_no = item['article_no']
                detail_url = f"https://www.aipartner.com/offerings/detail/{article_no}"
                
                logger.info(f"🔎 [{idx+1}/{total_len}] 매물 분석: {article_no}")
                
                try:
                    await page.goto(detail_url, wait_until="domcontentloaded", timeout=60000)
                    
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

                    # 데이터 가공
                    clean_name = details['name'].split('(').strip()
                    name_parts = clean_name.split(' ')
                    clean_price = re.sub(r'[^0-9]', '', details['price'])
                    floor_nums = re.findall(r'\d+', details.get('floor_row', ''))
                    
                    payload = {
                        "article_no": article_no,
                        "articlename": clean_name,
                        "realestatetypename": "아파트",
                        "tradetypename": "매매" if "매매" in details['name'] else "전세",
                        "dealorwarrantprc": clean_price,
                        "articleconfirmymd": safe_format_date(details['date']),
                        "buildingname": name_parts if name_parts else "",
                        "floorinfo": name_parts[-1] if len(name_parts) > 1 else "",
                        "room_count": re.sub(r'[^0-9]', '', details['room']),
                        "bath_count": re.sub(r'[^0-9]', '', details['bath']),
                        "current_floor": floor_nums if floor_nums else "",
                        "total_floors": floor_nums[-1] if len(floor_nums) > 1 else "",
                        "direction": details['dir'],
                        "entrance_type": details['ent'],
                        "parking_total": re.sub(r'[^0-9]', '', details['p_total']),
                        "parking_per_unit": details['p_per'].replace("대", ""),
                        "heat_type": details['heat'],
                        "maintenance_fee": re.sub(r'[^0-9]', '', details['fee']),
                        "move_in_date": details['move'],
                        "feature_desc": details['feat'],
                        "description": details['memo'],
                        "realtorname": "자이에이스",
                        "cppcarticleurl": f"https://new.land.naver.com/?articleNo={article_no}",
                        "updated_at": datetime.now().isoformat()
                    }
                    
                    supabase.table("real_estate_articles").upsert(payload, on_conflict="article_no").execute()

                except Exception as e:
                    logger.error(f"⚠️ {article_no} 스킵: {e}")
                
                # 메모리 관리: 5건마다 가비지 컬렉션
                if idx % 5 == 0: gc.collect()
                await asyncio.sleep(random.uniform(5, 8))

            logger.info("✨ [SUCCESS] 전체 수집 완료!")

        except Exception as e:
            logger.error(f"❌ [CRITICAL] 프로세스 실패: {e}")
        finally:
            await browser.close()
            gc.collect()

# --- [FastAPI 엔드포인트: 404 방지] ---

@app.get("/")
async def root():
    return {"status": "online", "message": "Zai Ace Bot is Awake"}

@app.get("/run-crawl")
async def trigger_crawl(background_tasks: BackgroundTasks):
    global last_crawl_time
    now = datetime.now()
    
    # 12시간 주기 체크
    if last_crawl_time is None or (now - last_crawl_time).total_seconds() >= CRAWL_INTERVAL_SECONDS:
        last_crawl_time = now
        background_tasks.add_task(run_optimized_crawl)
        return {"status": "started", "time": str(last_crawl_time)}
    
    remaining = int((CRAWL_INTERVAL_SECONDS - (now - last_crawl_time).total_seconds()) / 60)
    return {"status": "skipping", "message": f"휴식 중 ({remaining}분 남음)"}

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)