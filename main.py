import sys
import asyncio
import os
import logging
import random
import re
import gc
from datetime import datetime
from playwright.async_api import async_playwright
from dotenv import load_dotenv
from supabase import create_client, Client
from fastapi import FastAPI, BackgroundTasks

# 1. 초기 설정
load_dotenv()
app = FastAPI()

if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

logger = logging.getLogger("ZaiAceBot")
logger.setLevel(logging.INFO)
if not logger.handlers:
    sh = logging.StreamHandler()
    sh.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
    logger.addHandler(sh)

# 전역 설정
last_crawl_time = None
CRAWL_INTERVAL_SECONDS = 43200
supabase: Client = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))

# --- [에러 방지 유틸리티 함수] ---

def safe_format_date(date_val):
    """'26.04.02 ~ 26.05.02' -> '2026-04-02' (에러 방어형)"""
    try:
        d_str = str(date_val) if date_val else ""
        if not d_str: return datetime.now().strftime("%Y-%m-%d")
        
        # split 후을 먼저 해서 '문자열'로 만든 뒤 strip() 해야 에러가 안 납니다.
        raw = d_str.split('~').strip()
        return datetime.strptime(raw, "%y.%m.%d").strftime("%Y-%m-%d")
    except:
        return datetime.now().strftime("%Y-%m-%d")

def clean_only_number(text):
    """단위 다 떼고 숫자만 남김"""
    if not text: return ""
    return re.sub(r'[^0-9]', '', str(text))

async def block_resources(route):
    """메모리 절약을 위해 이미지/폰트 차단"""
    if route.request.resource_type in ["image", "font", "media", "stylesheet"]:
        await route.abort()
    else:
        await route.continue_()

# --- [핵심 크롤링 로직] ---

async def run_full_crawl():
    logger.info("🚀 [CRAWL] 정밀 수집 및 데이터 정제를 시작합니다.")
    
    async with async_playwright() as p:
        # Render 환경 고려 옵션
        browser = await p.chromium.launch(
            headless=True, # 배포용은 True, 로컬 테스트는 False로 바꿔도 됨
            args=['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage', '--single-process']
        )
        context = await browser.new_context(viewport={'width': 1280, 'height': 800})
        
        # 첫 페이지 (로그인용)
        page = await context.new_page()
        await page.route("**/*", block_resources)

        try:
            # 1. 로그인
            logger.info("🔗 로그인 중...")
            await page.goto("https://www.aipartner.com/integrated/login?serviceCode=1000", wait_until="domcontentloaded")
            await page.fill('input[placeholder*="아이디"]', os.getenv("AI_PARTNER_ID"))
            await page.fill('input[placeholder*="비밀번호"]', os.getenv("AI_PARTNER_PW"))
            
            async with page.expect_navigation(wait_until="load"):
                await page.keyboard.press("Enter")
            await page.wait_for_timeout(3000)

            # 2. 리스트 페이지 이동
            await page.goto("https://www.aipartner.com/offerings/ad_list", wait_until="load", timeout=90000)
            await page.wait_for_selector("table.tableAdSale tbody tr", timeout=60000)

            list_items = await page.evaluate("""() => {
                return Array.from(document.querySelectorAll("table.tableAdSale tbody tr"))
                    .map(row => ({ "article_no": row.querySelector(".numberA")?.innerText.trim() || "" }))
                    .filter(i => i.article_no.length > 3);
            }""")

            total_len = len(list_items)
            logger.info(f"📊 대상 {total_len}건 수집 루프 진입.")

            # 3. 상세 페이지 순회
            for idx, item in enumerate(list_items):
                article_no = str(item['article_no'])
                detail_url = f"https://www.aipartner.com/offerings/detail/{article_no}"
                
                # 메모리 누수 방지를 위해 매번 새 탭 열기
                det_page = await context.new_page()
                await det_page.route("**/*", block_resources)

                try:
                    await det_page.goto(detail_url, wait_until="domcontentloaded", timeout=60000)
                    
                    details = await det_page.evaluate("""() => {
                        const getV = (label) => {
                            const ths = Array.from(document.querySelectorAll('th'));
                            const target = ths.find(th => th.innerText.includes(label));
                            return target ? target.nextElementSibling.innerText.trim() : "";
                        };
                        return {
                            name: document.querySelector(".saleDetailName")?.innerText.trim() || "",
                            price: getV("매물 가격"),
                            date: getV("등록일"),
                            floor_raw: getV("층"),
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

                    # --- [중요: 데이터 정제 핵심] ---
                    # 1. articlename 정제 (괄호 제거)
                    full_name = str(details['name']).split('(').strip()
                    
                    # 2. floorinfo 정제 (마지막 호수/층만 추출)
                    name_parts = full_name.split(' ')
                    building_name = name_parts if name_parts else ""
                    floor_info = name_parts[-1] if len(name_parts) > 1 else ""

                    # 3. 가격 (숫자만)
                    clean_price = clean_only_number(details['price'])

                    # 4. 층수 (리스트 방지, 숫자만)
                    floor_nums = re.findall(r'\d+', str(details.get('floor_raw', '')))
                    curr_f = floor_nums if floor_nums else ""
                    total_f = floor_nums[-1] if len(floor_nums) > 1 else ""

                    payload = {
                        "article_no": article_no,
                        "articlename": full_name,
                        "realestatetypename": "아파트",
                        "tradetypename": "매매" if "매매" in str(details['name']) else "전세",
                        "dealorwarrantprc": clean_price,
                        "articleconfirmymd": safe_format_date(details['date']),
                        "buildingname": str(building_name), # 강제 문자열
                        "floorinfo": str(floor_info),       # 강제 문자열
                        "room_count": clean_only_number(details['room']),
                        "bath_count": clean_only_number(details['bath']),
                        "current_floor": str(curr_f),       # 강제 문자열 (리스트 방지)
                        "total_floors": str(total_f),       # 강제 문자열
                        "direction": str(details['dir']),
                        "entrance_type": str(details['ent']),
                        "parking_total": clean_only_number(details['p_total']),
                        "parking_per_unit": str(details['p_per']).replace("대", ""),
                        "heat_type": str(details['heat']),
                        "maintenance_fee": clean_only_number(details['fee']),
                        "move_in_date": str(details['move']),
                        "feature_desc": str(details['feat']),
                        "description": str(details['memo']),
                        "realtorname": "자이에이스",
                        "cppcarticleurl": f"https://new.land.naver.com/?articleNo={article_no}",
                        "updated_at": datetime.now().isoformat()
                    }
                    
                    supabase.table("real_estate_articles").upsert(payload, on_conflict="article_no").execute()
                    logger.info(f"✅ [{idx+1}/{total_len}] 저장: {article_no}")

                except Exception as inner_e:
                    logger.error(f"⚠️ {article_no} 상세 분석 실패: {inner_e}")
                finally:
                    await det_page.close() # 메모리 즉시 반환
                
                if idx % 5 == 0: gc.collect() # 가비지 컬렉션
                await asyncio.sleep(random.uniform(3, 5))

            logger.info("✨ [SUCCESS] 전체 수집 및 정제 완료!")

        finally:
            await browser.close()
            gc.collect()

# --- [API 엔드포인트] ---

@app.get("/")
async def root(): return {"status": "online"}

@app.api_route("/run-crawl", methods=["GET", "HEAD"])
async def trigger_crawl(background_tasks: BackgroundTasks):
    global last_crawl_time
    now = datetime.now()
    if last_crawl_time is None or (now - last_crawl_time).total_seconds() >= CRAWL_INTERVAL_SECONDS:
        last_crawl_time = now
        background_tasks.add_task(run_full_crawl)
        return {"status": "started"}
    return {"status": "skipping"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))