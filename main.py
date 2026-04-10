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

logger = logging.getLogger("ZaiAceDeployV8")
logger.setLevel(logging.INFO)
if not logger.handlers:
    sh = logging.StreamHandler()
    sh.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
    logger.addHandler(sh)

# 전역 상태 관리
last_crawl_time = None
CRAWL_INTERVAL_SECONDS = 43200 

# Supabase 연결
supabase: Client = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))

# --- [안전 가드: 로컬에서 성공한 v8.0 정규식 로직] ---

def super_safe_cleaner(val):
    """정규식을 사용하여 리스트 에러를 원천 차단하고 이름만 추출"""
    try:
        if not val: return ""
        # 1. 리스트로 들어올 경우 첫 번째 값만 사용
        if isinstance(val, list):
            val = val if val else ""
        
        text = str(val).strip()
        # 2. 정규식: 여는 괄호 '(' 앞까지만 텍스트 추출 (리스트 에러 발생 안 함)
        match = re.search(r'^[^(\n]+', text)
        if match:
            return match.group(0).strip()
        return text
    except:
        return ""

def safe_format_date(date_val):
    """날짜 변환: '26.04.10' -> '2026-04-10'"""
    try:
        clean_d = super_safe_cleaner(date_val) # 안전 정제기 재사용
        if not clean_d: return datetime.now().strftime("%Y-%m-%d")
        return datetime.strptime(clean_d, "%y.%m.%d").strftime("%Y-%m-%d")
    except:
        return datetime.now().strftime("%Y-%m-%d")

def only_num(text):
    """숫자만 추출"""
    return re.sub(r'[^0-9]', '', str(text)) if text else ""

async def block_resources(route):
    """Render 메모리 보호를 위해 무거운 리소스 차단"""
    if route.request.resource_type in ["image", "media", "font", "stylesheet"]:
        await route.abort()
    else:
        await route.continue_()

# --- [핵심 크롤링 로직] ---

async def run_production_crawl():
    logger.info("🚀 [PRODUCTION v8.0] Render 배포용 최종 수집을 시작합니다.")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage', '--single-process', '--disable-gpu']
        )
        context = await browser.new_context(viewport={'width': 1280, 'height': 800})
        page = await context.new_page()

        try:
            # 1. 로그인
            logger.info("🔗 로그인 접속 중...")
            await page.goto("https://www.aipartner.com/integrated/login?serviceCode=1000", wait_until="domcontentloaded")
            await page.fill('input[placeholder*="아이디"]', os.getenv("AI_PARTNER_ID"))
            await page.fill('input[placeholder*="비밀번호"]', os.getenv("AI_PARTNER_PW"))
            await page.keyboard.press("Enter")
            await page.wait_for_timeout(5000)

            # 2. 목록 확보
            await page.goto("https://www.aipartner.com/offerings/ad_list", wait_until="load")
            await page.wait_for_selector("table.tableAdSale tbody tr", timeout=60000)

            list_items = await page.evaluate("""() => {
                return Array.from(document.querySelectorAll("table.tableAdSale tbody tr"))
                    .map(row => ({ "article_no": row.querySelector(".numberA")?.innerText.trim() || "" }))
                    .filter(i => i.article_no.length > 3);
            }""")

            total_len = len(list_items)
            logger.info(f"📊 대상 {total_len}건 분석 시작.")

            # 3. 상세 페이지 순회
            for idx, item in enumerate(list_items):
                article_no = str(item['article_no'])
                detail_url = f"https://www.aipartner.com/offerings/detail/{article_no}"
                
                det_page = await context.new_page()
                await det_page.route("**/*", block_resources)

                try:
                    await det_page.goto(detail_url, wait_until="domcontentloaded", timeout=60000)
                    
                    # ⭐ [핵심] 제목 로딩 대기
                    await det_page.wait_for_selector(".saleDetailName", timeout=10000)
                    
                    data = await det_page.evaluate("""() => {
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
                            dir: getV("방향")
                        };
                    }""")

                    # --- [데이터 정제: 정규식 기반] ---
                    raw_name = str(data.get('name', ''))
                    clean_articlename = super_safe_cleaner(raw_name)
                    
                    # 아파트명과 호수 분리
                    name_parts = clean_articlename.split(' ')
                    b_name = name_parts if name_parts else ""
                    f_info = name_parts[-1] if len(name_parts) > 1 else ""

                    f_nums = re.findall(r'\d+', str(data.get('floor_raw', '')))
                    c_floor = f_nums if f_nums else ""

                    payload = {
                        "article_no": article_no,
                        "articlename": str(clean_articlename),
                        "realestatetypename": "아파트",
                        "tradetypename": "매매" if "매매" in raw_name else "전세",
                        "dealorwarrantprc": only_num(data.get('price')),
                        "articleconfirmymd": safe_format_date(data.get('date')),
                        "buildingname": str(b_name),
                        "floorinfo": str(f_info),
                        "room_count": only_num(data.get('room')),
                        "bath_count": only_num(data.get('bath')),
                        "current_floor": str(c_floor),
                        "direction": str(data.get('dir')),
                        "realtorname": "자이에이스",
                        "cppcarticleurl": f"https://new.land.naver.com/?articleNo={article_no}",
                        "updated_at": datetime.now().isoformat()
                    }
                    
                    supabase.table("real_estate_articles").upsert(payload, on_conflict="article_no").execute()
                    logger.info(f"✅ [{idx+1}/{total_len}] 저장: {article_no} | {clean_articlename}")

                except Exception as inner_e:
                    logger.error(f"⚠️ {article_no} 오류: {inner_e}")
                finally:
                    await det_page.close()
                
                await asyncio.sleep(random.uniform(5, 8))

            logger.info("✨ [SUCCESS] 전체 수집 완료!")

        finally:
            await browser.close()
            gc.collect()

# --- [FastAPI 엔드포인트] ---

@app.get("/")
async def root(): return {"status": "online"}

@app.api_route("/run-crawl", methods=["GET", "HEAD"])
async def trigger_crawl(background_tasks: BackgroundTasks):
    global last_crawl_time
    now = datetime.now()
    if last_crawl_time is None or (now - last_crawl_time).total_seconds() >= CRAWL_INTERVAL_SECONDS:
        last_crawl_time = now
        background_tasks.add_task(run_production_crawl)
        return {"status": "started", "time": str(last_crawl_time)}
    return {"status": "skipping"}

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)