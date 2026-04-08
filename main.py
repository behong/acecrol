import asyncio
import os
import logging
import random
import re
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
    stream_handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

# 2. 전역 변수 및 설정
last_crawl_time = None
CRAWL_INTERVAL_SECONDS = 43200  # 12시간

supabase: Client = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))
USER_ID = os.getenv("AI_PARTNER_ID")
USER_PW = os.getenv("AI_PARTNER_PW")

# --- [유틸리티 함수: 데이터 정제 및 에러 방지] ---

def safe_format_date(date_str):
    """날짜 형식 변환: '26.04.02' -> '2026-04-02'"""
    try:
        if not date_str: return datetime.now().strftime("%Y-%m-%d")
        # [수정 완료] split('~') 결과인 리스트의 첫 번째 요소를 가져와서 strip 합니다.
        raw = date_str.split('~').strip() if '~' in date_str else date_str.strip()
        return datetime.strptime(raw, "%y.%m.%d").strftime("%Y-%m-%d")
    except:
        return datetime.now().strftime("%Y-%m-%d")

async def take_debug_screenshot(page, name):
    try:
        screenshot_bytes = await page.screenshot(type="jpeg", quality=40)
        b64_str = base64.b64encode(screenshot_bytes).decode()
        logger.error(f"📸 [{name}] 화면 로그: data:image/jpeg;base64,{b64_str}")
    except: pass

# --- [핵심 크롤링 로직] ---

async def run_full_production_crawl():
    logger.info("🚀 [CRAWL] 데이터 정밀 수집 및 정제 프로세스를 시작합니다.")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=['--no-sandbox', '--disable-setuid-sandbox'])
        context = await browser.new_context(
            viewport={'width': 1280, 'height': 1000},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        )
        page = await context.new_page()

        try:
            # 1. 로그인
            await page.goto("https://www.aipartner.com/integrated/login?serviceCode=1000", wait_until="domcontentloaded")
            await page.fill('input[placeholder*="아이디"]', USER_ID)
            await page.fill('input[placeholder*="비밀번호"]', USER_PW)
            
            async with page.expect_navigation(wait_until="load", timeout=60000):
                await page.keyboard.press("Enter")
            await page.wait_for_timeout(5000)

            # 2. 매물 리스트 페이지 이동
            await page.goto("https://www.aipartner.com/offerings/ad_list", wait_until="load", timeout=90000)
            await page.wait_for_selector("table.tableAdSale tbody tr", timeout=60000)

            # 3. 대상 매물 번호 확보
            list_items = await page.evaluate("""() => {
                return Array.from(document.querySelectorAll("table.tableAdSale tbody tr"))
                    .map(row => ({ "article_no": row.querySelector(".numberA")?.innerText.trim() || "" }))
                    .filter(i => i.article_no.length > 3);
            }""")

            total_len = len(list_items)
            logger.info(f"📊 대상 {total_len}건 수집 루프 시작.")

            # 4. 상세 페이지 순회 분석
            for idx, item in enumerate(list_items):
                article_no = item['article_no']
                detail_url = f"https://www.aipartner.com/offerings/detail/{article_no}"
                
                await asyncio.sleep(random.uniform(5.0, 9.0))
                logger.info(f"🔎 [{idx+1}/{total_len}] 매물 분석: {article_no}")
                
                try:
                    await page.goto(detail_url, wait_until="domcontentloaded", timeout=60000)
                    
                    details = await page.evaluate("""() => {
                        const res = {};
                        const getV = (label) => {
                            const ths = Array.from(document.querySelectorAll('th'));
                            const target = ths.find(th => th.innerText.includes(label));
                            return target ? target.nextElementSibling.innerText.trim() : "";
                        };
                        res.name = document.querySelector(".saleDetailName")?.innerText.trim() || "";
                        res.price = getV("매물 가격");
                        res.date = getV("등록일");
                        res.floor_row = getV("층");
                        res.room = getV("방수");
                        res.bath = getV("욕실수");
                        res.dir = getV("방향");
                        res.ent = getV("현관구조");
                        res.p_total = getV("총 주차대수");
                        res.p_per = getV("세대당주차대수");
                        res.heat = getV("난방시설");
                        res.fee = document.querySelector(".price-wrap.total .price")?.innerText.trim() || "0";
                        res.feat = getV("매물특징");
                        res.memo = document.querySelector("textarea")?.value || "";
                        res.move = getV("입주 가능일");
                        return res;
                    }""")

                    # --- [데이터 정제] ---
                    # 1. articlename 정제 (괄호 안의 매물번호 제거)
                    clean_name = details['name'].split('(').strip()

                    # 2. floorinfo (마지막 단어인 호수만 추출)
                    name_parts = clean_name.split(' ')
                    floor_info = name_parts[-1] if len(name_parts) > 1 else ""

                    # 3. dealorwarrantprc (순수 숫자만 추출)
                    clean_price = re.sub(r'[^0-9]', '', details['price'])

                    # 4. 층수 숫자만 추출
                    floor_nums = re.findall(r'\d+', details.get('floor_row', ''))
                    curr_f = floor_nums if floor_nums else ""
                    total_f = floor_nums[-1] if len(floor_nums) > 1 else ""

                    payload = {
                        "article_no": article_no,
                        "articlename": clean_name,
                        "realestatetypename": "아파트",
                        "tradetypename": "매매" if "매매" in details['name'] else "전세",
                        "dealorwarrantprc": clean_price,
                        "articleconfirmymd": safe_format_date(details['date']),
                        "buildingname": name_parts if name_parts else "",
                        "floorinfo": floor_info,
                        "room_count": re.sub(r'[^0-9]', '', details['room']),
                        "bath_count": re.sub(r'[^0-9]', '', details['bath']),
                        "current_floor": curr_f,
                        "total_floors": total_f,
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
                    logger.error(f"⚠️ {article_no} 처리 중 오류 스킵: {e}")
                    continue

            logger.info(f"✨ [SUCCESS] {total_len}건 수집 및 정제 완료!")

        except Exception as e:
            logger.error(f"❌ [CRITICAL] 프로세스 실패: {e}")
            await take_debug_screenshot(page, "Critical_Error")
        finally:
            await browser.close()

# --- [FastAPI 엔드포인트] ---

@app.api_route("/", methods=["GET", "HEAD"])
async def root(): return {"status": "online"}

@app.api_route("/run-crawl", methods=["GET", "HEAD"])
async def trigger_crawl(background_tasks: BackgroundTasks):
    global last_crawl_time
    now = datetime.now()
    if last_crawl_time is None or (now - last_crawl_time).total_seconds() >= CRAWL_INTERVAL_SECONDS:
        last_crawl_time = now
        background_tasks.add_task(run_full_production_crawl)
        return {"status": "started", "time": str(last_crawl_time)}
    return {"status": "skipping"}

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, proxy_headers=True)