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

logger = logging.getLogger("ZaiAceUltima")
logger.setLevel(logging.INFO)
if not logger.handlers:
    sh = logging.StreamHandler()
    sh.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
    logger.addHandler(sh)

last_crawl_time = None
CRAWL_INTERVAL_SECONDS = 43200 
supabase: Client = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))

# --- [안전 가드: 리스트 에러 원천 봉쇄 함수] ---

def safe_split_first(text, separator):
    """문자열을 자르고 첫 번째 조각을 안전하게 반환 (리스트 에러 방지용)"""
    try:
        t_str = str(text) if text else ""
        parts = t_str.split(separator)
        # 리스트의 첫 번째 요소를 꺼낸 뒤 strip을 해야 에러가 안 납니다!
        return parts.strip() if parts else ""
    except:
        return ""

def safe_format_date(date_val):
    """날짜 변환: '26.04.02 ~ 26.05.02' -> '2026-04-02'"""
    try:
        raw = safe_split_first(date_val, '~')
        if not raw: return datetime.now().strftime("%Y-%m-%d")
        return datetime.strptime(raw, "%y.%m.%d").strftime("%Y-%m-%d")
    except:
        return datetime.now().strftime("%Y-%m-%d")

def only_num(text):
    """단위 다 떼고 순수 숫자 문자열만 반환"""
    return re.sub(r'[^0-9]', '', str(text)) if text else ""

# --- [핵심 크롤링 로직] ---

async def run_ultima_crawl():
    logger.info("🚀 [CRAWL] 에러 방지 로직이 적용된 최종 수집을 시작합니다.")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True, # 로컬 테스트 시 False로 변경 가능
            args=['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage', '--single-process']
        )
        context = await browser.new_context(viewport={'width': 1280, 'height': 800})
        page = await context.new_page()

        try:
            # 1. 로그인
            logger.info("🔗 로그인 진행 중...")
            await page.goto("https://www.aipartner.com/integrated/login?serviceCode=1000", wait_until="domcontentloaded")
            await page.fill('input[placeholder*="아이디"]', os.getenv("AI_PARTNER_ID"))
            await page.fill('input[placeholder*="비밀번호"]', os.getenv("AI_PARTNER_PW"))
            async with page.expect_navigation(wait_until="load", timeout=60000):
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
            logger.info(f"📊 {total_len}건 수집 루프 시작.")

            # 3. 상세 페이지 순회
            for idx, item in enumerate(list_items):
                article_no = str(item['article_no'])
                detail_url = f"https://www.aipartner.com/offerings/detail/{article_no}"
                
                det_page = await context.new_page()
                try:
                    await det_page.goto(detail_url, wait_until="domcontentloaded", timeout=60000)
                    
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

                    # --- [안전한 데이터 가공 로직] ---
                    
                    # 1. articlename 정제 (괄호 제거)
                    # safe_split_first 함수를 써서 리스트 에러를 원천 차단합니다.
                    clean_name = safe_split_first(data.get('name'), '(')
                    
                    # 2. 단지명(첫단어) 및 호수(마지막단어)
                    name_parts = clean_name.split(' ')
                    building_name = name_parts if name_parts else ""
                    floor_info = name_parts[-1] if len(name_parts) > 1 else ""

                    # 3. 층수 추출 (리스트 방지)
                    f_nums = re.findall(r'\d+', str(data.get('floor_raw', '')))
                    c_floor = f_nums if f_nums else ""
                    t_floor = f_nums[-1] if len(f_nums) > 1 else ""

                    payload = {
                        "article_no": article_no,
                        "articlename": clean_name,
                        "realestatetypename": "아파트",
                        "tradetypename": "매매" if "매매" in str(data.get('name')) else "전세",
                        "dealorwarrantprc": only_num(data.get('price')),
                        "articleconfirmymd": safe_format_date(data.get('date')),
                        "buildingname": str(building_name),
                        "floorinfo": str(floor_info),
                        "room_count": only_num(data.get('room')),
                        "bath_count": only_num(data.get('bath')),
                        "current_floor": str(c_floor),
                        "total_floors": str(t_floor),
                        "direction": str(data.get('dir')),
                        "entrance_type": str(data.get('ent')),
                        "parking_total": only_num(data.get('p_total')),
                        "parking_per_unit": str(data.get('p_per')).replace("대", ""),
                        "heat_type": str(data.get('heat')),
                        "maintenance_fee": only_num(data.get('fee')),
                        "move_in_date": str(data.get('move')),
                        "feature_desc": str(data.get('feat')),
                        "description": str(data.get('memo')),
                        "realtorname": "자이에이스",
                        "cppcarticleurl": f"https://new.land.naver.com/?articleNo={article_no}",
                        "updated_at": datetime.now().isoformat()
                    }
                    
                    supabase.table("real_estate_articles").upsert(payload, on_conflict="article_no").execute()
                    logger.info(f"✅ [{idx+1}/{total_len}] 저장: {article_no}")

                except Exception as inner_e:
                    logger.error(f"⚠️ {article_no} 처리 중 에러: {inner_e}")
                finally:
                    await det_page.close()
                
                await asyncio.sleep(random.uniform(4, 7))

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
        background_tasks.add_task(run_ultima_crawl)
        return {"status": "started"}
    return {"status": "skipping"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))