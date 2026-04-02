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

# 1. 초기 설정
load_dotenv()
app = FastAPI()

logger = logging.getLogger("ZaiAceCrawler")
logger.setLevel(logging.INFO)
if not logger.handlers:
    stream_handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

last_crawl_time = None
CRAWL_INTERVAL_SECONDS = 43200 

supabase: Client = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))
USER_ID = os.getenv("AI_PARTNER_ID")
USER_PW = os.getenv("AI_PARTNER_PW")

# --- [유틸리티] ---

def safe_format_date(date_str):
    try:
        raw = date_str.split('~').strip() if '~' in date_str else date_str.strip()
        return datetime.strptime(raw, "%y.%m.%d").strftime("%Y-%m-%d")
    except: return datetime.now().strftime("%Y-%m-%d")

async def take_debug_screenshot(page, name):
    """Render 로그에 현재 화면을 강제로 텍스트화해서 남깁니다."""
    try:
        screenshot_bytes = await page.screenshot(type="jpeg", quality=50)
        b64_str = base64.b64encode(screenshot_bytes).decode()
        logger.error(f"📸 [{name}] 스크린샷 텍스트 로그 (디버깅용): data:image/jpeg;base64,{b64_str[:100]}...")
        # 이 텍스트를 복사해서 브라우저 주소창에 넣으면 화면이 보입니다.
    except: pass

# --- [핵심 로직] ---

async def run_production_crawl():
    logger.info("🚀 [CRAWL] 정밀 수집 프로세스를 시작합니다.")
    
    async with async_playwright() as p:
        # 가벼운 실행을 위해 옵션 추가
        browser = await p.chromium.launch(headless=True, args=['--no-sandbox', '--disable-setuid-sandbox'])
        context = await browser.new_context(
            viewport={'width': 1280, 'height': 1000},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        )
        page = await context.new_page()

        try:
            # 1. 로그인
            logger.info("🔗 로그인 페이지 접속...")
            await page.goto("https://www.aipartner.com/integrated/login?serviceCode=1000", wait_until="domcontentloaded")
            await page.fill('input[placeholder*="아이디"]', USER_ID)
            await page.fill('input[placeholder*="비밀번호"]', USER_PW)
            await page.keyboard.press("Enter")
            
            await page.wait_for_timeout(8000) # 로그인 처리 대기
            
            # 2. 리스트 페이지 이동
            logger.info("🔗 매물 리스트 페이지로 이동...")
            # 타임아웃을 90초로 더 늘리고, 로딩 전략을 여유 있게 설정
            await page.goto("https://www.aipartner.com/offerings/ad_list", wait_until="load", timeout=90000)
            
            # 테이블 확인 (셀렉터 단순화)
            logger.info("⏳ 테이블이 나타날 때까지 대기...")
            try:
                await page.wait_for_selector("table", timeout=60000)
            except Exception:
                await take_debug_screenshot(page, "Table_Timeout")
                raise Exception("테이블 로딩 타임아웃 (화면 확인 필요)")

            # 100개씩 보기 (실패해도 진행)
            try:
                await page.click(".sortingWrap .selectBox a.selectInfoOrder", timeout=10000)
                await page.click("a.perPage[data-cd='100']", timeout=10000)
                await page.wait_for_timeout(5000)
            except: logger.warning("⚠️ 100개씩 보기 설정 실패")

            # 3. 매물번호 수집
            list_items = await page.evaluate("""() => {
                return Array.from(document.querySelectorAll("table tbody tr"))
                    .map(row => ({ "article_no": row.querySelector(".numberA")?.innerText.trim() || "" }))
                    .filter(i => i.article_no.length > 3);
            }""")

            total_len = len(list_items)
            logger.info(f"📊 대상 매물: {total_len}건. 상세 분석 시작.")

            # 4. 상세 페이지 루프
            for idx, item in enumerate(list_items):
                article_no = item['article_no']
                detail_url = f"https://www.aipartner.com/offerings/detail/{article_no}"
                
                await asyncio.sleep(random.uniform(5.0, 10.0)) # 더 느리고 안전하게
                logger.info(f"🔎 [{idx+1}/{total_len}] 분석 중: {article_no}")
                
                try:
                    await page.goto(detail_url, wait_until="domcontentloaded", timeout=60000)
                    
                    details = await page.evaluate("""() => {
                        const results = {};
                        const getValue = (label) => {
                            const ths = Array.from(document.querySelectorAll('th'));
                            const targetTh = ths.find(th => th.innerText.includes(label));
                            return targetTh ? targetTh.nextElementSibling.innerText.trim() : "";
                        };
                        results.articlename = document.querySelector(".saleDetailName")?.innerText.trim() || "";
                        results.price = getValue("매물 가격");
                        results.reg_date = getValue("등록일");
                        results.move_in = getValue("입주 가능일");
                        results.floors = getValue("층");
                        results.rooms = getValue("방수");
                        results.baths = getValue("욕실수");
                        results.direction = getValue("방향");
                        results.entrance = getValue("현관구조");
                        results.parking_total = getValue("총 주차대수");
                        results.parking_per = getValue("세대당주차대수");
                        results.heat = getValue("난방시설");
                        results.fee = document.querySelector(".price-wrap.total .price")?.innerText.trim() || "0";
                        results.feature = getValue("매물특징");
                        results.memo = document.querySelector("textarea")?.value || "";
                        return results;
                    }""")

                    floor_match = re.findall(r'\d+', details.get('floors', ''))
                    curr_f = floor_match if floor_match else ""
                    total_f = floor_match[-1] if len(floor_match) > 1 else ""

                    payload = {
                        "article_no": article_no,
                        "articlename": details['articlename'],
                        "realestatetypename": "아파트",
                        "tradetypename": "매매" if "매매" in details['articlename'] else "전세",
                        "dealorwarrantprc": details['price'].replace(",", "").replace("만원", "").strip(),
                        "articleconfirmymd": safe_format_date(details['reg_date']),
                        "buildingname": details['articlename'].split(' '),
                        "floorinfo": details['articlename'].split(' ')[-1],
                        "room_count": details['rooms'].replace("개", ""),
                        "bath_count": details['baths'].replace("개", ""),
                        "current_floor": curr_f,
                        "total_floors": total_f,
                        "direction": details['direction'],
                        "entrance_type": details['entrance'],
                        "parking_total": details['parking_total'].replace("대", "").replace(",", ""),
                        "parking_per_unit": details['parking_per'].replace("대", ""),
                        "heat_type": details['heat'],
                        "maintenance_fee": details['fee'].replace(",", "").replace("원", "").strip(),
                        "move_in_date": details['move_in'],
                        "feature_desc": details['feature'],
                        "description": details['memo'],
                        "realtorname": "자이에이스",
                        "cppcarticleurl": f"https://new.land.naver.com/?articleNo={article_no}",
                        "updated_at": datetime.now().isoformat()
                    }
                    supabase.table("real_estate_articles").upsert(payload, on_conflict="article_no").execute()
                except Exception as e:
                    logger.error(f"⚠️ {article_no} 상세 페이지 에러: {e}")
                    continue

            logger.info(f"✨ [SUCCESS] {total_len}건 수집 완료!")
        except Exception as e:
            logger.error(f"❌ [CRITICAL] 프로세스 실패: {e}")
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
        background_tasks.add_task(run_production_crawl)
        return {"status": "started", "time": str(last_crawl_time)}
    return {"status": "skipping"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.environ.get("PORT", 8000)), proxy_headers=True)