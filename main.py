import asyncio
import os
import logging
import random
import re
import gc
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

# Supabase 및 로그인 정보
supabase: Client = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))
USER_ID = os.getenv("AI_PARTNER_ID")
USER_PW = os.getenv("AI_PARTNER_PW")

# --- [유틸리티 함수: 에러 및 리스트 형태 원천 차단] ---

def safe_format_date(date_str):
    """날짜 형식 변환: '26.04.02 ~ 26.05.02' -> '2026-04-02'"""
    try:
        if not date_str: 
            return datetime.now().strftime("%Y-%m-%d")
        
        # [수정 완료] 리스트 에러 방지용 명시적 처리
        date_str_clean = str(date_str)
        if '~' in date_str_clean:
            parts = date_str_clean.split('~')
            raw = parts.strip() # 첫 번째 요소를 확실히 꺼낸 후 strip
        else:
            raw = date_str_clean.strip()
            
        return datetime.strptime(raw, "%y.%m.%d").strftime("%Y-%m-%d")
    except:
        return datetime.now().strftime("%Y-%m-%d")

async def block_aggressively(route):
    """메모리 보호를 위해 이미지/폰트/CSS 차단"""
    if route.request.resource_type in ["image", "media", "font", "stylesheet"]:
        await route.abort()
    else:
        await route.continue_()

# --- [핵심 크롤링 로직] ---

async def run_optimized_crawl():
    logger.info("🚀 [CRAWL] 최적화 수집 프로세스를 시작합니다.")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage', '--single-process']
        )
        context = await browser.new_context(viewport={'width': 1024, 'height': 800})
        page = await context.new_page()
        
        # 리소스 차단 적용
        await page.route("**/*", block_aggressively)

        try:
            # 1. 로그인
            logger.info("🔗 로그인 접속 중...")
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

            # 목록 가져오기
            list_items = await page.evaluate("""() => {
                return Array.from(document.querySelectorAll("table.tableAdSale tbody tr"))
                    .map(row => ({ "article_no": row.querySelector(".numberA")?.innerText.trim() || "" }))
                    .filter(i => i.article_no.length > 3);
            }""")

            total_len = len(list_items)
            logger.info(f"📊 대상 {total_len}건 수집 시작.")

            # 3. 상세 페이지 순회
            for idx, item in enumerate(list_items):
                article_no = item['article_no']
                detail_url = f"https://www.aipartner.com/offerings/detail/{article_no}"
                
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

                    # --- [데이터 정제: 리스트/매물번호 완벽 제거] ---
                    
                    # 1. articlename: "동천자이 110동 3602호 (58696400)" -> "동천자이 110동 3602호"
                    full_name = str(details['name']).split('(').strip()
                    
                    # 2. buildingname & floorinfo: "동천자이", "3602호"
                    name_parts = full_name.split(' ')
                    b_name = name_parts if name_parts else ""
                    f_info = name_parts[-1] if len(name_parts) > 1 else ""

                    # 3. dealorwarrantprc: 숫자만
                    clean_price = re.sub(r'[^0-9]', '', str(details['price']))

                    # 4. current_floor & total_floors: "24", "36"
                    floor_nums = re.findall(r'\d+', str(details.get('floor_row', '')))
                    c_floor = floor_nums if floor_nums else ""
                    t_floor = floor_nums[-1] if len(floor_nums) > 1 else ""

                    payload = {
                        "article_no": article_no,
                        "articlename": full_name,
                        "realestatetypename": "아파트",
                        "tradetypename": "매매" if "매매" in details['name'] else "전세",
                        "dealorwarrantprc": clean_price,
                        "articleconfirmymd": safe_format_date(details['date']),
                        "buildingname": str(b_name), # 리스트 방지 강제 형변환
                        "floorinfo": str(f_info),    # 리스트 방지 강제 형변환
                        "room_count": re.sub(r'[^0-9]', '', str(details['room'])),
                        "bath_count": re.sub(r'[^0-9]', '', str(details['bath'])),
                        "current_floor": str(c_floor), # 리스트 방지 강제 형변환
                        "total_floors": str(t_floor),
                        "direction": details['dir'],
                        "entrance_type": details['ent'],
                        "parking_total": re.sub(r'[^0-9]', '', str(details['p_total'])),
                        "parking_per_unit": str(details['p_per']).replace("대", ""),
                        "heat_type": details['heat'],
                        "maintenance_fee": re.sub(r'[^0-9]', '', str(details['fee'])),
                        "move_in_date": details['move'],
                        "feature_desc": details['feat'],
                        "description": details['memo'],
                        "realtorname": "자이에이스",
                        "cppcarticleurl": f"https://new.land.naver.com/?articleNo={article_no}",
                        "updated_at": datetime.now().isoformat()
                    }
                    
                    # Supabase 전송
                    supabase.table("real_estate_articles").upsert(payload, on_conflict="article_no").execute()
                    logger.info(f"✅ [{idx+1}/{total_len}] 저장: {article_no}")

                except Exception as e:
                    logger.error(f"⚠️ {article_no} 처리 오류: {e}")
                
                # 메모리 청소
                if idx % 5 == 0: gc.collect()
                await asyncio.sleep(random.uniform(4, 7))

            logger.info("✨ [SUCCESS] 전체 수집 완료!")

        except Exception as e:
            logger.error(f"❌ [CRITICAL] 프로세스 실패: {e}")
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
        background_tasks.add_task(run_optimized_crawl)
        return {"status": "started", "time": str(last_crawl_time)}
    return {"status": "skipping"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))