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

# 1. 설정 및 로깅
load_dotenv()
app = FastAPI()

logger = logging.getLogger("ZaiAceCrawler")
logger.setLevel(logging.INFO)
if not logger.handlers:
    sh = logging.StreamHandler()
    sh.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
    logger.addHandler(sh)

last_crawl_time = None
CRAWL_INTERVAL_SECONDS = 43200 

supabase: Client = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))
USER_ID = os.getenv("AI_PARTNER_ID")
USER_PW = os.getenv("AI_PARTNER_PW")

# --- [유틸리티 함수: 어떤 데이터가 들어와도 에러 안 나게 방어] ---

def to_str(val):
    """데이터가 리스트면 첫 번째 항목을, 아니면 그대로 문자열로 반환"""
    if isinstance(val, list):
        return str(val) if val else ""
    return str(val) if val is not None else ""

def safe_format_date(date_val):
    """날짜 변환 시 리스트 에러 원천 차단"""
    try:
        d_str = to_str(date_val)
        if not d_str: return datetime.now().strftime("%Y-%m-%d")
        
        # 물결표 제거 및 첫 번째 날짜만 선택
        raw = d_str.split('~').strip()
        return datetime.strptime(raw, "%y.%m.%d").strftime("%Y-%m-%d")
    except:
        return datetime.now().strftime("%Y-%m-%d")

async def block_aggressively(route):
    """메모리 보호를 위해 무거운 리소스 차단"""
    if route.request.resource_type in ["image", "media", "font", "stylesheet"]:
        await route.abort()
    else:
        await route.continue_()

# --- [핵심 크롤링 로직] ---

async def run_optimized_crawl():
    logger.info("🚀 [CRAWL] 최적화 수집 프로세스 가동 (v3.0 - 에러 원천 차단)")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage', '--single-process']
        )
        # 로그인을 위한 초기 페이지
        context = await browser.new_context(viewport={'width': 1024, 'height': 800})
        page = await context.new_page()
        await page.route("**/*", block_aggressively)

        try:
            # 1. 로그인
            logger.info("🔗 로그인 중...")
            await page.goto("https://www.aipartner.com/integrated/login?serviceCode=1000", wait_until="domcontentloaded")
            await page.fill('input[placeholder*="아이디"]', USER_ID)
            await page.fill('input[placeholder*="비밀번호"]', USER_PW)
            async with page.expect_navigation(wait_until="load", timeout=60000):
                await page.keyboard.press("Enter")
            await page.wait_for_timeout(3000)

            # 2. 리스트 수집
            logger.info("🔗 매물 목록 확보 중...")
            await page.goto("https://www.aipartner.com/offerings/ad_list", wait_until="load", timeout=90000)
            await page.wait_for_selector("table.tableAdSale tbody tr", timeout=60000)

            list_items = await page.evaluate("""() => {
                return Array.from(document.querySelectorAll("table.tableAdSale tbody tr"))
                    .map(row => ({ "article_no": row.querySelector(".numberA")?.innerText.trim() || "" }))
                    .filter(i => i.article_no.length > 3);
            }""")

            total_len = len(list_items)
            logger.info(f"📊 대상 {total_len}건 수집을 시작합니다.")

            # 3. 상세 페이지 순회 (메모리 관리를 위해 내부 루프 최적화)
            for idx, item in enumerate(list_items):
                article_no = to_str(item.get('article_no'))
                detail_url = f"https://www.aipartner.com/offerings/detail/{article_no}"
                
                # [메모리 최적화] 매번 새 페이지를 열고 닫음으로써 메모리 누수 방지
                det_page = await context.new_page()
                await det_page.route("**/*", block_aggressively)

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

                    # --- [데이터 정제 로직 - 가장 안전한 방식] ---
                    raw_name = to_str(details.get('name'))
                    clean_name = raw_name.split('(').strip()
                    
                    name_parts = clean_name.split(' ')
                    b_name = name_parts if name_parts else ""
                    f_info = name_parts[-1] if len(name_parts) > 1 else ""

                    clean_price = re.sub(r'[^0-9]', '', to_str(details.get('price')))
                    
                    floor_nums = re.findall(r'\d+', to_str(details.get('floor_row')))
                    c_floor = floor_nums if floor_nums else ""
                    t_floor = floor_nums[-1] if len(floor_nums) > 1 else ""

                    payload = {
                        "article_no": article_no,
                        "articlename": clean_name,
                        "realestatetypename": "아파트",
                        "tradetypename": "매매" if "매매" in raw_name else "전세",
                        "dealorwarrantprc": clean_price,
                        "articleconfirmymd": safe_format_date(details.get('date')),
                        "buildingname": b_name,
                        "floorinfo": f_info,
                        "room_count": re.sub(r'[^0-9]', '', to_str(details.get('room'))),
                        "bath_count": re.sub(r'[^0-9]', '', to_str(details.get('bath'))),
                        "current_floor": c_floor,
                        "total_floors": t_floor,
                        "direction": to_str(details.get('dir')),
                        "entrance_type": to_str(details.get('ent')),
                        "parking_total": re.sub(r'[^0-9]', '', to_str(details.get('p_total'))),
                        "parking_per_unit": to_str(details.get('p_per')).replace("대", ""),
                        "heat_type": to_str(details.get('heat')),
                        "maintenance_fee": re.sub(r'[^0-9]', '', to_str(details.get('fee'))),
                        "move_in_date": to_str(details.get('move')),
                        "feature_desc": to_str(details.get('feat')),
                        "description": to_str(details.get('memo')),
                        "realtorname": "자이에이스",
                        "cppcarticleurl": f"https://new.land.naver.com/?articleNo={article_no}",
                        "updated_at": datetime.now().isoformat()
                    }
                    
                    supabase.table("real_estate_articles").upsert(payload, on_conflict="article_no").execute()
                    logger.info(f"✅ [{idx+1}/{total_len}] 저장: {article_no}")

                except Exception as e:
                    logger.error(f"⚠️ {article_no} 처리 에러: {e}")
                finally:
                    # 페이지를 닫아서 메모리 즉시 반환
                    await det_page.close()
                
                if idx % 5 == 0: gc.collect()
                await asyncio.sleep(random.uniform(3, 5))

            logger.info("✨ [SUCCESS] 전체 수집 완료!")

        except Exception as e:
            logger.error(f"❌ [CRITICAL] 프로세스 실패: {e}")
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
        background_tasks.add_task(run_optimized_crawl)
        return {"status": "started", "time": str(last_crawl_time)}
    
    rem = int((CRAWL_INTERVAL_SECONDS - (now - last_crawl_time).total_seconds()) / 60)
    return {"status": "skipping", "message": f"{rem}분 남음"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))