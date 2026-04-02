import asyncio
import os
import logging
import random
import re
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

# 2. 전역 설정 (12시간 주기)
last_crawl_time = None
CRAWL_INTERVAL_SECONDS = 43200  # 12시간 (60초 * 60분 * 12)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

USER_ID = os.getenv("AI_PARTNER_ID")
USER_PW = os.getenv("AI_PARTNER_PW")

# --- [유틸리티 함수] ---

def clean_text(text):
    """단위 및 콤마 제거 후 순수 숫자/텍스트만 반환"""
    if not text: return ""
    return text.replace("개", "").replace("대", "").replace("원", "").replace("만원", "").replace(",", "").strip()

def safe_format_date(date_str):
    try:
        raw = date_str.split('~').strip() if '~' in date_str else date_str.strip()
        return datetime.strptime(raw, "%y.%m.%d").strftime("%Y-%m-%d")
    except:
        return datetime.now().strftime("%Y-%m-%d")

# --- [핵심 크롤링 로직: 목록 + 상세 페이지 순회] ---

async def run_full_production_crawl():
    logger.info("🚀 [CRAWL] 12시간 주기 정밀 수집을 시작합니다.")
    
    async with async_playwright() as p:
        # 배포 환경이므로 headless=True
        browser = await p.chromium.launch(headless=True)
        # 실제 브라우저처럼 보이기 위해 User-Agent 설정
        context = await browser.new_context(
            viewport={'width': 1600, 'height': 1200},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        )
        page = await context.new_page()

        try:
            # 1. 로그인
            await page.goto("https://www.aipartner.com/integrated/login?serviceCode=1000")
            await page.fill('input[placeholder*="아이디"]', USER_ID)
            await page.fill('input[placeholder*="비밀번호"]', USER_PW)
            await page.keyboard.press("Enter")
            await page.wait_for_timeout(5000)
            
            # 2. 매물관리 리스트 페이지로 이동
            await page.goto("https://www.aipartner.com/offerings/ad_list", wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_selector("table.tableAdSale", timeout=30000)

            # 100개씩 보기 설정
            try:
                await page.click(".sortingWrap .GTM_offerings_ad_list_listing_list_more a.selectInfoOrder")
                await page.wait_for_timeout(1000)
                await page.click("a.perPage[data-cd='100']")
                await page.wait_for_timeout(5000)
            except: pass

            # 3. 목록에서 매물 번호 수집
            list_items = await page.evaluate("""() => {
                return Array.from(document.querySelectorAll("table.tableAdSale tbody tr")).map(row => ({
                    "article_no": row.querySelector(".numberA")?.innerText.trim() || ""
                })).filter(i => i.article_no.length > 3);
            }""")

            total_len = len(list_items)
            logger.info(f"📊 수집 대상: 총 {total_len}건 발견. 상세 페이지 분석을 시작합니다.")

            # 4. 상세 페이지 하나씩 방문 (Loop)
            for idx, item in enumerate(list_items):
                article_no = item['article_no']
                detail_url = f"https://www.aipartner.com/offerings/detail/{article_no}"
                
                # 차단 방지를 위한 사람 같은 랜덤 휴식 (3~7초)
                await asyncio.sleep(random.uniform(3.0, 7.0))
                
                logger.info(f"🔎 [{idx+1}/{total_len}] 매물 분석 중: {article_no}")
                
                try:
                    await page.goto(detail_url, wait_until="domcontentloaded", timeout=60000)
                    await page.wait_for_timeout(2000)

                    # 상세 데이터 추출 JS
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

                    # 층수 정제 (예: "24/36")
                    floor_match = re.findall(r'\d+', details['flo론s'])
                    curr_f = floor_match if len(floor_match) > 0 else ""
                    total_f = floor_match[-1] if len(floor_match) > 1 else ""

                    # DB 페이로드 구성
                    payload = {
                        "article_no": article_no,
                        "articlename": details['articlename'],
                        "realestatetypename": "아파트",
                        "tradetypename": "매매" if "매매" in details['articlename'] else "전세",
                        "dealorwarrantprc": clean_text(details['price']),
                        "articleconfirmymd": safe_format_date(details['reg_date']),
                        "buildingname": details['articlename'].split(' '),
                        "floorinfo": details['articlename'].split(' ')[-1],
                        
                        "room_count": clean_text(details['rooms']),
                        "bath_count": clean_text(details['baths']),
                        "current_floor": curr_f,
                        "total_floors": total_f,
                        "direction": details['direction'],
                        "entrance_type": details['entrance'],
                        "parking_total": clean_text(details['parking_total']),
                        "parking_per_unit": clean_text(details['parking_per']),
                        "heat_type": details['heat'],
                        "maintenance_fee": clean_text(details['fee']),
                        "move_in_date": details['move_in'],
                        "feature_desc": details['feature'],
                        "description": details['memo'],
                        
                        "realtorname": "자이에이스",
                        "cppcarticleurl": f"https://new.land.naver.com/?articleNo={article_no}",
                        "updated_at": datetime.now().isoformat()
                    }

                    # Supabase에 덮어쓰기
                    supabase.table("real_estate_articles").upsert(payload, on_conflict="article_no").execute()

                except Exception as e:
                    logger.error(f"⚠️ {article_no} 수집 중 에러 발생: {e}")
                    continue # 다음 매물로 진행

            logger.info(f"✨ [SUCCESS] {total_len}건의 상세 수집 완료!")

        except Exception as e:
            logger.error(f"❌ [CRITICAL] 크롤링 프로세스 실패: {e}")
        finally:
            await browser.close()

# --- [API 엔드포인트] ---

@app.api_route("/", methods=["GET", "HEAD"])
async def root():
    return {"status": "online", "message": "Zai Ace Real Estate Bot is Active"}

@app.api_route("/run-crawl", methods=["GET", "HEAD"])
async def trigger_crawl(background_tasks: BackgroundTasks):
    global last_crawl_time
    now = datetime.now()

    # 12시간 주기 체크
    if last_crawl_time is None or (now - last_crawl_time).total_seconds() >= CRAWL_INTERVAL_SECONDS:
        last_crawl_time = now
        logger.info(f"🔔 [TRIGGER] 수집 주기 도달. 작업을 시작합니다. (마지막 실행: {last_crawl_time})")
        background_tasks.add_task(run_full_production_crawl)
        return {"status": "started", "next_allowed_after": str(last_crawl_time + timedelta(seconds=CRAWL_INTERVAL_SECONDS))}
    
    # 아직 주기가 안 됐을 때
    remaining = int((CRAWL_INTERVAL_SECONDS - (now - last_crawl_time).total_seconds()) / 60)
    return {"status": "skipping", "message": f"휴식 중입니다. {remaining}분 후에 다시 가능합니다."}

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, proxy_headers=True)