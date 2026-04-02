import asyncio
import os
import logging
from datetime import datetime, timedelta
from fastapi import FastAPI, BackgroundTasks
from playwright.async_api import async_playwright
from dotenv import load_dotenv
from supabase import create_client, Client

# 1. 초기 설정
load_dotenv()
app = FastAPI()

logger = logging.getLogger("EstateCrawler")
logger.setLevel(logging.INFO)
if not logger.handlers:
    stream_handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

# 2. 전역 변수 및 설정
last_crawl_time = None
CRAWL_INTERVAL_SECONDS = 10800  # 3시간 주기
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# 3. 크롤링 대상 설정
LOGIN_URL = "https://www.aipartner.com/integrated/login?serviceCode=1000"
MONITORING_URL = "https://www.aipartner.com/monitoring/monitoring"
MY_ADS_URL = "https://www.aipartner.com/offerings/ad_list" # 새로 추가된 타켓

USER_ID = os.getenv("AI_PARTNER_ID", "lljh7771")
USER_PW = os.getenv("AI_PARTNER_PW", "")

COMPLEXES = [
    {"cd": "39667", "nm": "동천자이"},
    {"cd": "4912",  "nm": "진산마을삼성5차"},
    {"cd": "16921", "nm": "동천디이스트"}, 
    {"cd": "40892", "nm": "동천센트럴자이"},
    {"cd": "4918",  "nm": "진산마을삼성7차"},
]

# --- [유틸리티 함수] ---

def format_date(date_str):
    try:
        return datetime.strptime(date_str, "%y.%m.%d").strftime("%Y-%m-%d")
    except:
        return datetime.now().strftime("%Y-%m-%d")

async def save_to_supabase(all_data, source_name):
    """공통 저장 함수"""
    if not all_data:
        logger.warning(f"⚠️ [{source_name}] 저장할 데이터가 없습니다.")
        return

    logger.info(f"📤 [{source_name}] {len(all_data)}건 DB 동기화 시도")
    upload_list = [{
        "article_no": item["article_no"],
        "articlename": item.get("complex_nm", "아파트 매물"),
        "realestatetypename": "아파트",
        "tradetypename": item.get("deal_type"),
        "floorinfo": item.get("floor"),
        "dealorwarrantprc": item.get("price"),
        "articleconfirmymd": format_date(item.get("reg_date", "")),
        "articlefeaturedesc": f"{item.get('dong', '')} / {source_name}",
        "buildingname": item.get("complex_nm", "").split(' '),
        "realtorname": item.get("agency", "자이에이스"),
        "cppcarticleurl": f"https://new.land.naver.com/?articleNo={item['article_no']}" if item.get("article_no") else "",
        "isPopular": False
    } for item in all_data if item.get("article_no")]

    try:
        for i in range(0, len(upload_list), 100):
            chunk = upload_list[i:i + 100]
            supabase.table("real_estate_articles").upsert(chunk, on_conflict="article_no").execute()
        logger.info(f"✨ [{source_name}] DB 업데이트 완료!")
    except Exception as e:
        logger.error(f"❌ [DB ERROR] {e}")

# --- [핵심 로직: 모니터링 페이지용] ---

async def crawl_monitoring():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        try:
            # 로그인
            await page.goto(LOGIN_URL)
            await page.fill('input[placeholder*="아이디"]', USER_ID); await page.fill('input[placeholder*="비밀번호"]', USER_PW)
            await page.keyboard.press("Enter")
            await page.wait_for_url("**/monitoring/**", timeout=60000)
            
            all_data = []
            for info in COMPLEXES:
                logger.info(f"📍 단지 수집: {info['nm']}")
                await page.click("#mainAreaText")
                await page.wait_for_timeout(500)
                await page.click(f"a.mainArea[data-cd='{info['cd']}']")
                await page.wait_for_timeout(3500)
                
                # 데이터 추출 JS (기존 로직 유지)
                page_data = await page.evaluate(f"""() => {{
                    return Array.from(document.querySelectorAll("#reportTable tbody tr")).map(row => {{
                        const cols = row.querySelectorAll("td");
                        return {{
                            "article_no": row.querySelector(".naverUrl")?.getAttribute("data-seq") || "",
                            "complex_nm": "{info['nm']}",
                            "deal_type": cols?.innerText.trim(),
                            "price": cols?.innerText.trim(),
                            "dong": cols?.innerText.trim(),
                            "floor": cols?.innerText.trim(),
                            "reg_date": cols?.innerText.trim(),
                            "agency": cols?.innerText.trim()
                        }};
                    }}).filter(item => item.article_no);
                }}""")
                all_data.extend(page_data)
            
            await save_to_supabase(all_data, "모니터링")
        finally:
            await browser.close()

# --- [핵심 로직: 내 매물 관리(ad_list) 테스트용] ---

async def crawl_ad_list():
    async with async_playwright() as p:
        logger.info("🚀 [TEST] 내 매물 관리(ad_list) 수집 시작")
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        try:
            # 로그인 및 이동
            await page.goto(LOGIN_URL)
            await page.fill('input[placeholder*="아이디"]', USER_ID); await page.fill('input[placeholder*="비밀번호"]', USER_PW)
            await page.keyboard.press("Enter")
            await page.wait_for_timeout(3000)
            await page.goto(MY_ADS_URL, wait_until="networkidle")
            
            # 테이블 데이터 긁기
            all_data = await page.evaluate("""() => {
                const rows = Array.from(document.querySelectorAll("table tbody tr"));
                return rows.map(row => {
                    const cols = row.querySelectorAll("td");
                    return {
                        "article_no": row.querySelector("[data-seq], a")?.innerText.replace(/[^0-9]/g, ""),
                        "complex_nm": cols?.innerText.trim(),
                        "deal_type": cols?.innerText.trim(),
                        "price": cols?.innerText.trim(),
                        "floor": cols?.innerText.trim(),
                        "reg_date": cols?.innerText.trim()
                    };
                }).filter(i => i.article_no);
            }""")
            
            await save_to_supabase(all_data, "내매물관리")
        finally:
            await browser.close()

# --- [API 엔드포인트] ---

@app.api_route("/", methods=["GET", "HEAD"])
async def root(): return {"status": "online"}

@app.api_route("/run-crawl", methods=["GET", "HEAD"])
async def trigger_crawl(background_tasks: BackgroundTasks):
    """정식 주기 수집 (3시간 마다)"""
    global last_crawl_time
    now = datetime.now()
    if last_crawl_time is None or (now - last_crawl_time).total_seconds() >= CRAWL_INTERVAL_SECONDS:
        last_crawl_time = now
        background_tasks.add_task(crawl_monitoring)
        return {"status": "started", "last_run": str(last_crawl_time)}
    return {"status": "skipping", "message": "휴식 중"}

@app.api_route("/test-crawl", methods=["GET", "HEAD"])
async def trigger_test(background_tasks: BackgroundTasks):
    """새로운 ad_list 페이지 수집 테스트"""
    logger.info("📡 [TEST] 내 매물 페이지 수집 수동 호출")
    background_tasks.add_task(crawl_ad_list)
    return {"status": "test_started", "target": "ad_list"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.environ.get("PORT", 8000)), proxy_headers=True)