import asyncio
import os
import logging
from datetime import datetime, timedelta
from fastapi import FastAPI, BackgroundTasks
from playwright.async_api import async_playwright
from dotenv import load_dotenv
from supabase import create_client, Client

# 1. 환경 변수 및 로깅 설정
load_dotenv()
app = FastAPI()

logger = logging.getLogger("EstateCrawler")
logger.setLevel(logging.INFO)
if not logger.handlers:
    stream_handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

# 2. 전역 변수 (마지막 실행 시간 기록)
# 서버가 켜져 있는 동안 메모리에 저장됩니다. (UptimeRobot이 계속 깨워주므로 유지됨)
last_crawl_time = None
CRAWL_INTERVAL_SECONDS = 10800  # 3시간 (6시간으로 하려면 21600으로 변경)

# 3. 외부 서비스 설정 (Supabase)
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# 4. 크롤링 설정
LOGIN_URL = "https://www.aipartner.com/integrated/login?serviceCode=1000"
MONITORING_URL = "https://www.aipartner.com/monitoring/monitoring"
USER_ID = os.getenv("AI_PARTNER_ID", "lljh7771")
USER_PW = os.getenv("AI_PARTNER_PW", "")

COMPLEXES = [
    {"cd": "39667", "nm": "동천자이"},
    {"cd": "4912",  "nm": "진산마을삼성5차"},
    {"cd": "16921", "nm": "동천디이스트"}, 
    {" aspiration_cd": "40892", "nm": "동천센트럴자이"}, # 오타 수정 가능성 대비 원본 유지
    {"cd": "40892", "nm": "동천센트럴자이"},
    {"cd": "4918",  "nm": "진산마을삼성7차"},
]

# --- [유틸리티 함수] ---

def format_date(date_str):
    try:
        return datetime.strptime(date_str, "%y.%m.%d").strftime("%Y-%m-%d")
    except:
        return datetime.now().strftime("%Y-%m-%d")

async def extract_data_js(page, complex_nm):
    return await page.evaluate(f"""
        () => {{
            const results = [];
            const rows = Array.from(document.querySelectorAll("#reportTable tbody tr"));
            rows.forEach(row => {{
                const cols = row.querySelectorAll("td");
                const viewBtn = row.querySelector(".naverUrl");
                if (cols.length >= 8 && !row.innerText.includes("없습니다")) {{
                    const t = (idx) => cols[idx] ? cols[idx].innerText.trim() : "";
                    results.push({{
                        "article_no": viewBtn ? viewBtn.getAttribute("data-seq") : "",
                        "complex_nm": "{complex_nm}",
                        "deal_type": t(1),
                        "price": t(2),
                        "dong": t(3),
                        "floor": t(4),
                        "area": t(5),
                        "reg_date": t(6),
                        "agency": t(7),
                        "same_count": t(8).replace("곳", "").trim()
                    }});
                }}
            }});
            return results;
        }}
    """)

# --- [핵심 크롤링 로직] ---

async def run_crawler_logic():
    async with async_playwright() as p:
        logger.info("🚀 [CRAWL] 작업을 시작합니다.")
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(viewport={'width': 1600, 'height': 1000})
        page = await context.new_page()

        try:
            # 로그인 프로세스
            await page.goto(LOGIN_URL, wait_until="networkidle")
            await page.fill('input[placeholder*="아이디"]', USER_ID)
            await page.fill('input[placeholder*="비밀번호"]', USER_PW)
            await page.keyboard.press("Enter")
            await page.wait_for_function("() => window.location.href.includes('home') || window.location.href.includes('monitoring')", timeout=60000)
            
            if "monitoring" not in page.url:
                await page.goto(MONITORING_URL, wait_until="domcontentloaded")
            await page.wait_for_selector("#reportTable", timeout=30000)

            all_collected_data = []
            for info in COMPLEXES:
                logger.info(f"📍 [CRAWL] 단지 수집: {info['nm']}")
                await page.click("#mainAreaText")
                await page.wait_for_timeout(700)
                await page.click(f"a.mainArea[data-cd='{info['cd']}']")
                await page.wait_for_timeout(3500)

                # 100개씩 보기 설정
                try:
                    await page.click("#chkPerPage")
                    await page.wait_for_timeout(500)
                    await page.click("a.perPage[data-cd='100']")
                    await page.wait_for_timeout(4000)
                except: pass

                current_page = 1
                while True:
                    page_data = await extract_data_js(page, info["nm"])
                    all_collected_data.extend(page_data)
                    next_btn = page.locator(".btnArrow.next")
                    if await next_btn.is_visible():
                        next_val = await next_btn.get_attribute("data-value")
                        if next_val and int(next_val) > current_page:
                            await next_btn.click()
                            current_page = int(next_val)
                            await page.wait_for_timeout(3000)
                        else: break
                    else: break

            # Supabase 저장 (Upsert)
            if all_collected_data:
                logger.info(f"📤 [DB] {len(all_collected_data)}건 동기화 시작")
                upload_list = [{
                    "article_no": item["article_no"],
                    "articlename": f"{item['complex_nm']} {item['area']}",
                    "realestatetypename": "아파트",
                    "tradetypename": item["deal_type"],
                    "floorinfo": item["floor"],
                    "dealorwarrantprc": item["price"],
                    "articleconfirmymd": format_date(item["reg_date"]),
                    "articlefeaturedesc": f"{item['dong']} / 동일 {item['same_count']}",
                    "buildingname": item["complex_nm"],
                    "realtorname": item["agency"],
                    "cppcarticleurl": f"https://new.land.naver.com/?articleNo={item['article_no']}",
                    "isPopular": False
                } for item in all_collected_data if item["article_no"]]

                for i in range(0, len(upload_list), 100):
                    chunk = upload_list[i:i + 100]
                    supabase.table("real_estate_articles").upsert(chunk, on_conflict="article_no").execute()
                logger.info("✨ [DB] 동기화 완료")

        except Exception as e:
            logger.error(f"❌ [ERROR] 크롤링 실패: {e}")
        finally:
            await browser.close()
            logger.info("🔚 [CRAWL] 프로세스 종료")

# --- [API 엔드포인트] ---

@app.api_route("/", methods=["GET", "HEAD"])
async def root():
    """서버 유지용 (UptimeRobot 5분 간격 권장)"""
    return {"status": "online", "message": "Server is awake"}

@app.api_route("/run-crawl", methods=["GET", "HEAD"])
async def trigger_crawl(background_tasks: BackgroundTasks):
    """실제 크롤링 실행 (시간 필터 적용)"""
    global last_crawl_time
    now = datetime.now()

    # 처음 실행하거나, 설정한 주기(3시간)가 지났을 때만 실행
    if last_crawl_time is None or (now - last_crawl_time).total_seconds() >= CRAWL_INTERVAL_SECONDS:
        logger.info(f"🔔 [TRIGGER] 주기 도달 ({CRAWL_INTERVAL_SECONDS}초). 크롤링을 시작합니다.")
        last_crawl_time = now
        background_tasks.add_task(run_crawler_logic)
        return {"status": "started", "last_run": str(last_crawl_time)}
    else:
        # 아직 주기가 되지 않았으면 로그만 남기고 스킵
        next_run = last_crawl_time + timedelta(seconds=CRAWL_INTERVAL_SECONDS)
        remaining = int((next_run - now).total_seconds() / 60)
        logger.info(f"☕ [SKIP] 아직 휴식 중입니다. (다음 실행까지 약 {remaining}분 남음)")
        return {
            "status": "skipping", 
            "message": f"Too early. Next run in {remaining} minutes.",
            "last_run": str(last_crawl_time)
        }

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, proxy_headers=True)