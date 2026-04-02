import asyncio
import os
import logging
from datetime import datetime
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

# 2. 외부 서비스 설정 (Supabase)
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# 3. 크롤링 대상 및 주소 설정
LOGIN_URL = "https://www.aipartner.com/integrated/login?serviceCode=1000"
MONITORING_URL = "https://www.aipartner.com/monitoring/monitoring"
USER_ID = os.getenv("AI_PARTNER_ID", "lljh7771")
USER_PW = os.getenv("AI_PARTNER_PW", "")

COMPLEXES = [
    {"cd": "39667", "nm": "동천자이"},
    {"cd": "4912",  "nm": "진산마을삼성5차"},
    {"cd": "16921", "nm": "동천디이스트"}, 
    {"cd": "40892", "nm": "동천센트럴자이"},
    {"cd": "4918",  "nm": "진산마을삼성7차"},
]

# --- [기능별 유틸리티 함수] ---

def format_date(date_str):
    """'26.04.01' -> '2026-04-01' 변환"""
    try:
        return datetime.strptime(date_str, "%y.%m.%d").strftime("%Y-%m-%d")
    except:
        return datetime.now().strftime("%Y-%m-%d")

async def login_process(page):
    """로그인 및 수집 페이지 진입"""
    logger.info(f"🔗 로그인 시도 중... (ID: {USER_ID})")
    await page.goto(LOGIN_URL, wait_until="networkidle")
    await page.fill('input[placeholder*="아이디"]', USER_ID)
    await page.fill('input[placeholder*="비밀번호"]', USER_PW)
    await page.keyboard.press("Enter")
    
    await page.wait_for_function(
        "() => window.location.href.includes('home') || window.location.href.includes('monitoring')", 
        timeout=60000
    )
    
    if "monitoring" not in page.url:
        await page.goto(MONITORING_URL, wait_until="domcontentloaded")
    
    await page.wait_for_selector("#reportTable", timeout=30000)
    logger.info("✅ 로그인 및 수집 페이지 도착")

async def set_view_to_100(page):
    """100개씩 보기 설정"""
    try:
        await page.click("#chkPerPage")
        await page.wait_for_timeout(500)
        await page.click("a.perPage[data-cd='100']")
        await page.wait_for_timeout(4000)
        logger.info("⚡ '100개씩 보기' 설정 완료")
    except Exception as e:
        logger.warning(f"⚠️ 보기 설정 변경 실패: {e}")

async def extract_current_page(page, complex_nm):
    """데이터 추출 JS"""
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

async def save_to_supabase(all_data):
    """Supabase Upsert"""
    if not all_data:
        return

    logger.info(f"📤 Supabase 동기화 시도 (총 {len(all_data)}건)")
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
    } for item in all_data if item["article_no"]]

    try:
        for i in range(0, len(upload_list), 100):
            chunk = upload_list[i:i + 100]
            supabase.table("real_estate_articles").upsert(chunk, on_conflict="article_no").execute()
            logger.info(f"✅ {min(i + 100, len(upload_list))}건 처리 완료")
        logger.info("✨ 모든 데이터 동기화 완료")
    except Exception as e:
        logger.error(f"❌ Supabase 저장 실패: {e}")

# --- [핵심 실행 로직] ---

async def run_crawler_logic():
    async with async_playwright() as p:
        logger.info("🚀 크롤링 프로세스 시작")
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(viewport={'width': 1600, 'height': 1000})
        page = await context.new_page()

        try:
            await login_process(page)
            all_collected_data = []
            for info in COMPLEXES:
                logger.info(f"📍 단지 수집 시작: {info['nm']}")
                await page.click("#mainAreaText")
                await page.wait_for_timeout(700)
                await page.click(f"a.mainArea[data-cd='{info['cd']}']")
                await page.wait_for_timeout(3500)
                await set_view_to_100(page)

                current_page = 1
                while True:
                    page_data = await extract_current_page(page, info["nm"])
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
            await save_to_supabase(all_collected_data)
        except Exception as e:
            logger.error(f"❌ 에러 발생: {e}")
        finally:
            await browser.close()
            logger.info("🔚 크롤링 프로세스 종료")

# --- [FastAPI 엔드포인트: HEAD 메서드 허용 추가] ---

@app.get("/", methods=["GET", "HEAD"])
async def root():
    """서버 상태 확인용 (UptimeRobot 에러 방지)"""
    return {"status": "online", "service": "Estate Crawler"}

@app.get("/run-crawl", methods=["GET", "HEAD"])
async def trigger_crawl(background_tasks: BackgroundTasks):
    """크롤링 시작용 (UptimeRobot 에러 방지)"""
    logger.info("📡 외부 API 호출 감지 (GET/HEAD) - 작업을 예약합니다.")
    background_tasks.add_task(run_crawler_logic)
    return {"status": "started", "message": "Crawler started in background"}

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)