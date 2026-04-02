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

# 2. 전역 변수 및 설정 (12시간 주기)
last_crawl_time = None
CRAWL_INTERVAL_SECONDS = 43200  # 12시간

# 환경 변수 로드
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

USER_ID = os.getenv("AI_PARTNER_ID")
USER_PW = os.getenv("AI_PARTNER_PW")

# --- [유틸리티 함수] ---

def clean_text(text):
    """숫자 외 단위 및 콤마 제거"""
    if not text: return ""
    return text.replace("개", "").replace("대", "").replace("원", "").replace("만원", "").replace(",", "").strip()

def safe_format_date(date_str):
    """날짜 형식 변환"""
    try:
        raw = date_str.split('~').strip() if '~' in date_str else date_str.strip()
        return datetime.strptime(raw, "%y.%m.%d").strftime("%Y-%m-%d")
    except:
        return datetime.now().strftime("%Y-%m-%d")

async def take_debug_screenshot(page, name):
    """에러 발생 시 화면을 로그에 남김"""
    try:
        screenshot_bytes = await page.screenshot(type="jpeg", quality=40)
        b64_str = base64.b64encode(screenshot_bytes).decode()
        logger.error(f"📸 [{name}] 화면 로그: data:image/jpeg;base64,{b64_str}")
    except: pass

# --- [핵심 크롤링 로직] ---

async def run_full_production_crawl():
    logger.info("🚀 [CRAWL] 상세 정보 포함 정밀 수집을 시작합니다.")
    
    async with async_playwright() as p:
        # 가벼운 실행 옵션
        browser = await p.chromium.launch(headless=True, args=['--no-sandbox', '--disable-setuid-sandbox'])
        context = await browser.new_context(
            viewport={'width': 1280, 'height': 1000},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
        )
        page = await context.new_page()

        try:
            # 1. 로그인 단계
            logger.info("🔗 로그인 페이지 접속...")
            await page.goto("https://www.aipartner.com/integrated/login?serviceCode=1000", wait_until="domcontentloaded")
            await page.fill('input[placeholder*="아이디"]', USER_ID)
            await page.fill('input[placeholder*="비밀번호"]', USER_PW)
            
            logger.info("⏳ 로그인 요청 및 대시보드 안착 대기...")
            async with page.expect_navigation(wait_until="load", timeout=60000):
                await page.keyboard.press("Enter")
            
            await page.wait_for_timeout(5000) # 정착 시간

            # 2. 방해 요소(팝업) 제거
            logger.info("Sweep: 🧹 모든 팝업 및 레이어 제거 시도...")
            try:
                await page.evaluate("""() => {
                    const selectors = ['.close', '.btnClose', '.btn-close', '.not-today', '.SYlayerPopupWrap', '#aipartner-popup-layout'];
                    selectors.forEach(s => {
                        document.querySelectorAll(s).forEach(el => {
                            if(el.click && el.tagName !== 'DIV') el.click();
                            el.style.display = 'none';
                        });
                    });
                }""")
            except: pass

            # 3. 매물 리스트 페이지 이동
            logger.info("🔗 매물 리스트 페이지로 이동합니다.")
            await page.goto("https://www.aipartner.com/offerings/ad_list", wait_until="load", timeout=90000)
            await page.wait_for_timeout(5000)

            logger.info("⏳ 테이블 로딩 대기 중...")
            try:
                await page.wait_for_selector("table.tableAdSale tbody tr", timeout=60000)
            except Exception:
                await take_debug_screenshot(page, "List_Fail")
                raise Exception("리스트 테이블을 찾지 못했습니다.")

            # 100개씩 보기 설정
            try:
                await page.click(".sortingWrap a.selectInfoOrder", timeout=10000)
                await page.wait_for_timeout(1000)
                await page.click("a.perPage[data-cd='100']", timeout=10000)
                await page.wait_for_load_state("networkidle")
                await page.wait_for_timeout(5000)
            except: pass

            # 4. 수집 대상 매물 번호 확보
            list_items = await page.evaluate("""() => {
                return Array.from(document.querySelectorAll("table.tableAdSale tbody tr"))
                    .map(row => ({ "article_no": row.querySelector(".numberA")?.innerText.trim() || "" }))
                    .filter(i => i.article_no.length > 3);
            }""")

            total_len = len(list_items)
            logger.info(f"📊 총 {total_len}건 수집 시작.")

            # 5. 상세 페이지 순회 분석
            for idx, item in enumerate(list_items):
                article_no = item['article_no']
                detail_url = f"https://www.aipartner.com/offerings/detail/{article_no}"
                
                # 차단 방지를 위해 천천히 이동
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
                        res.move = getV("입주 가능일");
                        res.floor = getV("층");
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
                        return res;
                    }""")

                    # 데이터 가공
                    floor_match = re.findall(r'\d+', details.get('floor', ''))
                    curr_f = floor_match if floor_match else ""
                    total_f = floor_match[-1] if len(floor_match) > 1 else ""

                    payload = {
                        "article_no": article_no,
                        "articlename": details['name'],
                        "realestatetypename": "아파트",
                        "tradetypename": "매매" if "매매" in details['name'] else "전세",
                        "dealorwarrantprc": clean_text(details['price']),
                        "articleconfirmymd": safe_format_date(details['date']),
                        "buildingname": details['name'].split(' '),
                        "floorinfo": details['name'].split(' ')[-1],
                        "room_count": clean_text(details['room']),
                        "bath_count": clean_text(details['bath']),
                        "current_floor": curr_f,
                        "total_floors": total_f,
                        "direction": details['dir'],
                        "entrance_type": details['ent'],
                        "parking_total": clean_text(details['p_total']),
                        "parking_per_unit": clean_text(details['p_per']),
                        "heat_type": details['heat'],
                        "maintenance_fee": clean_text(details['fee']),
                        "move_in_date": details['move'],
                        "feature_desc": details['feat'],
                        "description": details['memo'],
                        "realtorname": "자이에이스",
                        "cppcarticleurl": f"https://new.land.naver.com/?articleNo={article_no}",
                        "updated_at": datetime.now().isoformat()
                    }
                    
                    supabase.table("real_estate_articles").upsert(payload, on_conflict="article_no").execute()

                except Exception as e:
                    logger.error(f"⚠️ {article_no} 상세 페이지 스킵: {e}")
                    continue

            logger.info(f"✨ [SUCCESS] {total_len}건 수집 및 저장 완료!")

        except Exception as e:
            logger.error(f"❌ [CRITICAL] 프로세스 실패: {e}")
            await take_debug_screenshot(page, "Critical_Error")
        finally:
            await browser.close()

# --- [FastAPI 엔드포인트] ---

@app.api_route("/", methods=["GET", "HEAD"])
async def root():
    return {"status": "online", "bot": "Zai Ace Bot is Awake"}

@app.api_route("/run-crawl", methods=["GET", "HEAD"])
async def trigger_crawl(background_tasks: BackgroundTasks):
    global last_crawl_time
    now = datetime.now()

    if last_crawl_time is None or (now - last_crawl_time).total_seconds() >= CRAWL_INTERVAL_SECONDS:
        last_crawl_time = now
        logger.info(f"🔔 [TRIGGER] 수집 주기가 되었습니다. 작업을 실행합니다.")
        background_tasks.add_task(run_full_production_crawl)
        return {"status": "started", "last_run": str(last_crawl_time)}
    
    remaining = int((CRAWL_INTERVAL_SECONDS - (now - last_crawl_time).total_seconds()) / 60)
    return {"status": "skipping", "message": f"휴식 중입니다. {remaining}분 뒤에 오세요."}

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, proxy_headers=True)