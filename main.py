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
    try:
        screenshot_bytes = await page.screenshot(type="jpeg", quality=40)
        b64_str = base64.b64encode(screenshot_bytes).decode()
        logger.error(f"📸 [{name}] 화면 확인용 로그: data:image/jpeg;base64,{b64_str}")
    except: pass

# --- [핵심 로직] ---

async def run_production_crawl():
    logger.info("🚀 [CRAWL] 정밀 수집 프로세스를 가동합니다.")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True, args=['--no-sandbox'])
        context = await browser.new_context(viewport={'width': 1280, 'height': 1000})
        page = await context.new_page()

        try:
            # 1. 로그인
            logger.info("🔗 로그인 시도...")
            await page.goto("https://www.aipartner.com/integrated/login?serviceCode=1000", wait_until="domcontentloaded")
            await page.fill('input[placeholder*="아이디"]', USER_ID)
            await page.fill('input[placeholder*="비밀번호"]', USER_PW)
            await page.keyboard.press("Enter")
            
            # [중요] 로그인 후 '로그아웃' 버튼이나 '대시보드' 요소가 보일 때까지 대기
            logger.info("⏳ 로그인 승인 대기 중...")
            await page.wait_for_timeout(10000) 
            
            # 2. 팝업 제거 (화면을 가리는 모든 레이어 팝업 닫기 시도)
            logger.info("🧹 방해 요소(팝업) 제거 중...")
            await page.evaluate("""() => {
                const closeBtns = document.querySelectorAll('.close, .btnClose, .btn-close, [class*="close"]');
                closeBtns.forEach(btn => btn.click());
                // 배경 어두워지는 레이어 강제 삭제
                const overlays = document.querySelectorAll('.SYlayerPopupWrap, .modal-backdrop, [id*="popup"]');
                overlays.forEach(ov => ov.style.display = 'none');
            }""")
            
            # 3. 매물 리스트 페이지 이동
            logger.info("🔗 매물 리스트 페이지로 이동합니다.")
            await page.goto("https://www.aipartner.com/offerings/ad_list", wait_until="load", timeout=90000)
            
            # 테이블 로딩 확인 전 한 번 더 팝업 제거
            await page.wait_for_timeout(3000)
            await page.evaluate("() => document.querySelectorAll('.SYlayerPopupWrap').forEach(p => p.remove())")

            logger.info("⏳ 테이블 렌더링 확인 중...")
            try:
                # 'table' 보다는 실제 데이터가 들어있는 행(tr)이 생길 때까지 대기
                await page.wait_for_selector("table tbody tr", timeout=60000)
            except Exception:
                await take_debug_screenshot(page, "List_Fail")
                raise Exception("리스트 테이블을 찾을 수 없습니다. (스크린샷 확인 권장)")

            # 100개씩 보기 (실패해도 데이터 수집은 진행)
            try:
                await page.click(".sortingWrap a.selectInfoOrder", timeout=10000)
                await page.click("a.perPage[data-cd='100']", timeout=10000)
                await page.wait_for_timeout(5000)
            except: pass

            # 4. 매물번호 리스트 확보
            list_items = await page.evaluate("""() => {
                return Array.from(document.querySelectorAll("table.tableAdSale tbody tr"))
                    .map(row => ({ "article_no": row.querySelector(".numberA")?.innerText.trim() || "" }))
                    .filter(i => i.article_no.length > 3);
            }""")

            total_len = len(list_items)
            logger.info(f"📊 수집 대상 {total_len}건 확보. 상세 분석 루프 진입.")

            # 5. 상세 페이지 순회
            for idx, item in enumerate(list_items):
                article_no = item['article_no']
                detail_url = f"https://www.aipartner.com/offerings/detail/{article_no}"
                
                await asyncio.sleep(random.uniform(5.0, 10.0))
                logger.info(f"🔎 [{idx+1}/{total_len}] 매물번호: {article_no}")
                
                try:
                    await page.goto(detail_url, wait_until="domcontentloaded", timeout=60000)
                    await page.wait_for_timeout(3000)

                    # 상세 데이터 추출
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
                    logger.error(f"⚠️ {article_no} 상세 페이지 스킵: {e}")
                    continue

            logger.info(f"✨ [SUCCESS] {total_len}건 수집 완료!")
        except Exception as e:
            logger.error(f"❌ [CRITICAL] 수집 프로세스 중단: {e}")
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
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, proxy_headers=True)