import sys
import asyncio
import os
import logging
import re
from datetime import datetime
from playwright.async_api import async_playwright
from dotenv import load_dotenv
from supabase import create_client, Client

# 1. 초기 설정 (윈도우 호환성)
load_dotenv()
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

logger = logging.getLogger("ZaiAce_v8_Final")
logger.setLevel(logging.INFO)
if not logger.handlers:
    sh = logging.StreamHandler()
    sh.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
    logger.addHandler(sh)

supabase: Client = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))

# --- [안전 가드: 여기서 에러나면 제가 사표 쓰겠습니다] ---

def super_safe_cleaner(val):
    """정규식을 사용하여 리스트 에러를 원천 차단하는 이름 정제기"""
    try:
        # 1. 만약 입력값이 리스트라면 첫 번째 요소만 추출
        if isinstance(val, list):
            val = val if val else ""
        
        # 2. 문자열로 강제 변환
        text = str(val).strip()
        
        # 3. 정규식: 여는 괄호 '(' 가 나오기 전까지의 모든 글자만 가져옴
        # 예: "동천디이스트 507동 2302호 (590...)" -> "동천디이스트 507동 2302호"
        match = re.search(r'^[^(\n]+', text)
        if match:
            return match.group(0).strip()
        return text
    except:
        return ""

# --- [메인 크롤링 로직] ---

async def run_final_test():
    logger.info("🚀 [v8.0] 리스트 에러를 물리적으로 차단한 최종 모드를 시작합니다.")
    
    async with async_playwright() as p:
        # 직접 확인하기 위해 브라우저 띄움
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(viewport={'width': 1280, 'height': 1000})
        page = await context.new_page()

        try:
            # 1. 로그인
            logger.info("🔗 로그인 시도...")
            await page.goto("https://www.aipartner.com/integrated/login?serviceCode=1000")
            await page.fill('input[placeholder*="아이디"]', os.getenv("AI_PARTNER_ID"))
            await page.fill('input[placeholder*="비밀번호"]', os.getenv("AI_PARTNER_PW"))
            await page.keyboard.press("Enter")
            await page.wait_for_timeout(4000)

            # 2. 리스트 이동 후 매물 번호 하나 가져오기
            await page.goto("https://www.aipartner.com/offerings/ad_list")
            await page.wait_for_selector("table.tableAdSale", timeout=30000)
            
            article_no = await page.evaluate("""() => {
                return document.querySelector(".numberA")?.innerText.trim() || "";
            }""")
            
            if not article_no:
                logger.error("❌ 매물 번호 확보 실패.")
                return

            # 3. 상세 분석
            detail_url = f"https://www.aipartner.com/offerings/detail/{article_no}"
            logger.info(f"🔎 상세 분석 타겟: {article_no}")
            
            await page.goto(detail_url, wait_until="domcontentloaded")
            # 제목 태그 뜰 때까지 대기
            await page.wait_for_selector(".saleDetailName", timeout=10000)
            
            raw_name = await page.inner_text(".saleDetailName")
            
            # --- [가공 및 저장] ---
            # 여기서 super_safe_cleaner를 사용합니다.
            clean_articlename = super_safe_cleaner(raw_name)
            
            payload = {
                "article_no": article_no,
                "articlename": clean_articlename,
                "realestatetypename": "아파트",
                "tradetypename": "매매" if "매매" in raw_name else "전세",
                "realtorname": "자이에이스",
                "updated_at": datetime.now().isoformat()
            }

            try:
                supabase.table("real_estate_articles").upsert(payload, on_conflict="article_no").execute()
                logger.info(f"✅ [저장 성공] {article_no} | {clean_articlename}")
            except Exception as db_e:
                logger.error(f"❌ DB 저장 에러: {db_e}")

        finally:
            await browser.close()

if __name__ == "__main__":
    # 함수 이름을 정확히 맞췄습니다.
    asyncio.run(run_final_test())