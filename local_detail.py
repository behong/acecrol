import sys
import asyncio
import os
import logging
import random
import re
from datetime import datetime
from playwright.async_api import async_playwright
from dotenv import load_dotenv
from supabase import create_client, Client

# 윈도우 환경 asyncio 호환성 설정
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

# 로깅 설정
load_dotenv()
logger = logging.getLogger("DetailCrawler")
logger.setLevel(logging.INFO)
if not logger.handlers:
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
    logger.addHandler(stream_handler)

# Supabase 설정
supabase: Client = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))

def clean_text(text):
    """숫자와 점만 남기거나 불필요한 단위 제거"""
    if not text: return ""
    return text.replace("개", "").replace("대", "").replace("원", "").replace("만원", "").replace(",", "").strip()

async def run_final_detail_crawl():
    logger.info("🚀 [FINAL DETAIL] 상세 페이지 정밀 분석 수집을 시작합니다.")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False) # 눈으로 확인 모드
        context = await browser.new_context(viewport={'width': 1600, 'height': 1200})
        page = await context.new_page()

        try:
            # 1. 로그인
            logger.info("🔗 로그인 진행 중...")
            await page.goto("https://www.aipartner.com/integrated/login?serviceCode=1000")
            await page.fill('input[placeholder*="아이디"]', os.getenv("AI_PARTNER_ID"))
            await page.fill('input[placeholder*="비밀번호"]', os.getenv("AI_PARTNER_PW"))
            await page.keyboard.press("Enter")
            await page.wait_for_timeout(5000)
            
            # 2. 리스트 페이지에서 대상 매물 번호 확보
            await page.goto("https://www.aipartner.com/offerings/ad_list", wait_until="domcontentloaded")
            await page.wait_for_selector("table.tableAdSale", timeout=30000)

            list_items = await page.evaluate("""() => {
                return Array.from(document.querySelectorAll("table.tableAdSale tbody tr")).map(row => ({
                    "article_no": row.querySelector(".numberA")?.innerText.trim() || ""
                })).filter(i => i.article_no.length > 3);
            }""")

            # 테스트용으로 상위 3개만 진행
            target_items = list_items[:3]
            logger.info(f"📊 총 {len(list_items)}건 중 3건에 대해 상세 수집 테스트를 진행합니다.")

            for idx, item in enumerate(target_items):
                # 사람처럼 보이게 랜덤 휴식
                await asyncio.sleep(random.uniform(3, 5))
                
                article_no = item['article_no']
                detail_url = f"https://www.aipartner.com/offerings/detail/{article_no}"
                logger.info(f"🔎 [{idx+1}/{len(target_items)}] 상세 분석 시작: {article_no}")
                
                await page.goto(detail_url, wait_until="domcontentloaded")
                await page.wait_for_timeout(3000)

                # --- [HTML 기반 데이터 추출 JS] ---
                details = await page.evaluate("""() => {
                    const results = {};
                    // th(라벨)를 찾아서 그 옆의 td(값)를 가져오는 함수
                    const getValue = (label) => {
                        const ths = Array.from(document.querySelectorAll('th'));
                        const targetTh = ths.find(th => th.innerText.includes(label));
                        return targetTh ? targetTh.nextElementSibling.innerText.trim() : "";
                    };

                    results.articlename = document.querySelector(".saleDetailName")?.innerText.trim() || "";
                    results.price = getValue("매물 가격");
                    results.reg_date = getValue("등록일");
                    results.move_in = getValue("입주 가능일");
                    results.floors = getValue("층"); // "해당 24층[중 / 36층]"
                    results.rooms = getValue("방수");
                    results.baths = getValue("욕실수");
                    results.direction = getValue("방향");
                    results.entrance = getValue("현관구조");
                    results.parking_total = getValue("총 주차대수");
                    results.parking_per = getValue("세대당주차대수");
                    results.heat = getValue("난방시설");
                    results.fee = document.querySelector(".price-wrap.total .price")?.innerText.trim() || "0";
                    results.feature = getValue("매물특징");
                    results.memo = document.querySelector("textarea")?.value || ""; // 매물설명

                    return results;
                }""")

                # --- [데이터 가공: 층수/가격 등] ---
                # 층수 분리 예: "해당 24층[중 / 36층]" -> current: 24, total: 36
                floor_match = re.findall(r'\d+', details['floors'])
                curr_f = floor_match if len(floor_match) > 0 else ""
                total_f = floor_match[-1] if len(floor_match) > 1 else ""

                payload = {
                    "article_no": article_no,
                    "articlename": details['articlename'],
                    "realestatetypename": "아파트",
                    "tradetypename": "매매" if "매매" in details['articlename'] else "전세",
                    "dealorwarrantprc": clean_text(details['price']),
                    "articleconfirmymd": details['reg_date'].replace(".", "-"),
                    "buildingname": details['articlename'].split(' '),
                    "floorinfo": details['articlename'].split(' ')[-1],
                    
                    # 상세 정보
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

                # 3. Supabase 저장
                try:
                    supabase.table("real_estate_articles").upsert(payload, on_conflict="article_no").execute()
                    logger.info(f"   ✨ 저장 완료: {article_no} (방{payload['room_count']}, {payload['direction']})")
                except Exception as e:
                    logger.error(f"   ❌ DB 저장 실패: {e}")

            logger.info("🎉 모든 정밀 분석 테스트가 완료되었습니다!")

        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(run_final_detail_crawl())