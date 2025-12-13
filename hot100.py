import streamlit as st
import requests
from bs4 import BeautifulSoup
import re
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import pandas as pd


# ==========================================
# 1. 크롤링 엔진 (기존 로직 유지 + 캐싱 적용)
# ==========================================
class NaverThemeGrouper:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Referer': 'https://finance.naver.com/',
        }

    def get_soup(self, url):
        try:
            res = requests.get(url, headers=self.headers, timeout=5)
            content = res.content
            # 인코딩 처리
            for enc in ['cp949', 'euc-kr', 'utf-8']:
                try:
                    return BeautifulSoup(content.decode(enc), 'html.parser')
                except:
                    continue
            return BeautifulSoup(content.decode('utf-8', 'ignore'), 'html.parser')
        except:
            return None

    def get_top_100_stocks(self):
        base_url = "https://finance.naver.com/sise/sise_rise.naver?sosok={}"
        all_stocks = {}

        for sosok in [0, 1]:  # 코스피, 코스닥
            soup = self.get_soup(base_url.format(sosok))
            if not soup: continue

            rows = soup.select('table.type_2 tr')
            for row in rows:
                cols = row.find_all('td')
                if len(cols) < 10: continue
                try:
                    link = cols[1].find('a')
                    if not link: continue
                    name = link.text.strip()
                    code = link['href'].split('=')[-1]

                    price_txt = cols[2].get_text(strip=True).replace(',', '')
                    price = int(re.search(r'\d+', price_txt).group()) if re.search(r'\d+', price_txt) else 0

                    rate_txt = cols[4].get_text(strip=True)
                    rate_match = re.search(r'[+-]?\d+\.?\d*', rate_txt.replace('%', ''))
                    rate = float(rate_match.group()) if rate_match else 0.0

                    vol_txt = cols[6].get_text(strip=True).replace(',', '')
                    volume = int(re.search(r'\d+', vol_txt).group()) if re.search(r'\d+', vol_txt) else 0

                    if volume > 1000:
                        all_stocks[code] = {
                            'code': code, 'name': name, 'price': price,
                            'rate': rate, 'volume': volume,
                            'link': f"https://finance.naver.com/item/main.naver?code={code}"  # 링크 추가
                        }
                except:
                    continue

        return sorted(all_stocks.values(), key=lambda x: x['rate'], reverse=True)[:100]

    def get_all_themes_list(self, max_pages=7):
        themes = []
        base_url = "https://finance.naver.com/sise/theme.naver?&page={}"

        for page in range(1, max_pages + 1):
            soup = self.get_soup(base_url.format(page))
            if not soup: break

            rows = soup.select('table.type_1 tr')
            found_on_page = False
            for row in rows:
                cols = row.find_all('td')
                if len(cols) < 2: continue
                try:
                    link = cols[0].find('a')
                    if not link: continue
                    found_on_page = True
                    themes.append({
                        'name': link.text.strip(),
                        'url': "https://finance.naver.com" + link['href'],
                        'rate': float(re.search(r'[+-]?\d+\.?\d*', cols[1].get_text(strip=True)).group())
                    })
                except:
                    continue
            if not found_on_page: break
        return themes

    def fetch_stocks_in_theme(self, theme_info):
        soup = self.get_soup(theme_info['url'])
        stock_codes = set()
        if soup:
            links = soup.select('table.type_5 tr td a')
            for link in links:
                if 'code=' in link.get('href', ''):
                    stock_codes.add(link['href'].split('=')[-1])
        return {'theme': theme_info['name'], 'theme_rate': theme_info['rate'],
                'codes': stock_codes, 'url': theme_info['url']}

    def match_stocks_to_themes(self, top_100):
        all_themes = self.get_all_themes_list(max_pages=7)

        with ThreadPoolExecutor(max_workers=10) as executor:
            theme_map = list(executor.map(self.fetch_stocks_in_theme, all_themes))

        grouped_data = defaultdict(list)
        covered_stocks = set()
        top_100_dict = {s['code']: s for s in top_100}

        for t_info in theme_map:
            matched_stocks = []
            for code in t_info['codes']:
                if code in top_100_dict:
                    matched_stocks.append(top_100_dict[code])
                    covered_stocks.add(code)

            if matched_stocks:
                matched_stocks.sort(key=lambda x: x['rate'], reverse=True)
                grouped_data[t_info['theme']] = {
                    'theme_rate': t_info['theme_rate'],
                    'stocks': matched_stocks,
                    'url': t_info['url']
                }

        others = [s for s in top_100 if s['code'] not in covered_stocks]
        if others:
            grouped_data['[개별 급등주 / 기타 재료]'] = {
                'theme_rate': 0.0, 'stocks': others, 'url': None
            }

        return grouped_data


# ==========================================
# 2. Streamlit 웹 UI
# ==========================================
st.set_page_config(page_title="Top 100 테마 분석", page_icon="📈", layout="centered")

st.title("📈 TOP 100 급등주 테마 분석")
st.markdown("네이버 금융 실시간 데이터를 기반으로 **테마별 급등주**를 정리합니다.")


# 데이터 로딩 함수 (캐싱 적용으로 속도 향상)
@st.cache_data(ttl=60)  # 60초 동안 데이터 유지
def load_data():
    scraper = NaverThemeGrouper()
    top_100 = scraper.get_top_100_stocks()
    grouped_data = scraper.match_stocks_to_themes(top_100)
    return grouped_data


if st.button("🔄 실시간 분석 시작 (새로고침)", type="primary"):
    st.cache_data.clear()  # 버튼 누르면 캐시 삭제하고 다시 수집

with st.spinner('데이터를 수집하고 분석 중입니다... (약 5초 소요)'):
    try:
        data = load_data()

        # 정렬: 종목 많은 순서대로
        sorted_themes = sorted(data.items(), key=lambda x: len(x[1]['stocks']), reverse=True)

        st.success("분석 완료!")

        for theme_name, info in sorted_themes:
            stocks = info['stocks']
            theme_url = info.get('url')

            # 헤더 텍스트 구성
            if theme_name.startswith('[개별'):
                header_text = f"📂 {theme_name} ({len(stocks)}종목)"
            else:
                header_text = f"🔥 {theme_name} (평균 {info['theme_rate']}%) - {len(stocks)}종목"

            # 아코디언 형태로 펼치기/접기
            with st.expander(header_text, expanded=True if not theme_name.startswith('[개별') else False):
                if theme_url:
                    st.markdown(f"🔗 [네이버 테마 상세 보기]({theme_url})")

                # 데이터프레임으로 변환하여 표 출력
                df = pd.DataFrame(stocks)
                if not df.empty:
                    # 화면에 보여줄 컬럼 선택 및 복사
                    display_df = df[['name', 'rate', 'price', 'volume', 'link']].copy()

                    # [1] 데이터 포맷팅 (문자열 변환) - 기존과 동일
                    display_df['price'] = display_df['price'].apply(lambda x: f"{x:,}원")
                    display_df['volume'] = display_df['volume'].apply(lambda x: f"{x:,}")

                    # [2] 스타일 적용 (우측 정렬 추가)
                    # price와 volume 컬럼의 텍스트 정렬을 'right'로 설정합니다.
                    styled_df = display_df.style.set_properties(
                        subset=['price', 'volume'],
                        **{'text-align': 'right'}
                    )

                    # [3] 표 출력 (styled_df 전달)
                    st.dataframe(
                        styled_df,
                        column_config={
                            "name": "종목명",
                            "rate": st.column_config.NumberColumn("등락률", format="%.2f%%"),
                            "price": st.column_config.TextColumn("현재가"),
                            "volume": st.column_config.TextColumn("거래량"),
                            "link": st.column_config.LinkColumn("상세정보", display_text="네이버이동"),
                        },
                        hide_index=True,
                        use_container_width=True
                    )

    except Exception as e:
        st.error(f"오류가 발생했습니다: {e}")