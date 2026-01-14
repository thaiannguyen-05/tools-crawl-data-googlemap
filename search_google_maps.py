"""
Google Maps Business Scraper
Tự động tìm kiếm và thu thập thông tin doanh nghiệp từ Google Maps
"""

import json
import asyncio
import re
from datetime import datetime
from typing import List, Dict, Optional
from playwright.async_api import async_playwright, Page, BrowserContext, TimeoutError as PlaywrightTimeoutError


class GoogleMapsScraper:
    """Scraper Google Maps sử dụng Playwright"""
    
    def __init__(self, headless: bool = False, concurrent_tabs: int = 5):
        self.headless = headless
        self.concurrent_tabs = concurrent_tabs
        self.max_scroll_attempts = 10  # Số lần scroll tối đa để load hết kết quả
    
    async def search_google_maps(self, query: str, page: Page, context: BrowserContext) -> List[Dict]:
        """
        Tìm kiếm trên Google Maps và lấy danh sách kết quả
        
        Args:
            query: Từ khóa tìm kiếm
            page: Playwright page instance
            context: Browser context for multi-tab processing
            
        Returns:
            List các kết quả business
        """
        from urllib.parse import quote_plus
        
        encoded_query = quote_plus(query)
        maps_url = f"https://www.google.com/maps/search/{encoded_query}"
        
        try:
            print(f"   🗺️  Đang truy cập Google Maps...")
            await page.goto(maps_url, wait_until="domcontentloaded", timeout=60000)
            
            # Đợi kết quả load với smart wait
            print(f"   ⏳ Đang chờ kết quả Maps load...")
            try:
                await page.wait_for_selector('div[role="feed"]', timeout=10000)
                print(f"   ✅ Đã load được danh sách kết quả")
            except:
                print(f"   ⚠️ Không tìm thấy danh sách kết quả")
                return []
            
            # Scroll để load tất cả kết quả
            results_count = await self._scroll_to_load_all(page)
            print(f"   📊 Tổng số kết quả sau khi scroll: {results_count}")
            
            # Parse tất cả kết quả với multi-tab
            businesses = await self._parse_all_results_with_tabs(page, context)
            
            return businesses
            
        except PlaywrightTimeoutError:
            print(f"   ⏱️ Timeout khi load Google Maps")
            return []
        except Exception as e:
            print(f"   ❌ Lỗi: {type(e).__name__}: {e}")
            return []
    
    async def _scroll_to_load_all(self, page: Page) -> int:
        """
        Scroll sidebar để load toàn bộ kết quả
        
        Returns:
            Số lượng kết quả hiện tại
        """
        print(f"   🔄 Đang scroll để load thêm kết quả...")
        
        # Selector cho scrollable container
        # Google Maps có thể thay đổi, thử nhiều selector
        scrollable_selectors = [
            'div[role="feed"]',
            'div.m6QErb',  # Class name có thể thay đổi
            '[aria-label*="Results"]',
        ]
        
        scrollable_elem = None
        for selector in scrollable_selectors:
            elem = await page.query_selector(selector)
            if elem:
                scrollable_elem = elem
                print(f"      ✓ Tìm thấy scrollable container: {selector}")
                break
        
        if not scrollable_elem:
            print(f"      ⚠️ Không tìm thấy scrollable container")
            return 0
        
        try:
            previous_count = 0
            no_change_attempts = 0
            
            for i in range(self.max_scroll_attempts):
                # Scroll xuống
                await page.evaluate('''
                    const scrollable = document.querySelector('div[role="feed"]');
                    if (scrollable) {
                        scrollable.scrollBy(0, scrollable.scrollHeight);
                    }
                ''')
                
                # Đợi load - giảm từ 3s xuống 1.5s
                await asyncio.sleep(1.5)
                
                # Đếm số item
                # Thử nhiều selector
                items = []
                for sel in ['a[href*="/maps/place/"]', 'div[role="article"]', 'a.hfpxzc']:
                    items = await page.query_selector_all(sel)
                    if items:
                        break
                
                current_count = len(items)
                
                if current_count > previous_count:
                    print(f"      ├─ Scroll {i+1}: {current_count} kết quả (+{current_count - previous_count})")
                    previous_count = current_count
                    no_change_attempts = 0
                else:
                    no_change_attempts += 1
                    print(f"      ├─ Scroll {i+1}: {current_count} kết quả (không tăng)")
                    
                    if no_change_attempts >= 3:
                        print(f"      └─ Đã load hết (không tăng sau 3 lần)")
                        break
            
            return previous_count
            
        except Exception as e:
            print(f"   ⚠️ Lỗi scroll: {e}")
            return 0
    
    async def _parse_all_results_with_tabs(self, page: Page, context: BrowserContext) -> List[Dict]:
        """
        Parse tất cả kết quả sử dụng multi-tab parallel processing
        
        Returns:
            List các business info
        """
        businesses = []
        
        try:
            # Thu thập tất cả URLs từ search results
            possible_selectors = [
                'a.hfpxzc',  # Link chính của mỗi business (phổ biến nhất)
                'a[href*="/maps/place/"]',  # Fallback
            ]
            
            urls = []
            used_selector = None
            
            for selector in possible_selectors:
                items = await page.query_selector_all(selector)
                if items and len(items) > 0:
                    used_selector = selector
                    print(f"   ✅ Tìm thấy {len(items)} items với selector: {selector}")
                    
                    # Extract URLs
                    for item in items:
                        href = await item.get_attribute('href')
                        if href and '/maps/place/' in href:
                            urls.append(href)
                    break
            
            if not urls:
                print(f"   ❌ Không tìm thấy business URLs!")
                # Debug: lưu HTML và screenshot
                html_content = await page.content()
                with open('debug_maps.html', 'w', encoding='utf-8') as f:
                    f.write(html_content)
                await page.screenshot(path='debug_maps.png')
                print(f"   💾 Đã lưu debug_maps.html và debug_maps.png")
                return businesses
            
            # Loại bỏ duplicates
            urls = list(dict.fromkeys(urls))
            
            # Giới hạn số lượng
            max_items = min(len(urls), 30)
            urls = urls[:max_items]
            
            print(f"   📝 Sẽ crawl {len(urls)} businesses với {self.concurrent_tabs} tabs song song")
            print(f"   💡 Multi-tab parallel processing...\n")
            
            # Process URLs in batches
            batch_size = self.concurrent_tabs
            total_processed = 0
            
            for batch_idx in range(0, len(urls), batch_size):
                batch_urls = urls[batch_idx:batch_idx + batch_size]
                batch_num = (batch_idx // batch_size) + 1
                total_batches = (len(urls) + batch_size - 1) // batch_size
                
                print(f"   🔄 Batch {batch_num}/{total_batches}: Processing {len(batch_urls)} items in parallel...")
                
                # Process batch in parallel
                tasks = [
                    self._extract_from_url(url, context, total_processed + i + 1, max_items)
                    for i, url in enumerate(batch_urls)
                ]
                
                batch_results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Collect successful results
                for result in batch_results:
                    if isinstance(result, dict) and result.get('name'):
                        businesses.append(result)
                    elif isinstance(result, Exception):
                        print(f"      ⚠️ Error in batch: {result}")
                
                total_processed += len(batch_urls)
                print()
            
            print(f"   ✅ Đã parse thành công {len(businesses)}/{max_items} kết quả")
            return businesses
            
        except Exception as e:
            print(f"   ❌ Lỗi khi parse: {e}")
            import traceback
            traceback.print_exc()
            return businesses
    
    async def _extract_from_url(self, url: str, context: BrowserContext, index: int, total: int) -> Optional[Dict]:
        """
        Mở URL trong tab mới và extract business info
        
        Args:
            url: Business detail URL
            context: Browser context
            index: Current index for logging
            total: Total items for logging
            
        Returns:
            Business info dict hoặc None
        """
        page = None
        try:
            # Mở tab mới
            page = await context.new_page()
            
            # Stagger tab opening để tránh bị detect (50ms delay)
            await asyncio.sleep(0.05 * (index % 5))
            
            # Navigate với timeout đủ dài
            await page.goto(url, wait_until="domcontentloaded", timeout=45000)
            
            # Thay vì wait networkidle, wait cho selector quan trọng
            try:
                # Wait cho tên business xuất hiện
                await page.wait_for_selector('h1', timeout=8000)
            except:
                # Nếu không có h1, vẫn thử extract
                pass
            
            # Thêm delay nhỏ để panel load đầy đủ
            await asyncio.sleep(1)
            
            # Extract info
            business_info = await self._extract_from_detail_panel(page)
            
            if business_info and business_info.get('name'):
                print(f"      ✓ [{index}/{total}] {business_info['name'][:50]}")
                if business_info.get('phone'):
                    print(f"          📞 {business_info['phone']}")
            else:
                print(f"      ⚠️ [{index}/{total}] Không lấy được thông tin")
            
            return business_info
            
        except Exception as e:
            print(f"      ❌ [{index}/{total}] Lỗi: {type(e).__name__}: {str(e)[:50]}")
            return None
        finally:
            if page:
                await page.close()
    
    async def _extract_from_detail_panel(self, page: Page) -> Optional[Dict]:
        """
        Extract thông tin từ detail panel bên phải
        (Sau khi đã click vào một business)
        """
        try:
            
            # Lấy tên - nhiều selector khác nhau
            name = None
            name_selectors = [
                'h1.DUwDvf',  # Selector phổ biến nhất
                'h1.fontHeadlineLarge',
                'h1',
                'div.fontHeadlineLarge span',
                '[role="main"] h1',
            ]
            
            for selector in name_selectors:
                name_elem = await page.query_selector(selector)
                if name_elem:
                    name_text = await name_elem.inner_text()
                    name_text = name_text.strip()
                    if name_text and len(name_text) > 2:
                        name = name_text
                        break
            
            if not name:
                return None
            
            # Lấy số điện thoại - nhiều cách
            phone = None
            
            # Cách 1: Tìm button có data-item-id chứa "phone"
            phone_button = await page.query_selector('button[data-item-id*="phone"]')
            if phone_button:
                aria_label = await phone_button.get_attribute('aria-label') or ''
                phone = self._extract_phone(aria_label)
            
            # Cách 2: Tìm link tel:
            if not phone:
                tel_link = await page.query_selector('a[href^="tel:"]')
                if tel_link:
                    href = await tel_link.get_attribute('href') or ''
                    phone = self._extract_phone(href)
            
            # Cách 3: Tìm trong aria-label có "Phone"
            if not phone:
                phone_buttons = await page.query_selector_all('button[aria-label*="Phone"], button[aria-label*="Điện thoại"]')
                for btn in phone_buttons:
                    aria_label = await btn.get_attribute('aria-label') or ''
                    phone = self._extract_phone(aria_label)
                    if phone:
                        break
            
            # Cách 4: Tìm trong toàn bộ panel text
            if not phone:
                # Lấy text từ phần thông tin chi tiết
                detail_sections = await page.query_selector_all('div.rogA2c')  # Sections chứa info
                for section in detail_sections:
                    text = await section.inner_text()
                    phone = self._extract_phone(text)
                    if phone:
                        break
            
            # Lấy địa chỉ
            address = "Chưa có thông tin"
            
            # Cách 1: Từ button address
            addr_button = await page.query_selector('button[data-item-id*="address"]')
            if addr_button:
                aria_label = await addr_button.get_attribute('aria-label') or ''
                if 'Address:' in aria_label or 'Địa chỉ:' in aria_label:
                    parts = aria_label.replace('Address:', '|').replace('Địa chỉ:', '|').split('|')
                    if len(parts) > 1:
                        address = parts[1].strip()
            
            # Cách 2: Tìm trong div chứa địa chỉ (thường có class fontBodyMedium)
            if address == "Chưa có thông tin":
                addr_divs = await page.query_selector_all('div.fontBodyMedium')
                for div in addr_divs:
                    text = await div.inner_text()
                    text = text.strip()
                    # Địa chỉ thường có tên thành phố và dài hơn
                    if any(city in text for city in ['Hà Nội', 'TP.HCM', 'Đà Nẵng', 'Cần Thơ', 'Hải Phòng', 'Việt Nam']):
                        if len(text) > 15 and not any(x in text for x in ['★', 'đánh giá', 'rating', 'Mở cửa', 'Đóng cửa']):
                            address = text
                            break
            
            # Cách 3: Fallback - tìm trong toàn bộ panel
            if address == "Chưa có thông tin":
                panel_elem = await page.query_selector('[role="main"]')
                if panel_elem:
                    panel_text = await panel_elem.inner_text()
                    address = self._extract_address_from_text(panel_text)
            
            # Lấy website
            website = None
            
            # Cách 1: Tìm button có data-item-id chứa "authority" hoặc "website"
            website_button = await page.query_selector('button[data-item-id*="authority"], button[data-item-id*="website"]')
            if website_button:
                aria_label = await website_button.get_attribute('aria-label') or ''
                # Extract URL từ aria-label
                website = self._extract_website(aria_label)
            
            # Cách 2: Tìm link có href bắt đầu bằng http
            if not website:
                # Tìm trong panel chính, tránh các link internal của Google Maps
                panel_elem = await page.query_selector('[role="main"]')
                if panel_elem:
                    website_links = await panel_elem.query_selector_all('a[href^="http"]')
                    for link in website_links:
                        href = await link.get_attribute('href') or ''
                        # Loại bỏ các link của Google
                        if 'google.com' not in href and 'gstatic.com' not in href:
                            website = href
                            break
            
            # Lấy thời gian hoạt động (opening hours)
            opening_hours = None
            
            # Cách 1: Tìm button có data-item-id chứa "hours"
            hours_button = await page.query_selector('button[data-item-id*="hours"]')
            if hours_button:
                aria_label = await hours_button.get_attribute('aria-label') or ''
                opening_hours = self._extract_opening_hours(aria_label)
            
            # Cách 2: Tìm trong các div chứa thông tin giờ mở cửa
            if not opening_hours:
                # Tìm text có chứa "Open", "Closes", "Mở cửa", "Đóng cửa"
                hours_indicators = ['Open', 'Closes', 'Opens', 'Mở cửa', 'Đóng cửa', '24 hours', '24 giờ']
                all_divs = await page.query_selector_all('div.fontBodyMedium, div.fontBodySmall')
                for div in all_divs:
                    text = await div.inner_text()
                    text = text.strip()
                    if any(indicator in text for indicator in hours_indicators):
                        # Tìm thêm context xung quanh để lấy đầy đủ thông tin
                        parent = await div.evaluate_handle('el => el.parentElement')
                        if parent:
                            hours_text = await parent.as_element().inner_text()
                            hours_text = hours_text.strip()
                            if len(hours_text) > 3:
                                opening_hours = hours_text
                                break
            
            return {
                "name": name,
                "phone": phone,
                "address": address,
                "website": website,
                "opening_hours": opening_hours,
            }
            
        except Exception as e:
            print(f"         Lỗi extract detail: {e}")
            return None
    
    def _extract_address_from_text(self, text: str) -> str:
        """Extract địa chỉ từ một đoạn text dài"""
        lines = text.split('\n')
        cities = ['Hà Nội', 'TP.HCM', 'TP HCM', 'Sài Gòn', 'Đà Nẵng', 'Cần Thơ', 'Hải Phòng', 'Việt Nam']
        
        for line in lines:
            line = line.strip()
            # Tìm dòng chứa tên thành phố và đủ dài
            for city in cities:
                if city in line and len(line) > 15:
                    # Loại bỏ các prefix không cần thiết
                    if ':' in line:
                        line = line.split(':', 1)[1].strip()
                    return line
        
        return "Chưa có thông tin"
    
    def _extract_website(self, text: str) -> Optional[str]:
        """Trích xuất website URL từ text"""
        # Pattern cho URL
        url_pattern = r'https?://[^\s\"\',<>]+'
        matches = re.findall(url_pattern, text)
        
        if matches:
            url = matches[0]
            # Loại bỏ các URL của Google
            if 'google.com' not in url and 'gstatic.com' not in url:
                return url
        
        # Nếu không tìm thấy http://, thử tìm domain pattern
        domain_pattern = r'(?:www\.)?[a-zA-Z0-9-]+\.[a-zA-Z]{2,}(?:\.[a-zA-Z]{2,})?'
        domain_matches = re.findall(domain_pattern, text)
        
        if domain_matches:
            domain = domain_matches[0]
            # Thêm https:// nếu chưa có
            if not domain.startswith(('http://', 'https://')):
                return f'https://{domain}'
            return domain
        
        return None
    
    def _extract_opening_hours(self, text: str) -> Optional[str]:
        """Trích xuất thông tin giờ mở cửa từ text"""
        # Làm sạch aria-label
        # Thường có format: "Hours: Open ⋅ Closes 5 PM" hoặc "Giờ: Mở cửa ⋅ Đóng cửa 17:00"
        
        # Loại bỏ các prefix như "Hours:", "Giờ:", etc.
        cleaned = text
        for prefix in ['Hours:', 'Giờ:', 'Opening hours:', 'Thời gian mở cửa:']:
            if prefix in cleaned:
                cleaned = cleaned.split(prefix, 1)[1].strip()
        
        # Nếu có nội dung hợp lệ
        if len(cleaned) > 3:
            # Làm sạch thêm các ký tự đặc biệt
            cleaned = cleaned.replace('⋅', '•').strip()
            return cleaned
        
        return None
    
    def _extract_phone(self, text: str) -> Optional[str]:
        """Trích xuất số điện thoại từ text"""
        # Pattern cho số điện thoại Việt Nam
        patterns = [
            r'(?:\+84|84|0)[\s.-]?\d{1,4}[\s.-]?\d{3}[\s.-]?\d{3,4}',
            r'(?:\+84|84|0)\d{9,10}',
            r'\b\d{10,11}\b',
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, text)
            if matches:
                # Làm sạch
                phone = re.sub(r'[^\d+]', '', matches[0])
                
                # Chuẩn hóa
                if phone.startswith('+84'):
                    phone = '0' + phone[3:]
                elif phone.startswith('84'):
                    phone = '0' + phone[2:]
                
                if 10 <= len(phone) <= 11:
                    return phone
        
        return None
    
    async def run_searches(self, queries: List[str], delay: float = 3.0) -> Dict[str, List[Dict]]:
        """
        Chạy nhiều query search trên Maps
        
        Args:
            queries: Danh sách query
            delay: Delay giữa các query
            
        Returns:
            Dict với key là query, value là list kết quả
        """
        all_results = {}
        
        async with async_playwright() as p:
            print("🌐 Đang khởi động browser...")
            
            # Launch với args tương tự batdongsan_final.py
            browser = await p.chromium.launch(
                headless=self.headless,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--disable-web-security',
                    '--no-sandbox',
                    '--disable-setuid-sandbox',
                ]
            )
            
            # Context options
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                viewport={"width": 1920, "height": 1080},
                locale="vi-VN",
                timezone_id="Asia/Ho_Chi_Minh",
            )
            
            page = await context.new_page()
            
            try:
                for i, query in enumerate(queries, 1):
                    print(f"\n🔍 [{i}/{len(queries)}] Đang search: {query}")
                    
                    # Search trên Maps với context for multi-tab
                    businesses = await self.search_google_maps(query, page, context)
                    
                    all_results[query] = businesses
                    print(f"   ✅ Tổng cộng: {len(businesses)} kết quả\n")
                    
                    # Delay
                    if i < len(queries):
                        print(f"   ⏳ Chờ {delay}s trước query tiếp theo...")
                        await asyncio.sleep(delay)
            
            finally:
                await browser.close()
        
        return all_results


def save_results(results: Dict[str, List[Dict]], output_file: str, timestamp: str = ""):
    """Lưu kết quả vào JSON file với timestamp prefix
    
    Args:
        results: Kết quả scraping
        output_file: Tên file gốc
        timestamp: Timestamp để thêm vào prefix (format: YYYYMMDD_HHMMSS)
    """
    # Gộp và loại trùng
    all_businesses = []
    seen_names = set()
    
    for query, businesses in results.items():
        for business in businesses:
            name = business.get("name", "")
            
            # Loại trùng theo tên
            if name and name not in seen_names:
                seen_names.add(name)
                all_businesses.append(business)
    
    # Thêm timestamp vào tên file nếu có
    if timestamp:
        # Tách tên file và extension
        if '.' in output_file:
            name_parts = output_file.rsplit('.', 1)
            final_filename = f"{timestamp}_{name_parts[0]}.{name_parts[1]}"
        else:
            final_filename = f"{timestamp}_{output_file}"
    else:
        final_filename = output_file
    
    # Lưu file
    with open(final_filename, 'w', encoding='utf-8') as f:
        json.dump(all_businesses, f, ensure_ascii=False, indent=2)
    
    print(f"\n💾 Đã lưu {len(all_businesses)} doanh nghiệp vào {final_filename}")


# ===== Các hàm helper để nhập query =====

def get_queries_from_args():
    """Lấy queries từ command line"""
    import sys
    if len(sys.argv) > 1:
        return sys.argv[1:]
    return None


def get_queries_from_file(file_path: str) -> List[str]:
    """Đọc queries từ file"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return [line.strip() for line in f if line.strip()]
    except Exception as e:
        print(f"❌ Lỗi đọc file: {e}")
        return []


def get_queries_interactive() -> List[str]:
    """Nhập queries interactive"""
    print("\n📝 NHẬP CÁC QUERY TÌM KIẾM")
    print("\n🎯 Chọn chế độ nhập:")
    print("   1. Nhập từng query (mỗi dòng 1 query, Enter 2 lần để kết thúc)")
    print("   2. Paste tất cả queries cùng lúc (Ctrl+D hoặc Ctrl+Z để kết thúc)")
    print("=" * 60)
    
    mode = input("Chọn chế độ (1/2, Enter = 1): ").strip() or "1"
    
    if mode == "2":
        # Chế độ paste nhiều queries
        print("\n📋 Paste tất cả queries vào đây (mỗi dòng 1 query)")
        print("   Nhấn Ctrl+D (Linux/Mac) hoặc Ctrl+Z + Enter (Windows) để kết thúc\n")
        
        queries = []
        try:
            while True:
                line = input()
                if line.strip():
                    queries.append(line.strip())
        except EOFError:
            # Ctrl+D hoặc Ctrl+Z
            pass
        
        if queries:
            print(f"\n✅ Đã nhận {len(queries)} queries:")
            for i, q in enumerate(queries, 1):
                print(f"   {i}. {q}")
        
        return queries
    
    else:
        # Chế độ nhập từng query
        print("\n📝 Nhập từng query, mỗi query 1 dòng")
        print("   Nhấn Enter 2 lần liên tiếp để kết thúc\n")
        
        queries = []
        empty_count = 0
        
        while True:
            query = input(f"Query {len(queries) + 1}: ").strip()
            
            if not query:
                empty_count += 1
                if empty_count >= 2:
                    break
                continue
            
            empty_count = 0
            queries.append(query)
            print(f"   ✓ Đã thêm: {query}")
        
        return queries


async def main():
    """Hàm chính"""
    import sys
    
    # ============== CẤU HÌNH ==============
    OUTPUT_FILE = "google_maps_results.json"
    HEADLESS = False  # False = hiện browser để xem process
    DELAY_BETWEEN_SEARCHES = 5  # Giây
    CONCURRENT_TABS = 5  # Số tabs song song
    # =====================================
    
    print("=" * 70)
    print("🗺️  GOOGLE MAPS BUSINESS SCRAPER (ASYNC MULTI-TAB)")
    print("=" * 70)
    
    # ===== NHẬP QUERIES =====
    queries = None
    
    # Cách 1: Command line
    queries = get_queries_from_args()
    if queries:
        print(f"\n✅ Đã nhận {len(queries)} queries từ command line\n")
    
    # Cách 2: Từ file
    if not queries and len(sys.argv) == 3 and sys.argv[1] == "--file":
        queries = get_queries_from_file(sys.argv[2])
        if queries:
            print(f"\n✅ Đã đọc {len(queries)} queries từ file: {sys.argv[2]}\n")
    
    # Cách 3: Interactive
    if not queries:
        print("\n💡 Hướng dẫn sử dụng:")
        print("   1. Command line: python script.py \"query 1\" \"query 2\"")
        print("   2. Từ file: python script.py --file queries.txt")
        print("   3. Interactive: nhập trực tiếp\n")
        
        use_interactive = input("Bạn có muốn nhập queries ngay? (y/n): ").lower()
        if use_interactive == 'y':
            queries = get_queries_interactive()
    
    if not queries:
        print("\n❌ Không có query để search!")
        return
    
    # Hiển thị queries
    print("\n📋 DANH SÁCH QUERIES:")
    for i, q in enumerate(queries, 1):
        print(f"   {i}. {q}")
    print()
    
    print(f"⚡ Chế độ: {CONCURRENT_TABS} tabs song song (async)")
    print()
    
    # Khởi tạo scraper
    scraper = GoogleMapsScraper(headless=HEADLESS, concurrent_tabs=CONCURRENT_TABS)
    
    # Chạy searches
    results = await scraper.run_searches(queries, delay=DELAY_BETWEEN_SEARCHES)
    
    # Tạo timestamp khi hoàn thành
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Lưu kết quả với timestamp
    save_results(results, OUTPUT_FILE, timestamp=timestamp)
    
    print("\n" + "=" * 70)
    print("✅ HOÀN THÀNH!")
    print(f"📊 Đã search {len(queries)} queries")
    print(f"📁 Kết quả: {OUTPUT_FILE}")
    print("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
