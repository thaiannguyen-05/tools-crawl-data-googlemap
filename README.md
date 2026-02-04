# Google Maps Business Scraper

Công cụ crawl thông tin doanh nghiệp từ Google Maps với hỗ trợ resume và export Excel.

## 🚀 Cài đặt

```bash
# Tạo virtual environment (chỉ cần làm 1 lần)
python3 -m venv .venv

# Cài đặt dependencies
./.venv/bin/pip install openpyxl unidecode playwright

# Cài đặt browser
./.venv/bin/playwright install chromium
```

## 📖 Cách sử dụng

### Chạy crawl
```bash
# Kích hoạt venv
source .venv/bin/activate

# Crawl 1 query
python search_google_maps.py "bất động sản"

# Crawl nhiều queries
python search_google_maps.py "nhà hàng" "quán cà phê" "spa"

# Crawl từ file
python search_google_maps.py --file queries.txt
```

### Dừng đột ngột
Nhấn **Ctrl+C** để dừng. Dữ liệu sẽ được:
- Lưu vào `crawl_state/` (để tiếp tục sau)
- Export ra Excel vào `output/`

### Các lệnh khác
```bash
# Export Excel từ state đã lưu
python search_google_maps.py --export

# Xem trạng thái các crawl đang dở
python search_google_maps.py --status
```

## 📁 Cấu trúc thư mục

```
├── crawl_state/           # State files để resume
│   └── batdongsan_state.json
├── output/                # File Excel kết quả
│   └── batdongsan_20260204_183000.xlsx
└── search_google_maps.py  # Script chính
```

## 📊 Excel Output

| STT | Tên | Điện thoại | Địa chỉ | Website | Giờ mở cửa |
|-----|-----|------------|---------|---------|------------|
| 1   | ... | ...        | ...     | ...     | ...        |

## 🔄 Resume từ vị trí dừng

Khi chạy lại cùng query, script sẽ hỏi:
```
📥 Tìm thấy state trước đó:
   • Đã crawl: 30 kết quả
   • Vị trí: 30/57

   Tiếp tục từ vị trí dừng? (y/n, Enter=y):
```

## ⚠️ Lưu ý

- Mỗi query sẽ tạo 1 file Excel riêng
- Tên file được tự động tạo từ query (bỏ dấu tiếng Việt)
- State được lưu sau mỗi 5 items
