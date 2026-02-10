# Google Maps Business Scraper (Update 2026-02-10)

Bản cập nhật này bổ sung **tùy chọn lưu kết quả**: lưu **theo từng query** hoặc **gộp chung 1 file Excel**.

## ✅ Điểm mới chính
- `--save-mode per_query|combined` để chọn chế độ lưu.
- `--combined` là alias nhanh cho `--save-mode combined`.
- Khi gộp chung, Excel có thêm cột `Query`.

## ▶️ Cách dùng nhanh
```bash
# Mặc định: mỗi query -> 1 file Excel
python search_google_maps.py "spa ha noi" "nha khoa quan 1"

# Gộp chung 1 file Excel
python search_google_maps.py --save-mode combined "spa ha noi" "nha khoa quan 1"

# Alias ngắn
python search_google_maps.py --combined "spa ha noi" "nha khoa quan 1"

# Dùng file queries
python search_google_maps.py --save-mode combined --file queries.txt
```

## 📊 Excel Output

### `per_query`
Tạo 1 file cho mỗi query như trước.

### `combined`
Chỉ 1 file, có thêm cột `Query`.

| STT | Query | Tên | Điện thoại | Địa chỉ | Website | Giờ mở cửa |
|-----|-------|-----|------------|---------|---------|------------|
| 1   | ...   | ... | ...        | ...     | ...     | ...        |

## ℹ️ Ghi chú
- Mặc định vẫn là `per_query`.
- State vẫn được lưu để resume. Khi `combined`, state **không bị xóa tự động** để đảm bảo an toàn dữ liệu.

## 📌 File liên quan
- `search_google_maps.py`
