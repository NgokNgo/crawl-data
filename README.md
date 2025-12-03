# VN-Index Stock Data Crawler

Python crawler để thu thập dữ liệu chứng khoán Việt Nam (OHLC + fundamentals) từ **cafef.vn**.

## Tính năng

- ✅ **Gọi API trực tiếp** - Sử dụng cafef JSON API, nhanh và đáng tin cậy
- ✅ **Historical data** - Lấy toàn bộ lịch sử giao dịch, lưu CSV
- ✅ **Realtime polling** - Poll giá realtime theo chu kỳ, append vào CSV
- ✅ **Fallback** - Nếu API fail, tự động fallback sang HTML scraping + Playwright

## Cài đặt

```bash
# Clone và tạo virtualenv
cd crawl-data
python -m venv .venv
source .venv/bin/activate

# Cài dependencies
pip install -r requirements.txt

# (Optional) Cài Playwright nếu cần render JS pages
pip install playwright
playwright install chromium
```

## Sử dụng

### 1. Crawl dữ liệu lịch sử (Historical)

```bash
# Crawl một mã
python run_crawl.py historical --symbol ACV

# Crawl nhiều mã từ file
python run_crawl.py historical --symbols-file symbols.txt

# Chỉ định thư mục output
python run_crawl.py historical --symbol VIC --outdir data/historical
```

### 2. Poll dữ liệu realtime

```bash
# Poll một mã mỗi 60 giây
python run_crawl.py realtime --symbol VIC --url-template 'https://cafef.vn/thi-truong-chung-khoan/hose/{symbol}.chn' --interval 60

# Poll nhiều mã, dừng sau 10 lần
python run_crawl.py realtime --symbols-file symbols.txt --url-template 'https://cafef.vn/thi-truong-chung-khoan/hose/{symbol}.chn' --interval 30 --iterations 10
```

### 3. Quản lý danh sách mã

```bash
# Load từ file
python run_crawl.py symbols --from-file symbols.txt

# Tạo file symbols.txt với các mã phổ biến
echo -e "VIC\nVHM\nVCB\nMSN\nVNM\nHPG\nBID\nACB\nMWG\nVPB" > symbols.txt
```

## Output CSV

### Historical (`data/historical/{SYMBOL}.csv`)

| Column | Mô tả |
|--------|-------|
| date | Ngày giao dịch |
| open | Giá mở cửa (nghìn VND) |
| high | Giá cao nhất |
| low | Giá thấp nhất |
| close | Giá đóng cửa |
| adj_close | Giá điều chỉnh |
| volume | Khối lượng khớp lệnh |
| value | Giá trị khớp lệnh |
| deal_volume | KL thỏa thuận |
| deal_value | GT thỏa thuận |
| change | Thay đổi (%) |

### Realtime (`data/realtime/{SYMBOL}_realtime.csv`)

Append mỗi lần poll với timestamp và giá hiện tại.

## Cấu trúc project

```
crawl-data/
├── run_crawl.py           # CLI chính
├── symbols.txt            # Danh sách mã chứng khoán
├── requirements.txt       # Dependencies
├── crawler/
│   ├── __init__.py
│   ├── cafef_api.py       # Gọi cafef JSON API trực tiếp
│   ├── cafef_parser.py    # Parse HTML (fallback)
│   ├── historical.py      # Fetch historical data
│   ├── realtime.py        # Poll realtime data
│   ├── storage.py         # Lưu CSV
│   └── symbols.py         # Quản lý symbols
└── data/
    ├── historical/        # CSV lịch sử
    └── realtime/          # CSV realtime
```

## API Endpoint (cafef.vn)

Crawler sử dụng API sau (đã reverse-engineer):

```
GET https://cafef.vn/du-lieu/Ajax/PageNew/DataHistory/PriceHistory.ashx
    ?Symbol=ACV
    &StartDate=
    &EndDate=
    &PageIndex=1
    &PageSize=1000
```

Response: JSON với cấu trúc `{"Data": {"TotalCount": N, "Data": [...]}}`

## Lưu ý

- Sử dụng `--interval` hợp lý (khuyến nghị >= 30 giây)
- API có thể thay đổi, kiểm tra và cập nhật nếu cần
