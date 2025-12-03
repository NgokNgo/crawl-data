# VN-Index Stock Data Crawler

Python crawler để thu thập dữ liệu chứng khoán Việt Nam từ **cafef.vn** (OHLC) và **TCBS** (fundamental), lưu trữ vào SQLite database để phục vụ alpha research.

## Tính năng

- ✅ **Historical OHLC** - Dữ liệu lịch sử giá (Open, High, Low, Close, Volume) từ cafef API
- ✅ **Fundamental Data** - Chỉ số tài chính (P/E, P/B, ROE, ROA, EPS...) từ TCBS API
- ✅ **Financial Statements** - Báo cáo tài chính (income, balance sheet, cash flow)
- ✅ **SQLite Database** - Lưu trữ tối ưu cho alpha research với schema chuẩn
- ✅ **Realtime Polling** - Poll giá realtime theo chu kỳ
- ✅ **Fallback** - Tự động fallback sang HTML scraping + Playwright nếu API fail

## Cài đặt

```bash
# Clone repo
git clone https://github.com/NgokNgo/crawl-data.git
cd crawl-data

# Tạo virtualenv
python -m venv .venv
source .venv/bin/activate

# Cài dependencies
pip install -r requirements.txt

# (Optional) Cài Playwright nếu cần render JS pages
pip install playwright
playwright install chromium
```

## Sử dụng

### 1. Crawl dữ liệu lịch sử (Historical OHLC)

```bash
# Crawl một mã
python crawl.py historical --symbol VIC

# Crawl nhiều mã từ file
python crawl.py historical --symbols-file symbols.txt

# Chỉ định thư mục output
python crawl.py historical --symbol VIC --outdir data/historical
```

### 2. Crawl dữ liệu cơ bản (Fundamental)

```bash
# Crawl fundamental cho một mã (overview, ratios, income, balance, cashflow)
python crawl.py fundamental --symbol VIC

# Crawl nhiều mã từ file
python crawl.py fundamental --symbols-file symbols.txt

# Chỉ xem chỉ số mới nhất (không lưu file)
python crawl.py fundamental --symbol VIC --latest
```

### 3. Quản lý Database

```bash
# Khởi tạo database schema
python managedb.py init

# Import tất cả CSV vào database
python managedb.py import

# Xem thống kê database
python managedb.py info

# Query dữ liệu giá
python managedb.py query --type prices --symbols VIC,VCB

# Query dữ liệu fundamental
python managedb.py query --type fundamentals --symbols VIC

# Query merged data (price + fundamental)
python managedb.py query --type merged --symbols VIC,VCB --start 2024-01-01

# Export ra CSV
python managedb.py export --type all --outdir data/export
```

### 4. Poll dữ liệu realtime

```bash
# Poll một mã mỗi 60 giây
python crawl.py realtime --symbol VIC \
  --url-template 'https://cafef.vn/thi-truong-chung-khoan/hose/{symbol}.chn' \
  --interval 60

# Poll nhiều mã, dừng sau 10 lần
python crawl.py realtime --symbols-file symbols.txt \
  --url-template 'https://cafef.vn/thi-truong-chung-khoan/hose/{symbol}.chn' \
  --interval 30 --iterations 10
```

### 5. Quản lý danh sách mã

```bash
# Load từ file
python crawl.py symbols --from-file symbols.txt

# Tạo file symbols.txt
echo -e "VIC\nVHM\nVCB\nMSN\nVNM\nHPG\nBID\nACB\nMWG\nVPB" > symbols.txt
```

## Database Schema

Database SQLite được thiết kế tối ưu cho alpha research:

### Tables

| Table | Mô tả |
|-------|-------|
| `symbols` | Master list mã chứng khoán (tên, ngành, sàn...) |
| `daily_prices` | OHLCV hàng ngày (indexed by date, symbol) |
| `fundamentals_quarterly` | Chỉ số tài chính theo quý |
| `income_statement` | Báo cáo KQKD |
| `balance_sheet` | Bảng CĐKT |
| `cashflow` | Lưu chuyển tiền tệ |
| `alpha_factors` | Pre-computed factors (optional) |

### Python API cho Alpha Research

```python
from crawler.database import (
    get_price_matrix,      # Giá theo matrix (dates x symbols)
    get_price_panel,       # Giá theo panel (MultiIndex)
    get_fundamentals,      # Fundamental data
    get_merged_data,       # Merge price + fundamental (point-in-time)
    compute_returns,       # Tính returns
    compute_volatility,    # Tính volatility
    rank_cross_sectional,  # Rank cross-sectional
)

# Lấy close price matrix
prices = get_price_matrix('close', start_date='2024-01-01')

# Lấy merged data cho alpha
data = get_merged_data(
    symbols=['VIC', 'VCB', 'VNM'],
    price_cols=['close', 'volume'],
    fund_cols=['pe', 'pb', 'roe', 'eps']
)
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

### Fundamental (`data/fundamental/`)

| File | Mô tả |
|------|-------|
| `{SYMBOL}_overview.csv` | Thông tin công ty (ngành, số nhân viên, % nước ngoài...) |
| `{SYMBOL}_ratios.csv` | Chỉ số tài chính theo quý (P/E, P/B, ROE, ROA, EPS, margin...) |
| `{SYMBOL}_income.csv` | Báo cáo kết quả kinh doanh (doanh thu, lợi nhuận...) |
| `{SYMBOL}_balance.csv` | Bảng cân đối kế toán (tài sản, nợ, vốn...) |
| `{SYMBOL}_cashflow.csv` | Báo cáo lưu chuyển tiền tệ |

**Các chỉ số trong ratios.csv:**

| Column | Mô tả |
|--------|-------|
| priceToEarning | P/E ratio |
| priceToBook | P/B ratio |
| roe | Return on Equity |
| roa | Return on Assets |
| earningPerShare | EPS (VND) |
| bookValuePerShare | Book value per share |
| grossProfitMargin | Biên lợi nhuận gộp |
| operatingMargin | Biên lợi nhuận hoạt động |
| netProfitMargin | Biên lợi nhuận ròng |
| currentPayment | Khả năng thanh toán hiện hành |
| quickPayment | Khả năng thanh toán nhanh |

## Cấu trúc project

```
crawl-data/
├── crawl.py               # CLI crawl data
├── managedb.py            # CLI quản lý database
├── symbols.txt            # Danh sách mã chứng khoán
├── requirements.txt       # Dependencies
├── crawler/
│   ├── __init__.py
│   ├── cafef_api.py       # Cafef JSON API (OHLC)
│   ├── cafef_parser.py    # Parse HTML (fallback)
│   ├── database.py        # SQLite database module
│   ├── fundamental.py     # TCBS API (fundamental data)
│   ├── historical.py      # Fetch historical OHLC
│   ├── realtime.py        # Poll realtime data
│   ├── storage.py         # Lưu CSV
│   └── symbols.py         # Quản lý symbols
└── data/
    ├── historical/        # CSV lịch sử OHLC
    ├── fundamental/       # CSV fundamental data
    ├── realtime/          # CSV realtime
    └── stock_data.db      # SQLite database
```

## API Endpoints

### Cafef (Historical OHLC)

```
GET https://cafef.vn/du-lieu/Ajax/PageNew/DataHistory/PriceHistory.ashx
    ?Symbol=VIC&PageIndex=1&PageSize=1000
```

### TCBS (Fundamental)

```
# Overview
GET https://apipubaws.tcbs.com.vn/tcanalysis/v1/ticker/{symbol}/overview

# Financial Ratios
GET https://apipubaws.tcbs.com.vn/tcanalysis/v1/finance/{symbol}/financialratio?yearly=0&isAll=true

# Income Statement
GET https://apipubaws.tcbs.com.vn/tcanalysis/v1/finance/{symbol}/incomestatement?yearly=1&isAll=true

# Balance Sheet
GET https://apipubaws.tcbs.com.vn/tcanalysis/v1/finance/{symbol}/balancesheet?yearly=1&isAll=true

# Cash Flow
GET https://apipubaws.tcbs.com.vn/tcanalysis/v1/finance/{symbol}/cashflow?yearly=1&isAll=true
```

## Workflow khuyến nghị

```bash
# 1. Crawl dữ liệu
python crawl.py historical --symbols-file symbols.txt
python crawl.py fundamental --symbols-file symbols.txt

# 2. Import vào database
python managedb.py import

# 3. Kiểm tra
python managedb.py info

# 4. Sử dụng trong Python
from crawler.database import get_merged_data
data = get_merged_data(symbols=['VIC', 'VCB'])
```

## Lưu ý

- Sử dụng `--interval` hợp lý cho realtime polling (khuyến nghị >= 30 giây)
- API có thể thay đổi, kiểm tra và cập nhật nếu cần
- Dữ liệu fundamental từ TCBS cập nhật theo quý
