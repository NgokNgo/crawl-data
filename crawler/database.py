"""Database module for stock data storage optimized for alpha research.

Schema designed for:
- Fast time-series queries (by date range, symbol)
- Cross-sectional analysis (all stocks on a given date)
- Easy joins between price and fundamental data
- Efficient alpha factor computation

Uses SQLite for simplicity, can be extended to PostgreSQL/TimescaleDB for production.
"""
import sqlite3
from pathlib import Path
from typing import Optional, List, Dict, Union
import pandas as pd
from datetime import datetime, date


# Default database path
DEFAULT_DB_PATH = "data/stock_data.db"


def get_connection(db_path: str = DEFAULT_DB_PATH) -> sqlite3.Connection:
    """Get database connection with optimizations."""
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    # Enable WAL mode for better concurrent read performance
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=10000")
    return conn


def init_database(db_path: str = DEFAULT_DB_PATH) -> None:
    """Initialize database with optimized schema for alpha research.
    
    Tables:
    - symbols: Master list of stock symbols
    - daily_prices: OHLCV data (indexed by date, symbol)
    - fundamentals_quarterly: Quarterly financial ratios
    - income_statement: Income data
    - balance_sheet: Balance sheet data
    - cashflow: Cash flow data
    """
    conn = get_connection(db_path)
    cur = conn.cursor()
    
    # ============================================================
    # SYMBOLS TABLE - Master list
    # ============================================================
    cur.execute("""
        CREATE TABLE IF NOT EXISTS symbols (
            symbol TEXT PRIMARY KEY,
            name TEXT,
            exchange TEXT,          -- HOSE, HNX, UPCOM
            industry TEXT,
            industry_en TEXT,
            no_employees INTEGER,
            foreign_percent REAL,
            outstanding_shares REAL,
            listed_date DATE,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    
    # ============================================================
    # DAILY PRICES TABLE - Core OHLCV data
    # Optimized for time-series and cross-sectional queries
    # ============================================================
    cur.execute("""
        CREATE TABLE IF NOT EXISTS daily_prices (
            symbol TEXT NOT NULL,
            date DATE NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            adj_close REAL,
            volume INTEGER,
            value REAL,             -- Trading value (VND)
            deal_volume INTEGER,    -- Block deal volume
            deal_value REAL,        -- Block deal value
            change_pct REAL,        -- Daily return %
            
            -- Computed fields for alpha (can be updated by triggers/jobs)
            returns REAL,           -- ln(close/prev_close)
            volatility_20d REAL,    -- 20-day rolling volatility
            avg_volume_20d REAL,    -- 20-day avg volume
            
            PRIMARY KEY (symbol, date),
            FOREIGN KEY (symbol) REFERENCES symbols(symbol)
        )
    """)
    
    # Indexes for fast queries
    cur.execute("CREATE INDEX IF NOT EXISTS idx_prices_date ON daily_prices(date)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_prices_symbol ON daily_prices(symbol)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_prices_date_symbol ON daily_prices(date, symbol)")
    
    # ============================================================
    # FUNDAMENTALS QUARTERLY - Key ratios for alpha
    # ============================================================
    cur.execute("""
        CREATE TABLE IF NOT EXISTS fundamentals_quarterly (
            symbol TEXT NOT NULL,
            year INTEGER NOT NULL,
            quarter INTEGER NOT NULL,  -- 1-4 for quarters, 5 for annual
            report_date DATE,          -- Actual report release date
            
            -- Valuation ratios
            pe REAL,                   -- Price to Earnings
            pb REAL,                   -- Price to Book
            ps REAL,                   -- Price to Sales (computed)
            ev_ebitda REAL,            -- EV/EBITDA
            
            -- Profitability
            roe REAL,                  -- Return on Equity
            roa REAL,                  -- Return on Assets
            gross_margin REAL,         -- Gross profit margin
            operating_margin REAL,     -- Operating margin
            net_margin REAL,           -- Net profit margin
            
            -- Per share metrics
            eps REAL,                  -- Earnings per share
            bvps REAL,                 -- Book value per share
            dividend REAL,             -- Dividend per share
            
            -- Growth
            revenue_growth_yoy REAL,   -- Revenue YoY growth
            eps_growth_yoy REAL,       -- EPS YoY growth
            
            -- Efficiency
            asset_turnover REAL,       -- Revenue / Assets
            days_receivable REAL,
            days_inventory REAL,
            days_payable REAL,
            cash_cycle REAL,           -- Days receivable + inventory - payable
            
            -- Leverage & Liquidity
            debt_to_equity REAL,
            debt_to_assets REAL,
            current_ratio REAL,
            quick_ratio REAL,
            interest_coverage REAL,    -- EBIT / Interest expense
            
            -- Quality metrics
            accruals REAL,             -- (Net income - CFO) / Assets
            earnings_quality REAL,     -- CFO / Net income
            
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (symbol, year, quarter),
            FOREIGN KEY (symbol) REFERENCES symbols(symbol)
        )
    """)
    
    cur.execute("CREATE INDEX IF NOT EXISTS idx_fund_date ON fundamentals_quarterly(year, quarter)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_fund_symbol ON fundamentals_quarterly(symbol)")
    
    # ============================================================
    # INCOME STATEMENT - Detailed quarterly income
    # ============================================================
    cur.execute("""
        CREATE TABLE IF NOT EXISTS income_statement (
            symbol TEXT NOT NULL,
            year INTEGER NOT NULL,
            quarter INTEGER NOT NULL,
            
            revenue REAL,
            cost_of_goods REAL,
            gross_profit REAL,
            operating_expense REAL,
            operating_profit REAL,
            interest_expense REAL,
            pretax_profit REAL,
            tax REAL,
            net_profit REAL,
            shareholder_income REAL,   -- Net income to common shareholders
            ebitda REAL,
            
            revenue_growth_yoy REAL,
            revenue_growth_qoq REAL,
            profit_growth_yoy REAL,
            profit_growth_qoq REAL,
            
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (symbol, year, quarter),
            FOREIGN KEY (symbol) REFERENCES symbols(symbol)
        )
    """)
    
    # ============================================================
    # BALANCE SHEET - Assets, Liabilities, Equity
    # ============================================================
    cur.execute("""
        CREATE TABLE IF NOT EXISTS balance_sheet (
            symbol TEXT NOT NULL,
            year INTEGER NOT NULL,
            quarter INTEGER NOT NULL,
            
            -- Assets
            total_assets REAL,
            current_assets REAL,
            cash REAL,
            short_term_investments REAL,
            receivables REAL,
            inventory REAL,
            fixed_assets REAL,
            
            -- Liabilities
            total_liabilities REAL,
            current_liabilities REAL,
            short_term_debt REAL,
            long_term_debt REAL,
            total_debt REAL,
            
            -- Equity
            total_equity REAL,
            retained_earnings REAL,
            
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (symbol, year, quarter),
            FOREIGN KEY (symbol) REFERENCES symbols(symbol)
        )
    """)
    
    # ============================================================
    # CASH FLOW - Operating, Investing, Financing
    # ============================================================
    cur.execute("""
        CREATE TABLE IF NOT EXISTS cashflow (
            symbol TEXT NOT NULL,
            year INTEGER NOT NULL,
            quarter INTEGER NOT NULL,
            
            cfo REAL,                  -- Cash from operations
            cfi REAL,                  -- Cash from investing
            cff REAL,                  -- Cash from financing
            net_cash_change REAL,
            
            capex REAL,                -- Capital expenditure
            fcf REAL,                  -- Free cash flow (CFO - Capex)
            dividends_paid REAL,
            
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (symbol, year, quarter),
            FOREIGN KEY (symbol) REFERENCES symbols(symbol)
        )
    """)
    
    # ============================================================
    # ALPHA FACTORS - Pre-computed factors (optional, for performance)
    # ============================================================
    cur.execute("""
        CREATE TABLE IF NOT EXISTS alpha_factors (
            symbol TEXT NOT NULL,
            date DATE NOT NULL,
            
            -- Momentum factors
            mom_1m REAL,               -- 1-month momentum
            mom_3m REAL,               -- 3-month momentum
            mom_6m REAL,               -- 6-month momentum
            mom_12m REAL,              -- 12-month momentum
            mom_12m_1m REAL,           -- 12-1 month momentum (skip recent month)
            
            -- Mean reversion
            rev_5d REAL,               -- 5-day reversal
            rev_20d REAL,              -- 20-day reversal
            
            -- Volatility
            vol_20d REAL,              -- 20-day volatility
            vol_60d REAL,              -- 60-day volatility
            idio_vol REAL,             -- Idiosyncratic volatility
            
            -- Liquidity
            turnover_20d REAL,         -- 20-day avg turnover
            amihud_illiq REAL,         -- Amihud illiquidity
            
            -- Size
            market_cap REAL,           -- Market capitalization
            log_market_cap REAL,
            
            -- Value (point-in-time, lag-adjusted)
            pe_ttm REAL,               -- Trailing 12m P/E
            pb_mrq REAL,               -- Most recent quarter P/B
            ps_ttm REAL,               -- Trailing 12m P/S
            
            -- Quality
            roe_ttm REAL,
            roa_ttm REAL,
            gross_margin_ttm REAL,
            
            -- Growth
            eps_growth_ttm REAL,
            revenue_growth_ttm REAL,
            
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (symbol, date),
            FOREIGN KEY (symbol) REFERENCES symbols(symbol)
        )
    """)
    
    cur.execute("CREATE INDEX IF NOT EXISTS idx_alpha_date ON alpha_factors(date)")
    
    conn.commit()
    conn.close()
    print(f"Database initialized at {db_path}")


# ============================================================
# DATA IMPORT FUNCTIONS
# ============================================================

def import_daily_prices(df: pd.DataFrame, symbol: str, db_path: str = DEFAULT_DB_PATH) -> int:
    """Import daily price data from DataFrame.
    
    Args:
        df: DataFrame with columns [date, open, high, low, close, adj_close, volume, ...]
        symbol: Stock symbol
        db_path: Database path
    
    Returns:
        Number of rows inserted/updated
    """
    conn = get_connection(db_path)
    
    # Ensure symbol exists
    conn.execute(
        "INSERT OR IGNORE INTO symbols (symbol) VALUES (?)",
        (symbol,)
    )
    
    # Prepare data
    df = df.copy()
    df['symbol'] = symbol
    
    # Convert date to string for SQLite
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
    
    # Rename columns to match schema
    col_map = {
        'adj_close': 'adj_close',
        'deal_volume': 'deal_volume',
        'deal_value': 'deal_value',
    }
    df = df.rename(columns=col_map)
    
    # Parse change percentage if present
    if 'change' in df.columns:
        # Extract percentage from format like "6(4.80 %)"
        df['change_pct'] = df['change'].astype(str).str.extract(r'\(([-\d.]+)\s*%\)')[0].astype(float)
    
    # Select columns that exist
    cols = ['symbol', 'date', 'open', 'high', 'low', 'close', 'adj_close', 
            'volume', 'value', 'deal_volume', 'deal_value', 'change_pct']
    available_cols = [c for c in cols if c in df.columns]
    df = df[available_cols]
    
    # Upsert data
    rows = df.to_dict('records')
    placeholders = ', '.join(['?' for _ in available_cols])
    cols_str = ', '.join(available_cols)
    
    conn.executemany(f"""
        INSERT OR REPLACE INTO daily_prices ({cols_str})
        VALUES ({placeholders})
    """, [tuple(r[c] for c in available_cols) for r in rows])
    
    conn.commit()
    count = len(rows)
    conn.close()
    return count


def import_fundamentals_from_ratios(df: pd.DataFrame, symbol: str, db_path: str = DEFAULT_DB_PATH) -> int:
    """Import fundamental ratios from TCBS ratios CSV.
    
    Args:
        df: DataFrame from {symbol}_ratios.csv
        symbol: Stock symbol
        db_path: Database path
    
    Returns:
        Number of rows inserted/updated
    """
    conn = get_connection(db_path)
    
    # Ensure symbol exists
    conn.execute("INSERT OR IGNORE INTO symbols (symbol) VALUES (?)", (symbol,))
    
    # Map TCBS columns to our schema
    col_map = {
        'priceToEarning': 'pe',
        'priceToBook': 'pb',
        'valueBeforeEbitda': 'ev_ebitda',
        'roe': 'roe',
        'roa': 'roa',
        'grossProfitMargin': 'gross_margin',
        'operatingProfitMargin': 'operating_margin',
        'postTaxMargin': 'net_margin',
        'earningPerShare': 'eps',
        'bookValuePerShare': 'bvps',
        'dividend': 'dividend',
        'epsChange': 'eps_growth_yoy',
        'daysReceivable': 'days_receivable',
        'daysInventory': 'days_inventory',
        'daysPayable': 'days_payable',
        'cashCirculation': 'cash_cycle',
        'debtOnEquity': 'debt_to_equity',
        'debtOnAsset': 'debt_to_assets',
        'currentPayment': 'current_ratio',
        'quickPayment': 'quick_ratio',
        'ebitOnInterest': 'interest_coverage',
        'revenueOnAsset': 'asset_turnover',
    }
    
    df = df.copy()
    df['symbol'] = symbol
    df = df.rename(columns=col_map)
    
    # Select available columns
    target_cols = ['symbol', 'year', 'quarter', 'pe', 'pb', 'ev_ebitda', 'roe', 'roa',
                   'gross_margin', 'operating_margin', 'net_margin', 'eps', 'bvps',
                   'dividend', 'eps_growth_yoy', 'days_receivable', 'days_inventory',
                   'days_payable', 'cash_cycle', 'debt_to_equity', 'debt_to_assets',
                   'current_ratio', 'quick_ratio', 'interest_coverage', 'asset_turnover']
    available_cols = [c for c in target_cols if c in df.columns]
    df = df[available_cols]
    
    # Upsert
    rows = df.to_dict('records')
    placeholders = ', '.join(['?' for _ in available_cols])
    cols_str = ', '.join(available_cols)
    
    conn.executemany(f"""
        INSERT OR REPLACE INTO fundamentals_quarterly ({cols_str})
        VALUES ({placeholders})
    """, [tuple(r.get(c) for c in available_cols) for r in rows])
    
    conn.commit()
    count = len(rows)
    conn.close()
    return count


def import_income_statement(df: pd.DataFrame, symbol: str, db_path: str = DEFAULT_DB_PATH) -> int:
    """Import income statement data."""
    conn = get_connection(db_path)
    conn.execute("INSERT OR IGNORE INTO symbols (symbol) VALUES (?)", (symbol,))
    
    col_map = {
        'costOfGoodSold': 'cost_of_goods',
        'grossProfit': 'gross_profit',
        'operationExpense': 'operating_expense',
        'operationProfit': 'operating_profit',
        'interestExpense': 'interest_expense',
        'preTaxProfit': 'pretax_profit',
        'postTaxProfit': 'net_profit',
        'shareHolderIncome': 'shareholder_income',
        'yearRevenueGrowth': 'revenue_growth_yoy',
        'quarterRevenueGrowth': 'revenue_growth_qoq',
        'yearShareHolderIncomeGrowth': 'profit_growth_yoy',
        'quarterShareHolderIncomeGrowth': 'profit_growth_qoq',
    }
    
    df = df.copy()
    df['symbol'] = symbol
    df = df.rename(columns=col_map)
    
    target_cols = ['symbol', 'year', 'quarter', 'revenue', 'cost_of_goods', 'gross_profit',
                   'operating_expense', 'operating_profit', 'interest_expense', 'pretax_profit',
                   'net_profit', 'shareholder_income', 'ebitda', 'revenue_growth_yoy',
                   'revenue_growth_qoq', 'profit_growth_yoy', 'profit_growth_qoq']
    available_cols = [c for c in target_cols if c in df.columns]
    df = df[available_cols]
    
    rows = df.to_dict('records')
    placeholders = ', '.join(['?' for _ in available_cols])
    cols_str = ', '.join(available_cols)
    
    conn.executemany(f"""
        INSERT OR REPLACE INTO income_statement ({cols_str})
        VALUES ({placeholders})
    """, [tuple(r.get(c) for c in available_cols) for r in rows])
    
    conn.commit()
    count = len(rows)
    conn.close()
    return count


def import_balance_sheet(df: pd.DataFrame, symbol: str, db_path: str = DEFAULT_DB_PATH) -> int:
    """Import balance sheet data."""
    conn = get_connection(db_path)
    conn.execute("INSERT OR IGNORE INTO symbols (symbol) VALUES (?)", (symbol,))
    
    col_map = {
        'asset': 'total_assets',
        'shortAsset': 'current_assets',
        'cash': 'cash',
        'shortInvest': 'short_term_investments',
        'shortReceivable': 'receivables',
        'inventory': 'inventory',
        'fixedAsset': 'fixed_assets',
        'debt': 'total_liabilities',
        'shortDebt': 'short_term_debt',
        'longDebt': 'long_term_debt',
        'equity': 'total_equity',
        'unDistributedIncome': 'retained_earnings',
    }
    
    df = df.copy()
    df['symbol'] = symbol
    df = df.rename(columns=col_map)
    
    # Compute total_debt if not present
    if 'total_debt' not in df.columns and 'short_term_debt' in df.columns:
        df['total_debt'] = df.get('short_term_debt', 0) + df.get('long_term_debt', 0)
    
    target_cols = ['symbol', 'year', 'quarter', 'total_assets', 'current_assets', 'cash',
                   'short_term_investments', 'receivables', 'inventory', 'fixed_assets',
                   'total_liabilities', 'short_term_debt', 'long_term_debt', 'total_debt',
                   'total_equity', 'retained_earnings']
    available_cols = [c for c in target_cols if c in df.columns]
    df = df[available_cols]
    
    rows = df.to_dict('records')
    placeholders = ', '.join(['?' for _ in available_cols])
    cols_str = ', '.join(available_cols)
    
    conn.executemany(f"""
        INSERT OR REPLACE INTO balance_sheet ({cols_str})
        VALUES ({placeholders})
    """, [tuple(r.get(c) for c in available_cols) for r in rows])
    
    conn.commit()
    count = len(rows)
    conn.close()
    return count


def import_cashflow(df: pd.DataFrame, symbol: str, db_path: str = DEFAULT_DB_PATH) -> int:
    """Import cash flow data."""
    conn = get_connection(db_path)
    conn.execute("INSERT OR IGNORE INTO symbols (symbol) VALUES (?)", (symbol,))
    
    col_map = {
        'investCost': 'cfi',
        'fromInvest': 'cfi',
        'fromFinancial': 'cff',
        'fromSale': 'cfo',
        'freeCashFlow': 'fcf',
    }
    
    df = df.copy()
    df['symbol'] = symbol
    df = df.rename(columns=col_map)
    
    target_cols = ['symbol', 'year', 'quarter', 'cfo', 'cfi', 'cff', 'fcf']
    available_cols = [c for c in target_cols if c in df.columns]
    df = df[available_cols]
    
    rows = df.to_dict('records')
    if not rows:
        conn.close()
        return 0
        
    placeholders = ', '.join(['?' for _ in available_cols])
    cols_str = ', '.join(available_cols)
    
    conn.executemany(f"""
        INSERT OR REPLACE INTO cashflow ({cols_str})
        VALUES ({placeholders})
    """, [tuple(r.get(c) for c in available_cols) for r in rows])
    
    conn.commit()
    count = len(rows)
    conn.close()
    return count


def import_overview(df: pd.DataFrame, symbol: str, db_path: str = DEFAULT_DB_PATH) -> int:
    """Import company overview data into symbols table."""
    conn = get_connection(db_path)
    
    if df.empty:
        conn.close()
        return 0
    
    row = df.iloc[0].to_dict()
    
    conn.execute("""
        INSERT OR REPLACE INTO symbols (symbol, name, exchange, industry, industry_en, 
                                        no_employees, foreign_percent, outstanding_shares)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        symbol,
        row.get('shortName'),
        row.get('exchange'),
        row.get('industry'),
        row.get('industryEn'),
        row.get('noEmployees'),
        row.get('foreignPercent'),
        row.get('outstandingShare'),
    ))
    
    conn.commit()
    conn.close()
    return 1


def import_from_csv_files(data_dir: str = "data", db_path: str = DEFAULT_DB_PATH) -> Dict[str, int]:
    """Import all CSV files from data directory into database.
    
    Args:
        data_dir: Root data directory containing historical/ and fundamental/
        db_path: Database path
    
    Returns:
        Dict with counts of imported records by type
    """
    from pathlib import Path
    
    data_path = Path(data_dir)
    counts = {'prices': 0, 'ratios': 0, 'income': 0, 'balance': 0, 'cashflow': 0, 'overview': 0}
    
    # Import historical prices
    hist_dir = data_path / "historical"
    if hist_dir.exists():
        for csv_file in hist_dir.glob("*.csv"):
            symbol = csv_file.stem
            df = pd.read_csv(csv_file, parse_dates=['date'])
            n = import_daily_prices(df, symbol, db_path)
            counts['prices'] += n
            print(f"Imported {n} price rows for {symbol}")
    
    # Import fundamental data
    fund_dir = data_path / "fundamental"
    if fund_dir.exists():
        # Overview
        for csv_file in fund_dir.glob("*_overview.csv"):
            symbol = csv_file.stem.replace("_overview", "")
            df = pd.read_csv(csv_file)
            n = import_overview(df, symbol, db_path)
            counts['overview'] += n
            print(f"Imported overview for {symbol}")
        
        # Ratios
        for csv_file in fund_dir.glob("*_ratios.csv"):
            symbol = csv_file.stem.replace("_ratios", "")
            df = pd.read_csv(csv_file)
            n = import_fundamentals_from_ratios(df, symbol, db_path)
            counts['ratios'] += n
            print(f"Imported {n} ratio rows for {symbol}")
        
        # Income statement
        for csv_file in fund_dir.glob("*_income.csv"):
            symbol = csv_file.stem.replace("_income", "")
            df = pd.read_csv(csv_file)
            n = import_income_statement(df, symbol, db_path)
            counts['income'] += n
            print(f"Imported {n} income rows for {symbol}")
        
        # Balance sheet
        for csv_file in fund_dir.glob("*_balance.csv"):
            symbol = csv_file.stem.replace("_balance", "")
            df = pd.read_csv(csv_file)
            n = import_balance_sheet(df, symbol, db_path)
            counts['balance'] += n
            print(f"Imported {n} balance rows for {symbol}")
        
        # Cash flow
        for csv_file in fund_dir.glob("*_cashflow.csv"):
            symbol = csv_file.stem.replace("_cashflow", "")
            df = pd.read_csv(csv_file)
            n = import_cashflow(df, symbol, db_path)
            counts['cashflow'] += n
            print(f"Imported {n} cashflow rows for {symbol}")
    
    return counts


# ============================================================
# QUERY FUNCTIONS FOR ALPHA RESEARCH
# ============================================================

def get_price_panel(
    symbols: Optional[List[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    columns: List[str] = ['close', 'volume'],
    db_path: str = DEFAULT_DB_PATH
) -> pd.DataFrame:
    """Get price data as a panel (MultiIndex: date, symbol).
    
    This is the most common format for alpha research.
    
    Args:
        symbols: List of symbols (None = all)
        start_date: Start date YYYY-MM-DD
        end_date: End date YYYY-MM-DD
        columns: Price columns to include
        db_path: Database path
    
    Returns:
        DataFrame with MultiIndex (date, symbol)
    """
    conn = get_connection(db_path)
    
    cols = ', '.join(['symbol', 'date'] + columns)
    query = f"SELECT {cols} FROM daily_prices WHERE 1=1"
    params = []
    
    if symbols:
        placeholders = ', '.join(['?' for _ in symbols])
        query += f" AND symbol IN ({placeholders})"
        params.extend(symbols)
    
    if start_date:
        query += " AND date >= ?"
        params.append(start_date)
    
    if end_date:
        query += " AND date <= ?"
        params.append(end_date)
    
    query += " ORDER BY date, symbol"
    
    df = pd.read_sql_query(query, conn, params=params, parse_dates=['date'])
    conn.close()
    
    if not df.empty:
        df = df.set_index(['date', 'symbol'])
    
    return df


def get_price_matrix(
    column: str = 'close',
    symbols: Optional[List[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    db_path: str = DEFAULT_DB_PATH
) -> pd.DataFrame:
    """Get price data as a matrix (rows=dates, columns=symbols).
    
    Useful for computing cross-sectional factors.
    
    Args:
        column: Which price column ('close', 'adj_close', 'volume', etc.)
        symbols: List of symbols (None = all)
        start_date: Start date YYYY-MM-DD
        end_date: End date YYYY-MM-DD
        db_path: Database path
    
    Returns:
        DataFrame with dates as index and symbols as columns
    """
    df = get_price_panel(symbols, start_date, end_date, [column], db_path)
    if df.empty:
        return df
    
    # Pivot to matrix form
    df = df.reset_index()
    matrix = df.pivot(index='date', columns='symbol', values=column)
    return matrix


def get_fundamentals(
    symbols: Optional[List[str]] = None,
    years: Optional[List[int]] = None,
    columns: Optional[List[str]] = None,
    db_path: str = DEFAULT_DB_PATH
) -> pd.DataFrame:
    """Get fundamental data.
    
    Args:
        symbols: List of symbols (None = all)
        years: List of years (None = all)
        columns: Columns to select (None = all)
        db_path: Database path
    
    Returns:
        DataFrame with fundamental data
    """
    conn = get_connection(db_path)
    
    cols = '*' if not columns else ', '.join(['symbol', 'year', 'quarter'] + columns)
    query = f"SELECT {cols} FROM fundamentals_quarterly WHERE 1=1"
    params = []
    
    if symbols:
        placeholders = ', '.join(['?' for _ in symbols])
        query += f" AND symbol IN ({placeholders})"
        params.extend(symbols)
    
    if years:
        placeholders = ', '.join(['?' for _ in years])
        query += f" AND year IN ({placeholders})"
        params.extend(years)
    
    query += " ORDER BY symbol, year, quarter"
    
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df


def get_merged_data(
    symbols: Optional[List[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    price_cols: List[str] = ['close', 'volume'],
    fund_cols: List[str] = ['pe', 'pb', 'roe', 'eps'],
    db_path: str = DEFAULT_DB_PATH
) -> pd.DataFrame:
    """Get merged price and fundamental data.
    
    Fundamental data is point-in-time adjusted (uses most recent available quarter).
    
    Args:
        symbols: List of symbols
        start_date: Start date
        end_date: End date
        price_cols: Price columns
        fund_cols: Fundamental columns
        db_path: Database path
    
    Returns:
        DataFrame with both price and fundamental data
    """
    # Get price data
    prices = get_price_panel(symbols, start_date, end_date, price_cols, db_path)
    if prices.empty:
        return prices
    
    prices = prices.reset_index()
    
    # Get fundamentals
    funds = get_fundamentals(symbols, columns=fund_cols, db_path=db_path)
    if funds.empty:
        return prices.set_index(['date', 'symbol'])
    
    # Create period column for matching (YYYY-Q format)
    prices['year'] = prices['date'].dt.year
    prices['quarter'] = prices['date'].dt.quarter
    
    # Merge with forward-fill logic (use most recent available fundamental)
    # This ensures point-in-time correctness
    merged = prices.merge(
        funds[['symbol', 'year', 'quarter'] + fund_cols],
        on=['symbol', 'year', 'quarter'],
        how='left'
    )
    
    # Forward fill fundamentals within each symbol
    merged = merged.sort_values(['symbol', 'date'])
    for col in fund_cols:
        merged[col] = merged.groupby('symbol')[col].ffill()
    
    merged = merged.drop(columns=['year', 'quarter'])
    merged = merged.set_index(['date', 'symbol'])
    
    return merged


# ============================================================
# ALPHA FACTOR COMPUTATION HELPERS  
# ============================================================

def compute_returns(
    prices: pd.DataFrame,
    periods: List[int] = [1, 5, 20, 60],
    column: str = 'close'
) -> pd.DataFrame:
    """Compute returns over various periods.
    
    Args:
        prices: DataFrame with dates as index, symbols as columns
        periods: List of periods (days) for return computation
        column: Column name if MultiIndex columns
    
    Returns:
        DataFrame with return columns
    """
    if isinstance(prices.columns, pd.MultiIndex):
        prices = prices[column]
    
    result = pd.DataFrame(index=prices.index)
    
    for p in periods:
        ret = prices.pct_change(p)
        result[f'ret_{p}d'] = ret.stack() if len(prices.columns) > 1 else ret
    
    return result


def compute_volatility(
    prices: pd.DataFrame,
    windows: List[int] = [20, 60],
    column: str = 'close'
) -> pd.DataFrame:
    """Compute rolling volatility.
    
    Args:
        prices: DataFrame with dates as index, symbols as columns
        windows: List of rolling windows
        column: Column name if MultiIndex columns
    
    Returns:
        DataFrame with volatility columns
    """
    if isinstance(prices.columns, pd.MultiIndex):
        prices = prices[column]
    
    returns = prices.pct_change()
    result = pd.DataFrame(index=prices.index)
    
    for w in windows:
        vol = returns.rolling(w).std() * (252 ** 0.5)  # Annualized
        result[f'vol_{w}d'] = vol.stack() if len(prices.columns) > 1 else vol
    
    return result


def rank_cross_sectional(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """Rank values cross-sectionally (across symbols for each date).
    
    Args:
        df: DataFrame with MultiIndex (date, symbol) or date index
        columns: Columns to rank
    
    Returns:
        DataFrame with ranked values (percentile 0-1)
    """
    result = df.copy()
    
    for col in columns:
        if col in result.columns:
            # Rank within each date
            result[f'{col}_rank'] = result.groupby(level='date')[col].rank(pct=True)
    
    return result


if __name__ == "__main__":
    # Initialize database
    init_database()
    
    # Import existing CSV data
    counts = import_from_csv_files()
    print(f"\nImport complete: {counts}")
