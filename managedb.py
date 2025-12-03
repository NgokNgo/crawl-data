#!/usr/bin/env python3
"""CLI for database management - separate from crawler.

Commands:
- `init` - Initialize database schema
- `import` - Import CSV data into database
- `query` - Query data (prices, fundamentals, merged)
- `info` - Show database statistics
"""
import argparse
import sys


def cmd_init(args):
    from crawler.database import init_database
    init_database(args.db)
    print(f"Database initialized at {args.db}")


def cmd_import(args):
    from crawler.database import init_database, import_from_csv_files
    init_database(args.db)  # Ensure tables exist
    counts = import_from_csv_files(args.data_dir, args.db)
    print(f"\nImport complete:")
    for k, v in counts.items():
        print(f"  {k}: {v} rows")


def cmd_query(args):
    import pandas as pd
    from crawler.database import get_price_matrix, get_fundamentals, get_merged_data
    
    pd.set_option('display.max_columns', 20)
    pd.set_option('display.width', 200)
    
    symbols = args.symbols.split(',') if args.symbols else None
    
    if args.type == "prices":
        df = get_price_matrix(
            column=args.column or 'close',
            symbols=symbols,
            start_date=args.start,
            end_date=args.end,
            db_path=args.db
        )
        print(f"\nPrice matrix ({args.column or 'close'}):")
        print(df.tail(args.limit))
    
    elif args.type == "fundamentals":
        df = get_fundamentals(
            symbols=symbols,
            db_path=args.db
        )
        print(f"\nFundamentals:")
        print(df.tail(args.limit))
    
    elif args.type == "merged":
        df = get_merged_data(
            symbols=symbols,
            start_date=args.start,
            end_date=args.end,
            db_path=args.db
        )
        print(f"\nMerged data:")
        print(df.tail(args.limit))


def cmd_info(args):
    import sqlite3
    from pathlib import Path
    
    if not Path(args.db).exists():
        print(f"Database not found: {args.db}")
        print("Run `python db.py init` first.")
        sys.exit(1)
    
    conn = sqlite3.connect(args.db)
    cur = conn.cursor()
    
    print(f"\nDatabase: {args.db}")
    print("-" * 50)
    
    # Table info
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
    tables = [r[0] for r in cur.fetchall()]
    
    for table in tables:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()[0]
        print(f"  {table}: {count:,} rows")
    
    # Symbol count
    cur.execute("SELECT COUNT(DISTINCT symbol) FROM daily_prices")
    n_symbols = cur.fetchone()[0]
    
    # Date range
    cur.execute("SELECT MIN(date), MAX(date) FROM daily_prices")
    date_range = cur.fetchone()
    
    print(f"\nCoverage:")
    print(f"  Symbols: {n_symbols}")
    if date_range[0]:
        print(f"  Date range: {date_range[0]} to {date_range[1]}")
    
    # List symbols
    cur.execute("SELECT DISTINCT symbol FROM daily_prices ORDER BY symbol")
    symbols = [r[0] for r in cur.fetchall()]
    if symbols:
        print(f"  Symbols list: {', '.join(symbols)}")
    
    conn.close()


def cmd_export(args):
    """Export data from database to CSV."""
    import pandas as pd
    from pathlib import Path
    from crawler.database import get_price_matrix, get_fundamentals
    
    out_dir = Path(args.outdir)
    out_dir.mkdir(parents=True, exist_ok=True)
    
    symbols = args.symbols.split(',') if args.symbols else None
    
    if args.type in ["prices", "all"]:
        df = get_price_matrix(
            column='close',
            symbols=symbols,
            start_date=args.start,
            end_date=args.end,
            db_path=args.db
        )
        if not df.empty:
            path = out_dir / "prices_matrix.csv"
            df.to_csv(path)
            print(f"Exported prices to {path}")
    
    if args.type in ["fundamentals", "all"]:
        df = get_fundamentals(symbols=symbols, db_path=args.db)
        if not df.empty:
            path = out_dir / "fundamentals.csv"
            df.to_csv(path, index=False)
            print(f"Exported fundamentals to {path}")


def main():
    p = argparse.ArgumentParser(
        description="Stock database management CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python db.py init                           # Initialize database
  python db.py import                         # Import all CSV from data/
  python db.py info                           # Show database stats
  python db.py query --type prices            # Query price data
  python db.py query --type fundamentals      # Query fundamental data
  python db.py query --symbols VIC,VCB        # Query specific symbols
  python db.py export --type all              # Export to CSV
"""
    )
    sub = p.add_subparsers(dest="cmd")
    
    # Init
    ip = sub.add_parser("init", help="Initialize database schema")
    ip.add_argument("--db", default="data/stock_data.db", help="Database path")
    ip.set_defaults(func=cmd_init)
    
    # Import
    imp = sub.add_parser("import", help="Import CSV data into database")
    imp.add_argument("--db", default="data/stock_data.db", help="Database path")
    imp.add_argument("--data-dir", default="data", help="Data directory containing CSV files")
    imp.set_defaults(func=cmd_import)
    
    # Query
    qp = sub.add_parser("query", help="Query data from database")
    qp.add_argument("--db", default="data/stock_data.db", help="Database path")
    qp.add_argument("--type", choices=["prices", "fundamentals", "merged"], default="prices",
                    help="Data type to query")
    qp.add_argument("--symbols", help="Comma-separated symbols (e.g. VIC,VCB,VNM)")
    qp.add_argument("--start", help="Start date (YYYY-MM-DD)")
    qp.add_argument("--end", help="End date (YYYY-MM-DD)")
    qp.add_argument("--column", default="close", help="Price column (close, volume, etc.)")
    qp.add_argument("--limit", type=int, default=20, help="Number of rows to display")
    qp.set_defaults(func=cmd_query)
    
    # Info
    infop = sub.add_parser("info", help="Show database statistics")
    infop.add_argument("--db", default="data/stock_data.db", help="Database path")
    infop.set_defaults(func=cmd_info)
    
    # Export
    ep = sub.add_parser("export", help="Export data from database to CSV")
    ep.add_argument("--db", default="data/stock_data.db", help="Database path")
    ep.add_argument("--type", choices=["prices", "fundamentals", "all"], default="all",
                    help="Data type to export")
    ep.add_argument("--symbols", help="Comma-separated symbols")
    ep.add_argument("--start", help="Start date (YYYY-MM-DD)")
    ep.add_argument("--end", help="End date (YYYY-MM-DD)")
    ep.add_argument("--outdir", default="data/export", help="Output directory")
    ep.set_defaults(func=cmd_export)
    
    args = p.parse_args()
    if not args.cmd:
        p.print_help()
        sys.exit(1)
    args.func(args)


if __name__ == "__main__":
    main()
