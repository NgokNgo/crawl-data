#!/usr/bin/env python3
"""CLI entrypoint for the VN-Index crawler.

Usage examples are in the project README. This script supports three subcommands:
- `symbols` to fetch or load symbol lists
- `historical` to fetch historical OHLC for one or more symbols (uses cafef API by default)
- `realtime` to poll symbols and append realtime rows
"""
import argparse
from crawler import symbols as symbols_mod
from crawler.historical import fetch_historical
from crawler.realtime import poll_symbols
import sys


def cmd_symbols(args):
    if args.from_file:
        syms = symbols_mod.load_symbols_from_file(args.from_file)
        print("Loaded", len(syms), "symbols from file")
        for s in syms:
            print(s)
    elif args.from_url:
        syms = symbols_mod.fetch_symbols_from_cafef(args.from_url)
        print("Fetched", len(syms), "symbols from URL")
        for s in syms:
            print(s)
    else:
        print("Provide --from-file PATH or --from-url URL")


def cmd_historical(args):
    if not args.symbol and not args.symbols_file:
        print("Provide --symbol SYMBOL or --symbols-file FILE")
        sys.exit(1)
    syms = []
    if args.symbol:
        syms.append(args.symbol)
    if args.symbols_file:
        syms.extend(symbols_mod.load_symbols_from_file(args.symbols_file))
    for s in syms:
        try:
            # url_template is now optional (API is used by default)
            path = fetch_historical(s, url_template=args.url_template, out_dir=args.outdir)
            if path:
                print(f"Saved historical for {s} -> {path}")
            else:
                print(f"No historical data found for {s}")
        except Exception as e:
            print(f"Error fetching historical for {s}: {e}")


def cmd_realtime(args):
    if args.symbols_file:
        syms = symbols_mod.load_symbols_from_file(args.symbols_file)
    elif args.symbol:
        syms = [args.symbol]
    else:
        print("Provide --symbol or --symbols-file")
        sys.exit(1)
    poll_symbols(syms, args.url_template, interval=args.interval, out_dir=args.outdir, max_iterations=args.iterations)


def main():
    p = argparse.ArgumentParser(description="VN-Index stock data crawler (cafef.vn)")
    sub = p.add_subparsers(dest="cmd")

    sp = sub.add_parser("symbols", help="List or fetch stock symbols")
    sp.add_argument("--from-file", help="Path to symbols.txt")
    sp.add_argument("--from-url", help="URL that lists components (cafef)")
    sp.set_defaults(func=cmd_symbols)

    hp = sub.add_parser("historical", help="Fetch historical OHLC data")
    hp.add_argument("--symbol", help="Single symbol to fetch (e.g. VIC, ACV)")
    hp.add_argument("--symbols-file", help="File with symbols, one per line")
    hp.add_argument("--url-template", default=None, help="Optional URL template for HTML fallback (contains {symbol})")
    hp.add_argument("--outdir", default="data/historical", help="Output directory for CSV files")
    hp.set_defaults(func=cmd_historical)

    rp = sub.add_parser("realtime", help="Poll realtime prices")
    rp.add_argument("--symbol", help="Single symbol to poll")
    rp.add_argument("--symbols-file", help="File with symbols, one per line")
    rp.add_argument("--url-template", required=True, help="URL template with {symbol}")
    rp.add_argument("--interval", type=int, default=60, help="Seconds between polls")
    rp.add_argument("--outdir", default="data/realtime", help="Output directory for CSV files")
    rp.add_argument("--iterations", type=int, default=None, help="Stop after N iterations (useful for tests)")
    rp.set_defaults(func=cmd_realtime)

    args = p.parse_args()
    if not args.cmd:
        p.print_help()
        sys.exit(1)
    args.func(args)


if __name__ == "__main__":
    main()
