"""Realtime poller: periodically fetch current price and append to CSV."""
import time
import requests
from typing import List, Optional
from .cafef_parser import parse_stock_page
from .storage import append_realtime_row


def poll_symbols(symbols: List[str], url_template: str, interval: int = 60, out_dir: str = "data/realtime", max_iterations: Optional[int] = None):
    """Poll a list of symbols repeatedly every `interval` seconds.

    `url_template` must contain `{symbol}` placeholder.
    """
    iter_count = 0
    while True:
        for sym in symbols:
            try:
                url = url_template.format(symbol=sym)
                r = requests.get(url, timeout=15)
                r.raise_for_status()
                parsed = parse_stock_page(r.text)
                row = {**parsed}
                # include symbol in row
                row["symbol"] = sym
                append_realtime_row(sym, row, out_dir=out_dir)
            except Exception as e:
                # continue with other symbols
                print(f"Error polling {sym}: {e}")
        iter_count += 1
        if max_iterations is not None and iter_count >= max_iterations:
            break
        time.sleep(interval)
