from pathlib import Path
import pandas as pd


def ensure_dir(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)


def save_ohlc_csv(symbol: str, df: pd.DataFrame, out_dir: str = "data/historical") -> Path:
    """Save historical OHLC DataFrame to CSV. Overwrites existing file for that symbol.

    Args:
        symbol: stock symbol string used for filename (safe to include exchange prefix)
        df: pandas DataFrame with a Date-like index or a `date` column
        out_dir: directory to store CSV files

    Returns:
        Path to saved CSV file
    """
    out = Path(out_dir) / f"{symbol}.csv"
    ensure_dir(out)
    if not df.index.name:
        if "date" in df.columns:
            df = df.set_index("date")
    df.to_csv(out, index=True)
    return out


def append_realtime_row(symbol: str, row: dict, out_dir: str = "data/realtime") -> Path:
    """Append a single row (dict) for realtime data into CSV (creates file if missing).

    The row should contain a timestamp field (e.g., `timestamp`) and price fields.
    """
    out = Path(out_dir) / f"{symbol}_realtime.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame([row])
    header = not out.exists()
    df.to_csv(out, mode="a", header=header, index=False)
    return out
