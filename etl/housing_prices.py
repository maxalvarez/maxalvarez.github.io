"""
ETL script: ABS Total Value of Dwellings
Source: https://www.abs.gov.au/statistics/economy/price-indexes-and-inflation/total-value-dwellings/

Downloads the raw Excel file from ABS and saves cleaned data to data/processed/.
Run this script manually to refresh the data before rendering the analysis.

Usage:
    python etl/housing_prices.py
"""

import pandas as pd
from pathlib import Path

# --- Config ---
URL = "https://www.abs.gov.au/statistics/economy/price-indexes-and-inflation/total-value-dwellings/dec-quarter-2025/643202.xlsx"
RAW_DIR = Path(__file__).parent.parent / "data" / "raw"
PROCESSED_DIR = Path(__file__).parent.parent / "data" / "processed"
RAW_FILE = RAW_DIR / "643202.xlsx"
PROCESSED_FILE = PROCESSED_DIR / "housing_prices.csv"


def download(url: str, dest: Path) -> None:
    import urllib.request
    print(f"Downloading {url}")
    urllib.request.urlretrieve(url, dest)
    print(f"Saved raw file to {dest}")


def extract(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name="Data1")
    print(f"Extracted {len(df)} rows from sheet 'Data1'")
    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:
    # TODO: expand this as the analysis develops.
    return df


def load(df: pd.DataFrame, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(dest, index=False)
    print(f"Saved processed data to {dest}")


if __name__ == "__main__":
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    download(URL, RAW_FILE)
    xl = extract(RAW_FILE)
    df = transform(xl)
    load(df, PROCESSED_FILE)
    print("Done.")
