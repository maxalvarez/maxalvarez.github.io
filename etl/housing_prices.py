"""
ETL for ABS Total Value of Dwellings, Table 2 (Median Price and Number of
Transfers, Capital City and Rest of State).

Downloads the ABS Excel workbook for the chosen release and writes a tidy
long CSV to data/processed/housing_prices.csv with one row per
(date, region, measure).

To point at a newer release, bump QUARTER_MONTH / YEAR and re-run.
"""

import calendar
import urllib.request
from pathlib import Path

import pandas as pd

QUARTER_MONTH = 6
YEAR = 2026

CATALOGUE = "6432.0"
TABLE = "Table 2"
RELEASE_LABEL = f"{calendar.month_name[QUARTER_MONTH]} Quarter {YEAR}"
_QUARTER_SLUG = f"{calendar.month_name[QUARTER_MONTH][:3].lower()}-quarter-{YEAR}"
URL = f"https://www.abs.gov.au/statistics/economy/price-indexes-and-inflation/total-value-dwellings/{_QUARTER_SLUG}/643202.xlsx"

_ROOT = Path(__file__).parent.parent
RAW_FILE = _ROOT / "data" / "raw" / "643202.xlsx"
PROCESSED_FILE = _ROOT / "data" / "processed" / "housing_prices.csv"

# ABS Data1 layout: row 0 is the per-column title, data starts at row 10.
TITLE_ROW = 0
DATA_START = 10


def download(url: str, dest: Path) -> None:
    print(f"Downloading {url}")
    dest.parent.mkdir(parents=True, exist_ok=True)
    urllib.request.urlretrieve(url, dest)
    print(f"Saved raw file to {dest}")


def extract(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name="Data1", header=None)
    print(f"Extracted {df.shape[0]} rows x {df.shape[1]} cols from sheet 'Data1'")
    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:
    titles = df.iloc[TITLE_ROW, 1:].tolist()
    body = df.iloc[DATA_START:, :].copy()
    body.columns = ["date"] + titles
    body["date"] = pd.to_datetime(body["date"])

    long = body.melt(id_vars="date", var_name="title", value_name="value").dropna(subset=["value"])
    split = long["title"].str.split(" ;  ", n=2, expand=True)
    long["measure"] = split[0].str.strip(" ;")
    long["region"] = split[1].str.strip(" ;")
    long["value"] = pd.to_numeric(long["value"])

    out = (
        long[["date", "region", "measure", "value"]]
        .sort_values(["measure", "region", "date"])
        .reset_index(drop=True)
    )
    print(
        f"Tidied to {len(out)} rows across "
        f"{out['measure'].nunique()} measures and {out['region'].nunique()} regions"
    )
    return out


def load(df: pd.DataFrame, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(dest, index=False)
    print(f"Saved processed data to {dest}")


if __name__ == "__main__":
    download(URL, RAW_FILE)
    df = extract(RAW_FILE)
    df = transform(df)
    load(df, PROCESSED_FILE)
    print("Done.")
