"""
ETL for ABS Total Value of Dwellings, Table 2 (Median Price and Number of
Transfers, Capital City and Rest of State).

Downloads the ABS Excel workbook for the chosen release and writes a tidy
long CSV to data/processed/housing_prices.csv with one row per
(date, region, measure).

To point at a newer release, bump QUARTER_MONTH / YEAR. The download URL and the
post's link and sample dates follow, and CI re-runs this on push.
"""

import calendar
import urllib.request
from pathlib import Path

import pandas as pd

# Reference quarter of the ABS release: end month (3, 6, 9 or 12) and year.
QUARTER_MONTH = 6
YEAR = 2026

_MONTH = calendar.month_name[QUARTER_MONTH]
_QUARTER_SLUG = f"{_MONTH[:3].lower()}-quarter-{YEAR}"
# The release's web page (linked from the post) and its Table 2 workbook.
RELEASE_PAGE = f"https://www.abs.gov.au/statistics/economy/price-indexes-and-inflation/total-value-dwellings/{_QUARTER_SLUG}"
URL = f"{RELEASE_PAGE}/643202.xlsx"

_ROOT = Path(__file__).parent.parent
RAW_FILE = _ROOT / "data" / "raw" / "643202.xlsx"
PROCESSED_FILE = _ROOT / "data" / "processed" / "housing_prices.csv"

# Series analysed in the post, keyed by dwelling type, and the order cities are displayed in.
DWELLINGS = {
    "Established houses": "Median Price of Established House Transfers (Unstratified)",
    "Attached dwellings": "Median Price of Attached Dwelling Transfers (Unstratified)",
}
CAPITALS = [
    "Sydney", "Melbourne", "Brisbane", "Adelaide",
    "Perth", "Hobart", "Darwin", "Canberra",
]

# ABS Data1 layout: row 0 is the per-column title, data starts at row 10.
TITLE_ROW = 0
DATA_START = 10


def download(url: str, dest: Path) -> None:
    print(f"Downloading {url}")
    urllib.request.urlretrieve(url, dest)
    print(f"Saved raw file to {dest}")


def extract(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name="Data1", header=None)
    print(f"Extracted {df.shape[0]} rows x {df.shape[1]} cols from sheet 'Data1'")
    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:
    titles = df.iloc[TITLE_ROW, 1:].tolist()
    body = df.iloc[DATA_START:].copy()
    body.columns = ["date"] + titles
    body["date"] = pd.to_datetime(body["date"])

    long = body.melt(id_vars="date", var_name="title", value_name="value").dropna(subset=["value"])
    # ABS titles read "<measure> ;  <region> ;", with a double space after the first semicolon.
    long[["measure", "region"]] = long["title"].str.rstrip(" ;").str.split(" ;  ", expand=True)
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


def load_capitals(dwelling: str) -> pd.DataFrame:
    """Wide table of one DWELLINGS series from the processed CSV: one column per capital, in CAPITALS order."""
    df = pd.read_csv(PROCESSED_FILE, parse_dates=["date"])
    return (
        df.loc[df["measure"] == DWELLINGS[dwelling]]
          .pivot(index="date", columns="region", values="value")[CAPITALS]
    )


def load(df: pd.DataFrame, dest: Path) -> None:
    df.to_csv(dest, index=False)
    print(f"Saved processed data to {dest}")


if __name__ == "__main__":
    download(URL, RAW_FILE)
    df = extract(RAW_FILE)
    df = transform(df)
    load(df, PROCESSED_FILE)
    print("Done.")
