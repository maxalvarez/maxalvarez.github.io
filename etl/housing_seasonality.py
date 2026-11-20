"""
X-13ARIMA-SEATS seasonality analysis of capital-city median dwelling prices.

For each capital and dwelling type (established houses and attached dwellings),
runs the US Census Bureau's X-13ARIMA-SEATS program (through statsmodels) on
log median prices: automatic SARIMA selection, automatic outlier
detection and the X-11 seasonal filter. Records the seasonality tests X-13
reports and the seasonal factors by quarter.

Needs the x13as binary: set the X13PATH environment variable to the folder that
holds x13as (or x13as.exe). Run after etl/housing_prices.py.
"""

import os
import re

import numpy as np
import pandas as pd
from statsmodels.tsa.x13 import x13_arima_analysis

from housing_prices import CAPITALS, DWELLINGS, PROCESSED_FILE, load_capitals

TESTS_FILE = PROCESSED_FILE.parent / "housing_x13_tests.csv"
FACTORS_FILE = PROCESSED_FILE.parent / "housing_x13_factors.csv"

# statsmodels fills in the series{} block. Log transform, automatic model and outliers,
# no trading-day regressors (quarterly medians). print=all is the only way to get table
# D 8.A, which holds the combined test for identifiable seasonality.
SPEC = """series{}
transform{function=log}
outlier{}
automdl{maxorder=(2 1) maxdiff=(2 1)}
x11{print=all save=(d11 d12 d13)}"""


def parse_output(text: str) -> dict:
    """Pull the model, outliers and seasonality tests out of the X-13 text output."""
    model = re.search(r"Final automatic model choice\s*:\s*(.+)", text)
    outliers = re.findall(r"^\s+((?:AO|LS|TC)\d{4}\.\d)\s", text, flags=re.MULTILINE)

    # Summary table F 2.I: statistic, then probability level in per cent.
    def f2i(label: str) -> tuple[float, float]:
        match = re.search(label + r"[^:]*:\s*([\d.]+)\s+([\d.]+)%", text, flags=re.DOTALL)
        if not match:
            return np.nan, np.nan
        return float(match.group(1)), round(float(match.group(2)) / 100, 6)

    stable_f, stable_p = f2i(r"F-test for stable seasonality from Table D 8\.")
    kw, kw_p = f2i(r"Kruskal-Wallis Chi Squared test\s+for stable seasonality from Table D 8\.")
    moving_f, moving_p = f2i(r"F-test for moving seasonality from Table D 8\.")

    combined = re.search(
        r"COMBINED TEST FOR THE PRESENCE OF IDENTIFIABLE SEASONALITY\s+(.+?)\n", text
    )
    m7 = re.search(r"M7\s*=\s*([\d.]+)", text)

    return {
        "x13_model": model.group(1).strip() if model else None,
        "outliers": " ".join(outliers),
        "stable_f": stable_f, "stable_p": stable_p,
        "kruskal_wallis": kw, "kruskal_wallis_p": kw_p,
        "moving_f": moving_f, "moving_p": moving_p,
        # Verdict only, e.g. "present" or "probably not present".
        "combined_test": (
            combined.group(1).strip().lower().removeprefix("identifiable seasonality ")
            if combined else None
        ),
        "m7": float(m7.group(1)) if m7 else np.nan,
    }


def run_city(y: pd.Series) -> tuple[dict, pd.Series]:
    """Run X-13 on one city's price series (quarterly PeriodIndex).

    Returns the test results and the seasonal factors in % (observed / seasonally adjusted).
    """
    res = x13_arima_analysis(y, rawspec=SPEC)
    tests = parse_output(res.results)
    factors = (y / res.seasadj - 1) * 100
    return tests, factors


def main() -> None:
    if not os.getenv("X13PATH"):
        raise SystemExit(
            "X13PATH is not set. Point it at the folder holding the Census Bureau's "
            "x13as binary (https://www.census.gov/data/software/x13as.html)."
        )
    tests, factors = [], []
    for dwelling in DWELLINGS:
        prices = load_capitals(dwelling)
        prices.index = pd.PeriodIndex(prices.index, freq="Q")
        by_city = []
        for city in CAPITALS:
            print(f"Running X-13 for {city}, {dwelling.lower()}")
            result, factor = run_city(prices[city].dropna())
            tests.append({"dwelling": dwelling, "city": city, **result})
            by_city.append(factor.rename(city))
        by_city = pd.concat(by_city, axis=1)
        # Same date convention as the processed prices: first day of the quarter's last month.
        by_city.index = by_city.index.asfreq("M", "end").to_timestamp().rename("date")
        factors.append(by_city.assign(dwelling=dwelling).set_index("dwelling", append=True))

    tests = pd.DataFrame(tests)
    # One row per (date, dwelling), one column per capital.
    factors = pd.concat(factors).sort_index()

    tests.to_csv(TESTS_FILE, index=False)
    factors.to_csv(FACTORS_FILE)
    print(f"Saved tests to {TESTS_FILE} and seasonal factors to {FACTORS_FILE}")


if __name__ == "__main__":
    main()
