"""
X-13ARIMA-SEATS seasonality analysis of capital-city median house prices.

For each capital, runs the US Census Bureau's X-13ARIMA-SEATS program (through
statsmodels) on log median prices: automatic SARIMA selection, automatic outlier
detection and the X-11 seasonal filter. Records the seasonality tests X-13
reports and the seasonal factors by quarter. pmdarima's auto_arima is fitted to
the same log series as a cross-check of the SARIMA order X-13 chooses.

Needs the x13as binary: set the X13PATH environment variable to the folder that
holds x13as (or x13as.exe). Run after etl/housing_prices.py.
"""

import os
import re
import warnings

import numpy as np
import pandas as pd
import pmdarima as pm
from statsmodels.tsa.x13 import x13_arima_analysis

from housing_prices import CAPITALS, PROCESSED_FILE, load_capitals

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


def _full_order(order: str) -> str:
    """A model with no seasonal ARIMA terms prints as "(p d q)" only; add "(0 0 0)"."""
    return order if order.count("(") == 2 else order + "(0 0 0)"


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
        "x13_model": _full_order(model.group(1).strip()) if model else None,
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


def pmdarima_order(y: pd.Series) -> str:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        fit = pm.auto_arima(
            np.log(y), seasonal=True, m=4, information_criterion="bic",
            stepwise=True, suppress_warnings=True, error_action="ignore",
        )
    (p, d, q), (sp, sd, sq, _) = fit.order, fit.seasonal_order
    return f"({p} {d} {q})({sp} {sd} {sq})"


def run_city(y: pd.Series) -> tuple[dict, pd.Series]:
    """Run X-13 on one city's price series (quarterly PeriodIndex).

    Returns the test results and the seasonal factors in % (observed / seasonally adjusted).
    """
    res = x13_arima_analysis(y, rawspec=SPEC)
    tests = parse_output(res.results)
    tests["pmdarima_model"] = pmdarima_order(y)
    factors = (y / res.seasadj - 1) * 100
    return tests, factors


def main() -> None:
    if not os.getenv("X13PATH"):
        raise SystemExit(
            "X13PATH is not set. Point it at the folder holding the Census Bureau's "
            "x13as binary (https://www.census.gov/data/software/x13as.html)."
        )
    prices = load_capitals()
    prices.index = pd.PeriodIndex(prices.index, freq="Q")

    tests, factors = [], []
    for city in CAPITALS:
        print(f"Running X-13 for {city}")
        result, factor = run_city(prices[city].dropna())
        tests.append({"city": city, **result})
        factors.append(factor.rename(city))

    tests = pd.DataFrame(tests)
    factors = pd.concat(factors, axis=1)
    # Same date convention as the processed prices: first day of the quarter's last month.
    factors.index = factors.index.asfreq("M", "end").to_timestamp().rename("date")

    tests.to_csv(TESTS_FILE, index=False)
    factors.to_csv(FACTORS_FILE)
    print(f"Saved tests to {TESTS_FILE} and seasonal factors to {FACTORS_FILE}")


if __name__ == "__main__":
    main()
