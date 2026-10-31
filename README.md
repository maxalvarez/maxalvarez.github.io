# maxalvarez.github.io

Personal site of brief economic and population analyses, built with Quarto and published to GitHub Pages.

## Setup

Install [Quarto](https://quarto.org) and Python 3.13, then:

```
pip install -r requirements.txt
```

## Build locally

The data is not committed, so download it first:

```
python etl/housing_prices.py
quarto preview
```

## Refresh the data

Set `QUARTER_MONTH` and `YEAR` in `etl/housing_prices.py` to the latest ABS release and push to `main`. The publish workflow re-downloads the data, renders the site and deploys it. If the ABS download fails, the deploy fails.
