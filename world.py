#!/usr/bin/env python3
# Market Data World
# HanishKVC, 2021
# GPL
#

import sys
import time
import hlpr


## From https://fred.stlouisfed.org/graph/fredgraph.csv
## If date range is not specified or wrong date given, the server seems to dump all data it has.
## Certain things like S&P500/DJIA seem to provide only Last 10Years of data at max.
#INDEX_US_NASDAQCOM_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=NASDAQCOM&cosd=1971-02-05&coed=2021-03-08"
#INDEX_US_NASDAQ100_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=NASDAQ100&cosd=1986-01-02&coed=2021-03-08"
#INDEX_US_SANDP500_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=SP500&cosd=2011-03-09&coed=2021-03-08"
#INDEX_US_DJIA_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DJIA&cosd=2011-03-09&coed=2021-03-08"
#INDEX_US_DJCA_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DJCA&cosd=2011-03-09&coed=2021-03-08"
#INDEX_US_ICEBOFAUSCORP_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=BAMLCC0A0CMTRIV&cosd=1971-02-05&coed=2021-03-08"
#COM_GLOBAL_BRENTCRUDE_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=POILBREUSDM&cosd=1990-01-01&coed=2021-03-08"
#COM_UK_GOLD_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=GOLDPMGBD228NLBM&cosd=1968-04-01&coed=2021-03-08"
#COM_UK_SILVER_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=SLVPRUSD&cosd=2017-10-02&coed=2021-03-08"
WORLD_DATE_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id={}&cosd={}-{:02}-{:02}&coed={}-{:02}-{:02}"
WORLD_FULL_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id={}"

## Alternate data sources
## The datahub.io sources are not uptodate
#INDEX_SANDP500_URL = "https://datahub.io/core/s-and-p-500/r/data.csv"
#COM_GOLD_URL = "https://datahub.io/core/gold-prices/r/monthly.csv"


DATASETS = [
        [ "INDEX_US_NASDAQCOM", "NASDAQCOM" ],
        [ "INDEX_US_NASDAQ100", "NASDAQ100" ],
        [ "INDEX_US_SANDP500", "SP500" ],
        [ "INDEX_US_DJIA", "DJIA" ],
        [ "INDEX_US_DJCA", "DJCA" ],
        [ "INDEX_US_ICEBOFAUSCORP", "BAMLCC0A0CMTRIV" ],
        [ "COM_GLOBAL_BRENTCRUDE", "POILBREUSDM" ],
        [ "COM_UK_GOLD", "GOLDPMGBD228NLBM" ],
        [ "COM_UK_SILVER", "SLVPRUSD" ],
        ]


def fetch(y=None):
    """
    Fetch either a given year's data or the full data available from the given source.

    If year is not specified, then get what ever the server gives when date range is not given.
    """
    for ds in DATASETS:
        if y != None:
            turl = WORLD_DATE_URL.format(ds[1], y,1,1,y,12,31)
            fName = "{}-{}.csv".format(ds[0], y)
        else:
            turl = WORLD_FULL_URL.format(ds[1])
            fTime = time.strftime("%Y%m%d")
            fName = "{}-{}.csv".format(ds[0], fTime)
        hlpr.wget_better(turl, fName)


if len(sys.argv) < 2:
    year = None
else:
    year = sys.argv[1]
fetch(year)

