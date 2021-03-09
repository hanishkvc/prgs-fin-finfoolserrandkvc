#!/usr/bin/env python3
# Market Data World
# HanishKVC, 2021
# GPL
#

import sys
import hlpr


## From https://fred.stlouisfed.org/graph/fredgraph.csv
#INDEX_US_NASDAQCOM_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=NASDAQCOM&cosd=1971-02-05&coed=2021-03-08"
#INDEX_US_NASDAQ100_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=NASDAQ100&cosd=1986-01-02&coed=2021-03-08"
#INDEX_US_SANDP500_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=SP500&cosd=2011-03-09&coed=2021-03-08"
#INDEX_US_DJIA_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DJIA&cosd=2011-03-09&coed=2021-03-08"
#INDEX_US_DJCA_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DJCA&cosd=2011-03-09&coed=2021-03-08"
#INDEX_US_ICEBOFAUSCORP_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=BAMLCC0A0CMTRIV&cosd=1971-02-05&coed=2021-03-08"
#COM_BRENTCRUDE_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=POILBREUSDM&cosd=1990-01-01&coed=2021-03-08"
#COM_UK_GOLD_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=GOLDPMGBD228NLBM&cosd=1968-04-01&coed=2021-03-08"
#COM_UK_SILVER_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id=SLVPRUSD&cosd=2017-10-02&coed=2021-03-08"
WORLD_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv?id={}&cosd={}-{:02}-{:02}&coed={}-{:02}-{:02}"

## Alternate data sources
## The datahub.io sources are not uptodate
#INDEX_SANDP500_URL = "https://datahub.io/core/s-and-p-500/r/data.csv"
#COM_GOLD_URL = "https://datahub.io/core/gold-prices/r/monthly.csv"


DATASETS = [
        [ "INDEX_US_NASDAQCOM", "NASDAQCOM" ],
        [ "INDEX_US_SANDP500", "SP500" ]
        ]


def fetch4year(y):
    for ds in DATASETS:
        turl = WORLD_URL.format(ds[1], y,1,1,y,12,31)
        fName = "{}-{}.csv".format(ds[0], y)
        hlpr.wget_better(turl, fName)


fetch4year(sys.argv[1])
