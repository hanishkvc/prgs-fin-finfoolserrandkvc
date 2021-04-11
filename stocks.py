# Work with stock data
# HanishKVC, 2021
# GPL



import time
import edb
import plot as mPlot
import procedb



def load(startDate=None, endDate=None):
    if startDate == None:
        endDate = time.gmtime().tm_year
        startDate = endDate - 7
    edb.load_data_stocks(startDate, endDate)


def prep():
    procedb.ops(['dma50=dma50(data)', 'dma200=dma200(data)'])


def _plot(entCodes):
    mPlot._data(['data', 'dma50', 'dma200'], entCodes)
    mPlot.linregress('data', entCodes)


