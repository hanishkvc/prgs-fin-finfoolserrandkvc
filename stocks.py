# Work with stock data
# HanishKVC, 2021
# GPL



import edb
import plot as mPlot
import procedb



def load(startDate, endDate=None):
    edb.load_data_stocks(startDate, endDate)


def prep():
    procedb.ops(['dma50=dma50(data)', 'dma200=dma200(data)'])


def _plot(entCodes):
    mPlot._data(['data', 'dma50', 'dma200'], entCodes)
    mPlot.linregress('data', entCodes)


