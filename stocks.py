# Work with stock data
# HanishKVC, 2021
# GPL



import time
import edb
import plot as mPlot
import procedb



def load(startDate=None, endDate=None):
    """
    Load data from stocks related data sources.
    If no date range is given, then it loads data for the last 7 years.
    """
    if startDate == None:
        endDate = time.gmtime().tm_year
        startDate = endDate - 7
    edb.load_data_stocks(startDate, endDate)


def prep():
    """
    Calculate some of the data required for later.
    """
    procedb.ops(['mas50=mas50(data)', 'mas200=mas200(data)'])
    procedb.ops(['mae24=mae24(data)', 'mae50=mae50(data)'])


def _plot(entCodes):
    """
    Plot data related to the given set of entCodes.

    This includes the close related
        raw, mas50 and mas200 data as well as
        linear regression based lines wrt 1Y, 3Y and 5Y.
    """
    mPlot._data(['data', 'mas50', 'mas200', 'mae24', 'mae50'], entCodes)
    mPlot.linregress('data', entCodes)


