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


def _plot_prep():
    """
    Calculate some of the data required for later.
    """
    procedb.ops(['mas50=mas50(data)', 'mas200=mas200(data)'])
    procedb.ops(['mae9=mae9(data)', 'mae26=mae26(data)', 'mae50=mae50(data)'])


def _plot(entCodes, bLinRegress=False):
    """
    Plot data related to the given set of entCodes.

    This includes the close related
        raw, mas50 and mas200 data as well as
        linear regression based lines wrt 3M, 6M, 1Y and 3Y.
    """
    mPlot._data(['data', 'mas200', 'mae9', 'mae26', 'mae50'], entCodes)
    if bLinRegress:
        mPlot.linregress('data', entCodes, days=['3M','6M','1Y','3Y'])


def plot(entCodes, bLinRegress=False):
    """
    Plot a predefined set of data wrt each entCode in the given list.
    """
    for entCode in entCodes:
        _plot(entCode, bLinRegress)
        mPlot.show()


def prep():
    _plot_prep()
    procedb.infoset1_prep()


def topbottom():
    for sType in [ 'nse nifty 50', 'nse nifty 100' ]:
        input("About to look at Top and Bottom N stocks wrt {}".format(sType))
        procedb.infoset1_result(sType, resultType='result2', bPrompt=True)


