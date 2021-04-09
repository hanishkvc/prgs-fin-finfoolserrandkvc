# Plot data
# HanishKVC, 2021
# GPL

import numpy
from scipy import stats
import matplotlib.pyplot as plt
import hlpr


giLabelNameChopLen = 36
giLabelCodeChopLen = 16


gEntDB = None
def _entDB(entDB=None):
    """
    Either use the passed entDB or else the gEntDB, which might
    have been set by the user previously.
    """
    if entDB == None:
        return gEntDB
    return entDB


def data(dataKeys, entCodes, startDate=-1, endDate=-1, entDB=None):
    """
    Plot specified datas for the specified entities from entDB, over the specified
    date range.

    dataKeys: Is a key or a list of keys used to retreive the data from entDB.data.
    entCodes: Is a entCode or a list of entCodes.
    startDate and endDate: specify the date range over which the data should be
        retreived and plotted.

    Remember to call show func, when you want to see plots, accumulated till then.
    """
    entDB = _entDB(entDB)
    startDateIndex, endDateIndex = entDB.daterange2index(startDate, endDate)
    x = numpy.arange(startDateIndex, endDateIndex)
    if type(dataKeys) == str:
        dataKeys = [ dataKeys ]
    if (type(entCodes) == int) or (type(entCodes) == str):
        entCodes = [ entCodes]
    for dataKey in dataKeys:
        print("DBUG:plot_data:{}".format(dataKey))
        dataKeyMetaData, dataKeyMetaLabel = hlpr.data_metakeys(dataKey)
        for entCode in entCodes:
            index = entDB.meta['codeD'][entCode]
            name = entDB.meta['name'][index][:giLabelNameChopLen]
            try:
                dataLabel = entDB.data[dataKeyMetaLabel][index]
            except:
                dataLabel = ""
            label = "{:{cwidth}}:{:{width}}: {}".format(entCode, name, dataLabel, cwidth=giLabelCodeChopLen, width=giLabelNameChopLen)
            print("\t{}:{}".format(label, index))
            label = "{:{cwidth}}:{:{width}}: {:16} : {}".format(entCode, name, dataKey, dataLabel, cwidth=giLabelCodeChopLen, width=giLabelNameChopLen)
            plt.plot(x, entDB.data[dataKey][index, startDateIndex:endDateIndex+1], label=label)


def _linregress(dataKeys, entCodes, startDate=-1, endDate=-1, entDB=None):
    """
    Use linear regression to plot a fitting line along with the data it
    tries to fit.

    User can specify the subset of data that needs to be fitted, by specifying
    a) the entCodes
    b) the dataKeys
    c) the date range
    within the entDB.

    The data will be plotted at the appropriate location corresponding to the
    specified date range, in the plot.
    """
    entDB = _entDB(entDB)
    startDateIndex, endDateIndex = entDB.daterange2index(startDate, endDate)
    if type(dataKeys) == str:
        dataKeys = [ dataKeys ]
    if (type(entCodes) == int) or (type(entCodes) == str):
        entCodes = [ entCodes]
    for dataKey in dataKeys:
        for entCode in entCodes:
            index = entDB.meta['codeD'][entCode]
            y = entDB.data[dataKey][index][startDateIndex:endDateIndex]
            x = numpy.arange(startDateIndex, endDateIndex)
            lr = stats.linregress(x,y)
            plt.plot(x,y,'.')
            plt.plot(x,x*lr.slope+lr.intercept)


def linregress(dataKeys, entCodes, entDB=None):
    """
    For the given dataKeys and entCodes, plot the corresponding data
    as well as curve fitting lines based on linear regression for
    1Year, 3Year and 5Years of data.
    """
    entDB = _entDB(entDB)
    startDateIndex, endDateIndex = entDB.daterange2index(-1, -1)
    for d in [1, 3, 5]:
        endDate = entDB.dates[endDateIndex]
        startDate = endDate-d*10000
        if entDB.datesD.get(startDate, -1) >= 0:
            _linregress(dataKeys, entCodes, startDate, endDate, entDB)


def _show(entDB):
    """
    Show the data plotted till now.
    """
    leg = plt.legend()
    plt.setp(leg.texts, family='monospace')
    for line in leg.get_lines():
        line.set_linewidth(8)
    plt.grid(True)
    startDateIndex, endDateIndex = entDB.daterange2index(-1,-1)
    curDates = entDB.dates[startDateIndex:endDateIndex+1]
    numX = len(curDates)
    xTicks = (numpy.linspace(0,1,9)*numX).astype(int)
    xTicks[-1] -= 1
    xTickLabels = numpy.array(curDates)[xTicks]
    plt.xticks(xTicks, xTickLabels, rotation='vertical')
    plt.show()


def show(entDB=None):
    """
    Show the data plotted till now.
    """
    entDB = _entDB(entDB)
    _show(entDB)


