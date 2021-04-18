# Plot data
# HanishKVC, 2021
# GPL

import numpy
import scipy
from scipy import stats
import matplotlib
import matplotlib.pyplot as plt
import hlpr
import edb


giLabelNameChopLen = 36
giLabelCodeChopLen = 16


def _entDB(entDB=None):
    """
    Either use the passed entDB or else the gEntDB, which might
    have been set by the user previously.
    """
    if entDB == None:
        return edb.gEntDB
    return entDB


def _axes(axes=None):
    """
    Return the main axes being used for plotting,
    if a explicit axes is not specified.
    """
    if axes == None:
        return plt.gca()
    return axes


def inset_axes(bounds, sTitle=None, bTransparent=True, bShareXAxis=True, bYTicksRight=True, axes=None):
    """
    Create a inset axes, for the given axes.

    By default it
        Shares the x-axis with parent axes
        is transparent
        has yticks to the right side
    """
    axes = _axes(axes)
    if bShareXAxis:
        sharex = axes
    else:
        sharex = None
    iaxes = axes.inset_axes(bounds, sharex=sharex)
    if sTitle:
        iaxes.set_title(sTitle)
    if bTransparent:
        iaxes.patch.set_alpha(0.5)
    if bYTicksRight:
        iaxes.yaxis.tick_right()
    return iaxes


def _bar(dataKey, entCode, startDate=-1, endDate=-1, entDB=None, axes=None):
    """
    Bar plot the dataKey's data for the specified entCode, over the given date range.
    """
    entDB = _entDB(entDB)
    axes = _axes(axes)
    startDateIndex, endDateIndex = entDB.daterange2index(startDate, endDate)
    x = numpy.arange(startDateIndex, endDateIndex+1)
    index = entDB.meta['codeD'][entCode]
    axes.bar(x, entDB.data[dataKey][index,startDateIndex:endDateIndex+1])


def _data(dataKeys, entCodes, startDate=-1, endDate=-1, entDB=None, axes=None):
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
    axes = _axes(axes)
    startDateIndex, endDateIndex = entDB.daterange2index(startDate, endDate)
    x = numpy.arange(startDateIndex, endDateIndex+1)
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
            label = "{:<{cwidth}}:{:{width}}: {}".format(entCode, name, dataLabel, cwidth=giLabelCodeChopLen, width=giLabelNameChopLen)
            print("\t{}:{}".format(label, index))
            label = "{:<{cwidth}}:{:{width}}: {:16} : {}".format(entCode, name, dataKey, dataLabel, cwidth=giLabelCodeChopLen, width=giLabelNameChopLen)
            axes.plot(x, entDB.data[dataKey][index, startDateIndex:endDateIndex+1], label=label)


def data(dataKeys, entTypeTmpls, entNameTmpls, startDate=-1, endDate=-1, entDB=None, axes=None):
    """
    Plot specified datas for the specified entities from entDB, over the specified
    date range.

    dataKeys: Is a key or a list of keys used to retreive the data from entDB.data.
    entTypeTmpls: matching templates used to identify entTypes.
    entNameTmpls: matching templates used to identify entities within selected entTypes.
    startDate and endDate: specify the date range over which the data should be
        retreived and plotted.

    Remember to call show func, when you want to see plots, accumulated till then.
    """
    entDB = _entDB(entDB)
    entCodes = entDB.list_type_members(entTypeTmpls, entNameTmpls)
    _data(dataKeys, entCodes, startDate, endDate, entDB, axes)


def _fit(dataKeys, entCodes, startDate=-1, endDate=-1, fitType='linregress', entDB=None, axes=None):
    """
    Plot a fitting line along with the data it tries to fit.

    It supports fitting using different logics.

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
            entName = entDB.meta['name'][index][:giLabelNameChopLen]
            y = entDB.data[dataKey][index][startDateIndex:endDateIndex]
            x = numpy.arange(startDateIndex, endDateIndex)
            gFits[fitType](entCode, entName, x, y, axes)


gbLineRegressPlotData = False
def _linregress(entCode, entName, x, y, axes):
    """
    Use linear regression to plot a fitting line along with the data it
    tries to fit.

    By default the actual data is not plotted. Set gbLineRegressPlotData,
    if you want this logic to plot data.
    """
    axes = _axes(axes)
    numDays = len(x)
    lr = stats.linregress(x,y)
    if gbLineRegressPlotData:
        label = "{:<20}:{}:Data:{}days".format(entCode, entName, numDays)
        plt.plot(x, y, label=label)
    label = "{:<20}:{}:LinRegressLineFit:{}".format(entCode, entName, numDays)
    axes.plot(x, x*lr.slope+lr.intercept, label=label)


def linregress(dataKeys, entCodes, days=[7, '1M', '6M', '1Y', '3Y', '5Y'], entDB=None, axes=None):
    """
    For the given dataKeys and entCodes, plot the corresponding data
    as well as curve fitting lines based on linear regression for
    1Year, 3Year and 5Years of data.
    """
    entDB = _entDB(entDB)
    startDateIndex, endDateIndex = entDB.daterange2index(-1, -1)
    for d in days:
        if type(d) == str:
            d = hlpr.days_in(d, entDB.bSkipWeekends)
        endDate = entDB.dates[endDateIndex]
        startDate = entDB.dates[endDateIndex-d]
        if entDB.datesD.get(startDate, -1) >= 0:
            _fit(dataKeys, entCodes, startDate, endDate, 'linregress', entDB, axes)


def _spline(entCode, entName, x, y, axes):
    """
    Use linear regression to plot a fitting line along with the data it
    tries to fit.
    """
    axes = _axes(axes)
    numDays = len(x)
    spl = scipy.interpolate.splrep(x,y, k=3)
    label = "{}:{}:Data:{}days".format(entCode, entName, numDays)
    axes.plot(x, y*1.01, label=label)
    label = "{}:{}:SplineFit".format(entCode, entName)
    x = numpy.append(x, numpy.arange(x[-1],x[-1]+2))
    ySpl = scipy.interpolate.splev(x, spl)
    axes.plot(x, ySpl, label=label)


gFits = {
    'linregress': _linregress,
    'spline': _spline
    }


def _show(entDB, axes=None):
    """
    Show the data plotted till now.
    """
    axes = _axes(axes)
    leg = axes.legend()
    plt.setp(leg.texts, family='monospace')
    for line in leg.get_lines():
        line.set_linewidth(8)
    axes.grid(True)
    startDateIndex, endDateIndex = entDB.daterange2index(-1,-1)
    curDates = entDB.dates[startDateIndex:endDateIndex+1].astype('int')
    """
    numX = len(curDates)
    xTicks = (numpy.linspace(0,1,9)*numX).astype(int)
    xTicks[-1] -= 1
    xTickLabels = numpy.array(curDates)[xTicks]
    plt.xticks(xTicks, xTickLabels, rotation='vertical')
    """
    axes.xaxis.set_major_formatter(matplotlib.ticker.IndexFormatter(curDates))


def show(entDB=None, axes=None):
    """
    Show the data plotted till now.
    """
    entDB = _entDB(entDB)
    _show(entDB, axes)
    plt.show()


