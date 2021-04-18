# Work with stock data
# HanishKVC, 2021
# GPL



import time
import edb
import plot as eplot
import procedb
import ops
import hlpr



def load(startDate=None, endDate=None, bSkipWeekends=True):
    """
    Load data from stocks related data sources.
    If no date range is given, then it loads data for the last 7 years.
    NOTE: By default weekends are skipped, user can change this
    by setting bSkipWeekends argument appropriately.
    """
    edb.skip_weekends(bSkipWeekends)
    if startDate == None:
        endDate = time.gmtime().tm_year
        startDate = endDate - 7
    edb.load_data_stocks(startDate, endDate)


def _plot_prep(opts):
    """
    Calculate some of the data required for later.
    opts: a dictionary of optional arguments to control the logic.
        'bJwwRSI': If True, Jww RSI will be shown by plot, by default.
            else SMA based RSI.
    """
    procedb.ops(['mas50=mas50(data)', 'mas200=mas200(data)'])
    procedb.ops(['mae9=mae9(data)', 'mae26=mae26(data)', 'mae50=mae50(data)'])
    procedb.ops(['mas10Vol=mas10(volume)'])
    ops.weekly_view(['open','high','low','close','volume'], ['s','M','m','e','a'], "w.{}")
    ops.monthly_view(['open','high','low','close','volume'], ['s','M','m','e','a'], "m.{}")
    ops.pivotpoints('pp')
    ops.pivotpoints('ppW', "w.{}", dateIndex=-1)
    ops.pivotpoints('ppM', "m.{}", dateIndex=-1)
    ops.jww_rsi('rsiJWW', 'data')
    ops.ma_rsi('rsiSMA', 'data')
    bJwwRSI = opts.get('bJwwRSI', False)
    if bJwwRSI:
        edb.gEntDB.data['rsi'] = edb.gEntDB.data['rsiJWW']
    else:
        edb.gEntDB.data['rsi'] = edb.gEntDB.data['rsiSMA']


def _plot_volume(dataKeys, entCode, insetId=0):
    ia = eplot.inset_axes([0,0.0*insetId,1,0.1], sTitle="Volumes")
    eplot._data(dataKeys, entCode, axes=ia)


def _plot_rsi(dataKey, entCode, insetId=0):
    ia = eplot.inset_axes([0,0.1*insetId,1,0.1], sTitle="RSI")
    ops.plot_rsi(dataKey, entCode, axes=ia)


def _plot(entCodes, bPivotPoints=True, bVolumes=True, bRSI=True, bLinRegress=False):
    """
    Plot data related to the given set of entCodes.

    This includes
        the close related
            raw, mas50 and mas200 data as well as
            linear regression based lines wrt 3M, 6M, 1Y and 3Y.
        Volumes traded.
        PivotPoints.
            day based pivot line drawn across 2 weeks
            week based pivot line drawn across 6 weeks (1.5 months)
            month based pivot line drawn across 12 weeks (3 months)

    Even thou entCodes can be passed as a list, passing a single
    entCode may be more practically useful. Also plot_pivotpoints
    currently supports a single entCode only.
    """
    entDB = edb.gEntDB
    weekDays = hlpr.days_in('1W', entDB.bSkipWeekends)
    eplot._data(['data', 'mas200', 'mae9', 'mae26', 'mae50'], entCodes)
    if bPivotPoints:
        ops.plot_pivotpoints('pp', entCodes, plotRange=weekDays, axes=eplot._axes())
        ops.plot_pivotpoints('ppW', entCodes, plotRange=weekDays*3, axes=eplot._axes())
        ops.plot_pivotpoints('ppM', entCodes, plotRange=weekDays*6, axes=eplot._axes())
    if bVolumes:
        _plot_volume(['volume', 'mas10Vol'], entCodes, 1)
    if bRSI:
        _plot_rsi('rsi', entCodes, 0)
    if bLinRegress:
        eplot.linregress('data', entCodes, days=['3M','6M','1Y','3Y'])


def plot(entCodes, bPivotPoints=True, bVolumes=True, bRSI=True, bLinRegress=False):
    """
    Plot a predefined set of data wrt each entCode in the given list.
    """
    datas = [
        ['srel', 'srelMetaLabel'],
        ['roll3Y', 'roll3YMetaLabel'],
        ['retOn', 'rosafMetaLabel']
        ]
    entDB = edb.gEntDB
    if type(entCodes) == str:
        entCodes = [ entCodes ]
    for entCode in entCodes:
        entIndex = entDB.meta['codeD'][entCode]
        entName = entDB.meta['name'][entIndex]
        print("\n\nEntity: {:20} {}".format(entCode, entName))
        ops.print_pivotpoints('pp', entCode, "PivotPntsD")
        ops.print_pivotpoints('ppW', entCode, "PivotPntsW", False)
        ops.print_pivotpoints('ppM', entCode, "PivotPntsM", False)
        for d in datas:
            print("{:10} {}".format(d[0], entDB.data[d[1]][entIndex]))
        _plot(entCode, bPivotPoints=bPivotPoints, bVolumes=bVolumes, bRSI=bRSI, bLinRegress=bLinRegress)
        eplot.show()


def prep(bJwwRSI=True):
    """
    Process available entity raw datasets to generated useful processed data.
    This is needed before calling plot or topbottom.
    """
    opts = { 'bJwwRSI': bJwwRSI }
    _plot_prep(opts)
    procedb.infoset1_prep()


def topbottom():
    for sType in [ 'nse nifty 50', 'nse nifty 100' ]:
        input("About to look at Top and Bottom N stocks wrt {}".format(sType))
        procedb.infoset1_result(sType, resultType='result2', bPrompt=True)


