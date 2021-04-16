# A collection of operations
# HanishKVC, 2021
# GPL


import numpy
import edb
import plot as eplot


def _entDB(entDB=None):
    if not entDB:
        return edb.gEntDB
    return entDB


def print_pivotpoints(dataKey, entCodes, tag="", bPrintHdr=True, entDB=None):
    """
    Print the pivot points corresponding to all the entCodes passed.
    By default a header identifying each of the members is also printed at the top.
    """
    entDB = _entDB(entDB)
    if bPrintHdr:
        print("{}: {:16} {:32} {:7} {:7} {:7} {:7} {:7}".format(tag, "Code", "Name", "S2", "S1", "P", "R1", "R2"))
    if type(entCodes) == str:
        entCodes = [ entCodes ]
    for entCode in entCodes:
        entIndex = entDB.meta['codeD'][entCode]
        entName = entDB.meta['name'][entIndex]
        pp = entDB.data[dataKey][entIndex]
        print("{}: {:16} {:32} {:7.2f} {:7.2f} {:7.2f} {:7.2f} {:7.2f}".format(tag, entCode, entName, pp[0], pp[1], pp[2], pp[3], pp[4]))


def pivotpoints(dataDst, date=-1, dateIndex=None, entDB=None):
    """
    Calculate the pivot points for all the entities and store at dataDst within entDB.
    By default its calculated wrt the last date in the entities db. User can change
    it to a different date by passing the date or its corresponding dateIndex.
    The dataDst array will contain [S2,S1,P,R1,R2] wrt each entity.
    NOTE: If date is specified, pivot points will be calculated using data corresponding
    to the given date.
    """
    print("DBUG:Ops:PivotPoints:", dataDst)
    entDB = _entDB(entDB)
    if not dateIndex:
        dummyDateIndex, dateIndex = entDB.daterange2index(date, date)
    high = entDB.data['high'][:,dateIndex]
    low = entDB.data['low'][:,dateIndex]
    close = entDB.data['close'][:,dateIndex]
    tP = (high + low + close)/3
    tR1 = (tP*2) - low
    tS1 = (tP*2) - high
    tR2 = tP + (high - low)
    tS2 = tP - (high - low)
    tP = tP.reshape(-1,1)
    tR1 = tR1.reshape(-1,1)
    tR2 = tR2.reshape(-1,1)
    tS1 = tS1.reshape(-1,1)
    tS2 = tS2.reshape(-1,1)
    entDB.data[dataDst] = numpy.hstack((tS2,tS1,tP,tR1,tR2))


def plot_pivotpoints(dataKey, entCode, date=-1, entDB=None, axes=None):
    """
    Plot the pivot points of the given entCode on the given axes.
    The pivot points are plotted around the specified date's location.
    """
    entDB = _entDB(entDB)
    dummyDateIndex, dateIndex = entDB.daterange2index(date, date)
    axes = eplot._axes(axes)
    entIndex = entDB.meta['codeD'][entCode]
    pp = entDB.data[dataKey][entIndex]
    for p,s,c in zip(pp, ['S2', 'S1', 'P', 'R1', 'R2'], ['green','green','black','red','red']):
        axes.text(dateIndex-10, p, s)
        axes.plot([dateIndex-20, dateIndex], [p, p], color=c, alpha=0.5, linestyle='dashed')


def _weekly_view(dataSrc, dataDst="w.{}", entDB=None):
    entDB = _entDB(entDB)
    srcShape = entDB.data[dataSrc].shape
    dstShape = list(srcShape)
    dstShape[1] = int(dstShape[1]/7)
    dataDst = dataDst.format(dataSrc)
    entDB.data[dataDst] = numpy.zeros(dstShape)
    weekDays = hlpr.days_in('1W', entDB.bSkipWeekends)
    endI = entDB.nxtDateIndex
    startI = endI - weekDays
    iDst = -1
    while startI > 0:
        if mode == 'M':
            entDB.data[dataDst][:,iDst] = numpy.max(entDB.data[dataSrc][:,startI:endI], axis=1)
        elif mode == 'm':
            entDB.data[dataDst][:,iDst] = numpy.min(entDB.data[dataSrc][:,startI:endI], axis=1)
        elif mode == 's':
            entDB.data[dataDst][:,iDst] = entDB.data[dataSrc][:,startI]
        elif mode == 'e':
            entDB.data[dataDst][:,iDst] = entDB.data[dataSrc][:,endI-1]
        elif mode == 'a':
            entDB.data[dataDst][:,iDst] = numpy.average(entDB.data[dataSrc][:,startI:endI], axis=1)
        endI = startI
        startI = endI - weekDays


