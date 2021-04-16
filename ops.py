# A collection of operations
# HanishKVC, 2021
# GPL


import numpy
import edb
import plot as eplot
import hlpr


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


def pivotpoints(dataDst, srcKeyNameTmpl="{}", date=-1, dateIndex=None, entDB=None):
    """
    Calculate the pivot points for all the entities and store at dataDst within entDB.
    By default its calculated wrt the last date in the entities db. User can change
    it to a different date by passing the date or its corresponding dateIndex.
    The dataDst array will contain [S2,S1,P,R1,R2] wrt each entity.
    NOTE: If date/dateIndex is specified, pivot points will be calculated using data
    corresponding to the given date/dateIndex.
    srcKeyNameTmpl: Is used to decide whether one is working with base data set or
        one of the derived/calculated data sets like weekly/monthly view or so.
    """
    print("DBUG:Ops:PivotPoints:", dataDst)
    entDB = _entDB(entDB)
    if not dateIndex:
        dummyDateIndex, dateIndex = entDB.daterange2index(date, date)
    highKey, lowKey, closeKey = hlpr.derive_keys(['high', 'low', 'close'], srcKeyNameTmpl)
    high = entDB.data[highKey][:,dateIndex]
    low = entDB.data[lowKey][:,dateIndex]
    close = entDB.data[closeKey][:,dateIndex]
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


def _weekly_view(dataSrcs, modes, destKeyNameTmpl="w.{}", entDB=None):
    """
    Generate data(s) which provide a weekly view of the passed source data(s).
    dataSrcs: A list of source data keys for which weekly view needs to be created.
    modes: Specifies how to generate the weekly view. It could be one of
        'M': Use the max value from among all the data for a given week.
        'm': Use the min value from among all the data for a given week.
        's': Use the value belonging to the first day for a given week.
        'e': Use the value belonging to the last day for a given week.
        'a': Use the average value of all the data for a given week.
    destKeyNameTmpl: A template which specifies how the destination dataKeys
        should be named.
    NOTE: The weeks are assumed starting from the lastday in the data set,
        as the last day of a week, irrespective of which calender day it
        may be.
    """
    entDB = _entDB(entDB)
    weekDays = hlpr.days_in('1W', entDB.bSkipWeekends)
    if type(dataSrcs) == str:
        dataSrcs = [ dataSrcs ]
    srcShape = entDB.data[dataSrcs[0]].shape
    dstShape = list(srcShape)
    dstShape[1] = int(dstShape[1]/weekDays)
    dataDsts = hlpr.derive_keys(dataSrcs, destKeyNameTmpl)
    for dDst in dataDsts:
        entDB.data[dDst] = numpy.zeros(dstShape)
    endI = entDB.nxtDateIndex
    startI = endI - weekDays
    iDst = -1
    while startI > 0:
        for dSrc, mode, dDst in zip(dataSrcs, modes, dataDsts):
            if mode == 'M':
                entDB.data[dDst][:,iDst] = numpy.max(entDB.data[dSrc][:,startI:endI], axis=1)
            elif mode == 'm':
                entDB.data[dDst][:,iDst] = numpy.min(entDB.data[dSrc][:,startI:endI], axis=1)
            elif mode == 's':
                entDB.data[dDst][:,iDst] = entDB.data[dSrc][:,startI]
            elif mode == 'e':
                entDB.data[dDst][:,iDst] = entDB.data[dSrc][:,endI-1]
            elif mode == 'a':
                entDB.data[dDst][:,iDst] = numpy.average(entDB.data[dSrc][:,startI:endI], axis=1)
        endI = startI
        startI = endI - weekDays
        iDst -= 1


