# A bunch of stupid logics
# HanishKVC, 2021
# GPL


import numpy
import edb
import procedb
import hlpr
import plot


def _entDB(entDB=None):
    if entDB == None:
        return edb.gEntDB
    return entDB


def above_ndays(dataKey='close', dataIndex=-1, cmpKey='high', cmpStartDateIndex=-14, cmpEndDateIndex=-1):
    """
    Find entities whose specified attribute's value on a given date is above the
    max value found for another of its attribute, over a given time period.

    The default argument values are setup to find entities whose last close is
    higher than the high seen over last 2 weeks.

    NOTE: Remember to load such that the last date loaded is a working day and not a holiday/weekend.
    Or else control dataIndex and cmpEndDateIndex such that they dont fall on a holiday/weekend.
    """
    entDB = _entDB()
    tCmp = numpy.max(entDB.data[cmpKey][:,cmpStartDateIndex:cmpEndDateIndex], axis=1)
    tAbove = tCmp < entDB.data[dataKey][:,dataIndex]
    tNames = entDB.meta['name'][tAbove]
    tCodes = entDB.meta['codeL'][tAbove]
    tEntities = list(zip(tCodes, tNames))
    return tEntities


def below_ndays(dataKey='close', dataIndex=-1, cmpKey='low', cmpStartDateIndex=-14, cmpEndDateIndex=-1):
    """
    Find entities whose specified attribute's value on a given date is below the
    min value found for another of its attribute, over a given time period.

    The default argument values are setup to find entities whose last close is
    lower than the low seen over last 2 weeks.

    NOTE: Remember to load such that the last date loaded is a working day and not a holiday/weekend.
    Or else control dataIndex and cmpEndDateIndex such that they dont fall on a holiday/weekend.
    """
    entDB = _entDB()
    tCmp = numpy.min(entDB.data[cmpKey][:,cmpStartDateIndex:cmpEndDateIndex], axis=1)
    tBelow = tCmp > entDB.data[dataKey][:,dataIndex]
    tNames = entDB.meta['name'][tBelow]
    tCodes = entDB.meta['codeL'][tBelow]
    tEntities = list(zip(tCodes, tNames))
    return tEntities


