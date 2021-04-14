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
    """
    entDB = _entDB()
    tCmp = numpy.max(entDB.data[cmpKey][:,cmpStartDateIndex:cmpEndDateIndex], axis=1)
    tAbove = tCmp < entDB.data[dataKey][:,dataIndex]
    tNames = entDB.meta['name'][tAbove]
    tCodes = entDB.meta['codeL'][tAbove]
    tEntities = list(zip(tCodes, tNames))
    return tEntities


