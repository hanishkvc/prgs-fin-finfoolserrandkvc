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
    entDB = _entDB()
    tCmp = numpy.max(entDB.data[cmpKey][:,cmpStartDateIndex:cmpEndDateIndex], axis=1)
    tAbove = tCmp < entDB.data[dataKey][:,dataIndex]
    print(entDB.meta['name'][tAbove])


