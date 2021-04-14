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


def above_2wh():
    entDB = _entDB()
    tHigh = numpy.max(entDB.data['high'][:,-14:-1], axis=1)
    tAbove = tHigh < entDB.data['close'][:,-1]
    print(entDB.meta['name'][tAbove])


