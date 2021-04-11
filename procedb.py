# Process and or look at data
# HanishKVC, 2021
# GPL
#

import sys
import os
import numpy
import matplotlib.pyplot as plt
import time
import traceback
import readline
import warnings
import hlpr
import enttypes
import indexes
import entities
import edb


# 1D, 1W, 1M, 3M, 6M, 1Y, 3Y, 5Y, 10Y
gHistoricGaps = numpy.array([1, 7, 30, 92, 183, 365, 1095, 1825, 3650])
gHistoricGapsHdr = numpy.array(["1D", "7D", "1M", "3M", "6M", "1Y", "3Y", "5Y", "10Y"])


def _entDB(entDB=None):
    """
    Either use the passed entDB or else the gEntDB, which might
    have been set by the user previously.
    """
    if entDB == None:
        return edb.gEntDB
    return entDB


def update_metas(op, dataSrc, dataDst, entDB=None):
    pass


gbMAShift2End = True
gbRelDataPlusFloat = False
gfRollingRetPAMinThreshold = 4.0
def ops(opsList, startDate=-1, endDate=-1, bDebug=False, entDB=None):
    """
    Allow data from any valid data key in entDB.data to be operated on and the results to be saved
    into a destination data key in entDB.data.

    opsList is a list of operations, which specifies the key of the data source to work with,
    as well as the operation to do. It may also optionally specify the dataDst key to use to
    store the result. Each operation is specified using the format

        dataDst=opCode(dataSrc)

    The operationCode could be one of

        "srel": calculate absolute return ratio across the full date range wrt given start date.
                if the startDate contains a 0 value, then it tries to find a valid non zero value
                and then use that.
                It also stores following meta data relative to the endDate
                MetaData  = AbsoluteReturn, ReturnPerAnnum, durationInYears
                MetaLabel = AbsoluteReturn, ReturnPerAnnum, durationInYears, dataSrcBeginVal, dataSrcEndVal

        "rel[<BaseDate>]": calculate absolute return ration across the full date range wrt the
                val corresponding to the given baseDate.
                If BaseDate is not given, then startDate will be used as the baseDate.
                It also stores following meta data relative to the endDate
                MetaData  = AbsoluteReturn, ReturnPerAnnum, durationInYears
                MetaLabel = AbsoluteReturn, ReturnPerAnnum, durationInYears, dataSrcBaseDateVal, dataSrcEndVal
                    DurationInYears: the duration between endDate and baseDate in years.

        "reton[_Type]": calculate the absolute returns and or returnsPerAnnum as on endDate wrt/relative_to
                all the other dates.
                reton_absret: Calculates the absolute return
                reton_retpa: calculates the returnPerAnnum
                reton_safe: calculates absRet for days within a year range, retPA wrt other days.
                If type is not specified, it is assumed to be safe type.
                MetaData  = Ret for 1D, 5D, 1M, 3M, 6M, 1Y, 3Y, 5Y, 10Y
                MetaLabel = Ret for 1D, 5D, 1M, 3M, 6M, 1Y, 3Y, 5Y, 10Y

        "mas<DAYSInINT>": Calculate a moving average across the full date range, with a windowsize
                of DAYSInINT. THere will be partially calculated data regions at the beginning and
                end of the dateRange, which dont have sufficient data to one of the sides. Inturn
                the valid mas data is shifted to align with the end, such that each location value
                corresponds ot average of last N days wrt that location. Inturn N days at the start
                will be set to NaN, as they dont have sufficient historic data to calculate average
                of last N days.
                MetaLabel = dataSrcMetaLabel, validMAResultBeginVal, validMAResultEndVal

                NOTE: User can set gbMAShift2End to avoid the shifting to align with end.

        "roll<DAYSInINT>[_abs]": Calculate a rolling return rate across the full date range, with a
                windowsize of DAYSInINT. Again the region at the begining of the dateRange, which
                cant satisfy the windowsize to calculate the rolling return rate, will be set to
                NaN.
                If _abs is specified, it calculates absolute return.
                    If Not (i.e by default) it calculates the ReturnPerAnnum.
                DAYSInINT: The gap in days over which the return is calculated.
                MetaData  = RollRetAvg, RollRetStd, RollRetBelowMinThreshold, MaSharpeMinT
                MetaLabel = RollRetAvg, RollRetStd, RollRetBelowMinThreshold, MaSharpeMinT
                NOTE: MaSharpeMinT = (RollRetAvg-MinThreshold)/RollRetStd

        "block<BlockDaysInt>: Divide the given dataSrc content into multiple blocks, where each
                block corresponds to the BlockDays specified. Inturn for each of the block,
                calculate the following
                    Average
                    Standard Deviation
                    Quantiles
                MetaLabel = BlockAvgs, AvgBlockAvgs, AvgBlockStds

    NOTE: NaN is used, because plot will ignore those data points and keep the corresponding
    verticals blank.

    NOTE: If no Destination data key is specified, then it is constructed using the template

        '<OP>(<DataSrc>[<startDate>:<endDate>])'

    TODO: Currently dont change startDate and endDate from their default, because many operations
    dont account for them being different from the default.
    """
    entDB = _entDB(entDB)
    startDateIndex, endDateIndex = entDB.daterange2index(startDate, endDate)
    if type(opsList) == str:
        opsList = [ opsList ]
    for curOp in opsList:
        curOpFull = curOp
        if '=' in curOp:
            dataDst, curOp = curOp.split('=')
        else:
            dataDst = ''
        op, dataSrc = curOp.split('(', 1)
        dataSrc = dataSrc[:-1]
        if dataDst == '':
            dataDst = "{}({}[{}:{}])".format(op, dataSrc, startDate, endDate)
        print("DBUG:ops:op[{}]:dst[{}]".format(curOpFull, dataDst))
        #dataLen = endDateIndex - startDateIndex + 1
        tResult = entDB.data[dataSrc].copy()
        dataSrcMetaData, dataSrcMetaLabel = hlpr.data_metakeys(dataSrc)
        dataDstMetaData, dataDstMetaLabel = hlpr.data_metakeys(dataDst)
        entDB.data[dataDstMetaLabel] = []
        #### Op specific things to do before getting into individual records
        if op == 'srel':
            entDB.data[dataDstMetaData] = numpy.zeros([entDB.nxtEntIndex,3])
        elif op.startswith("rel"):
            entDB.data[dataDstMetaData] = numpy.zeros([entDB.nxtEntIndex,3])
        elif op.startswith("roll"):
            # RollWindowSize number of days at beginning will not have
            # Rolling ret data, bcas there arent enough days to calculate
            # rolling ret while satisfying the RollingRetWIndowSize requested.
            rollDays = int(op[4:].split('_')[0])
            entDB.data[dataDstMetaData] = numpy.zeros([entDB.nxtEntIndex, 4])
        elif op.startswith("block"):
            blockDays = int(op[5:])
            blockTotalDays = endDateIndex - startDateIndex + 1
            blockCnt = int(blockTotalDays/blockDays)
            dataDstAvgs = "{}Avgs".format(dataDst)
            dataDstStds = "{}Stds".format(dataDst)
            dataDstQntls = "{}Qntls".format(dataDst)
            entDB.data[dataDstAvgs] = numpy.zeros([entDB.nxtEntIndex,blockCnt])
            entDB.data[dataDstStds] = numpy.zeros([entDB.nxtEntIndex,blockCnt])
            entDB.data[dataDstQntls] = numpy.zeros([entDB.nxtEntIndex,blockCnt,5])
            tResult = []
        elif op.startswith("reton"):
            if '_' in op:
                retonT, retonType = op.split('_')
            else:
                retonT = op
                retonType = 'safe'
            if retonT == "reton":
                retonDateIndex = endDateIndex
            else:
                retonDate = int(retonT[5:])
                retonDateIndex = entDB.datesD[retonDate]
            entDB.data[dataDstMetaData] = numpy.ones([entDB.nxtEntIndex,gHistoricGaps.shape[0]])*numpy.nan
            validHistoric = gHistoricGaps[gHistoricGaps < (retonDateIndex+1)]
            histDays = abs(numpy.arange(endDateIndex+1)-retonDateIndex)
        update_metas(op, dataSrc, dataDst)
        #### Handle each individual record as specified by the op
        for r in range(entDB.nxtEntIndex):
            try:
                if op == "srel":
                    #breakpoint()
                    iStart = -1
                    dStart = 0
                    nonZeros = numpy.nonzero(entDB.data[dataSrc][r, startDateIndex:endDateIndex+1])[0]
                    if (len(nonZeros) > 0):
                        iStart = nonZeros[0] + startDateIndex
                        dStart = entDB.data[dataSrc][r, iStart]
                    dEnd = entDB.data[dataSrc][r, endDateIndex]
                    if dStart != 0:
                        if gbRelDataPlusFloat:
                            tResult[r,:] = (entDB.data[dataSrc][r,:]/dStart)
                        else:
                            tResult[r,:] = ((entDB.data[dataSrc][r,:]/dStart)-1)*100
                        tResult[r,:iStart] = numpy.nan
                        dAbsRet = tResult[r, -1]
                        durationInYears = ((endDateIndex-startDateIndex+1)-iStart)/365
                        dRetPA = (((dEnd/dStart)**(1/durationInYears))-1)*100
                        label = "{:7.2f}% {:7.2f}%pa {:4.1f}Yrs : {:9.4f} - {:9.4f}".format(dAbsRet, dRetPA, durationInYears, dStart, dEnd)
                        entDB.data[dataDstMetaLabel].append(label)
                        entDB.data[dataDstMetaData][r,:] = numpy.array([dAbsRet, dRetPA, durationInYears])
                    else:
                        durationInYears = (endDateIndex-startDateIndex+1)/365
                        entDB.data[dataDstMetaLabel].append("")
                        entDB.data[dataDstMetaData][r,:] = numpy.array([0.0, 0.0, durationInYears])
                elif op.startswith("rel"):
                    baseDate = op[3:]
                    if baseDate != '':
                        baseDate = int(baseDate)
                        baseDateIndex = entDB.datesD[baseDate]
                    else:
                        baseDateIndex = startDateIndex
                    baseData = entDB.data[dataSrc][r, baseDateIndex]
                    dEnd = entDB.data[dataSrc][r, endDateIndex]
                    tResult[r,:] = (((entDB.data[dataSrc][r,:])/baseData)-1)*100
                    dAbsRet = tResult[r, -1]
                    durationInYears = (endDateIndex-baseDateIndex+1)/365
                    dRetPA = ((((dAbsRet/100)+1)**(1/durationInYears))-1)*100
                    label = "{:6.2f}% {:6.2f}%pa {:4.1f}Yrs : {:8.4f} - {:8.4f}".format(dAbsRet, dRetPA, durationInYears, baseData, dEnd)
                    entDB.data[dataDstMetaLabel].append(label)
                    entDB.data[dataDstMetaData][r,:] = numpy.array([dAbsRet, dRetPA, durationInYears])
                elif op.startswith("reton"):
                    retonData = entDB.data[dataSrc][r, retonDateIndex]
                    tROAbs = ((retonData/entDB.data[dataSrc][r,:])-1)*100
                    tRORPA = (((retonData/entDB.data[dataSrc][r,:])**(365/histDays))-1)*100
                    if retonType == 'absret':
                        tResult[r,:] = tROAbs
                    elif retonType == 'retpa':
                        tResult[r,:] = tRORPA
                    else:
                        if len(tROAbs) > 365:
                            tResult[r,-365:] = tROAbs[-365:]
                            tResult[r,:-365] = tRORPA[:-365]
                        else:
                            tResult[r,:] = tROAbs
                    entDB.data[dataDstMetaData][r,:validHistoric.shape[0]] = tResult[r,-(validHistoric+1)]
                    entDB.data[dataDstMetaLabel].append(hlpr.array_str(entDB.data[dataDstMetaData][r], width=7))
                elif op.startswith("mas"):
                    days = int(op[3:])
                    inv = int(days/2)
                    if gbMAShift2End:
                        tMARes = numpy.convolve(entDB.data[dataSrc][r,:], numpy.ones(days)/days, 'same')
                        tResult[r,days:] = tMARes[inv:-inv]
                        tResult[r,:days] = numpy.nan
                    else:
                        tResult[r,:] = numpy.convolve(entDB.data[dataSrc][r,:], numpy.ones(days)/days, 'same')
                        tResult[r,:inv] = numpy.nan
                        tResult[r,entDB.nxtDateIndex-inv:] = numpy.nan
                    if True:
                        try:
                            dataSrcLabel = entDB.data[dataSrcMetaLabel][r]
                        except:
                            if bDebug:
                                print("WARN:ProcDataEx:{}:No dataSrcMetaLabel".format(op))
                            dataSrcLabel = ""
                        tArray = tResult[r,:]
                        tFinite = tArray[numpy.isfinite(tArray)]
                        tNonZero = numpy.nonzero(tFinite)[0]
                        if len(tNonZero) >= 2:
                            tStart,tEnd = tFinite[tNonZero[0]],tFinite[tNonZero[-1]]
                            label = "{:8.4f} - {:8.4f}".format(tStart, tEnd)
                        else:
                            label = ""
                        label = "{} : {}".format(dataSrcLabel, label)
                        entDB.data[dataDstMetaLabel].append(label)
                elif op.startswith("roll"):
                    durationForPA = rollDays/365
                    if '_' in op:
                        opType = op.split('_')[1]
                        if opType == 'abs':
                            durationForPA = 1
                    if gbRelDataPlusFloat:
                        tResult[r,rollDays:] = (entDB.data[dataSrc][r,rollDays:]/entDB.data[dataSrc][r,:-rollDays])**(1/durationForPA)
                    else:
                        tResult[r,rollDays:] = (((entDB.data[dataSrc][r,rollDays:]/entDB.data[dataSrc][r,:-rollDays])**(1/durationForPA))-1)*100
                    tResult[r,:rollDays] = numpy.nan
                    # Additional meta data
                    trValidResult = tResult[r][numpy.isfinite(tResult[r])]
                    trLenValidResult = len(trValidResult)
                    if trLenValidResult > 0:
                        trValidBelowMinThreshold = (trValidResult < gfRollingRetPAMinThreshold)
                        trBelowMinThreshold = (numpy.count_nonzero(trValidBelowMinThreshold)/trLenValidResult)*100
                        trBelowMinThresholdLabel = "[{:5.2f}%<]".format(trBelowMinThreshold)
                        trAvg = numpy.mean(trValidResult)
                        trStd = numpy.std(trValidResult)
                        trMaSharpeMinT = (trAvg-gfRollingRetPAMinThreshold)/trStd
                    else:
                        trBelowMinThreshold = numpy.nan
                        trBelowMinThresholdLabel = "[--NA--<]"
                        trAvg = numpy.nan
                        trStd = numpy.nan
                        trMaSharpeMinT = numpy.nan
                    entDB.data[dataDstMetaData][r] = [trAvg, trStd, trBelowMinThreshold, trMaSharpeMinT]
                    label = "{:7.2f} {:7.2f} {} {:7.2f}".format(trAvg, trStd, trBelowMinThresholdLabel, trMaSharpeMinT)
                    entDB.data[dataDstMetaLabel].append(label)
                elif op.startswith("block"):
                    # Calc the Avgs
                    iEnd = endDateIndex+1
                    lAvgs = []
                    lStds = []
                    for i in range(blockCnt):
                        iStart = iEnd-blockDays
                        tBlockData = entDB.data[dataSrc][r,iStart:iEnd]
                        tBlockData = tBlockData[numpy.isfinite(tBlockData)]
                        lAvgs.insert(0, numpy.mean(tBlockData))
                        lStds.insert(0, numpy.std(tBlockData))
                        iEnd = iStart
                        if len(tBlockData) == 0:
                            tBlockData = [0]
                        entDB.data[dataDstQntls][r, blockCnt-1-i] = numpy.quantile(tBlockData,[0,0.25,0.5,0.75,1])
                    avgAvgs = numpy.nanmean(lAvgs)
                    avgStds = numpy.nanmean(lStds)
                    entDB.data[dataDstAvgs][r,:] = lAvgs
                    entDB.data[dataDstStds][r,:] = lStds
                    #entDB.data[dataDstMetaData][r] = [avgAvgs, avgStds]
                    label = "<{} {:5.2f} {:5.2f}>".format(hlpr.array_str(lAvgs,4,1), avgAvgs, avgStds)
                    entDB.data[dataDstMetaLabel].append(label)
            except:
                traceback.print_exc()
                print("DBUG:ProcDataEx:Exception skipping entity at ",r)
        if len(tResult) > 0:
            entDB.data[dataDst] = tResult


def _mabeta(dataSrc, refCode, entCodes, entDB=None):
    """
    Get the slope of the entCodes wrt refCode.
    When the passed dataSrc is rollingRet, this is also called MaBeta.
    """
    entDB = _entDB(entDB)
    refIndex = entDB.meta['codeD'][refCode]
    refValid = entDB.data[dataSrc][refIndex][numpy.isfinite(entDB.data[dataSrc][refIndex])]
    refAvg = numpy.mean(refValid)
    maBeta = []
    for entCode in entCodes:
        entIndex = entDB.meta['codeD'][entCode]
        entValid = entDB.data[dataSrc][entIndex][numpy.isfinite(entDB.data[dataSrc][entIndex])]
        entAvg = numpy.mean(entValid)
        entMaBeta = numpy.sum((entValid-entAvg)*(refValid-refAvg))/numpy.sum((refValid-refAvg)**2)
        maBeta.append(entMaBeta)
    return maBeta


def mabeta(dataSrc, refCode, entCodes, entDB=None):
    '''
    Calculate a measure of how similar or dissimilar is the change/movement in
    the value of given entities in entCodes list wrt changes in value of the
    given refCode entity.
    Value of 1 means - Value of both entities change/move in same way.
    Value below 1 - Both entities move in similar ways, but then
        the given entity moves less compared to ref entity.
    Value above 1 - Both entities move in similar ways, but then
        the given entity moves more compared to the ref entity.
    Value lower than 0 - Both entities move in dissimilar/oppositive manner.
    '''
    entDB = _entDB(entDB)
    ops('roll1Abs=roll1_abs({})'.format(dataSrc))
    return _mabeta('roll1Abs', refCode, entCodes)


def _forceval_entities(data, entCodes, forcedValue, entSelectType='normal', entDB=None):
    """
    Set the specified locations in the data, to the given forcedValue.

    The locations is specified using a combination of entCodes and entSelectType.
        'normal': for locations corresponding to the specified entCodes,
        'invert': for locations not specified in entCodes.
    """
    entDB = _entDB(entDB)
    if entCodes == None:
        return data
    indexes = [entDB.meta['codeD'][code] for code in entCodes]
    if entSelectType == 'normal':
        mask = numpy.zeros(data.size, dtype=bool)
        mask[indexes] = True
    else:
        mask = numpy.ones(data.size, dtype=bool)
        mask[indexes] = False
    data[mask] = forcedValue
    return data


gAnalSimpleBasePrintFormats = [ "{:<{width}}", "{:{width}}", {'num':"{:{width}.2f}",'str':"{:{width}}"} ]
gAnalSimpleBasePrintWidths =  [ 16, 40, 8 ]
def anal_simple(dataSrc, analType='normal', order="top", theDate=None, theIndex=None, numEntities=10, entCodes=None,
                        minEntityLifeDataInYears=1.5, bCurrentEntitiesOnly=True, bDebug=False, iClipNameWidth=64, entDB=None):
    """
    Look at specified data in dataSrc, and find top/bottom N entities.
    The rows of the dataSrc represent the entities and
    the cols represent the data associated with the individual entities.
    One can specify the data one wants to look at by using
    * [For Normal] the date one is interested in or
    * [For Normal] the index of the data
    * [For Others] op specific attribute/meta data that
      one is interested in.

    order: could be either 'top' or 'bottom'

    analType: could be one of 'normal', 'srel_absret', 'srel_retpa',
        'roll_avg', 'block_ranked'

        normal: Look at data corresponding to identified date or index,
        in the given dataSrc, to decide on entities to select.

        srel_absret: Look at the Absolute Returns data associated
        with the given dataSrc (which should be generated using
        srel operation), to decide on entities.

        srel_retpa: Look at the Returns PerAnnum data associated
        with the given dataSrc (which should be generated using
        srel operation), to decide on entities.

        roll_avg: look at Average ReturnsPerAnnum, calculated using
        rolling return (dataSrc specified should be generated using
        roll operation), to decide on entities ranking.

        block_ranked: look at the Avgs calculated by block op,
        for each sub date periods(blocks), rank them and average
        over all the sub date periods to calculate the rank for
        full date Range. Use this final rank to decide on entities
        ranking. (dataSrc should have been generated using block
        operation).

    theDate and theIndex: [Used by normal analType]
        If both are None, then the logic will try to find a date
        which contains atleast some valid data, starting from the
        lastDate and moving towards startDate wrt given dataSrc.

        NOTE: ValidData: Any Non Zero, Non NaN, Non Inf data

        If theDate is a date, then values in dataSrc corresponding
        to this date will be used for sorting.

        If theDate is -1, then the lastDate wrt the currently
        loaded dataset, is used as the date from which values
        should be used to identify the entities.

        If theIndex is set and theDate is None, then the values
        in dataSrc corresponding to given index, is used for
        sorting.

        NOTE: date follows YYYYMMDD format.

    entCodes: One can restrict the logic to look at data belonging to
        the specified list of entities. If None, then all entities
        in the loaded dataset will be considered, for ranking.

    minEntityLifeDataInYears: This ranking logic will ignore entities
        who have been in existance for less than the specified duration
        of years, AND OR if we have data for only less than the specified
        duration of years for the entity.

        NOTE: The default is 1.5 years, If you have loaded less than that
        amount of data, then remember to set this to a smaller value,
        if required.

        NOTE: It expects the info about duration for which data is
        available for each entity, to be available under 'srelMetaData'
        key. If this is not the case, it will trigger a generic 'srel'
        operation through ops to generate the same.

            It also means that the check is done wrt overall amount
            of data available for a given entity in the loaded dataset,
            While a dataSrc key which corresponds to a view of partial
            data from the loaded dataset, which is less than specified
            minEntityLifeDataInYears, can still be done.

    bCurrentEntitiesOnly: Will drop entities which have not been seen
        in the last 1 week, wrt the dateRange currently loaded.

    iClipNameWidth:
        If None, the program prints full name, with space alloted by
        default for 64 chars.
        Else the program limits the Name to specified width.

    entDB: The data set from which to pick the data to work with.
        If specified, it will be used. Else what ever default
        data set was previously initialised by the program
        will be used.
    """
    entDB = _entDB(entDB)
    theAnal = "{}_{}".format(analType, order)
    print("DBUG:AnalSimple:{}:{}".format(theAnal, dataSrc))
    printFmts = gAnalSimpleBasePrintFormats.copy()
    printWidths = gAnalSimpleBasePrintWidths.copy()
    printHdr = [ "Code", "Name" ]
    if order == 'top':
        iSkip = -numpy.inf
    else:
        iSkip = numpy.inf
    theSaneArray = None
    if analType == 'normal':
        printHdr.extend(['Value'])
        if (type(theDate) == type(None)) and (type(theIndex) == type(None)):
            for i in range(-1, -entDB.nxtDateIndex, -1):
                if bDebug:
                    print("DBUG:AnalSimple:{}:findDateIndex:{}".format(theAnal, i))
                theSaneArray = entDB.data[dataSrc][:,i].copy()
                theSaneArray[numpy.isinf(theSaneArray)] = iSkip
                theSaneArray[numpy.isnan(theSaneArray)] = iSkip
                if not numpy.all(numpy.isinf(theSaneArray) | numpy.isnan(theSaneArray)):
                    dateIndex = entDB.nxtDateIndex+i
                    print("INFO:AnalSimple:{}:DateIndex:{}".format(theAnal, dateIndex))
                    break
        else:
            if (type(theIndex) == type(None)) and (type(theDate) != type(None)):
                startDateIndex, theIndex = entDB.daterange2index(theDate, theDate)
            print("INFO:AnalSimple:{}:theIndex:{}".format(theAnal, theIndex))
            theSaneArray = entDB.data[dataSrc][:,theIndex].copy()
            theSaneArray[numpy.isinf(theSaneArray)] = iSkip
            theSaneArray[numpy.isnan(theSaneArray)] = iSkip
    elif analType.startswith("srel"):
        dataSrcMetaData, dataSrcMetaLabel = hlpr.data_metakeys(dataSrc)
        if analType == 'srel_absret':
            printHdr.extend(['AbsRet'])
            theSaneArray = entDB.data[dataSrcMetaData][:,0].copy()
        elif analType == 'srel_retpa':
            printHdr.extend(['RetPA'])
            theSaneArray = entDB.data[dataSrcMetaData][:,1].copy()
        else:
            input("ERRR:AnalSimple:{}:dataSrc[{}]: unknown srel anal subType, returning...".format(theAnal, dataSrc))
            return None
    elif analType.startswith("roll"):
        dataSrcMetaData, dataSrcMetaLabel = hlpr.data_metakeys(dataSrc)
        if analType == 'roll_avg':
            printHdr.extend(['Avg', 'Std', '<minT', 'MaSha'])
            theSaneArray = entDB.data[dataSrcMetaData][:,0].copy()
            theSaneArray[numpy.isinf(theSaneArray)] = iSkip
            theSaneArray[numpy.isnan(theSaneArray)] = iSkip
            printFmts.extend([{'num':"{:{width}.2f}",'str':'{:{width}}'}, {'num':"{:{width}.2f}",'str':'{:{width}}'}, {'num':"{:{width}.2f}",'str':'{:{width}}'}])
            printWidths.extend([7, 7, 7])
    elif analType == "block_ranked":
        printHdr.extend(['AvgRank', 'blockRanks', 'blockAvgs'])
        metaDataAvgs = "{}Avgs".format(dataSrc)
        tNumEnts, tNumBlocks = entDB.data[metaDataAvgs].shape
        theRankArray = numpy.zeros([tNumEnts, tNumBlocks+1])
        iValidBlockAtBegin = 0
        bValidBlockFound = False
        for b in range(tNumBlocks):
            tArray = entDB.data[metaDataAvgs][:,b]
            tValidArray = tArray[numpy.isfinite(tArray)]
            tSaneArray = hlpr.sane_array(tArray, iSkip)
            if len(tValidArray) != 0:
                tQuants = numpy.quantile(tValidArray, [0, 0.2, 0.4, 0.6, 0.8, 1])
                theRankArray[:,b] = numpy.digitize(tSaneArray, tQuants, True)
                bValidBlockFound = True
            else:
                if not bValidBlockFound:
                    iValidBlockAtBegin = b+1
                theRankArray[:,b] = numpy.zeros(len(theRankArray[:,b]))
        theRankArray[:,tNumBlocks] = numpy.average(theRankArray[:,iValidBlockAtBegin:tNumBlocks], axis=1)
        theRankArray = theRankArray[:, iValidBlockAtBegin:tNumBlocks+1]
        tNumBlocks = tNumBlocks - iValidBlockAtBegin
        theSaneArray = theRankArray[:,tNumBlocks]
    else:
        input("ERRR:AnalSimple:{}:dataSrc[{}]: unknown analType, returning...".format(theAnal, dataSrc))
        return None
    if type(theSaneArray) == type(None):
        print("WARN:DBUG:AnalSimple:{}:{}: No SaneArray????".format(theAnal, dataSrc))
        breakpoint()
    theSaneArray = _forceval_entities(theSaneArray, entCodes, iSkip, 'invert', entDB=entDB)
    if minEntityLifeDataInYears > 0:
        dataYearsAvailable = entDB.nxtDateIndex/365
        if (dataYearsAvailable < minEntityLifeDataInYears):
            print("WARN:AnalSimple:{}: dataYearsAvailable[{}] < minENtityLifeDataInYears[{}]".format(theAnal, dataYearsAvailable, minEntityLifeDataInYears))
        srelMetaData, srelMetaLabel = hlpr.data_metakeys('srel')
        theSRelMetaData = entDB.data.get(srelMetaData, None)
        if type(theSRelMetaData) == type(None):
            ops('srel=srel(data)')
        if bDebug:
            tNames = numpy.array(entDB.meta['name'])
            tDroppedNames = tNames[entDB.data[srelMetaData][:,2] < minEntityLifeDataInYears]
            print("INFO:AnalSimple:{}:{}:Dropping if baby Entity".format(theAnal, dataSrc), tDroppedNames)
        theSaneArray[entDB.data[srelMetaData][:,2] < minEntityLifeDataInYears] = iSkip
    if bCurrentEntitiesOnly:
        oldEntities = numpy.nonzero(entDB.meta['lastSeen'] < (entDB.dates[entDB.nxtDateIndex-1]-7))[0]
        if bDebug:
            #aNames = numpy.array(entDB.meta['name'])
            #print(aNames[oldEntities])
            for index in oldEntities:
                print("DBUG:AnalSimple:{}:IgnoringOldEntity:{}, {}".format(theAnal, entDB.meta['name'][index], entDB.meta['lastSeen'][index]))
        theSaneArray[oldEntities] = iSkip
    #theRows=numpy.argsort(theSaneArray)[-numEntities:]
    theRows=numpy.argsort(theSaneArray)
    rowsLen = len(theRows)
    if numEntities > rowsLen:
        print("WARN:AnalSimple:{}:RankContenders[{}] < numEntities[{}] requested, adjusting".format(theAnal, rowsLen, numEntities))
        numEntities = rowsLen
    if order == 'top':
        lStart = -1
        lStop = -(numEntities+1)
        lDelta = -1
    elif order == 'bottom':
        lStart = 0
        lStop = numEntities
        lDelta = 1
    theSelected = []
    print("INFO:AnalSimple:{}:{}".format(theAnal, dataSrc))
    hlpr.printl(printFmts, printHdr, " ", "\t", "", printWidths)
    for i in range(lStart,lStop,lDelta):
        index = theRows[i]
        if (theSaneArray[index] == iSkip) or ((analType == 'block_ranked') and (theSaneArray[index] == 0)):
            print("    WARN:AnalSimple:{}:No more valid elements".format(theAnal))
            break
        curEntry = [entDB.meta['codeL'][index], entDB.meta['name'][index], theSaneArray[index]]
        if analType == "roll_avg":
            curEntry.extend(entDB.data[dataSrcMetaData][index,1:])
        theSelected.append(curEntry)
        if iClipNameWidth == None:
            curEntry[1] = "{:64}".format(curEntry[1])
        else:
            curEntry[1] = "{:{width}}".format(curEntry[1][:iClipNameWidth], width=iClipNameWidth)
        curEntry[2] = numpy.round(curEntry[2],2)
        if analType == "roll_avg":
            curEntry[3:] = numpy.round(curEntry[3:],2)
        elif analType == "block_ranked":
            theSelected[-1] = theSelected[-1] + [ theRankArray[index] ]
            extra = "{}:{}".format(hlpr.array_str(theRankArray[index],4,"A0L1"), hlpr.array_str(entDB.data[metaDataAvgs][index, iValidBlockAtBegin:],6,2))
            curEntry.append(extra)
        #print("    {} {}".format(extra, curEntry))
        hlpr.printl(printFmts, curEntry, " ", "\t", "", printWidths)
    return theSelected


def infoset1_prep(entDB=None):
    """
    Run a common set of operations, which can be used to get some amount of
    potentially useful info, on the loaded data,
    """
    entDB = _entDB(entDB)
    warnings.filterwarnings('ignore')
    ops(['srel=srel(data)', 'mas50Srel=mas50(srel)'], entDB=entDB)
    ops(['roabs=reton_absret(data)', 'rosaf=reton(data)'], entDB=entDB)
    ops(['roll1095=roll1095(data)', 'mas50Roll1095=mas50(roll1095)'], entDB=entDB)
    ops(['roll1825=roll1825(data)', 'mas50Roll1825=mas50(roll1825)'], entDB=entDB)
    blockDays = int(entDB.nxtDateIndex/5)
    ops(['blockNRoll1095=block{}(roll1095)'.format(blockDays)], entDB=entDB)
    warnings.filterwarnings('default')


def infoset1_result1_entcodes(entCodes, bPrompt=False, numEntities=-1, entDB=None):
    """
    Print data generated by processing the loaded data, wrt the specified entities,
    to the user.

    NOTE: As 2nd part of the result dump, where it prints data across all specified
    entities, wrt each data aspect that was processed during prep, it tries to sort
    them based on the average meta data info from roll1095 (3Y). And entities which
    are less than 3 years will get collated to the end of the sorted list, based on
    the last RetPA from srel operation. If there are entities which get dropped by
    both the sort operations, then they will get collated to the end.

    numEntities if greater than 0, will limit the number of entities that are shown
    in the comparitive print wrt each processed data type.
    """
    entDB = _entDB(entDB)
    dataSrcs = [
            ['srel', 'srelMetaLabel'],
            ['absRet', 'roabsMetaLabel'],
            ['retOn', 'rosafMetaLabel'],
            ['roll1095', 'roll1095MetaLabel'],
            ['roll1825', 'roll1825MetaLabel'],
            ]
    for entCode in entCodes:
        entIndex = entDB.meta['codeD'][entCode]
        print("Name:", entDB.meta['name'][entIndex])
        for dataSrc in dataSrcs:
            print("\t{:16}: {}".format(dataSrc[0], entDB.data[dataSrc[1]][entIndex]))

    dateDuration = entDB.nxtDateIndex/365
    if dateDuration > 1.5:
        dateDuration = 1.5
    print("INFO:dateDuration:", dateDuration)
    analR1095 = anal_simple('roll1095', 'roll_avg', 'top', entCodes=entCodes, numEntities=len(entCodes), minEntityLifeDataInYears=dateDuration, entDB=entDB)
    analR1095EntCodes = [ x[0] for x in analR1095 ]
    s1 = set(entCodes)
    s2 = set(analR1095EntCodes)
    otherEntCodes = s1-s2
    analSRelRPA = anal_simple('srel', 'srel_retpa', 'top', entCodes=otherEntCodes, numEntities=len(otherEntCodes), minEntityLifeDataInYears=dateDuration, entDB=entDB)
    analSRelRPAEntCodes = [ x[0] for x in analSRelRPA ]
    s3 = set(analSRelRPAEntCodes)
    entCodes = analR1095EntCodes + analSRelRPAEntCodes + list(s1-(s2.union(s3)))

    anal_simple('blockNRoll1095', 'block_ranked', 'top', entCodes=entCodes, numEntities=len(entCodes), minEntityLifeDataInYears=dateDuration, entDB=entDB)

    totalEntities = len(entCodes)
    if numEntities > totalEntities:
        numEntities = totalEntities
    printFmt = "\t{:<20}:{:24}:"
    for dataSrc in dataSrcs:
        print("DataSrc:{}: >>showing {} of {} entities<<".format(dataSrc, numEntities, totalEntities))
        if dataSrc[0] in [ 'absRet', 'retOn' ]:
            print((printFmt+" {}").format("code", "name",hlpr.array_str(gHistoricGapsHdr, width=7)))
        elif dataSrc[0] == 'srel':
            print((printFmt+"   AbsRet     RetPA   DurYrs : startVal  -  endVal").format("code", "name"))
        elif dataSrc[0].startswith('roll'):
            print((printFmt+"     Avg     Std [ <{}% ]   MaShaMT").format("code", "name", gfRollingRetPAMinThreshold))
            x = []
            y = []
            c = []
            dataSrcMetaData = dataSrc[1].replace('Label','Data')
        entCount = 0
        for entCode in entCodes:
            entIndex = entDB.meta['codeD'][entCode]
            entName = entDB.meta['name'][entIndex][:24]
            if dataSrc[0].startswith('roll'):
                x.append(entDB.data[dataSrcMetaData][entIndex,0])
                y.append(entDB.data[dataSrcMetaData][entIndex,1])
                c.append(entCode)
            print((printFmt+" {}").format(entCode, entName, entDB.data[dataSrc[1]][entIndex]))
            entCount += 1
            if (numEntities > 0) and (entCount > numEntities):
                break
        if dataSrc[0].startswith('roll'):
            plt.scatter(x,y)
            for i,txt in enumerate(c):
                plt.annotate(txt,(x[i],y[i]))
            plt.xlabel('RollRet Avg')
            plt.ylabel('RollRet StD')
            plt.show()
        if bPrompt:
            input("Press any key to continue...")


def infoset1_result2_entcodes(entCodes=None, bPrompt=True, numEntities=20, entDB=None):
    """
    Identify the top and bottom N entities based on performance over
    last day, week, month, 3months and inturn print some processed
    data about them.

    entCodes: If given, compare within them, else compare across all entities.
    """
    entDB = _entDB(entDB)
    lTop = set()
    lBot = set()
    for i in [0, 1, 2, 3]:
        print("INFO:InfoSet1Result2: Top {} entities wrt last {}".format(numEntities, gHistoricGapsHdr[i]))
        t = anal_simple('roabsMetaData', 'normal', 'top', theIndex=i, entCodes=entCodes, numEntities=numEntities, entDB=entDB)
        for tEnt in [x[0] for x in t]:
            lTop.add(tEnt)
        print("INFO:InfoSet1Result2: Bottom {} entities wrt last {}".format(numEntities, gHistoricGapsHdr[i]))
        b = anal_simple('roabsMetaData', 'normal', 'bottom', theIndex=i, entCodes=entCodes, numEntities=numEntities, entDB=entDB)
        for tEnt in [x[0] for x in b]:
            lBot.add(tEnt)
        if bPrompt:
            input('INFO:Press any key to continue...')
    lAll = list(lTop.union(lBot))
    infoset1_result1_entcodes(lAll, bPrompt, len(lAll), entDB=entDB)


def infoset1_result(entTypeTmpls=[], entNameTmpls=[], bPrompt=False, numEntities=20, resultType='result1', entDB=None):
    """
    Print data generated by processing the loaded data, wrt the specified entities,
    to the user, so that they can compare data across the entities.

    If no argument is passed, then processed data is printed for all entities.

    If only a single argument is passed, then its treated as a match template
    for entity types. Processed data for all members of the matching entTypes
    will be printed.

    If both arguments are given, then processed data for all members of matching
    entTypes, which inturn match the entNameTmpls, will be printed.

    If only entNameTmpls is passed, then all entities in the loaded dataset,
    which match any of the given templates, will have processed data related
    to them printed.

    NOTE: For the comparitive prints, where for each kind of processed data type,
    info from all selected entities is dumped together, the logic tries to sort
    based on average of 3 year rolling ret. For entities younger than 3 years,
    it tries to sort based on the latest returnPerAnnum as on the last date
    in the loaded dateset (from start of scheme), and inturn bundle this to
    the earlier rollret sorted list. And any remaining after this.

    numEntities if greater than 0, will limit the number of entities that are shown
    in the comparitive print wrt each processed data type.
    """
    entDB = _entDB(entDB)
    entCodes = []
    if (len(entTypeTmpls) == 0) and (len(entNameTmpls) == 0):
        entCodes = list(entDB.meta['codeD'].keys())
    elif (len(entTypeTmpls) == 0) and (len(entNameTmpls) > 0):
        entCodes = enttypes._members(entDB, [ '-RE-.*'], entNameTmpls)
    else:
        entCodes = enttypes._members(entDB, entTypeTmpls, entNameTmpls)
    if resultType.lower() == 'result1':
        infoset1_result1_entcodes(entCodes, bPrompt, numEntities, entDB=entDB)
    else:
        infoset1_result2_entcodes(entCodes, bPrompt, numEntities, entDB=entDB)

