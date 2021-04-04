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


gEntDB = None
def _entDB(entDB=None):
    """
    Either use the passed entDB or else the gEntDB, which might
    have been set by the user previously.
    """
    if entDB == None:
        return gEntDB
    return entDB


def data_metakeys(dataDst):
    """
    Returns the Meta Keys related to given dataDst key.

    MetaData: This key points to raw meta data wrt each MF, which can be
        processed further for comparing with other MFs etc.
    MetaLabel: This key points to processed label/summary info wrt each MF.
        This is useful for labeling plots etc.
    """
    dataKey="{}MetaData".format(dataDst)
    labelKey="{}MetaLabel".format(dataDst)
    return dataKey, labelKey


def update_metas(op, dataSrc, dataDst, entDB=None):
    pass


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

        "reton<_Type>": calculate the absolute returns or returnsPerAnnum as on endDate wrt/relative_to
                all the other dates.
                reton_absret: Calculates the absolute return
                reton_retpa: calculates the returnPerAnnum
                MetaData  = Ret for 1D, 5D, 1M, 3M, 6M, 1Y, 3Y, 5Y, 10Y
                MetaLabel = Ret for 1D, 5D, 1M, 3M, 6M, 1Y, 3Y, 5Y, 10Y

        "dma<DAYSInINT>": Calculate a moving average across the full date range, with a windowsize
                of DAYSInINT. It sets the Partial calculated data regions at the beginning and end
                of the dateRange to NaN (bcas there is not sufficient data to one of the sides,
                in these locations, so the result wont be proper, so force it to NaN).
                MetaLabel = dataSrcMetaLabel, validDmaResultBeginVal, validDmaResultEndVal

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
        dataSrcMetaData, dataSrcMetaLabel = data_metakeys(dataSrc)
        dataDstMetaData, dataDstMetaLabel = data_metakeys(dataDst)
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
            retonT, retonType = op.split('_')
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
                        label = "{:6.2f}% {:6.2f}%pa {:4.1f}Yrs : {:8.4f} - {:8.4f}".format(dAbsRet, dRetPA, durationInYears, dStart, dEnd)
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
                    retonType = op.split('_')[1]
                    retonData = entDB.data[dataSrc][r, retonDateIndex]
                    if retonType == 'absret':
                        tResult[r,:] = ((retonData/entDB.data[dataSrc][r,:])-1)*100
                    else:
                        tResult[r,:] = (((retonData/entDB.data[dataSrc][r,:])**(365/histDays))-1)*100
                    entDB.data[dataDstMetaData][r,:validHistoric.shape[0]] = tResult[r,-validHistoric]
                    entDB.data[dataDstMetaLabel].append(hlpr.array_str(entDB.data[dataDstMetaData][r], width=7))
                elif op.startswith("dma"):
                    days = int(op[3:])
                    tResult[r,:] = numpy.convolve(entDB.data[dataSrc][r,:], numpy.ones(days)/days, 'same')
                    inv = int(days/2)
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
                    label = "{:5.2f} {:5.2f} {} {:5.2f}".format(trAvg, trStd, trBelowMinThresholdLabel, trMaSharpeMinT)
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


def anal_simple(dataSrc, op, opType='normal', theDate=None, theIndex=None, numEntities=10, entCodes=None,
                        minEntityLifeDataInYears=1.5, bCurrentEntitiesOnly=True, bDebug=False, iClipNameWidth=64, entDB=None):
    """
    Find the top/bottom N entities, [wrt the given date or index,]
    from the given dataSrc.

    op: could be either 'top' or 'bottom'

    opType: could be one of 'normal', 'srel_absret', 'srel_retpa',
        'roll_avg', 'block_ranked'

        normal: Look at data corresponding to the identified date,
        in the given dataSrc, to decide on entities to select.

        srel_absret: Look at the Absolute Returns data associated
        with the given dataSrc (which should be generated using
        srel ops operation), to decide on entities.

        srel_retpa: Look at the Returns PerAnnum data associated
        with the given dataSrc (which should be generated using
        srel ops operation), to decide on entities.

        roll_avg: look at Average ReturnsPerAnnum, calculated using
        rolling return (dataSrc specified should be generated using
        roll ops operation), to decide on entities ranking.

        block_ranked: look at the Avgs calculated by block op,
        for each sub date periods, rank them and average over all
        the sub date periods to calculate the rank for full date
        Range. Use this final rank to decide on entities ranking.

    theDate and theIndex:
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

    """
    entDB = _entDB(entDB)
    print("DBUG:AnalSimple:{}-{}:{}".format(dataSrc, opType, op))
    if op == 'top':
        iSkip = -numpy.inf
    else:
        iSkip = numpy.inf
    theSaneArray = None
    if opType == 'normal':
        if (type(theDate) == type(None)) and (type(theIndex) == type(None)):
            for i in range(-1, -entDB.nxtDateIndex, -1):
                if bDebug:
                    print("DBUG:AnalSimple:{}:findDateIndex:{}".format(op, i))
                theSaneArray = entDB.data[dataSrc][:,i].copy()
                theSaneArray[numpy.isinf(theSaneArray)] = iSkip
                theSaneArray[numpy.isnan(theSaneArray)] = iSkip
                if not numpy.all(numpy.isinf(theSaneArray) | numpy.isnan(theSaneArray)):
                    dateIndex = entDB.nxtDateIndex+i
                    print("INFO:AnalSimple:{}:DateIndex:{}".format(op, dateIndex))
                    break
        else:
            if (type(theIndex) == type(None)) and (type(theDate) != type(None)):
                startDateIndex, theIndex = entDB.daterange2index(theDate, theDate)
            print("INFO:AnalSimple:{}:theIndex:{}".format(op, theIndex))
            theSaneArray = entDB.data[dataSrc][:,theIndex].copy()
            theSaneArray[numpy.isinf(theSaneArray)] = iSkip
            theSaneArray[numpy.isnan(theSaneArray)] = iSkip
    elif opType.startswith("srel"):
        dataSrcMetaData, dataSrcMetaLabel = data_metakeys(dataSrc)
        if opType == 'srel_absret':
            theSaneArray = entDB.data[dataSrcMetaData][:,0].copy()
        elif opType == 'srel_retpa':
            theSaneArray = entDB.data[dataSrcMetaData][:,1].copy()
        else:
            input("ERRR:AnalSimple:dataSrc[{}]:{} unknown srel opType, returning...".format(opType))
            return None
    elif opType.startswith("roll"):
        dataSrcMetaData, dataSrcMetaLabel = data_metakeys(dataSrc)
        if opType == 'roll_avg':
            theSaneArray = entDB.data[dataSrcMetaData][:,0].copy()
            theSaneArray[numpy.isinf(theSaneArray)] = iSkip
            theSaneArray[numpy.isnan(theSaneArray)] = iSkip
    elif opType == "block_ranked":
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
        input("ERRR:AnalSimple:dataSrc[{}]:{} unknown opType, returning...".format(opType))
        return None
    if type(theSaneArray) == type(None):
        print("DBUG:AnalSimple:{}:{}:{}: No SaneArray????".format(op, dataSrc, opType))
        breakpoint()
    theSaneArray = _forceval_entities(theSaneArray, entCodes, iSkip, 'invert', entDB=entDB)
    if minEntityLifeDataInYears > 0:
        dataYearsAvailable = entDB.nxtDateIndex/365
        if (dataYearsAvailable < minEntityLifeDataInYears):
            print("WARN:AnalSimple:{}: dataYearsAvailable[{}] < minENtityLifeDataInYears[{}]".format(op, dataYearsAvailable, minEntityLifeDataInYears))
        srelMetaData, srelMetaLabel = data_metakeys('srel')
        theSRelMetaData = entDB.data.get(srelMetaData, None)
        if type(theSRelMetaData) == type(None):
            ops('srel=srel(data)')
        if bDebug:
            tNames = numpy.array(entDB.meta['name'])
            tDroppedNames = tNames[entDB.data[srelMetaData][:,2] < minEntityLifeDataInYears]
            print("INFO:AnalSimple:{}:{}:{}:Dropping if baby Entity".format(op, dataSrc, opType), tDroppedNames)
        theSaneArray[entDB.data[srelMetaData][:,2] < minEntityLifeDataInYears] = iSkip
    if bCurrentEntitiesOnly:
        oldEntities = numpy.nonzero(entDB.meta['lastSeen'] < (entDB.dates[entDB.nxtDateIndex-1]-7))[0]
        if bDebug:
            #aNames = numpy.array(entDB.meta['name'])
            #print(aNames[oldEntities])
            for index in oldEntities:
                print("DBUG:AnalSimple:{}:IgnoringOldEntity:{}, {}".format(op, entDB.meta['name'][index], entDB.meta['lastSeen'][index]))
        theSaneArray[oldEntities] = iSkip
    theRows=numpy.argsort(theSaneArray)[-numEntities:]
    rowsLen = len(theRows)
    if numEntities > rowsLen:
        print("WARN:AnalSimple:{}:RankContenders[{}] < numEntities[{}] requested, adjusting".format(op, rowsLen, numEntities))
        numEntities = rowsLen
    if op == 'top':
        lStart = -1
        lStop = -(numEntities+1)
        lDelta = -1
    elif op == 'bottom':
        lStart = 0
        lStop = numEntities
        lDelta = 1
    theSelected = []
    print("INFO:AnalSimple:{}:{}:{}".format(op, dataSrc, opType))
    for i in range(lStart,lStop,lDelta):
        index = theRows[i]
        if (theSaneArray[index] == iSkip) or ((opType == 'block_ranked') and (theSaneArray[index] == 0)):
            print("    WARN:AnalSimple:{}:No more valid elements".format(op))
            break
        curEntry = [entDB.meta['codeL'][index], entDB.meta['name'][index], theSaneArray[index]]
        if opType == "roll_avg":
            curEntry.extend(entDB.data[dataSrcMetaData][index,1:])
        theSelected.append(curEntry)
        if iClipNameWidth == None:
            curEntry[1] = "{:64}".format(curEntry[1])
        else:
            curEntry[1] = "{:{width}}".format(curEntry[1][:iClipNameWidth], width=iClipNameWidth)
        curEntry[2] = numpy.round(curEntry[2],2)
        if opType == "roll_avg":
            curEntry[3:] = numpy.round(curEntry[3:],2)
            extra = ""
        elif opType == "block_ranked":
            theSelected[-1] = theSelected[-1] + [ theRankArray[index] ]
            extra = "{}:{}".format(hlpr.array_str(theRankArray[index],4,"A0L1"), hlpr.array_str(entDB.data[metaDataAvgs][index, iValidBlockAtBegin:],6,2))
        else:
            extra = ""
        print("    {} {}".format(extra, curEntry))
    return theSelected


def infoset1_prep(entDB=None):
    """
    Run a common set of operations, which can be used to get some amount of
    potentially useful info, on the loaded data,
    """
    entDB = _entDB(entDB)
    warnings.filterwarnings('ignore')
    ops(['srel=srel(data)', 'dma50Srel=dma50(srel)'], entDB=entDB)
    ops(['roabs=reton_absret(data)', 'rorpa=reton_retpa(data)'], entDB=entDB)
    ops(['roll1095=roll1095(data)', 'dma50Roll1095=dma50(roll1095)'], entDB=entDB)
    ops(['roll1825=roll1825(data)', 'dma50Roll1825=dma50(roll1825)'], entDB=entDB)
    blockDays = int(entDB.nxtDateIndex/5)
    ops(['blockNRoll1095=block{}(roll1095)'.format(blockDays)], entDB=entDB)
    warnings.filterwarnings('default')


def infoset1_result_entcodes(entCodes, bPrompt=False, numEntries=-1, entDB=None):
    """
    Print data generated by processing the loaded data, wrt the specified entities,
    to the user.

    NOTE: As 2nd part of the result dump, where it prints data across all specified
    entities, wrt each data aspect that was processed during prep, it tries to sort
    them based on the average meta data info from roll1095 (3Y). And entities which
    are less than 3 years will get collated to the end of the sorted list, based on
    the last RetPA from srel operation. If there are entities which get dropped by
    both the sort operations, then they will get collated to the end.

    numEntries if greater than 0, will limit the number of entities that are shown
    in the comparitive print wrt each processed data type.
    """
    entDB = _entDB(entDB)
    dataSrcs = [
            ['srel', 'srelMetaLabel'],
            ['absRet', 'roabsMetaLabel'],
            ['retPA', 'rorpaMetaLabel'],
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
    analR1095 = anal_simple('roll1095', 'top', 'roll_avg', entCodes=entCodes, numEntities=len(entCodes), minEntityLifeDataInYears=dateDuration, entDB=entDB)
    analR1095EntCodes = [ x[0] for x in analR1095 ]
    s1 = set(entCodes)
    s2 = set(analR1095EntCodes)
    otherEntCodes = s1-s2
    analSRelRPA = anal_simple('srel', 'top', 'srel_retpa', entCodes=otherEntCodes, numEntities=len(otherEntCodes), minEntityLifeDataInYears=dateDuration, entDB=entDB)
    analSRelRPAEntCodes = [ x[0] for x in analSRelRPA ]
    s3 = set(analSRelRPAEntCodes)
    entCodes = analR1095EntCodes + analSRelRPAEntCodes + list(s1-(s2.union(s3)))

    anal_simple('blockNRoll1095', 'top', 'block_ranked', entCodes=entCodes, numEntities=len(entCodes), minEntityLifeDataInYears=dateDuration, entDB=entDB)

    totalEntries = len(entCodes)
    if numEntries > totalEntries:
        numEntries = totalEntries
    for dataSrc in dataSrcs:
        print("DataSrc:{}: >>showing {} of {} entries<<".format(dataSrc, numEntries, totalEntries))
        if dataSrc[0] in [ 'absRet', 'retPA' ]:
            print("\t{:6}:{:24}: {}".format("code", "name",hlpr.array_str(gHistoricGapsHdr, width=7)))
        elif dataSrc[0] == 'srel':
            print("\t{:6}:{:24}:  AbsRet    RetPA   DurYrs : startVal - endVal".format("code", "name"))
        elif dataSrc[0].startswith('roll'):
            print("\t{:6}:{:24}:   Avg   Std [ <{}% ] MaShaMT".format("code", "name", gfRollingRetPAMinThreshold))
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
            print("\t{}:{:24}: {}".format(entCode, entName, entDB.data[dataSrc[1]][entIndex]))
            entCount += 1
            if (numEntries > 0) and (entCount > numEntries):
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


def infoset1_result(entTypeTmpls=[], entNameTmpls=[], bPrompt=False, numEntries=20, entDB=None):
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

    numEntries if greater than 0, will limit the number of entities that are shown
    in the comparitive print wrt each processed data type.
    """
    entDB = _entDB(entDB)
    entCodes = []
    if (len(entTypeTmpls) == 0) and (len(entNameTmpls) == 0):
        entCodes = list(entDB.meta['codeD'].keys())
    elif (len(entTypeTmpls) == 0):
        if len(entNameTmpls) > 0:
            fm,pm = search_data(entNameTmpls) # TODO this call requires to be fixed
            entCodesMore = [ x[0] for x in fm ]
        else:
            entCodesMore = []
        entCodes = entCodes + entCodesMore
    else:
        entCodes = enttypes.members(entDB, entTypeTmpls, entNameTmpls)
    infoset1_result_entcodes(entCodes, bPrompt, numEntries, entDB=entDB)


