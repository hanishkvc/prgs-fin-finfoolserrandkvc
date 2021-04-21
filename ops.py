# A collection of operations
# HanishKVC, 2021
# GPL


import numpy
import edb
import plot as eplot
import hlpr


# By default returns data is stored as percentage and not float
gbRetDataAsFloat = False
# The Default MinRetPA assumed/checked wrt
gfMinRetPA = 4.0


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
    entDB = _entDB(entDB)
    if not dateIndex:
        dummyDateIndex, dateIndex = entDB.daterange2index(date, date)
    highKey, lowKey, closeKey = hlpr.derive_keys(['high', 'low', 'close'], srcKeyNameTmpl)
    print("DBUG:Ops:PivotPoints:", dataDst, highKey, lowKey, closeKey)
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


def plot_pivotpoints(dataKey, entCode, plotRange=10, date=-1, entDB=None, axes=None):
    """
    Plot the pivot points of the given entCode on the given axes.
    The pivot points are plotted around the specified date's location.
    plotRange: length of the pivot point lines.
    """
    entDB = _entDB(entDB)
    dummyDateIndex, dateIndex = entDB.daterange2index(date, date)
    axes = eplot._axes(axes)
    entIndex = entDB.meta['codeD'][entCode]
    pp = entDB.data[dataKey][entIndex]
    for p,s,c in zip(pp, ['S2', 'S1', 'P', 'R1', 'R2'], ['green','green','black','red','red']):
        axes.text(dateIndex-plotRange, p, s)
        axes.plot([dateIndex-plotRange*2, dateIndex], [p, p], color=c, alpha=0.5, linestyle='dashed')


def _blocky_view(dataSrcs, modes, blockDays, destKeyNameTmpl, entDB=None):
    """
    Generate data(s) which provide a blocks based view of the passed source data(s).
        For each block of days, within the overall dataset, a single representative
        value is identified, as defined by the mode.
    dataSrcs: list of source data keys for which blocks based view needs to be created.
    modes: Specifies how to generate the blocks based view. It could be one of
        'M': Use the max value from among all the data wrt each of the blocks.
        'm': Use the min value from among all the data wrt each of the blocks.
        's': Use the values belonging to the first day from each of the blocks.
        'e': Use the values belonging to the last day from each of the blocks.
        'a': Use the average value of all the data wrt each of the blocks.
    blockDays: The size of each block wrt the blocks the overall data is divided into.
    destKeyNameTmpl: A template which specifies how the destination dataKeys
        should be named.
    NOTE: The blocks are assumed starting from the lastday in the data set,
        as the last day of the last block, irrespective of which calender day
        it may be.
    """
    entDB = _entDB(entDB)
    if type(blockDays) == str:
        blockDays = hlpr.days_in(blockDays, entDB.bSkipWeekends)
    if type(dataSrcs) == str:
        dataSrcs = [ dataSrcs ]
    srcShape = entDB.data[dataSrcs[0]].shape
    dstShape = list(srcShape)
    dstShape[1] = int(dstShape[1]/blockDays)
    dataDsts = hlpr.derive_keys(dataSrcs, destKeyNameTmpl)
    for dDst in dataDsts:
        entDB.data[dDst] = numpy.zeros(dstShape)
    endI = entDB.nxtDateIndex
    startI = endI - blockDays
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
        startI = endI - blockDays
        iDst -= 1


def weekly_view(dataSrcs, modes, destKeyNameTmpl="w.{}", entDB=None):
    """
    Reduce the given data into smaller set, by grouping adjacent data
    at a weekly level. i.e each week of data will get replaced with
    a single representative value.
    """
    return _blocky_view(dataSrcs, modes, "1W", destKeyNameTmpl, entDB)


def monthly_view(dataSrcs, modes, destKeyNameTmpl="m.{}", entDB=None):
    """
    Reduce the given data into smaller set, by grouping adjacent data
    at a monthly level. i.e each month amount of data will get replaced
    with a single representative value.
    """
    return _blocky_view(dataSrcs, modes, "1M", destKeyNameTmpl, entDB)


def rsi_sma(dataDst, dataSrc, lookBackDays=14, bEMASmooth=False, entDB=None):
    """
    Calculate RSI wrt all the entities in the entities database,
    for the full date range of data available, with the given
    lookBack period.
    NOTE: This uses a simple moving average wrt Gain and Loss.
    """
    print("DBUG:Ops:MaRSI:", dataDst, dataSrc, lookBackDays)
    entDB = _entDB(entDB)
    tData = entDB.data[dataSrc][:,1:]
    tPrev = entDB.data[dataSrc][:,:-1]
    #tData = ((tData/tPrev)-1)*100
    tData = tData - tPrev
    tPos = tData.copy()
    tNeg = tData.copy()
    tPos[tPos < 0] = 0
    tNeg[tNeg > 0] = 0
    tNeg = numpy.abs(tNeg)
    srcShape = entDB.data[dataSrc].shape
    tGain = numpy.zeros(srcShape)
    tLoss = numpy.zeros(srcShape)
    for i in range(lookBackDays):
        iStart = i
        iEnd = iStart + (srcShape[1]-lookBackDays)
        tGain[:, lookBackDays:] += tPos[:, iStart:iEnd]
        tLoss[:, lookBackDays:] += tNeg[:, iStart:iEnd]
    tGainAvg = tGain/lookBackDays
    tLossAvg = tLoss/lookBackDays
    if bEMASmooth:
        smoothDays=5
        for i in range(lookBackDays+1,srcShape[1]):
            tGainAvg[:,i] = (tGainAvg[:,i-1]*(smoothDays-3) + 3*tGainAvg[:,i])/smoothDays
            tLossAvg[:,i] = (tLossAvg[:,i-1]*(smoothDays-3) + 3*tLossAvg[:,i])/smoothDays
    tGainAvg[:,:lookBackDays] = numpy.nan
    tLossAvg[:,:lookBackDays] = numpy.nan
    tRSI = 100 - (100/(1+(tGainAvg/tLossAvg)))
    entDB.data[dataDst] = tRSI


def rsi_jww(dataDst, dataSrc, lookBackDays=14, entDB=None):
    """
    Calculate RSI wrt all the entities in the entities database,
    for the full date range of data available, with the given
    lookBack period.
    """
    print("DBUG:Ops:JwwRSI:", dataDst, dataSrc, lookBackDays)
    entDB = _entDB(entDB)
    tData = entDB.data[dataSrc][:,1:]
    tPrev = entDB.data[dataSrc][:,:-1]
    tData = tData - tPrev
    tPos = tData.copy()
    tNeg = tData.copy()
    tPos[tPos < 0] = 0
    tNeg[tNeg > 0] = 0
    tNeg = numpy.abs(tNeg)
    srcShape = entDB.data[dataSrc].shape
    tGainAvg = numpy.zeros(srcShape)
    tLossAvg = numpy.zeros(srcShape)
    tGainAvg[:,lookBackDays] = numpy.average(tPos[:,:lookBackDays], axis=1)
    tLossAvg[:,lookBackDays] = numpy.average(tNeg[:,:lookBackDays], axis=1)
    for i in range(lookBackDays+1,srcShape[1]):
        tGainAvg[:,i] = (tGainAvg[:,i-1]*(lookBackDays-1) + tPos[:,i-1])/lookBackDays
        tLossAvg[:,i] = (tLossAvg[:,i-1]*(lookBackDays-1) + tNeg[:,i-1])/lookBackDays
    tGainAvg[:,:lookBackDays] = numpy.nan
    tLossAvg[:,:lookBackDays] = numpy.nan
    tRSI = 100 - (100/(1+(tGainAvg/tLossAvg)))
    entDB.data[dataDst] = tRSI


def plot_rsi(dataKey, entCode, plotRefs=[30,50,70], entDB=None, axes=None):
    """
    Plot rsi data along with reference lines.
    """
    entDB = _entDB(entDB)
    axes = eplot._axes(axes)
    entIndex = entDB.meta['codeD'][entCode]
    eplot._data([dataKey], entCode, entDB=entDB, axes=axes)
    numDates=entDB.data[dataKey].shape[1]
    fRSI=entDB.data[dataKey][entIndex][-1]
    sRSI=str(round(fRSI,2))
    axes.text(numDates,fRSI,sRSI)
    for ref,c in zip(plotRefs,['green','black','red']):
        axes.plot([0, numDates], [ref, ref], color=c, alpha=0.5, linestyle='dashed')


def _valid_nonzero_firstlast(dataArray):
    """
    Return the valid first and last non zero value wrt each row (i.e wrt each entity).
    """
    tFinite = dataArray[numpy.isfinite(dataArray)]
    tNonZero = numpy.nonzero(tFinite)[0]
    if len(tNonZero) >= 2:
        return tFinite[tNonZero[0]],tFinite[tNonZero[-1]]
    else:
        return numpy.nan, numpy.nan


def valid_nonzero_firstlast_md(dataKey, entDB):
    """
    Return a array of valid first and last value wrt each row (i.e wrt each entity).
    """
    tData = entDB.data[dataKey]
    lFLData = []
    lFLLabel = []
    for r in range(tData.shape[0]):
        tFirst, tLast = _valid_nonzero_firstlast(tData[r])
        lFLData.append([tFirst, tLast])
        lFLLabel.append(valid_nonzero_firstlast_md2str([tFirst, tLast]))
    return numpy.array(lFLData), numpy.array(lFLLabel)


def _ssconvolve_data(data, weight):
    """
    A simple stupid convolve, which returns the valid set.
    NOTE: This doesnt mirror the weight before using it.
          Which is what makes sense for its use here.
    """
    resLen = len(data)-len(weight)+1
    #tResult = numpy.zeros(resLen)
    tResult = numpy.zeros(len(data))
    for i in range(len(weight)):
        tResult[:resLen] += data[i:resLen+i]*weight[i]
    return tResult[:resLen]


def _movavg_init(maDays, mode='s'):
    """
    Create intermediate data required to do moving average operation.
    """
    mode = mode.lower()
    if mode == 'e':
        baseWeight = 2/(maDays+1)
        weightLen = int(numpy.log(0.001)/-baseWeight)
        maWinWeights = numpy.arange(weightLen-1,-1,-1)
        maWinWeights = (1-baseWeight)**maWinWeights
        maWinWeights = maWinWeights/sum(maWinWeights)
    else:
        maWinWeights = numpy.ones(maDays)/maDays
        baseWeight = maWinWeights[0]
    xMA = { 'maDays': maDays, 'mode': mode, 'baseWeight': baseWeight, 'winWeights': maWinWeights }
    return xMA


def _movavg(xMA, dataDst, dataSrc, entDB):
    """
    Calculate the moving average.
    """
    eCnt, dCnt = entDB.data[dataSrc].shape
    entDB.data[dataDst] = numpy.zeros([eCnt, dCnt])
    weightsLen = len(xMA['winWeights'])
    validLen = dCnt-weightsLen+1
    for i in range(weightsLen):
        entDB.data[dataDst][:,weightsLen-1:] += entDB.data[dataSrc][:,i:validLen+i]*xMA['winWeights'][i]
    entDB.data[dataDst][:,:weightsLen-1] = numpy.nan


def movavg_md2str(entMD):
    """
    Convert moving averages meta data into string.
    """
    label = "{:8.4f} - {:8.4f}".format(entMD[0], entMD[1])
    return label


valid_nonzero_firstlast_md2str=movavg_md2str


def movavg(dataDst, dataSrc, maDays, mode='s', entDB=None):
    """
    Calculate the Moving average (simple or exponential) for the given dataSrc.
    """
    dataDstMT, dataDstMD, dataDstML = hlpr.data_metakeys(dataDst)
    entDB = _entDB(entDB)
    entDB.data[dataDstMT] = 'movavg'
    xMA = _movavg_init(maDays, mode)
    _movavg(xMA, dataDst, dataSrc, entDB)
    entDB.data[dataDstMD], entDB.data[dataDstML] = valid_nonzero_firstlast_md(dataDst, entDB)


def reton_md2str(entMD):
    """
    Convert retOn meta data into string.
    """
    return hlpr.array_str(entMD, width=7)


def reton(dataDst, dataSrc, retonDateIndex, retonType, historicGaps, entDB=None):
    """
    Calculate the absolute returns and or returnsPerAnnum as on endDate wrt/relative_to
    all the other dates.
    """
    dataDstMT, dataDstMD, dataDstML = hlpr.data_metakeys(dataDst)
    entDB = _entDB(entDB)
    entDB.data[dataDstMT] = 'reton'
    daysInAYear = hlpr.days_in('1Y', entDB.bSkipWeekends)
    startDateIndex, endDateIndex = entDB.daterange2index(-1, -1)
    entDB.data[dataDstMD] = numpy.ones([entDB.nxtEntIndex,historicGaps.shape[0]])*numpy.nan
    validHistoric = historicGaps[historicGaps < (retonDateIndex+1)]
    histDays = abs(numpy.arange(endDateIndex+1)-retonDateIndex)
    retonData = entDB.data[dataSrc][:, retonDateIndex].reshape(entDB.nxtEntIndex,1)
    tROAbs = ((retonData/entDB.data[dataSrc])-1)*100
    tRORPA = (((retonData/entDB.data[dataSrc])**(daysInAYear/histDays))-1)*100
    if retonType == 'absret':
        tResult = tROAbs
    elif retonType == 'retpa':
        tResult = tRORPA
    else:
        if len(tROAbs) > daysInAYear:
            #tResult[:, -daysInAYear:] = tROAbs[:, -daysInAYear:]
            tResult = tROAbs
            tResult[:, :-daysInAYear] = tRORPA[:, :-daysInAYear]
        else:
            tResult = tROAbs
    entDB.data[dataDstMD][:, :validHistoric.shape[0]] = tResult[:, -(validHistoric+1)]
    entDB.data[dataDstML] = []
    for md in entDB.data[dataDstMD]:
        entDB.data[dataDstML].append(reton_md2str(md))


def relto_md2str(entMD):
    """
    Convert the relto MetaData to string
    """
    label = "{:6.2f}% {:6.2f}%pa {:4.1f}Yrs : {:8.4f} - {:8.4f}".format(entMD[0], entMD[1], entMD[2], entMD[3], entMD[4])
    return label


def relto(dataDst, dataSrc, baseDate, entDB=None):
    """
    Calculate the absolute return for all dates wrt/relative_to a given base date.
    """
    dataDstMT, dataDstMD, dataDstML = hlpr.data_metakeys(dataDst)
    entDB = _entDB(entDB)
    entDB.data[dataDstMT] = 'relto'
    daysInAYear = hlpr.days_in('1Y', entDB.bSkipWeekends)
    startDateIndex, endDateIndex = entDB.daterange2index(-1, -1)
    baseDateIndex = entDB.datesD[baseDate]
    dBase = entDB.data[dataSrc][:, baseDateIndex].reshape(entDB.nxtEntIndex,1)
    dEnd = entDB.data[dataSrc][:, endDateIndex]
    tResult = ((entDB.data[dataSrc]/dBase)-1)*100
    dLatestAbsRet = tResult[:, -1]
    durationInYears = hlpr.days2year(endDateIndex-baseDateIndex+1, entDB.bSkipWeekends)
    dLatestRetPA = ((((dLatestAbsRet/100)+1)**(1/durationInYears))-1)*100
    entDB.data[dataDstMD] = numpy.zeros([entDB.nxtEntIndex,5])
    entDB.data[dataDstMD][:,0] = dLatestAbsRet
    entDB.data[dataDstMD][:,1] = dLatestRetPA
    entDB.data[dataDstMD][:,2] = durationInYears
    entDB.data[dataDstMD][:,3] = dBase.transpose()
    entDB.data[dataDstMD][:,4] = dEnd
    entDB.data[dataDstML] = []
    for md in entDB.data[dataDstMD]:
        entDB.data[dataDstML].append(relto_md2str(md))


def blockstats_md2str(entMD):
    label = "<{} {:5.2f} {:5.2f}>".format(hlpr.array_str(entMD[0],4,1), entMD[1], entMD[3])
    return label


def blockstats(dataDst, dataSrc, blockDays, entDB=None):
    """
    Calculate stats like Avg,STD,Qnts wrt each block of data.
    The data in the specified dataSrc is divided into blocks of blockDays duration
    and the statistics calculated for each resultant block.
    NOTE: Any Inf or NaN value will be converted to 0, before stats are calculated.
    TODO1: Replace invalid with calculated avg, before calculating STD, so that it
        is not affected by those entries much. Or rather better still simply ignore
        invalid values, when calculating Std.
    TODO2: Add a skipBlocksAtBegin argument, to skip any blocks at the begining of
        the chain of blocks, if so specified by the user.
        Could be used to skip Non Data blocks/duration at begining of RollRet op.
    """
    # Get generic things required
    dataDstMT, dataDstMD, dataDstML = hlpr.data_metakeys(dataDst)
    entDB = _entDB(entDB)
    entDB.data[dataDstMT] = 'blockstats'
    daysInAYear = hlpr.days_in('1Y', entDB.bSkipWeekends)
    startDateIndex, endDateIndex = entDB.daterange2index(-1, -1)
    # Prepare the job specific params
    blockTotalDays = endDateIndex - startDateIndex + 1
    blockCnt = int(blockTotalDays/blockDays)
    dataDstAvgs = "{}Avgs".format(dataDst)
    dataDstStds = "{}Stds".format(dataDst)
    dataDstQntls = "{}Qntls".format(dataDst)
    entDB.data[dataDstAvgs] = numpy.zeros([entDB.nxtEntIndex,blockCnt])
    entDB.data[dataDstStds] = numpy.zeros([entDB.nxtEntIndex,blockCnt])
    entDB.data[dataDstQntls] = numpy.zeros([entDB.nxtEntIndex,blockCnt,5])
    entDB.data[dataDstMD] = numpy.empty([entDB.nxtEntIndex,4], dtype=object)
    # Calc the stats
    iEnd = endDateIndex+1
    lAvgs = []
    lStds = []
    for i in range(blockCnt):
        iDst = blockCnt-i-1
        iStart = iEnd-blockDays
        tBlockData = entDB.data[dataSrc][:,iStart:iEnd].copy()
        tBlockData[~numpy.isfinite(tBlockData)] = 0
        entDB.data[dataDstAvgs][:,iDst] = numpy.mean(tBlockData,axis=1)
        entDB.data[dataDstStds][:,iDst] = numpy.std(tBlockData,axis=1)
        entDB.data[dataDstQntls][:,iDst] = numpy.quantile(tBlockData,[0,0.25,0.5,0.75,1],axis=1).transpose()
        iEnd = iStart
    entDB.data[dataDstML] = []
    for i in range(entDB.nxtEntIndex):
        entDB.data[dataDstMD][i,0] = entDB.data[dataDstAvgs][i]
        entDB.data[dataDstMD][i,1] = numpy.mean(entDB.data[dataDstAvgs][i])
        entDB.data[dataDstMD][i,2] = entDB.data[dataDstStds][i]
        entDB.data[dataDstMD][i,3] = numpy.mean(entDB.data[dataDstStds][i])
        entDB.data[dataDstML].append(blockstats_md2str(entDB.data[dataDstMD][i]))


def rollret_md2str(entMD):
    """
    Convert the entity MetaData wrt rollret op to a string.
    """
    theStr = "{:7.2f} {:7.2f} [{:5.2f}%<] {:7.2f} {:4.1f}".format(entMD[0], entMD[1], entMD[2], entMD[3], entMD[4])
    return theStr


def rollret(dataDst, dataSrc, rollDays, rollType, entDB=None):
    """
    Calculate the rolling return corresponding to the given rollDays,
    for each day in the database.
    rollDays: Calculate the returns got after the specified time duration.
    rollType: Whether to keep the returns as AbsoluteReturn or ReturnPerAnnum.
        'abs' | 'retpa'
    """
    # Get generic things required
    dataDstMT, dataDstMD, dataDstML = hlpr.data_metakeys(dataDst)
    entDB = _entDB(entDB)
    entDB.data[dataDstMT] = 'rollret'
    daysInAYear = hlpr.days_in('1Y', entDB.bSkipWeekends)
    startDateIndex, endDateIndex = entDB.daterange2index(-1, -1)
    # Rolling ret related logic starts
    durationForPA = rollDays/daysInAYear
    if rollType == 'abs':
        durationForPA = 1
    tResult = numpy.zeros(entDB.data[dataSrc].shape)
    tResult[:,rollDays:] = (entDB.data[dataSrc][:,rollDays:]/entDB.data[dataSrc][:,:-rollDays])**(1/durationForPA)
    if not gbRetDataAsFloat:
        tResult = (tResult - 1)*100
    tResult[:,:rollDays] = numpy.nan
    entDB.data[dataDst] = tResult
    # Create the meta datas
    entDB.data[dataDstMD] = numpy.zeros([entDB.nxtEntIndex, 5])
    trValid = numpy.ma.masked_invalid(tResult)
    # The Avgs
    trAvg = numpy.mean(trValid, axis=1)
    trAvg.set_fill_value(numpy.nan)
    entDB.data[dataDstMD][:,0] = trAvg.filled()
    # The Stds
    trStd = numpy.std(trValid, axis=1)
    trStd.set_fill_value(numpy.nan)
    entDB.data[dataDstMD][:,1] = trStd.filled()
    # The BelowMinRetPA
    trValidBelowMinRetPA = numpy.count_nonzero(trValid < gfMinRetPA, axis=1)*1.0
    trValidBelowMinRetPA.set_fill_value(numpy.nan)
    trValidLens = numpy.count_nonzero(trValid, axis=1)*1.0
    trValidLens.set_fill_value(numpy.nan)
    trBelowMinRetPA = (trValidBelowMinRetPA.filled()/trValidLens.filled())*100
    entDB.data[dataDstMD][:,2] = trBelowMinRetPA
    # The MaSharpeMinT
    trMaSharpeMinT = (trAvg-gfMinRetPA)/trStd
    trMaSharpeMinT.set_fill_value(numpy.nan)
    entDB.data[dataDstMD][:,3] = trMaSharpeMinT.filled()
    # The Years alive
    trYears = ((entDB.meta['lastSeenDI'] - entDB.meta['firstSeenDI'])+1)/daysInAYear
    entDB.data[dataDstMD][:,4] = trYears
    # Meta label and Years
    entDB.data[dataDstML] = []
    for md in entDB.data[dataDstMD]:
        entDB.data[dataDstML].append(rollret_md2str(md))


def srel_md2str(entMD):
    theStr = "{:7.2f}% {:7.2f}%pa {:4.1f}Yrs : {:9.4f} - {:9.4f}".format(entMD[0], entMD[1], entMD[2], entMD[3], entMD[4])
    return theStr


def srel(dataDst, dataSrc, entDB):
    """
    Calculate the absolute return for all dates wrt/relative_to start date.
    NOTE: If a entity was active from day 1 or rather 0th day wrt database,
        then the return is calculated wrt that.
        However if the entity started later than start date, then calculate
        relative to the start date of that given entity.
    """
    # Get generic things required
    dataDstMT, dataDstMD, dataDstML = hlpr.data_metakeys(dataDst)
    entDB = _entDB(entDB)
    entDB.data[dataDstMT] = 'srel'
    daysInAYear = hlpr.days_in('1Y', entDB.bSkipWeekends)
    startDateIndex, endDateIndex = entDB.daterange2index(-1, -1)
    # Rolling ret related logic starts
    iStart = entDB.meta['firstSeenDI']
    dStart = entDB.data[dataSrc][range(entDB.nxtEntIndex), iStart]
    dStartT = dStart.reshape(entDB.nxtEntIndex,1)
    dEnd = entDB.data[dataSrc][:, endDateIndex]
    tResult = entDB.data[dataSrc]/dStartT
    if not gbRetDataAsFloat:
        tResult = (tResult - 1)*100
    entDB.data[dataDst] = tResult
    # Work on the meta data
    entDB.data[dataDstMD] = numpy.zeros([entDB.nxtEntIndex,5])
    dAbsRet = tResult[:, -1]
    totalDays = endDateIndex-startDateIndex+1
    durationInYears = (totalDays - iStart)/daysInAYear
    dRetPA = (((dEnd/dStart)**(1/durationInYears))-1)*100
    entDB.data[dataDstMD][:,0] = dAbsRet
    entDB.data[dataDstMD][:,1] = dRetPA
    entDB.data[dataDstMD][:,2] = durationInYears
    entDB.data[dataDstMD][:,3] = dStart
    entDB.data[dataDstMD][:,4] = dEnd
    entDB.data[dataDstML] = []
    for i in range(entDB.nxtEntIndex):
        entDB.data[dataDst][i, :iStart[i]] = numpy.nan
        md = entDB.data[dataDstMD][i]
        entDB.data[dataDstML].append(srel_md2str(md))


