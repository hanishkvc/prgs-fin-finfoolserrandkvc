#!/usr/bin/env python3
# Work with sequences of data belonging to related groups of entities
# HanishKVC, 2021
# GPL
#

import sys
import calendar
import os
import numpy
import matplotlib.pyplot as plt
import time
import traceback
import readline
import warnings
import tabcomplete as tc
import hlpr
import indiamf
import enttypes
import indexes



class Entities:


    def _init_types(self):
        """
        Create the types db related members.
        typesD: Allows identify the typeIndex/typeId associated with given typeName
        typesL: Allows one to get the typeName from a given typeId
        """
        self.nxtTypeIndex = 0
        self.typesD = {}
        self.typesL = []
        self.typeMembers = {}


    def _init_dates(self, dateCnt):
        """
        Create things related to dates.
        """
        self.nxtDateIndex = 0
        self.dates = numpy.zeros(dateCnt)


    def _init_ents(self, dataKey, entCnt):
        """
        Create the members required to handle the data related to the entities.
        """
        self.nxtEntIndex = 0
        self.data = {}
        self.meta = {}
        self.data[dataKey] = numpy.zeros(entCnt)
        self.meta['name'] = numpy.empty(entCnt, dtype=object)
        self.meta['codeL'] = numpy.zeros(entCnt)
        self.meta['codeD'] = {}
        self.meta['typeId'] = numpy.empty(entCnt, dtype=object)
        self.meta['firstSeen'] = numpy.zeros(entCnt)
        self.meta['lastSeen'] = numpy.zeros(entCnt)


    def __init__(self, dataKey, entCnt, dateCnt):
        """
        Initialise a entities object.
        """
        self._init_types()
        self._init_dates(dateCnt)
        self._init_ents(dataKey, entCnt)


    def add_type(typeName):
        """
        Add a type (typeName) into types db, if not already present.
        Return the typeId.
        """
        if typeName not in self.types:
            self.typesD[typeName]=self.nxtTypeIn
            self.typesL.append(typeName)
            self.typeMembers[typeName] = []
            self.nxtTypeIn += 1
        return self.typesD[typeName]


    def


gbDEBUG = False
FINFOOLSERRAND_BASE = None
#
# A set of default load filters for different dataset sources
#
gLoadFilters = { }

#
# Data processing and related
#
gbDoRawData=False
gbDoRelData=True
# MovingAvg related globals
gbDoMovingAvg=False
MOVINGAVG_WINSIZE = 20
MOVINGAVG_CONVOLVEMODE = 'valid'
# Rolling returns
gbDoRollingRet=False
ROLLINGRET_WINSIZE = 365
# 1D, 1W, 1M, 3M, 6M, 1Y, 3Y, 5Y, 10Y
gHistoricGaps = numpy.array([1, 5, 30, 92, 183, 365, 1095, 1825, 3650])
gHistoricGapsHdr = numpy.array(["1D", "5D", "1M", "3M", "6M", "1Y", "3Y", "5Y", "10Y"])

#
# proc_days related controls
#
# Should proc_days process beyond yesterday (i.e into today or future)
gbNotBeyondYesterday = True
# Should proc_days ignore weekends.
gbSkipWeekEnds = False
# Should relative data calc ignore non data at begining of dataset
gbDataRelIgnoreBeginingNonData = True

#
# Misc
#
giLabelNameChopLen = 36



gCal = calendar.Calendar()
gData = {}
gMeta = {}
gCB = {
        'fetch_data':[],
        'fetch4date':[],
        'load_data' :[],
        'load4date' :[],
    }



def setup_paths():
    """
    Account for FINFOOLSERRAND_BASE env variable if set
    """
    global FINFOOLSERRAND_BASE
    FINFOOLSERRAND_BASE = os.environ.get('FINFOOLSERRAND_BASE',"~/")
    print("INFO:Main:setup_paths:", FINFOOLSERRAND_BASE)


def setup_gdata(startDate=-1, endDate=-1):
    """
    Initialise the gData dictionary

    NumOfRows (corresponding to MFs) is set to a fixed value.
    NumOfCols (corresponding to Dates) is set based on date range.
    """
    numDates = ((int(str(endDate)[:4]) - int(str(startDate)[:4]))+2)*365
    gData.clear()
    gData['data'] = numpy.zeros([8192*4, numDates])
    gMeta.clear()
    gMeta['code2index'] = {}
    gMeta['index2code'] = {}
    gMeta['nextEntIndex'] = 0
    gMeta['dataIndex'] = -1
    gMeta['names'] = []
    gMeta['dates'] = []
    gMeta['skipped'] = set()
    gMeta['dateRange'] = [-1, -1]
    gMeta['plots'] = set()
    enttypes.init(gMeta)
    gMeta['typeId'] = numpy.ones(8192*4, dtype=numpy.int32)
    gData['metas'] = {}
    gMeta['lastSeen'] = numpy.zeros(8192*4, dtype=numpy.int32)


def setup_modules():
    tc.gData = gData
    tc.gMeta = gMeta
    indiamf.setup(FINFOOLSERRAND_BASE, gData, gMeta, gCB, gLoadFilters)
    indexes.setup(FINFOOLSERRAND_BASE, gData, gMeta, gCB, gLoadFilters)


def setup():
    setup_gdata()
    setup_paths()
    setup_modules()
    gLoadFilters['default'] = gLoadFilters['indiamf'].copy()
    loadfilters_list()


def proc_days(start, end, handle_date_func, opts=None, bNotBeyondYesterday=True, bDebug=False):
    """
    call the passed function for each date with the given start and end range.
        The date will be passed to the passed function as year, month, date
        as integers.

    start and end need to be dictionaries {'y': year_int, 'm': month_int, 'd': date_int}
        month_int should be from 1 to 12
        date_int should be from 1 to 31; 'd' and thus inturn date_int is optional
    """
    print("INFO:proc_days:from {} to {}".format(start, end))
    now = time.localtime(time.time())
    if bNotBeyondYesterday:
        bChanged = False
        if end['y'] > now.tm_year:
            end['y'] = now.tm_year
            bChanged = True
        elif end['y'] == now.tm_year:
            if end['m'] > now.tm_mon:
                end['m'] = now.tm_mon
                bChanged = True
        if bChanged:
            print("WARN:proc_days:end adjusted to be within today")
    for y in range(start['y'], end['y']+1):
        for m in range(1,13):
            startDate = None
            endDate = None
            if (y == start['y']):
                if (m < start['m']):
                    continue
                elif (m == start['m']):
                    startDate = start.get('d', None)
            if (y == end['y']):
                if (m > end['m']):
                    continue
                elif (m == end['m']):
                    endDate = end.get('d', None)
            print("INFO:proc_days:handlingmonth:{}{:02}:DayLimitsIfAny [{} to {}]".format(y,m,startDate, endDate))
            for d in gCal.itermonthdays(y,m):
                if d == 0:
                    continue
                if (startDate != None) and (d < startDate):
                    continue
                if (endDate != None) and (d > endDate):
                    continue
                if gbSkipWeekEnds and (calendar.weekday(y,m,d) in [5, 6]):
                    continue
                if bNotBeyondYesterday and (y == now.tm_year) and (m == now.tm_mon) and (d >= now.tm_mday):
                    continue
                if bDebug:
                    print("INFO:proc_days:handledate:{}{:02}{:02}".format(y,m,d))
                try:
                    handle_date_func(y,m,d,opts)
                except:
                    traceback.print_exc()


def fetch4date(y, m, d, opts):
    """
    Fetch data for the given date.

    This is the default handler function passed to proc_days.

    One can call this directly by passing the year, month and date one is interested in.
        month should be one of 1 to 12
        day (month day) should be one of 1 to 31, as appropriate for month specified.
    """
    #print(y,m,d)
    for cb in gCB['fetch4date']:
        cb(y, m, d, opts)


def date2datedict(date, fallBackMonth=1):
    """
    Convert a date specified in YYYYMMDD format into internal date dictionary format
        MM and DD are optional.
        MM if not specified fallsback to the value passed through fallBackMonth arg.
        If DD is needed, then MM needs to be used.
    NOTE: date could be either a interger or string in YYYY[MM[DD]] format.
    """
    dateStr = str(date)
    year = dateStr[:4]
    month = dateStr[4:6]
    day = dateStr[6:8]
    if (year == '') or (len(year) != 4):
        exit()
    date = {}
    date['y'] = int(year)
    if month != '':
        date['m'] = int(month)
    else:
        date['m'] = fallBackMonth
    if day != '':
        date['d'] = int(day)
    return date


def proc_date_startend(startDate, endDate):
    """
    Convert the start and end dates given as integer/string notation of YYYYMMDD
    into this programs internal date dictionary representation.

    The dates should follow the YYYY[MM[DD]] format, where [] means optional.
    """
    start = date2datedict(startDate, 1)
    end = date2datedict(endDate, 12)
    return start, end


def fetch4daterange(startDate, endDate, opts):
    """
    Fetch data for given date range.

    The dates should follow one of these formats YYYY or YYYYMM or YYYYMMDD i.e YYYY[MM[DD]]
    """
    start, end = proc_date_startend(startDate, endDate)
    proc_days(start, end, fetch4date, opts, gbNotBeyondYesterday)


def fetch_data(startDate, endDate=None, opts={'ForceRemote': True}):
    """
    Fetch data for a given date or range of dates

    If only startDate is given, then endDate is assumed to be same as startDate.
    This is useful to fetch a full year or a full month of data, where one gives
    only the YYYY or YYYYMM as the startDate, then the logic across the call
    chain will ensure that starts correspond to 1 and ends correspond to 12 or
    31, as the case may be..

    NOTE: Fetch may look for two possible options ForceLocal and ForceRemote.
    Based on these options and health of data pickle file, it may decide how
    to handle the fetch.

    NOTE: By default fetch_data gives priority to fetching data from remote
    server. While fetch data triggered by load_data, will give priority to
    fetching data from local cached file, before falling back to remote server
    based fetch.
    """
    if endDate == None:
        endDate = startDate
    for cb in gCB['fetch_data']:
        cb(startDate, endDate, opts)
    return fetch4daterange(startDate, endDate, opts)


def print_skipped():
    """
    Print the skipped list of MFs, so that user can cross verify, things are fine.
    """
    msg = "WARN: About to print the list of SKIPPED/Filtered out MFs"
    if gbDEBUG:
        input("{}, press any key...".format(msg))
    else:
        print(msg)
    for skippedMF in gMeta['skipped']:
        print(skippedMF)
    print("WARN: The above MFs were skipped/filtered out when loading")


def load4date(y, m, d, opts):
    """
    Load data for the given date.

    NOTE: This logic wont fill in prev nav for holidays,
    you will have to call fillin4holidays explicitly.
    """
    gMeta['dataIndex'] += 1
    gMeta['dates'].append(hlpr.dateint(y,m,d))
    for cb in gCB['load4date']:
        cb(y, m, d, opts)



def load4daterange(startDate, endDate, opts=None):
    """
    Load data for given date range.

    The dates should follow one of these formats YYYY or YYYYMM or YYYYMMDD i.e YYYY[MM[DD]]

    NOTE: This logic takes care of filling in nav values for holidays
    automatically by calling fillin4holidays.

    NOTE: If we dont have csv files for all the dates specified, in the date range,
    then ensure that we have atleast data loaded till the 1st non existant date. Rather
    the logic ensures that data is loaded for all the dates for which data csv exists.
    """
    start, end = proc_date_startend(startDate, endDate)
    try:
        proc_days(start, end, load4date, opts, gbNotBeyondYesterday)
    except:
        excInfo = sys.exc_info()
        print(excInfo)
    fillin4holidays()
    if gbDEBUG:
        print_skipped()


gWhiteListEntTypes = None
gWhiteListEntNames = None
gBlackListEntNames = None


def _loadfilters_set(whiteListEntTypes=None, whiteListEntNames=None, blackListEntNames=None):
    """
    Setup global filters used by load logic.

    If whiteListEntTypes is set, loads only Entities which belong to a EntityType which matches one of the given EntityType templates.
    If whiteListEntNames is set, loads only Entities whose name matches any one of the given match templates.
    If blackListEntNames is set, loads only Entities whose names dont match any of the corresponding match templates.
    """
    global gMeta
    gMeta['whiteListEntTypes'] = whiteListEntTypes
    gMeta['whiteListEntNames'] = whiteListEntNames
    gMeta['blackListEntNames'] = blackListEntNames
    print("LoadFiltersSet:Global Filters:\n\twhiteListEntTypes {}\n\twhiteListEntNames {}\n\tblackListEntNames {}".format(whiteListEntTypes, whiteListEntNames, blackListEntNames))


def _loadfilters_clear():
    """
    Clear any global white/blacklist filters setup wrt load operation.
    """
    loadfilters_set(None, None, None)


def _loadfilters_load(loadFiltersName=None):
    """
    Helper function to load from a previously defined set of loadfilters wrt different data sources/users preferences.

    If None is passed, then loadfilters will be cleared.
    """
    if loadFiltersName != None:
        group = gLoadFilters[loadFiltersName]
        _loadfilters_set(group['whiteListEntTypes'], group['whiteListEntNames'], group['blackListEntNames'])
    else:
        _loadfilters_clear()


def loadfilters_setup(loadFiltersName, whiteListEntTypes=None, whiteListEntNames=None, blackListEntNames=None):
    """
    Setup a named loadFilters, which inturn can be used with load_data later.
    """
    hlpr.loadfilters_setup(gLoadFilters, loadFiltersName, whiteListEntTypes, whiteListEntNames, blackListEntNames)


def loadfilters_list():
    print("INFO:MAIN:LoadFilters")
    for lf in gLoadFilters:
        print("    {}".format(lf))
        for t in gLoadFilters[lf]:
            print("        {} : {}".format(t, gLoadFilters[lf][t]))


def load_data(startDate, endDate = None, bClearData=True, bOptimizeSize=True, loadFiltersName='default'):
    """
    Load data for given date range.

    The dates should follow one of these formats YYYY or YYYYMM or YYYYMMDD i.e YYYY[MM[DD]]

    bClearData if set, resets the gData by calling setup_gdata.

    loadFiltersName: User can optionally specify a previously defined loadFiltersName, in
    which case the whiteListEntTypes/whiteListEntNames/blackListEntNames, used by underlying
    load logic, if any, will be set as defined by the given loadFiltersName.

        If this argument is not specified, then the default loadFilters will be used.
        If you dont want any loadfilters to be applied, then pass None.

        NOTE: The _findmatching logic will be used for matching templates.

    bOptimizeSize if set, resizes the data array to be only as big as actual loaded data.

    NOTE: This logic takes care of filling in nav values for holidays
    automatically by calling fillin4holidays.

    NOTE: If we dont have data files for some of the dates specified, in the date range, the
    load4daterange logic ensures that data for dates for which data file exists will be loaded.
    """
    if endDate == None:
        endDate = startDate
    if bClearData:
        setup_gdata(startDate, endDate)
    _loadfilters_load(loadFiltersName)
    for cb in gCB['load_data']:
        cb(startDate, endDate)
    load4daterange(startDate, endDate)
    if bOptimizeSize:
        gData['data'] = gData['data'][:gMeta['nextEntIndex'],:gMeta['dataIndex']+1]
        gMeta['lastSeen'] = gMeta['lastSeen'][:gMeta['nextEntIndex']]
        gMeta['typeId'] = gMeta['typeId'][:gMeta['nextEntIndex']]


def _fillin4holidays(mfIndex=-1):
    """
    As there wont be any Nav data for holidays including weekends,
    so fill them with the nav from the prev working day for the corresponding mf.
    """
    lastNav = -1
    for c in range(gMeta['dataIndex']+1):
        if gData['data'][mfIndex,c] == 0:
            if lastNav > 0:
                gData['data'][mfIndex,c] = lastNav
        else:
            lastNav = gData['data'][mfIndex,c]


def fillin4holidays():
    """
    As there wont be any Nav data for holidays including weekends,
    so fill them with the nav from the prev working day for the corresponding mf.
    """
    for r in range(gMeta['nextEntIndex']):
        _fillin4holidays(r)


def _findmatching(searchTmpl, dataSet, fullMatch=False, partialTokens=False, ignoreCase=True):
    """
    Find strings from dataSet, which match the given searchTemplate.

    Look at matches_templates function to understand how the matching
    works, and the impact of the options to control the same. It also
    gives details about the template specification.

    It returns those strings which match all given tokens,
    as well as those strings which only match certain tokens.
    """
    searchTmplFullMatch = []
    searchTmplPartMatch = []
    namesIndex = -1
    for curName in dataSet:
        namesIndex += 1
        fm, pm = hlpr.matches_templates(curName, [searchTmpl], fullMatch, partialTokens, ignoreCase)
        if (len(fm) > 0):
            searchTmplFullMatch.append([curName, namesIndex])
        if (len(pm) > 0):
            searchTmplPartMatch.append([curName, namesIndex])
    return searchTmplFullMatch, searchTmplPartMatch


def findmatchingmf(entNameTmpl, fullMatch=False, partialTokens=False, ignoreCase=True):
    """
    Find MFs from the MF dataSet, which match the given entName Template.

    NOTE: look at help of _findmatching for the search/matching behaviour.
    """
    fm, pm = _findmatching(entNameTmpl, gMeta['names'], fullMatch, partialTokens, ignoreCase)
    #breakpoint()
    fmNew = []
    for curName, curIndex in fm:
        fmNew.append([gMeta['index2code'][curIndex], curName, curIndex])
    pmNew = []
    for curName, curIndex in pm:
        pmNew.append([gMeta['index2code'][curIndex], curName, curIndex])
    return fmNew, pmNew


def search_data(findNameTmpls, bFullMatch=False, bPartialTokens=False, bIgnoreCase=True, bPrintAllTokenMatch=True, bPrintSomeTokenMatch=False):
    """
    Search/Find if there are any MFs which match the given name parts in findNameTmpls.

    findNameTmpls could either be a single matchingTemplate or a list of matchingTemplates.

    bPrintAllTokenMatch: If enabled prints MFs which match all the tokens in the specified findName.
    bPrintSomeTokenMatch: If enabled prints MFs even if they match only some of the tokens in the specified findName.
    """
    fullMatch = []
    partMatch = []
    if type(findNameTmpls) == str:
        findNameTmpls = [ findNameTmpls ]
    for nameTmpl in findNameTmpls:
        fM,pM = findmatchingmf(nameTmpl, bFullMatch, bPartialTokens, bIgnoreCase)
        if bPrintAllTokenMatch:
            print("INFO:search_data: List of Entities with All tokens Match for", nameTmpl)
        for n in fM:
            fullMatch.append(n)
            if bPrintAllTokenMatch:
                print(n)
        if bPrintSomeTokenMatch:
            print("INFO:search_data: List of Entities with Some tokens Match for", nameTmpl)
        for n in pM:
            partMatch.append(n)
            if bPrintSomeTokenMatch:
                print(n)
    return fullMatch, partMatch


def datadst_metakeys(dataDst):
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


def update_metas(op, dataSrc, dataDst):
    """
    Helps identify the last set of meta keys for a given kind of operation.
    """
    if op == "srel":
        srelMetaData, srelMetaLabel = datadst_metakeys('srel')
        gData['metas'][srelMetaData], gData['metas'][srelMetaLabel] = datadst_metakeys(dataDst)


def procdata_relative(data, bMovingAvg=False, bRollingRet=False):
    """
    Process the data relative to its 1st Non Zero value
    It calculates the
        Absolute return percentage,
        Return per annum (taking compounding into account),
        MovingAverage (optional)
        RollingReturn (optional)
    """
    dataLen = len(data)
    iStart = -1
    for i in range(dataLen):
        dStart = data[i]
        if dStart > 0:
            iStart = i
            break
    if dStart == 0:
        return data, 0, 0, 0
    dEnd = data[-1]
    dataRel = ((data/dStart)-1)*100
    if gbDataRelIgnoreBeginingNonData and (iStart != -1):
        dataRel[:iStart] = 0
    dAbsRetPercent = dataRel[-1]
    durationInYears = (dataLen-iStart)/365
    dRetPA = (((dEnd/dStart)**(1/durationInYears))-1)*100
    if bMovingAvg:
        dMovAvg = numpy.convolve(dataRel, numpy.ones(MOVINGAVG_WINSIZE)/MOVINGAVG_WINSIZE, MOVINGAVG_CONVOLVEMODE)
    else:
        dMovAvg = None
    if bRollingRet:
        dRollingRetPercents = numpy.zeros(dataLen)
        tStart = iStart
        tStart = 0
        for i in range(tStart, dataLen):
            tiEnd = i + ROLLINGRET_WINSIZE
            if tiEnd >= dataLen:
                break
            dRollingRetPercents[tiEnd] = ((data[tiEnd]/data[i]) - 1)*100
    else:
        dRollingRetPercents = None
    return dataRel, dMovAvg, dRollingRetPercents, dStart, dEnd, dAbsRetPercent, dRetPA, durationInYears


gbRelDataPlusFloat = False
gfRollingRetPAMinThreshold = 4.0
def procdata_ex(opsList, startDate=-1, endDate=-1, bDebug=False):
    """
    Allow data from any valid data key in gData to be operated on and the results to be saved
    into a destination data key in gData.

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
    startDateIndex, endDateIndex = _date2index(startDate, endDate)
    if not _daterange_checkfine(startDateIndex, endDateIndex, "procdata_ex"):
        return
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
        print("DBUG:procdata_ex:op[{}]:dst[{}]".format(curOpFull, dataDst))
        #dataLen = endDateIndex - startDateIndex + 1
        tResult = gData[dataSrc].copy()
        dataSrcMetaData, dataSrcMetaLabel = datadst_metakeys(dataSrc)
        dataDstMetaData, dataDstMetaLabel = datadst_metakeys(dataDst)
        gData[dataDstMetaLabel] = []
        #### Op specific things to do before getting into individual records
        if op == 'srel':
            gData[dataDstMetaData] = numpy.zeros([gMeta['nextEntIndex'],3])
        elif op.startswith("rel"):
            gData[dataDstMetaData] = numpy.zeros([gMeta['nextEntIndex'],3])
        elif op.startswith("roll"):
            # RollWindowSize number of days at beginning will not have
            # Rolling ret data, bcas there arent enough days to calculate
            # rolling ret while satisfying the RollingRetWIndowSize requested.
            rollDays = int(op[4:].split('_')[0])
            gData[dataDstMetaData] = numpy.zeros([gMeta['nextEntIndex'], 4])
        elif op.startswith("block"):
            blockDays = int(op[5:])
            blockTotalDays = endDateIndex - startDateIndex + 1
            blockCnt = int(blockTotalDays/blockDays)
            dataDstAvgs = "{}Avgs".format(dataDst)
            dataDstStds = "{}Stds".format(dataDst)
            dataDstQntls = "{}Qntls".format(dataDst)
            gData[dataDstAvgs] = numpy.zeros([gMeta['nextEntIndex'],blockCnt])
            gData[dataDstStds] = numpy.zeros([gMeta['nextEntIndex'],blockCnt])
            gData[dataDstQntls] = numpy.zeros([gMeta['nextEntIndex'],blockCnt,5])
            tResult = []
        elif op.startswith("reton"):
            retonT, retonType = op.split('_')
            if retonT == "reton":
                retonDateIndex = endDateIndex
            else:
                retonDate = int(retonT[5:])
                retonDateIndex = gMeta['dates'].index(retonDate)
            gData[dataDstMetaData] = numpy.ones([gMeta['nextEntIndex'],gHistoricGaps.shape[0]])*numpy.nan
            validHistoric = gHistoricGaps[gHistoricGaps < (retonDateIndex+1)]
            histDays = abs(numpy.arange(endDateIndex+1)-retonDateIndex)
        update_metas(op, dataSrc, dataDst)
        #### Handle each individual record as specified by the op
        for r in range(gMeta['nextEntIndex']):
            try:
                if op == "srel":
                    #breakpoint()
                    iStart = -1
                    dStart = 0
                    nonZeros = numpy.nonzero(gData[dataSrc][r, startDateIndex:endDateIndex+1])[0]
                    if (len(nonZeros) > 0):
                        iStart = nonZeros[0] + startDateIndex
                        dStart = gData[dataSrc][r, iStart]
                    dEnd = gData[dataSrc][r, endDateIndex]
                    if dStart != 0:
                        if gbRelDataPlusFloat:
                            tResult[r,:] = (gData[dataSrc][r,:]/dStart)
                        else:
                            tResult[r,:] = ((gData[dataSrc][r,:]/dStart)-1)*100
                        tResult[r,:iStart] = numpy.nan
                        dAbsRet = tResult[r, -1]
                        durationInYears = ((endDateIndex-startDateIndex+1)-iStart)/365
                        dRetPA = (((dEnd/dStart)**(1/durationInYears))-1)*100
                        label = "{:6.2f}% {:6.2f}%pa {:4.1f}Yrs : {:8.4f} - {:8.4f}".format(dAbsRet, dRetPA, durationInYears, dStart, dEnd)
                        gData[dataDstMetaLabel].append(label)
                        gData[dataDstMetaData][r,:] = numpy.array([dAbsRet, dRetPA, durationInYears])
                    else:
                        durationInYears = (endDateIndex-startDateIndex+1)/365
                        gData[dataDstMetaLabel].append("")
                        gData[dataDstMetaData][r,:] = numpy.array([0.0, 0.0, durationInYears])
                elif op.startswith("rel"):
                    baseDate = op[3:]
                    if baseDate != '':
                        baseDate = int(baseDate)
                        baseDateIndex = gMeta['dates'].index(baseDate)
                    else:
                        baseDateIndex = startDateIndex
                    baseData = gData[dataSrc][r, baseDateIndex]
                    dEnd = gData[dataSrc][r, endDateIndex]
                    tResult[r,:] = (((gData[dataSrc][r,:])/baseData)-1)*100
                    dAbsRet = tResult[r, -1]
                    durationInYears = (endDateIndex-baseDateIndex+1)/365
                    dRetPA = ((((dAbsRet/100)+1)**(1/durationInYears))-1)*100
                    label = "{:6.2f}% {:6.2f}%pa {:4.1f}Yrs : {:8.4f} - {:8.4f}".format(dAbsRet, dRetPA, durationInYears, baseData, dEnd)
                    gData[dataDstMetaLabel].append(label)
                    gData[dataDstMetaData][r,:] = numpy.array([dAbsRet, dRetPA, durationInYears])
                elif op.startswith("reton"):
                    retonType = op.split('_')[1]
                    retonData = gData[dataSrc][r, retonDateIndex]
                    if retonType == 'absret':
                        tResult[r,:] = ((retonData/gData[dataSrc][r,:])-1)*100
                    else:
                        tResult[r,:] = (((retonData/gData[dataSrc][r,:])**(365/histDays))-1)*100
                    gData[dataDstMetaData][r,:validHistoric.shape[0]] = tResult[r,-validHistoric]
                    gData[dataDstMetaLabel].append(hlpr.array_str(gData[dataDstMetaData][r], width=7))
                elif op.startswith("dma"):
                    days = int(op[3:])
                    tResult[r,:] = numpy.convolve(gData[dataSrc][r,:], numpy.ones(days)/days, 'same')
                    inv = int(days/2)
                    tResult[r,:inv] = numpy.nan
                    tResult[r,gMeta['dataIndex']-inv:] = numpy.nan
                    if True:
                        try:
                            dataSrcLabel = gData[dataSrcMetaLabel][r]
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
                        gData[dataDstMetaLabel].append(label)
                elif op.startswith("roll"):
                    durationForPA = rollDays/365
                    if '_' in op:
                        opType = op.split('_')[1]
                        if opType == 'abs':
                            durationForPA = 1
                    if gbRelDataPlusFloat:
                        tResult[r,rollDays:] = (gData[dataSrc][r,rollDays:]/gData[dataSrc][r,:-rollDays])**(1/durationForPA)
                    else:
                        tResult[r,rollDays:] = (((gData[dataSrc][r,rollDays:]/gData[dataSrc][r,:-rollDays])**(1/durationForPA))-1)*100
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
                    gData[dataDstMetaData][r] = [trAvg, trStd, trBelowMinThreshold, trMaSharpeMinT]
                    label = "{:5.2f} {:5.2f} {} {:5.2f}".format(trAvg, trStd, trBelowMinThresholdLabel, trMaSharpeMinT)
                    gData[dataDstMetaLabel].append(label)
                elif op.startswith("block"):
                    # Calc the Avgs
                    iEnd = endDateIndex+1
                    lAvgs = []
                    lStds = []
                    for i in range(blockCnt):
                        iStart = iEnd-blockDays
                        tBlockData = gData[dataSrc][r,iStart:iEnd]
                        tBlockData = tBlockData[numpy.isfinite(tBlockData)]
                        lAvgs.insert(0, numpy.mean(tBlockData))
                        lStds.insert(0, numpy.std(tBlockData))
                        iEnd = iStart
                        if len(tBlockData) == 0:
                            tBlockData = [0]
                        gData[dataDstQntls][r, blockCnt-1-i] = numpy.quantile(tBlockData,[0,0.25,0.5,0.75,1])
                    avgAvgs = numpy.nanmean(lAvgs)
                    avgStds = numpy.nanmean(lStds)
                    gData[dataDstAvgs][r,:] = lAvgs
                    gData[dataDstStds][r,:] = lStds
                    #gData[dataDstMetaData][r] = [avgAvgs, avgStds]
                    label = "<{} {:5.2f} {:5.2f}>".format(hlpr.array_str(lAvgs,4,1), avgAvgs, avgStds)
                    gData[dataDstMetaLabel].append(label)
            except:
                traceback.print_exc()
                print("DBUG:ProcDataEx:Exception skipping entity at ",r)
        if len(tResult) > 0:
            gData[dataDst] = tResult


def plot_data(dataSrcs, entCodes, startDate=-1, endDate=-1):
    """
    Plot specified datas for the specified MFs, over the specified date range.

    dataSrcs: Is a key or a list of keys used to retreive the data from gData.
    entCodes: Is a entCode or a list of entCodes.
    startDate and endDate: specify the date range over which the data should be
        retreived and plotted.

    Remember to call plt.plot or show_plot, when you want to see the plots,
    accumulated till that time.
    """
    startDateIndex, endDateIndex = _date2index(startDate, endDate)
    if type(dataSrcs) == str:
        dataSrcs = [ dataSrcs ]
    if type(entCodes) == int:
        entCodes = [ entCodes]
    srelMetaData, srelMetaLabel = datadst_metakeys('srel')
    for dataSrc in dataSrcs:
        print("DBUG:plot_data:{}".format(dataSrc))
        dataSrcMetaData, dataSrcMetaLabel = datadst_metakeys(dataSrc)
        for entCode in entCodes:
            index = gMeta['code2index'][entCode]
            name = gMeta['names'][index][:giLabelNameChopLen]
            try:
                dataLabel = gData[dataSrcMetaLabel][index]
            except:
                dataLabel = ""
            try:
                metaKey = gData['metas'].get(srelMetaLabel, None)
                if metaKey != None:
                    srelLabel = gData[metaKey][index]
                else:
                    srelLabel = ""
            except:
                srelLabel = ""
            if dataLabel == "":
                dataLabel = srelLabel
            label = "{}:{:{width}}: {}".format(entCode, name, dataLabel, width=giLabelNameChopLen)
            print("\t{}:{}".format(label, index))
            label = "{}:{:{width}}: {:16} : {}".format(entCode, name, dataSrc, dataLabel, width=giLabelNameChopLen)
            plt.plot(gData[dataSrc][index, startDateIndex:endDateIndex+1], label=label)


def _procdata_mabeta(dataSrc, refCode, entCodes):
    """
    Get the slope of the entCodes wrt refCode.
    When the passed dataSrc is rollingRet, this is also called MaBeta.
    """
    refIndex = gMeta['code2index'][refCode]
    refValid = gData[dataSrc][refIndex][numpy.isfinite(gData[dataSrc][refIndex])]
    refAvg = numpy.mean(refValid)
    maBeta = []
    for entCode in entCodes:
        entIndex = gMeta['code2index'][entCode]
        entValid = gData[dataSrc][entIndex][numpy.isfinite(gData[dataSrc][entIndex])]
        entAvg = numpy.mean(entValid)
        entMaBeta = numpy.sum((entValid-entAvg)*(refValid-refAvg))/numpy.sum((refValid-refAvg)**2)
        maBeta.append(entMaBeta)
    return maBeta


def procdata_mabeta(dataSrc, refCode, entCodes):
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
    procdata_ex('roll1Abs=roll1_abs({})'.format(dataSrc))
    return _procdata_mabeta('roll1Abs', refCode, entCodes)


def _forceval_entities(data, entCodes, forcedValue, entSelectType='normal'):
    """
    Set the specified locations in the data, to the given forcedValue.

    The locations is specified using a combination of entCodes and entSelectType.
        'normal': for locations corresponding to the specified entCodes,
        'invert': for locations not specified in entCodes.
    """
    if entCodes == None:
        return data
    indexes = [gMeta['code2index'][code] for code in entCodes]
    if entSelectType == 'normal':
        mask = numpy.zeros(data.size, dtype=bool)
        mask[indexes] = True
    else:
        mask = numpy.ones(data.size, dtype=bool)
        mask[indexes] = False
    data[mask] = forcedValue
    return data


def analdata_simple(dataSrc, op, opType='normal', theDate=None, theIndex=None, numEntities=10, entCodes=None,
                        minEntityLifeDataInYears=1.5, bCurrentEntitiesOnly=True, bDebug=False, iClipNameWidth=64):
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
        srel procdata_ex operation), to decide on entities.

        srel_retpa: Look at the Returns PerAnnum data associated
        with the given dataSrc (which should be generated using
        srel procdata_ex operation), to decide on entities.

        roll_avg: look at Average ReturnsPerAnnum, calculated using
        rolling return (dataSrc specified should be generated using
        roll procdata_ex operation), to decide on entities ranking.

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
        operation through procdata_ex to generate the same.

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
    print("DBUG:AnalDataSimple:{}-{}:{}".format(dataSrc, opType, op))
    if op == 'top':
        iSkip = -numpy.inf
    else:
        iSkip = numpy.inf
    theSaneArray = None
    if opType == 'normal':
        if (type(theDate) == type(None)) and (type(theIndex) == type(None)):
            for i in range(-1, -(gMeta['dataIndex']+1),-1):
                if bDebug:
                    print("DBUG:AnalDataSimple:{}:findDateIndex:{}".format(op, i))
                theSaneArray = gData[dataSrc][:,i].copy()
                theSaneArray[numpy.isinf(theSaneArray)] = iSkip
                theSaneArray[numpy.isnan(theSaneArray)] = iSkip
                if not numpy.all(numpy.isinf(theSaneArray) | numpy.isnan(theSaneArray)):
                    dateIndex = gMeta['dataIndex']+1+i
                    print("INFO:AnalDataSimple:{}:DateIndex:{}".format(op, dateIndex))
                    break
        else:
            if (type(theIndex) == type(None)) and (type(theDate) != type(None)):
                startDateIndex, theIndex = _date2index(theDate, theDate)
            print("INFO:AnalDataSimple:{}:theIndex:{}".format(op, theIndex))
            theSaneArray = gData[dataSrc][:,theIndex].copy()
            theSaneArray[numpy.isinf(theSaneArray)] = iSkip
            theSaneArray[numpy.isnan(theSaneArray)] = iSkip
    elif opType.startswith("srel"):
        dataSrcMetaData, dataSrcMetaLabel = datadst_metakeys(dataSrc)
        if opType == 'srel_absret':
            theSaneArray = gData[dataSrcMetaData][:,0].copy()
        elif opType == 'srel_retpa':
            theSaneArray = gData[dataSrcMetaData][:,1].copy()
        else:
            input("ERRR:AnalDataSimple:dataSrc[{}]:{} unknown srel opType, returning...".format(opType))
            return None
    elif opType.startswith("roll"):
        dataSrcMetaData, dataSrcMetaLabel = datadst_metakeys(dataSrc)
        if opType == 'roll_avg':
            theSaneArray = gData[dataSrcMetaData][:,0].copy()
            theSaneArray[numpy.isinf(theSaneArray)] = iSkip
            theSaneArray[numpy.isnan(theSaneArray)] = iSkip
    elif opType == "block_ranked":
        metaDataAvgs = "{}Avgs".format(dataSrc)
        tNumEnts, tNumBlocks = gData[metaDataAvgs].shape
        theRankArray = numpy.zeros([tNumEnts, tNumBlocks+1])
        iValidBlockAtBegin = 0
        bValidBlockFound = False
        for b in range(tNumBlocks):
            tArray = gData[metaDataAvgs][:,b]
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
        input("ERRR:AnalDataSimple:dataSrc[{}]:{} unknown opType, returning...".format(opType))
        return None
    if type(theSaneArray) == type(None):
        print("DBUG:AnalDataSimple:{}:{}:{}: No SaneArray????".format(op, dataSrc, opType))
        breakpoint()
    theSaneArray = _forceval_entities(theSaneArray, entCodes, iSkip, 'invert')
    if minEntityLifeDataInYears > 0:
        dataYearsAvailable = (gMeta['dataIndex']+1)/365
        if (dataYearsAvailable < minEntityLifeDataInYears):
            print("WARN:AnalDataSimple:{}: dataYearsAvailable[{}] < minENtityLifeDataInYears[{}]".format(op, dataYearsAvailable, minEntityLifeDataInYears))
        srelMetaData, srelMetaLabel = datadst_metakeys('srel')
        theSRelMetaData = gData.get(srelMetaData, None)
        if type(theSRelMetaData) == type(None):
            procdata_ex('srel=srel(data)')
        if bDebug:
            tNames = numpy.array(gMeta['names'])
            tDroppedNames = tNames[gData[srelMetaData][:,2] < minEntityLifeDataInYears]
            print("INFO:AnalDataSimple:{}:{}:{}:Dropping if baby Entity".format(op, dataSrc, opType), tDroppedNames)
        theSaneArray[gData[srelMetaData][:,2] < minEntityLifeDataInYears] = iSkip
    if bCurrentEntitiesOnly:
        oldEntities = numpy.nonzero(gMeta['lastSeen'] < (gMeta['dates'][gMeta['dataIndex']]-7))[0]
        if bDebug:
            #aNames = numpy.array(gMeta['names'])
            #print(aNames[oldEntities])
            for index in oldEntities:
                print("DBUG:AnalDataSimple:{}:IgnoringOldEntity:{}, {}".format(op, gMeta['names'][index], gMeta['lastSeen'][index]))
        theSaneArray[oldEntities] = iSkip
    theRows=numpy.argsort(theSaneArray)[-numEntities:]
    rowsLen = len(theRows)
    if numEntities > rowsLen:
        print("WARN:AnalDataSimple:{}:RankContenders[{}] < numEntities[{}] requested, adjusting".format(op, rowsLen, numEntities))
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
    print("INFO:AnalDataSimple:{}:{}:{}".format(op, dataSrc, opType))
    for i in range(lStart,lStop,lDelta):
        index = theRows[i]
        if (theSaneArray[index] == iSkip) or ((opType == 'block_ranked') and (theSaneArray[index] == 0)):
            print("    WARN:AnalDataSimple:{}:No more valid elements".format(op))
            break
        curEntry = [gMeta['index2code'][index], gMeta['names'][index], theSaneArray[index]]
        if opType == "roll_avg":
            curEntry.extend(gData[dataSrcMetaData][index,1:])
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
            extra = "{}:{}".format(hlpr.array_str(theRankArray[index],4,"A0L1"), hlpr.array_str(gData[metaDataAvgs][index, iValidBlockAtBegin:],6,2))
        else:
            extra = ""
        print("    {} {}".format(extra, curEntry))
    return theSelected


def infoset1_prep():
    """
    Run a common set of operations, which can be used to get some amount of
    potentially useful info, on the loaded data,
    """
    warnings.filterwarnings('ignore')
    procdata_ex(['srel=srel(data)', 'dma50Srel=dma50(srel)'])
    procdata_ex(['roabs=reton_absret(data)', 'rorpa=reton_retpa(data)'])
    procdata_ex(['roll1095=roll1095(data)', 'dma50Roll1095=dma50(roll1095)'])
    procdata_ex(['roll1825=roll1825(data)', 'dma50Roll1825=dma50(roll1825)'])
    blockDays = int((gMeta['dataIndex']+1)/5)
    procdata_ex(['blockNRoll1095=block{}(roll1095)'.format(blockDays)])
    warnings.filterwarnings('default')


def infoset1_result_entcodes(entCodes, bPrompt=False, numEntries=-1):
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
    dataSrcs = [
            ['srel', 'srelMetaLabel'],
            ['absRet', 'roabsMetaLabel'],
            ['retPA', 'rorpaMetaLabel'],
            ['roll1095', 'roll1095MetaLabel'],
            ['roll1825', 'roll1825MetaLabel'],
            ]
    for entCode in entCodes:
        entIndex = gMeta['code2index'][entCode]
        print("Name:", gMeta['names'][entIndex])
        for dataSrc in dataSrcs:
            print("\t{:16}: {}".format(dataSrc[0], gData[dataSrc[1]][entIndex]))

    dateDuration = (gMeta['dataIndex']+1)/365
    if dateDuration > 1.5:
        dateDuration = 1.5
    print("INFO:dateDuration:", dateDuration)
    analR1095 = analdata_simple('roll1095', 'top', 'roll_avg', entCodes=entCodes, numEntities=len(entCodes), minEntityLifeDataInYears=dateDuration)
    analR1095EntCodes = [ x[0] for x in analR1095 ]
    s1 = set(entCodes)
    s2 = set(analR1095EntCodes)
    otherEntCodes = s1-s2
    analSRelRPA = analdata_simple('srel', 'top', 'srel_retpa', entCodes=otherEntCodes, numEntities=len(otherEntCodes), minEntityLifeDataInYears=dateDuration)
    analSRelRPAEntCodes = [ x[0] for x in analSRelRPA ]
    s3 = set(analSRelRPAEntCodes)
    entCodes = analR1095EntCodes + analSRelRPAEntCodes + list(s1-(s2.union(s3)))

    analdata_simple('blockNRoll1095', 'top', 'block_ranked', entCodes=entCodes, numEntities=len(entCodes), minEntityLifeDataInYears=dateDuration)

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
            entIndex = gMeta['code2index'][entCode]
            entName = gMeta['names'][entIndex][:24]
            if dataSrc[0].startswith('roll'):
                x.append(gData[dataSrcMetaData][entIndex,0])
                y.append(gData[dataSrcMetaData][entIndex,1])
                c.append(entCode)
            print("\t{}:{:24}: {}".format(entCode, entName, gData[dataSrc[1]][entIndex]))
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


def infoset1_result(entTypeTmpls=[], entNameTmpls=[], bPrompt=False, numEntries=20):
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
    entCodes = []
    if (len(entTypeTmpls) == 0) and (len(entNameTmpls) == 0):
        entCodes = list(gMeta['code2index'].keys())
    elif (len(entTypeTmpls) == 0):
        if len(entNameTmpls) > 0:
            fm,pm = search_data(entNameTmpls);
            entCodesMore = [ x[0] for x in fm ]
        else:
            entCodesMore = []
        entCodes = entCodes + entCodesMore
    else:
        entCodes = enttypes.members(entTypeTmpls, entNameTmpls)
    infoset1_result_entcodes(entCodes, bPrompt, numEntries)


def _date2index(startDate, endDate):
    """
    Get the indexes corresponding to the start and end date

    If either of the date is -1, then it will be mapped to
    either the beginning or end of the current valid dataset,
    as appropriate. i.e start maps to 0, end maps to dateIndex.
    """
    if startDate == -1:
        startDateIndex = 0
    else:
        startDateIndex = gMeta['dates'].index(startDate)
    if endDate == -1:
        endDateIndex = gMeta['dataIndex']
    else:
        endDateIndex = gMeta['dates'].index(endDate)
    return startDateIndex, endDateIndex


def _daterange_checkfine(startDateIndex, endDateIndex, caller):
    """
    Check the DateRange specified matches previously saved DateRange.
    Else alert user.
    """
    if gMeta['dateRange'][0] == -1:
        gMeta['dateRange'][0] = startDateIndex
    if gMeta['dateRange'][1] == -1:
        gMeta['dateRange'][1] = endDateIndex
    savedStartDateIndex = gMeta['dateRange'][0]
    savedEndDateIndex = gMeta['dateRange'][1]
    if (savedStartDateIndex != startDateIndex) or (savedEndDateIndex != endDateIndex):
        print("WARN:{}:previously used dateRange:{} - {}".format(caller, gMeta['dates'][savedStartDateIndex], gMeta['dates'][savedEndDateIndex]))
        print("WARN:{}:passed dateRange:{} - {}".format(caller, gMeta['dates'][startDateIndex], gMeta['dates'][endDateIndex]))
        print("INFO:{}:call again with matching dateRange OR".format(caller))
        input("INFO:{}:load_data|show_plot will clear saved dateRange, so that you can lookat a new dateRange that you want to".format(caller))
        return False
    return True


def lookatents_codes(entCodes, startDate=-1, endDate=-1):
    """
    Given a list of MF codes (as in AMFI dataset), look at their data.

    Different representations of the data is plotted for the range of date given,
    provided the corresponding global flags are enabled.

    NOTE: The plot per se is not shown, till user calls show_plot()
    NOTE: DateRange used should match that used in previous calls.
          However load_data and or show_plot will reset dateRange to a clean slate,
          and user will be free again to look at a new date range of their choosing.
    """
    startDateIndex, endDateIndex = _date2index(startDate, endDate)
    if not _daterange_checkfine(startDateIndex, endDateIndex, "lookatents_codes"):
        return
    for code in entCodes:
        index = gMeta['code2index'][code]
        name = gMeta['names'][index]
        aRawData = gData['data'][index, startDateIndex:endDateIndex+1]
        aRelData, aMovAvg, aRollingRet, aStart, aEnd, aAbsRetPercent, aRetPA, durYrs = procdata_relative(aRawData, gbDoMovingAvg, gbDoRollingRet)
        aLabel = "{}: {:6.2f}% {:6.2f}%pa ({:4.1f}Yrs) : {:8.4f} - {:8.4f}".format(code, round(aAbsRetPercent,2), round(aRetPA,2), round(durYrs,1), aStart, aEnd)
        print(aLabel, name)
        if gbDoRawData:
            _plot_data(code, None, aRawData, "{}, {}".format(aLabel,name[:giLabelNameChopLen]), "Raw")
        if gbDoRelData:
            _plot_data(code, None, aRelData, "{}, {}".format(aLabel,name[:giLabelNameChopLen]), "Rel")
        if gbDoMovingAvg:
            if MOVINGAVG_CONVOLVEMODE == 'valid':
                tStartPos = int(MOVINGAVG_WINSIZE/2)
            else:
                tStartPos = 0
            typeTag = "DMA{}".format(MOVINGAVG_WINSIZE)
            _plot_data(code, list(range(tStartPos,len(aMovAvg)+tStartPos)), aMovAvg, "{}, {}".format(aLabel,name[:giLabelNameChopLen]), typeTag)
        if gbDoRollingRet:
            typeTag = "Rol{}".format(ROLLINGRET_WINSIZE)
            _plot_data(code, None, aRollingRet, "{}, {}".format(aLabel,name[:giLabelNameChopLen]), typeTag)


def _plot_data(entCode, xData, yData, label, typeTag):
    theTag = str([entCode, typeTag])
    if theTag in gMeta['plots']:
        print("WARN:_plot_data: Skipping", entCode)
        return
    gMeta['plots'].add(theTag)
    label = "{}:{}".format(label, typeTag)
    if xData == None:
        plt.plot(yData, label=label)
    else:
        plt.plot(xData, yData, label=label)


def _get_daterange_indexes():
    """
    Get the dateRange related indexes stored in gData.

    If there is no valid dateRange info stored in gData, then return
    indexes related to the date range of the currently loaded data.
    """
    iSDate = gMeta['dateRange'][0]
    if iSDate == -1:
        iSDate = 0
    iEDate = gMeta['dateRange'][1]
    if iEDate == -1:
        iEDate = gMeta['dataIndex']
    return iSDate, iEDate


def _show_plot():
    """
    Show the data plotted till now.
    """
    leg = plt.legend()
    plt.setp(leg.texts, family='monospace')
    for line in leg.get_lines():
        line.set_linewidth(8)
    plt.grid(True)
    startDateIndex, endDateIndex = _get_daterange_indexes()
    curDates = gMeta['dates'][startDateIndex:endDateIndex+1]
    numX = len(curDates)
    xTicks = (numpy.linspace(0,1,9)*numX).astype(int)
    xTicks[-1] -= 1
    xTickLabels = numpy.array(curDates)[xTicks]
    plt.xticks(xTicks, xTickLabels, rotation='vertical')
    plt.show()


def show_plot(clearGDataDateRangePlus=True):
    """
    Show the data plotted till now.

    clearGDataDateRangePlus if True, will clear gData dateRange and plots.
    """
    _show_plot()
    if clearGDataDateRangePlus:
        gMeta['dateRange'] = [-1, -1]
        gMeta['plots'] = set()


def lookatents_names(entNames, startDate=-1, endDate=-1):
    """
    Given a list of MF names, look at their data.

    findmatchingmf logic is called wrt each given name in the list.
        All MFs returned as part of its full match list, will be looked at.

    The data is plotted for the range of date given.
    """
    if type(entNames) != list:
        entNames = entNames.split(';')
    entCodes = []
    for name in entNames:
        f,p = findmatchingmf(name)
        for c in f:
            #print(c)
            entCodes.append(c[0])
    lookatents_codes(entCodes, startDate, endDate)


def lookatents_ops(opType="TOP", startDate=-1, endDate=-1, count=10):
    """
    Look for MFs which are at the top or the bottom among all the MFs,
    based on their performance over the date range given.

    opType could be either "TOP" or "BOTTOM"
    startDate and endDate specify the date range to consider.
        should be numerals of the form YYYYMMDD
    count tells how many MFs to list from the top or bottom of the performance list.
    """
    if opType.upper() not in ["TOP", "BOTTOM"]:
        print("ERRR:look4mfs: Unknown operation:", opType)
        return
    startDateIndex, endDateIndex = _date2index(startDate, endDate)
    tData = numpy.zeros([gMeta['nextEntIndex'], (endDateIndex-startDateIndex+1)])
    for r in range(gMeta['nextEntIndex']):
        try:
            tData[r,:], tMovAvg, tRollingRet, tStart, tEnd, tAbsRetPercent, tRetPA, tDurYrs = procdata_relative(gData['data'][r,startDateIndex:endDateIndex+1])
        except:
            traceback.print_exc()
            print("WARN:{}:{}".format(r,gMeta['names'][r]))
    #breakpoint()
    sortedIndex = numpy.argsort(tData[:,-1])
    entCodes = []
    if opType.upper() == "TOP":
        startIndex = -1
        endIndex = startIndex-count
        delta = -1
    elif opType.upper() == "BOTTOM":
        startIndex = 0
        endIndex = startIndex+count
        delta = 1
    for si in range(startIndex, endIndex, delta):
        i = sortedIndex[si]
        entName = gMeta['names'][i]
        entCode = gMeta['index2code'][i]
        entCodes.append(entCode)
        #print("{}: {}:{}".format(i, entCode, entName))
    lookatents_codes(entCodes, startDate, endDate)


def _update_dataproccontrols(dataProcs):
    """
    Save current state of data proc controls, and inturn set them to
    what is specified by the user.
    """
    if dataProcs == None:
        return None
    global gbDoRawData, gbDoRelData, gbDoMovingAvg, MOVINGAVG_WINSIZE, gbDoRollingRet, ROLLINGRET_WINSIZE
    savedDataProcControls = [ gbDoRawData, gbDoRelData, gbDoMovingAvg, MOVINGAVG_WINSIZE, gbDoRollingRet, ROLLINGRET_WINSIZE ]
    gbDoRawData, gbDoRelData, gbDoMovingAvg, gbDoRollingRet = False, False, False, False
    for dp in dataProcs:
        if dp.upper() == "RAW":
            gbDoRawData = True
        elif dp.upper() == "REL":
            gbDoRelData = True
        elif dp.upper().startswith("DMA"):
            gbDoMovingAvg = True
            days = dp.split('_')
            if len(days) > 1:
                MOVINGAVG_WINSIZE = int(days[1])
        elif dp.upper().startswith("ROLL"):
            gbDoRollingRet = True
            days =dp.split('_')
            if len(days) > 1:
                ROLLINGRET_WINSIZE = int(days[1])
    return savedDataProcControls


def _restore_dataproccontrols(savedDataProcControls):
    """
    Restore data proc controls to a previously saved state.
    """
    if savedDataProcControls == None:
        return
    global gbDoRawData, gbDoRelData, gbDoMovingAvg, MOVINGAVG_WINSIZE, gbDoRollingRet, ROLLINGRET_WINSIZE
    [ gbDoRawData, gbDoRelData, gbDoMovingAvg, MOVINGAVG_WINSIZE, gbDoRollingRet, ROLLINGRET_WINSIZE ] = savedDataProcControls


def lookat_data(job, startDate=-1, endDate=-1, count=10, dataProcs=None):
    """
    Look at data of MFs from the currently loaded set.

    Job could either be

        a list of MF name parts like [ "mf name parts 1", "mf name parts 2", ... ]
            a matching MF name should contain all the tokens in any one
            of the match name parts in the list.

            NOTE: if one prefixes -NO- to a token, then the MFName shouldnt contain
            that token as part of its name.

        a string specifying a operation like
            "OP:TOP" - will get the top 10 performing MFs by default.
            "OP:BOTTOM" - will get the bottom 10 performing MFs by default.
            NOTE: absolute return is used to decide the TOP or BOTTOM candidates currently.

    startDate and endDate specify the range of date over which the data should be collated.
    If startDate is -1, then startDate corresponding to the currently loaded data is used.
    If endDate is -1, then endDate corresponding to the currently loaded data is used.

    count specifies how many MFs should be picked for TOP or BOTTOM operations.

    dataProcs can be a list containing one or more of the following string tokens
        "raw" | "rel" | "dma_<NumOfDays>" | "roll_<NumOfDays>"
        This controls which aspects of the data is looked at and inturn plotted.
    """
    savedDataProcControls = _update_dataproccontrols(dataProcs)
    if type(job) == list:
        lookatents_names(job, startDate, endDate)
    else:
        if job.upper() in [ "OP:TOP", "OP:BOTTOM" ]:
            job = job[3:]
            lookatents_ops(job, startDate, endDate, count)
        else:
            print("ERRR:lookat_data: unknown operation:", job)
            print("INFO:lookat_data: If you want to look up MF names put them in a list")
    _restore_dataproccontrols(savedDataProcControls)


def session_save(sessionName):
    """
    Save current gData-gMeta into a pickle, so that it can be restored fast later.
    """
    fName = os.path.join(FINFOOLSERRAND_BASE, "SSN_{}".format(sessionName))
    hlpr.save_pickle(fName, gData, gMeta, "Main:SessionSave")


def session_restore(sessionName):
    """
    Restore a previously saved gData-gMeta fast from a pickle.
    Also setup the modules used by the main logic.
    """
    global gData, gMeta
    fName = os.path.join(FINFOOLSERRAND_BASE, "SSN_{}".format(sessionName))
    ok, gData, gMeta = hlpr.load_pickle(fName)
    enttypes.init(gMeta, False)
    setup_modules()


def input_multi(prompt="OO>", altPrompt="...", theFile=None):
    """
    Allow reading a single line or multiline of python block
    either from console or from the file specified.

    If user is entering a multiline python block, the program
    will show a different prompt to make it easy for the user
    to identify the same.

    Entering a empty line or a line with a smaller indentation
    than that used when multiline block entry started, will
    lead to the logic getting out of the multiline input mode.

    It allows user to split lists, sets, dictionary etc to be
    set across multiple lines, provided there is ',' as the
    last char in the inbetween lines.

    It allows if-else or if-elif-else multiline blocks.
    """
    lines = ""
    bMulti = False
    bIf = False
    lineCnt = 0
    refStartWS = 0
    while True:
        if theFile == None:
            line = input(prompt)
        else:
            line = theFile.readline()
            if line == '':
                theFile=None
                continue
        if prompt != altPrompt:
            prompt = altPrompt
        lineCnt += 1
        lineStripped = line.strip()
        if (lineCnt == 1):
            lines = line
            if (lineStripped != "") and (lineStripped[-1] in ':,\\'):
                if lineStripped.split()[0] == 'if':
                    bIf = True
                bMulti = True
                continue
            else:
                break
        else:
            if lineStripped == "":
                break
            curStartWS = len(line) - len(line.lstrip())
            #print(curStartWS)
            if (lineCnt == 2):
                refStartWS = curStartWS
            lines = "{}\n{}".format(lines,line)
            if (refStartWS > curStartWS):
                if bIf and (lineStripped.split()[0] in [ 'else:', 'elif']):
                    continue
                break
    return lines


gbREPLPrint = True
def do_run(theFile=None):
    """
    Run the REPL logic of this program.
    Read-Eval-Print Loop

    NOTE: If a script file is passed to the logic, it will fall back to
    interactive mode, once there are no more commands in the script file.
        Script file can use quit() to exit the program automatically
        if required.

    NOTE: One can control printing of REPL, by controlling gbREPLPrint.
    NOTE: User can suppress auto printing of individual python statements
            entered into interactive mode by suffixing them with ';'
    """
    bQuit = False
    while not bQuit:
        bPrint = False
        try:
            #breakpoint()
            cmd = input_multi(theFile=theFile)
            if gbREPLPrint:
                cmdStripped = cmd.strip()
                if (cmdStripped != "") and ('\n' not in cmd) and (cmdStripped[-1] != ';'):
                    cmd = "_THE_RESULT = {}".format(cmd)
                    bPrint=True
            exec(cmd,globals())
            if bPrint and (type(_THE_RESULT) != type(None)):
                print(_THE_RESULT)
        except:
            excInfo = sys.exc_info()
            if excInfo[0] == SystemExit:
                break
            traceback.print_exc()


def handle_args():
    """
    Logic to handle the commandline arguments
    """
    if sys.argv[1].endswith(".mf"):
        print("INFO:Running ", sys.argv[1])
        f = open(sys.argv[1])
        do_run(f)
    else:
        fetch_data(sys.argv[1], sys.argv[2])
        load_data(sys.argv[1], sys.argv[2])
        lookat_data("OP:TOP")
        show_plot()


#
# The main flow starts here
#
print("FinFoolsErrandKVC: A stupid exploration of multiple sets of numbers (MFs/Indexes/...) data")
print("License: GPL")
input("PLEASE DO NOT USE THIS PROGRAM TO MAKE ANY DECISIONS OR INFERENCES OR ...")

setup()
if len(sys.argv) > 1:
    handle_args()
else:
    do_run()

