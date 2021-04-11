# Handle the data related operations
# HanishKVC, 2021
# GPL
#

import sys
import calendar
import os
import numpy
import time
import traceback
import readline
import warnings
import hlpr
import datasrc
import india
import entities
import loadfilters
import enttypes as et


gbDEBUG = False

#
# proc_days related controls
#
# Should proc_days process beyond yesterday (i.e into today or future)
gbNotBeyondYesterday = True
# Should proc_days ignore weekends.
gbSkipWeekEnds = False

#
# Misc
#
LOADFILTERSNAME_AUTO = '-AUTO-'



gCal = calendar.Calendar()
gEntDB = None
gDataKeys = ['open', 'high', 'low', 'data', 'volume']
gDataAliases = { 'data': [ 'nav', 'close' ] }
gDS = []
gBasePath = "./ffe.data/"



def setup_gentdb(startDate=-1, endDate=-1):
    """
    Initialise the gEntDB

    NumOfRows (corresponding to Entities) is set to a fixed value.
    NumOfCols (corresponding to Dates) is set based on date range.
    """
    global gEntDB
    numDates = ((int(str(endDate)[:4]) - int(str(startDate)[:4]))+2)*365
    gEntDB = entities.EntitiesDB(gDataKeys, gDataAliases, 8192*4, numDates)


def setup_modules(basePath):
    gDS.append(india.IndiaMFDS(basePath))
    gDS.append(india.IndiaSTKDS(basePath))


def setup(basePath):
    global gBasePath
    gBasePath = basePath
    setup_gentdb()
    setup_modules(basePath)
    loadfilters.list()


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
    for ds in gDS:
        if 'fetch4date' in dir(ds):
            ds.fetch4date(y, m, d, opts)


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
    return fetch4daterange(startDate, endDate, opts)


def load4date(y, m, d, opts):
    """
    Load data for the given date.

    NOTE: This logic wont fill in prev nav for holidays,
    you will have to call fillin4holidays explicitly.
    """
    gEntDB.add_date(hlpr.dateint(y,m,d))
    dataSrcTypeReqd = opts['dataSrcType']
    for ds in gDS:
        if dataSrcTypeReqd != ds.dataSrcType:
            if dataSrcTypeReqd != datasrc.DSType.Any:
                #print("WARN:Load4Date:ReqdDSType[{}], so skipping [{}:{}]...".format(dataSrcTypeReqd, ds.tag, ds.dataSrcType))
                continue
        #print("INFO:Load4Date:ReqdDSType[{}], Loading [{}:{}]...".format(dataSrcTypeReqd, ds.tag, ds.dataSrcType))
        loadFiltersName = opts['loadFiltersName']
        if loadFiltersName == LOADFILTERSNAME_AUTO:
            loadFiltersName = ds.tag
        loadfilters.activate(loadFiltersName)
        if 'load4date' in dir(ds):
            ds.load4date(y, m, d, gEntDB, opts)


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


def load_ftypes(opts):
    """
    Load rarely changing fixed grouping/types if any wrt any of the data sources.
    """
    dataSrcTypeReqd = opts['dataSrcType']
    for ds in gDS:
        if dataSrcTypeReqd != ds.dataSrcType:
            if dataSrcTypeReqd != datasrc.DSType.Any:
                continue
        if 'load_ftypes' in dir(ds):
            ds.load_ftypes(gEntDB, opts)


def load_data(startDate, endDate = None, dataSrcType=datasrc.DSType.Any,
        bClearData=True, bOptimizeSize=True, loadFiltersName=LOADFILTERSNAME_AUTO, opts=None):
    """
    Load data for given date range.

    The dates should follow one of these formats YYYY or YYYYMM or YYYYMMDD i.e YYYY[MM[DD]]

    dataSrcType: Could be datasrc.DSType.Any|DSType.MF|DSType.Stock
        Any  : then both stock or MF data sources are loaded.
        MF   : Only MF related data sources are loaded.
        Stock: Only Stock related data sources are loaded.

    bClearData if set, resets the gEntDB by calling setup_gentdb.

    loadFiltersName: User can optionally specify a previously defined loadFiltersName, in
    which case the whiteListEntTypes/whiteListEntNames/blackListEntNames, used by underlying
    load logic, if any, will be set as defined by the given loadFiltersName.

        If this argument is not specified (i.e LOADFILTERSNAME_AUTO) , then the loadFilters
        suggested by individual data sources, will be used wrt their load logic.
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
        setup_gentdb(startDate, endDate)
    for ds in gDS:
        ds.listNoDataDates = []
    if opts == None:
        opts = {}
    if 'LoadLocalOnly' not in opts:
        opts['LoadLocalOnly'] = True
    if 'loadFiltersName' not in opts:
        opts['loadFiltersName'] = loadFiltersName
    if 'dataSrcType' not in opts:
        opts['dataSrcType'] = dataSrcType
    load4daterange(startDate, endDate, opts)
    if bOptimizeSize:
        gEntDB.optimise_size(gDataKeys)
    load_ftypes(opts)
    for ds in gDS:
        if len(ds.listNoDataDates) > 0:
            print("WARN:LoadData:{}:Data missing for {}".format(ds.tag, ds.listNoDataDates))


def load_data_mfs(startDate, endDate = None, dataSrcType=datasrc.DSType.MF,
        bClearData=True, bOptimizeSize=True, loadFiltersName=LOADFILTERSNAME_AUTO, opts=None):
    load_data(startDate, endDate, dataSrcType, bClearData, bOptimizeSize, loadFiltersName, opts)


def load_data_stocks(startDate, endDate = None, dataSrcType=datasrc.DSType.Stock,
        bClearData=True, bOptimizeSize=True, loadFiltersName=LOADFILTERSNAME_AUTO, opts=None):
    load_data(startDate, endDate, dataSrcType, bClearData, bOptimizeSize, loadFiltersName, opts)


def _fillin4holidays(entIndex=-1):
    """
    As there may not be any data for holidays including weekends,
    so fill them with the data from the prev working day for the corresponding entity.
    """
    for key in gDataKeys:
        lastData = -1
        for c in range(gEntDB.nxtDateIndex):
            if gEntDB.data[key][entIndex,c] == 0:
                if lastData > 0:
                    gEntDB.data[key][entIndex,c] = lastData
            else:
                lastData = gEntDB.data[key][entIndex,c]


def fillin4holidays():
    """
    As there may not be any data for holidays including weekends,
    so fill them with the data from the prev working day for the corresponding entity.
    """
    for r in range(gEntDB.nxtEntIndex):
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
    fm, pm = _findmatching(entNameTmpl, gEntDB.meta['name'], fullMatch, partialTokens, ignoreCase)
    #breakpoint()
    fmNew = []
    for curName, curIndex in fm:
        fmNew.append([gEntDB.meta['codeL'][curIndex], curName, curIndex])
    pmNew = []
    for curName, curIndex in pm:
        pmNew.append([gEntDB.meta['codeL'][curIndex], curName, curIndex])
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


def list_enttypes(entTypeTmpls=et.TYPE_MATCH_ALL):
    """
    Get list of entTypes in the entities db.
    """
    gEntDB.list_types(entTypeTmpls)


def list_enttype_members(entTypeTmpls=et.TYPE_MATCH_ALL, entNameTmpls=et.NAME_MATCH_ALL):
    """
    Get list of matching entities in the matching entTypes.
    """
    gEntDB.list_type_members(entTypeTmpls, entNameTmpls)


def session_save(sessionName):
    """
    Save current gEntDB.data-gEntDB.meta into a pickle, so that it can be restored fast later.
    """
    fName = os.path.join(gBasePath, "SSN_{}".format(sessionName))
    hlpr.save_pickle(fName, gEntDB, None, "Data:SessionSave")


def session_restore(sessionName):
    """
    Restore a previously saved gEntDB.data-gEntDB.meta fast from a pickle.
    Also setup the modules used by the main logic.
    """
    global gEntDB
    fName = os.path.join(gBasePath, "SSN_{}".format(sessionName))
    ok, gEntDB, tIgnore = hlpr.load_pickle(fName)


fetch=fetch_data
load=load_data
search=search_data
load_mfs=load_data_mfs
load_stocks=load_data_stocks
enttypes=list_enttypes
enttype_members=list_enttype_members


