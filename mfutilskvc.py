#!/usr/bin/env python3
# Get and work with MF nav data
# HanishKVC, 2021
# GPL
#

import sys
import calendar
import os
import datetime
import numpy
import matplotlib.pyplot as plt
import time
import traceback
import readline
import tabcomplete as tc
import hlpr


"""

Usage scenario

Old/Low level:
    fetch4daterange(2015, 202102)
    load4daterange(2015, 2020)
    Older:
        #fillin4holidays()
        f,p = findmatchingmf("token1 token2")
        [print(x) for x in f]
        plt.plot(gData['data'][mfIndex])
        plt.show()
    InBetween:
        lookatmfs_names("mf name parts")
        lookatmfs_names("mf name parts 1; mf name parts 2; mf name parts 3")
        lookatmfs_names(["mf name parts 1", "mf name parts 2"])
        look4mfs()
        look4mfs("TOP", 20150101, 20210228)
Newer:
    fetch_data(2010, 202103)
    load_data(2013, 20190105)
    lookat_data("OP:TOP")
    lookat_data(["us direct", "hybrid direct abc"])
    show_plot()
    lookat_data(["another mf or mfs", "related mfs"], dataProcs=[ "raw", "rel", "dma"])
    show_plot()
    quit()
TODO:
    20 day, 50 day line, 10 week line, 200 day (Moving averages(simple, exponential))
    52 week high/low,
    Extract fund type from Nav data file
    Negative tokens while matching
"""

gbDEBUG=False
# The tokens in the SKIP_NAMETOKENS list will be matched against MFName,
# and matching MFs will be silently ignored while loading the MF data.
MF_ALLOW_MFTYPETOKENS = [ "equity", "other", "hybrid", "solution" ]
MF_ALLOW_MFNAMETOKENS = None
MF_SKIP_MFNAMETOKENS = [ "~PART~dividend" ]

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
# Fetching and Saving related
#
MFS_FNAMECSV_TMPL = "data/{}{:02}{:02}.csv"
#https://www.amfiindia.com/spages/NAVAll.txt?t=27022021
#http://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?frmdt=01-Feb-2021
MFS_BaseURL = "http://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?frmdt={}-{}-{}"

#
# DATA URLs TOCHECK
#
## Index historic data
INDEX_BSESENSEX_URL = "https://api.bseindia.com/BseIndiaAPI/api/ProduceCSVForDate/w?strIndex=SENSEX&dtFromDate=01/01/2011&dtToDate=05/03/2021"

#
# Misc
#
giLabelNameChopLen = 36



gCal = calendar.Calendar()
gData = {}


def setup_paths():
    """
    Account for MFUTILS_BASE env variable if set
    """
    global MFS_FNAMECSV_TMPL
    MFS_FNAMECSV_TMPL = os.path.expanduser(os.path.join(os.environ.get('MFUTILS_BASE',"~/"), MFS_FNAMECSV_TMPL))
    print("INFO:setup_paths:", MFS_FNAMECSV_TMPL)


def setup_gdata(startDate=-1, endDate=-1):
    """
    Initialise the gData dictionary

    NumOfRows (corresponding to MFs) is set to a fixed value.
    NumOfCols (corresponding to Dates) is set based on date range.
    """
    numDates = ((int(str(endDate)[:4]) - int(str(startDate)[:4]))+2)*365
    gData.clear()
    gData['code2index'] = {}
    gData['index2code'] = {}
    gData['data'] = numpy.zeros([8192*4, numDates])
    gData['nextMFIndex'] = 0
    gData['dateIndex'] = -1
    gData['names'] = []
    gData['dates'] = []
    gData['skipped'] = set()
    gData['dateRange'] = [-1, -1]
    gData['plots'] = set()
    gData['mfTypes'] = {}
    gData['metas'] = {}
    gData['lastSeen'] = numpy.zeros(8192*4, dtype=numpy.int32)


def setup():
    tc.gData = gData
    setup_gdata()
    setup_paths()
    loadfilters_set(MF_ALLOW_MFTYPETOKENS, MF_ALLOW_MFNAMETOKENS, MF_SKIP_MFNAMETOKENS)
    print("INFO:setup:gNameCleanupMap:", gNameCleanupMap)


gNameCleanupMap = [
        ['-', ' '],
        ['Divided', 'Dividend'],
        ['Diviend', 'Dividend'],
        ['Divdend', 'Dividend'],
        ]
def string_cleanup(theString, cleanupMap):
    """
    Use the given cleanup map to replace elements of the passed string.
    """
    for cm in cleanupMap:
        theString = theString.replace(cm[0], cm[1])
    return theString


def dateint(y, m, d):
    """
    Convert year, month and day into a numeric YYYYMMDD format.
    """
    return y*10000+m*100+d


def proc_days(start, end, handle_date_func, bNotBeyondYesterday=True):
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
            print("INFO:proc_days:handlingmonth:{}{:02}:{} to {}".format(y,m,startDate, endDate))
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
                print("INFO:proc_days:handledate:{}{:02}{:02}".format(y,m,d))
                try:
                    handle_date_func(y,m,d)
                except:
                    traceback.print_exc()


def fetch4date(y, m, d):
    """
    Fetch data for the given date.

    This is the default handler function passed to proc_days.

    One can call this directly by passing the year, month and date one is interested in.
        month should be one of 1 to 12
        day (month day) should be one of 1 to 31, as appropriate for month specified.
    """
    print(y,m,d)
    url = MFS_BaseURL.format(d,calendar.month_name[m][:3],y)
    fName = MFS_FNAMECSV_TMPL.format(y,m,d)
    print(url, fName)
    hlpr.wget_better(url, fName)
    f = open(fName)
    l = f.readline()
    if not l.startswith("Scheme Code"):
        print("ERRR:fetch4date:Not a valid nav file, removing it")
        os.remove(fName)
    f.close()


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


def fetch4daterange(startDate, endDate):
    """
    Fetch data for given date range.

    The dates should follow one of these formats YYYY or YYYYMM or YYYYMMDD i.e YYYY[MM[DD]]
    """
    start, end = proc_date_startend(startDate, endDate)
    proc_days(start, end, fetch4date, gbNotBeyondYesterday)


def fetch_data(startDate, endDate=None):
    """
    Fetch data for a given date or range of dates

    If only startDate is given, then endDate is assumed to be same as startDate.
    This is useful to fetch a full year or a full month of data, where one gives
    only the YYYY or YYYYMM as the startDate, then the logic across the call
    chain will ensure that starts correspond to 1 and ends correspond to 12 or
    31, as the case may be..
    """
    if endDate == None:
        endDate = startDate
    return fetch4daterange(startDate, endDate)


def parse_csv(sFile):
    """
    Parse the specified data csv file and load it into global data dictionary.

    NOTE: It uses the white and black lists wrt MFTypes and MFNames, if any
    specified in gData, to decide whether a given MF should be added to the
    dataset or not. User can control this in the normal usage flow by either
    passing these lists explicitly to load_data and or by setting related
    global variables before calling load_data.
    """
    tFile = open(sFile)
    curMFType = ""
    bSkipCurMFType = False
    for l in tFile:
        l = l.strip()
        if l == '':
            continue
        if l[0].isalpha():
            #print("WARN:parse_csv:Skipping:{}".format(l))
            if l[-1] == ')':
                curMFType = l
                if curMFType not in gData['mfTypes']:
                    gData['mfTypes'][curMFType] = []
                if gData['whiteListMFTypes'] == None:
                    bSkipCurMFType = False
                else:
                    #breakpoint()
                    fm,pm = matches_templates(curMFType, gData['whiteListMFTypes'])
                    if len(fm) == 0:
                        bSkipCurMFType = True
                    else:
                        bSkipCurMFType = False
            continue
        if bSkipCurMFType:
            continue
        try:
            la = l.split(';')
            code = int(la[0])
            name = string_cleanup(la[1], gNameCleanupMap)
            if (gData['whiteListMFNames'] != None):
                fm, pm = matches_templates(name, gData['whiteListMFNames'])
                if len(fm) == 0:
                    gData['skipped'].add(str([code, name]))
                    continue
            if (gData['blackListMFNames'] != None):
                fm, pm = matches_templates(name, gData['blackListMFNames'])
                if len(fm) > 0:
                    gData['skipped'].add(str([code, name]))
                    continue
            try:
                nav  = float(la[4])
            except:
                nav = 0
            date = datetime.datetime.strptime(la[7], "%d-%b-%Y")
            date = dateint(date.year,date.month,date.day)
            #print(code, name, nav, date)
            mfIndex = gData['code2index'].get(code, None)
            if mfIndex == None:
                mfIndex = gData['nextMFIndex']
                gData['nextMFIndex'] += 1
                gData['code2index'][code] = mfIndex
                gData['index2code'][mfIndex] = code
                gData['names'].append(name)
                gData['mfTypes'][curMFType].append(code)
            else:
                if name != gData['names'][mfIndex]:
                    input("WARN:parse_csv:Name mismatch?:{} != {}".format(name, gData['names'][mfIndex]))
            gData['data'][mfIndex,gData['dateIndex']] = nav
            gData['lastSeen'][mfIndex] = date
        except:
            print("ERRR:parse_csv:{}".format(l))
            print(sys.exc_info())
    tFile.close()


def print_skipped():
    """
    Print the skipped list of MFs, so that user can cross verify, things are fine.
    """
    msg = "WARN: About to print the list of SKIPPED/Filtered out MFs"
    if gbDEBUG:
        input("{}, press any key...".format(msg))
    else:
        print(msg)
    for skippedMF in gData['skipped']:
        print(skippedMF)
    print("WARN: The above MFs were skipped/filtered out when loading")


def load4date(y, m, d):
    """
    Load data for the given date.

    NOTE: This logic wont fill in prev nav for holidays,
    you will have to call fillin4holidays explicitly.
    """
    gData['dateIndex'] += 1
    gData['dates'].append(dateint(y,m,d))
    fName = MFS_FNAMECSV_TMPL.format(y,m,d)
    parse_csv(fName)


def load4daterange(startDate, endDate):
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
        proc_days(start, end, load4date, gbNotBeyondYesterday)
    except:
        excInfo = sys.exc_info()
        print(excInfo)
    fillin4holidays()
    if gbDEBUG:
        print_skipped()


gWhiteListMFTypes = None
gWhiteListMFNames = None
gBlackListMFNames = None


def loadfilters_set(whiteListMFTypes=None, whiteListMFNames=None, blackListMFNames=None):
    """
    Setup global filters used by load logic.

    If whiteListMFTypes is set, loads only MFs which belong to the given MFType.
    If whiteListMFNames is set, loads only MFs whose name matches any one of the given match templates.
    If blackListMFNames is set, loads only MFs whose names dont match any of the corresponding match templates.

    NOTE: Call load_filter with required filters, before calling load_data.
    """
    global gWhiteListMFTypes, gWhiteListMFNames, gBlackListMFNames
    if whiteListMFTypes != None:
        gWhiteListMFTypes = whiteListMFTypes
    if whiteListMFNames != None:
        gWhiteListMFNames = whiteListMFNames
    if blackListMFNames != None:
        gBlackListMFNames = blackListMFNames
    print("LoadFiltersSet:Global Filters:\n\tgWhiteListMFTypes {}\n\tgWhiteListMFNames {}\n\tgBlackListMFNames {}".format(gWhiteListMFTypes, gWhiteListMFNames, gBlackListMFNames))


def loadfilters_clear():
    """
    Clear any global white/blacklist filters setup wrt load operation.
    """
    global gWhiteListMFTypes, gWhiteListMFNames, gBlackListMFNames
    gWhiteListMFTypes = None
    gWhiteListMFNames = None
    gBlackListMFTypes = None
    print("LoadFiltersClear:Global Filters Cleared:\n\tgWhiteListMFTypes {}\n\tgWhiteListMFNames {}\n\tgBlackListMFNames {}".format(gWhiteListMFTypes, gWhiteListMFNames, gBlackListMFNames))


def load_data(startDate, endDate = None, bClearData=True, whiteListMFTypes=None, whiteListMFNames=None, blackListMFNames=None, bOptimizeSize=True):
    """
    Load data for given date range.

    The dates should follow one of these formats YYYY or YYYYMM or YYYYMMDD i.e YYYY[MM[DD]]

    bClearData if set, resets the gData by calling setup_gdata.

    User can optionally specify whiteListMFTypes/whiteListMFNames/blackListMFNames.

        if any of them is set to a list, the same will be written into gData.
        if any of them is set to None, then the logic will check to see, if
            there is a corresponding global list available or not.
            If available, the same will be written into gData.
                gWhiteListMFTypes, gWhiteListMFNames, gBlackListMFNames.
            Else the corresponding key in gData will point to None.

    These are used by the underlying parse and load logic, as follows.

        if gData['whiteListMFTypes'] is a valid list (i.e not None), then the logic will load
        only MFs, which belong to a MFType which has matching tokens corresponding to all the
        tokens in one of the strings in the passed list. Else MFs are not filtered based on
        MFType.

        if gData['whiteListMFNames'] is a valid list (i.e not None), then the logic will load
        only MFs, whose name contain matching tokens corresponding to all the tokens in one of
        the strings in the passed list.

        if gData['blackListMFNames'] is a valid list (i.e not None), then the logic will load
        only MFs, whose name dont contain matching tokens corresponding to all the tokens in
        any of the strings in the passed list.

        NONE-NONE-NONE : All MFs will be loaded.
        LIST-NONE-NONE : All MFs which belong to any of the MFTypes specified, will be loaded.
        NONE-LIST-NONE : MFs whose name match with a template in MFNames whitelist, will be loaded.
        NONE-NONE-LIST : MFs whose name match with a template in MFNames blacklist, will be skipped.
        NONE-LIST-LIST : MFs whose name match a template in MFNames whitelist, and doesnt match any
                         of the templates in the MFNames blacklist, will be loaded.
        ....

        NOTE: The _findmatching logic will be used for matching templates.

    bOptimizeSize if set, resizes the data array to be only as big as actual loaded data.

    NOTE: This logic takes care of filling in nav values for holidays
    automatically by calling fillin4holidays.

    NOTE: If we dont have csv files for some of the dates specified, in the date range,
    the load4daterange logic ensures that data for dates for which csv exists will be loaded.
    """
    if endDate == None:
        endDate = startDate
    if bClearData:
        setup_gdata(startDate, endDate)
    if whiteListMFTypes == None:
        whiteListMFTypes = gWhiteListMFTypes
    if whiteListMFNames == None:
        whiteListMFNames = gWhiteListMFNames
    if blackListMFNames == None:
        blackListMFNames = gBlackListMFNames
    gData['whiteListMFTypes'] = whiteListMFTypes
    gData['whiteListMFNames'] = whiteListMFNames
    gData['blackListMFNames'] = blackListMFNames
    load4daterange(startDate, endDate)
    if bOptimizeSize:
        gData['data'] = gData['data'][:gData['nextMFIndex'],:gData['dateIndex']+1]
        gData['lastSeen'] = gData['lastSeen'][:gData['nextMFIndex']]


def mftypes_list():
    """
    List MFTypes found in currently loaded data.
    """
    for k in gData['mfTypes']:
        print(k)


def mftypes_members(mfType):
    """
    List the members of the specified MFType
    """
    print("INFO:mfTypesMembers:", mfType)
    for m in gData['mfTypes'][mfType]:
        print(m, gData['names'][gData['code2index'][m]])


def _fillin4holidays(mfIndex=-1):
    """
    As there wont be any Nav data for holidays including weekends,
    so fill them with the nav from the prev working day for the corresponding mf.
    """
    lastNav = -1
    for c in range(gData['dateIndex']+1):
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
    for r in range(gData['nextMFIndex']):
        _fillin4holidays(r)


def matches_templates(theString, matchTemplates, fullMatch=False, partialTokens=False, ignoreCase=True):
    """
    Find match templates which are satisfied by the given string.

    If fullMatch is True, then checks for a full match, else
    it tries to find strings in its dataset, which contain
    some or all of the tokens in the given searchTmpl.

    If partialTokens is True, then tokens in the given searchTmpl
    could appear as part of bigger token in strings in its
    dataset. Else the token in given searchTmpl and token in the
    strings in the dataset should match fully at the individual
    token level.

    if ignoreCase is True, then case of the given searchTmpl,
    is ignored while trying to find a match.

    It returns fullMatch index list which contains index of all templates,
    which match the given string wrt all the tokens in it, as well as a
    partialMatch index list which contains the index of all templates,
    which only match some of the tokens given in it wrt the given string.

    NOTE: One can prefix any token in a matchTemplate with -NO-, if such
    a token is found to be present in the given string, the string wont
    match wrt the corresponding matchTemplate.

    NOTE: One can prefix any token in the matchTemplate with ~PART~,
    in which case such a token can occur either independently or as
    part of a bigger word in the given string, to trigger a match.

    NOTE: If you want to mix NO and PART, do it in that order
    i.e -NO-~PART~TheToken.
    """
    theString_asis = theString
    if ignoreCase:
        theString = theString.upper()
    matchTmplFullMatch = []
    matchTmplPartMatch = []
    tmplIndex = -1
    #breakpoint()
    for matchTmpl in matchTemplates:
        if ignoreCase:
            matchTmpl = matchTmpl.upper()
        tmplIndex += 1
        if fullMatch:
            if theString == matchTmpl:
                matchTmplFullMatch.append([theString_asis, tmplIndex])
            continue
        matchTmplTokens = matchTmpl.split()
        bSkip = False
        noTokenCnt = 0
        matchCnt = 0
        for token in matchTmplTokens:
            if token.startswith("-NO-"):
                token = token[4:]
                bNoFlag=True
                noTokenCnt += 1
            else:
                bNoFlag=False
            if token.startswith("~PART~"):
                token = token[6:]
                bPartialTokenMatch = True
            else:
                bPartialTokenMatch = False
            if partialTokens:
                bPartialTokenMatch = True
            if bPartialTokenMatch:
                if theString.find(token) != -1:
                    if bNoFlag:
                        bSkip = True
                    else:
                        matchCnt += 1
            else:
                if token in theString.split():
                    if bNoFlag:
                        bSkip = True
                    else:
                        matchCnt += 1
        if bSkip:
            continue
        if matchCnt == (len(matchTmplTokens) - noTokenCnt):
            matchTmplFullMatch.append([theString_asis, tmplIndex])
        elif matchCnt > 0:
            matchTmplPartMatch.append([theString_asis, tmplIndex])
    return matchTmplFullMatch, matchTmplPartMatch


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
        fm, pm = matches_templates(curName, [searchTmpl], fullMatch, partialTokens, ignoreCase)
        if (len(fm) > 0):
            searchTmplFullMatch.append([curName, namesIndex])
        if (len(pm) > 0):
            searchTmplPartMatch.append([curName, namesIndex])
    return searchTmplFullMatch, searchTmplPartMatch


def findmatchingmf(mfNameTmpl, fullMatch=False, partialTokens=False, ignoreCase=True):
    """
    Find MFs from the MF dataSet, which match the given mfName Template.

    NOTE: look at help of _findmatching for the search/matching behaviour.
    """
    fm, pm = _findmatching(mfNameTmpl, gData['names'], fullMatch, partialTokens, ignoreCase)
    #breakpoint()
    fmNew = []
    for curName, curIndex in fm:
        fmNew.append([curName, gData['index2code'][curIndex], curIndex])
    pmNew = []
    for curName, curIndex in pm:
        pmNew.append([curName, gData['index2code'][curIndex], curIndex])
    return fmNew, pmNew


def search_data(findName, bFullMatch=False, bPartialTokens=False, bIgnoreCase=True, bPrintAllTokenMatch=True, bPrintSomeTokenMatch=False):
    """
    Search/Find if there are any MFs which match the given name parts in findName.

    bPrintAllTokenMatch: If enabled prints MFs which match all the tokens in the specified findName.
    bPrintSomeTokenMatch: If enabled prints MFs even if they match only some of the tokens in the specified findName.
    """
    f,p = findmatchingmf(findName, bFullMatch, bPartialTokens, bIgnoreCase)
    if bPrintAllTokenMatch:
        print("INFO:search_data: List of All tokens Match")
        for n in f:
            print(n)
    if bPrintSomeTokenMatch:
        print("INFO:search_data: List of Some tokens Match")
        for n in p:
            print(n)


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
def procdata_ex(opsList, startDate=-1, endDate=-1):
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
        "rel[<BaseDate>]": calculate absolute return ration across the full date range wrt the
                val corresponding to the given baseDate.
                If BaseDate is not given, then startDate will be used as the baseDate.
        "dma<DAYSInINT>": Calculate a moving average across the full date range, with a windowsize
                of DAYSInINT. It sets the Partial calculated data regions at the beginning and end
                of the dateRange to NaN (bcas there is not sufficient data to one of the sides,
                in these locations, so the result wont be proper, so force it to NaN).
        "roll<DAYSInINT>[_abs]": Calculate a rolling return rate across the full date range, with a
                windowsize of DAYSInINT. Again the region at the begining of the dateRange, which
                cant satisfy the windowsize to calculate the rolling return rate, will be set to
                NaN.
                If _abs is specified, it calculates absolute return. If Not (i.e by default)
                it calculates the ReturnPerAnnum.

    NOTE: NaN is used, because plot will ignore those data points and keep the corresponding
    verticals blank.

    NOTE: If no Destination data key is specified, then it is constructed using the template

        '<OP>(<DataSrc>[<startDate>:<endDate>])'
    """
    startDateIndex, endDateIndex = _date2index(startDate, endDate)
    if not _daterange_checkfine(startDateIndex, endDateIndex, "procdata_ex"):
        return
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
        dataDstMetaData, dataDstMetaLabel = datadst_metakeys(dataDst)
        gData[dataDstMetaLabel] = []
        if op == 'srel':
            gData[dataDstMetaData] = numpy.zeros([gData['nextMFIndex'],3])
        update_metas(op, dataSrc, dataDst)
        for r in range(gData['nextMFIndex']):
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
                    baseDateIndex = gData['dates'].index(baseDate)
                    baseData = gData[dataSrc][r, baseDateIndex]
                else:
                    baseData = gData[dataSrc][r, startDateIndex]
                tResult[r,:] = (((gData[dataSrc][r,:])/baseData)-1)*100
            elif op.startswith("dma"):
                days = int(op[3:])
                tResult[r,:] = numpy.convolve(gData[dataSrc][r,:], numpy.ones(days)/days, 'same')
                inv = int(days/2)
                tResult[r,:inv] = numpy.nan
                tResult[r,gData['dateIndex']-inv:] = numpy.nan
            elif op.startswith("roll"):
                days = int(op[4:])
                durationForPA = days/365
                if '_' in op:
                    op,opType = op.split('_')
                    if opType == 'abs':
                        durationForPA = 1
                if gbRelDataPlusFloat:
                    tResult[r,days:] = (gData[dataSrc][r,days:]/gData[dataSrc][r,:-days])**(1/durationForPA)
                else:
                    tResult[r,days:] = (((gData[dataSrc][r,days:]/gData[dataSrc][r,:-days])**(1/durationForPA))-1)*100
                tResult[r,:days] = numpy.nan
        gData[dataDst] = tResult


def plot_data(dataSrcs, mfCodes, startDate=-1, endDate=-1):
    """
    Plot specified datas for the specified MFs, over the specified date range.

    dataSrcs: Is a key or a list of keys used to retreive the data from gData.
    mfCodes: Is a mfCode or a list of mfCodes.
    startDate and endDate: specify the date range over which the data should be
        retreived and plotted.

    Remember to call plt.plot or show_plot, when you want to see the plots,
    accumulated till that time.
    """
    startDateIndex, endDateIndex = _date2index(startDate, endDate)
    if type(dataSrcs) == str:
        dataSrcs = [ dataSrcs ]
    if type(mfCodes) == int:
        mfCodes = [ mfCodes]
    for dataSrc in dataSrcs:
        dataSrcMetaData, dataSrcMetaLabel = datadst_metakeys(dataSrc)
        for mfCode in mfCodes:
            index = gData['code2index'][mfCode]
            name = gData['names'][index][:giLabelNameChopLen]
            try:
                dataLabel = gData[dataSrcMetaLabel][index]
            except:
                srelMetaData, srelMetaLabel = datadst_metakeys('srel')
                metaKey = gData['metas'].get(srelMetaLabel, None)
                if metaKey != None:
                    dataLabel = gData[metaKey][index]
                else:
                    dataLabel = ""
            label = "{}:{:{width}}: {:16} : {}".format(mfCode, name, dataSrc, dataLabel, width=giLabelNameChopLen)
            print("DBUG:plot_data:{}:{}".format(label, index))
            plt.plot(gData[dataSrc][index, startDateIndex:endDateIndex+1], label=label)


def analdata_simple(dataSrc, op, opType='normal', theDate=None, numEntries=10, bIgnoreLessThanAYear=True, bCurrentEntitiesOnly=True, bDebug=False):
    """
    Find the top/bottom N entities, [wrt the given date,] from the given dataSrc.

    op: could be either 'top' or 'bottom'

    opType: could be one of 'normal', 'srel_absret', 'srel_retpa'

        normal: Look at data corresponding to the identified date,
        in the given dataSrc, to decide on entities to select.

        srel_absret: Look at the Absolute Returns data associated
        with the given dataSrc (which should be generated using
        srel procdata_ex operation), to decide on entities.

        srel_retpa: Look at the Returns PerAnnum data associated
        with the given dataSrc (which should be generated using
        srel procdata_ex operation), to decide on entities.

    theDate:
        If None, then the logic will try to find a date
        which contains atleast some valid data, starting from the
        lastDate and moving towards startDate wrt given dataSrc.

        NOTE: ValidData: Any Non Zero, Non NaN, Non Inf data

        If -1, then the lastDate wrt the currently loaded dataset,
        is used as the date from which values should be used to
        identify the entities.

        A date in YYYYMMDD format.

    bIgnoreLessThanAYear: srel related operations, will ignore entities
        who have been in existance of less than a year, AND OR if we
        have data for only less than a year for the entity.

        NOTE: If you have loaded less than a year of data, then remember
        to set this to False, if required.

    bCurrentEntitiesOnly: Will drop entities which have not been seen
        in the last 1 week, wrt the dateRange currently loaded.

    TODO: Currently any entities to be ignored are set to a value of 0,
    which is fine for top operation, but is a disaster for bottom operation.

    """
    if op == 'top':
        iSkip = -numpy.inf
    else:
        iSkip = numpy.inf
    if opType == 'normal':
        if type(theDate) == type(None):
            for i in range(-1, -(gData['dateIndex']+1),-1):
                if bDebug:
                    print("DBUG:AnalDataSimple:{}:findDateIndex:{}".format(op, i))
                theSaneArray = gData[dataSrc][:,i].copy()
                theSaneArray[numpy.isinf(theSaneArray)] = iSkip
                theSaneArray[numpy.isnan(theSaneArray)] = iSkip
                if numpy.count_nonzero(theSaneArray) > 0:
                    dateIndex = gData['dateIndex']+1+i
                    print("INFO:AnalDataSimple:{}:DateIndex:{}".format(op, dateIndex))
                    break
        else:
            startDateIndex, dateIndex = _date2index(theDate, theDate)
            print("INFO:AnalDataSimple:{}:DateIndex:{}".format(op, dateIndex))
            theSaneArray = gData[dataSrc][:,dateIndex].copy()
            theSaneArray[numpy.isinf(theSaneArray)] = iSkip
            theSaneArray[numpy.isnan(theSaneArray)] = iSkip
    elif opType.startswith("srel"):
        dataSrcMetaData, dataSrcMetaLabel = datadst_metakeys(dataSrc)
        if opType == 'srel_absret':
            theSaneArray = gData[dataSrcMetaData][:,0].copy()
        elif opType == 'srel_retpa':
            theSaneArray = gData[dataSrcMetaData][:,1].copy()
        if bIgnoreLessThanAYear:
            theSaneArray[gData[dataSrcMetaData][:,2] < 1.0] = iSkip
    if bCurrentEntitiesOnly:
        oldEntities = numpy.nonzero(gData['lastSeen'] < (gData['dates'][gData['dateIndex']]-7))[0]
        if bDebug:
            #aNames = numpy.array(gData['names'])
            #print(aNames[oldEntities])
            for index in oldEntities:
                print("DBUG:AnalDataSimple:{}:IgnoringOldEntity:{}, {}".format(op, gData['names'][index], gData['lastSeen'][index]))
        theSaneArray[oldEntities] = iSkip
    theRows=numpy.argsort(theSaneArray)[-numEntries:]
    if op == 'top':
        lStart = -1
        lStop = -(numEntries+1)
        lDelta = -1
    elif op == 'bottom':
        lStart = 0
        lStop = numEntries
        lDelta = 1
    theSelected = []
    for i in range(lStart,lStop,lDelta):
        index = theRows[i]
        curEntry = [gData['index2code'][index], gData['names'][index], theSaneArray[index]]
        theSelected.append(curEntry)
        print("INFO:AnalDataSimple:{}:{}:{}".format(op, dataSrc, curEntry))
    return theSelected


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
        startDateIndex = gData['dates'].index(startDate)
    if endDate == -1:
        endDateIndex = gData['dateIndex']
    else:
        endDateIndex = gData['dates'].index(endDate)
    return startDateIndex, endDateIndex


def _daterange_checkfine(startDateIndex, endDateIndex, caller):
    """
    Check the DateRange specified matches previously saved DateRange.
    Else alert user.
    """
    if gData['dateRange'][0] == -1:
        gData['dateRange'][0] = startDateIndex
    if gData['dateRange'][1] == -1:
        gData['dateRange'][1] = endDateIndex
    savedStartDateIndex = gData['dateRange'][0]
    savedEndDateIndex = gData['dateRange'][1]
    if (savedStartDateIndex != startDateIndex) or (savedEndDateIndex != endDateIndex):
        print("WARN:{}:previously used dateRange:{} - {}".format(caller, gData['dates'][savedStartDateIndex], gData['dates'][savedEndDateIndex]))
        print("WARN:{}:passed dateRange:{} - {}".format(caller, gData['dates'][startDateIndex], gData['dates'][endDateIndex]))
        print("INFO:{}:call again with matching dateRange OR".format(caller))
        input("INFO:{}:load_data|show_plot will clear saved dateRange, so that you can lookat a new dateRange that you want to".format(caller))
        return False
    return True


def lookatmfs_codes(mfCodes, startDate=-1, endDate=-1):
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
    if not _daterange_checkfine(startDateIndex, endDateIndex, "lookatmfs_codes"):
        return
    for code in mfCodes:
        index = gData['code2index'][code]
        name = gData['names'][index]
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


def _plot_data(mfCode, xData, yData, label, typeTag):
    theTag = str([mfCode, typeTag])
    if theTag in gData['plots']:
        print("WARN:_plot_data: Skipping", mfCode)
        return
    gData['plots'].add(theTag)
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
    iSDate = gData['dateRange'][0]
    if iSDate == -1:
        iSDate = 0
    iEDate = gData['dateRange'][1]
    if iEDate == -1:
        iEDate = gData['dateIndex']
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
    curDates = gData['dates'][startDateIndex:endDateIndex+1]
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
        gData['dateRange'] = [-1, -1]
        gData['plots'] = set()


def lookatmfs_names(mfNames, startDate=-1, endDate=-1):
    """
    Given a list of MF names, look at their data.

    findmatchingmf logic is called wrt each given name in the list.
        All MFs returned as part of its full match list, will be looked at.

    The data is plotted for the range of date given.
    """
    if type(mfNames) != list:
        mfNames = mfNames.split(';')
    mfCodes = []
    for name in mfNames:
        f,p = findmatchingmf(name)
        for c in f:
            #print(c)
            mfCodes.append(c[1])
    lookatmfs_codes(mfCodes, startDate, endDate)


def lookatmfs_ops(opType="TOP", startDate=-1, endDate=-1, count=10):
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
    tData = numpy.zeros([gData['nextMFIndex'], (endDateIndex-startDateIndex+1)])
    for r in range(gData['nextMFIndex']):
        try:
            tData[r,:], tMovAvg, tRollingRet, tStart, tEnd, tAbsRetPercent, tRetPA, tDurYrs = procdata_relative(gData['data'][r,startDateIndex:endDateIndex+1])
        except:
            traceback.print_exc()
            print("WARN:{}:{}".format(r,gData['names'][r]))
    #breakpoint()
    sortedIndex = numpy.argsort(tData[:,-1])
    mfCodes = []
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
        mfName = gData['names'][i]
        mfCode = gData['index2code'][i]
        mfCodes.append(mfCode)
        #print("{}: {}:{}".format(i, mfCode, mfName))
    lookatmfs_codes(mfCodes, startDate, endDate)


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
        lookatmfs_names(job, startDate, endDate)
    else:
        if job.upper() in [ "OP:TOP", "OP:BOTTOM" ]:
            job = job[3:]
            lookatmfs_ops(job, startDate, endDate, count)
        else:
            print("ERRR:lookat_data: unknown operation:", job)
            print("INFO:lookat_data: If you want to look up MF names put them in a list")
    _restore_dataproccontrols(savedDataProcControls)


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

    NOTE: By default it follows the same prompts as python.
    """
    lines = ""
    bMulti = False
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
            if (lineStripped != "") and (lineStripped[-1] == ':'):
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
    """
    bQuit = False
    while not bQuit:
        try:
            #breakpoint()
            cmd = input_multi(theFile=theFile)
            if gbREPLPrint:
                if '\n' not in cmd:
                    if '=' not in cmd:
                        if not cmd.startswith("print"):
                            cmd = "print({})".format(cmd)
            exec(cmd,globals())
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
print("MFUtilsKVC: A stupid exploration of MF NAV data")
print("License: GPL")
print("PLEASE DONT USE THIS PROGRAM TO MAKE ANY DECISIONS OR INFERENCES OR ...")

setup()
if len(sys.argv) > 1:
    handle_args()
else:
    do_run()

