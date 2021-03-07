#!/usr/bin/env python3
# Get and work with MF nav data
# HanishKVC, 2021

import sys
import calendar
import os
import datetime
import numpy
import matplotlib.pyplot as plt
import time
import traceback
import readline


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

"""

# The tokens in the REMOVE_NAMETOKENS list will be matched against MFName,
# and matching MFs will be silently ignored while loading the MF data.
MF_REMOVE_NAMETOKENS = [ "dividend", "low duration", "liquid", "overnight", "money market" ]

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
# Fetching Index historic data
#
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


def setup_gdata():
    """
    Initialise the gData dictionary
    """
    gData['code2index'] = {}
    gData['index2code'] = {}
    gData['data'] = numpy.zeros([8192*4,8192])
    gData['nextMFIndex'] = 0
    gData['dateIndex'] = -1
    gData['names'] = []
    gData['dates'] = []
    gData['removed'] = set()


def setup():
    setup_gdata()
    setup_paths()
    print("WARN:MF_REMOVE_NAMETOKENS:", MF_REMOVE_NAMETOKENS)


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
                handle_date_func(y,m,d)


def wget_better(url, localFName):
    #cmd = "curl {} --remote-time --time-cond {} --output {}".format(url,fName,fName)
    mtimePrev = os.stat(localFName).st_mtime
    cmd = "wget {} --continue --output-document={}".format(url,localFName)
    os.system(cmd)
    mtimeNow = os.stat(localFName).st_mtime
    if mtimeNow != mtimePrev:
        os.remove(localFName)
        os.system(cmd)


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
    wget_better(url, fName)


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

    NOTE: Any MFs whose name contain any of the tokens specified in MF_REMOVE_NAMETOKENS
    will be silently ignored and not added to the dataset.
    """
    tFile = open(sFile)
    removedMFsCnt = 0
    for l in tFile:
        l = l.strip()
        if l == '':
            continue
        if l[0].isalpha():
            #print("WARN:parse_csv:Skipping:{}".format(l))
            continue
        try:
            la = l.split(';')
            code = int(la[0])
            name = la[1]
            bNameMatch = False
            for nameToken in MF_REMOVE_NAMETOKENS:
                if nameToken.upper() in name.upper():
                    bNameMatch = True
            if bNameMatch:
                gData['removed'].add(str([code, name]))
                removedMFsCnt += 1
                continue
            try:
                nav  = float(la[4])
            except:
                nav = -1
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
            else:
                if name != gData['names'][mfIndex]:
                    input("WARN:parse_csv:Name mismatch?:{} != {}".format(name, gData['names'][mfIndex]))
            gData['data'][mfIndex,gData['dateIndex']] = nav
        except:
            print("ERRR:parse_csv:{}".format(l))
            print(sys.exc_info())
    tFile.close()


def print_removed():
    """
    Print the removed list of MFs, so that user can cross verify, things are fine.
    """
    print("WARN: List of REMOVED MFs")
    for removedMF in gData['removed']:
        print(removedMF)
    print("WARN: The above MFs were removed when loading")


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
    then ensure that we have atleast data loaded till the 1st non existant date.
    """
    start, end = proc_date_startend(startDate, endDate)
    try:
        proc_days(start, end, load4date, gbNotBeyondYesterday)
    except:
        excInfo = sys.exc_info()
        print(excInfo)
    fillin4holidays()
    print_removed()


def load_data(startDate, endDate = None, bClearData=True):
    """
    Load data for given date range.

    The dates should follow one of these formats YYYY or YYYYMM or YYYYMMDD i.e YYYY[MM[DD]]

    NOTE: This logic takes care of filling in nav values for holidays
    automatically by calling fillin4holidays.

    NOTE: If we dont have csv files for all the dates specified, in the date range,
    then ensure that we have atleast data loaded till the 1st non existant date.
    """
    if endDate == None:
        endDate = startDate
    if bClearData:
        setup_gdata()
    load4daterange(startDate, endDate)


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


def findmatchingmf(mfName, fullMatch=False, partialTokens=False, ignoreCase=True):
    """
    Find the MFs which match the given mfName.

    If fullMatch is True, then checks for a full match, else
    it tries to find MFNames in its dataset, which contain
    some or all of the tokens in the given mfName,.

    If partialTokens is True, then tokens in the given mfName
    could appear as part of bigger token in MFNames in its
    dataset. Else the token in given mfName and token in the
    MFNames in the dataset should match fully at the individual
    token level.

    if ignoreCase is True, then case of the given mfName,
    is ignored while trying to find a match.

    It returns those names which match all given tokens,
    as well as those names which only match certain tokens.
    """
    if ignoreCase:
        mfName = mfName.upper()
    mfName = mfName.strip()
    mfNameTokens = mfName.split(' ')
    mfNameFullMatch = []
    mfNamePartMatch = []
    namesIndex = -1
    for curName in gData['names']:
        curName_asis = curName
        if ignoreCase:
            curName = curName.upper()
        namesIndex += 1
        matchCnt = 0
        if fullMatch:
            if curName == mfName:
                mfNameFullMatch.append([curName_asis, gData['index2code'][namesIndex], namesIndex])
            continue
        for token in mfNameTokens:
            if partialTokens:
                if curName.find(token) != -1:
                    matchCnt += 1
            else:
                if token in curName.split(' '):
                    matchCnt += 1
        if matchCnt == len(mfNameTokens):
            mfNameFullMatch.append([curName_asis, gData['index2code'][namesIndex], namesIndex])
        elif matchCnt > 0:
            mfNamePartMatch.append([curName_asis, gData['index2code'][namesIndex], namesIndex])
    return mfNameFullMatch, mfNamePartMatch


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


def lookatmfs_codes(mfCodes, startDate=-1, endDate=-1):
    """
    Given a list of MF codes (as in AMFI dataset), look at their data.

    Different representations of the data is plotted for the range of date given,
    provided the corresponding global flags are enabled.

    NOTE: The plot per se is not shown, till user calls show_plot()
    """
    startDateIndex, endDateIndex = _date2index(startDate, endDate)
    for code in mfCodes:
        index = gData['code2index'][code]
        name = gData['names'][index]
        aRawData = gData['data'][index, startDateIndex:endDateIndex+1]
        aRelData, aMovAvg, aRollingRet, aStart, aEnd, aAbsRetPercent, aRetPA, durYrs = procdata_relative(aRawData, gbDoMovingAvg, gbDoRollingRet)
        aLabel = "{}: {:6.2f}% {:6.2f}%pa ({:4.1f}Yrs) : {:8.4f} - {:8.4f}".format(code, round(aAbsRetPercent,2), round(aRetPA,2), round(durYrs,1), aStart, aEnd)
        print(aLabel, name)
        if gbDoRawData:
            plt.plot(aRawData, label="{}, {}:Raw".format(aLabel,name[:giLabelNameChopLen]))
        if gbDoRelData:
            plt.plot(aRelData, label="{}, {}:Rel".format(aLabel,name[:giLabelNameChopLen]))
        if gbDoMovingAvg:
            if MOVINGAVG_CONVOLVEMODE == 'valid':
                tStartPos = int(MOVINGAVG_WINSIZE/2)
            else:
                tStartPos = 0
            plt.plot(list(range(tStartPos,len(aMovAvg)+tStartPos)), aMovAvg, label="{}, {}:DMA{}".format(aLabel,MOVINGAVG_WINSIZE,name[:giLabelNameChopLen]))
        if gbDoRollingRet:
            plt.plot(aRollingRet, label="{}, {}:Rol{}".format(aLabel,name[:giLabelNameChopLen],ROLLINGRET_WINSIZE))


def show_plot():
    """
    Show the data plotted till now.
    """
    leg = plt.legend()
    for line in leg.get_lines():
        line.set_linewidth(4)
    plt.grid(True)
    plt.show()


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


def do_run(theFile=None):
    """
    Run the REPL logic of this program.
    Read-Eval-Print Loop

    NOTE: If a script file is passed to the logic, it will fall back to
    interactive mode, once there are no more commands in the script file.
        Script file can use quit() to exit the program automatically
        if required.
    """
    bQuit = False
    while not bQuit:
        try:
            if theFile == None:
                cmd = input(":")
            else:
                cmd = theFile.readline()
                if cmd == '':
                    theFile=None
                    continue
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
setup()
if len(sys.argv) > 1:
    handle_args()
else:
    do_run()

