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


"""

Usage scenario

fetch4daterange("2015", "202102")
load4daterange("2015", "2020")
#fillin4holidays()
#f,p = findmatchingmf("token1 token2")
#[print(x) for x in f]
#plt.plot(gData['data'][mfIndex])
#plt.show()
lookupmfs("token1 token2")
look4mfs()
look4mfs("TOP", 20150101, 20210228)

TODO:
    20 day, 50 day line, 10 week line, 200 day (Moving averages(simple, exponential))
    52 week high/low,

"""

# The tokens in the REMOVE_NAMETOKENS list will be matched against MFName,
# and matching MFs will be silently ignored while loading the MF data.
MF_REMOVE_NAMETOKENS = [ "dividend", "low duration", "liquid", "overnight", "money market" ]

gbNotBeyondYesterday = True
gbSkipWeekEnds = False
FNAMECSV_TMPL = "data/{}{:02}{:02}.csv"
#https://www.amfiindia.com/spages/NAVAll.txt?t=27022021
#http://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?frmdt=01-Feb-2021
gBaseURL = "http://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?frmdt={}-{}-{}"
gCal = calendar.Calendar()
gData = {}


def setup_paths():
    """
    Account for MFUTILS_BASE env variable if set
    """
    global FNAMECSV_TMPL
    FNAMECSV_TMPL = os.path.expanduser(os.path.join(os.environ.get('MFUTILS_BASE',"~/"), FNAMECSV_TMPL))
    print("INFO:setup_paths:", FNAMECSV_TMPL)


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


def fetch4date(y, m, d):
    """
    Fetch data for the given date.

    This is the default handler function passed to proc_days.

    One can call this directly by passing the year, month and date one is interested in.
        month should be one of 1 to 12
        day (month day) should be one of 1 to 31, as appropriate for month specified.
    """
    print(y,m,d)
    url = gBaseURL.format(d,calendar.month_name[m][:3],y)
    fName = FNAMECSV_TMPL.format(y,m,d)
    print(url, fName)
    cmd = "wget {} --continue --output-document={}".format(url,fName)
    os.system(cmd)


def datestr2datedict(dateStr, fallBackMonth=1):
    """
    Convert a date specified in YYYYMMDD format into internal date dictionary format
        MM and DD are optional.
        MM if not specified fallsback to the value passed through fallBackMonth arg.
        If DD is needed, then MM needs to be used.
    """
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


def date2datedict(date, fallBackMonth=1):
    return datestr2datedict(str(date), fallBackMonth)


def proc_date_startend(startDate, endDate):
    """
    Convert the start and end dates given in string notation of YYYYMMDD into
    this programs internal date dictionary representation.

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
    input("WARN: The above MFs were removed when loading")


def load4date(y, m, d):
    """
    Load data for the given date.

    NOTE: This logic wont fill in prev nav for holidays,
    you will have to call fillin4holidays explicitly.
    """
    gData['dateIndex'] += 1
    gData['dates'].append(dateint(y,m,d))
    fName = FNAMECSV_TMPL.format(y,m,d)
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


def procdata_relative(data):
    """
    Process the data relative to its 1st Non Zero value
    """
    for i in range(len(data)):
        dStart = data[i]
        if dStart > 0:
            break
    if dStart == 0:
        return data, 0, 0, 0
    dEnd = data[-1]
    data = ((data/dStart)-1)*100
    dPercent = data[-1]
    return data, dStart, dEnd, dPercent


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


def lookupmfs_codes(mfCodes, startDate=-1, endDate=-1):
    """
    Given a list of MF codes (as in AMFI dataset), look at their data.

    The data is plotted for the range of date given.
    """
    startDateIndex, endDateIndex = _date2index(startDate, endDate)
    for code in mfCodes:
        index = gData['code2index'][code]
        name = gData['names'][index]
        aTemp = gData['data'][index, startDateIndex:endDateIndex+1]
        aTemp, aStart, aEnd, aPercent = procdata_relative(aTemp)
        aLabel = "{}: {:6.2f}, {:8.4f} - {:8.4f}".format(code, round(aPercent,2), aStart, aEnd)
        print(aLabel, name)
        plt.plot(aTemp, label="{}, {}".format(aLabel,name[:36]))
    plt.legend()
    plt.grid(True)
    plt.show()


def lookupmfs(mfNames, startDate=-1, endDate=-1):
    """
    Given a list of MF names, look at their data.

    findmatchingmf logic is called wrt each given name in the list.
        All MFs returned as part of its full match list, will be looked at.

    The data is plotted for the range of date given.
    """
    mfNames = mfNames.split(';')
    mfCodes = []
    for name in mfNames:
        f,p = findmatchingmf(name)
        for c in f:
            #print(c)
            mfCodes.append(c[1])
    lookupmfs_codes(mfCodes, startDate, endDate)


def look4mfs(opType="TOP", startDate=-1, endDate=-1, count=10):
    """
    Look for MFs which are at the top or the bottom among all the MFs,
    based on their performance over the date range given.

    opType could be either "TOP" or "BOTTOM"
    startDate and endDate specify the date range to consider.
        should be numerals of the form YYYYMMDD
    count tells how many MFs to list from the top or bottom of the performance list.
    """
    startDateIndex, endDateIndex = _date2index(startDate, endDate)
    tData = numpy.zeros([gData['nextMFIndex'], (endDateIndex-startDateIndex+1)])
    for r in range(gData['nextMFIndex']):
        try:
            tData[r,:], tStart, tEnd, tPercent = procdata_relative(gData['data'][r,startDateIndex:endDateIndex+1])
        except:
            traceback.print_exc()
            print("WARN:{}:{}".format(r,gData['names'][r]))
    #breakpoint()
    sortedIndex = numpy.argsort(tData[:,-1])
    mfCodes = []
    if opType == "TOP":
        startIndex = -1
        endIndex = startIndex-count
        delta = -1
    elif opType == "BOTTOM":
        startIndex = 0
        endIndex = startIndex+count
        delta = 1
    for si in range(startIndex, endIndex, delta):
        i = sortedIndex[si]
        mfName = gData['names'][i]
        mfCode = gData['index2code'][i]
        mfCodes.append(mfCode)
        #print("{}: {}:{}".format(i, mfCode, mfName))
    lookupmfs_codes(mfCodes, startDate, endDate)


def do_interactive():
    """
    Run the interactive [REPL] logic of this program.
    Read-Eval-Print Loop
    """
    bQuit = False
    while not bQuit:
        try:
            cmd = input(":")
            exec(cmd,globals())
        except:
            excInfo = sys.exc_info()
            if excInfo[0] == SystemExit:
                break
            traceback.print_exc()


#
# The main flow starts here
#
setup()
if len(sys.argv) > 1:
    fetch4daterange(sys.argv[1], sys.argv[2])
else:
    do_interactive()

