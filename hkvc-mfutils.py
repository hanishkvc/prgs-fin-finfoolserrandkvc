#!/usr/bin/env python3
# Get and work with MF nav data
# HanishKVC, 2021

import sys
import calendar
import os
import datetime
import numpy


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


def setup():
    setup_gdata()
    setup_paths()


def proc_days(start, end, handle_date_func):
    """
    call the passed function for each date with the given start and end range.
        The date will be passed to the passed function as year, month, date
        as integers.

    start and end need to be dictionaries {'y': year_int, 'm': month_int, 'd': date_int}
        month_int should be from 1 to 12
        date_int should be from 1 to 31; 'd' and thus inturn date_int is optional
    """
    print("INFO:proc_days:from {} to {}".format(start, end))
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


def proc_datestr(dateStr, fallBackMonth=1):
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


def proc_datestr_startend(sStart, sEnd):
    """
    Convert the start and end dates given in string notation of YYYYMMDD into
    this programs internal date dictionary representation.

    The dates should follow the YYYY[MM[DD]] format, where [] means optional.
    """
    start = proc_datestr(sStart, 1)
    end = proc_datestr(sEnd, 12)
    return start, end


def fetch4daterange(sStart, sEnd):
    """
    Fetch data for given date range.

    The dates should follow one of these formats YYYY or YYYYMM or YYYYMMDD i.e YYYY[MM[DD]]
    """
    start, end = proc_datestr_startend(sStart, sEnd)
    proc_days(start, end, fetch4date)


def parse_csv(sFile):
    """
    Parse the specified data csv file and load it into global data dictionary.
    """
    tFile = open(sFile)
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
            try:
                nav  = float(la[4])
            except:
                nav = -1
            date = datetime.datetime.strptime(la[7], "%d-%b-%Y")
            date = date.year*10000+date.month*100+date.day
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


def load4date(y, m, d):
    """
    Load data for the given date.
    """
    gData['dateIndex'] += 1
    fName = FNAMECSV_TMPL.format(y,m,d)
    parse_csv(fName)


def load4daterange(sStart, sEnd):
    """
    Load data for given date range.

    The dates should follow one of these formats YYYY or YYYYMM or YYYYMMDD i.e YYYY[MM[DD]]
    """
    start, end = proc_datestr_startend(sStart, sEnd)
    proc_days(start, end, load4date)


def findmatchingmf(mfName, fullMatch=False, partialTokens=True, ignoreCase=True):
    """
    Find the MFs which match the given mfName.

    If fullMatch is True, then checks for a full match, else
    it tries to find MFNames in its dataset, which contain
    some or all of the tokens in the given mfName,.

    If partialTokens is True, then tokens in the given mfName
    could appear as part of bigger token in MFNames in its
    dataset.

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
        if ignoreCase:
            curName = curName.upper()
        namesIndex += 1
        matchCnt = 0
        if fullMatch:
            if curName == mfName:
                mfNameFullMatch.append([curName, namesIndex])
            continue
        for token in mfNameTokens:
            if partialTokens:
                if curName.find(token) != -1:
                    matchCnt += 1
            else:
                if token in curName.split(' '):
                    matchCnt += 1
        if matchCnt == len(mfNameTokens):
            mfNameFullMatch.append([curName, namesIndex])
        elif matchCnt > 0:
            mfNamePartMatch.append([curName, namesIndex])
    return mfNameFullMatch, mfNamePartMatch


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
            print(excInfo)


#
# The main flow starts here
#
setup()
if len(sys.argv) > 1:
    fetch4daterange(sys.argv[1], sys.argv[2])
else:
    do_interactive()

