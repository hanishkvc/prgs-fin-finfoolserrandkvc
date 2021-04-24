#!/usr/bin/env python3
# Helper routines
# HanishKVC, 2021
# GPL

import os
import re
import pickle
import numpy
import calendar
import time
import datetime


wgetLastTime = 0
wgetMinTimeGap = 3
wgetForcedDelay = 3
def wget_better(url, localFName):
    """
    If the file on the server is bigger than the local file,
    then redownload the file freshly, rather than appending to it,
    as chances are the local file was not a partial download, but
    rather a older version of the file with different data.
    """
    global wgetLastTime
    if (time.time()-wgetLastTime) < wgetMinTimeGap:
        print("INFO:WgetBetter:Sleeping for {} to avoid overloading web...".format(wgetForcedDelay))
        time.sleep(wgetForcedDelay)
    wgetLastTime = time.time()
    #cmd = "curl {} --remote-time --time-cond {} --output {}".format(url,fName,fName)
    if os.path.exists(localFName):
        mtimePrev = os.stat(localFName).st_mtime
    else:
        mtimePrev = -1
    cmd = "wget '{}' --continue --timeout=4 --tries=4 --output-document={}".format(url,localFName)
    print(cmd)
    os.system(cmd)
    if os.path.exists(localFName):
        mtimeNow = os.stat(localFName).st_mtime
        if (mtimePrev != -1) and (mtimeNow != mtimePrev):
            os.remove(localFName)
            os.system(cmd)


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

    NOTE: If -RE- is prefixed at begining of any of the match templates,
    then that particular match template will be processed using pythons
    re.fullmatch, instead of program's match logics.

        You can ignore case while re matching by usinging -RE-(?i)

        Remember its a full match so remember to put .* on eitherside
        as required.
    """
    theString_asis = theString
    if ignoreCase:
        theString = theString.upper()
    matchTmplFullMatch = []
    matchTmplPartMatch = []
    tmplIndex = -1
    #breakpoint()
    if type(matchTemplates) == str:
        matchTemplates = [ matchTemplates ]
    for matchTmpl in matchTemplates:
        matchTmplRaw = matchTmpl
        if ignoreCase:
            matchTmpl = matchTmpl.upper()
        tmplIndex += 1
        if fullMatch:
            if theString == matchTmpl:
                matchTmplFullMatch.append([theString_asis, tmplIndex])
            continue
        if matchTmpl.startswith('-RE-'):
            matchTmpl = matchTmplRaw[4:]
            if re.fullmatch(matchTmpl, theString_asis) != None:
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


def string_cleanup(theString, cleanupMap):
    """
    Use the given cleanup map to replace elements of the passed string.
    """
    for cm in cleanupMap:
        theString = theString.replace(cm[0], cm[1])
    return theString


def days_in(sDuration, bSkipWeekends):
    """
    Get the number of days in the given duration.
    The duration could be
        <Number> = number of days
        <Number>W = number of weeks
        <Number>M = number of months
        <Number>Y = number of years
    """
    if sDuration[-1].isnumeric():
        return int(sDuration)
    dType = sDuration[-1].upper()
    iDur = int(sDuration[:-1])
    if dType == 'Y':
        if bSkipWeekends:
            return int((365*5/7)*iDur)
        else:
            return 365*iDur
    elif dType == 'M':
        if bSkipWeekends:
            return int((365/12)*iDur*5/7)
        else:
            return int((365/12)*iDur)
    elif dType == 'W':
        if bSkipWeekends:
            return 5*iDur
        else:
            return 7*iDur
    return int(iDur)


def days2year(days, bSkipWeekends):
    """
    Convert num of days into equivalent years.
    This accounts for bSkipWeekends, when calculating the years.
    """
    if bSkipWeekends:
        years = days/260
    else:
        years = days/365
    return years


def dateint(y, m, d):
    """
    Convert year, month and day into a numeric YYYYMMDD format.
    """
    return y*10000+m*100+d


def date2dateint(date):
    """
    Convert a datetime.date or datetime.datetime object to date int of YYYYMMDD format.
    """
    return dateint(date.year, date.month, date.day)


def datestr2dateint(sDate):
    """
    Convert a date string of type DD-MM-YYYY or DD/MM/YYYY or DDMMYY to the date int
    of format YYYYMMDD.
    """
    if '/' in sDate:
        dtDate = datetime.datetime.strptime(sDate, "%d/%m/%Y")
    elif '-' in sDate:
        dtDate = datetime.datetime.strptime(sDate, "%d-%m-%Y")
    else:
        dtDate = datetime.datetime.strptime(sDate, "%d%m%Y")
    iDate = date2dateint(dtDate)
    return iDate


def not_beyond_today(date, bSkipTodayAlso=True):
    """
    If passed date is beyond today, then return today, else return passed date.
    """
    today = datetime.date.today()
    if bSkipTodayAlso:
        today = today - datetime.timedelta(1)
    if date > today:
        return today
    return date


def dateint2date(theDate, bStart=True, bNotInFuture=True):
    """
    Convert the full date YYYYMMDD int to a datetime date object.

    If the user passes only YYYY or YYYYMM, then it will assign a suitable month and
    day as required.

    NOTE: This logic cant handle situation where the date is before year 1000, because
    there wont be 4 digits to year part.
    TODO: May allow theDate to be passed as a string, so that we can have 0 prefixed as
    part of the years smaller than 1000, as required.
    """
    if theDate < 9999:
        if bStart:
            m,d = 1,1
        else:
            m,d = 12,31
        theDate = dateint(theDate,m,d)
    elif theDate < 999999:
        if bStart:
            theDate = theDate*100+1
        else:
            y = int(theDate/100)
            m = theDate%100
            theDate = theDate*100+calendar.monthlen(y,m)
    y = int(theDate/10000)
    t = theDate%10000
    m = int(t/100)
    d = t%100
    date = datetime.date(y,m,d)
    if bNotInFuture:
        date = not_beyond_today(date)
    return date


def pickle_ok(fName, minSize=16e3):
    """
    Check that a associated pickle file exists and that it has a safe
    minimum size to consider as potentially being valid pickle file.
    """
    fName = "{}.pickle".format(fName)
    if os.path.exists(fName):
        if os.stat(fName).st_size > minSize:
            f = open(fName, 'rb')
            pickleVer = pickle.load(f)
            f.close()
            if pickleVer == gPICKLEVER:
                return True
    print("WARN:pickle_ok:Failed for", fName)
    return False


gPICKLEVER="ffe.hkvc.v01"
def save_pickle(fName, data, meta, msgTag='SavePickle'):
    fName = "{}.pickle".format(fName)
    print("INFO:{}:SavePickle:{}".format(msgTag, fName))
    f = open(fName, 'wb+')
    pickle.dump(gPICKLEVER, f)
    pickle.dump(data, f)
    pickle.dump(meta, f)
    f.close()


def load_pickle(fName):
    fName = "{}.pickle".format(fName)
    if os.path.exists(fName):
        f = open(fName, 'rb')
        pickleVer = pickle.load(f)
        if pickleVer == gPICKLEVER:
            data = pickle.load(f)
            meta = pickle.load(f)
            f.close()
            return True, data, meta
        f.close()
    return False, None, None


def sane_array(theArray, skip):
    theSaneArray = theArray.copy()
    theSaneArray[numpy.isinf(theSaneArray)] = skip
    theSaneArray[numpy.isnan(theSaneArray)] = skip
    return theSaneArray


def array_str(arr, width=5, precision=2):
    if type(precision) == str:
        firstPrec = -1
        lastPrec = -1
        i = 0
        while (i < len(precision)):
            if precision[i] == 'A':
                allPrec = int(precision[i+1])
            elif precision[i] == 'L':
                lastPrec = int(precision[i+1])
            elif precision[i] == 'F':
                firstPrec = int(precision[i+1])
            i += 1
        if firstPrec == -1:
            firstPrec = allPrec
        if lastPrec == -1:
            lastPrec = allPrec
    else:
        allPrec = precision
        lastPrec = precision
        firstPrec = precision
    strA = "[ "
    iLast = len(arr)-1
    typeNumpyStr = type(numpy.str_())
    for i in range(iLast+1):
        if i == iLast:
            precision = lastPrec
        elif i == 0:
            precision = firstPrec
        else:
            precision = allPrec
        if type(arr[i]) == typeNumpyStr:
            strA = "{}{:>{width}} ".format(strA, arr[i], width=width)
        else:
            strA = "{}{:{width}.{precision}f} ".format(strA, arr[i], width=width, precision=precision)
    strA += "]"
    return strA


def printl(lFmt, lData, printInBtw=" ", printPrefix=None, printSufix=None, lWidths=None):
    """
    Print a list of formats and corresponding list of data.
    lFmt is the list of formats to use wrt each element in the data list (lData).

    printInBtw: The string to print inbetween each data element.
    printPrefix: The string to print if any before the actual data elements.
        If None, then no prefix is printed.
    printSufix: The string to print if any after the actual data elements.
        If None, then no sufix is printed.
    lWidths: If the user wants string data elements if any while printing,
        to be truncated before printing, then one should pass lWidths list,
        with the info about the string width. Also the user should use
        the keyword width as part of the lFmt formats.

    Each format element in the lFmt can either be a simple fmt string or a
    dictionary containing different format string for a string element and
    different format string for a number (or rather any other type) element.
    """
    if printPrefix != None:
        print(printPrefix, end="")
    for i,pf in enumerate(lFmt):
        if type(pf) == dict:
            if type(lData[i]) == str:
                pf = pf['str']
            else:
                pf = pf['num']
        if lWidths == None:
            print(pf.format(lData[i]), end=printInBtw)
        else:
            theData = lData[i]
            if type(theData) == str:
                theData = theData[:lWidths[i]]
            print(pf.format(theData, width=lWidths[i]), end=printInBtw)
    rem2Print = lData[i+1:]
    for rp in rem2Print:
        print(rp, end=printInBtw)
    if printSufix != None:
        print(printSufix)


def print_list(lData, width1st=16, widthRem=32):
    """
    Print a list of data, one item in a line.
    If each item in the list is inturn a list/tuple, then the
        1st element in each of it is given a minimum width
        Remaining elements are given a different minimum width
    """
    for data in lData:
        if type(data) in [ tuple, set, list]:
            ele1st = data[0]
            eleRem = data[1:]
            sEleRem = ""
            for ele in eleRem:
                sEleRem = "{}, {:{width}}".format(sEleRem, ele, width=widthRem)
            print("{:{width}} {}".format(ele1st, sEleRem, width=width1st))
        else:
            print(data)


def data_metakeys(dataKey):
    """
    Returns the possible Meta Keys related to given data key.

    MetaType: This key identifies the type of Op which generated the MetaData
    MetaData: This key points to raw meta data wrt each entity, which can be
        processed further for comparing with other entities etc.
    MetaLabel: This key points to processed label/summary info wrt each entity.
        This is useful for labeling plots etc.
    """
    mtypeKey="{}.MetaType".format(dataKey)
    mdataKey="{}.MetaData".format(dataKey)
    mlabelKey="{}.MetaLabel".format(dataKey)
    return mtypeKey, mdataKey, mlabelKey


def derive_keys(inKeys, keyNameTmpl="{}"):
    """
    Generate new key names for given list of keys, using the keyNameTmpl.
    Example keyNameTmpls include
        "{}", "w.{}", ...
    """
    outKeys = []
    for kI in inKeys:
        kO = keyNameTmpl.format(kI)
        outKeys.append(kO)
    return outKeys


