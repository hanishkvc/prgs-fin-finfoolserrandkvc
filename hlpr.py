#!/usr/bin/env python3
# Helper routines
# HanishKVC, 2021
# GPL

import os
import re
import pickle
import numpy


def wget_better(url, localFName):
    """
    If the file on the server is bigger than the local file,
    then redownload the file freshly, rather than appending to it,
    as chances are the local file was not a partial download, but
    rather a older version of the file with different data.
    """
    #cmd = "curl {} --remote-time --time-cond {} --output {}".format(url,fName,fName)
    if os.path.exists(localFName):
        mtimePrev = os.stat(localFName).st_mtime
    else:
        mtimePrev = -1
    cmd = "wget '{}' --continue --output-document={}".format(url,localFName)
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


def dateint(y, m, d):
    """
    Convert year, month and day into a numeric YYYYMMDD format.
    """
    return y*10000+m*100+d


def dateintparts(theDate):
    """
    Convert the full date YYYYMMDD int to its constituent parts i.e year, month and day.
    """
    y = int(theDate/10000)
    t = theDate%10000
    m = int(t/100)
    d = t%100
    return y,m,d


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
            return True, data, meta
    return False, None, None


def gdata_add(gData, gMeta, entTypeId, entType, code, name, nav, date, msgTag):
    entIndex = gMeta['code2index'].get(code, None)
    if entIndex == None:
        entIndex = gMeta['nextEntIndex']
        gMeta['nextEntIndex'] += 1
        gMeta['code2index'][code] = entIndex
        gMeta['index2code'][entIndex] = code
        gMeta['names'].append(name)
        gMeta['entTypes'][entType].append(code)
        gMeta['typeId'][entIndex] = entTypeId
    else:
        if (name != gMeta['names'][entIndex]):
            input("DBUG:{}:Name mismatch?:{} != {}".format(msgTag, name, gMeta['names'][entIndex]))
    gData['data'][entIndex,gMeta['dataIndex']] = nav
    gMeta['lastSeen'][entIndex] = date


def loadfilters_setup(theLoadFilters, loadFiltersName, whiteListEntTypes=None, whiteListEntNames=None, blackListEntNames=None):
    theLoadFilters[loadFiltersName] = {
            'whiteListEntTypes': whiteListEntTypes,
            'whiteListEntNames': whiteListEntNames,
            'blackListEntNames': blackListEntNames
        }


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


