#!/usr/bin/env python3
# Helper routines
# HanishKVC, 2021
# GPL

import os
import re
import pickle


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


def pickleok(fName, minSize=16e3):
    """
    Check that a associated pickle file exists and that it has a safe
    minimum size to consider as potentially being valid pickle file.
    """
    fName = "{}.pickle".format(fName)
    if os.path.exists(fName):
        if os.stat(fName).st_size > minSize:
            return True
    return False


def savepickle(fName, data, msgTag='SavePickle'):
    fName = "{}.pickle".format(fName)
    print("INFO:{}:SavePickle:{}".format(msgTag, fName))
    f = open(fName, 'wb+')
    pickle.dump(data, f)
    f.close()


def loadpickle(fName):
    fName = "{}.pickle".format(fName)
    if os.path.exists(fName):
        f = open(fName, 'rb')
        data = pickle.load(f)
        return True, data
    return False, None


def gdata_add(gData, entTypeId, entType, code, name, nav, date, msgTag):
    entIndex = gData['code2index'].get(code, None)
    if entIndex == None:
        entIndex = gData['nextEntIndex']
        gData['nextEntIndex'] += 1
        gData['code2index'][code] = entIndex
        gData['index2code'][entIndex] = code
        gData['names'].append(name)
        gData['entTypes'][entType].append(code)
        gData['typeId'][entIndex] = entTypeId
    else:
        if (name != gData['names'][entIndex]):
            input("DBUG:{}:Name mismatch?:{} != {}".format(msgTag, name, gData['names'][entIndex]))
    gData['data'][entIndex,gData['dateIndex']] = nav
    gData['lastSeen'][entIndex] = date


