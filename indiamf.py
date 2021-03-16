# Module to help work with Indian MFs
# HanishKVC, 2021

import os
import calendar
import sys
import datetime
import pickle
import hlpr


gData = None
#
# Fetching and Saving related
#
MFS_FNAMECSV_TMPL = "data/{}{:02}{:02}.csv"
#https://www.amfiindia.com/spages/NAVAll.txt?t=27022021
#http://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?frmdt=01-Feb-2021
MFS_BaseURL = "http://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?frmdt={}-{}-{}"



def setup_paths(basePath):
    """
    Setup the basepath for data files, based on path passed by main logic
    """
    global MFS_FNAMECSV_TMPL
    MFS_FNAMECSV_TMPL = os.path.expanduser(os.path.join(basePath, MFS_FNAMECSV_TMPL))
    print("INFO:IndiaMF:setup_paths:", MFS_FNAMECSV_TMPL)


def setup(basePath):
    setup_paths(basePath)
    print("INFO:setup:gNameCleanupMap:", gNameCleanupMap)


gNameCleanupMap = [
        ['-', ' '],
        ['Divided', 'Dividend'],
        ['Diviend', 'Dividend'],
        ['Divdend', 'Dividend'],
        ]
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
    today = {
                'mfTypes': [],
                'code2index': {},
                'mfs': []
            }
    typeId = -1
    mfIndex = -1
    for l in tFile:
        l = l.strip()
        if l == '':
            continue
        if l[0].isalpha():
            if l[-1] == ')':
                curMFType = l
                if curMFType not in today['mfTypes']:
                    typeId += 1
                    today['mfTypes'].append([curMFType,[]])
            continue
        try:
            la = l.split(';')
            code = int(la[0])
            name = hlpr.string_cleanup(la[1], gNameCleanupMap)
            try:
                nav  = float(la[4])
            except:
                nav = 0
            date = datetime.datetime.strptime(la[7], "%d-%b-%Y")
            date = hlpr.dateint(date.year,date.month,date.day)
            #print(code, name, nav, date)
            checkMFIndex = today['code2index'].get(code, None)
            if checkMFIndex == None:
                mfIndex += 1
                today['code2index'][code] = mfIndex
                today['mfs'].append([code, name, nav, date, typeId])
                today['mfTypes'][typeId][1].append(code)
            else:
                input("WARN:IndiaMF:parse_csv:Duplicate MF {}:{}=={}".format(code, name, today['mfs'][checkMFIndex][1]))
        except:
            print("ERRR:IndiaMF:parse_csv:{}".format(l))
            print(sys.exc_info())
    tFile.close()
    return today


def _loaddata(today):
    """
    Parse the specified data csv file and load it into global data dictionary.

    NOTE: It uses the white and black lists wrt MFTypes and MFNames, if any
    specified in gData, to decide whether a given MF should be added to the
    dataset or not. User can control this in the normal usage flow by either
    passing these lists explicitly to load_data and or by setting related
    global variables before calling load_data.
    """
    # Handle MFTypes
    mfTypesId = -1
    mfIndex = -1
    for [curMFType, mfCodes] in today['mfTypes']:
        mfTypesId += 1
        if curMFType not in gData['mfTypes']:
            gData['mfTypes'][curMFType] = []
            checkTypeId = gData['mfTypesId'].get(curMFType, -1)
            if checkTypeId != -1:
                if checkTypeId != mfTypesId:
                    input("DBUG:IndiaMF:_LoadData:MFTypesId Mismatch")
            gData['mfTypesId'][curMFType] = mfTypesId
        if gData['whiteListEntTypes'] == None:
            bSkipCurMFType = False
        else:
            fm,pm = hlpr.matches_templates(curMFType, gData['whiteListEntTypes'])
            if len(fm) == 0:
                bSkipCurMFType = True
            else:
                bSkipCurMFType = False
        if bSkipCurMFType:
            continue

        # Handle MFs
        for mfCode in mfCodes:
            mfIndex += 1
            todayMFIndex = today['code2index'][mfCode]
            code, name, nav, date, typeId = today['mfs'][todayMFIndex]
            if (mfCode != code) or (typeId != mfTypesId):
                print("DBUG:IndiaMF:_LoadData: Code[{}]|TypeId[{}] NotMatchExpected [{}]|[{}], skipping".format(code, typeId, mfCode, mfTypesId))
                continue
            if (gData['whiteListEntNames'] != None):
                fm, pm = hlpr.matches_templates(name, gData['whiteListEntNames'])
                if len(fm) == 0:
                    gData['skipped'].add(str([code, name]))
                    continue
            if (gData['blackListEntNames'] != None):
                fm, pm = hlpr.matches_templates(name, gData['blackListEntNames'])
                if len(fm) > 0:
                    gData['skipped'].add(str([code, name]))
                    continue

            checkMFIndex = gData['code2index'].get(code, None)
            if checkMFIndex == None:
                theIndex = gData['nextMFIndex']
                if mfIndex != theIndex:
                    print("DBUG:IndiaMF:_LoadData:{}: mfIndex[{}] NotMatchExpected [{}], skipping".format(code, mfIndex, theIndex))
                    continue
                gData['nextMFIndex'] += 1
                gData['code2index'][code] = mfIndex
                gData['index2code'][mfIndex] = code
                gData['names'].append(name)
                gData['mfTypes'][curMFType].append(code)
                gData['typeId'][mfIndex] = mfTypesId
            else:
                if (name != gData['names'][checkMFIndex]) or (checkMFIndex != mfIndex):
                    input("DBUG:IndiaMF:_LoadData:Name[{}]|mfIndex[{}]  mismatch [{}]|[{}]".format(name, mfIndex, gData['names'][checkMFIndex], checkMFIndex))
                    continue
            gData['data'][mfIndex,gData['dateIndex']] = nav
            gData['lastSeen'][mfIndex] = date


def _fetchdata(url, fName):
    """
    Fetch give url to specified file, and check its valid.
    """
    print(url, fName)
    hlpr.wget_better(url, fName)
    f = open(fName)
    l = f.readline()
    if not l.startswith("Scheme Code"):
        print("ERRR:fetch4date:Not a valid nav file, removing it")
        os.remove(fName)
    f.close()


def _pickleok(fName):
    fName = "{}.pickle".format(fName)
    return os.path.exists(fName)


def fetch4date(y, m, d):
    """
    Fetch data for the given date.
    """
    url = MFS_BaseURL.format(d,calendar.month_name[m][:3],y)
    fName = MFS_FNAMECSV_TMPL.format(y,m,d)
    if not _pickleok(fName):
        _fetchdata(url, fName)
        today = parse_csv(fName)
        _savepickle(fName, today)


def _savepickle(fName, today):
    fName = "{}.pickle".format(fName)
    print("INFO:IndiaMF:SavePickle:", fName)
    f = open(fName, 'wb+')
    pickle.dump(today, f)
    f.close()


def _loadpickle(fName):
    fName = "{}.pickle".format(fName)
    if os.path.exists(fName):
        f = open(fName, 'rb')
        today = pickle.load(f)
        return True, today
    return False, None


def load4date(y, m, d):
    """
    Load data for the given date.

    NOTE: This logic wont fill in prev nav for holidays,
    you will have to call fillin4holidays explicitly.
    """
    fName = MFS_FNAMECSV_TMPL.format(y,m,d)
    ok,today = _loadpickle(fName)
    if ok:
        _loaddata(today)


