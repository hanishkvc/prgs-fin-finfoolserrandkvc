# Module to help work with Indexes
# HanishKVC, 2021

import os
import calendar
import sys
import datetime
import hlpr
import enttypes


gData = None
gMeta = None
#
# Fetching and Saving related
#
FNAMECSV_TMPL = "data/BSESENSEX-{:04}{:02}{:02}.csv"
## Index historic data
#INDEX_BSESENSEX_URL = "https://api.bseindia.com/BseIndiaAPI/api/ProduceCSVForDate/w?strIndex=SENSEX&dtFromDate=01/01/2011&dtToDate=05/03/2021"
INDEX_BSESENSEX_BASEURL = "https://api.bseindia.com/BseIndiaAPI/api/ProduceCSVForDate/w?strIndex=SENSEX&dtFromDate=01/01/2000&dtToDate={:02}/{:02}/{:04}"



def setup_paths(basePath):
    """
    Setup the basepath for data files, based on path passed by main logic
    """
    global FNAMECSV_TMPL
    FNAMECSV_TMPL = os.path.expanduser(os.path.join(basePath, FNAMECSV_TMPL))
    print("INFO:Indexes:setup_paths:", FNAMECSV_TMPL)


def setup(basePath, theGData, theGMeta, theCB, theLoadFilters):
    global gData, gMeta
    setup_paths(basePath)
    gData = theGData
    gMeta = theGMeta
    theCB['fetch_data'].append(fetch_data)
    print("INFO:Indexes:Setup done")


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
                'entTypes': [],
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
                if curMFType not in today['entTypes']:
                    typeId += 1
                    today['entTypes'].append([curMFType,[]])
                else:
                    input("DBUG:Indexes:_parsecsv:Duplicate entType [{}] in [{}]".format(curMFType, sFile))
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
                today['entTypes'][typeId][1].append(code)
            else:
                input("WARN:Indexes:parse_csv:Duplicate MF {}:{}=={}".format(code, name, today['mfs'][checkMFIndex][1]))
        except:
            print("ERRR:Indexes:parse_csv:{}".format(l))
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
    for [curMFType, mfCodes] in today['entTypes']:
        mfTypesId = enttypes.add(curMFType)
        if gMeta['whiteListEntTypes'] == None:
            bSkipCurMFType = False
        else:
            fm,pm = hlpr.matches_templates(curMFType, gMeta['whiteListEntTypes'])
            if len(fm) == 0:
                bSkipCurMFType = True
            else:
                bSkipCurMFType = False
        if bSkipCurMFType:
            continue

        # Handle MFs
        for mfCode in mfCodes:
            todayMFIndex = today['code2index'][mfCode]
            code, name, nav, date, typeId = today['mfs'][todayMFIndex]
            if (mfCode != code):
                input("DBUG:Indexes:_LoadData: Code[{}] NotMatchExpected [{}], skipping".format(code, mfCode))
                continue
            if False and (typeId != mfTypesId):
                # Csv data files for different dates could have difference in what fund types and funds they have in them
                # especially wrt working days and holidays. So ignoring this.
                enttypes.list()
                print([x[0] for x in today['entTypes']])
                breakpoint()
            if (gMeta['whiteListEntNames'] != None):
                fm, pm = hlpr.matches_templates(name, gMeta['whiteListEntNames'])
                if len(fm) == 0:
                    gMeta['skipped'].add(str([code, name]))
                    continue
            if (gMeta['blackListEntNames'] != None):
                fm, pm = hlpr.matches_templates(name, gMeta['blackListEntNames'])
                if len(fm) > 0:
                    gMeta['skipped'].add(str([code, name]))
                    continue
            hlpr.gdata_add(gData, gMeta, mfTypesId, curMFType, code, name, nav, date, "Indexes:_LoadData")


def _fetchdata(url, fName):
    """
    Fetch give url to specified file, and check its valid.
    """
    print(url, fName)
    hlpr.wget_better(url, fName)
    f = open(fName)
    l = f.readline()
    if not l.startswith("Date,Open,High"):
        print("ERRR:fetch4date:Not a valid index file, removing it")
        os.remove(fName)
    f.close()


def fetch_data(startDate, endDate, opts=None):
    """
    Fetch data for the given date range.

    opts: a list of options supported by this logic
        'ForceLocal': When the logic decides that it has to fetch
            data file from the internet, it will cross check, if
            ForceLocal is True. If True, then the logic wont try
            to redownload
        'ForceRemote': If true, then the logic will try to fetch
            the data file again from the internet, irrespective
            of the local data pickle file is ok or not.
        NOTE: ForceRemote takes precedence over ForceLocal.
    """
    y,m,d = hlpr.dateintparts(endDate)
    url = INDEX_BSESENSEX_BASEURL.format(d,m,y)
    fName = FNAMECSV_TMPL.format(y,m,d)
    bParseCSV=False
    if opts == None:
        opts = {}
    bForceRemote = opts.get('ForceRemote', False)
    bForceLocal = opts.get('ForceLocal', False)
    if bForceRemote:
        _fetchdata(url, fName)
        bParseCSV=True
    elif not hlpr.pickle_ok(fName):
        if not bForceLocal:
            _fetchdata(url, fName)
        bParseCSV=True
    if bParseCSV:
        try:
            today = parse_csv(fName)
            hlpr.save_pickle(fName, today, [], "Indexes:fetch4Date")
        except:
            print("ERRR:Indexes:fetch4date:{}:ForceRemote[{}], ForceLocal[{}]".format(fName, bForceRemote, bForceLocal))
            print(sys.exc_info())


def load4date(y, m, d, opts):
    """
    Load data for the given date.

    NOTE: If loading pickled data fails, then it will try to load
    the data corresponding to given date, from the locally downloaded
    csv file if possible, else it will try to fetch it freshly from
    the internet/remote server.

    NOTE: This logic wont fill in prev nav for holidays,
    you will have to call fillin4holidays explicitly.
    """
    fName = FNAMECSV_TMPL.format(y,m,d)
    ok = False
    for i in range(3):
        ok,today,tIgnore = hlpr.load_pickle(fName)
        if ok:
            break
        else:
            print("WARN:Indexes:load4date:Try={}: No data pickle found for {}".format(i, fName))
            if i > 0:
                opts = { 'ForceRemote': True }
            else:
                opts = { 'ForceLocal': True }
            fetch4date(y, m, d, opts)
    _loaddata(today)


