# Module to help work with Indian MFs
# HanishKVC, 2021

import os
import calendar
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
    typeId = -1
    for l in tFile:
        l = l.strip()
        if l == '':
            continue
        if l[0].isalpha():
            #print("WARN:parse_csv:Skipping:{}".format(l))
            if l[-1] == ')':
                curMFType = l
                if curMFType not in gData['mfTypes']:
                    typeId += 1
                    gData['mfTypes'][curMFType] = []
                    checkTypeId = gData['mfTypesId'].get(curMFType, -1)
                    if checkTypeId != -1:
                        if checkTypeId != typeId:
                            input("DBUG:ParseCSV:TypeId Mismatch")
                    gData['mfTypesId'][curMFType] = typeId
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
                gData['typeId'][mfIndex] = typeId
            else:
                if name != gData['names'][mfIndex]:
                    input("WARN:parse_csv:Name mismatch?:{} != {}".format(name, gData['names'][mfIndex]))
            gData['data'][mfIndex,gData['dateIndex']] = nav
            gData['lastSeen'][mfIndex] = date
        except:
            print("ERRR:parse_csv:{}".format(l))
            print(sys.exc_info())
    tFile.close()


def fetch4date(y, m, d):
    """
    Fetch data for the given date.
    """
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


def load4date(y, m, d):
    """
    Load data for the given date.

    NOTE: This logic wont fill in prev nav for holidays,
    you will have to call fillin4holidays explicitly.
    """
    fName = MFS_FNAMECSV_TMPL.format(y,m,d)
    parse_csv(fName)


