# Module to get Indian MFs data
# HanishKVC, 2021

import sys
import time
import zipfile
import traceback
import datetime
import os
import hlpr
import datasrc
import todayfile
import loadfilters


#
# Fetching and Saving related
#
MFS_FNAMECSV_TMPL = "data/IMF_%Y%m%d.csv"
#https://www.amfiindia.com/spages/NAVAll.txt?t=27022021
#http://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?frmdt=01-Feb-2021
MFS_BASEURL = "http://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?frmdt=%d-%b-%Y"

MF_ALLOW_ENTTYPES=[ "open equity", "open elss", "open other", "open hybrid", "open solution" ]
MF_ALLOW_ENTNAMES=None
MF_SKIP_ENTNAMES =[ "~PART~dividend", "-RE-(?i).*regular plan.*", "-RE-(?i).*bonus.*" ]

gNameCleanupMap = [
        ['-', ' '],
        ['Divided', 'Dividend'],
        ['Diviend', 'Dividend'],
        ['Divdend', 'Dividend'],
        ]

class IndiaMFDS(datasrc.DataSrc):

    urlTmpl = MFS_BASEURL
    pathTmpl = MFS_FNAMECSV_TMPL
    dataKeys = [ 'nav' ]
    tag = "IndiaMFDS"
    earliestDate = 20060401

    def __init__(self, basePath="~/", loadFilters=None, nameCleanupMap=None):
        """
        Initialise the India MFs related data source.

        Add a possible loadFilters wrt MFs.
        Also add NameCleanupMap to fix some errors noticed in MF Names.
        """
        if nameCleanupMap == None:
            nameCleanupMap = gNameCleanupMap
        super().__init__(basePath, loadFilters, nameCleanupMap)
        self.dataSrcType = datasrc.DSType.MF
        loadfilters.setup(self.tag, MF_ALLOW_ENTTYPES, MF_ALLOW_ENTNAMES, MF_SKIP_ENTNAMES, loadFilters)


    def _valid_remotefile(self, fName):
        """
        Check the file downloaded from the remote server seems fine
        by checking for the header in the 1st line as well as
        closed funds or multiple fund types.
        As No closed funds in 2006 and so, so avoiding close funds check.
        """
        f = open(fName)
        l = f.readline()
        bFoundHdr = False
        if l.startswith("Scheme Code"):
            bFoundHdr = True
        bFoundClosed = False
        bFundTypesCnt = 0
        for l in f:
            l = l.strip()
            if l == '':
                continue
            if l.lower().startswith('close'):
                bFoundClosed = True
            if l[0].isalpha() and l[-1] == ')':
                bFundTypesCnt += 1
        f.close()
        return (bFoundHdr and (bFundTypesCnt > 3))


    def _parse_file(self, sFile, today):
        """
        Parse the specified data csv file and load it into passed today dictionary.

        The MF CSV file is seen to have these three characteristics wrt lines specifying MFType
        1) They start with a Alpha character; NOTE: THis is being checked.
        2) They end with " )" NOTE: Currently only ')' being checked.
        3) They are followed by a emtpy line; NOTE: This is being checked.
        """
        tFile = open(sFile)
        curMFType = ""
        maybeMFType = ""
        maybeLCnt = -1
        lCnt = 0
        for l in tFile:
            lCnt += 1
            l = l.strip()
            if l == '':
                if (lCnt - maybeLCnt) == 1:
                    curMFType = maybeMFType
                continue
            if l[0].isalpha():
                if l[-1] == ')':
                    maybeMFType = l
                    maybeLCnt = lCnt
                continue
            try:
                la = l.split(';')
                code = int(la[0])
                name = la[1]
                try:
                    nav  = float(la[4])
                except:
                    nav = 0
                date = time.strptime(la[7], "%d-%b-%Y")
                date = hlpr.dateint(date.tm_year,date.tm_mon,date.tm_mday)
                #print(code, name, nav, date)
                todayfile.add_ent(today, code, name, [nav], curMFType, date)
            except:
                print("ERRR:IndiaMFDS:parse_csv:{}".format(l))
                print(sys.exc_info())
        tFile.close()



#
# Work with Indian Stocks
#

STK_FNAMECSV_TMPL = "data/ISTK_%Y%m%d.zip"
#https://archives.nseindia.com/archives/equities/bhavcopy/pr/PR010321.zip
STK_BASEURL = "https://archives.nseindia.com/archives/equities/bhavcopy/pr/PR%d%m%y.zip"
STK_FILEINZIP = "Pr%d%m%y.csv"
STK_FTYPESURL = "https://www1.nseindia.com/content/indices/ind_{}list.csv"


class IndiaSTKDS(datasrc.DataSrc):

    urlTmpl = STK_BASEURL
    pathTmpl = STK_FNAMECSV_TMPL
    dataKeys = [ 'open', 'high', 'low', 'close', 'volume' ]
    tag = "IndiaSTKDS"
    name = "NSE"
    bSkipWeekEnds = True
    earliestDate = 19940101
    urlFTypesTmpl = STK_FTYPESURL
    listFTypes = [ ['nifty 50', 'nifty50'], ['nifty 100', 'nifty100'], ['nifty 200', 'nifty200'], ['nifty 500', 'nifty500'],
                   ['nifty midcap 150', 'niftymidcap150'], ['nifty smallcap 250', 'niftysmallcap250'] ]

    def __init__(self, basePath="~/", loadFilters=None, nameCleanupMap=None):
        if nameCleanupMap == None:
            nameCleanupMap = gNameCleanupMap
        super().__init__(basePath, loadFilters, nameCleanupMap)
        self.dataSrcType = datasrc.DSType.Stock
        loadfilters.setup(self.tag, None, None, None, loadFilters)


    def _get_parts(self, fName):
        date = time.strptime(fName[-12:-4], "%Y%m%d")
        csvPDFile = time.strftime("Pd%d%m%y.csv", date)
        csvBCFile = time.strftime("Bc%d%m%y.csv", date)
        #print("DBUG:{}:GetParts:{},{}".format(self.tag, csvFile, date))
        return csvPDFile, csvBCFile, date


    def _valid_remotefile(self, fName):
        z = zipfile.ZipFile(fName)
        csvPDFile, csvBCFile, dateT = self._get_parts(fName)
        f = z.open(csvPDFile)
        l = f.readline()
        l = l.decode()
        f.close()
        if not l.startswith("MKT,SERIES,SYMBOL,SECURITY"):
            return False
        return True


    def _parse_pdfile(self, z, csvPDFile, today, dateT):
        """
        Parse the Pd file containing stocks related data.
        """
        tFile = z.open(csvPDFile)
        tFile.readline()
        for l in tFile:
            l = l.decode(errors='ignore')
            l = l.strip()
            lt = l.split(',')
            la = []
            for c in lt:
                la.append(c.strip())
            if la[0] == '':
                continue
            try:
                curType = la[0]
                series = la[1]
                code = la[2]
                name = la[3]
                if curType.lower() == 'y':
                    curType = '{} Index'.format(self.name)
                    code = name
                else:
                    curType = '{} Stock'.format(self.name)
                    if series.lower() != 'eq':
                        continue
                try:
                    vOpen  = float(la[5])
                    vHigh  = float(la[6])
                    vLow   = float(la[7])
                    vClose = float(la[8])
                    volume = int(la[10])
                    values = [vOpen, vHigh, vLow, vClose, volume]
                except:
                    values = [0, 0, 0, 0, 0]
                date = hlpr.dateint(dateT.tm_year,dateT.tm_mon,dateT.tm_mday)
                #print(code, name, nav, date)
                todayfile.add_ent(today, code, name, values, curType, date)
            except:
                print("ERRR:IndiaSTKDS:parse_pd_csv:{}".format(l))
                traceback.print_exc()
        tFile.close()


    #[ "EQ SHARES", "" ],
    #[ "DVR", "" ],
    purposesCM = [
        [ "SPLDIV", "DIVXYZ" ],
        [ "SPL DIV", "DIVXYZ" ],
        [ "+FV SPL", "/FV SPLIT" ],
        [ "/-", "" ],
        [ "ISSUE", "" ],
        ]
    purposeBCM = [
        [ "BON ", "BONUS" ],
        [ "BON-", "BONUS" ],
        [ " ", "" ],
        [ "BONUS", "BONUS " ],
        ]
    purposeSCM = [
        [ "-", "" ],
        [ "FV SPLIT", "SPLIT" ],
        [ "FV SPLT", "SPLIT" ],
        [ "FV SPL", "SPLIT" ],
        [ "FVSPLIT", "SPLIT" ],
        [ "FVSPLT", "SPLIT" ],
        [ "FVSPL", "SPLIT" ],
        [ "RS.", "" ],
        [ "RE.", "" ],
        [ "RS", "" ],
        [ "RE", "" ],
        [ "FROM", "" ],
        [ "FRM", "" ],
        [ "FR", "" ],
        [ " ", "" ],
        [ "SPLIT", "SPLIT " ],
        [ "TO", " TO " ],
        ]
    purposeDCM = [
        [ "-", "" ],
        [ "DIV.", "DIV" ],
        [ "DIVIDEND", "DIV" ],
        [ "PER", "" ],
        [ "PR", "" ],
        [ "PE", "" ],
        [ "SHARE", "" ],
        [ "SHAR", "" ],
        [ "SHAE", "" ],
        [ "SAHRE", "" ],
        [ "SHR", "" ],
        [ "SHA", "" ],
        [ "SH", "" ],
        [ "PS", "" ],
        [ "EQ", "" ],
        [ "UNIT", "" ],
        [ "UNI", "" ],
        [ "UN", "" ],
        [ "RS.", "" ],
        [ "RE.", "" ],
        [ "RS", "" ],
        [ "RE", "" ],
        [ "SPDIV", "SPL" ],
        [ "SP DIV", "SPL" ],
        [ ".@", " " ],
        [ "@", " " ],
        [ ":", " " ],
        [ "+", " " ],
        [ "&", " " ],
        [ " ", "" ],
        [ "PS", "" ],
        [ "DIVXYZ", "SPL" ],
        [ "DIV", "DIV " ],
        [ "INT ", "INTDIV" ],
        [ "INTDIV", " INTDIV" ],
        [ "SLDIV", " SLDIV" ],
        [ "SPL", " SPL " ],
        [ "INTRM", " INTRM " ],
        [ "FINAL", " FIN" ],
        [ "FIN", " FIN " ],
        [ "RIGHT", " RIGHT" ],
        [ "  ", " " ],
        ]
    def _parse_purposes(self, purposes, code=None, exDate=None):
        purposesC1 = hlpr.string_cleanup(purposes, self.purposesCM)
        lPurposes = purposesC1.upper().split('/')
        lReturn = []
        for purpose in lPurposes:
            bAdd = False
            try:
                if purpose.startswith('BON'):
                    os.system("echo '{}' >> /tmp/t.B".format(purpose))
                    purpose = hlpr.string_cleanup(purpose, self.purposeBCM)
                    if "DVR" in purpose:
                        print("WARN:IndiaStks:parse_purposes:{}:{}:Ignoring DVR...".format(code, exDate))
                        continue
                    tpurpose,datas = purpose.split(' ',1)
                    if datas == '':
                        continue
                    datas = datas.strip()
                    new,cur = datas.split(':')
                    new,cur = float(new), float(cur)
                    total = new+cur
                    adj = cur/total
                    actType = 'B'
                    bAdd = True
                elif (purpose.startswith("FV") or purpose.startswith('SPL')) and not (purpose.startswith('SPL INT DIV') or purpose.startswith("SPL-R")):
                    os.system("echo '{}' >> /tmp/t.S".format(purpose))
                    purpose = hlpr.string_cleanup(purpose, self.purposeSCM)
                    #if "FV" not in purposes:
                    #    input("DBUG:IndiaStks:parse_purposes:{}:{}:Split without FV?:{}:{}".format(code, exDate, purposes, purpose))
                    parts = purpose.split(' ')
                    cur,new = float(parts[1]),float(parts[3])
                    adj = new/cur
                    actType = 'S'
                    bAdd = True
                elif purpose.startswith('DIV'):
                    os.system("echo '{}' >> /tmp/t.D".format(purpose))
                    purpose = hlpr.string_cleanup(purpose, self.purposeDCM)
                    purpose = purpose.strip()
                    #input("DBUG:IndiaSTK:parse_purposes:{}:{}:{}:{}".format(code, exDate, purposes, purpose))
                    parts = purpose.split(' ')
                    if len(parts) <= 1:
                        continue
                    adj = 0
                    for part in parts:
                        if part == "":
                            continue
                        if part[0].isalpha():
                            continue
                        adj += abs(float(part))
                    actType = 'D'
                    bAdd = True
                if bAdd:
                    lReturn.append([actType, adj, purpose])
            except:
                traceback.print_exc()
                os.system("echo '{}' >> /tmp/t.E".format(purposes))
                #input("DBUG:IndiaSTK:parse_purposes:{}:{}:Exception:{}:{}".format(code, exDate, purposes, purpose))
        return lReturn


    def _parse_bcfile(self, z, csvBCFile, today):
        """
        Parse the Bc file containing corporate actions related to stocks.
        """
        todayfile.add_morecat(today, 'corpActD')
        tFile = z.open(csvBCFile)
        tFile.readline()
        for l in tFile:
            l = l.decode(errors='ignore')
            l = l.strip()
            lt = l.split(',')
            la = []
            for c in lt:
                la.append(c.strip())
            if la[0] == '':
                continue
            try:
                series = la[0]
                if series.lower() != 'eq':
                    continue
                code = la[1]
                name = la[2]
                exDate = la[6]
                if exDate == '':
                    continue
                exDate = hlpr.datestr2dateint(exDate)
                purposes = la[9]
                lPurposes = self._parse_purposes(purposes, code, exDate)
                for purpose in lPurposes:
                    todayfile.add_morecat_data(today, 'corpActD',[exDate, code, purpose[0], purpose[1], purpose[2]])
            except:
                input("ERRR:IndiaSTKDS:parse_bc_csv:{}:{}".format(csvBCFile, l))
                traceback.print_exc()
        tFile.close()


    def _parse_file(self, sFile, today):
        """
        Parse the specified data csv file and load it into passed today dictionary.
        """
        z = zipfile.ZipFile(sFile)
        csvPDFile, csvBCFile, dateT = self._get_parts(sFile)
        self._parse_pdfile(z, csvPDFile, today, dateT)
        self._parse_bcfile(z, csvBCFile, today)
        z.close()


    def _load_ftype(self, theName, theFName, entDB, opts, hdrLines=1):
        """
        Load the Fixed/Rarely changing types available from this data source (NSEIndia)

        This includes the explicitly defined Categories/Grouping like Nifty 50, Nifty xyz, ...
        This also includes the industry categorisation wrt the entities.
        """
        fName = self._ftype_fname(theFName)
        f = open(fName)
        for i in range(hdrLines):
            f.readline() # Skip the header
        entTypeName = "{} {}".format(self.name, theName)
        entTypeId = entDB.add_type(entTypeName)
        for l in f:
            l = l.strip()
            if l == '':
                continue
            la = l.split(',')
            indName = la[1]
            entName = la[2]
            entDB.add_type_member(entTypeId, entName)
            indName = "{} {}".format(self.name, indName)
            indId = entDB.add_type(indName)
            entDB.add_type_member(indId, entName)
        f.close()


