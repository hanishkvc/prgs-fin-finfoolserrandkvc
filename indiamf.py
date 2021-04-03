# Module to get Indian MFs data
# HanishKVC, 2021

import sys
import time
import zipfile
import traceback
import hlpr
import datasrc
import todayfile


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
    dataKeys = [ 'data' ]
    tag = "IndiaMFDS"

    def __init__(self, basePath="~/", loadFilters=None, nameCleanupMap=None):
        if nameCleanupMap == None:
            nameCleanupMap = gNameCleanupMap
        super().__init__(basePath, loadFilters, nameCleanupMap)
        hlpr.loadfilters_setup(loadFilters, "indiamf", MF_ALLOW_ENTTYPES, MF_ALLOW_ENTNAMES, MF_SKIP_ENTNAMES)


    def _valid_remotefile(self, fName):
        f = open(fName)
        l = f.readline()
        f.close()
        if not l.startswith("Scheme Code"):
            return False
        return True


    def _parse_file(self, sFile, today):
        """
        Parse the specified data csv file and load it into passed today dictionary.
        """
        tFile = open(sFile)
        curMFType = ""
        for l in tFile:
            l = l.strip()
            if l == '':
                continue
            if l[0].isalpha():
                if l[-1] == ')':
                    curMFType = l
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

class IndiaSTKDS(datasrc.DataSrc):

    urlTmpl = STK_BASEURL
    pathTmpl = STK_FNAMECSV_TMPL
    dataKeys = [ 'data' ]
    tag = "IndiaSTKDS"

    def __init__(self, basePath="~/", loadFilters=None, nameCleanupMap=None):
        if nameCleanupMap == None:
            nameCleanupMap = gNameCleanupMap
        super().__init__(basePath, loadFilters, nameCleanupMap)
        hlpr.loadfilters_setup(loadFilters, "indiastk", None, None, None)


    def _get_parts(self, fName):
        date = time.strptime(fName[-12:-4], "%Y%m%d")
        csvFile = time.strftime("Pr%d%m%y.csv", date)
        print("DBUG:{}:GetParts:{},{}".format(self.tag, csvFile, date))
        return csvFile, date


    def _valid_remotefile(self, fName):
        z = zipfile.ZipFile(fName)
        csvFile, dateT = self._get_parts(fName)
        f = z.open(csvFile)
        l = f.readline()
        l = l.decode()
        f.close()
        if not l.startswith("MKT,SECURITY"):
            return False
        return True


    def _parse_file(self, sFile, today):
        """
        Parse the specified data csv file and load it into passed today dictionary.
        """
        z = zipfile.ZipFile(sFile)
        csvFile, dateT = self._get_parts(sFile)
        tFile = z.open(csvFile)
        tFile.readline()
        for l in tFile:
            l = l.decode()
            l = l.strip()
            lt = l.split(',')
            la = []
            for c in lt:
                la.append(c.strip())
            if la[0] == '':
                continue
            try:
                curType = la[0]
                if curType.lower() == 'y':
                    curType = 'Index'
                else:
                    curType = 'Stock'
                code = la[1]
                name = la[1]
                try:
                    val  = float(la[6])
                except:
                    val = 0
                date = hlpr.dateint(dateT.tm_year,dateT.tm_mon,dateT.tm_mday)
                #print(code, name, nav, date)
                todayfile.add_ent(today, code, name, [val], curType, date)
            except:
                print("ERRR:IndiaSTKDS:parse_csv:{}".format(l))
                traceback.print_exc()
        tFile.close()
        z.close()




