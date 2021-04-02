# Module to get Indian MFs data
# HanishKVC, 2021

import os
import calendar
import sys
import datetime
import hlpr
import enttypes
import datasrc


#
# Fetching and Saving related
#
MFS_FNAMECSV_TMPL = "data/%Y%m%d.csv"
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
    fileTmpl = MFS_FNAMECSV_TMPL
    tag = "IndiaMFDS"

    def __init__(self, basePath="~/", loadFilters=None, nameCleanupMap=None):
        if nameCleanupMap == None:
            nameCleanupMap = gNameCleanupMap
        super().__init__(basePath, loadFilters, nameCleanupMap)
        if loadFilters != None:
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
                date = datetime.datetime.strptime(la[7], "%d-%b-%Y")
                date = hlpr.dateint(date.year,date.month,date.day)
                #print(code, name, nav, date)
                todayfile.add_ent(today, code, name, nav, curMFType, date)
            except:
                print("ERRR:IndiaMFDS:parse_csv:{}".format(l))
                print(sys.exc_info())
        tFile.close()




